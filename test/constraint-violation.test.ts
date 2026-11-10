import type { Generated } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import dialect, { getDialect } from './dialect.js'
import { addPrimaryKey } from './test-utils.js'

import { KyselyService } from '../src/index.js'

interface DB {
  constraint_items: {
    id: Generated<number>
    email: string
    tenant: number
    age: number | null
  }
  constraint_children: {
    id: Generated<number>
    parent_id: number | null
  }
}

type ConstraintItem = {
  id: number
  email: string
  tenant: number
  age: number | null
}

type ConstraintChild = {
  id: number
  parent_id: number | null
}

const dialectName = getDialect()

function setup() {
  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    if (dialectName === 'sqlite') {
      await sql`PRAGMA foreign_keys = ON`.execute(db)
    }

    await db.schema.dropTable('constraint_children').ifExists().execute()
    await db.schema.dropTable('constraint_items').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('constraint_items')
        .addColumn('email', 'varchar(255)', (col) => col.notNull())
        .addColumn('tenant', 'integer', (col) => col.notNull())
        .addColumn('age', 'integer', (col) => col.check(sql`age >= 0`))
        .addUniqueConstraint('constraint_items_email_tenant', [
          'email',
          'tenant',
        ]),
      'id',
    ).execute()

    await addPrimaryKey(
      db.schema
        .createTable('constraint_children')
        .addColumn('parent_id', 'integer')
        .addForeignKeyConstraint(
          'constraint_children_parent_fk',
          ['parent_id'],
          'constraint_items',
          ['id'],
        ),
      'id',
    ).execute()
  }

  const properties = { id: true, email: true, tenant: true, age: true }

  const app = feathers<{
    constraint_items: KyselyService<ConstraintItem>
    exposed_constraint_items: KyselyService<ConstraintItem>
    constraint_children: KyselyService<ConstraintChild>
  }>()
    .use(
      'constraint_items',
      new KyselyService<ConstraintItem>({
        Model: db,
        name: 'constraint_items',
        properties,
      }),
    )
    .use(
      'exposed_constraint_items',
      new KyselyService<ConstraintItem>({
        Model: db,
        name: 'constraint_items',
        properties,
        exposeConflictColumns: ['email'],
      }),
    )
    .use(
      'constraint_children',
      new KyselyService<ConstraintChild>({
        Model: db,
        name: 'constraint_children',
        properties: { id: true, parent_id: true },
      }),
    )

  return { db, clean, app }
}

const { app, db, clean } = setup()

/** Resolve to the rejection reason, or fail if the promise resolves. */
async function rejection(promise: Promise<unknown>): Promise<any> {
  try {
    await promise
  } catch (err) {
    return err
  }
  throw new Error('expected the promise to reject')
}

describe('constraint violation', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  describe('unique', () => {
    it('→ Conflict, naming neither the value nor the table', async () => {
      await app
        .service('constraint_items')
        .create({ email: 'x@y.z', tenant: 1 })

      const err = await rejection(
        app.service('constraint_items').create({ email: 'x@y.z', tenant: 1 }),
      )

      expect(err.name).toBe('Conflict')
      expect(err.message).toBe('duplicate key value violates unique constraint')
      expect(err.data).toBeUndefined()
    })

    it('of the primary key → Conflict', async () => {
      const { id } = await app
        .service('constraint_items')
        .create({ email: 'a@y.z', tenant: 1 })

      const err = await rejection(
        app
          .service('constraint_items')
          .create({ id, email: 'b@y.z', tenant: 1 }),
      )

      expect(err.name).toBe('Conflict')
      expect(err.message).toBe('duplicate key value violates unique constraint')
    })

    it.skipIf(dialectName === 'mysql')(
      'names the columns the service exposes',
      async () => {
        await app
          .service('constraint_items')
          .create({ email: 'x@y.z', tenant: 1 })

        const err = await rejection(
          app
            .service('exposed_constraint_items')
            .create({ email: 'x@y.z', tenant: 1 }),
        )

        expect(err.name).toBe('Conflict')
        expect(err.data).toEqual({ columns: ['email'] })
      },
    )

    it.runIf(dialectName === 'mysql')(
      'names no column on MySQL, whose error names an index',
      async () => {
        await app
          .service('constraint_items')
          .create({ email: 'x@y.z', tenant: 1 })

        const err = await rejection(
          app
            .service('exposed_constraint_items')
            .create({ email: 'x@y.z', tenant: 1 }),
        )

        expect(err.name).toBe('Conflict')
        expect(err.data).toBeUndefined()
      },
    )
  })

  it('not-null → BadRequest, naming the column but not the table', async () => {
    const err = await rejection(
      app.service('constraint_items').create({ tenant: 1 } as any),
    )

    expect(err.name).toBe('BadRequest')
    expect(err.message).toContain('email')
    expect(err.message).not.toContain('constraint_items')
  })

  it('check → BadRequest, naming neither the constraint nor its expression', async () => {
    const err = await rejection(
      app
        .service('constraint_items')
        .create({ email: 'x@y.z', tenant: 1, age: -1 }),
    )

    expect(err.name).toBe('BadRequest')
    expect(err.message).not.toContain('constraint_items')
    expect(err.message).not.toContain('>=')
  })

  it('foreign key → BadRequest, naming neither table nor constraint', async () => {
    const err = await rejection(
      app.service('constraint_children').create({ parent_id: 999999 }),
    )

    expect(err.name).toBe('BadRequest')
    expect(err.message).not.toContain('constraint_children')
    expect(err.message).not.toContain('constraint_items')
  })

  it('removing a referenced row → BadRequest, naming neither table nor constraint', async () => {
    const parent = await app
      .service('constraint_items')
      .create({ email: 'x@y.z', tenant: 1 })
    await app.service('constraint_children').create({ parent_id: parent.id })

    const err = await rejection(
      app.service('constraint_items').remove(parent.id),
    )

    expect(err.name).toBe('BadRequest')
    expect(err.message).not.toContain('constraint_children')
    expect(err.message).not.toContain('constraint_items')
  })
})
