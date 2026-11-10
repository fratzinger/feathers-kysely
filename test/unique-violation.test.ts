import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import dialect, { getDialect } from './dialect.js'
import { addPrimaryKey } from './test-utils.js'

import { KyselyService } from '../src/index.js'

interface DB {
  unique_items: {
    id: Generated<number>
    email: string
    tenant: number
  }
}

type UniqueItem = {
  id: number
  email: string
  tenant: number
}

const dialectName = getDialect()

function setup() {
  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    await db.schema.dropTable('unique_items').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('unique_items')
        .addColumn('email', 'varchar(255)', (col) => col.notNull())
        .addColumn('tenant', 'integer', (col) => col.notNull())
        .addUniqueConstraint('unique_items_email_tenant', ['email', 'tenant']),
      'id',
    ).execute()
  }

  const properties = { id: true, email: true, tenant: true }

  const app = feathers<{
    unique_items: KyselyService<UniqueItem>
    exposed_unique_items: KyselyService<UniqueItem>
  }>()
    .use(
      'unique_items',
      new KyselyService<UniqueItem>({
        Model: db,
        name: 'unique_items',
        properties,
      }),
    )
    .use(
      'exposed_unique_items',
      new KyselyService<UniqueItem>({
        Model: db,
        name: 'unique_items',
        properties,
        exposeConflictColumns: ['email'],
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

describe('unique violation', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  it('→ Conflict, naming neither the value nor the table', async () => {
    await app.service('unique_items').create({ email: 'x@y.z', tenant: 1 })

    const err = await rejection(
      app.service('unique_items').create({ email: 'x@y.z', tenant: 1 }),
    )

    expect(err.name).toBe('Conflict')
    expect(err.message).toBe('duplicate key value violates unique constraint')
    expect(err.data).toBeUndefined()
  })

  it('of the primary key → Conflict', async () => {
    const { id } = await app
      .service('unique_items')
      .create({ email: 'a@y.z', tenant: 1 })

    const err = await rejection(
      app.service('unique_items').create({ id, email: 'b@y.z', tenant: 1 }),
    )

    expect(err.name).toBe('Conflict')
    expect(err.message).toBe('duplicate key value violates unique constraint')
  })

  it.skipIf(dialectName === 'mysql')(
    'names the columns the service exposes',
    async () => {
      await app.service('unique_items').create({ email: 'x@y.z', tenant: 1 })

      const err = await rejection(
        app
          .service('exposed_unique_items')
          .create({ email: 'x@y.z', tenant: 1 }),
      )

      expect(err.name).toBe('Conflict')
      expect(err.data).toEqual({ columns: ['email'] })
    },
  )

  it.runIf(dialectName === 'mysql')(
    'names no column on MySQL, whose error names an index',
    async () => {
      await app.service('unique_items').create({ email: 'x@y.z', tenant: 1 })

      const err = await rejection(
        app
          .service('exposed_unique_items')
          .create({ email: 'x@y.z', tenant: 1 }),
      )

      expect(err.name).toBe('Conflict')
      expect(err.data).toBeUndefined()
    },
  )
})
