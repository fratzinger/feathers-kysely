import type { Generated } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { describe } from 'vitest'
import dialect, { getDialect } from './dialect.js'

import { KyselyService, ERROR } from '../src/index.js'

interface DB {
  error_items: {
    id: Generated<number>
    name: string
    email: string | null
    age: number | null
  }
  error_children: {
    id: Generated<number>
    parent_id: number | null
  }
}

type ErrorItem = {
  id: number
  name: string
  email: string | null
  age: number | null
}

type ErrorChild = {
  id: number
  parent_id: number | null
}

function setup() {
  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    await db.schema.dropTable('error_children').ifExists().execute()
    await db.schema.dropTable('error_items').ifExists().execute()

    await db.schema
      .createTable('error_items')
      .addColumn('id', 'serial', (col) => col.primaryKey())
      .addColumn('name', 'varchar', (col) => col.notNull())
      .addColumn('email', 'varchar', (col) => col.unique())
      .addColumn('age', 'integer', (col) => col.check(sql`age >= 0`))
      .execute()

    await db.schema
      .createTable('error_children')
      .addColumn('id', 'serial', (col) => col.primaryKey())
      .addColumn('parent_id', 'integer', (col) =>
        col.references('error_items.id'),
      )
      .execute()
  }

  const app = feathers<{
    error_items: KyselyService<ErrorItem>
    error_children: KyselyService<ErrorChild>
  }>()

  app.use(
    'error_items',
    new KyselyService<ErrorItem>({
      Model: db,
      id: 'id',
      name: 'error_items',
      multi: true,
      properties: { id: true, name: true, email: true, age: true },
    }),
  )

  app.use(
    'error_children',
    new KyselyService<ErrorChild>({
      Model: db,
      id: 'id',
      name: 'error_children',
      properties: { id: true, parent_id: true },
    }),
  )

  return { db, clean, app }
}

const { app, db, clean } = setup()

const dialectName = getDialect()

/** Resolve to the rejection reason, or fail if the promise resolves. */
async function rejection(promise: Promise<unknown>): Promise<any> {
  try {
    await promise
  } catch (err) {
    return err
  }
  throw new Error('expected the promise to reject')
}

describe.skipIf(dialectName !== 'postgres')('postgres error handler', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  it('not-null violation → BadRequest, keeps the column, strips the table', async () => {
    const err = await rejection(
      app.service('error_items').create({ email: 'a@b.c' }),
    )

    expect(err.name).toBe('BadRequest')
    expect(err.message).toContain('not-null constraint')
    // The declared column survives (useful for the client, already public).
    expect(err.message).toContain('"name"')
    // The table/relation name is stripped, and the message is not the mangled
    // `null constraint` the old split('-') produced.
    expect(err.message).not.toContain('error_items')
    expect(err.message).not.toBe('null constraint')
    // The raw error is preserved with identifiers intact for server logs.
    expect(err[ERROR].message).toContain('error_items')
  })

  it('unique violation → Conflict, strips the constraint name', async () => {
    await app.service('error_items').create({ name: 'a', email: 'x@y.z' })

    const err = await rejection(
      app.service('error_items').create({ name: 'b', email: 'x@y.z' }),
    )

    expect(err.name).toBe('Conflict')
    expect(err.message).toContain('unique constraint')
    expect(err.message).not.toContain('"')
  })

  it('foreign key violation → BadRequest', async () => {
    const err = await rejection(
      app.service('error_children').create({ parent_id: 999999 }),
    )

    expect(err.name).toBe('BadRequest')
    expect(err.message).toContain('foreign key constraint')
    expect(err.message).not.toContain('"')
  })

  it('check violation → BadRequest', async () => {
    const err = await rejection(
      app.service('error_items').create({ name: 'a', age: -1 }),
    )

    expect(err.name).toBe('BadRequest')
    expect(err.message).toContain('check constraint')
    expect(err.message).not.toContain('"')
  })

  it('invalid input syntax (bad id) → NotFound', async () => {
    // A malformed id for an integer PK raises 22P02; Feathers adapters surface
    // that as NotFound (a malformed id reads as a missing resource).
    const err = await rejection(app.service('error_items').get('not-a-number'))

    expect(err.name).toBe('NotFound')
    expect(err.message).toContain('invalid input syntax')
    expect(err.message).not.toContain('"')
  })

  it('patch to a duplicate value → Conflict', async () => {
    const a = await app
      .service('error_items')
      .create({ name: 'a', email: 'a@x.com' })
    await app.service('error_items').create({ name: 'b', email: 'b@x.com' })

    const err = await rejection(
      app.service('error_items').patch(a.id, { email: 'b@x.com' }),
    )

    expect(err.name).toBe('Conflict')
    expect(err.message).toContain('unique constraint')
    expect(err.message).not.toContain('"')
  })

  it('multi-create hitting a constraint → Conflict', async () => {
    const err = await rejection(
      app.service('error_items').create([
        { name: 'a', email: 'dup@x.com' },
        { name: 'b', email: 'dup@x.com' },
      ]),
    )

    expect(err.name).toBe('Conflict')
    expect(err.message).toContain('unique constraint')
  })
})
