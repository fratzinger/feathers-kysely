import type { ExpressionBuilder, Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'
import { KyselyService } from '../src/index.js'
import { describe, it, expect, beforeEach, afterAll } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

type Eb = ExpressionBuilder<any, any>

interface UsersTable {
  id: Generated<number>
  name: string
  age: number | null
  score: number | null
}
interface DB {
  users: UsersTable
}
type User = {
  id: number
  name: string
  age: number | null
  score: number | null
}

function setup() {
  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    await db.schema.dropTable('users').ifExists().execute()
    await addPrimaryKey(
      db.schema
        .createTable('users')
        .addColumn('name', 'text', (col) => col.notNull())
        .addColumn('age', 'integer')
        .addColumn('score', 'integer'),
      'id',
    ).execute()
  }

  const users = new KyselyService<User>({
    Model: db,
    name: 'users',
    multi: true,
    properties: { id: true, name: true, age: true, score: true },
  })

  const app = feathers<{ users: typeof users }>().use('users', users)
  return { app, db, clean }
}

const { app, db, clean } = setup()

// A column value can be a Kysely expression-builder factory `(eb) => Expression`
// passed straight through `.values()` (create) and `.set()` (patch/update). This
// is the server-side escape hatch for DB-computed values (e.g. column-to-column
// copies, SQL functions) — no hook or operator required. The function form is
// what reliably survives the adapter's SQLite `convertValues` pass.
describe('expression / factory column values (cross-dialect)', () => {
  beforeEach(clean)
  afterAll(() => db.destroy())

  it('create accepts a factory value computed in the database', async () => {
    const created = await app.service('users').create({
      name: 'a',
      age: ((eb: Eb) => eb(eb.lit(5), '+', eb.lit(5))) as any,
    })

    // resolved by the DB (insert ... values (..., (5 + 5))) and returned
    expect(created.age).toBe(10)
  })

  it('patch accepts (eb) => eb.ref(...) (column-to-column copy)', async () => {
    const created = await app
      .service('users')
      .create({ name: 'a', age: 7, score: 1 })

    const patched = await app
      .service('users')
      .patch(created.id, { score: ((eb: Eb) => eb.ref('age')) as any })

    // set "score" = "age"
    expect(patched.score).toBe(7)
  })

  it('update accepts a factory value (survives the full-replace path)', async () => {
    const created = await app
      .service('users')
      .create({ name: 'a', age: 9, score: 1 })

    const updated = await app.service('users').update(created.id, {
      name: 'a',
      age: 9,
      score: ((eb: Eb) => eb.ref('age')) as any,
    })

    expect(updated.score).toBe(9)
    expect(updated.age).toBe(9)
  })

  it('applies a factory value to every matched row in a multi-patch', async () => {
    await app.service('users').create([
      { name: 'a', age: 1, score: 0 },
      { name: 'b', age: 2, score: 0 },
      { name: 'c', age: 3, score: 0 },
    ])

    const patched = await app.service('users').patch(
      null,
      { score: ((eb: Eb) => eb.ref('age')) as any },
      {
        query: { $sort: { age: 1 } },
      },
    )

    expect((patched as User[]).map((u) => u.score)).toEqual([1, 2, 3])
  })
})
