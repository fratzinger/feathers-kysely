import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'
import { KyselyService, updateOperators } from '../src/index.js'
import { describe, it, expect, beforeEach, afterAll } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

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
  app.service('users').hooks({
    before: {
      patch: [updateOperators()],
      update: [updateOperators()],
    },
  })
  return { app, db, clean }
}

const { app, db, clean } = setup()

describe('update operators (cross-dialect)', () => {
  beforeEach(clean)
  afterAll(() => db.destroy())

  it('$inc increments a column atomically', async () => {
    const created = await app.service('users').create({ name: 'a', age: 10 })

    const patched = await app
      .service('users')
      .patch(created.id, { $inc: { age: 5 } } as any)

    expect(patched.age).toBe(15)
  })

  it('a negative $inc decrements', async () => {
    const created = await app.service('users').create({ name: 'a', age: 10 })

    const patched = await app
      .service('users')
      .patch(created.id, { $inc: { age: -4 } } as any)

    expect(patched.age).toBe(6)
  })

  it('$mul multiplies a column', async () => {
    const created = await app.service('users').create({ name: 'a', score: 6 })

    const patched = await app
      .service('users')
      .patch(created.id, { $mul: { score: 3 } } as any)

    expect(patched.score).toBe(18)
  })

  it('$min keeps the smaller of the current and given value', async () => {
    const created = await app.service('users').create({ name: 'a', age: 10 })

    // a greater value leaves the column untouched
    let patched = await app
      .service('users')
      .patch(created.id, { $min: { age: 20 } } as any)
    expect(patched.age).toBe(10)

    // a smaller value replaces it
    patched = await app
      .service('users')
      .patch(created.id, { $min: { age: 4 } } as any)
    expect(patched.age).toBe(4)
  })

  it('$max keeps the larger of the current and given value', async () => {
    const created = await app.service('users').create({ name: 'a', age: 10 })

    let patched = await app
      .service('users')
      .patch(created.id, { $max: { age: 5 } } as any)
    expect(patched.age).toBe(10)

    patched = await app
      .service('users')
      .patch(created.id, { $max: { age: 25 } } as any)
    expect(patched.age).toBe(25)
  })

  it('$min / $max initialize a NULL column to the given value', async () => {
    const created = await app
      .service('users')
      .create({ name: 'a', score: null })

    const patched = await app
      .service('users')
      .patch(created.id, { $min: { score: 50 } } as any)
    expect(patched.score).toBe(50)
  })

  it('combines $inc and $mul (different columns) in one patch', async () => {
    const created = await app
      .service('users')
      .create({ name: 'a', age: 10, score: 5 })

    const patched = await app
      .service('users')
      .patch(created.id, { $inc: { age: 1 }, $mul: { score: 2 } } as any)

    expect(patched.age).toBe(11)
    expect(patched.score).toBe(10)
  })

  it('mixes a literal assignment with an operator', async () => {
    const created = await app.service('users').create({ name: 'a', age: 10 })

    const patched = await app
      .service('users')
      .patch(created.id, { name: 'b', $inc: { age: 1 } } as any)

    expect(patched.name).toBe('b')
    expect(patched.age).toBe(11)
  })

  it('applies the operator to every matched row in a multi-patch', async () => {
    await app.service('users').create([
      { name: 'a', age: 1 },
      { name: 'b', age: 2 },
      { name: 'c', age: 3 },
    ])

    const patched = await app
      .service('users')
      .patch(null, { $inc: { age: 10 } } as any, {
        query: { $sort: { age: 1 } },
      })

    expect((patched as User[]).map((u) => u.age)).toEqual([11, 12, 13])
  })

  it('rejects update operators on update (full replace) with BadRequest', async () => {
    const created = await app.service('users').create({ name: 'a', age: 10 })

    await expect(
      app.service('users').update(created.id, { $inc: { age: 1 } } as any),
    ).rejects.toMatchObject({ name: 'BadRequest' })
  })

  it('rejects a non-numeric operator value with BadRequest', async () => {
    const created = await app.service('users').create({ name: 'a', age: 10 })

    await expect(
      app.service('users').patch(created.id, { $inc: { age: 'x' } } as any),
    ).rejects.toMatchObject({ name: 'BadRequest' })
  })

  it('also works when registered as an around hook (optional next)', async () => {
    const aroundUsers = new KyselyService<User>({
      Model: db,
      name: 'users',
      multi: true,
      properties: { id: true, name: true, age: true, score: true },
    })
    const aroundApp = feathers<{ users: typeof aroundUsers }>().use(
      'users',
      aroundUsers,
    )
    aroundApp.service('users').hooks({
      around: {
        patch: [updateOperators()],
        update: [updateOperators()],
      },
    })

    const created = await aroundApp
      .service('users')
      .create({ name: 'a', age: 10 })

    const patched = await aroundApp
      .service('users')
      .patch(created.id, { $inc: { age: 7 } } as any)
    expect(patched.age).toBe(17)

    await expect(
      aroundApp
        .service('users')
        .update(created.id, { $inc: { age: 1 } } as any),
    ).rejects.toMatchObject({ name: 'BadRequest' })
  })
})
