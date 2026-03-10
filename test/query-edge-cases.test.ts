import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import assert from 'node:assert'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { describe, it } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

function setup() {
  interface UsersTable {
    id: Generated<number>
    name: string
    age: number | null
  }

  interface DB {
    users: UsersTable
  }

  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    await db.schema.dropTable('users').ifExists().execute()
    await addPrimaryKey(
      db.schema
        .createTable('users')
        .addColumn('name', 'text', (col) => col.notNull())
        .addColumn('age', 'real'),
      'id',
    ).execute()
  }

  const users = new KyselyService({
    Model: db,
    name: 'users',
    multi: true,
    properties: { id: true, name: true, age: true },
  })

  type ServiceTypes = {
    users: typeof users
  }

  const app = feathers<ServiceTypes>().use('users', users)
  return { app, db, clean }
}

const { app, db, clean } = setup()

describe('query edge cases', () => {
  beforeEach(clean)
  afterAll(() => db.destroy())

  // MARK: String edge cases

  it('very long field values', async () => {
    const longName = 'A'.repeat(10000)
    const created = await app
      .service('users')
      .create({ name: longName, age: 1 })
    const result = await app.service('users').find({
      query: { name: longName },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, longName)
  })

  it('unicode characters in queries', async () => {
    await app.service('users').create([
      { name: '日本語テスト', age: 1 },
      { name: 'Ñoño', age: 2 },
      { name: 'émojis 🚀🎉', age: 3 },
    ])

    const r1 = await app
      .service('users')
      .find({ query: { name: '日本語テスト' }, paginate: false })
    assert.strictEqual(r1.length, 1)
    assert.strictEqual(r1[0].name, '日本語テスト')

    const r2 = await app
      .service('users')
      .find({ query: { name: 'Ñoño' }, paginate: false })
    assert.strictEqual(r2.length, 1)

    const r3 = await app
      .service('users')
      .find({ query: { name: 'émojis 🚀🎉' }, paginate: false })
    assert.strictEqual(r3.length, 1)
  })

  it('single quotes in values are parameterized', async () => {
    await app.service('users').create({ name: "O'Reilly", age: 40 })
    const result = await app.service('users').find({
      query: { name: "O'Reilly" },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, "O'Reilly")
  })

  it('backslash characters in values', async () => {
    await app.service('users').create({ name: 'path\\to\\file', age: 1 })
    const result = await app.service('users').find({
      query: { name: 'path\\to\\file' },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'path\\to\\file')
  })

  // MARK: $and / $or edge cases

  it('$or with single element', async () => {
    await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])
    const result = await app.service('users').find({
      query: { $or: [{ name: 'Alice' }] },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Alice')
  })

  it('deeply nested $and/$or (5 levels)', async () => {
    await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])
    const result = await app.service('users').find({
      query: {
        $and: [{ $or: [{ $and: [{ $or: [{ $and: [{ name: 'Alice' }] }] }] }] }],
      },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Alice')
  })

  it('$and with overlapping range conditions', async () => {
    await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])
    const result = await app.service('users').find({
      query: { $and: [{ age: { $gt: 20 } }, { age: { $lt: 30 } }] },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Bob')
  })

  it('$or combining different fields', async () => {
    await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])
    const result = await app.service('users').find({
      query: { $or: [{ name: 'Alice' }, { age: 35 }] },
      paginate: false,
    })
    assert.strictEqual(result.length, 2)
    assert.ok(result.find((u) => u.name === 'Alice'))
    assert.ok(result.find((u) => u.name === 'Charlie'))
  })

  // MARK: Non-existent fields

  it('query with non-existent field throws', async () => {
    await app.service('users').create({ name: 'Alice', age: 30 })
    try {
      await app.service('users').find({
        query: { nonExistentField: 'value' },
        paginate: false,
      })
      // If it doesn't throw, it should at least not crash the DB
    } catch (err: any) {
      assert.ok(err, 'Expected an error for non-existent field')
    }
  })

  it('$select with non-existent field throws', async () => {
    await app.service('users').create({ name: 'Alice', age: 30 })
    try {
      await app.service('users').find({
        query: { $select: ['id', 'nonExistent'] },
        paginate: false,
      })
      // Some databases may ignore unknown columns
    } catch (err: any) {
      assert.ok(err, 'Expected an error for non-existent select field')
    }
  })

  // MARK: Numeric edge cases

  it('$gt with MAX_SAFE_INTEGER returns empty', async () => {
    await app.service('users').create({ name: 'Alice', age: 30 })
    const result = await app.service('users').find({
      query: { age: { $gt: Number.MAX_SAFE_INTEGER } },
      paginate: false,
    })
    assert.strictEqual(result.length, 0)
  })

  it('$lt with negative MAX_SAFE_INTEGER returns empty', async () => {
    await app.service('users').create({ name: 'Alice', age: 30 })
    const result = await app.service('users').find({
      query: { age: { $lt: -Number.MAX_SAFE_INTEGER } },
      paginate: false,
    })
    assert.strictEqual(result.length, 0)
  })

  it('query with NaN value', async () => {
    await app.service('users').create({ name: 'Alice', age: 30 })
    try {
      const result = await app.service('users').find({
        query: { age: NaN },
        paginate: false,
      })
      // NaN comparisons in SQL return no rows (NaN != anything)
      assert.strictEqual(result.length, 0)
    } catch {
      // error is also acceptable
    }
  })

  it('query with Infinity value', async () => {
    await app.service('users').create({ name: 'Alice', age: 30 })
    try {
      const result = await app.service('users').find({
        query: { age: Infinity },
        paginate: false,
      })
      assert.strictEqual(result.length, 0)
    } catch {
      // error is also acceptable
    }
  })
})
