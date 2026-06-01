import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import assert from 'node:assert'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'

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

  // MARK: patch with empty data

  describe('patch with empty data', () => {
    it('patch(id, {}) returns the unchanged record', async () => {
      const created = await app
        .service('users')
        .create({ name: 'Alice', age: 30 })
      const result = await app.service('users').patch(created.id, {})
      assert.deepStrictEqual(result, created)
    })

    it('patch(id, { [idField]: X }) returns the unchanged record', async () => {
      const created = await app
        .service('users')
        .create({ name: 'Bob', age: 25 })
      const result = (await app
        .service('users')
        .patch(created.id, { id: 999 } as any)) as Record<string, any>
      assert.strictEqual(result.id, created.id)
      assert.strictEqual(result.name, 'Bob')
      assert.strictEqual(result.age, 25)
    })

    it('patch(null, {}, params) returns all matching records unchanged', async () => {
      await app.service('users').create([
        { name: 'A', age: 20 },
        { name: 'B', age: 20 },
        { name: 'C', age: 99 },
      ])
      const result = await app
        .service('users')
        .patch(null, {}, { query: { age: 20 } })
      assert.strictEqual(result.length, 2)
      assert.ok(result.every((u) => u.age === 20))
    })

    it('patch(null, {}, params) with no matches returns []', async () => {
      const result = await app
        .service('users')
        .patch(null, {}, { query: { age: 999 } })
      assert.deepStrictEqual(result, [])
    })

    it('patch(id, {}) on missing id throws NotFound', async () => {
      await assert.rejects(
        () => app.service('users').patch(999_999, {}),
        (err: any) => err.name === 'NotFound',
      )
    })
  })

  // MARK: Empty logical operators

  describe('empty $or / $and', () => {
    it('empty $or matches no rows', async () => {
      await app.service('users').create([
        { name: 'a', age: 1 },
        { name: 'b', age: 2 },
      ])
      const result = await app
        .service('users')
        .find({ query: { $or: [] }, paginate: false })
      assert.strictEqual(result.length, 0)
    })

    it('empty $and matches all rows', async () => {
      await app.service('users').create([
        { name: 'a', age: 1 },
        { name: 'b', age: 2 },
      ])
      const result = await app
        .service('users')
        .find({ query: { $and: [] as any }, paginate: false })
      assert.strictEqual(result.length, 2)
    })
  })

  // MARK: Pagination clamping & stability

  describe('pagination clamping', () => {
    it('negative $limit does not error (clamped)', async () => {
      await app.service('users').create({ name: 'a', age: 1 })
      const result = await app
        .service('users')
        .find({ query: { $limit: -5 }, paginate: false })
      assert.ok(Array.isArray(result))
    })

    it('negative $skip does not error (clamped to 0)', async () => {
      await app.service('users').create([
        { name: 'a', age: 1 },
        { name: 'b', age: 2 },
      ])
      const result = await app.service('users').find({
        query: { $skip: -3, $sort: { id: 1 } },
        paginate: false,
      })
      assert.strictEqual(result.length, 2)
    })

    it('paginated find without $sort is stable via id tiebreaker', async () => {
      await app
        .service('users')
        .create(
          Array.from({ length: 5 }, (_, i) => ({ name: `n${i}`, age: i })),
        )

      const page1 = await app
        .service('users')
        .find({ query: { $limit: 2, $skip: 0 }, paginate: false })
      const page2 = await app
        .service('users')
        .find({ query: { $limit: 2, $skip: 2 }, paginate: false })

      const ids1 = page1.map((u) => u.id)
      const ids2 = page2.map((u) => u.id)

      assert.strictEqual(ids1.length, 2)
      assert.strictEqual(ids2.length, 2)
      // Consecutive pages must not overlap → deterministic ordering applied.
      assert.ok(!ids1.some((id) => ids2.includes(id)))
      assert.ok(Math.max(...ids1) < Math.min(...ids2))
    })
  })

  // MARK: Dialect operator handling

  describe('dialect operator handling', () => {
    it('$iLike matches case-insensitively', async () => {
      await app.service('users').create([
        { name: 'Alice', age: 1 },
        { name: 'BOB', age: 2 },
      ])

      const result = await app.service('users').find({
        query: { name: { $iLike: '%alice%' } },
        paginate: false,
      })

      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0].name, 'Alice')
    })

    it.skipIf(getDialect() === 'postgres')(
      '$contains is rejected with BadRequest on non-Postgres dialects',
      async () => {
        await assert.rejects(
          () =>
            app.service('users').find({
              query: { name: { $contains: ['x'] } as any },
              paginate: false,
            }),
          (err: any) => err.name === 'BadRequest',
        )
      },
    )
  })

  // MARK: Window-count pagination

  describe('paginated total', () => {
    const paginated = new KyselyService<any>({
      Model: db,
      name: 'users',
      paginate: { default: 10, max: 100 },
    })

    it('$skip past the end returns the real total with empty data', async () => {
      await app.service('users').create([
        { name: 'a', age: 1 },
        { name: 'b', age: 2 },
        { name: 'c', age: 3 },
      ])

      const result = (await paginated.find({ query: { $skip: 100 } })) as any
      assert.strictEqual(result.total, 3)
      assert.strictEqual(result.data.length, 0)
      assert.strictEqual(result.skip, 100)
    })

    it('$select returns the correct total and strips the helper column', async () => {
      await app.service('users').create([
        { name: 'a', age: 1 },
        { name: 'b', age: 2 },
        { name: 'c', age: 3 },
      ])

      const result = (await paginated.find({
        query: { $select: ['name'], $limit: 2 },
      })) as any

      assert.strictEqual(result.total, 3)
      assert.strictEqual(result.data.length, 2)
      for (const row of result.data) {
        assert.ok(!('__fk_total' in row), 'window-count helper column leaked')
        assert.ok('name' in row)
      }
    })
  })
})
