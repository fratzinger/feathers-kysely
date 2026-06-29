import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'
import { KyselyService } from '../src/index.js'
import { describe, it, expect, beforeEach, afterAll } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

interface UsersTable {
  id: Generated<number>
  name: string
  age: number | null
}
interface DB {
  users: UsersTable
}
type User = { id: number; name: string; age: number | null }

function setup() {
  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    await db.schema.dropTable('users').ifExists().execute()
    await addPrimaryKey(
      db.schema
        .createTable('users')
        .addColumn('name', 'text', (col) => col.notNull())
        .addColumn('age', 'integer'),
      'id',
    ).execute()
  }

  const users = new KyselyService<User>({
    Model: db,
    name: 'users',
    multi: true,
    properties: { id: true, name: true, age: true },
  })

  const app = feathers<{ users: typeof users }>().use('users', users)
  return { app, db, clean }
}

const { app, db, clean } = setup()
const dialectName = getDialect()

describe('query operators (cross-dialect)', () => {
  beforeEach(clean)
  afterAll(() => db.destroy())

  it('$between / $notBetween', async () => {
    await app.service('users').create([
      { name: 'a', age: 10 },
      { name: 'b', age: 20 },
      { name: 'c', age: 30 },
    ])

    const between = await app.service('users').find({
      query: { age: { $between: [15, 25] } },
      paginate: false,
    })
    expect(between.map((u) => u.name)).toEqual(['b'])

    const notBetween = await app.service('users').find({
      query: { age: { $notBetween: [15, 25] }, $sort: { age: 1 } },
      paginate: false,
    })
    expect(notBetween.map((u) => u.name)).toEqual(['a', 'c'])

    // Combined with another predicate: proves the unparenthesized NOT BETWEEN
    // fragment AND-composes with correct precedence.
    const combined = await app.service('users').find({
      query: { age: { $notBetween: [15, 25] }, name: 'a' },
      paginate: false,
    })
    expect(combined.map((u) => u.name)).toEqual(['a'])
  })

  describe('$not', () => {
    it('negates a whole condition (single, nested, multi-key)', async () => {
      await app.service('users').create([
        { name: 'a', age: 10 },
        { name: 'b', age: 20 },
        { name: 'c', age: 20 },
      ])

      // single key: NOT (age = 20)
      const single = await app.service('users').find({
        query: { $not: { age: 20 }, $sort: { name: 1 } },
        paginate: false,
      })
      expect(single.map((u) => u.name)).toEqual(['a'])

      // nested inside $and
      const nested = await app.service('users').find({
        query: { $and: [{ $not: { age: 20 } }], $sort: { name: 1 } },
        paginate: false,
      })
      expect(nested.map((u) => u.name)).toEqual(['a'])

      // multi-key: NOT (age = 20 AND name = 'b') keeps 'a' (age!=20) and 'c'
      // (age=20 but name!='b'). A per-property inversion (age!=20 AND name!='b')
      // would wrongly drop 'c'.
      const multi = await app.service('users').find({
        query: { $not: { age: 20, name: 'b' }, $sort: { name: 1 } },
        paginate: false,
      })
      expect(multi.map((u) => u.name)).toEqual(['a', 'c'])
    })

    it('is operator-agnostic (negates $gt, not just equality)', async () => {
      await app.service('users').create([
        { name: 'a', age: 10 },
        { name: 'b', age: 20 },
        { name: 'c', age: 30 },
      ])

      // NOT (age > 15) keeps only 'a'
      const result = await app.service('users').find({
        query: { $not: { age: { $gt: 15 } }, $sort: { name: 1 } },
        paginate: false,
      })
      expect(result.map((u) => u.name)).toEqual(['a'])
    })

    it('around $or follows De Morgan', async () => {
      await app.service('users').create([
        { name: 'a', age: 10 },
        { name: 'b', age: 20 },
        { name: 'c', age: 30 },
      ])

      // NOT (age = 10 OR age = 20) === age != 10 AND age != 20 -> keeps 'c'
      const result = await app.service('users').find({
        query: {
          $not: { $or: [{ age: 10 }, { age: 20 }] },
          $sort: { name: 1 },
        },
        paginate: false,
      })
      expect(result.map((u) => u.name)).toEqual(['c'])
    })

    it('with an empty condition is a no-op', async () => {
      await app.service('users').create([
        { name: 'a', age: 10 },
        { name: 'b', age: 20 },
      ])

      const result = await app.service('users').find({
        query: { $not: {}, $sort: { name: 1 } },
        paginate: false,
      })
      expect(result.map((u) => u.name)).toEqual(['a', 'b'])
    })
  })

  it('$startsWith / $endsWith', async () => {
    await app.service('users').create([
      { name: 'alpha', age: 1 },
      { name: 'beta', age: 2 },
      { name: 'alabama', age: 3 },
    ])

    const sw = await app.service('users').find({
      query: { name: { $startsWith: 'al' }, $sort: { name: 1 } },
      paginate: false,
    })
    expect(sw.map((u) => u.name)).toEqual(['alabama', 'alpha'])

    const ew = await app.service('users').find({
      query: { name: { $endsWith: 'a' }, $sort: { name: 1 } },
      paginate: false,
    })
    expect(ew.map((u) => u.name)).toEqual(['alabama', 'alpha', 'beta'])
  })

  it('$startsWith treats LIKE wildcards in the value literally', async () => {
    await app.service('users').create([
      { name: '10%off', age: 1 },
      { name: '10ish', age: 2 },
    ])

    // '%' must match literally, so '10%' matches only '10%off', not '10ish'.
    const res = await app.service('users').find({
      query: { name: { $startsWith: '10%' } },
      paginate: false,
    })
    expect(res.map((u) => u.name)).toEqual(['10%off'])
  })

  it('$notILike (case-insensitive negated match)', async () => {
    await app.service('users').create([
      { name: 'Hello', age: 1 },
      { name: 'World', age: 2 },
    ])

    const res = await app.service('users').find({
      query: { name: { $notILike: '%hello%' } },
      paginate: false,
    })
    expect(res.map((u) => u.name)).toEqual(['World'])
  })

  // Postgres (~/!~) and MySQL (REGEXP) support regex; SQLite has no built-in
  // REGEXP and is not registered there.
  it.skipIf(dialectName === 'sqlite')('$regex / $notRegex', async () => {
    await app.service('users').create([
      { name: 'foo123', age: 1 },
      { name: 'bar', age: 2 },
    ])

    const match = await app.service('users').find({
      query: { name: { $regex: '[0-9]+' } },
      paginate: false,
    })
    expect(match.map((u) => u.name)).toEqual(['foo123'])

    const notMatch = await app.service('users').find({
      query: { name: { $notRegex: '[0-9]+' } },
      paginate: false,
    })
    expect(notMatch.map((u) => u.name)).toEqual(['bar'])
  })

  it.skipIf(dialectName !== 'sqlite')(
    '$regex is rejected on SQLite with a BadRequest',
    async () => {
      await expect(
        app.service('users').find({
          query: { name: { $regex: 'x' } },
          paginate: false,
        }),
      ).rejects.toMatchObject({ name: 'BadRequest' })
    },
  )
})
