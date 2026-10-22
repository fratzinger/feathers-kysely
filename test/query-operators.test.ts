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
  nick: string | null
}
interface DB {
  users: UsersTable
}
type User = {
  id: number
  name: string
  age: number | null
  nick?: string | null
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
        .addColumn('nick', 'text'),
      'id',
    ).execute()
  }

  const users = new KyselyService<User>({
    Model: db,
    name: 'users',
    multi: true,
    properties: { id: true, name: true, age: true, nick: true },
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

    it('partitions the rows with its condition, NULL rows included', async () => {
      await app.service('users').create([
        { name: 'a', age: 10, nick: 'al' },
        { name: 'b', age: 20, nick: null },
        { name: 'c', age: null, nick: 'cy' },
      ])

      const names = async (query: Record<string, any>) =>
        (
          await app.service('users').find({
            query: { ...query, $sort: { name: 1 } },
            paginate: false,
          })
        ).map((u) => u.name)

      // Every row matches either the condition or its $not, never both or
      // neither. A bare NOT (age > 15) is UNKNOWN for 'c' and would drop it
      // from both sides.
      const conditions: Record<string, any>[] = [
        { age: 20 },
        { age: { $gt: 15 } },
        { age: { $ne: 20 } },
        { age: { $in: [null, 10] } },
        { age: { $nin: [10] } },
        { age: { $notBetween: [15, 25] } },
        { nick: { $like: 'a%' } },
        { nick: { $notLike: 'a%' } },
        { age: { $gt: 15 }, nick: 'cy' },
        { $or: [{ age: 10 }, { nick: 'cy' }] },
      ]

      for (const condition of conditions) {
        const matching = await names(condition)
        const rest = await names({ $not: condition })
        expect(
          [...matching, ...rest].sort(),
          JSON.stringify(condition),
        ).toEqual(['a', 'b', 'c'])
      }
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

  it('negated operators match NULL rows', async () => {
    await app.service('users').create([
      { name: 'a', age: 10, nick: 'al' },
      { name: 'b', age: null, nick: null },
    ])

    // A NULL is not 10, not like 'a%', not between 5 and 15 — although SQL
    // evaluates each of those comparisons to UNKNOWN for it.
    const queries: Record<string, any>[] = [
      { age: { $ne: 10 } },
      { age: { $nin: [10] } },
      { age: { $notBetween: [5, 15] } },
      { nick: { $notLike: 'a%' } },
      { nick: { $notILike: 'A%' } },
    ]
    // No REGEXP on SQLite (see below).
    if (dialectName !== 'sqlite') queries.push({ nick: { $notRegex: '^a' } })

    for (const query of queries) {
      const res = await app.service('users').find({ query, paginate: false })
      expect(
        res.map((u) => u.name),
        JSON.stringify(query),
      ).toEqual(['b'])
    }
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

  // MARK: null inside $in / $nin
  //
  // Feathers queries are Mongo-shaped, so a `null` inside the array is a value
  // you can match rather than SQL's "never equal to anything" — `buildIn` lifts
  // it out of the list into an explicit IS (NOT) NULL.
  //
  // The base matrix ($in/$nin with [null] and [null, value], and the empty-array
  // identities) belongs to the shared adapter suite and runs from there over
  // every service variant. What is left here is what the shared suite does not
  // own: the boundaries and the interaction with our own query builder.
  describe('$in / $nin with a null in the array', () => {
    beforeEach(async () => {
      await app.service('users').create([
        { name: 'one', age: 1 },
        { name: 'two', age: 2 },
        { name: 'none', age: null },
      ])
    })

    it('repeated nulls behave like a single one', async () => {
      const res = await app.service('users').find({
        query: { age: { $in: [null, null, 1] }, $sort: { name: 1 } },
        paginate: false,
      })
      expect(res.map((u) => u.name)).toEqual(['none', 'one'])
    })

    it('an array without a null', async () => {
      const inRes = await app.service('users').find({
        query: { age: { $in: [1, 2] }, $sort: { name: 1 } },
        paginate: false,
      })
      expect(inRes.map((u) => u.name)).toEqual(['one', 'two'])

      // A NULL is not 1, so it matches a plain $nin, as in Mongo — although
      // `age NOT IN (1)` alone is UNKNOWN for it.
      const ninRes = await app.service('users').find({
        query: { age: { $nin: [1] }, $sort: { name: 1 } },
        paginate: false,
      })
      expect(ninRes.map((u) => u.name)).toEqual(['none', 'two'])
    })

    it('$not over $nin / $ne leaves only the matching value', async () => {
      // NOT (age NOT IN (1) OR age IS NULL) — the NULL row stays out.
      for (const age of [{ $nin: [1] }, { $ne: 1 }]) {
        const res = await app.service('users').find({
          query: { $not: { age } },
          paginate: false,
        })
        expect(res.map((u) => u.name)).toEqual(['one'])
      }
    })

    it('composes with $not (De Morgan over the null branch)', async () => {
      // NOT (age = 1 OR age IS NULL) leaves only the 2.
      const res = await app.service('users').find({
        query: { $not: { age: { $in: [null, 1] } } },
        paginate: false,
      })
      expect(res.map((u) => u.name)).toEqual(['two'])
    })
  })
})
