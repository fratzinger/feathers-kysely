import { sql } from 'kysely'
import type { ExpressionBuilder } from 'kysely'

/**
 * Build an `$in` / `$nin` expression from an array value, treating a `null`
 * element as a value you can actually match.
 *
 * Plain SQL `IN` / `NOT IN` compare with `=` / `<>`, so a `null` in the list is
 * never equal to anything — `age IN (null, 1)` misses every NULL row, and
 * `age NOT IN (null, 2)` is UNKNOWN for *every* row and therefore matches
 * nothing at all. Feathers queries are Mongo-shaped, where `null` in `$in` is a
 * real candidate, so the null is pulled out of the list and turned into an
 * explicit `IS NULL` / `IS NOT NULL`:
 *
 * - `$in: [null, 1]`  -> `(age in (1) or age is null)`
 * - `$in: [null]`     -> `age is null`
 * - `$nin: [null, 2]` -> `(age not in (2) and age is not null)`
 * - `$nin: [null]`    -> `age is not null`
 *
 * Empty arrays keep their boolean identity: `$in: []` matches nothing (`1 = 0`),
 * `$nin: []` matches everything (`1 = 1`). An array without a `null` compiles to
 * an untouched `IN` / `NOT IN`.
 *
 * `column` may be a plain column reference or an already-built expression (e.g.
 * a JSON-path accessor from `traverseJSON`). Standard SQL — all dialects.
 */
export function buildIn(
  eb: ExpressionBuilder<any, any>,
  column: any,
  operator: '$in' | '$nin',
  value: any[],
) {
  const isIn = operator === '$in'
  const hasNull = value.some((entry) => entry === null)
  const values = hasNull ? value.filter((entry) => entry !== null) : value

  // An empty operand has no null to fall back on: `$in: []` matches nothing,
  // `$nin: []` matches everything. Explicit, so an authorization hook that
  // injects an empty list can never widen a query.
  if (!values.length && !hasNull) {
    return isIn ? sql<boolean>`1 = 0` : sql<boolean>`1 = 1`
  }

  if (!hasNull) {
    return eb(column, isIn ? 'in' : 'not in', values)
  }

  const nullCheck = eb(column, isIn ? 'is' : 'is not', null)

  // `[null]` on its own is just a null check.
  if (!values.length) {
    return nullCheck
  }

  return isIn
    ? eb.or([eb(column, 'in', values), nullCheck])
    : eb.and([eb(column, 'not in', values), nullCheck])
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    SqliteAdapter,
    SqliteIntrospector,
    SqliteQueryCompiler,
    expressionBuilder,
  } = await import('kysely')

  const mk = (Adapter: any, Introspector: any, Compiler: any) =>
    new Kysely<any>({
      dialect: {
        createAdapter: () => new Adapter(),
        createDriver: () => new DummyDriver(),
        createIntrospector: (db) => new Introspector(db),
        createQueryCompiler: () => new Compiler(),
      },
    })
  const pg = mk(PostgresAdapter, PostgresIntrospector, PostgresQueryCompiler)
  const sqlite = mk(SqliteAdapter, SqliteIntrospector, SqliteQueryCompiler)

  const eb = expressionBuilder<any, any>()
  const compile = (
    db: any,
    operator: '$in' | '$nin',
    value: any[],
    column = 'age',
  ) => {
    const { sql: text, parameters } = db
      .selectFrom('users')
      .select('id')
      .where(buildIn(eb, column, operator, value))
      .compile()
    // strip the constant prefix so the assertions read as just the predicate
    return {
      sql: text.replace('select "id" from "users" where ', ''),
      parameters,
    }
  }

  describe('buildIn', () => {
    it('compiles an array without a null to a plain IN / NOT IN', () => {
      expect(compile(pg, '$in', [1, 2])).toEqual({
        sql: '"age" in ($1, $2)',
        parameters: [1, 2],
      })
      expect(compile(pg, '$nin', [1, 2])).toEqual({
        sql: '"age" not in ($1, $2)',
        parameters: [1, 2],
      })
    })

    it('ORs an IS NULL next to $in when the array contains a null', () => {
      expect(compile(pg, '$in', [null, 1])).toEqual({
        sql: '("age" in ($1) or "age" is null)',
        parameters: [1],
      })
    })

    it('ANDs an IS NOT NULL next to $nin when the array contains a null', () => {
      expect(compile(pg, '$nin', [null, 2])).toEqual({
        sql: '("age" not in ($1) and "age" is not null)',
        parameters: [2],
      })
    })

    it('reduces a null-only array to a bare null check', () => {
      expect(compile(pg, '$in', [null])).toEqual({
        sql: '"age" is null',
        parameters: [],
      })
      expect(compile(pg, '$nin', [null])).toEqual({
        sql: '"age" is not null',
        parameters: [],
      })
    })

    it('collapses repeated nulls instead of emitting them as parameters', () => {
      expect(compile(pg, '$in', [null, null, 1])).toEqual({
        sql: '("age" in ($1) or "age" is null)',
        parameters: [1],
      })
    })

    it('keeps the boolean identity of an empty array', () => {
      expect(compile(pg, '$in', []).sql).toBe('1 = 0')
      expect(compile(pg, '$nin', []).sql).toBe('1 = 1')
    })

    it('emits the same standard SQL on SQLite', () => {
      expect(compile(sqlite, '$in', [null, 1])).toEqual({
        sql: '("age" in (?) or "age" is null)',
        parameters: [1],
      })
      expect(compile(sqlite, '$nin', [null, 2])).toEqual({
        sql: '("age" not in (?) and "age" is not null)',
        parameters: [2],
      })
    })
  })
}
