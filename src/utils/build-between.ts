import { sql } from 'kysely'
import { BadRequest } from '@feathersjs/errors'

/**
 * Build a `BETWEEN` / `NOT BETWEEN` range expression. The value must be a
 * `[min, max]` tuple; both bounds are bound as parameters. `column` may be a
 * plain column name (rendered as a reference) or an already-built expression
 * (e.g. a JSON-path accessor from `traverseJSON`). Standard SQL — all dialects.
 */
export function buildBetween(
  column: any,
  operator: '$between' | '$notBetween',
  value: any,
) {
  if (!Array.isArray(value) || value.length !== 2) {
    throw new BadRequest(
      `The value for '${operator}' must be a [min, max] tuple`,
    )
  }

  const ref = typeof column === 'string' ? sql.ref(column) : column
  const [min, max] = value

  return operator === '$notBetween'
    ? sql<boolean>`${ref} not between ${min} and ${max}`
    : sql<boolean>`${ref} between ${min} and ${max}`
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
    sql: sqlTag,
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

  describe('buildBetween', () => {
    it('throws unless the value is a [min, max] tuple', () => {
      expect(() => buildBetween('col', '$between', 5)).toThrow(
        "The value for '$between' must be a [min, max] tuple",
      )
      expect(() => buildBetween('col', '$between', [1])).toThrow()
      expect(() => buildBetween('col', '$between', [1, 2, 3])).toThrow()
    })

    it('builds BETWEEN with both bounds bound as parameters (Postgres)', () => {
      const { sql: text, parameters } = sqlTag`${buildBetween('col', '$between', [1, 10])}`.compile(pg)
      expect(text).toBe('"col" between $1 and $2')
      expect(parameters).toEqual([1, 10])
    })

    it('builds NOT BETWEEN', () => {
      const { sql: text } = sqlTag`${buildBetween('col', '$notBetween', [1, 10])}`.compile(pg)
      expect(text).toBe('"col" not between $1 and $2')
    })

    it('works on SQLite (standard SQL, ? placeholders)', () => {
      const { sql: text, parameters } = sqlTag`${buildBetween('col', '$between', ['a', 'z'])}`.compile(sqlite)
      expect(text).toBe('"col" between ? and ?')
      expect(parameters).toEqual(['a', 'z'])
    })
  })
}
