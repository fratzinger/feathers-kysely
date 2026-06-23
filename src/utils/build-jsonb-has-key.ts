import { sql } from 'kysely'
import { BadRequest } from '@feathersjs/errors'

/**
 * Build a Postgres jsonb key-existence check. Uses the *function* forms of the
 * `?` / `?|` / `?&` operators (parameter-safe, avoiding the `?`-placeholder
 * clash):
 *
 *   $hasKey    -> jsonb_exists(col, 'k')          (top-level key/element exists)
 *   $hasKeyAny -> jsonb_exists_any(col, ARRAY[…]) (any listed key exists)
 *   $hasKeyAll -> jsonb_exists_all(col, ARRAY[…]) (all listed keys exist)
 *
 * The column is cast `::jsonb` so it works on both `json` and `jsonb` columns.
 * `column` may be a plain name or an already-built expression.
 */
export function buildJsonbHasKey(
  column: any,
  operator: '$hasKey' | '$hasKeyAny' | '$hasKeyAll',
  value: any,
) {
  const ref = typeof column === 'string' ? sql.ref(column) : column

  if (operator === '$hasKey') {
    if (typeof value !== 'string') {
      throw new BadRequest(`The value for '$hasKey' must be a string key`)
    }
    return sql<boolean>`jsonb_exists(${ref}::jsonb, ${value})`
  }

  if (!Array.isArray(value) || value.some((k) => typeof k !== 'string')) {
    throw new BadRequest(
      `The value for '${operator}' must be an array of string keys`,
    )
  }

  const keys = sql`ARRAY[${sql.join(value)}]::text[]`
  return operator === '$hasKeyAny'
    ? sql<boolean>`jsonb_exists_any(${ref}::jsonb, ${keys})`
    : sql<boolean>`jsonb_exists_all(${ref}::jsonb, ${keys})`
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    sql: sqlTag,
  } = await import('kysely')

  const pg = new Kysely<any>({
    dialect: {
      createAdapter: () => new PostgresAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new PostgresIntrospector(db),
      createQueryCompiler: () => new PostgresQueryCompiler(),
    },
  })
  const compile = (v: any) => sqlTag`${v}`.compile(pg)

  describe('buildJsonbHasKey', () => {
    it('builds jsonb_exists for a single key', () => {
      const { sql: text, parameters } = compile(
        buildJsonbHasKey('col', '$hasKey', 'k'),
      )
      expect(text).toBe('jsonb_exists("col"::jsonb, $1)')
      expect(parameters).toEqual(['k'])
    })

    it('builds jsonb_exists_any for $hasKeyAny', () => {
      const { sql: text, parameters } = compile(
        buildJsonbHasKey('col', '$hasKeyAny', ['a', 'b']),
      )
      expect(text).toBe('jsonb_exists_any("col"::jsonb, ARRAY[$1, $2]::text[])')
      expect(parameters).toEqual(['a', 'b'])
    })

    it('builds jsonb_exists_all for $hasKeyAll', () => {
      const { sql: text } = compile(buildJsonbHasKey('col', '$hasKeyAll', ['a']))
      expect(text).toBe('jsonb_exists_all("col"::jsonb, ARRAY[$1]::text[])')
    })

    it('validates the value type per operator', () => {
      expect(() => buildJsonbHasKey('col', '$hasKey', ['a'])).toThrow(
        "The value for '$hasKey' must be a string key",
      )
      expect(() => buildJsonbHasKey('col', '$hasKeyAny', 'a')).toThrow(
        "must be an array of string keys",
      )
      expect(() => buildJsonbHasKey('col', '$hasKeyAll', [1, 2])).toThrow(
        "must be an array of string keys",
      )
    })
  })
}
