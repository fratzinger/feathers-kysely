import { sql } from 'kysely'
import { BadRequest } from '@feathersjs/errors'

/**
 * Build a `LIKE` prefix/suffix match for `$startsWith` / `$endsWith`. The LIKE
 * wildcards (`%`, `_`) and the escape char (`\`) in the user value are escaped
 * so they match literally; the resulting pattern is bound as a parameter and the
 * statement declares `ESCAPE '\'`. Case-sensitive (note: SQLite's LIKE is
 * ASCII-case-insensitive by default). `column` may be a plain name or an
 * already-built expression (e.g. a JSON-path accessor).
 */
export function buildLikePattern(
  column: any,
  operator: '$startsWith' | '$endsWith',
  value: any,
) {
  if (typeof value !== 'string') {
    throw new BadRequest(`The value for '${operator}' must be a string`)
  }

  // Escape backslash first (via the single char class), then % and _.
  const escaped = value.replace(/[\\%_]/g, '\\$&')
  const pattern = operator === '$startsWith' ? `${escaped}%` : `%${escaped}`

  const ref = typeof column === 'string' ? sql.ref(column) : column
  return sql<boolean>`${ref} like ${pattern} escape '\\'`
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

  describe('buildLikePattern', () => {
    it('throws when the value is not a string', () => {
      expect(() => buildLikePattern('col', '$startsWith', 5)).toThrow(
        "The value for '$startsWith' must be a string",
      )
    })

    it('builds a prefix LIKE with the pattern bound as a parameter', () => {
      const { sql: text, parameters } = compile(
        buildLikePattern('col', '$startsWith', 'foo'),
      )
      expect(text).toBe(`"col" like $1 escape '\\'`)
      expect(parameters).toEqual(['foo%'])
    })

    it('builds a suffix LIKE for $endsWith', () => {
      const { parameters } = compile(buildLikePattern('col', '$endsWith', 'bar'))
      expect(parameters).toEqual(['%bar'])
    })

    it('escapes LIKE wildcards and the escape char in the value', () => {
      const { parameters } = compile(
        buildLikePattern('col', '$startsWith', 'a%b_c\\d'),
      )
      // %, _ and \ become literal (backslash-prefixed); trailing % is the wildcard
      expect(parameters).toEqual(['a\\%b\\_c\\\\d%'])
    })
  })
}
