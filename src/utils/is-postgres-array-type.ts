// Matches a Postgres array type name as it would appear in an `x-db-type`
// annotation: a base type (optionally multi-word like `double precision` or
// length/precision-qualified like `varchar(255)` / `numeric(10,2)` / `char(4)`)
// followed by `[]`. Used to drive the cast of an array literal so it matches the
// column's exact element type — Postgres array operators (@>, <@, &&) require
// both operands to be the *same* array type (e.g. `varchar[] @> text[]`,
// `bigint[] @> integer[]` and `float8[] @> integer[]` all fail). The pattern
// also bounds what we emit via `sql.raw`, so an annotation can't inject SQL.
export const POSTGRES_ARRAY_TYPE =
  /^[a-z][a-z0-9 ]*(\(\s*\d+\s*(,\s*\d+\s*)?\))?\s*\[\]$/i

export function isPostgresArrayType(type: string | undefined): type is string {
  return typeof type === 'string' && POSTGRES_ARRAY_TYPE.test(type.trim())
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('isPostgresArrayType', () => {
    it('accepts simple array types', () => {
      expect(isPostgresArrayType('text[]')).toBe(true)
      expect(isPostgresArrayType('integer[]')).toBe(true)
      expect(isPostgresArrayType('bigint[]')).toBe(true)
      expect(isPostgresArrayType('float8[]')).toBe(true)
      expect(isPostgresArrayType('date[]')).toBe(true)
      expect(isPostgresArrayType('timestamptz[]')).toBe(true)
    })

    it('accepts multi-word base types', () => {
      expect(isPostgresArrayType('double precision[]')).toBe(true)
    })

    it('accepts length/precision-qualified types', () => {
      expect(isPostgresArrayType('varchar(255)[]')).toBe(true)
      expect(isPostgresArrayType('char(4)[]')).toBe(true)
      expect(isPostgresArrayType('numeric(10,2)[]')).toBe(true)
      expect(isPostgresArrayType('numeric(10, 2)[]')).toBe(true)
    })

    it('is case-insensitive and trims surrounding whitespace', () => {
      expect(isPostgresArrayType('TEXT[]')).toBe(true)
      expect(isPostgresArrayType('  varchar[]  ')).toBe(true)
    })

    it('rejects non-array and scalar types', () => {
      expect(isPostgresArrayType('text')).toBe(false)
      expect(isPostgresArrayType('jsonb')).toBe(false)
      expect(isPostgresArrayType('integer')).toBe(false)
    })

    it('rejects undefined and malformed input', () => {
      expect(isPostgresArrayType(undefined)).toBe(false)
      expect(isPostgresArrayType('')).toBe(false)
      expect(isPostgresArrayType('[]')).toBe(false)
      expect(isPostgresArrayType('text[')).toBe(false)
      expect(isPostgresArrayType('integer[][]')).toBe(false)
    })

    it('rejects injection-shaped annotations', () => {
      expect(isPostgresArrayType('text[]; drop table users')).toBe(false)
      expect(isPostgresArrayType("text[]'")).toBe(false)
    })
  })
}
