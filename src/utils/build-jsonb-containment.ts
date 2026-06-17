import { sql } from 'kysely'
import type { ExpressionBuilder } from 'kysely'
import { BadRequest } from '@feathersjs/errors'

/**
 * Build the Postgres containment/overlap expression for a `jsonb`/`json`
 * column. Unlike the native-array codegen in `transformOperatorValue`, the
 * operands here are themselves `jsonb` so the comparison is `jsonb`-vs-`jsonb`:
 *
 *   $contains  ->  column @> '[...]'::jsonb   (column contains all listed elements)
 *   $contained ->  column <@ '[...]'::jsonb   (column is contained by the list)
 *   $overlap   ->  (column @> '[a]'::jsonb OR column @> '[b]'::jsonb ...)
 *
 * `$overlap` has no native `jsonb &&` operator, so we express "any listed
 * element present" as an OR of single-element containment checks. This works
 * for both string and numeric jsonb arrays (avoiding the string-only `?|`
 * key-existence operator). The JSON payload is always bound as a parameter.
 */
export function buildJsonbContainment(
  eb: ExpressionBuilder<any, any>,
  column: any,
  operator: '$contains' | '$contained' | '$overlap',
  value: any,
) {
  if (!Array.isArray(value)) {
    throw new BadRequest(`The value for '${operator}' must be an array`)
  }

  const ref = sql.ref(column)

  if (operator === '$contains') {
    return sql<boolean>`${ref} @> ${JSON.stringify(value)}::jsonb`
  }

  if (operator === '$contained') {
    return sql<boolean>`${ref} <@ ${JSON.stringify(value)}::jsonb`
  }

  // $overlap: any listed element present. An empty list overlaps nothing.
  if (value.length === 0) {
    return sql<boolean>`1 = 0`
  }

  return eb.or(
    value.map(
      (element) => sql<boolean>`${ref} @> ${JSON.stringify([element])}::jsonb`,
    ),
  )
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    expressionBuilder,
  } = await import('kysely')

  const pg = new Kysely<any>({
    dialect: {
      createAdapter: () => new PostgresAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new PostgresIntrospector(db),
      createQueryCompiler: () => new PostgresQueryCompiler(),
    },
  })
  const eb = expressionBuilder<any, any>()
  // Wrap in a sql template so both RawBuilder (sql`…`) and Expression (eb.or(…))
  // results compile through a uniform .compile() path.
  const compile = (v: any) => sql`${v}`.compile(pg)

  describe('buildJsonbContainment', () => {
    it('throws when the value is not an array', () => {
      expect(() => buildJsonbContainment(eb, 'col', '$contains', 5)).toThrow(
        "The value for '$contains' must be an array",
      )
    })

    it('builds a jsonb @> containment with the payload bound as a parameter', () => {
      const { sql: text, parameters } = compile(
        buildJsonbContainment(eb, 'col', '$contains', ['a', 'b']),
      )
      expect(text).toBe('"col" @> $1::jsonb')
      expect(parameters).toEqual(['["a","b"]'])
    })

    it('builds a jsonb <@ contained check', () => {
      const { sql: text, parameters } = compile(
        buildJsonbContainment(eb, 'col', '$contained', ['a']),
      )
      expect(text).toBe('"col" <@ $1::jsonb')
      expect(parameters).toEqual(['["a"]'])
    })

    it('expands $overlap into an OR of single-element containment checks', () => {
      const { sql: text, parameters } = compile(
        buildJsonbContainment(eb, 'col', '$overlap', ['a', 'b']),
      )
      expect(text).toBe('("col" @> $1::jsonb or "col" @> $2::jsonb)')
      expect(parameters).toEqual(['["a"]', '["b"]'])
    })

    it('treats an empty $overlap as matching nothing', () => {
      const { sql: text } = compile(
        buildJsonbContainment(eb, 'col', '$overlap', []),
      )
      expect(text).toBe('1 = 0')
    })

    it('works for numeric jsonb arrays too', () => {
      const { sql: text, parameters } = compile(
        buildJsonbContainment(eb, 'col', '$overlap', [1, 2]),
      )
      expect(text).toBe('("col" @> $1::jsonb or "col" @> $2::jsonb)')
      expect(parameters).toEqual(['[1]', '[2]'])
    })
  })
}
