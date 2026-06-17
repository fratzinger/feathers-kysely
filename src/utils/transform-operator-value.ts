import { sql } from 'kysely'
import { BadRequest } from '@feathersjs/errors'
import { isPostgresArrayType } from './is-postgres-array-type.js'

/**
 * Build the right-hand operand for the Postgres array operators (`@>`, `<@`,
 * `&&`). For every other operator the value is returned untouched. The array is
 * rendered as a typed literal with each element bound as a parameter; the cast
 * is driven by the column's declared `x-db-type` array type when available, and
 * otherwise inferred from the first element (only reliable for text[]/integer[]).
 */
export function transformOperatorValue(
  op: string,
  value: any,
  propertyType?: string,
) {
  if (op !== '$contains' && op !== '$contained' && op !== '$overlap') {
    return value
  }

  if (!value) {
    return value
  }

  if (!Array.isArray(value)) {
    throw new BadRequest(`The value for '${op}' must be an array`)
  }

  // These are the Postgres array operators (@>, <@, &&). Build a properly
  // typed array literal with every element bound as a parameter.

  // Prefer the column's declared array type (via `x-db-type`): the operators
  // require both operands to be the exact same array type, so a hard-coded
  // ::text[]/::integer[] is wrong for varchar[], bigint[], float[], numeric[],
  // "char(4)[]", etc. Casting to the declared type covers all of them.
  if (isPostgresArrayType(propertyType)) {
    return sql`ARRAY[${sql.join(value)}]::${sql.raw(propertyType.trim())}`
  }

  // Fallback when the column type is unknown: infer from the first element.
  // Only genuine text[]/integer[] columns are reliably supported this way;
  // other element types (varchar[], bigint[], float[], numeric[], char(n)[])
  // must declare their type via an `x-db-type` annotation.
  const firstElement = value[0]
  if (typeof firstElement === 'number') {
    return sql`ARRAY[${sql.join(value)}]::integer[]`
  } else if (typeof firstElement === 'string') {
    return sql`ARRAY[${sql.join(value)}]::text[]`
  } else {
    // Default case - let PostgreSQL try to infer
    return sql`ARRAY[${sql.join(value)}]`
  }
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
  } = await import('kysely')

  const pg = new Kysely<any>({
    dialect: {
      createAdapter: () => new PostgresAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new PostgresIntrospector(db),
      createQueryCompiler: () => new PostgresQueryCompiler(),
    },
  })
  const compile = (v: any) => v.compile(pg)

  describe('transformOperatorValue', () => {
    it('passes non-array operators through unchanged', () => {
      expect(transformOperatorValue('$eq', 5)).toBe(5)
      expect(transformOperatorValue('$like', '%x%')).toBe('%x%')
    })

    it('passes a falsy value through unchanged', () => {
      expect(transformOperatorValue('$contains', undefined)).toBeUndefined()
      expect(transformOperatorValue('$contains', null)).toBeNull()
    })

    it('throws when an array operator gets a non-array value', () => {
      expect(() => transformOperatorValue('$contains', 5)).toThrow(
        "The value for '$contains' must be an array",
      )
    })

    it('casts to the declared array type when annotated', () => {
      const { sql: text, parameters } = compile(
        transformOperatorValue('$contains', ['a', 'b'], 'varchar[]'),
      )
      expect(text).toBe('ARRAY[$1, $2]::varchar[]')
      expect(parameters).toEqual(['a', 'b'])
    })

    it('casts a precision-qualified declared type, trimming whitespace', () => {
      const { sql: text } = compile(
        transformOperatorValue('$contains', ['aaaa'], '  char(4)[]  '),
      )
      expect(text).toBe('ARRAY[$1]::char(4)[]')
    })

    it('infers integer[] for numeric values without an annotation', () => {
      const { sql: text, parameters } = compile(
        transformOperatorValue('$overlap', [1, 2]),
      )
      expect(text).toBe('ARRAY[$1, $2]::integer[]')
      expect(parameters).toEqual([1, 2])
    })

    it('infers text[] for string values without an annotation', () => {
      const { sql: text } = compile(transformOperatorValue('$contained', ['a']))
      expect(text).toBe('ARRAY[$1]::text[]')
    })

    it('leaves the cast to Postgres for other element types', () => {
      const { sql: text } = compile(transformOperatorValue('$contains', [true]))
      expect(text).toBe('ARRAY[$1]')
    })
  })
}
