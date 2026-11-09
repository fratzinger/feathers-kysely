import type { DbPropertyType, PropertySchema } from '../declarations.js'

const isNullSchema = (schema: unknown) =>
  !!schema && (schema as PropertySchema).type === 'null'

/**
 * The `x-db-type` annotation of a column's JSON schema property. A nullable
 * column is a union of one schema and `{ type: 'null' }` (TypeBox's
 * `Type.Union([T, Type.Null()])`, `anyOf` / `oneOf` elsewhere) and carries the
 * annotation on its non-null branch, so that branch is read when the property
 * itself has none. A union of several non-null branches is ambiguous: none.
 */
export function getAnnotatedDbType(
  schema: unknown,
): DbPropertyType | undefined {
  if (!schema || typeof schema !== 'object') return undefined

  const { 'x-db-type': annotated, anyOf, oneOf } = schema as PropertySchema
  if (typeof annotated === 'string') return annotated

  const union = anyOf ?? oneOf
  if (!Array.isArray(union)) return undefined

  const branches = union.filter((branch) => !isNullSchema(branch))
  return branches.length === 1 ? getAnnotatedDbType(branches[0]) : undefined
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  const jsonb = { type: 'array', 'x-db-type': 'jsonb' }

  describe('getAnnotatedDbType', () => {
    it('reads the annotation of the property', () => {
      expect(getAnnotatedDbType(jsonb)).toBe('jsonb')
    })

    it('reads the non-null branch of a nullable anyOf or oneOf', () => {
      expect(getAnnotatedDbType({ anyOf: [jsonb, { type: 'null' }] })).toBe(
        'jsonb',
      )
      expect(getAnnotatedDbType({ oneOf: [{ type: 'null' }, jsonb] })).toBe(
        'jsonb',
      )
    })

    it('prefers the annotation of the union itself', () => {
      expect(
        getAnnotatedDbType({
          anyOf: [jsonb, { type: 'null' }],
          'x-db-type': 'text[]',
        }),
      ).toBe('text[]')
    })

    it('unwraps a nullable union nested in another', () => {
      expect(
        getAnnotatedDbType({
          anyOf: [{ anyOf: [jsonb, { type: 'null' }] }, { type: 'null' }],
        }),
      ).toBe('jsonb')
    })

    it('reads nothing off a union of several non-null branches', () => {
      expect(
        getAnnotatedDbType({ anyOf: [jsonb, { type: 'string' }] }),
      ).toBeUndefined()
    })

    it('reads nothing off a missing, boolean or unannotated property', () => {
      expect(getAnnotatedDbType(undefined)).toBeUndefined()
      expect(getAnnotatedDbType(true)).toBeUndefined()
      expect(getAnnotatedDbType({ type: 'string' })).toBeUndefined()
      expect(getAnnotatedDbType({ anyOf: [{ type: 'null' }] })).toBeUndefined()
    })
  })
}
