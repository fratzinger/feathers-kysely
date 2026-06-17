import type { ComparisonOperatorExpression } from 'kysely'
import type { DialectType } from '../declarations.js'

// See https://kysely-org.github.io/kysely-apidoc/variables/OPERATORS.html
export const OPERATORS: Record<string, ComparisonOperatorExpression> = {
  $lt: '<',
  $lte: '<=',
  $gt: '>',
  $gte: '>=',
  $in: 'in',
  $nin: 'not in',
  $eq: '=',
  $ne: '!=',
  $like: 'like',
  $notLike: 'not like',
  $iLike: 'ilike',
  $contains: '@>',
  $contained: '<@',
  $overlap: '&&',
}

// Operators that only exist in Postgres. They are not registered (and therefore
// rejected by Feathers with a BadRequest) on other dialects. NOTE: $iLike is
// intentionally NOT here — it is translated to a case-insensitive LIKE instead.
export const POSTGRES_ONLY_OPERATORS = ['$contains', '$contained', '$overlap']

/**
 * Resolve a Feathers query operator to the Kysely comparison operator, applying
 * two dialect-aware special cases:
 *  - a `null` value turns `$eq`/`$ne` into `is`/`is not`;
 *  - `$iLike` falls back to a plain (case-insensitive) `like` on every dialect
 *    except Postgres, which is the only one with an `ilike` keyword.
 */
export function getOperator(
  op: string,
  value: any,
  dialectType: DialectType | undefined,
) {
  if (value === null) {
    if (op === '$ne') return 'is not'
    if (op === '$eq') return 'is'
    return OPERATORS[op]
  }

  // No dialect except Postgres has an ILIKE keyword. MySQL's default collation
  // is case-insensitive and SQLite's LIKE is case-insensitive for ASCII, so
  // plain LIKE gives equivalent behavior for typical input (case folding of
  // non-ASCII on SQLite/MySQL depends on the column collation).
  if (op === '$iLike' && dialectType !== 'postgres') {
    return 'like'
  }

  return OPERATORS[op]
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('getOperator', () => {
    it('maps standard operators straight through the table', () => {
      expect(getOperator('$gt', 5, 'sqlite')).toBe('>')
      expect(getOperator('$in', [1], 'postgres')).toBe('in')
      expect(getOperator('$contains', [1], 'postgres')).toBe('@>')
    })

    it('turns $eq/$ne against null into is / is not', () => {
      expect(getOperator('$eq', null, 'postgres')).toBe('is')
      expect(getOperator('$ne', null, 'postgres')).toBe('is not')
    })

    it('still resolves other operators normally when value is null', () => {
      expect(getOperator('$in', null, 'postgres')).toBe('in')
    })

    it('keeps ilike on Postgres but falls back to like elsewhere', () => {
      expect(getOperator('$iLike', 'x', 'postgres')).toBe('ilike')
      expect(getOperator('$iLike', 'x', 'mysql')).toBe('like')
      expect(getOperator('$iLike', 'x', 'sqlite')).toBe('like')
      expect(getOperator('$iLike', 'x', undefined)).toBe('like')
    })
  })

  describe('OPERATORS / POSTGRES_ONLY_OPERATORS', () => {
    it('lists exactly the Postgres-only array operators', () => {
      expect(POSTGRES_ONLY_OPERATORS).toEqual([
        '$contains',
        '$contained',
        '$overlap',
      ])
      // every Postgres-only operator has a mapping in the table
      for (const op of POSTGRES_ONLY_OPERATORS) {
        expect(OPERATORS[op]).toBeTruthy()
      }
    })
  })
}
