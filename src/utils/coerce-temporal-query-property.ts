import type { TemporalKind } from './temporal-kind.js'
import { coerceTemporalValue } from './coerce-temporal-value.js'

// Comparison operators whose values are temporal scalars (or arrays of them).
// Pattern operators ($like, …) and the Postgres array operators are excluded so
// their values are never reinterpreted as dates.
const TEMPORAL_OPERATORS = new Set([
  '$lt',
  '$lte',
  '$gt',
  '$gte',
  '$eq',
  '$ne',
  '$in',
  '$nin',
])

/**
 * Coerce the value side of a single column's query for a temporal column. The
 * input is either a bare value (`{ col: value }`) or an operator object
 * (`{ col: { $gt: value, $in: [...] } }`). Operator keys and non-temporal
 * operators are left untouched; only the leaf values of temporal comparison
 * operators (and bare equality) are normalized via `coerceTemporalValue`.
 */
export function coerceTemporalQueryProperty(
  queryProperty: any,
  kind: TemporalKind,
): any {
  // An operator object like { $gt: ..., $in: [...] }. A Date is an object too,
  // so treat only record-like objects as operator maps; a Date/array/scalar is
  // a bare value.
  if (
    queryProperty !== null &&
    typeof queryProperty === 'object' &&
    !(queryProperty instanceof Date) &&
    !Array.isArray(queryProperty)
  ) {
    const out: Record<string, any> = {}
    for (const operator in queryProperty) {
      const value = queryProperty[operator]
      if (!TEMPORAL_OPERATORS.has(operator)) {
        out[operator] = value
        continue
      }
      out[operator] = Array.isArray(value)
        ? value.map((v) => coerceTemporalValue(v, kind))
        : coerceTemporalValue(value, kind)
    }
    return out
  }

  // Bare value: { col: dateLike }
  return coerceTemporalValue(queryProperty, kind)
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('coerceTemporalQueryProperty', () => {
    it('coerces a bare scalar value', () => {
      expect(coerceTemporalQueryProperty('2026-06-17', 'date')).toBe(
        '2026-06-17',
      )
    })

    it('coerces a bare Date value', () => {
      const d = new Date('2026-06-17T00:00:00.000Z')
      expect(coerceTemporalQueryProperty(d, 'date')).toBe('2026-06-17')
    })

    it('coerces the leaf values of temporal operators', () => {
      const d = new Date('2026-06-17T00:00:00.000Z')
      expect(coerceTemporalQueryProperty({ $gt: d }, 'date')).toEqual({
        $gt: '2026-06-17',
      })
    })

    it('coerces each element of array-valued temporal operators ($in/$nin)', () => {
      // Inputs whose coercion is observable (a Date and an epoch-ms number), so
      // the test actually proves the per-element .map ran.
      const result = coerceTemporalQueryProperty(
        {
          $in: [
            new Date('2026-06-17T12:00:00.000Z'),
            Date.parse('2026-06-18T00:00:00.000Z'),
          ],
        },
        'date',
      )
      expect(result).toEqual({ $in: ['2026-06-17', '2026-06-18'] })
    })

    it('coerces a scalar (non-array) value for a temporal operator', () => {
      const d = new Date('2026-06-17T00:00:00.000Z')
      expect(coerceTemporalQueryProperty({ $eq: d }, 'date')).toEqual({
        $eq: '2026-06-17',
      })
      // a scalar $in (not an array) still hits the single-value branch
      expect(coerceTemporalQueryProperty({ $in: d }, 'date')).toEqual({
        $in: '2026-06-17',
      })
    })

    it('leaves non-temporal operators untouched', () => {
      expect(
        coerceTemporalQueryProperty({ $like: '%2026%' }, 'date'),
      ).toEqual({ $like: '%2026%' })
    })

    it('never reinterprets Postgres array-operator values as dates', () => {
      // A Date element would be coerced to a 'YYYY-MM-DD' string if $contains
      // were treated as temporal; it must stay a Date in the same array ref.
      const date = new Date('2026-06-17T00:00:00.000Z')
      const input = { $contains: [date] }
      const result = coerceTemporalQueryProperty(input, 'date')
      expect(result.$contains).toBe(input.$contains)
      expect(result.$contains[0]).toBe(date)
    })

    it('mixes coerced and passthrough operators in one object', () => {
      const d = new Date('2026-06-17T00:00:00.000Z')
      expect(
        coerceTemporalQueryProperty({ $gte: d, $like: 'x' }, 'date'),
      ).toEqual({ $gte: '2026-06-17', $like: 'x' })
    })
  })
}
