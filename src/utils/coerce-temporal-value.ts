import type { TemporalKind } from './temporal-kind.js'

/**
 * Normalize a single query value for a temporal column into the canonical
 * string representation that every supported driver compares correctly: a full
 * ISO-8601 UTC string for an instant column, or a "YYYY-MM-DD" string for a date
 * column. Accepts a Date, an epoch-millisecond number, an ISO string, or a
 * "YYYY-MM-DD" string. Normalization is done in UTC. Values that cannot be
 * parsed into a valid date (including null) are returned unchanged.
 */
export function coerceTemporalValue(
  value: unknown,
  kind: TemporalKind,
): unknown {
  if (value == null) return value

  let date: Date
  if (value instanceof Date) {
    date = value
  } else if (typeof value === 'number' || typeof value === 'string') {
    date = new Date(value)
  } else {
    return value
  }

  if (Number.isNaN(date.getTime())) return value

  const iso = date.toISOString()
  return kind === 'date' ? iso.slice(0, 10) : iso
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('coerceTemporalValue', () => {
    it('returns null/undefined unchanged', () => {
      expect(coerceTemporalValue(null, 'instant')).toBeNull()
      expect(coerceTemporalValue(undefined, 'date')).toBeUndefined()
    })

    it('normalizes a Date to a full ISO string for instant columns', () => {
      const d = new Date('2026-06-17T12:34:56.000Z')
      expect(coerceTemporalValue(d, 'instant')).toBe('2026-06-17T12:34:56.000Z')
    })

    it('normalizes a Date to YYYY-MM-DD for date columns', () => {
      const d = new Date('2026-06-17T12:34:56.000Z')
      expect(coerceTemporalValue(d, 'date')).toBe('2026-06-17')
    })

    it('accepts epoch-millisecond numbers', () => {
      const ms = Date.parse('2026-06-17T00:00:00.000Z')
      expect(coerceTemporalValue(ms, 'date')).toBe('2026-06-17')
    })

    it('accepts ISO and YYYY-MM-DD strings', () => {
      expect(coerceTemporalValue('2026-06-17', 'date')).toBe('2026-06-17')
      expect(coerceTemporalValue('2026-06-17T08:00:00.000Z', 'instant')).toBe(
        '2026-06-17T08:00:00.000Z',
      )
    })

    it('returns unparseable strings unchanged', () => {
      expect(coerceTemporalValue('not a date', 'date')).toBe('not a date')
    })

    it('returns values of an unsupported type unchanged', () => {
      expect(coerceTemporalValue(true as unknown, 'date')).toBe(true)
      const obj = { foo: 'bar' }
      expect(coerceTemporalValue(obj, 'instant')).toBe(obj)
    })
  })
}
