// Recognized temporal column kinds for opt-in date coercion. 'instant' covers
// timestamp/timestamptz/datetime columns; 'date' is a calendar day with no time.
export type TemporalKind = 'instant' | 'date'

/**
 * Map a `getPropertyType` return value to a temporal coercion kind, or
 * `undefined` when the type is not a recognized temporal type (so no coercion
 * happens). Matching is case-insensitive: anything containing "timestamp" or
 * equal to "datetime" is an instant; an exact "date" is a calendar day.
 */
export function temporalKind(type: unknown): TemporalKind | undefined {
  if (typeof type !== 'string') return undefined
  const t = type.toLowerCase()
  if (t === 'datetime' || t.includes('timestamp')) return 'instant'
  if (t === 'date') return 'date'
  return undefined
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('temporalKind', () => {
    it('maps an exact "date" to the date kind (case-insensitive)', () => {
      expect(temporalKind('date')).toBe('date')
      expect(temporalKind('DATE')).toBe('date')
    })

    it('maps datetime and any timestamp variant to instant', () => {
      expect(temporalKind('datetime')).toBe('instant')
      expect(temporalKind('timestamp')).toBe('instant')
      expect(temporalKind('timestamptz')).toBe('instant')
      expect(temporalKind('TIMESTAMP WITH TIME ZONE')).toBe('instant')
    })

    it('does not treat a date[] array column as a temporal scalar', () => {
      // Crucial: array containment queries on date[] must not be coerced.
      expect(temporalKind('date[]')).toBeUndefined()
    })

    it('still flags timestamp[] as instant (substring match)', () => {
      // Array-operator values are excluded from coercion elsewhere, so this is
      // harmless, but the substring match is intentional.
      expect(temporalKind('timestamp[]')).toBe('instant')
    })

    it('returns undefined for non-temporal and non-string types', () => {
      expect(temporalKind('text')).toBeUndefined()
      expect(temporalKind('jsonb')).toBeUndefined()
      expect(temporalKind(undefined)).toBeUndefined()
      expect(temporalKind(123)).toBeUndefined()
      expect(temporalKind(null)).toBeUndefined()
    })
  })
}
