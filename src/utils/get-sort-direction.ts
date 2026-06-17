import type { SortDirection, SortProperty } from '../declarations.js'

export function getSortDirection(order: SortProperty): SortDirection {
  if (typeof order === 'object' && order !== null && 'direction' in order) {
    return order.direction
  }
  return order
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('getSortDirection', () => {
    it('returns the direction of an object form', () => {
      expect(getSortDirection({ direction: 'asc' })).toBe('asc')
      expect(getSortDirection({ direction: -1, filter: {} })).toBe(-1)
    })

    it('returns a scalar direction as-is', () => {
      expect(getSortDirection(1)).toBe(1)
      expect(getSortDirection(-1)).toBe(-1)
      expect(getSortDirection('desc')).toBe('desc')
      expect(getSortDirection('asc nulls last')).toBe('asc nulls last')
    })
  })
}
