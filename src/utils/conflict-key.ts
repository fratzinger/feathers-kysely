/**
 * The values of an item's conflict fields as one string, to look items up by
 * their conflict key in a `Map` or `Set` instead of scanning for each of them.
 */
export function conflictKey(
  item: Record<string, unknown>,
  fields: readonly string[],
): string {
  // JSON.stringify can't serialize a bigint on its own
  return JSON.stringify(
    fields.map((field) => item[field]),
    (_key, value) => (typeof value === 'bigint' ? value.toString() : value),
  )
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('conflictKey', () => {
    it('tells items apart by the values of their conflict fields', () => {
      const fields = ['shop', 'sku']
      expect(conflictKey({ shop: 1, sku: 'a', name: 'x' }, fields)).toBe(
        conflictKey({ shop: 1, sku: 'a', name: 'y' }, fields),
      )
      expect(conflictKey({ shop: 1, sku: 'a' }, fields)).not.toBe(
        conflictKey({ shop: 2, sku: 'a' }, fields),
      )
    })

    it('keeps types apart, like a strict comparison', () => {
      expect(conflictKey({ id: 1 }, ['id'])).not.toBe(
        conflictKey({ id: '1' }, ['id']),
      )
    })

    it('handles bigint values', () => {
      expect(conflictKey({ id: 1n }, ['id'])).toBe('["1"]')
    })
  })
}
