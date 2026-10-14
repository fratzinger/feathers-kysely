/**
 * The first two items sharing a conflict key, as `[earlier, later]` indexes, or
 * `undefined`. An item with a null or missing value in any conflict field has
 * no key: NULLs never conflict in a unique index, and a missing value is left to
 * the database (an auto-increment id, a default).
 */
export function findDuplicateConflictKey(
  items: readonly Record<string, unknown>[],
  fields: readonly string[],
): [number, number] | undefined {
  const seen = new Map<string, number>()

  for (const [index, item] of items.entries()) {
    const values = fields.map((field) => item?.[field])
    if (values.some((value) => value == null)) continue

    // JSON.stringify can't serialize a bigint on its own
    const key = JSON.stringify(values, (_key, value) =>
      typeof value === 'bigint' ? value.toString() : value,
    )
    const first = seen.get(key)
    if (first !== undefined) return [first, index]
    seen.set(key, index)
  }

  return undefined
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('findDuplicateConflictKey', () => {
    it('returns the first two items sharing a key', () => {
      expect(
        findDuplicateConflictKey(
          [{ sku: 'a' }, { sku: 'b' }, { sku: 'a' }, { sku: 'b' }],
          ['sku'],
        ),
      ).toEqual([0, 2])
    })

    it('returns undefined when every key is distinct', () => {
      expect(
        findDuplicateConflictKey([{ sku: 'a' }, { sku: 'b' }], ['sku']),
      ).toBeUndefined()
    })

    it('compares composite keys as a whole', () => {
      const items = [
        { shop: 1, sku: 'a' },
        { shop: 2, sku: 'a' },
      ]
      expect(findDuplicateConflictKey(items, ['shop', 'sku'])).toBeUndefined()
      expect(
        findDuplicateConflictKey(
          [...items, { shop: 2, sku: 'a' }],
          ['shop', 'sku'],
        ),
      ).toEqual([1, 2])
    })

    it('gives an item with a null or missing key value no key', () => {
      expect(
        findDuplicateConflictKey([{ id: null }, { id: null }, {}, {}], ['id']),
      ).toBeUndefined()
      expect(
        findDuplicateConflictKey(
          [
            { shop: 1, sku: null },
            { shop: 1, sku: null },
          ],
          ['shop', 'sku'],
        ),
      ).toBeUndefined()
    })

    it('handles bigint values', () => {
      expect(
        findDuplicateConflictKey([{ id: 1n }, { id: 2n }, { id: 1n }], ['id']),
      ).toEqual([0, 2])
    })
  })
}
