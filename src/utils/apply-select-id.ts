export function applySelectId($select: string[] | undefined, idField: string) {
  if (!$select) return $select
  return $select.includes(idField) ? $select : $select.concat(idField)
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('applySelectId', () => {
    it('returns the value unchanged when $select is undefined', () => {
      expect(applySelectId(undefined, 'id')).toBeUndefined()
    })

    it('returns the same array when the id field is already selected', () => {
      const $select = ['id', 'name']
      expect(applySelectId($select, 'id')).toBe($select)
    })

    it('appends the id field when it is missing', () => {
      expect(applySelectId(['name'], 'id')).toEqual(['name', 'id'])
    })

    it('appends to an empty selection', () => {
      expect(applySelectId([], 'id')).toEqual(['id'])
    })

    it('honors a custom id field name', () => {
      expect(applySelectId(['name'], '_id')).toEqual(['name', '_id'])
    })
  })
}
