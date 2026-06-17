export function convertBooleansToNumbers<T>(data: T): T {
  // Handle primitive types
  if (typeof data === 'boolean') {
    return data ? (1 as any) : (0 as any)
  }

  // Handle null, undefined, functions, etc.
  if (data === null || typeof data !== 'object') {
    return data
  }

  // Handle arrays
  if (Array.isArray(data)) {
    let modified = false
    const result = []

    for (let i = 0; i < data.length; i++) {
      const converted = convertBooleansToNumbers(data[i])
      result[i] = converted

      // Track if any modifications were made
      if (converted !== data[i]) {
        modified = true
      }
    }

    // Return original array if no changes were needed
    return modified ? (result as T) : data
  }

  // Handle objects
  let modified = false
  const result = {} as T

  for (const key in data) {
    if (Object.prototype.hasOwnProperty.call(data, key)) {
      const converted = convertBooleansToNumbers(data[key] as any)
      result[key] = converted

      // Track if any modifications were made
      if (converted !== data[key]) {
        modified = true
      }
    }
  }

  // Return original object if no changes were needed
  return modified ? result : data
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  describe('convertBooleansToNumbers', () => {
    it('converts true to 1 and false to 0', () => {
      expect(convertBooleansToNumbers(true)).toBe(1)
      expect(convertBooleansToNumbers(false)).toBe(0)
    })

    it('leaves non-boolean primitives untouched', () => {
      expect(convertBooleansToNumbers(null)).toBeNull()
      expect(convertBooleansToNumbers(undefined)).toBeUndefined()
      expect(convertBooleansToNumbers(42)).toBe(42)
      expect(convertBooleansToNumbers('hello')).toBe('hello')
    })

    it('converts booleans inside arrays', () => {
      expect(convertBooleansToNumbers([true, false, 2])).toEqual([1, 0, 2])
    })

    it('returns the same array reference when nothing changed', () => {
      const input = [1, 2, 'x']
      expect(convertBooleansToNumbers(input)).toBe(input)
    })

    it('converts booleans inside nested objects', () => {
      expect(
        convertBooleansToNumbers({ a: true, b: { c: false, d: 'x' } }),
      ).toEqual({ a: 1, b: { c: 0, d: 'x' } })
    })

    it('returns the same object reference when nothing changed', () => {
      const input = { a: 1, b: { c: 'x' } }
      expect(convertBooleansToNumbers(input)).toBe(input)
    })

    it('rebuilds only the changed level and shares unchanged subtrees', () => {
      const inner = { c: 'x' }
      const input = { a: true, b: inner }
      const result = convertBooleansToNumbers(input)
      expect(result).not.toBe(input) // top level rebuilt (a changed)
      expect(result).toEqual({ a: 1, b: { c: 'x' } })
      expect(result.b).toBe(inner) // unchanged subtree reused, not copied
    })
  })
}
