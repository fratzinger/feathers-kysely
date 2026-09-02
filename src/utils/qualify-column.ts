/**
 * Prefix a column with the table or alias it belongs to, so a reference stays
 * unambiguous once a query joins anything.
 *
 * Three cases, in order:
 *  - an explicit `tableName` wins — `null` means "already qualified, leave it";
 *  - otherwise a column known to be on `ownTable` is qualified with it;
 *  - anything else is returned untouched, because it may already be a
 *    qualified ref (`alias.column`) or a JSON path.
 *
 * Arrays are mapped element-wise, and non-strings (raw expressions) pass
 * through, so callers can hand it a `$select` list unchanged.
 */
export function qualifyColumn<T>(
  column: T,
  options: {
    /** Table or alias to qualify with. `null` leaves the column as-is. */
    tableName?: string | null | undefined
    /** Table the service's own columns live on. */
    ownTable: string
    /** Whether a bare name is a declared column of `ownTable`. */
    isOwnColumn: (column: string) => boolean
  },
): T {
  if (Array.isArray(column)) {
    return column.map((item) => qualifyColumn(item, options)) as T
  }

  if (typeof column !== 'string') return column
  if (options.tableName === null) return column

  const tableName =
    options.tableName || (options.isOwnColumn(column) ? options.ownTable : null)

  if (!tableName || column.startsWith(`${tableName}.`)) return column

  return `${tableName}.${column}` as T
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  const own = new Set(['id', 'name'])
  const options = {
    ownTable: 'users',
    isOwnColumn: (column: string) => own.has(column),
  }

  describe('qualifyColumn', () => {
    it('qualifies a declared column with its own table', () => {
      expect(qualifyColumn('name', options)).toBe('users.name')
    })

    it('leaves an unknown bare name alone', () => {
      // may be a qualified ref added by a join, or a JSON path
      expect(qualifyColumn('meta.theme', options)).toBe('meta.theme')
      expect(qualifyColumn('bogus', options)).toBe('bogus')
    })

    it('prefers an explicit tableName over the own table', () => {
      expect(qualifyColumn('name', { ...options, tableName: 'manager' })).toBe(
        'manager.name',
      )
      expect(qualifyColumn('bogus', { ...options, tableName: 'manager' })).toBe(
        'manager.bogus',
      )
    })

    it('treats a null tableName as already qualified', () => {
      expect(
        qualifyColumn('manager.name', { ...options, tableName: null }),
      ).toBe('manager.name')
      // even a column that would otherwise be qualified
      expect(qualifyColumn('name', { ...options, tableName: null })).toBe(
        'name',
      )
    })

    it('falls back to the own table for an empty tableName', () => {
      expect(qualifyColumn('name', { ...options, tableName: '' })).toBe(
        'users.name',
      )
    })

    it('does not qualify twice', () => {
      expect(qualifyColumn('users.name', options)).toBe('users.name')
      expect(
        qualifyColumn('manager.name', { ...options, tableName: 'manager' }),
      ).toBe('manager.name')
    })

    it('maps arrays element-wise', () => {
      expect(qualifyColumn(['id', 'bogus'], options)).toEqual([
        'users.id',
        'bogus',
      ])
    })

    it('passes non-strings through', () => {
      const expression = { kind: 'raw' }
      expect(qualifyColumn(expression, options)).toBe(expression)
      expect(qualifyColumn(undefined, options)).toBe(undefined)
    })
  })
}
