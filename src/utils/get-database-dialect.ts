import type { Kysely } from 'kysely'
import type { DialectType } from '../declarations.js'

/**
 * Detect the SQL dialect of a Kysely instance from its adapter's class name.
 * Defaults to `sqlite` for unknown adapters; throws for the explicitly
 * unsupported MSSQL/SQL Server adapter.
 */
export function getDatabaseDialect(db: Kysely<any>): DialectType {
  const adapterName = db.getExecutor().adapter.constructor.name.toLowerCase()

  if (adapterName.includes('sqlite')) return 'sqlite'
  if (adapterName.includes('postgres')) return 'postgres'
  if (adapterName.includes('mysql')) return 'mysql'
  if (adapterName.includes('mssql') || adapterName.includes('sqlserver')) {
    throw new Error(
      'MSSQL is not supported by feathers-kysely. Supported dialects: postgres, mysql, sqlite.',
    )
  }

  return 'sqlite'
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  // The function only reads db.getExecutor().adapter.constructor.name, so a tiny
  // stand-in is enough to exercise every branch.
  const fakeDb = (adapterClassName: string) =>
    ({
      getExecutor: () => ({ adapter: { constructor: { name: adapterClassName } } }),
    }) as unknown as Kysely<any>

  describe('getDatabaseDialect', () => {
    it('detects each supported dialect from the adapter name', () => {
      expect(getDatabaseDialect(fakeDb('PostgresAdapter'))).toBe('postgres')
      expect(getDatabaseDialect(fakeDb('SqliteAdapter'))).toBe('sqlite')
      expect(getDatabaseDialect(fakeDb('MysqlAdapter'))).toBe('mysql')
    })

    it('defaults to sqlite for an unknown adapter', () => {
      expect(getDatabaseDialect(fakeDb('SomeOtherAdapter'))).toBe('sqlite')
    })

    it('throws for the unsupported MSSQL / SQL Server adapters', () => {
      expect(() => getDatabaseDialect(fakeDb('MssqlAdapter'))).toThrow(
        'MSSQL is not supported',
      )
      expect(() => getDatabaseDialect(fakeDb('SqlServerAdapter'))).toThrow(
        'MSSQL is not supported',
      )
    })
  })
}
