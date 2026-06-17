import { sql } from 'kysely'
import type { DialectType } from '../declarations.js'
import { Unprocessable } from '@feathersjs/errors'

/**
 * Build a JSON-path accessor expression for a column.
 *
 * Every path segment is bound as a SQL parameter (never interpolated as raw
 * SQL), so attacker-controlled query keys cannot inject SQL. The accessor is
 * dialect-specific: Postgres uses the native `->` / `->>` operators, while
 * SQLite and MySQL use `json_extract(col, '$.a.b')`.
 */
export function traverseJSON(
  column: string,
  path: string[],
  dialectType: DialectType = 'postgres',
) {
  if (!path.length) {
    throw new Unprocessable('Path must have at least one element')
  }

  if (dialectType === 'sqlite' || dialectType === 'mysql') {
    // The whole path is passed as a single bound parameter to json_extract.
    const jsonPath = `$${path.map((p) => `.${p}`).join('')}`
    return sql`json_extract(${sql.ref(column)}, ${jsonPath})`
  }

  // Postgres: col -> 'a' -> 'b' ->> 'c', each key bound as a parameter.
  let expr = sql`${sql.ref(column)}`
  for (const key of path.slice(0, -1)) {
    expr = sql`${expr}->${key}`
  }
  return sql`${expr}->>${path[path.length - 1]}`
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    SqliteAdapter,
    SqliteIntrospector,
    SqliteQueryCompiler,
  } = await import('kysely')

  // Offline Kysely instances (DummyDriver never connects) used purely to compile
  // the raw expression into a SQL string + bound parameters for assertions.
  const pg = new Kysely<any>({
    dialect: {
      createAdapter: () => new PostgresAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new PostgresIntrospector(db),
      createQueryCompiler: () => new PostgresQueryCompiler(),
    },
  })
  const sqlite = new Kysely<any>({
    dialect: {
      createAdapter: () => new SqliteAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new SqliteIntrospector(db),
      createQueryCompiler: () => new SqliteQueryCompiler(),
    },
  })

  describe('traverseJSON', () => {
    it('throws when the path is empty', () => {
      expect(() => traverseJSON('col', [])).toThrow(
        'Path must have at least one element',
      )
    })

    it('uses native -> / ->> operators on Postgres, binding each key', () => {
      const { sql: text, parameters } = traverseJSON('col', [
        'a',
        'b',
      ]).compile(pg)
      expect(text).toBe('"col"->$1->>$2')
      expect(parameters).toEqual(['a', 'b'])
    })

    it('emits a single ->> for a one-segment path on Postgres', () => {
      const { sql: text, parameters } = traverseJSON('col', ['a']).compile(pg)
      expect(text).toBe('"col"->>$1')
      expect(parameters).toEqual(['a'])
    })

    it('uses json_extract with a single bound path on SQLite/MySQL', () => {
      const { sql: text, parameters } = traverseJSON(
        'col',
        ['a', 'b'],
        'sqlite',
      ).compile(sqlite)
      expect(text).toBe('json_extract("col", ?)')
      expect(parameters).toEqual(['$.a.b'])
    })

    it('defaults to the Postgres accessor', () => {
      const { sql: text } = traverseJSON('col', ['a']).compile(pg)
      expect(text).toBe('"col"->>$1')
    })

    it('binds an injection-shaped key as a parameter on Postgres', () => {
      // The function's core guarantee: the key never reaches the SQL text.
      const evil = "a'; DROP TABLE users; --"
      const { sql: text, parameters } = traverseJSON('col', [evil]).compile(pg)
      expect(text).toBe('"col"->>$1')
      expect(parameters).toEqual([evil])
    })

    it('keeps an injection-shaped key inside the bound json_extract path on SQLite', () => {
      const evil = "a'; DROP TABLE users; --"
      const { sql: text, parameters } = traverseJSON(
        'col',
        [evil],
        'sqlite',
      ).compile(sqlite)
      expect(text).toBe('json_extract("col", ?)')
      expect(parameters).toEqual([`$.${evil}`])
    })
  })
}
