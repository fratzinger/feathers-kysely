import { sql } from 'kysely'
import { BadRequest } from '@feathersjs/errors'
import type { DialectType } from '../declarations.js'

// Dialect-specific regex operator tokens (from a fixed allow-list, never user
// input). Postgres uses POSIX `~` / `!~`; MySQL uses `REGEXP` / `NOT REGEXP`.
// SQLite has no built-in REGEXP and is intentionally absent — the operator is
// not registered there, so this is never reached with `dialectType === 'sqlite'`.
const REGEX_OPERATOR_TOKEN: Record<
  string,
  { $regex: string; $notRegex: string }
> = {
  postgres: { $regex: '~', $notRegex: '!~' },
  mysql: { $regex: 'regexp', $notRegex: 'not regexp' },
}

/**
 * Build a regex-match expression (`$regex` / `$notRegex`). The pattern is bound
 * as a parameter; the operator token is emitted raw from the fixed allow-list
 * above. `column` may be a plain name or an already-built expression.
 */
export function buildRegexMatch(
  column: any,
  operator: '$regex' | '$notRegex',
  dialectType: DialectType | undefined,
  value: any,
) {
  const tokens = dialectType ? REGEX_OPERATOR_TOKEN[dialectType] : undefined
  if (!tokens) {
    throw new BadRequest(
      `The '${operator}' operator is not supported on the '${dialectType}' dialect`,
    )
  }

  const ref = typeof column === 'string' ? sql.ref(column) : column
  return sql<boolean>`${ref} ${sql.raw(tokens[operator])} ${value}`
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    MysqlAdapter,
    MysqlIntrospector,
    MysqlQueryCompiler,
    sql: sqlTag,
  } = await import('kysely')

  const pg = new Kysely<any>({
    dialect: {
      createAdapter: () => new PostgresAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new PostgresIntrospector(db),
      createQueryCompiler: () => new PostgresQueryCompiler(),
    },
  })
  const my = new Kysely<any>({
    dialect: {
      createAdapter: () => new MysqlAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new MysqlIntrospector(db),
      createQueryCompiler: () => new MysqlQueryCompiler(),
    },
  })

  describe('buildRegexMatch', () => {
    it('uses POSIX ~ / !~ on Postgres with the pattern bound', () => {
      const r = sqlTag`${buildRegexMatch('col', '$regex', 'postgres', 'foo.*')}`.compile(pg)
      expect(r.sql).toBe('"col" ~ $1')
      expect(r.parameters).toEqual(['foo.*'])
      const nr = sqlTag`${buildRegexMatch('col', '$notRegex', 'postgres', 'x')}`.compile(pg)
      expect(nr.sql).toBe('"col" !~ $1')
    })

    it('uses REGEXP / NOT REGEXP on MySQL', () => {
      const r = sqlTag`${buildRegexMatch('col', '$regex', 'mysql', 'foo')}`.compile(my)
      expect(r.sql).toBe('`col` regexp ?')
      const nr = sqlTag`${buildRegexMatch('col', '$notRegex', 'mysql', 'foo')}`.compile(my)
      expect(nr.sql).toBe('`col` not regexp ?')
    })

    it('throws for dialects without regex support (e.g. sqlite)', () => {
      expect(() => buildRegexMatch('col', '$regex', 'sqlite', 'x')).toThrow(
        "The '$regex' operator is not supported on the 'sqlite' dialect",
      )
      expect(() => buildRegexMatch('col', '$regex', undefined, 'x')).toThrow()
    })
  })
}
