import {
  BadRequest,
  Conflict,
  FeathersError,
  Forbidden,
  GeneralError,
  NotFound,
  Unavailable,
  Unprocessable,
} from '@feathersjs/errors'

export const ERROR = Symbol.for('feathers-kysely/error')

const PG_IDENTIFIER = /"([^"]*)"/g

/**
 * Sanitize a Postgres error message for API clients. Postgres wraps every
 * object name (table, column, constraint) in double quotes, so quoted spans
 * are the only place schema internals leak into `message`. We keep an
 * identifier when it is a declared column (already public, and useful — it
 * tells the client which field failed) and strip the rest (table names,
 * constraint names, literal values). With no `knownColumns` every identifier
 * is stripped. The raw, un-sanitized error is preserved on
 * `feathersError[ERROR]` for server-side logging.
 */
function sanitizePgMessage(
  message: string,
  knownColumns?: { has(name: string): boolean },
): string {
  return message
    .replace(PG_IDENTIFIER, (match, identifier) =>
      knownColumns?.has(identifier) ? match : '',
    )
    .replace(/\s+/g, ' ')
    .replace(/[\s:;,]+$/g, '')
    .trim()
}

// A name or a value in single quotes, a clause as ` in 'field list'`, an
// identifier of a foreign key's definition in backticks
const MYSQL_QUOTED = /(?::|\s+in)?\s*(['`])([^'`]*)\1/g
// ` (`db`.`child`, CONSTRAINT `fk` FOREIGN KEY (`parent_id`) REFERENCES ...)`
const MYSQL_FOREIGN_KEY = / \(`.*\)$/

/**
 * Sanitize a MySQL error message like a Postgres one. MySQL quotes names and
 * values alike, a column at times with its table (`'users.email'`): we keep a
 * declared column, without its table, and strip everything else quoted, along
 * with a foreign key's definition.
 */
function sanitizeMysqlMessage(
  message: string,
  knownColumns?: { has(name: string): boolean },
): string {
  return message
    .replace(MYSQL_FOREIGN_KEY, '')
    .replace(MYSQL_QUOTED, (match, _quote, name: string) => {
      const column = name.slice(name.indexOf('.') + 1)
      return knownColumns?.has(column) ? match.replace(name, column) : ''
    })
    .replace(/\s+/g, ' ')
    .replace(/[\s:;,]+$/g, '')
    .trim()
}

// What a SQLite message names after its failure: columns with their table,
// `NOT NULL constraint failed: users.email`, `cannot store TEXT value in
// INTEGER column users.age`, `no such table: users`; a CHECK constraint's name
// or expression, or `index 'name'` for a unique index on an expression
const SQLITE_FAILURE =
  /^(.+? constraint failed:|no such \w+:|cannot store .+? column) (.+)$/

/** The columns a SQLite message names after its failure, without their table. */
function sqliteColumns(message: unknown): string[] {
  if (typeof message !== 'string') return []
  const names = SQLITE_FAILURE.exec(message)?.[2]
  if (!names) return []
  return names.split(', ').map((name) => name.slice(name.indexOf('.') + 1))
}

/**
 * Sanitize a SQLite error message like a Postgres one: keep the declared
 * columns it names after its failure, without their table, and strip the rest.
 */
function sanitizeSqliteMessage(
  message: string,
  knownColumns?: { has(name: string): boolean },
): string {
  const failure = SQLITE_FAILURE.exec(message)?.[1]
  if (!failure) return message
  const columns = sqliteColumns(message).filter((column) =>
    knownColumns?.has(column),
  )
  if (columns.length) return `${failure} ${columns.join(', ')}`
  return failure.endsWith(':') ? failure.slice(0, -1) : failure
}

export interface ErrorHandlerOptions {
  /**
   * The columns a unique or exclusion violation may name to the client, in
   * `Conflict#data.columns`. Naming one can reveal that a value exists (an
   * email, a token), so none are by default. Postgres and SQLite; MySQL names
   * the violated index, not its columns.
   */
  exposeConflictColumns?: readonly string[]
}

/**
 * The client-facing message of a unique violation on every dialect: Postgres'
 * with the constraint name stripped. MySQL's names the duplicate value and the
 * table, SQLite's the table.
 */
const UNIQUE_VIOLATION = 'duplicate key value violates unique constraint'

// `Key (email, "tenantId")=(...)`: the columns of the violated key, before its values
const PG_CONFLICT_KEY = /^Key \((.+?)\)=\(/
const PG_QUOTED = /^"(.*)"$/

// MySQL errors of a client's input that come with SQLSTATE HY000 or 01000
// rather than class 22 or 23: a missing not-null column, a failed check, a
// value of the wrong type or outside an ENUM
const MYSQL_BAD_INPUT = new Set([
  'ER_NO_DEFAULT_FOR_FIELD',
  'ER_CHECK_CONSTRAINT_VIOLATED',
  'ER_TRUNCATED_WRONG_VALUE_FOR_FIELD',
  'WARN_DATA_TRUNCATED',
])

// better-sqlite3's extended result codes of a unique violation
const SQLITE_UNIQUE_VIOLATION = new Set([
  'SQLITE_CONSTRAINT_UNIQUE',
  'SQLITE_CONSTRAINT_PRIMARYKEY',
])

// better-sqlite3's result codes of any other constraint violation, or of a
// value of the wrong type (`datatype mismatch`)
const SQLITE_BAD_INPUT = /^SQLITE_(CONSTRAINT|MISMATCH)/

/** The violated key's columns in a Postgres `detail`, never their values. */
function pgKeyColumns(detail: unknown): string[] {
  if (typeof detail !== 'string') return []
  const key = PG_CONFLICT_KEY.exec(detail)?.[1]
  if (!key) return []
  return key.split(', ').map((column) => column.replace(PG_QUOTED, '$1'))
}

/** A unique violation's `Conflict`, naming the key's exposed columns. */
function conflict(
  message: string,
  keyColumns: string[],
  exposed: readonly string[] | undefined,
): Conflict {
  const columns = keyColumns.filter((column) => exposed?.includes(column))
  return new Conflict(message, columns.length ? { columns } : undefined)
}

/**
 * Convert a database error into a Feathers error.
 *
 * `knownColumns` is the set of public column names (the service's
 * `properties`); identifiers matching it are kept in the client-facing
 * message, everything else is stripped. The raw error is always preserved on
 * `feathersError[ERROR]` for server-side logging.
 */
export function errorHandler(
  error: any,
  knownColumns?: { has(name: string): boolean },
  options?: ErrorHandlerOptions,
): never {
  const { message } = error
  let feathersError = error

  if (error.sqlState && error.sqlState.length) {
    // remove SQLSTATE marker (#) and pad/truncate SQLSTATE to 5 chars
    const sqlState = ('00000' + error.sqlState.replace('#', '')).slice(-5)
    const safe =
      typeof message === 'string'
        ? sanitizeMysqlMessage(message, knownColumns)
        : message

    if (error.code === 'ER_DUP_ENTRY') {
      // `Duplicate entry 'a@b.c' for key 'users.email'` names the value, and
      // an index rather than its columns
      feathersError = new Conflict(UNIQUE_VIOLATION)
    } else if (MYSQL_BAD_INPUT.has(error.code)) {
      feathersError = new BadRequest(safe)
    } else {
      switch (sqlState.slice(0, 2)) {
        case '02':
          feathersError = new NotFound(safe)
          break
        case '28':
          feathersError = new Forbidden(safe)
          break
        case '08':
        case '0A':
        case '0K':
          feathersError = new Unavailable(safe)
          break
        case '20':
        case '21':
        case '22':
        case '23':
        case '24':
        case '25':
        case '40':
        case '42':
        case '70':
          feathersError = new BadRequest(safe)
          break
        default:
          feathersError = new GeneralError(safe)
      }
    }
  } else if (error.code === 'SQLITE_ERROR') {
    const safe =
      typeof message === 'string'
        ? sanitizeSqliteMessage(message, knownColumns)
        : message

    // NOTE (EK): Error codes taken from
    // https://www.sqlite.org/c3ref/c_abort.html
    switch (error.errno) {
      case 1:
      case 8:
      case 18:
      case 19:
      case 20:
        feathersError = new BadRequest(safe)
        break
      case 2:
        feathersError = new Unavailable(safe)
        break
      case 3:
      case 23:
        feathersError = new Forbidden(safe)
        break
      case 12:
        feathersError = new NotFound(safe)
        break
      default:
        feathersError = new GeneralError(safe)
        break
    }
  } else if (SQLITE_UNIQUE_VIOLATION.has(error.code)) {
    feathersError = conflict(
      UNIQUE_VIOLATION,
      sqliteColumns(message),
      options?.exposeConflictColumns,
    )
  } else if (SQLITE_BAD_INPUT.test(error.code)) {
    feathersError = new BadRequest(
      typeof message === 'string'
        ? sanitizeSqliteMessage(message, knownColumns)
        : message,
    )
  } else if (
    typeof error.code === 'string' &&
    error.severity &&
    error.routine
  ) {
    // NOTE: Error codes taken from
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    const safe =
      typeof message === 'string'
        ? sanitizePgMessage(message, knownColumns)
        : message

    if (error.code === '23505' || error.code === '23P01') {
      // unique_violation / exclusion_violation
      feathersError = conflict(
        safe,
        pgKeyColumns(error.detail),
        options?.exposeConflictColumns,
      )
    } else {
      switch (error.code.slice(0, 2)) {
        case '22':
          // Data exception — most commonly an invalid id format on a lookup
          // (e.g. GET /service/<non-integer>). Feathers adapters map this to
          // NotFound so a malformed id reads as a missing resource (404); the
          // adapter conformance suite relies on this.
          feathersError = new NotFound(safe)
          break
        case '23': // integrity constraint: not_null, check, foreign_key
          feathersError = new BadRequest(safe)
          break
        case '28':
          feathersError = new Forbidden(safe)
          break
        case '3D':
        case '3F':
        case '42':
          feathersError = new Unprocessable(safe)
          break
        default:
          feathersError = new GeneralError(safe)
          break
      }
    }
  } else if (!(error instanceof FeathersError)) {
    feathersError = new GeneralError(message)
  }

  feathersError[ERROR] = error

  throw feathersError
}
