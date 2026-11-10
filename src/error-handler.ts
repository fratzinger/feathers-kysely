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

// `UNIQUE constraint failed: users.email, users.tenantId`, or `index 'name'`
// for an index on an expression
const SQLITE_CONFLICT_KEY = /^UNIQUE constraint failed: (.+)$/

// better-sqlite3's extended result codes of a unique violation
const SQLITE_UNIQUE_VIOLATION = new Set([
  'SQLITE_CONSTRAINT_UNIQUE',
  'SQLITE_CONSTRAINT_PRIMARYKEY',
])

/** The violated key's columns in a Postgres `detail`, never their values. */
function pgKeyColumns(detail: unknown): string[] {
  if (typeof detail !== 'string') return []
  const key = PG_CONFLICT_KEY.exec(detail)?.[1]
  if (!key) return []
  return key.split(', ').map((column) => column.replace(PG_QUOTED, '$1'))
}

/** The violated key's columns in a SQLite message, without their table. */
function sqliteKeyColumns(message: unknown): string[] {
  if (typeof message !== 'string') return []
  const key = SQLITE_CONFLICT_KEY.exec(message)?.[1]
  if (!key) return []
  return key.split(', ').map((column) => column.slice(column.indexOf('.') + 1))
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
 * Postgres message, everything else is stripped. The raw error is always
 * preserved on `feathersError[ERROR]` for server-side logging.
 */
export function errorHandler(
  error: any,
  knownColumns?: { has(name: string): boolean },
  options?: ErrorHandlerOptions,
): never {
  const { message } = error
  let feathersError = error

  if (error.code === 'ER_DUP_ENTRY') {
    // MySQL's unique violation: `Duplicate entry 'a@b.c' for key 'users.email'`
    feathersError = new Conflict(UNIQUE_VIOLATION)
  } else if (error.sqlState && error.sqlState.length) {
    // remove SQLSTATE marker (#) and pad/truncate SQLSTATE to 5 chars
    const sqlState = ('00000' + error.sqlState.replace('#', '')).slice(-5)

    switch (sqlState.slice(0, 2)) {
      case '02':
        feathersError = new NotFound(message)
        break
      case '28':
        feathersError = new Forbidden(message)
        break
      case '08':
      case '0A':
      case '0K':
        feathersError = new Unavailable(message)
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
        feathersError = new BadRequest(message)
        break
      default:
        feathersError = new GeneralError(message)
    }
  } else if (error.code === 'SQLITE_ERROR') {
    // NOTE (EK): Error codes taken from
    // https://www.sqlite.org/c3ref/c_abort.html
    switch (error.errno) {
      case 1:
      case 8:
      case 18:
      case 19:
      case 20:
        feathersError = new BadRequest(message)
        break
      case 2:
        feathersError = new Unavailable(message)
        break
      case 3:
      case 23:
        feathersError = new Forbidden(message)
        break
      case 12:
        feathersError = new NotFound(message)
        break
      default:
        feathersError = new GeneralError(message)
        break
    }
  } else if (SQLITE_UNIQUE_VIOLATION.has(error.code)) {
    feathersError = conflict(
      UNIQUE_VIOLATION,
      sqliteKeyColumns(message),
      options?.exposeConflictColumns,
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
