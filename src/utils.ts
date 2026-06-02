import { sql } from 'kysely'
import type { OrderByItemBuilder } from 'kysely'
import type {
  DialectType,
  SortDirection,
  SortProperty,
} from './declarations.js'
import { Unprocessable } from '@feathersjs/errors'

export function applySelectId($select: string[] | undefined, idField: string) {
  if (!$select) return $select
  return $select.includes(idField) ? $select : $select.concat(idField)
}

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

// Recognized temporal column kinds for opt-in date coercion. 'instant' covers
// timestamp/timestamptz/datetime columns; 'date' is a calendar day with no time.
export type TemporalKind = 'instant' | 'date'

/**
 * Map a `getPropertyType` return value to a temporal coercion kind, or
 * `undefined` when the type is not a recognized temporal type (so no coercion
 * happens). Matching is case-insensitive: anything containing "timestamp" or
 * equal to "datetime" is an instant; an exact "date" is a calendar day.
 */
export function temporalKind(type: unknown): TemporalKind | undefined {
  if (typeof type !== 'string') return undefined
  const t = type.toLowerCase()
  if (t === 'datetime' || t.includes('timestamp')) return 'instant'
  if (t === 'date') return 'date'
  return undefined
}

/**
 * Normalize a single query value for a temporal column into the canonical
 * string representation that every supported driver compares correctly: a full
 * ISO-8601 UTC string for an instant column, or a "YYYY-MM-DD" string for a date
 * column. Accepts a Date, an epoch-millisecond number, an ISO string, or a
 * "YYYY-MM-DD" string. Normalization is done in UTC. Values that cannot be
 * parsed into a valid date (including null) are returned unchanged.
 */
export function coerceTemporalValue(
  value: unknown,
  kind: TemporalKind,
): unknown {
  if (value == null) return value

  let date: Date
  if (value instanceof Date) {
    date = value
  } else if (typeof value === 'number' || typeof value === 'string') {
    date = new Date(value)
  } else {
    return value
  }

  if (Number.isNaN(date.getTime())) return value

  const iso = date.toISOString()
  return kind === 'date' ? iso.slice(0, 10) : iso
}

// Comparison operators whose values are temporal scalars (or arrays of them).
// Pattern operators ($like, …) and the Postgres array operators are excluded so
// their values are never reinterpreted as dates.
const TEMPORAL_OPERATORS = new Set([
  '$lt',
  '$lte',
  '$gt',
  '$gte',
  '$eq',
  '$ne',
  '$in',
  '$nin',
])

/**
 * Coerce the value side of a single column's query for a temporal column. The
 * input is either a bare value (`{ col: value }`) or an operator object
 * (`{ col: { $gt: value, $in: [...] } }`). Operator keys and non-temporal
 * operators are left untouched; only the leaf values of temporal comparison
 * operators (and bare equality) are normalized via `coerceTemporalValue`.
 */
export function coerceTemporalQueryProperty(
  queryProperty: any,
  kind: TemporalKind,
): any {
  // An operator object like { $gt: ..., $in: [...] }. A Date is an object too,
  // so treat only record-like objects as operator maps; a Date/array/scalar is
  // a bare value.
  if (
    queryProperty !== null &&
    typeof queryProperty === 'object' &&
    !(queryProperty instanceof Date) &&
    !Array.isArray(queryProperty)
  ) {
    const out: Record<string, any> = {}
    for (const operator in queryProperty) {
      const value = queryProperty[operator]
      if (!TEMPORAL_OPERATORS.has(operator)) {
        out[operator] = value
        continue
      }
      out[operator] = Array.isArray(value)
        ? value.map((v) => coerceTemporalValue(v, kind))
        : coerceTemporalValue(value, kind)
    }
    return out
  }

  // Bare value: { col: dateLike }
  return coerceTemporalValue(queryProperty, kind)
}

export function getSortDirection(order: SortProperty): SortDirection {
  if (typeof order === 'object' && order !== null && 'direction' in order) {
    return order.direction
  }
  return order
}

export function getOrderByModifier(order: SortProperty) {
  const dir = getSortDirection(order)
  if (dir === 1 || dir === '-1' || dir === 'asc') {
    return (ob: OrderByItemBuilder) => ob.asc()
  }
  if (dir === -1 || dir === '1' || dir === 'desc') {
    return (ob: OrderByItemBuilder) => ob.desc()
  }
  if (dir === 'asc nulls first') {
    return (ob: OrderByItemBuilder) => ob.asc().nullsFirst()
  }
  if (dir === 'asc nulls last') {
    return (ob: OrderByItemBuilder) => ob.asc().nullsLast()
  }
  if (dir === 'desc nulls first') {
    return (ob: OrderByItemBuilder) => ob.desc().nullsFirst()
  }
  if (dir === 'desc nulls last') {
    return (ob: OrderByItemBuilder) => ob.desc().nullsLast()
  }
  return (ob: OrderByItemBuilder) => ob.asc()
}
