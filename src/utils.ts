import { sql } from 'kysely'
import type {
  OrderByItemBuilder,
  ExpressionBuilder,
  StringReference,
} from 'kysely'
import type { SortDirection, SortProperty } from './declarations.js'
import { Unprocessable } from '@feathersjs/errors'

export function applySelectId($select: string[] | undefined, idField: string) {
  if (!$select) return $select
  return $select.includes(idField) ? $select : $select.concat(idField)
}

export function traverseJSON<DB, TB extends keyof DB>(
  eb: ExpressionBuilder<DB, TB>,
  column: StringReference<DB, TB>,
  path: string[],
) {
  if (!path.length) {
    throw new Unprocessable('Path must have at least one element')
  }

  const accessor = path
    .slice(0, -1)
    .map((p) => `'${p}'`)
    .join('->')
  const finalKey = path[path.length - 1]

  if (accessor) {
    return sql`${sql.ref(column)}->${sql.raw(accessor)}->>'${sql.raw(finalKey)}'`
  }
  return sql`${sql.ref(column)}->>'${sql.raw(finalKey)}'`
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
