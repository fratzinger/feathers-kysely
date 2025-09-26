import type { OrderByItemBuilder } from 'kysely'
import type { SortProperty } from './declarations.js'

export function applySelectId($select: string[] | undefined, idField: string) {
  if (!$select) return $select
  return $select.includes(idField) ? $select : $select.concat(idField)
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

export function getOrderByModifier(order: SortProperty) {
  if (order === 1 || order === '-1' || order === 'asc') {
    return (ob: OrderByItemBuilder) => ob.asc()
  }
  if (order === -1 || order === '1' || order === 'desc') {
    return (ob: OrderByItemBuilder) => ob.desc()
  }
  if (order === 'asc nulls first') {
    return (ob: OrderByItemBuilder) => ob.asc().nullsFirst()
  }
  if (order === 'asc nulls last') {
    return (ob: OrderByItemBuilder) => ob.asc().nullsLast()
  }
  if (order === 'desc nulls first') {
    return (ob: OrderByItemBuilder) => ob.desc().nullsFirst()
  }
  if (order === 'desc nulls last') {
    return (ob: OrderByItemBuilder) => ob.desc().nullsLast()
  }
  return (ob: OrderByItemBuilder) => ob.asc()
}
