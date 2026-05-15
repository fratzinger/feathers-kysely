import type { CreateTableBuilder } from 'kysely'
import { sql } from 'kysely'
import { getDialect } from './dialect.js'

export function addPrimaryKey<
  T extends CreateTableBuilder<any>,
  C extends string,
>(b: T, col: C) {
  const dialect = getDialect()

  if (dialect === 'postgres') {
    return b.addColumn(col, 'serial', (col) => col.primaryKey())
  } else if (dialect === 'mysql') {
    return b.addColumn(col, 'integer', (col) =>
      col.primaryKey().autoIncrement(),
    )
  }

  return b.addColumn(col, 'integer', (col) => col.primaryKey().autoIncrement())
}

// postgres-only: native uuid column with gen_random_uuid() default (PG 13+)
export function addUuidPrimaryKey<
  T extends CreateTableBuilder<any>,
  C extends string,
>(b: T, col: C) {
  return b.addColumn(col, 'uuid', (c) =>
    c.primaryKey().defaultTo(sql`gen_random_uuid()`),
  )
}
