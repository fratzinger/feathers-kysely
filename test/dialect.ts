import type { Dialect } from 'kysely'
import { MysqlDialect, PostgresDialect, SqliteDialect } from 'kysely'
import type { PoolConfig } from 'pg'
import { Pool } from 'pg'
import type { PoolOptions } from 'mysql2'
import { createPool } from 'mysql2'
import Database from 'better-sqlite3'

const createdDialects: Record<string, Dialect> = {}

const dialectValues = ['postgres', 'mysql', 'sqlite'] as const
export type DialectType = (typeof dialectValues)[number]

export const getDialect = (): DialectType => {
  if (!process.env.DB) return 'sqlite'
  if (dialectValues.includes(process.env.DB as any))
    return process.env.DB as DialectType

  return 'sqlite'
}

export default (): Dialect => {
  const DB = getDialect()

  if (createdDialects[DB]) {
    return createdDialects[DB]
  }

  if (DB === 'postgres') {
    const config: PoolConfig = {
      host: 'localhost',
      user: process.env.POSTGRES_USER ?? 'postgres',
      password:
        'POSTGRES_PASSWORD' in process.env
          ? process.env.POSTGRES_PASSWORD
          : 'password',
      database: process.env.POSTGRES_DB ?? 'test',
      port: process.env.POSTGRES_PORT
        ? Number(process.env.POSTGRES_PORT)
        : 5432,
      max: 10,
    }

    createdDialects[DB] = new PostgresDialect({
      pool: new Pool(config),
    })
  } else if (DB === 'mysql') {
    const config: PoolOptions = {
      database: process.env.MYSQL_DATABASE ?? 'test',
      host: process.env.MYSQL_HOST ?? 'localhost',
      user: process.env.MYSQL_USER ?? 'root',
      password:
        'MYSQL_PASSWORD' in process.env ? process.env.MYSQL_PASSWORD : '',
      port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : 3306,
      connectionLimit: 10,
    }

    createdDialects[DB] = new MysqlDialect({
      pool: createPool(config) as any,
    })
  } else {
    createdDialects[DB] = new SqliteDialect({
      database: new Database(':memory:'),
    })
  }

  return createdDialects[DB]
}
