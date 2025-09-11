import type { Dialect } from 'kysely'
import { MysqlDialect, PostgresDialect, SqliteDialect } from 'kysely'
import { Pool } from 'pg'
import { createPool } from 'mysql2'
import Database from 'better-sqlite3'

export default (DB: 'postgres' | 'mysql' | 'sqlite' = 'sqlite'): Dialect => {
  DB ??= (process.env.DB as any) || 'sqlite'

  if (DB === 'postgres') {
    console.log('Using Postgres')
    return new PostgresDialect({
      pool: new Pool({
        host: 'localhost',
        user: process.env.POSTGRES_USER ?? 'postgres',
        password: process.env.POSTGRES_PASSWORD ?? 'password',
        database: process.env.POSTGRES_DB ?? 'sequelize',
        port: 5432,
        max: 10,
      }),
    })
  } else if (DB === 'mysql') {
    console.log('Using MySQL')
    return new MysqlDialect({
      pool: createPool({
        database: process.env.MYSQL_DATABASE ?? 'test',
        host: process.env.MYSQL_HOST ?? 'localhost',
        user: process.env.MYSQL_USER ?? 'root',
        password: process.env.MYSQL_PASSWORD ?? '',
        port: process.env.MYSQL_PORT ? Number(process.env.MYSQL_PORT) : 3306,
        connectionLimit: 10,
      }) as any,
    })
  }

  console.log('Using SQLite')
  return new SqliteDialect({
    database: new Database(':memory:'),
  })
}
