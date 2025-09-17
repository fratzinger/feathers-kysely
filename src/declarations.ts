import type { Kysely } from 'kysely'
import type {
  AdapterServiceOptions,
  AdapterParams,
  AdapterQuery,
} from '@feathersjs/adapter-commons'

export type DialectType = 'mysql' | 'postgres' | 'sqlite' | 'mssql'

export interface KyselyAdapterOptions extends AdapterServiceOptions {
  Model: Kysely<any>
  /**
   * The table name
   */
  name: string
  dialectType?: DialectType
}

// export interface KyselyAdapterParams<Q = AdapterQuery, DB extends Database = Database> extends AdapterParams<Q, Partial<KyselyAdapterOptions<DB>> {
// }
export type KyselyAdapterParams<Q extends AdapterQuery = AdapterQuery> =
  AdapterParams<Q>
