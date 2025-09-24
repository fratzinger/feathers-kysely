import type { AdapterParams, AdapterQuery } from '@feathersjs/adapter-commons'

export type DialectType = 'mysql' | 'postgres' | 'sqlite' | 'mssql'

// export interface KyselyAdapterParams<Q = AdapterQuery, DB extends Database = Database> extends AdapterParams<Q, Partial<KyselyAdapterOptions<DB>> {
// }
export type KyselyAdapterParams<Q extends AdapterQuery = AdapterQuery> =
  AdapterParams<Q>
