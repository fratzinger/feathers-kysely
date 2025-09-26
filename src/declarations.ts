import type { AdapterParams, AdapterQuery } from '@feathersjs/adapter-commons'

export type DialectType = 'mysql' | 'postgres' | 'sqlite' | 'mssql'

// export interface KyselyAdapterParams<Q = AdapterQuery, DB extends Database = Database> extends AdapterParams<Q, Partial<KyselyAdapterOptions<DB>> {
// }
export type KyselyAdapterParams<Q extends AdapterQuery = AdapterQuery> =
  AdapterParams<Q>

export type SortProperty =
  | 1
  | -1
  // eslint-disable-next-line prettier/prettier
  | "1"
  // eslint-disable-next-line prettier/prettier
  | "-1"
  | 'asc'
  | 'desc'
  | 'asc nulls first'
  | 'asc nulls last'
  | 'desc nulls first'
  | 'desc nulls last'

export type SortFilter = Record<string, SortProperty>

export type Filters = {
  $select?: string[] | undefined
  $sort?: SortFilter | undefined
  $limit?: number | undefined
  $skip?: number | undefined
}
