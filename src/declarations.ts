import type { AdapterParams, AdapterQuery } from '@feathersjs/adapter-commons'

export type DialectType = 'mysql' | 'postgres' | 'sqlite' | 'mssql'

export interface UpsertOptions<T = any> {
  /**
   * Fields to use in the ON CONFLICT clause
   */
  onConflictFields: (keyof T)[]
  /**
   * Action to take on conflict: 'ignore' or 'merge'
   * - 'ignore': Do nothing on conflict (ON CONFLICT DO NOTHING)
   * - 'merge': Update the row on conflict (ON CONFLICT DO UPDATE)
   *
   * @default 'merge'
   */
  onConflictAction?: 'ignore' | 'merge'
  /**
   * Specific fields to merge on conflict (only used when onConflictAction is 'merge')
   * If not specified, all fields except the conflict fields will be merged
   */
  onConflictMergeFields?: (keyof T)[]
  /**
   * Fields to exclude from the merge (only used when onConflictAction is 'merge')
   * Takes precedence over onConflictMergeFields
   */
  onConflictExcludeFields?: (keyof T)[]
}

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
