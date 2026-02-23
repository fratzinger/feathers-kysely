import type {
  AdapterParams,
  AdapterQuery,
  AdapterServiceOptions,
} from '@feathersjs/adapter-commons'
import type { ControlledTransaction, Kysely } from 'kysely'

export type DialectType = 'mysql' | 'postgres' | 'sqlite' | 'mssql'

type Relation = {
  service: string
  keyHere: string
  keyThere: string
  asArray: boolean
  databaseTableName?: string
}

export interface KyselyAdapterOptions extends AdapterServiceOptions {
  Model: Kysely<any>
  /**
   * The table name
   */
  name: string
  dialectType?: DialectType
  // TODO
  relations?: Record<string, Relation>
  // TODO
  properties?: Record<string, any>
  getPropertyType?: (
    property: string,
  ) => 'json' | 'jsonb' | (string & {}) | undefined
}

export interface KyselyAdapterTransaction {
  trx: ControlledTransaction<any>
  id?: number
  starting: boolean
  parent?: KyselyAdapterTransaction
  committed?: Promise<boolean | undefined>
  resolve?: (value: boolean) => void
}

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

export interface KyselyAdapterParams<Q extends AdapterQuery = AdapterQuery>
  extends AdapterParams<Q, Partial<KyselyAdapterOptions>> {
  transaction?: KyselyAdapterTransaction
}

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
