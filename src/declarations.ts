import type {
  AdapterParams,
  AdapterQuery,
  AdapterServiceOptions,
} from '@feathersjs/adapter-commons'
import type { ControlledTransaction, Kysely } from 'kysely'

export type DialectType = 'mysql' | 'postgres' | 'sqlite'

export type Relation = {
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
  /**
   * Map of column name → JSON schema property object (typically the service's
   * schema `properties` block). Used as the set of known columns and as a
   * declarative source for the column's database type via an `x-db-type`
   * annotation, e.g. `{ createdAt: { type: 'string', 'x-db-type': 'timestamptz' } }`.
   * An explicit `getPropertyType` takes precedence over `x-db-type`.
   */
  properties?: Record<string, PropertySchema | true>
  /**
   * Resolve the database type of a column. Takes precedence over an
   * `x-db-type` annotation in `properties`; return `undefined` to fall back to
   * the annotation (or to no special handling).
   */
  getPropertyType?: (property: string) => DbPropertyType | undefined
}

/**
 * Database type of a column. `json`/`jsonb` enable dot-notation traversal into
 * JSON columns; the temporal types enable opt-in, type-aware date coercion of
 * query values (Date / ISO string / epoch-ms / "YYYY-MM-DD") on the column.
 */
export type DbPropertyType =
  | 'json'
  | 'jsonb'
  | 'date'
  | 'timestamp'
  | 'timestamptz'
  | 'datetime'
  | (string & {})

/**
 * A JSON schema property object. Arbitrary keywords are allowed; `x-db-type`
 * is read by the adapter to determine the column's database type.
 */
export type PropertySchema = {
  'x-db-type'?: DbPropertyType
  [key: string]: any
}

export interface KyselyAdapterTransaction {
  trx: ControlledTransaction<any>
  id?: number
  starting: boolean
  parent?: KyselyAdapterTransaction
  /**
   * Name of the savepoint backing a nested transaction. Set only for nested
   * transactions; root transactions commit/rollback the whole transaction.
   */
  savepoint?: string
  committed?: Promise<boolean | undefined>
  resolve?: (value: boolean) => void
  /**
   * Root-scoped queue of deferred Feathers event emitters. Populated by the
   * `withTransaction()` around hook; flushed on root commit, discarded on root
   * rollback. Nested transactions share the root's array. Undefined for legacy
   * `trxStart()` transactions (no deferral).
   */
  deferredEvents?: Array<() => void>
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

export type SortDirection =
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

export type SortProperty =
  | SortDirection
  | { direction: SortDirection; filter?: Record<string, any> }

export type SortFilter = Record<string, SortProperty>

export type Filters = {
  $select?: string[] | undefined
  $sort?: SortFilter | undefined
  $limit?: number | undefined
  $skip?: number | undefined
}
