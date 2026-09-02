import type {
  Id,
  NullableId,
  Paginated,
  PaginationParams,
  Params,
  Query,
} from '@feathersjs/feathers'
import { _ } from '@feathersjs/commons'
import type {
  PaginationOptions,
  AdapterQuery,
} from '@feathersjs/adapter-commons'
import { AdapterBase, getLimit } from '@feathersjs/adapter-commons'
import {
  BadRequest,
  GeneralError,
  MethodNotAllowed,
  NotFound,
} from '@feathersjs/errors'

import { errorHandler } from './error-handler.js'
import { RelationQuery } from './relation-query.js'
import type {
  DialectType,
  KyselyAdapterOptions,
  KyselyAdapterParams,
  KyselyParams,
  UpsertOptions,
} from './declarations.js'
import { sql } from 'kysely'
import type {
  SelectExpression,
  DeleteQueryBuilder,
  InsertQueryBuilder,
  Kysely,
  SelectQueryBuilder,
  UpdateQueryBuilder,
} from 'kysely'
import {
  ALL_DIALECTS_OPERATORS,
  applySelectId,
  convertBooleansToNumbers,
  getDatabaseDialect,
  NON_SQLITE_OPERATORS,
  OPERATORS,
  POSTGRES_ONLY_JSON_OPERATORS,
  POSTGRES_ONLY_OPERATORS,
  qualifyColumn,
} from './utils/index.js'
import { addToQuery } from 'feathers-utils'

// Alias for the window-count column injected into the data query so a paginated
// find can return rows and the grand total in a single round-trip. Stripped
// from every row before the result is returned.
const PAGINATION_TOTAL_KEY = '__fk_total'

type KyselyAdapterOptionsDefined = KyselyAdapterOptions & {
  id: string
  dialectType: DialectType
}

type FilterQueryResult = {
  paginate: PaginationParams | undefined
  filters: Filters
  query: Query
  params: Params
  options: KyselyAdapterOptionsDefined
}

type SortFilter = Record<
  string,
  | 1
  | -1
  | 'asc'
  | 'desc'
  | 'asc nulls first'
  | 'asc nulls last'
  | 'desc nulls first'
  | 'desc nulls last'
>

type Filters = {
  $select?: string[] | undefined
  $sort?: SortFilter | undefined
  $limit?: number | undefined
  $skip?: number | undefined
}

export class KyselyAdapter<
  Result extends Record<string, any>,
  Data = Partial<Result>,
  ServiceParams extends KyselyAdapterParams<any> = KyselyAdapterParams,
  PatchData = Partial<Data>,
> extends AdapterBase<
  Result,
  Data,
  PatchData,
  ServiceParams,
  KyselyAdapterOptions
> {
  declare options: KyselyAdapterOptionsDefined

  private propertyMap: Map<string, any>

  /**
   * Per-instance `.catch()` handler converting database errors into Feathers
   * errors. Passes the known columns (`properties`) so the client-facing
   * Postgres message keeps declared column names but strips other identifiers.
   */
  private handleError = (error: any): never =>
    errorHandler(error, this.propertyMap)

  declare app: any

  constructor(options: KyselyAdapterOptions, app?: any) {
    if (!options || !options.Model) {
      throw new Error(
        'You must provide a Kysely instance to the `Model` option',
      )
    }

    if (typeof options.name !== 'string') {
      throw new Error('No table name specified.')
    }

    const dialectType = options.dialectType ?? getDatabaseDialect(options.Model)

    super({
      id: 'id',
      ...options,
      filters: {
        ...options.filters,
        $and: (value: any) => value,
        $not: (value: any) => value,
      },
      operators: [
        ...new Set([
          ...(options.operators ?? []),
          // Don't register Postgres-only operators on other dialects so Feathers
          // rejects them with a BadRequest instead of producing invalid SQL.
          ...Object.keys(OPERATORS).filter(
            (op) =>
              dialectType === 'postgres' ||
              !POSTGRES_ONLY_OPERATORS.includes(op),
          ),
          // Branch operators (handled in buildPropertyExpression), registered per
          // dialect support so unsupported ones are rejected with a BadRequest.
          ...ALL_DIALECTS_OPERATORS,
          ...(dialectType !== 'sqlite' ? NON_SQLITE_OPERATORS : []),
          ...(dialectType === 'postgres' ? POSTGRES_ONLY_JSON_OPERATORS : []),
          '$none',
          '$some',
          '$every',
          '$not',
        ]),
      ],
    })

    this.options.dialectType ??= dialectType
    this.propertyMap = new Map<string, any>(
      Object.entries(options.properties || {}),
    )

    if (app) {
      this.app = app
    }
  }

  async setup(app: any, _path: string) {
    this.app ??= app
  }

  get Model() {
    return this.getModel()
  }

  getOptions(params: ServiceParams): KyselyAdapterOptionsDefined {
    return super.getOptions(params) as KyselyAdapterOptionsDefined
  }

  getModel(params: ServiceParams = {} as ServiceParams) {
    const { Model } = this.getOptions(params)
    return Model
  }

  db(params: ServiceParams = {} as ServiceParams): Kysely<any> {
    const transaction = params.transaction
    if (transaction?.trx) {
      return transaction.trx
    }
    return this.getModel(params)
  }

  filterQuery(params: ServiceParams, id?: NullableId): FilterQueryResult {
    const options = this.getOptions(params)

    params =
      id == null
        ? params
        : { ...params, query: addToQuery(params.query, { [options.id]: id }) }

    params = { ...params, query: this.convertValues(params.query) }

    const {
      $select: _select,
      $sort,
      $limit: _limit,
      $skip: _skip = 0,
      ...query
    } = (params.query || {}) as AdapterQuery

    // A negative $skip is invalid; floor it to 0 so it never reaches OFFSET.
    const $skip = typeof _skip === 'number' && _skip > 0 ? _skip : 0

    // getLimit only clamps the upper bound — floor negative client-supplied
    // limits to 0 (a negative LIMIT errors on Postgres/MySQL). The sqlite/mysql
    // "no limit" sentinels below are only reached when no limit was given.
    const baseLimit = getLimit(_limit, options.paginate)
    const clampedLimit =
      typeof baseLimit === 'number' && baseLimit < 0 ? 0 : baseLimit

    const $limit = $skip
      ? (clampedLimit ??
        (options.dialectType === 'sqlite'
          ? -1
          : options.dialectType === 'mysql'
            ? 4294967295 /** max value for mysql */
            : undefined))
      : clampedLimit

    const $select = applySelectId(_select, options.id)

    return {
      paginate: options.paginate,
      filters: {
        $select,
        $sort,
        $limit,
        $skip,
      },
      query,
      options,
      params,
    }
  }

  composeQuery(
    params: ServiceParams,
    options?: {
      id?: NullableId
      select?: boolean | SelectExpression<any, any>[]
      where?: boolean
      limit?: boolean | number
      offset?: boolean | number
      order?: boolean
    },
  ) {
    const filterQueryResult = this.filterQuery(params, options?.id)
    const filters = filterQueryResult.filters
    const relations = this.relations(params)

    let q: any = this.db(params).selectFrom(this.options.name)

    if (options?.select) {
      const $select = Array.isArray(options.select)
        ? options.select
        : filters.$select

      const select =
        $select && Array.isArray($select) ? this.col($select) : $select

      q = select ? q.select(select) : q.selectAll(this.options.name)
    }

    if (options?.where) {
      q = relations.applyWhere(q, filterQueryResult.params.query)
    }

    if (options?.limit) {
      const limit =
        typeof options.limit === 'number' ? options.limit : filters.$limit
      q = limit ? q.limit(limit) : q
    }

    if (options?.offset) {
      const skip =
        typeof options.offset === 'number' ? options.offset : filters.$skip
      q = skip ? q.offset(skip) : q
    }

    if (options?.order) {
      q = relations.applyOrder(q, filters.$sort)

      // When a result window (LIMIT/OFFSET) is in effect but the caller gave no
      // $sort, append the primary key as a deterministic tiebreaker. Without it,
      // OFFSET pagination can return overlapping or missing rows across pages.
      const hasSort = !!filters.$sort && Object.keys(filters.$sort).length > 0
      const windowed =
        (typeof filters.$limit === 'number' && filters.$limit > 0) ||
        (typeof filters.$skip === 'number' && filters.$skip > 0)
      if (!hasSort && windowed) {
        q = (q as any).orderBy(this.col(this.options.id), 'asc')
      }
    }

    return q
  }

  /**
   * Resolve the database type of a column, used for JSON traversal and opt-in
   * temporal date coercion. An explicit `getPropertyType` option wins; when it
   * is absent (or returns `undefined`) we fall back to an `x-db-type`
   * annotation on the column's entry in `properties` (typically the service's
   * JSON schema `properties` block).
   */
  getPropertyType(property: string): string | undefined {
    const explicit = this.options.getPropertyType?.(property)
    if (explicit != null) return explicit

    const meta = this.propertyMap.get(property)
    if (meta && typeof meta === 'object') {
      const annotated = (meta as Record<string, any>)['x-db-type']
      if (typeof annotated === 'string') return annotated
    }

    return undefined
  }

  /** `qualifyColumn` bound to this service's table and declared columns. */
  private col<T>(
    column: T,
    options?: { tableName: string | null | undefined },
  ): T {
    return qualifyColumn(column, {
      tableName: options?.tableName,
      ownTable: this.options.name,
      isOwnColumn: (name) => this.propertyMap.has(name),
    })
  }

  /**
   * The relation-aware query compiler for this call. Built per call because
   * `params.adapter` may override any option, `relations` and `name` included.
   */
  private relations(params?: ServiceParams): RelationQuery {
    const options = params ? this.getOptions(params) : this.options

    return new RelationQuery({
      tableName: options.name,
      idField: options.id,
      dialectType: options.dialectType,
      relations: options.relations,
      isOwnColumn: (name) => this.propertyMap.has(name),
      getPropertyType: (property) => this.getPropertyType(property),
      lookupService: (name) => {
        if (!this.app) return undefined
        try {
          return this.app.service(name)?.options
        } catch {
          return undefined
        }
      },
    })
  }

  applyWhere<Q extends Record<string, any>>(
    q: Q,
    query: Query,
    params?: ServiceParams,
  ) {
    return this.relations(params).applyWhere(q, query)
  }

  applyReturning<
    Q extends
      | InsertQueryBuilder<any, any, any>
      | UpdateQueryBuilder<any, any, any, any>
      | DeleteQueryBuilder<any, any, any>,
  >(q: Q, $select: string[] | undefined): Q {
    return this.options.dialectType !== 'mysql'
      ? $select
        ? (q as any).returning($select.map((item) => this.col(item)))
        : (q as any).returningAll()
      : q
  }

  /**
   * Whether the caller opted out of a return value via
   * `params.kysely.returning === false`. See {@link KyselyParams.returning}.
   */
  private wantsNoReturn(params: ServiceParams): boolean {
    return (params as KyselyAdapterParams).kysely?.returning === false
  }

  /**
   * Did an INSERT/UPDATE/DELETE executed without RETURNING touch at least one
   * row? Reads the driver's affected-row count (`numUpdatedRows`,
   * `numDeletedRows`, `numAffectedRows`, `numInsertedOrUpdatedRows`) so a single
   * mutation with `returning: false` can still throw `NotFound` when it matched
   * nothing. Not used on MySQL, which verifies existence via a pre-fetch.
   */
  private didAffectRow(result: any): boolean {
    if (!result) return false
    const count =
      result.numUpdatedRows ??
      result.numDeletedRows ??
      result.numAffectedRows ??
      result.numInsertedOrUpdatedRows
    return count != null && Number(count) > 0
  }

  private convertValues<T>(data: T): T {
    if (this.options.dialectType !== 'sqlite') {
      return data
    }

    // see https://github.com/WiseLibs/better-sqlite3/issues/907
    return convertBooleansToNumbers(data)
  }

  /**
   * Retrieve records matching the query
   * See https://kysely-org.github.io/kysely/classes/SelectQueryBuilder.html
   * @param params
   */
  async _find(
    params?: ServiceParams & { paginate?: PaginationOptions },
  ): Promise<Paginated<Result>>
  async _find(params?: ServiceParams & { paginate: false }): Promise<Result[]>
  async _find(params?: ServiceParams): Promise<Paginated<Result> | Result[]>
  async _find(
    params: ServiceParams = {} as ServiceParams,
  ): Promise<Paginated<Result> | Result[]> {
    const { filters, paginate } = this.filterQuery(params)
    const q = this.composeQuery(params, {
      select: true,
      where: true,
      limit: true,
      offset: true,
      order: true,
    })

    if (paginate && paginate.default) {
      const runCountQuery = () =>
        this.composeQuery(params, {
          select: [
            this.db(params).fn.count(this.col(this.options.id)).as('total'),
          ],
          where: true,
        })
          .executeTakeFirst()
          .catch(this.handleError)

      const buildResult = (total: any, data: Result[]): Paginated<Result> => ({
        total: Number((total as any)?.total ?? total ?? 0) || 0,
        limit: filters.$limit!,
        skip: filters.$skip || 0,
        data,
      })

      // Count-only request ($limit === 0): skip the data query entirely.
      if (filters.$limit === 0) {
        return buildResult(await runCountQuery(), [])
      }

      const { dialectType } = this.options

      // Postgres & SQLite: fetch the rows and the grand total in a single
      // round-trip via a window count. Window functions are evaluated over the
      // full filtered set before LIMIT/OFFSET, so the total is correct even when
      // a page is requested. Fall back to a separate count only when the page is
      // empty (e.g. $skip past the end), where no row carries the total.
      if (dialectType === 'postgres' || dialectType === 'sqlite') {
        const rows = (await (q as any)
          .select(sql`count(*) over()`.as(PAGINATION_TOTAL_KEY))
          .execute()
          .catch(this.handleError)) as any[]

        if (rows.length > 0) {
          const total = Number(rows[0][PAGINATION_TOTAL_KEY] ?? 0) || 0
          for (const row of rows) {
            delete row[PAGINATION_TOTAL_KEY]
          }
          return buildResult(total, rows as Result[])
        }

        return buildResult(await runCountQuery(), [])
      }

      // Other dialects: run the data and count queries in parallel.
      const [queryResult, countQueryResult] = await Promise.all([
        q.execute().catch(this.handleError),
        runCountQuery(),
      ])

      return buildResult(countQueryResult, queryResult as Result[])
    }

    const data =
      filters.$limit === 0 ? [] : await q.execute().catch(this.handleError)
    return data as Result[]
  }

  /**
   * Retrieve a single record by id
   * See https://kysely-org.github.io/kysely/classes/SelectQueryBuilder.html
   */
  async _get(
    id: Id,
    params: ServiceParams = {} as ServiceParams,
  ): Promise<Result> {
    const q = this.composeQuery(params, {
      id,
      select: true,
      limit: 1,
      where: true,
    })

    const item = await q.executeTakeFirst().catch(this.handleError)

    if (!item)
      throw new NotFound(`No record found for ${this.options.id} '${id}'`)

    return item as Result
  }

  /**
   * Build a SELECT over the service table, honoring `$select` (falling back
   * to selecting all columns of the table).
   */
  private selectFromTable(
    params: ServiceParams,
    name: string,
    $select?: string[],
  ): SelectQueryBuilder<any, any, any> {
    const from = this.db(params).selectFrom(name)
    const select =
      $select && Array.isArray($select) ? this.col($select) : $select
    return select ? from.select(select) : from.selectAll(name)
  }

  private async executeAndReturn<
    Q extends
      | InsertQueryBuilder<any, any, any>
      | UpdateQueryBuilder<any, any, any, any>,
  >(
    q: Q,
    context: {
      isArray: boolean
      options: KyselyAdapterOptionsDefined
      params: ServiceParams
      $select?: string[]
      /**
       * The original input data, used (MySQL only) to recover explicitly
       * supplied primary keys when re-fetching the written rows.
       */
      data?: any
      buildWhere?: (
        query: SelectQueryBuilder<any, any, any>,
      ) => SelectQueryBuilder<any, any, any>
    },
  ) {
    const { isArray, options, $select, params } = context
    const { id: idField, name, dialectType } = options

    const response = await (isArray && dialectType !== 'mysql'
      ? q.execute().catch(this.handleError)
      : q.executeTakeFirst().catch(this.handleError))

    if (dialectType !== 'mysql') {
      return response
    }

    // mysql only

    const selected = this.selectFromTable(params, name, $select)

    // If a custom WHERE builder is provided, use it
    if (context.buildWhere) {
      const query = context.buildWhere(selected)
      return isArray
        ? query.execute().catch(this.handleError)
        : query.executeTakeFirst().catch(this.handleError)
    }

    // Standard insert logic: figure out which rows to re-fetch. MySQL has no
    // RETURNING, so we identify the written rows by their primary key.
    const rows: any[] = isArray
      ? Array.isArray(context.data)
        ? context.data
        : []
      : context.data != null
        ? [context.data]
        : []

    const suppliedIds = rows
      .map((row) => (row == null ? undefined : row[idField]))
      .filter((value) => value !== undefined && value !== null)

    let ids: any[]
    if (suppliedIds.length > 0 && suppliedIds.length === rows.length) {
      // Every inserted row carried an explicit primary key (e.g. UUID or
      // application-assigned id) — re-fetch by those, never by guessing.
      ids = suppliedIds
    } else {
      // Fall back to MySQL's auto-increment block, which starts at insertId and
      // is contiguous for a single multi-row INSERT. Guard against a missing /
      // non-numeric insertId (e.g. a non-auto-increment key with no value).
      const { insertId, numInsertedOrUpdatedRows } = response as any
      const firstId = Number(insertId)
      const count = Number(numInsertedOrUpdatedRows ?? 1)

      if (
        !Number.isFinite(firstId) ||
        firstId <= 0 ||
        !Number.isFinite(count) ||
        count <= 0
      ) {
        throw new GeneralError(
          'Unable to determine the id(s) of the inserted MySQL row(s). ' +
            'Provide an explicit id in the data, or use a dialect that supports RETURNING.',
        )
      }

      ids = isArray
        ? Array.from({ length: count }, (_, i) => firstId + i)
        : [firstId]
    }

    const where =
      ids.length === 1
        ? selected.where(this.col(idField), '=', ids[0])
        : selected.where(this.col(idField), 'in', ids)

    return isArray
      ? where.execute().catch(this.handleError)
      : where.executeTakeFirst().catch(this.handleError)
  }

  /**
   * Build WHERE clause for fetching records by conflict fields
   */
  private buildWhereForConflictFields(
    selected: SelectQueryBuilder<any, any, any>,
    data: Data | Data[],
    conflictFields: (keyof Result)[],
    isArray: boolean,
  ): SelectQueryBuilder<any, any, any> {
    const dataArray = isArray ? (data as Data[]) : [data as Data]

    // Build OR conditions for each data item
    return selected.where((eb) =>
      eb.or(
        dataArray.map((item) =>
          eb.and(
            conflictFields.map((field) =>
              eb(this.col(field as string), '=', item[field as keyof Data]),
            ),
          ),
        ),
      ),
    )
  }

  /**
   * Apply upsert conflict resolution for MySQL using ON DUPLICATE KEY UPDATE
   */
  private applyMySqlUpsertConflict(
    query: InsertQueryBuilder<any, any, any>,
    options: {
      onConflictAction: 'ignore' | 'merge'
      onConflictFields: (keyof Result)[]
      onConflictMergeFields?: (keyof Result)[]
      onConflictExcludeFields: (keyof Result)[]
      data: Data | Data[]
      isArray: boolean
    },
  ): InsertQueryBuilder<any, any, any> {
    const { id: idField } = this.options

    const {
      onConflictAction,
      onConflictFields,
      onConflictMergeFields,
      onConflictExcludeFields,
      data,
      isArray,
    } = options

    if (onConflictAction === 'ignore') {
      // For ignore in MySQL, use a dummy update (set id = id) which doesn't change anything
      return query.onDuplicateKeyUpdate({
        [idField]: sql.ref(idField),
      })
    }

    // onConflictAction === 'merge'
    const fieldsToUpdate = this.getFieldsToUpdate({
      data,
      isArray,
      onConflictFields,
      onConflictMergeFields,
      onConflictExcludeFields,
    })

    if (fieldsToUpdate.length === 0) {
      // No fields to update, but we still need ON DUPLICATE KEY UPDATE
      // to prevent errors. Use a dummy update (id = id)
      return query.onDuplicateKeyUpdate({
        [idField]: sql.ref(idField),
      })
    }

    // Build the update set using VALUES() function for MySQL
    const updateObject = fieldsToUpdate.reduce(
      (acc, field) => {
        // In MySQL, we reference the new values using VALUES(column_name)
        // Don't use this.col() here as it might add table prefix which VALUES() doesn't support
        acc[field] = sql`VALUES(${sql.ref(field)})`
        return acc
      },
      {} as Record<string, any>,
    )

    return query.onDuplicateKeyUpdate(updateObject)
  }

  /**
   * Apply upsert conflict resolution for PostgreSQL/SQLite using ON CONFLICT
   */
  private applyPostgresUpsertConflict(
    query: InsertQueryBuilder<any, any, any>,
    options: {
      onConflictAction: 'ignore' | 'merge'
      onConflictFields: (keyof Result)[]
      onConflictMergeFields?: (keyof Result)[]
      onConflictExcludeFields: (keyof Result)[]
      data: Data | Data[]
      isArray: boolean
      name: string
      /**
       * Only write rows whose merge fields actually differ
       * (`DO UPDATE ... WHERE ... IS DISTINCT FROM ...`), so no-op merges are
       * skipped entirely and RETURNING omits them.
       */
      onlyChanged?: boolean
    },
  ): InsertQueryBuilder<any, any, any> {
    const {
      onConflictAction,
      onConflictFields,
      onConflictMergeFields,
      onConflictExcludeFields,
      data,
      isArray,
      name,
      onlyChanged,
    } = options

    if (onConflictAction === 'ignore') {
      return query.onConflict((oc) =>
        oc.columns(onConflictFields as string[]).doNothing(),
      )
    }

    // onConflictAction === 'merge'
    return query.onConflict((oc) => {
      const conflict = oc.columns(onConflictFields as string[])

      const fieldsToUpdate = this.getFieldsToUpdate({
        data,
        isArray,
        onConflictFields,
        onConflictMergeFields,
        onConflictExcludeFields,
      })

      if (fieldsToUpdate.length === 0) {
        return conflict.doNothing()
      }

      const updateObject = fieldsToUpdate.reduce(
        (acc, field) => {
          acc[field] = sql.ref(`excluded.${field}`)
          return acc
        },
        {} as Record<string, any>,
      )

      const updated = conflict.doUpdateSet(updateObject)

      if (!onlyChanged) {
        return updated
      }

      return updated.where((eb) =>
        eb.or(
          fieldsToUpdate.map((field) =>
            eb(
              sql.ref(`${name}.${field}`),
              'is distinct from',
              sql.ref(`excluded.${field}`),
            ),
          ),
        ),
      )
    })
  }

  /**
   * Determine which fields should be updated during an upsert
   */
  private getFieldsToUpdate(options: {
    data: Data | Data[]
    isArray: boolean
    onConflictFields: (keyof Result)[]
    onConflictMergeFields?: (keyof Result)[]
    onConflictExcludeFields: (keyof Result)[]
  }): string[] {
    const { id: idField } = this.options
    const {
      data,
      isArray,
      onConflictFields,
      onConflictMergeFields,
      onConflictExcludeFields,
    } = options

    if (onConflictMergeFields !== undefined) {
      // Explicitly specified merge fields (even if empty array)
      return onConflictMergeFields
        .filter(
          (field) =>
            !onConflictExcludeFields.includes(field) &&
            !onConflictFields.includes(field),
        )
        .map((field) => field as string)
    }

    // Use all fields from data except id, conflict fields, and excluded fields
    const dataKeys = isArray
      ? Object.keys((data as Data[])[0] || {})
      : Object.keys(data as Record<string, any>)

    return dataKeys.filter(
      (key) =>
        key !== idField &&
        !onConflictFields.includes(key as any) &&
        !onConflictExcludeFields.includes(key as any),
    )
  }

  /**
   * Create a single record
   * See https://kysely-org.github.io/kysely/classes/InsertQueryBuilder.html
   * @param data
   * @param params
   */
  async _create(data: Data, params?: ServiceParams): Promise<Result>
  async _create(data: Data[], params?: ServiceParams): Promise<Result[]>
  async _create(
    data: Data | Data[],
    _params?: ServiceParams,
  ): Promise<Result | Result[]>
  async _create(
    _data: Data | Data[],
    params: ServiceParams = {} as ServiceParams,
  ): Promise<Result | Result[]> {
    const { filters, options } = this.filterQuery(params)
    const { name, id: idField, dialectType } = options
    const isArray = Array.isArray(_data)

    if (isArray && _data.length === 0) {
      return []
    }

    const $select = applySelectId(filters.$select, idField)

    const {
      onConflictFields = [],
      onConflictAction = 'ignore',
      onConflictMergeFields,
      onConflictExcludeFields = [],
      onConflictReturning = 'all',
      returning: wantReturn = true,
    } = (params as { kysely?: KyselyParams<Result> }).kysely ?? {}

    const hasConflictHandling = onConflictFields.length > 0
    // `returning: false` forces the no-return path, overriding onConflictReturning.
    const returningMode = !wantReturn
      ? 'none'
      : hasConflictHandling
        ? onConflictReturning
        : 'all'

    // With 'ignore' (or a merge with zero fields to update) a conflicting row
    // is not written, so RETURNING omits it.
    const fieldsToUpdate =
      onConflictAction === 'merge'
        ? this.getFieldsToUpdate({
            data: _data,
            isArray,
            onConflictFields,
            onConflictMergeFields,
            onConflictExcludeFields,
          })
        : []

    const effectivelyIgnored =
      onConflictAction === 'ignore' ||
      (onConflictAction === 'merge' && fieldsToUpdate.length === 0)

    let q = this.db(params)
      .insertInto(name)
      .values(this.convertValues(_data) as any)

    // Apply conflict resolution based on database dialect (upsert via create)
    if (hasConflictHandling) {
      const upsertOptions = {
        onConflictAction,
        onConflictFields,
        onConflictMergeFields,
        onConflictExcludeFields,
        data: _data,
        isArray,
      }

      q =
        dialectType === 'mysql'
          ? this.applyMySqlUpsertConflict(q, upsertOptions)
          : this.applyPostgresUpsertConflict(q, {
              ...upsertOptions,
              name,
              onlyChanged: returningMode === 'changed',
            })
    }

    if (returningMode === 'none') {
      // Nothing to return: skip RETURNING and every post-fetch.
      await (isArray
        ? q.execute().catch(this.handleError)
        : q.executeTakeFirst().catch(this.handleError))
      return (isArray ? [] : undefined) as unknown as Result | Result[]
    }

    const returning = this.applyReturning(q, $select)

    // MySQL has no RETURNING: to return only written rows we have to know
    // beforehand which conflict keys already exist (the rows the INSERT is
    // going to ignore or merge). NOTE: `affectedRows` alone cannot detect an
    // ignored conflict — with the CLIENT_FOUND_ROWS flag (mysql2's default) a
    // no-op ON DUPLICATE KEY UPDATE reports 1, exactly like a fresh insert.
    let freshItems: Data[] | undefined
    if (
      dialectType === 'mysql' &&
      returningMode !== 'all' &&
      (effectivelyIgnored || (returningMode === 'changed' && !isArray))
    ) {
      const existingKeyRows = (await this.buildWhereForConflictFields(
        this.db(params)
          .selectFrom(name)
          .select(this.col(onConflictFields as string[]) as string[]),
        _data,
        onConflictFields,
        isArray,
      )
        .execute()
        .catch(this.handleError)) as any[]

      const keyOf = (row: any) =>
        JSON.stringify(onConflictFields.map((field) => row[field]))
      const existingKeys = new Set(existingKeyRows.map(keyOf))

      if (isArray) {
        freshItems = (_data as Data[]).filter(
          (item) => !existingKeys.has(keyOf(item)),
        )

        if (freshItems.length === 0) {
          // Every row conflicts — run the (no-op) INSERT, skip the re-fetch.
          await returning.execute().catch(this.handleError)
          return []
        }
      } else if (existingKeys.has(keyOf(_data))) {
        // Single create on a pre-existing row.
        const insertResult = await returning
          .executeTakeFirst()
          .catch(this.handleError)

        if (effectivelyIgnored) {
          return undefined as unknown as Result
        }

        // merge + 'changed': MySQL reports 2 affected rows for a real update
        // (a no-op reports 1 with CLIENT_FOUND_ROWS, 0 without).
        const affected = Number(
          (insertResult as any)?.numInsertedOrUpdatedRows ?? 0,
        )
        if (affected !== 2) {
          return undefined as unknown as Result
        }

        return (await this.buildWhereForConflictFields(
          this.selectFromTable(params, name, $select),
          _data,
          onConflictFields,
          false,
        )
          .executeTakeFirst()
          .catch(this.handleError)) as Result
      }
    }

    const response = await this.executeAndReturn(returning, {
      isArray,
      options,
      params,
      $select,
      data: _data,
      buildWhere:
        dialectType === 'mysql' && hasConflictHandling
          ? (selected) =>
              this.buildWhereForConflictFields(
                selected,
                isArray ? (freshItems ?? _data) : _data,
                onConflictFields,
                isArray,
              )
          : undefined,
    })

    if (effectivelyIgnored && hasConflictHandling && returningMode === 'all') {
      if (dialectType === 'mysql') {
        // For MySQL, executeAndReturn already handled fetching based on conflict fields
        return response
      }

      // PostgreSQL and SQLite: rows whose conflict was ignored are missing
      // from RETURNING — fetch them and merge them into the response.
      return this.fetchIgnoredConflictRows({
        response,
        data: _data,
        isArray,
        onConflictFields,
        $select,
        params,
        name,
      })
    }

    return response
  }

  /**
   * PostgreSQL/SQLite only: with `ON CONFLICT DO NOTHING`, RETURNING omits
   * rows whose conflict was ignored. Fetch those existing rows and merge them
   * into the response, following the input order.
   */
  private async fetchIgnoredConflictRows(args: {
    response: Result | Result[] | undefined
    data: Data | Data[]
    isArray: boolean
    onConflictFields: (keyof Result)[]
    $select?: string[]
    params: ServiceParams
    name: string
  }): Promise<Result | Result[]> {
    const { response, data, isArray, onConflictFields, $select, params, name } =
      args

    if (isArray) {
      // For arrays, some records might have been inserted and some ignored
      const responseArray = (response || []) as Result[]
      const dataArray = data as Data[]

      // Find which records were not inserted by comparing with input data
      if (responseArray.length < dataArray.length) {
        const selected = this.selectFromTable(params, name, $select)

        const matchesConflict = (row: any, item: any) =>
          onConflictFields.every(
            (field) =>
              row[field as keyof Result] === (item[field as keyof Data] as any),
          )

        // Items that were ignored (already existed) and thus not returned.
        const missingItems = dataArray.filter(
          (item) => !responseArray.some((r) => matchesConflict(r, item)),
        )

        if (missingItems.length === 0) {
          return responseArray
        }

        // Fetch all missing rows in a SINGLE round-trip (one OR-of-ANDs
        // SELECT) instead of one SELECT per item.
        const existingRows = (await this.buildWhereForConflictFields(
          selected,
          missingItems,
          onConflictFields,
          true,
        )
          .execute()
          .catch(this.handleError)) as Result[]

        // Re-order to follow the input order and drop any not found.
        const missingRecords = missingItems
          .map((item) => existingRows.find((row) => matchesConflict(row, item)))
          .filter((row): row is Result => !!row)

        return [...responseArray, ...missingRecords] as Result[]
      }
    } else if (!response) {
      // For single record, if response is undefined/null, fetch the existing record
      let query = this.selectFromTable(params, name, $select)
      for (const field of onConflictFields) {
        query = query.where(
          this.col(field as string),
          '=',
          (data as Data)[field as keyof Data],
        ) as any
      }

      const existing = await query.executeTakeFirst().catch(this.handleError)
      return existing as Result
    }

    return response as Result | Result[]
  }

  /**
   * @deprecated Use `create(data, { kysely: { onConflictFields, ... } })`
   * instead. `create` runs through the standard Feathers pipeline (emits
   * `created`, runs hooks, participates in transaction event deferral); this
   * method does not. The conflict-resolution logic now lives in `_create`; this
   * method simply forwards its options through `params.kysely`.
   */
  async _upsert(
    data: Data,
    params: ServiceParams & UpsertOptions<Result>,
  ): Promise<Result>
  async _upsert(
    data: Data[],
    params: ServiceParams & UpsertOptions<Result>,
  ): Promise<Result[]>
  async _upsert(
    data: Data | Data[],
    _params: ServiceParams & UpsertOptions<Result>,
  ): Promise<Result | Result[]>
  async _upsert(
    _data: Data | Data[],
    params: ServiceParams & UpsertOptions<Result>,
  ): Promise<Result | Result[]> {
    const {
      onConflictFields,
      onConflictAction,
      onConflictMergeFields,
      onConflictExcludeFields,
      onConflictReturning,
      ...rest
    } = params

    return this._create(
      _data as any,
      {
        ...rest,
        kysely: {
          onConflictFields,
          onConflictAction,
          onConflictMergeFields,
          onConflictExcludeFields,
          onConflictReturning,
          ...rest?.kysely,
        },
      } as unknown as ServiceParams,
    )
  }

  private async getWhereForUpdateOrDelete<
    Q extends
      | UpdateQueryBuilder<any, any, any, any>
      | DeleteQueryBuilder<any, any, any>,
  >(
    q: Q,
    id: NullableId,
    params: ServiceParams,
    $select?: string[] | undefined,
    applyReturningClause = true,
  ) {
    const { filters, options, query } = this.filterQuery(params, id)
    const { id: idField, dialectType } = options

    if (dialectType !== 'mysql') {
      const withWhere = this.applyWhere(q, query, params)
      // Skip RETURNING entirely for fire-and-forget writes — the affected-row
      // count on the plain result is enough to enforce NotFound on a single call.
      const q2 = applyReturningClause
        ? this.applyReturning(withWhere, filters.$select)
        : withWhere
      const result = {
        q: q2,
        buildWhere: undefined,
        items: undefined,
      }

      return result
    }

    // mysql does not allow sophisticated where in update/delete statements
    // so we need to do a find/get first to get the ids

    if (id !== null) {
      const result = await this._get(id, {
        ...params,
        query: {
          ...params.query,
          $select: $select || params.query?.$select,
        },
      }).catch(() => {
        throw new NotFound(`No record found for ${idField} '${id}'`)
      })

      const withWhere = (q as any).where(this.col(idField), '=', id)
      const returning = this.applyReturning(withWhere, filters.$select)

      return {
        q: returning as Q,
        buildWhere: (selected: SelectQueryBuilder<any, any, any>) =>
          selected.where(this.col(idField), '=', id),
        items: [result],
      }
    }

    const items = await this._find({
      ...params,
      query: {
        ...params.query,
        $select: $select || params.query?.$select,
      },
      paginate: false,
    })

    const ids = items.map((item) => item[idField])

    if (ids.length === 0) {
      return { q: undefined, buildWhere: undefined, items: undefined }
    }

    const withWhere =
      ids.length === 1
        ? (q as any).where(this.col(idField), '=', ids[0])
        : (q as any).where(this.col(idField), 'in', ids)

    const returning = this.applyReturning(withWhere, filters.$select)

    return {
      q: returning as Q,
      buildWhere: (selected: SelectQueryBuilder<any, any, any>) =>
        ids.length === 1
          ? selected.where(this.col(idField), '=', ids[0])
          : selected.where(this.col(idField), 'in', ids),
      items,
    }
  }

  /**
   * Patch a single record by id
   * See https://kysely-org.github.io/kysely/classes/UpdateQueryBuilder.html
   * @param id
   * @param data
   * @param params
   */
  async _patch(
    id: null,
    data: PatchData,
    params?: ServiceParams,
  ): Promise<Result[]>
  async _patch(id: Id, data: PatchData, params?: ServiceParams): Promise<Result>
  async _patch(
    id: NullableId,
    data: PatchData,
    _params?: ServiceParams,
  ): Promise<Result | Result[]>
  async _patch(
    id: NullableId,
    _data: PatchData,
    params: ServiceParams = {} as ServiceParams,
  ): Promise<Result | Result[]> {
    if (id === null && !this.allowsMulti('patch', params)) {
      throw new MethodNotAllowed('Can not patch multiple entries')
    }
    const asMulti = id === null

    const { filters, options } = this.filterQuery(params, id)

    const { id: idField, name, dialectType } = this.options

    const noReturn = this.wantsNoReturn(params)

    const data = this.convertValues(_data)
    const setData = _.omit(data, idField)

    if (Object.keys(setData).length === 0) {
      if (noReturn) {
        // No-op patch: there is no SET clause, so no UPDATE runs on ANY dialect
        // and no affected-row count exists to derive existence from. Enforce
        // NotFound for a single id with a minimal id-only existence probe.
        if (!asMulti) {
          await this._get(id as Id, {
            ...params,
            query: { ...params.query, $select: [idField] },
          })
        }
        return (asMulti ? [] : undefined) as unknown as Result | Result[]
      }
      return asMulti
        ? await this._find({ ...params, paginate: false })
        : await this._get(id as Id, params)
    }

    const updateTable = this.db(params).updateTable(name).set(setData)

    const { q, buildWhere } = await this.getWhereForUpdateOrDelete(
      updateTable,
      id,
      params,
      [this.options.id],
      !noReturn,
    )

    if (!q) {
      return [] // nothing to patch
    }

    if (noReturn) {
      // Skip RETURNING and every post-fetch. On MySQL, getWhereForUpdateOrDelete
      // already pre-fetched (and threw NotFound for a missing single id); on
      // other dialects we derive existence from the affected-row count.
      const execResult = await (
        asMulti ? (q as any).execute() : (q as any).executeTakeFirst()
      ).catch(this.handleError)

      if (
        !asMulti &&
        dialectType !== 'mysql' &&
        !this.didAffectRow(execResult)
      ) {
        throw new NotFound(`No record found for ${idField} '${id}'`)
      }

      return (asMulti ? [] : undefined) as unknown as Result | Result[]
    }

    const response = await this.executeAndReturn(q, {
      isArray: asMulti,
      options,
      params,
      $select: filters.$select,
      buildWhere,
    })

    if (!asMulti && !response) {
      throw new NotFound(`No record found for ${idField} '${id}'`)
    }

    return response as Result | Result[]
  }

  async _update(
    id: Id,
    _data: Data,
    params: ServiceParams = {} as ServiceParams,
  ): Promise<Result> {
    if (id === null) {
      throw new BadRequest(
        "You can not replace multiple instances. Did you mean 'patch'?",
      )
    }

    const data = _.omit(_data, this.id)

    // Replacing a record nulls out every column absent from `data`, so we need
    // the full set of column names. When `properties` is configured (the same
    // map col() treats as the known columns) we read only the id for the
    // existence check; otherwise we fall back to reading the whole row.
    const knownColumns =
      this.propertyMap.size > 0 ? [...this.propertyMap.keys()] : undefined

    const oldData = await this._get(id, {
      ...params,
      query: {
        ...params.query,
        $select: knownColumns ? [this.id] : undefined,
      },
    })

    const columns = knownColumns ?? Object.keys(oldData)

    // New data changes all fields except id
    const newObject = columns.reduce((result: any, key) => {
      if (key !== this.id) {
        result[key] = data[key] === undefined ? null : data[key]
      }
      return result
    }, {})

    const result = await this._patch(id, newObject, params)

    return result as Result
  }

  /**
   * Remove a single record by id
   * See https://kysely-org.github.io/kysely/classes/DeleteQueryBuilder.html
   * @param id
   * @param params
   */
  async _remove(id: null, params?: ServiceParams): Promise<Result[]>
  async _remove(id: Id, params?: ServiceParams): Promise<Result>
  async _remove(
    id: NullableId,
    _params?: ServiceParams,
  ): Promise<Result | Result[]>
  async _remove(
    id: NullableId,
    params: ServiceParams = {} as ServiceParams,
  ): Promise<Result | Result[]> {
    if (id === null && !this.allowsMulti('remove', params)) {
      throw new MethodNotAllowed('Cannot remove multiple entries')
    }

    const isMulti = id === null
    const noReturn = this.wantsNoReturn(params)

    const deleteFrom = this.db(params).deleteFrom(this.options.name)

    const { q, items: maybeItems } = await this.getWhereForUpdateOrDelete(
      deleteFrom,
      id,
      params,
      undefined,
      !noReturn,
    )

    if (!q) {
      return isMulti ? [] : Promise.reject(new NotFound())
    }

    if (noReturn) {
      // Skip RETURNING and the deleted-row payload. On MySQL a missing single id
      // already threw NotFound in getWhereForUpdateOrDelete; on other dialects we
      // derive existence from the affected-row count.
      const execResult = await (q as any)
        .executeTakeFirst()
        .catch(this.handleError)

      if (
        !isMulti &&
        this.options.dialectType !== 'mysql' &&
        !this.didAffectRow(execResult)
      ) {
        throw new NotFound()
      }

      return (isMulti ? [] : undefined) as unknown as Result | Result[]
    }

    const _result = await q.execute().catch(this.handleError)

    const result = maybeItems || _result

    if (isMulti) {
      return result as Result[]
    }

    if (result.length === 0) throw new NotFound()

    return result[0] as Result
  }
}
