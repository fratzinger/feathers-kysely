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
  AdapterServiceOptions,
} from '@feathersjs/adapter-commons'
import { AdapterBase, getLimit } from '@feathersjs/adapter-commons'
import { BadRequest, MethodNotAllowed, NotFound } from '@feathersjs/errors'

import { errorHandler } from './error-handler.js'
import type {
  DialectType,
  KyselyAdapterParams,
  UpsertOptions,
} from './declarations.js'
import { expressionBuilder, sql } from 'kysely'
import type {
  SelectExpression,
  ComparisonOperatorExpression,
  DeleteQueryBuilder,
  InsertQueryBuilder,
  Kysely,
  SelectQueryBuilder,
  UpdateQueryBuilder,
  ExpressionBuilder,
  ExpressionWrapper,
} from 'kysely'
import {
  applySelectId,
  convertBooleansToNumbers,
  getOrderByModifier,
} from './utils.js'
import { addToQuery } from 'feathers-utils'

// See https://kysely-org.github.io/kysely-apidoc/variables/OPERATORS.html
const OPERATORS: Record<string, ComparisonOperatorExpression> = {
  $lt: '<',
  $lte: '<=',
  $gt: '>',
  $gte: '>=',
  $in: 'in',
  $nin: 'not in',
  $eq: '=',
  $ne: '!=',
  $like: 'like',
  $notLike: 'not like',
  $iLike: 'ilike',
  $contains: '@>',
  $contained: '<@',
  $overlap: '&&',
}

// TODO: $between, $notBetween

const FILTERS = ['$select', '$sort', '$limit', '$skip'] as const
type Filter = (typeof FILTERS)[number]

type KyselyAdapterOptionsDefined = KyselyAdapterOptions & {
  id: string
  dialectType: DialectType
}

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

type HandleQueryOptions = {
  tableName?: string | null | undefined
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

  constructor(options: KyselyAdapterOptions) {
    if (!options || !options.Model) {
      throw new Error(
        'You must provide a Kysely instance to the `Model` option',
      )
    }

    if (typeof options.name !== 'string') {
      throw new Error('No table name specified.')
    }

    super({
      id: 'id',
      ...options,
      filters: {
        ...options.filters,
        $and: (value: any) => value,
      },
      operators: [
        ...new Set([...(options.operators ?? []), ...Object.keys(OPERATORS)]),
      ],
    })

    const dialectType = this.getDatabaseDialect(options.Model)

    this.options.dialectType ??= dialectType
    this.propertyMap = new Map<string, any>(
      Object.entries(options.properties || {}),
    )
    // console.log(options.name, this.propertyMap)
  }

  private getDatabaseDialect(db?: Kysely<any>): DialectType {
    const adapterName = (db ?? this.Model)
      .getExecutor()
      .adapter.constructor.name.toLowerCase()

    if (adapterName.includes('sqlite')) return 'sqlite'
    if (adapterName.includes('postgres')) return 'postgres'
    if (adapterName.includes('mysql')) return 'mysql'
    if (adapterName.includes('mssql') || adapterName.includes('sqlserver'))
      return 'mssql'

    return 'sqlite'
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
      $skip = 0,
      ...query
    } = (params.query || {}) as AdapterQuery
    const $limit = $skip
      ? (getLimit(_limit, options.paginate) ??
        (options.dialectType === 'sqlite'
          ? -1
          : options.dialectType === 'mysql'
            ? 4294967295 /** max value for mysql */
            : undefined))
      : getLimit(_limit, options.paginate)

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
    let query = filterQueryResult.query

    // console.log('name', this.options.name)

    let q = this.Model.selectFrom(this.options.name)
    const applyResult = this.applyJoins(q, filterQueryResult.params, {
      where: options?.where,
      order: options?.order,
    })
    q = applyResult.q
    query = applyResult.query

    if (options?.select) {
      const $select = Array.isArray(options.select)
        ? options.select
        : filters.$select

      const select =
        $select && Array.isArray($select) ? this.col($select) : $select

      q = select ? q.select(select) : q.selectAll(this.options.name)
    }

    if (options?.where) {
      q = this.applyWhere(q, query)
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
      q = this.applySort(q, filters)
    }

    return q
  }

  private applyJoins<Q extends Record<string, any>>(
    q: Q,
    params: Params,
    options: {
      where?: boolean
      order?: boolean
    },
  ): { q: Q; query: Query } {
    let query = params.query || {}
    if (!this.options.relations) return { q, query }

    const alreadyJoined: string[] = []

    if (options.where) {
      const whereResult = this.applyJoinsForWhere(q, params.query || {}, {
        alreadyJoined,
      })
      q = whereResult.q
      query = whereResult.query
    }

    if (options.order && query.$sort) {
      q = this.applyJoinsForOrderBy(q, query.$sort, { alreadyJoined })
    }

    return { q, query }
  }

  private applyJoinsForWhere<Q extends Record<string, any>>(
    q: Q,
    query: Query,
    options: {
      alreadyJoined: string[]
    },
  ): { q: Q; query: Query } {
    if (!this.options.relations) return { q, query }

    const cloned = false

    for (const key in query) {
      if (FILTERS.includes(key as Filter)) continue

      if ((key === '$and' || key === '$or') && Array.isArray(query[key])) {
        let array = query[key]
        let clonedArray = false
        for (let i = 0; i < array.length; i++) {
          const subQuery = array[i]
          const { q: subQ, query: modifiedSubQuery } = this.applyJoinsForWhere(
            q,
            subQuery,
            options,
          )

          q = subQ

          if (subQuery !== modifiedSubQuery) {
            if (!clonedArray) {
              array = [...array]
              clonedArray = true
            }

            array[i] = modifiedSubQuery
          }
        }

        if (query[key] !== array) {
          if (!cloned) {
            query = { ...query }
          }

          query[key] = array
        }

        continue
      }

      let relation = this.options.relations[key]
      let relationKey = key

      if (!relation && key.includes('.')) {
        const parts = key.split('.')
        if (parts.length !== 2) continue

        relationKey = parts[0]
        relation = this.options.relations[relationKey]
      }

      if (
        !relation ||
        !relation.databaseTableName ||
        !relation.keyHere ||
        !relation.keyThere ||
        relation.asArray /** only apply joins for belongsTo relations */
      )
        continue

      if (options.alreadyJoined.includes(relationKey)) continue

      const { databaseTableName, keyHere, keyThere } = relation

      q = q.leftJoin(
        `${databaseTableName} as ${relationKey}`,
        `${relationKey}.${keyThere}`,
        `${this.options.name}.${keyHere}`,
      )

      query = addToQuery(query, {
        [`${relationKey}.${keyThere}`]: { $ne: null },
      })

      options.alreadyJoined.push(relationKey)
    }

    return { q, query }
  }

  private handleHasMany(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
  ) {
    if (!this.options.relations) return

    let relation = this.options.relations[queryKey]

    if (!relation && !queryKey.includes('.')) {
      return
    }

    let relationKey = queryKey
    let nested = true

    if (!relation) {
      const parts = queryKey.split('.')
      if (parts.length !== 2) return

      relationKey = parts[0]
      nested = false

      relation = this.options.relations[relationKey]
    }

    if (
      !relation ||
      !relation.databaseTableName ||
      !relation.keyHere ||
      !relation.keyThere ||
      !relation.asArray
    ) {
      return
    }

    const subQueries: ExpressionWrapper<any, any, any>[] = []

    if (nested) {
      for (const subKey in queryProperty) {
        const subQuery = this.handleQueryProperty(
          eb,
          subKey,
          queryProperty[subKey],
          { tableName: relationKey },
        )
        if (subQuery) subQueries.push(subQuery)
      }
    } else {
      const nestedWhere = this.handleQueryPropertyNormal(
        eb,
        queryKey,
        queryProperty,
        {
          tableName: relationKey,
        },
      )
      if (nestedWhere) subQueries.push(nestedWhere)
    }

    const whereRef = eb
      .selectFrom(`${relation.databaseTableName} as ${relationKey}`)
      .select(sql`1` as any)
      .where((eb) =>
        eb.and([
          eb(
            `${relationKey}.${relation.keyThere}`,
            '=',
            eb.ref(this.col(relation.keyHere)),
          ),
          ...subQueries,
        ]),
      )

    return eb.exists(whereRef)
  }

  private handleBelongsTo(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
  ) {
    if (!this.options.relations) return

    let relation = this.options.relations[queryKey]

    if (!relation && !queryKey.includes('.')) {
      return
    }

    let relationKey = queryKey
    let nested = true

    if (!relation) {
      const parts = queryKey.split('.')
      if (parts.length !== 2) return

      relationKey = parts[0]
      nested = false

      relation = this.options.relations[relationKey]
    }

    if (
      !relation ||
      !relation.databaseTableName ||
      !relation.keyHere ||
      !relation.keyThere ||
      relation.asArray
    ) {
      return
    }

    const subQueries: ExpressionWrapper<any, any, any>[] = []

    if (nested) {
      for (const subKey in queryProperty) {
        const subQuery = this.handleQueryProperty(
          eb,
          subKey,
          queryProperty[subKey],
          { tableName: relationKey },
        )

        if (subQuery) subQueries.push(subQuery)
      }
    } else {
      const subQuery = this.handleQueryPropertyNormal(
        eb,
        queryKey,
        queryProperty,
        { tableName: null },
      )

      if (subQuery) subQueries.push(subQuery)
    }

    return subQueries.length === 0 ? undefined : eb.and(subQueries)
  }

  private handleQueryPropertyNormal(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
    options?: HandleQueryOptions,
  ) {
    // console.log('handleQueryPropertyNormal', queryKey, queryProperty)
    if (queryKey === '$and' || queryKey === '$or') {
      const method = eb[queryKey === '$and' ? 'and' : 'or']
      const subs = []
      for (const subQuery of queryProperty) {
        const result = this.handleQuery(eb, subQuery, options)

        if (result?.length) subs.push(eb.and(result))
      }

      return subs?.length ? method(subs) : undefined
    }

    const col = this.col(queryKey, { tableName: options?.tableName })

    if (_.isObject(queryProperty)) {
      // console.log('isObject', queryKey, queryProperty)
      const qs = []
      // loop through OPERATORS and apply them
      for (const operator in queryProperty) {
        const value = queryProperty[operator]
        const op = this.getOperator(operator, value)
        if (!op) continue
        // console.log(
        //   'property',
        //   col,
        //   op,
        //   value,
        //   this.transformOperatorValue(operator, value),
        // )
        qs.push(eb(col, op, this.transformOperatorValue(operator, value)))
      }

      if (qs.length) {
        return eb.and(qs)
      }

      // no operators matched - do a simple equality check
    }

    // console.log('not isObject', queryKey, queryProperty)
    const op = this.getOperator('$eq', queryProperty)
    if (!op) return
    // console.log('property', col, op, queryProperty)
    return eb(col, op, queryProperty)
  }

  private applyJoinsForOrderBy<Q extends Record<string, any>>(
    q: Q,
    $sort: SortFilter,
    options: {
      alreadyJoined: string[]
    },
  ): Q {
    if (!this.options.relations || !$sort) return q

    if (!$sort) return q

    for (const key in $sort) {
      if (!key.includes('.')) continue

      const mapKey = key.split('.')[0]

      const map = this.options.relations[mapKey]
      if (
        !map ||
        !map.databaseTableName ||
        !map.keyHere ||
        !map.keyThere ||
        map.asArray /** only apply joins for belongsTo relations */
      )
        continue

      if (options.alreadyJoined.includes(mapKey)) continue

      const { databaseTableName, keyHere, keyThere } = map

      q = q.leftJoin(
        `${databaseTableName} as ${mapKey}`,
        `${mapKey}.${keyThere}`,
        `${this.options.name}.${keyHere}`,
      )

      options.alreadyJoined.push(mapKey)
    }

    return q
  }

  private getOperator(op: string, value: any) {
    if (value !== null) {
      return OPERATORS[op]
    }

    if (op === '$ne') return 'is not'
    if (op === '$eq') return 'is'
    return OPERATORS[op]
  }

  private transformOperatorValue(op: string, value: any) {
    if (op !== '$contains' && op !== '$contained' && op !== '$overlap') {
      return value
    }

    if (!value) {
      return value
    }

    if (!Array.isArray(value)) {
      throw new BadRequest(`The value for '${op}' must be an array`)
    }

    // For PostgreSQL, we need to help with type inference
    // Cast based on the first element's type
    const firstElement = value[0]
    if (typeof firstElement === 'number') {
      return sql`ARRAY[${sql.join(value)}]::integer[]`
    } else if (typeof firstElement === 'string') {
      return sql`${JSON.stringify(value)}`
      //return sql`ARRAY[${sql.join(value)}]::varchar[]`
    } else {
      // Default case - let PostgreSQL try to infer
      return sql`ARRAY[${sql.join(value)}]`
    }
  }

  private col<T>(
    column: T,
    options?: { tableName: string | null | undefined },
  ): T {
    if (Array.isArray(column))
      return column.map((item) => this.col(item, options)) as T
    if (typeof column !== 'string') return column
    if (options?.tableName === null) return column

    const tableName =
      options?.tableName ||
      (this.propertyMap.has(column) ? this.options.name : null)

    if (!tableName || column.startsWith(`${tableName}.`)) return column

    return `${tableName}.${column}` as T
  }

  applyWhere<Q extends Record<string, any>>(q: Q, query: Query) {
    // loop through params and call the where filters

    if (!query || Object.keys(query).length === 0) {
      return q
    }

    const eb = expressionBuilder()

    const result = this.handleQuery(eb, query)

    return result?.length
      ? q.where((eb: ExpressionBuilder<any, any>) => eb.and(result))
      : q
  }

  handleQueryProperty(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
    options?: HandleQueryOptions,
  ) {
    // ignore filters - just for safety
    if (FILTERS.includes(queryKey as Filter)) {
      return undefined
    }

    const hasMany = this.handleHasMany(eb, queryKey, queryProperty)

    if (hasMany) return hasMany

    const belongsTo = this.handleBelongsTo(eb, queryKey, queryProperty)

    if (belongsTo) return belongsTo

    const normal = this.handleQueryPropertyNormal(
      eb,
      queryKey,
      queryProperty,
      options,
    )

    if (normal) return normal
  }

  private handleQuery(
    eb: ExpressionBuilder<any, any>,
    query: Query,
    options?: HandleQueryOptions,
  ): any {
    const qs: any[] = []
    if (!query) return qs

    for (const queryKey in query) {
      const q = this.handleQueryProperty(eb, queryKey, query[queryKey], options)

      if (!q) {
        continue
      }

      qs.push(q)
    }

    return qs?.length ? qs : undefined
  }

  applySort<Q extends SelectQueryBuilder<any, string, any>>(
    q: Q,
    filters: Filters,
  ) {
    if (!filters.$sort) return q

    for (const key in filters.$sort) {
      const value = filters.$sort[key]
      q = q.orderBy(this.col(key), getOrderByModifier(value)) as any
    }

    return q
  }

  /**
   * Add a returning statement alias for each key (bypasses bug in sqlite)
   * @param q kysely query builder
   * @param data data which is expected to be returned
   */
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

    // const compiled = q.compile()
    // console.log(compiled.sql, compiled.parameters)

    if (paginate && paginate.default) {
      const countQuery = this.composeQuery(params, {
        select: [this.Model.fn.count(this.col(this.options.id)).as('total')],
        where: true,
      })

      const [queryResult, countQueryResult] = await Promise.all([
        filters.$limit !== 0 ? q.execute().catch(errorHandler) : undefined,
        countQuery.executeTakeFirst().catch(errorHandler),
      ])

      const data = filters.$limit === 0 ? [] : queryResult
      const total = Number((countQueryResult as any)?.total ?? 0) || 0

      return {
        total,
        limit: filters.$limit!,
        skip: filters.$skip || 0,
        data: data as Result[],
      }
    }

    const data =
      filters.$limit === 0 ? [] : await q.execute().catch(errorHandler)
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

    // const compiled = q.compile()
    // console.log(compiled.sql, compiled.parameters)

    const item = await q.executeTakeFirst().catch(errorHandler)

    if (!item)
      throw new NotFound(`No record found for ${this.options.id} '${id}'`)

    return item as Result
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
      $select?: string[]
      buildWhere?: (
        query: SelectQueryBuilder<any, any, any>,
      ) => SelectQueryBuilder<any, any, any>
    },
  ) {
    const { isArray, options, $select } = context
    const { id: idField, name, dialectType } = options

    const response = await (isArray && dialectType !== 'mysql'
      ? q.execute().catch(errorHandler)
      : q.executeTakeFirst().catch(errorHandler))

    if (dialectType !== 'mysql') {
      return response
    }

    // mysql only

    const from = this.Model.selectFrom(name)
    const select =
      $select && Array.isArray($select) ? this.col($select) : $select
    const selected = select ? from.select(select) : from.selectAll(name)

    // If a custom WHERE builder is provided, use it
    if (context.buildWhere) {
      const query = context.buildWhere(selected)
      return isArray
        ? query.execute().catch(errorHandler)
        : query.executeTakeFirst().catch(errorHandler)
    }

    // Standard insert logic (build WHERE based on insertId)
    const { insertId, numInsertedOrUpdatedRows } = response as any
    const id = Number(insertId)
    const count = Number(numInsertedOrUpdatedRows)

    const ids = isArray ? [...Array(count).keys()].map((i) => id + i) : [id]

    const where =
      ids.length === 1
        ? selected.where(this.col(idField), '=', ids[0])
        : selected.where(this.col(idField), 'in', ids)

    return isArray
      ? where.execute().catch(errorHandler)
      : where.executeTakeFirst().catch(errorHandler)
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
      return query
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
    },
  ): InsertQueryBuilder<any, any, any> {
    const {
      onConflictAction,
      onConflictFields,
      onConflictMergeFields,
      onConflictExcludeFields,
      data,
      isArray,
    } = options

    const conflictColumns = onConflictFields.map((field) =>
      this.col(field as string),
    )

    if (onConflictAction === 'ignore') {
      return query.onConflict((oc) => oc.columns(conflictColumns).doNothing())
    }

    // onConflictAction === 'merge'
    return query.onConflict((oc) => {
      const conflict = oc.columns(conflictColumns)

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

      return conflict.doUpdateSet(updateObject)
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

    if (onConflictMergeFields && onConflictMergeFields.length > 0) {
      // Use explicitly specified merge fields
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
    const { name, id: idField } = options
    const isArray = Array.isArray(_data)
    const $select = applySelectId(filters.$select, idField)

    const q = this.Model.insertInto(name).values(
      this.convertValues(_data) as any,
    )

    const returning = this.applyReturning(q, $select)

    // const compiled = returning.compile()
    // console.log(compiled.sql, compiled.parameters)

    const response = await this.executeAndReturn(returning, {
      isArray,
      options,
      $select,
    })

    return response
  }

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
    const { filters, options } = this.filterQuery(params)
    const { name, id: idField, dialectType } = options
    const isArray = Array.isArray(_data)
    const $select = applySelectId(filters.$select, idField)

    const {
      onConflictFields = [],
      onConflictAction = 'ignore',
      onConflictMergeFields,
      onConflictExcludeFields = [],
    } = params

    let q = this.Model.insertInto(name).values(this.convertValues(_data) as any)

    // Apply conflict resolution based on database dialect
    if (onConflictFields.length > 0) {
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
          : this.applyPostgresUpsertConflict(q, upsertOptions)
    }

    const returning = this.applyReturning(q, $select)

    // const compiled = returning.compile()
    // console.log(compiled.sql, compiled.parameters)

    const response = await this.executeAndReturn(returning, {
      isArray,
      options,
      $select,
      buildWhere:
        dialectType === 'mysql' && onConflictFields.length > 0
          ? (selected) =>
              this.buildWhereForConflictFields(
                selected,
                _data,
                onConflictFields,
                isArray,
              )
          : undefined,
    })

    // When using onConflict with doNothing, if a conflict occurs, the returning clause
    // won't return anything. We need to fetch the existing records in that case.
    if (onConflictAction === 'ignore' && onConflictFields.length > 0) {
      if (dialectType === 'mysql') {
        // For MySQL, executeAndReturn already handled fetching based on conflict fields
        return response
      } else {
        // PostgreSQL and SQLite
        if (isArray) {
          // For arrays, some records might have been inserted and some ignored
          const responseArray = (response || []) as Result[]
          const dataArray = _data as Data[]

          // Find which records were not inserted by comparing with input data
          if (responseArray.length < dataArray.length) {
            // Build a query to find the missing records
            const from = this.Model.selectFrom(name)
            const select =
              $select && Array.isArray($select) ? this.col($select) : $select
            const selected = select ? from.select(select) : from.selectAll(name)

            // Build OR conditions for each conflict field combination
            const missingRecords: Result[] = []
            for (const item of dataArray) {
              // Check if this item is already in the response
              const isInResponse = responseArray.some((r) =>
                onConflictFields.every((field) => {
                  const rVal = r[field as keyof Result]
                  const itemVal = item[field as keyof Data]
                  return rVal === (itemVal as any)
                }),
              )

              if (!isInResponse) {
                // Fetch the existing record
                let query = selected
                for (const field of onConflictFields) {
                  query = query.where(
                    this.col(field as string),
                    '=',
                    item[field as keyof Data],
                  ) as any
                }
                const existing = await query
                  .executeTakeFirst()
                  .catch(errorHandler)
                if (existing) {
                  missingRecords.push(existing as Result)
                }
              }
            }

            return [...responseArray, ...missingRecords] as Result[]
          }
        } else {
          // For single record, if response is undefined/null, fetch the existing record
          if (!response) {
            const from = this.Model.selectFrom(name)
            const select =
              $select && Array.isArray($select) ? this.col($select) : $select
            const selected = select ? from.select(select) : from.selectAll(name)

            let query = selected
            for (const field of onConflictFields) {
              query = query.where(
                this.col(field as string),
                '=',
                (_data as Data)[field as keyof Data],
              ) as any
            }

            const existing = await query.executeTakeFirst().catch(errorHandler)
            return existing as Result
          }
        }
      }
    }

    return response
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
  ) {
    const { filters, options, query } = this.filterQuery(params, id)
    const { id: idField, dialectType } = options

    if (dialectType !== 'mysql') {
      const withWhere = this.applyWhere(q, query)
      const returning = this.applyReturning(withWhere, filters.$select)
      const result = {
        q: returning,
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

    const { id: idField, name } = this.options

    const data = this.convertValues(_data)

    const updateTable = this.Model.updateTable(name).set(_.omit(data, idField))

    const { q, buildWhere } = await this.getWhereForUpdateOrDelete(
      updateTable,
      id,
      params,
      [this.options.id],
    )

    if (!q) {
      return [] // nothing to patch
    }

    // const compiled = q.compile()
    // console.log(compiled.sql, compiled.parameters)

    const response = await this.executeAndReturn(q, {
      isArray: asMulti,
      options,
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
    const oldData = await this._get(id, {
      ...params,
      query: {
        ...params.query,
        $select: undefined,
      },
    })
    // New data changes all fields except id
    const newObject = Object.keys(oldData).reduce((result: any, key) => {
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

    const deleteFrom = this.Model.deleteFrom(this.options.name)

    const { q, items: maybeItems } = await this.getWhereForUpdateOrDelete(
      deleteFrom,
      id,
      params,
    )

    if (!q) {
      return isMulti ? [] : Promise.reject(new NotFound())
    }

    // const compiled = q.compile()
    // console.log(compiled.sql, compiled.parameters)

    const _result = await q.execute().catch(errorHandler)

    const result = maybeItems || _result

    if (isMulti) {
      return result as Result[]
    }

    if (result.length === 0) throw new NotFound()

    return result[0] as Result
  }
}
