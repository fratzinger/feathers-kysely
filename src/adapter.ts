import type { Id, NullableId, Paginated, Query } from '@feathersjs/feathers'
import { _ } from '@feathersjs/commons'
import type {
  PaginationOptions,
  AdapterQuery,
} from '@feathersjs/adapter-commons'
import { AdapterBase, getLimit } from '@feathersjs/adapter-commons'
import { BadRequest, MethodNotAllowed, NotFound } from '@feathersjs/errors'

import { errorHandler } from './error-handler.js'
import type {
  DialectType,
  KyselyAdapterOptions,
  KyselyAdapterParams,
} from './declarations.js'
import type {
  ComparisonOperatorExpression,
  DeleteQueryBuilder,
  InsertQueryBuilder,
  Kysely,
  SelectQueryBuilder,
  UpdateQueryBuilder,
} from 'kysely'
import { applySelectId, convertBooleansToNumbers } from './utils.js'

// See https://kysely-org.github.io/kysely-apidoc/variables/OPERATORS.html
const OPERATORS: Record<string, ComparisonOperatorExpression> = {
  $lt: '<',
  $lte: '<=',
  $gt: '>',
  $gte: '>=',
  $like: 'like',
  $notlike: 'not like',
  $ilike: 'ilike',
  $in: 'in',
  $nin: 'not in',
  $eq: '=',
  $ne: '!=',
}

type KyselyAdapterOptionsDefined = KyselyAdapterOptions & {
  id: string
  dialectType: DialectType
}

type Filters = {
  $select: string[] | undefined
  $sort: Record<string, 1 | -1> | undefined
  $limit: number | undefined
  $skip: number | undefined
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
      operators: [...(options.operators || []), '$like', '$notlike', '$ilike'],
    })

    const dialectType = this.getDatabaseDialect(options.Model)

    this.options.dialectType ??= dialectType
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

  // get fullName() {
  //   const { name, schema } = this.getOptions({} as ServiceParams)
  //   return schema ? `${schema}.${name}` : name
  // }

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

  filterQuery(params: ServiceParams, id?: NullableId) {
    const options = this.getOptions(params)
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

    const queryWithId =
      id == null || query[options.id] === id
        ? query
        : !(options.id in query)
          ? { ...query, [options.id]: id }
          : { ...query, $and: [...(query.$and || []), { [options.id]: id }] }

    const converted = this.convertValues(queryWithId)

    const $select = applySelectId(_select, options.id)

    return {
      paginate: options.paginate,
      filters: {
        $select,
        $sort,
        $limit,
        $skip,
      },
      query: converted,
      options,
    }
  }

  createQuery(filters: any, query: any) {
    const q = this.startSelectQuery(filters, query)
    const qWhere = this.applyWhere(q, query)
    const qLimit = filters.$limit ? qWhere.limit(filters.$limit) : qWhere
    const qSkip = filters.$skip ? qLimit.offset(filters.$skip) : qLimit
    const qSorted = this.applySort(qSkip, filters)
    return qSorted
  }

  startSelectQuery(filters: any, query: any) {
    let q = this.Model.selectFrom(this.options.name)
    q = this.applyInnerJoin(q, query)
    return filters.$select ? q.select(filters.$select) : q.selectAll()
  }

  createCountQuery(params: ServiceParams) {
    const { query } = this.filterQuery(params)

    const { name, id: idField } = this.options
    const q = this.Model.selectFrom(name)
    const joined = this.applyInnerJoin(q, query)
    const selected = joined.select(this.Model.fn.count(idField).as('total'))

    const qWhere = this.applyWhere(selected, query)

    return qWhere
  }

  applyInnerJoin<Q extends Record<string, any>>(
    q: Q,
    query: Query,
    alreadyJoined: string[] = [],
  ) {
    // @ts-expect-error TODO: add it to options
    if (!this.queryMap) return q

    for (const key in query) {
      if (key === '$and' || key === '$or') {
        q = this.applyInnerJoin(q, query[key], alreadyJoined)
        continue
      }

      if (!key.includes('.')) continue

      const mapKey = key.split('.')[0]

      // @ts-expect-error TODO: add it to options
      const map = this.queryMap[mapKey]
      if (!map) continue

      if (alreadyJoined.includes(mapKey)) continue

      const tableName = map.db || map.service
      const keyHere = map.keyHere || 'id'
      const keyThere = map.keyThere || 'id'

      q = q.innerJoin(
        `${tableName} as ${mapKey}`,
        `${mapKey}.${keyThere}`,
        `${this.options.name}.${keyHere}`,
      )
    }

    return q
  }

  getOperator(op: string, value: any) {
    if (value !== null) {
      return OPERATORS[op]
    }

    if (op === '$ne') return 'is not'
    if (op === '$eq') return 'is'
    return OPERATORS[op]
  }

  applyWhere<Q extends Record<string, any>>(q: Q, query: Query) {
    // loop through params and call the where filters
    return Object.entries(query).reduce((q, [queryKey, queryProperty]) => {
      if (['$and', '$or'].includes(queryKey)) {
        return q.where((qb: any) => {
          return this.handleAndOr(qb, queryKey, queryProperty)
        })
      } else if (_.isObject(queryProperty)) {
        // loop through OPERATORS and apply them
        for (const operator in queryProperty) {
          const value = queryProperty[operator]
          const op = this.getOperator(operator, value)
          if (!op) continue
          q = q.where(queryKey, op, value)
        }

        return q
      } else {
        const op = this.getOperator('$eq', queryProperty)
        if (!op) return q
        return q.where(queryKey, op, queryProperty)
      }
    }, q)
  }

  handleAndOr(qb: any, key: string, value: Query[]) {
    const method = qb[key.replace('$', '')]
    const subs = value.map((subParams: Query) => {
      return this.handleSubQuery(qb, subParams)
    })
    return method(subs)
  }

  handleSubQuery(qb: any, query: Query): any {
    return qb.and(
      Object.entries(query).map(([key, value]) => {
        if (['$and', '$or'].includes(key)) {
          return this.handleAndOr(qb, key, value)
        } else if (_.isObject(value)) {
          // loop through OPERATORS and apply them
          return qb.and(
            Object.entries(OPERATORS)
              .filter(([operator, op]) => {
                // eslint-disable-next-line no-prototype-builtins
                return value?.hasOwnProperty(operator)
              })
              .map(([operator, op]) => {
                const val = value[operator]
                return this.whereCompare(qb, key, operator, val)
              }),
          )
        } else {
          return this.whereCompare(qb, key, '$eq', value)
        }
      }),
    )
  }

  whereCompare(qb: any, key: string, operator: any, value: any) {
    return qb.eb(key, this.getOperator(operator, value), value)
  }

  applySort<Q extends SelectQueryBuilder<any, string, any>>(
    q: Q,
    filters: any,
  ) {
    return Object.entries(filters.$sort || {}).reduce(
      (q, [key, value]) => {
        return q.orderBy(key, value === 1 ? 'asc' : 'desc')
      },
      q as SelectQueryBuilder<any, string, any>,
    )
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
        ? (q as any).returning($select)
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
    const { filters, query, paginate } = this.filterQuery(params)
    const q = this.createQuery(filters, query)

    // const compiled = q.compile()
    // console.log(compiled.sql, compiled.parameters)

    if (paginate && paginate.default) {
      const countQuery = this.createCountQuery(params)

      try {
        const [queryResult, countQueryResult] = await Promise.all([
          filters.$limit !== 0 ? q.execute() : undefined,
          countQuery.executeTakeFirst(),
        ])

        const data = filters.$limit === 0 ? [] : queryResult
        const total = Number(countQueryResult?.total) || 0

        // console.log(data)

        return {
          total,
          limit: filters.$limit,
          skip: filters.$skip || 0,
          data: data as Result[],
        }
      } catch (error) {
        errorHandler(error)
        throw error
      }
    }

    try {
      const data = filters.$limit === 0 ? [] : await q.execute()
      return data as Result[]
    } catch (error) {
      errorHandler(error)
      throw error
    }
  }

  /**
   * Retrieve a single record by id
   * See https://kysely-org.github.io/kysely/classes/SelectQueryBuilder.html
   */
  async _get(
    id: Id,
    params: ServiceParams = {} as ServiceParams,
  ): Promise<Result> {
    const { filters, query, options } = this.filterQuery(params, id)
    const { id: idField } = options

    const q = this.startSelectQuery(filters, query)
    const qWhere = this.applyWhere(q, query)
    try {
      const item = await qWhere.executeTakeFirst()

      if (!item) throw new NotFound(`No record found for ${idField} '${id}'`)

      return item as Result
    } catch (error) {
      errorHandler(error)
      throw error
    }
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
      ids?: number[]
    },
  ) {
    const { isArray, options, $select } = context
    const { id: idField, name, dialectType } = options

    const response = await (isArray && dialectType !== 'mysql'
      ? q.execute()
      : q.executeTakeFirst())

    if (dialectType !== 'mysql') {
      return response
    }

    // mysql only

    const { insertId, numInsertedOrUpdatedRows } = response as any
    const id = Number(insertId)
    const count = Number(numInsertedOrUpdatedRows)

    const ids =
      context.ids ||
      (isArray ? [...Array(count).keys()].map((i) => id + i) : [id])

    const from = this.Model.selectFrom(name)
    const selected = $select ? from.select($select) : from.selectAll()
    const where =
      ids.length === 1
        ? selected.where(idField, '=', ids[0])
        : selected.where(idField, 'in', ids)

    return isArray ? where.execute() : where.executeTakeFirst()
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

    try {
      const response = await this.executeAndReturn(returning, {
        isArray,
        options,
        $select,
      })

      return response
    } catch (error) {
      errorHandler(error)
      throw error
    }
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
        ids: undefined,
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
      }).catch((err) => {
        throw new NotFound(`No record found for ${idField} '${id}'`)
      })

      const withWhere = (q as any).where(idField, '=', id)
      const returning = this.applyReturning(withWhere, filters.$select)

      return { q: returning as Q, ids: [id], items: [result] }
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
      return { q: undefined, ids: undefined, items: undefined }
    }

    const withWhere =
      ids.length === 1
        ? (q as any).where(idField, '=', ids[0])
        : (q as any).where(idField, 'in', ids)

    const returning = this.applyReturning(withWhere, filters.$select)

    return {
      q: returning as Q,
      ids,
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

    const { q, ids } = await this.getWhereForUpdateOrDelete(
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

    try {
      const response = await this.executeAndReturn(q, {
        isArray: asMulti,
        options,
        $select: filters.$select,
        ids,
      })

      if (!asMulti && !response) {
        throw new NotFound(`No record found for ${idField} '${id}'`)
      }

      return response as Result | Result[]
    } catch (error) {
      errorHandler(error)
      throw error
    }
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

    try {
      const _result = await q.execute()

      const result = maybeItems || _result

      if (isMulti) {
        return result as Result[]
      }

      if (result.length === 0) throw new NotFound()

      return result[0] as Result
    } catch (error) {
      errorHandler(error)
      throw error
    }
  }
}
