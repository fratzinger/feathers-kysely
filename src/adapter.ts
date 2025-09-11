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
  InsertQueryBuilder,
  Kysely,
  SelectQueryBuilder,
} from 'kysely'

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

// const RETURNING_CLIENTS = ['postgresql', 'pg', 'oracledb', 'mssql']

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
  schema?: string

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

  getOptions(
    params: ServiceParams,
  ): KyselyAdapterOptions & { id: string; dialectType: DialectType } {
    return super.getOptions(params) as KyselyAdapterOptions & {
      id: string
      dialectType: DialectType
    }
  }

  getModel(params: ServiceParams = {} as ServiceParams) {
    const { Model } = this.getOptions(params)
    return Model
  }

  filterQuery(params: ServiceParams) {
    const options = this.getOptions(params)
    const {
      $select,
      $sort,
      $limit: _limit,
      $skip = 0,
      ...query
    } = (params.query || {}) as AdapterQuery
    const $limit =
      options.dialectType === 'sqlite' && $skip
        ? getLimit(_limit, options.paginate) || -1
        : getLimit(_limit, options.paginate)

    return {
      paginate: options.paginate,
      filters: { $select, $sort, $limit, $skip },
      query: this.convertValues(query),
    }
  }

  createQuery(options: KyselyAdapterOptions, filters: any, query: any) {
    const q = this.startSelectQuery(options, filters, query)
    const qWhere = this.applyWhere(q, query)
    const qLimit = filters.$limit ? qWhere.limit(filters.$limit) : qWhere
    const qSkip = filters.$skip ? qLimit.offset(filters.$skip) : qLimit
    const qSorted = this.applySort(qSkip, filters)
    return qSorted
  }

  startSelectQuery(options: KyselyAdapterOptions, filters: any, query: any) {
    const { name, id: idField } = options
    let q = this.Model.selectFrom(name)
    q = this.applyInnerJoin(q, query)
    return filters.$select
      ? q.select([...new Set([...filters.$select, idField])])
      : q.selectAll()
  }

  createCountQuery(params: ServiceParams) {
    const options = this.getOptions(params)
    const { query } = this.filterQuery(params)

    const { name, id: idField } = options
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
  applyReturning<Q extends InsertQueryBuilder<any, any, any>>(
    q: Q,
    keys: string[],
  ) {
    return keys.reduce((q, key) => {
      return q.returning(`${key} as ${key}`)
    }, q.returningAll())
  }

  convertValues(data: Record<string, any>) {
    if (this.options.dialectType !== 'sqlite') return data

    // convert booleans to 0 or 1
    return Object.entries(data).reduce((data, [key, value]) => {
      if (typeof value === 'boolean') {
        return { ...data, [key]: value ? 1 : 0 }
      }
      return data
    }, data)
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
    const options = this.getOptions(params)
    const q = this.createQuery(options, filters, query)

    if (paginate && paginate.default) {
      const countQuery = this.createCountQuery(params)
      try {
        const [queryResult, countQueryResult] = await Promise.all([
          filters.$limit !== 0 ? q.execute() : undefined,
          countQuery.executeTakeFirst(),
        ])

        const data = filters.$limit === 0 ? [] : queryResult
        const total = Number(countQueryResult?.total) || 0

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
    const options = this.getOptions(params)
    const { id: idField } = options
    const { filters, query } = this.filterQuery(params)

    if (id != null && query[idField] != null) throw new NotFound()

    const q = this.startSelectQuery(options, filters, query)
    const qWhere = this.applyWhere(q, { [idField]: id, ...query })
    const compiled = qWhere.compile()
    try {
      const item = await qWhere.executeTakeFirst()

      if (!item) throw new NotFound(`No record found for ${idField} '${id}'`)

      return item as Result
    } catch (error) {
      errorHandler(error)
      throw error
    }
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
    const { name, id: idField } = this.getOptions(params)
    const { filters } = this.filterQuery(params)
    const isArray = Array.isArray(_data)
    const $select = filters.$select?.length
      ? filters.$select.concat(idField)
      : undefined

    const convertedData = isArray
      ? _data.map((d) => this.convertValues(d as any))
      : [this.convertValues(_data as any)]
    const q = this.Model.insertInto(name).values(convertedData)

    const qReturning = this.applyReturning(q, Object.keys(convertedData[0]))

    const compiled = qReturning.compile()

    const request = isArray
      ? qReturning.execute()
      : qReturning.executeTakeFirst()
    try {
      const response = (await request)!
      const toReturn = $select?.length
        ? isArray
          ? response.map((i: any) => _.pick(i, ...$select))
          : _.pick(response, ...$select)
        : response

      return toReturn
    } catch (error) {
      errorHandler(error)
      throw error
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
    const { name, id: idField } = this.getOptions(params)
    const { filters, query } = this.filterQuery(params)
    const $select = filters.$select?.length
      ? filters.$select.concat(idField)
      : undefined

    if (id != null && query[idField] != null) throw new NotFound()

    const q = this.Model.updateTable(name).set(_.omit(_data, idField))
    const qWhere = this.applyWhere(
      q,
      asMulti ? query : id == null ? query : { [idField]: id, ...query },
    )
    const toSelect = filters.$select?.length
      ? filters.$select
      : Object.keys(_data as any)
    const qReturning = this.applyReturning(
      qWhere as any,
      toSelect.concat(idField),
    )

    const compiled = qReturning.compile()

    const request = asMulti
      ? qReturning.execute()
      : qReturning.executeTakeFirst()
    try {
      const response = await request

      if (!asMulti && !response) {
        throw new NotFound(`No record found for ${idField} '${id}'`)
      }

      const toReturn = $select?.length
        ? Array.isArray(response)
          ? response.map((i: any) => _.pick(i, ...$select))
          : _.pick(response, ...$select)
        : response

      return toReturn as Result | Result[]
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
    const oldData = await this._get(id, params)
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

    params.paginate = false

    const originalData =
      id === null ? await this._find(params) : await this._get(id, params)
    const { name, id: idField } = this.getOptions(params)

    const q = this.Model.deleteFrom(name)
    const convertedQuery = this.convertValues(
      id === null ? params.query : { [idField]: id },
    )
    const qWhere = this.applyWhere(q as any, convertedQuery)
    const compiled = qWhere.compile()
    const request = id === null ? qWhere.execute() : qWhere.executeTakeFirst()
    try {
      const result = await request

      if (!result) throw new NotFound(`No record found for id '${id}'`)

      return originalData as Result | Result[]
    } catch (error) {
      errorHandler(error)
      throw error
    }
  }
}
