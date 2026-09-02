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
import type {
  DialectType,
  KyselyAdapterOptions,
  KyselyAdapterParams,
  KyselyParams,
  Relation,
  UpsertOptions,
} from './declarations.js'
import { expressionBuilder, sql } from 'kysely'
import type {
  SelectExpression,
  DeleteQueryBuilder,
  InsertQueryBuilder,
  Kysely,
  SelectQueryBuilder,
  UpdateQueryBuilder,
  ExpressionBuilder,
  Expression,
} from 'kysely'
import {
  ALL_DIALECTS_OPERATORS,
  applySelectId,
  buildBetween,
  buildJsonbContainment,
  buildJsonbHasKey,
  buildLikePattern,
  buildRegexMatch,
  coerceTemporalQueryProperty,
  convertBooleansToNumbers,
  getDatabaseDialect,
  getOperator,
  getOrderByModifier,
  getSortDirection,
  NON_SQLITE_OPERATORS,
  OPERATORS,
  POSTGRES_ONLY_JSON_OPERATORS,
  POSTGRES_ONLY_OPERATORS,
  temporalKind,
  transformOperatorValue,
  traverseJSON,
} from './utils/index.js'
import { addToQuery } from 'feathers-utils'

// Alias for the window-count column injected into the data query so a paginated
// find can return rows and the grand total in a single round-trip. Stripped
// from every row before the result is returned.
const PAGINATION_TOTAL_KEY = '__fk_total'

const FILTERS = new Set<string>(['$select', '$sort', '$limit', '$skip'])

// Column names and alias prefix of the GROUP BY derived tables that carry a
// `$sort` value. Never selected into a result — the data query selects either
// the service's own table or an explicit `$select`.
const SORT_KEY = '__fk_sort_key'
const SORT_VALUE = '__fk_sort_value'
const SORT_ALIAS_PREFIX = '__fk_sort__'

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

type HandleQueryOptions = {
  tableName?: string | null | undefined
  /**
   * The row source query keys are resolved against. Absent means the service's
   * own table; set for the sub-filter of a hasMany `EXISTS` subquery.
   */
  scope?: RelationScope
}

/**
 * The row source a query key is resolved against: the alias its unqualified
 * columns belong to, plus the relations of the service that owns it.
 */
type RelationScope = {
  alias: string
  relations: Record<string, Relation> | undefined
}

type JoinStep = {
  relation: Relation
  alias: string
  sourceAlias: string
  databaseTableName: string
  sourceKey: string
  targetKey: string
}

/** What a dot-path points at, as resolved by `walkRelationPath`. */
type RelationPathTarget =
  | {
      kind: 'column'
      /** belongsTo hops to join before the column can be referenced */
      steps: JoinStep[]
      columnAlias: string
      columnName: string
    }
  | {
      kind: 'hasMany'
      /** belongsTo hops to join before the subquery can correlate */
      steps: JoinStep[]
      /** alias the `EXISTS` correlates to */
      sourceAlias: string
      relationKey: string
      relation: Relation
      /** path left to resolve inside the related service's scope */
      rest: string[]
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

    let q = this.db(params).selectFrom(this.options.name)
    const applyResult = this.applyJoins(q, filterQueryResult.params, {
      order: options?.order,
    })
    q = applyResult.q
    const query = applyResult.query

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
      q = this.applySort(q, filters, applyResult.sortRefs)

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
   * Normalize the query's relation notation and add the JOINs that `$sort`
   * needs. Relation *filters* never join — they compile to `EXISTS`
   * subqueries, so they cannot duplicate parent rows.
   */
  private applyJoins<Q extends Record<string, any>>(
    q: Q,
    params: Params,
    options: {
      order?: boolean
    },
  ): { q: Q; query: Query; sortRefs: Map<string, string> } {
    let query = params.query || {}
    let sortRefs = new Map<string, string>()
    if (!this.options.relations) return { q, query, sortRefs }

    // Normalize nested belongsTo notation to dot-notation so the JOIN analysis
    // for $sort and the WHERE-clause generation see a single canonical shape.
    query = this.flattenRelationQuery(query)

    if (options.order && query.$sort) {
      const result = this.applyJoinsForOrderBy(q, query.$sort, {
        alreadyJoined: [],
      })
      q = result.q
      sortRefs = result.sortRefs
    }

    return { q, query, sortRefs }
  }

  private rootScope(): RelationScope {
    return { alias: this.options.name, relations: this.options.relations }
  }

  private lookupRelationsForService(
    serviceName: string,
  ): Record<string, Relation> | undefined {
    if (!this.app) return undefined
    try {
      const svc = this.app.service(serviceName)
      return svc?.options?.relations
    } catch {
      return undefined
    }
  }

  /** Collection operator keys present on a query property, if it is an object. */
  private collectionOperatorsIn(value: any): string[] {
    if (!_.isObject(value) || Array.isArray(value)) return []
    return Object.keys(value).filter((key) =>
      KyselyAdapter.COLLECTION_OPERATORS.includes(
        key as (typeof KyselyAdapter.COLLECTION_OPERATORS)[number],
      ),
    )
  }

  /** An empty condition object is a no-op, like `$not: {}` — never an error. */
  private isEmptyConditionObject(value: any): boolean {
    return (
      _.isObject(value) &&
      !Array.isArray(value) &&
      Object.keys(value).length === 0
    )
  }

  private isPlainRelationObject(value: any): boolean {
    if (!value || typeof value !== 'object' || Array.isArray(value))
      return false
    const keys = Object.keys(value)
    if (keys.length === 0) return false
    // Operator-only map (e.g. { $gt: 5 }) or collection operators ({ $some: ... })
    // should be treated as a leaf, not traversed further.
    if (keys.every((k) => k.startsWith('$'))) return false
    return true
  }

  private flattenRelationQuery(
    query: Query,
    relations: Record<string, Relation> | undefined = this.options.relations,
  ): Query {
    if (!relations || !query) return query

    const out: Record<string, any> = {}

    for (const key in query) {
      const value = query[key]

      if (FILTERS.has(key)) {
        out[key] = value
        continue
      }

      if (key === '$and' || key === '$or') {
        if (Array.isArray(value)) {
          out[key] = value.map((sub) =>
            this.flattenRelationQuery(sub, relations),
          )
        } else {
          out[key] = value
        }
        continue
      }

      const relation = relations[key]
      if (relation && !relation.asArray && this.isPlainRelationObject(value)) {
        this.flattenBelongsToInto(
          value,
          [key],
          out,
          this.lookupRelationsForService(relation.service),
        )
        continue
      }

      out[key] = value
    }

    return out
  }

  private flattenBelongsToInto(
    obj: any,
    prefix: string[],
    out: Record<string, any>,
    currentRelations: Record<string, Relation> | undefined,
  ) {
    for (const subKey in obj) {
      const value = obj[subKey]
      const nextRelation = currentRelations?.[subKey]
      if (
        nextRelation &&
        !nextRelation.asArray &&
        this.isPlainRelationObject(value)
      ) {
        this.flattenBelongsToInto(
          value,
          [...prefix, subKey],
          out,
          this.lookupRelationsForService(nextRelation.service),
        )
      } else {
        out[[...prefix, subKey].join('.')] = value
      }
    }
  }

  /**
   * Walk a dot-path from `scope` and report what it points at: a column on the
   * current row source (after zero or more belongsTo hops), or the first
   * hasMany hop, which needs an `EXISTS` subquery and carries the rest of the
   * path to resolve inside it. Returns `null` when the path is not resolvable
   * (unknown segment, incomplete relation, or a path ending on a belongsTo
   * relation, which references a relation and not a column).
   */
  private walkRelationPath(
    parts: string[],
    scope: RelationScope,
  ): RelationPathTarget | null {
    if (!parts.length) return null

    const steps: JoinStep[] = []
    let currentRelations = scope.relations
    let currentAlias = scope.alias
    // Alias chains are namespaced by the scope they were built in, so a
    // subquery never shadows an alias of the query it is correlated to.
    const aliasChain: string[] =
      scope.alias === this.options.name ? [] : [scope.alias]

    for (let i = 0; i < parts.length; i++) {
      const key = parts[i]
      const isLast = i === parts.length - 1
      const relation = currentRelations?.[key]

      if (!relation) {
        // Only the last segment may be a column; anything earlier has to be a
        // relation for the path to resolve.
        if (!isLast) return null

        return {
          kind: 'column',
          steps,
          columnAlias: currentAlias,
          columnName: key,
        }
      }

      if (
        !relation.databaseTableName ||
        !relation.keyHere ||
        !relation.keyThere
      ) {
        return null
      }

      if (relation.asArray) {
        return {
          kind: 'hasMany',
          steps,
          sourceAlias: currentAlias,
          relationKey: key,
          relation,
          rest: parts.slice(i + 1),
        }
      }

      // A path ending on a belongsTo relation points at a relation, not a
      // column — `{ user: {...} }` is normalized into dot-paths before it gets
      // here, so anything left is unresolvable.
      if (isLast) return null

      aliasChain.push(key)
      const alias = aliasChain.join('__')

      if (steps.some((step) => step.alias === alias)) return null

      steps.push({
        relation,
        alias,
        sourceAlias: currentAlias,
        databaseTableName: relation.databaseTableName,
        sourceKey: relation.keyHere,
        targetKey: relation.keyThere,
      })

      currentAlias = alias
      currentRelations = this.lookupRelationsForService(relation.service)
    }

    return null
  }

  /**
   * Resolve a dot-path to a (joined) column. Used by the JOIN passes and by
   * `$sort`, which can only order by a column — a path through a hasMany or one
   * ending on a relation resolves to `null` here.
   */
  private resolveRelationPath(
    parts: string[],
    scope?: RelationScope,
  ): {
    steps: JoinStep[]
    columnAlias: string
    columnName: string
    isSimpleColumn: boolean
  } | null {
    if (!parts.length) return null

    const currentScope = scope ?? this.rootScope()

    if (parts.length === 1) {
      return {
        steps: [],
        columnAlias: currentScope.alias,
        columnName: parts[0],
        isSimpleColumn: true,
      }
    }

    const target = this.walkRelationPath(parts, currentScope)
    if (!target || target.kind !== 'column') return null

    return {
      steps: target.steps,
      columnAlias: target.columnAlias,
      columnName: target.columnName,
      isSimpleColumn: target.steps.length === 0,
    }
  }

  private static readonly COLLECTION_OPERATORS = [
    '$none',
    '$some',
    '$every',
  ] as const

  /**
   * Express a chain of belongsTo hops as a correlated `EXISTS` subquery
   * instead of joins on the outer builder: the first hop becomes the
   * subquery's FROM and correlation, every further hop an INNER JOIN inside
   * it. `buildInner` contributes the condition on the row the chain arrives
   * at. Used where the outer builder cannot take a join (UPDATE/DELETE) or
   * where a joined predicate would sit inside a negation (`$not`).
   */
  private buildBelongsToExists(
    eb: ExpressionBuilder<any, any>,
    steps: JoinStep[],
    buildInner: (
      subEb: ExpressionBuilder<any, any>,
    ) => Expression<any> | undefined,
  ): Expression<any> | undefined {
    if (!steps.length) return

    const [first, ...rest] = steps

    let sub = eb
      .selectFrom(`${first.databaseTableName} as ${first.alias}`)
      .select(sql`1` as any)

    for (const step of rest) {
      sub = sub.innerJoin(
        `${step.databaseTableName} as ${step.alias}`,
        `${step.alias}.${step.targetKey}`,
        `${step.sourceAlias}.${step.sourceKey}`,
      )
    }

    const whereRef = sub.where((subEb: ExpressionBuilder<any, any>) => {
      const inner = buildInner(subEb)

      return subEb.and([
        subEb(
          `${first.alias}.${first.targetKey}`,
          '=',
          subEb.ref(`${first.sourceAlias}.${first.sourceKey}`),
        ),
        ...(inner ? [inner] : []),
      ])
    })

    return eb.exists(whereRef)
  }

  /**
   * Build the correlated `EXISTS` / `NOT EXISTS` for one hasMany hop. The
   * child filter is resolved in the related service's own scope, so it may
   * itself contain relation paths — belongsTo hops become joins on the
   * subquery, further hasMany hops become nested `EXISTS`.
   */
  private buildHasManyExists(
    eb: ExpressionBuilder<any, any>,
    target: Extract<RelationPathTarget, { kind: 'hasMany' }>,
    filterQuery: any,
    operator: '$some' | '$none' | '$every',
  ): Expression<any> | undefined {
    const { relation, relationKey, sourceAlias, rest } = target
    if (!relation.databaseTableName) return

    // Namespaced by the correlating alias so a nested or self-referencing
    // hasMany never shadows the row source it is correlated to.
    const alias =
      sourceAlias === this.options.name
        ? relationKey
        : `${sourceAlias}__${relationKey}`

    const scope: RelationScope = {
      alias,
      relations: this.lookupRelationsForService(relation.service),
    }

    // A dot-path that continues past the hop (`categories.type.name`) becomes a
    // filter in the child scope.
    const childInput =
      rest.length > 0 ? { [rest.join('.')]: filterQuery } : filterQuery

    if (!_.isObject(childInput) || Array.isArray(childInput)) return

    const childQuery = this.flattenRelationQuery(
      childInput as Query,
      scope.relations,
    )

    const sub = eb
      .selectFrom(`${relation.databaseTableName} as ${alias}`)
      .select(sql`1` as any)

    const whereRef = sub.where((subEb: ExpressionBuilder<any, any>) => {
      const conditions = this.handleQuery(subEb, childQuery, { scope }) ?? []

      // For $every we negate the filter conditions:
      // "every child matches X" = "no child exists that does NOT match X"
      const filterConditions =
        operator === '$every' && conditions.length
          ? [subEb.not(subEb.and(conditions))]
          : conditions

      return subEb.and([
        subEb(
          `${alias}.${relation.keyThere}`,
          '=',
          subEb.ref(`${sourceAlias}.${relation.keyHere}`),
        ),
        ...filterConditions,
      ])
    })

    // $some uses EXISTS, $none and $every use NOT EXISTS
    if (operator === '$some') {
      return eb.exists(whereRef)
    }
    return eb.not(eb.exists(whereRef))
  }

  /**
   * Resolve a query key that references a relation, in `scope`. Returns
   * `undefined` when the key is not a relation reference (a plain column) or
   * cannot be resolved, so the caller can fall back to normal handling.
   */
  private handleRelation(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
    scope: RelationScope,
  ): Expression<any> | undefined {
    if (!scope.relations) return

    const parts = queryKey.split('.')

    // Nested notation on a belongsTo relation. The top-level query is
    // normalized in `flattenRelationQuery` before it gets here; this covers
    // sub-filters built inside an EXISTS subquery.
    const direct = parts.length === 1 ? scope.relations[queryKey] : undefined
    if (
      direct &&
      !direct.asArray &&
      this.isPlainRelationObject(queryProperty) &&
      !Object.keys(queryProperty).some((key) =>
        KyselyAdapter.COLLECTION_OPERATORS.includes(key as any),
      )
    ) {
      const flattened: Record<string, any> = {}
      this.flattenBelongsToInto(
        queryProperty,
        [queryKey],
        flattened,
        this.lookupRelationsForService(direct.service),
      )

      const conditions = this.handleQuery(eb, flattened, { scope })
      return conditions?.length ? eb.and(conditions) : undefined
    }

    const target = this.walkRelationPath(parts, scope)

    // A collection operator is only meaningful on a hasMany relation. Anywhere
    // else it is a client error — dropping it silently would widen the result
    // set, which is exactly how an authorization filter turns into a leak.
    const collectionOperators = this.collectionOperatorsIn(queryProperty)
    if (
      collectionOperators.length &&
      (!target || target.kind !== 'hasMany' || target.rest.length > 0)
    ) {
      throw new BadRequest(
        `Invalid query: '${collectionOperators[0]}' is only valid on a hasMany relation, but '${queryKey}' is not one`,
        { [queryKey]: collectionOperators },
      )
    }

    if (!target) {
      // A path that starts at a declared relation but does not resolve is a
      // broken chain (typo, missing `app.setup()`, non-Kysely service). Keys
      // that do not start at a relation are left to the caller — they may be a
      // plain column or an already-qualified ref.
      if (
        scope.relations[parts[0]] &&
        !this.isEmptyConditionObject(queryProperty)
      ) {
        throw new BadRequest(
          `Invalid query: '${queryKey}' does not resolve to a column through the relations of '${scope.alias}'`,
          { [queryKey]: queryProperty },
        )
      }

      return
    }

    if (target.kind === 'column') {
      // No hops — a plain column, which the caller handles.
      if (target.steps.length === 0) return

      // The belongsTo prefix becomes a semi-join, never a JOIN on the outer
      // builder: EXISTS cannot duplicate parent rows, needs no null-protect,
      // and stays correct inside a negation and in UPDATE/DELETE.
      return this.buildBelongsToExists(eb, target.steps, (subEb) =>
        this.handleQueryPropertyNormal(
          subEb,
          `${target.columnAlias}.${target.columnName}`,
          queryProperty,
          { tableName: null },
        ),
      )
    }

    // A dot-path continuing past the hop always means "at least one child
    // matches" — $none / $every are only expressible in nested notation.
    const buildHasMany = (
      innerEb: ExpressionBuilder<any, any>,
    ): Expression<any> | undefined => {
      if (target.rest.length > 0) {
        return this.buildHasManyExists(innerEb, target, queryProperty, '$some')
      }

      if (!_.isObject(queryProperty) || Array.isArray(queryProperty)) return

      const results: Expression<any>[] = []
      const regularFilters: Record<string, any> = {}

      for (const subKey in queryProperty) {
        if (
          KyselyAdapter.COLLECTION_OPERATORS.includes(
            subKey as (typeof KyselyAdapter.COLLECTION_OPERATORS)[number],
          )
        ) {
          const expr = this.buildHasManyExists(
            innerEb,
            target,
            (queryProperty as Record<string, any>)[subKey],
            subKey as '$none' | '$some' | '$every',
          )
          if (expr) results.push(expr)
        } else {
          regularFilters[subKey] = (queryProperty as Record<string, any>)[
            subKey
          ]
        }
      }

      // Regular filters without an explicit operator default to $some
      if (Object.keys(regularFilters).length > 0) {
        const expr = this.buildHasManyExists(
          innerEb,
          target,
          regularFilters,
          '$some',
        )
        if (expr) results.push(expr)
      }

      if (results.length === 1) return results[0]
      if (results.length > 1) return innerEb.and(results)
      return undefined
    }

    // The hop correlates to the alias its belongsTo prefix arrives at, so the
    // hasMany condition sits inside the EXISTS over that prefix.
    return target.steps.length === 0
      ? buildHasMany(eb)
      : this.buildBelongsToExists(eb, target.steps, buildHasMany)
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

  private handleJson(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
  ) {
    if (!queryKey.includes('.')) {
      return
    }

    const parts = queryKey.split('.')

    const type = this.getPropertyType(parts[0])

    if (type !== 'json' && type !== 'jsonb') {
      return
    }

    const column = traverseJSON(
      this.col(parts[0]),
      parts.slice(1),
      this.options.dialectType,
    )

    return this.buildPropertyExpression(eb, column, queryProperty)
  }

  private buildPropertyExpression(
    eb: ExpressionBuilder<any, any>,
    column: any,
    queryProperty: any,
    propertyType?: string,
  ) {
    if (_.isObject(queryProperty)) {
      const qs: any[] = []
      // loop through OPERATORS and apply them
      for (const operator in queryProperty) {
        const value = (queryProperty as Record<string, any>)[operator]

        if (
          (operator === '$in' || operator === '$nin') &&
          Array.isArray(value) &&
          value.length === 0
        ) {
          qs.push(
            operator === '$in' ? sql<boolean>`1 = 0` : sql<boolean>`1 = 1`,
          )
          continue
        }

        // For a `jsonb`/`json` column the Postgres containment/overlap operators
        // need jsonb operands - the native-array codegen in
        // `transformOperatorValue` (`@> ARRAY[...]::text[]`) is only valid for
        // genuine `text[]`/`integer[]` columns.
        if (
          (propertyType === 'jsonb' || propertyType === 'json') &&
          (operator === '$contains' ||
            operator === '$contained' ||
            operator === '$overlap')
        ) {
          qs.push(buildJsonbContainment(eb, column, operator, value))
          continue
        }

        // Branch operators that can't be expressed as `eb(column, op, value)`:
        // range, prefix/suffix LIKE, regex, and jsonb key-existence.
        if (operator === '$between' || operator === '$notBetween') {
          qs.push(buildBetween(column, operator, value))
          continue
        }

        if (operator === '$startsWith' || operator === '$endsWith') {
          qs.push(buildLikePattern(column, operator, value))
          continue
        }

        if (operator === '$regex' || operator === '$notRegex') {
          qs.push(
            buildRegexMatch(column, operator, this.options.dialectType, value),
          )
          continue
        }

        if (
          operator === '$hasKey' ||
          operator === '$hasKeyAny' ||
          operator === '$hasKeyAll'
        ) {
          qs.push(buildJsonbHasKey(column, operator, value))
          continue
        }

        const op = getOperator(operator, value, this.options.dialectType)
        if (!op) continue

        qs.push(
          eb(column, op, transformOperatorValue(operator, value, propertyType)),
        )
      }

      if (qs.length) {
        return eb.and(qs)
      }

      // An operator-only object that produced no condition is unresolvable —
      // e.g. a collection operator (`{ $some: ... }`) on a key that is not a
      // hasMany relation, or a property-level `$not`. Falling through to the
      // equality branch would compare the column against the raw object and
      // emit invalid SQL, so drop the condition instead.
      const operatorKeys = Object.keys(queryProperty as Record<string, any>)
      if (
        operatorKeys.length > 0 &&
        operatorKeys.every((key) => key.startsWith('$'))
      ) {
        return
      }

      // no operators matched - fall through to simple equality check
    }

    const op = getOperator('$eq', queryProperty, this.options.dialectType)
    if (!op) return
    return eb(column, op, queryProperty)
  }

  private handleQueryPropertyNormal(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
    options?: HandleQueryOptions,
  ) {
    if (queryKey === '$and' || queryKey === '$or') {
      // Explicit boolean-identity semantics for an empty operand: an empty `$and`
      // matches everything (1 = 1), an empty `$or` matches nothing (1 = 0). This
      // prevents an authorization hook that injects `$or: []` (e.g. derived from
      // an empty list of permitted scopes) from silently matching all rows.
      if (Array.isArray(queryProperty) && queryProperty.length === 0) {
        return queryKey === '$and' ? sql<boolean>`1 = 1` : sql<boolean>`1 = 0`
      }

      const method = eb[queryKey === '$and' ? 'and' : 'or']
      const subs = []
      for (const subQuery of queryProperty) {
        const result = this.handleQuery(eb, subQuery, options)

        if (result?.length) subs.push(eb.and(result))
      }

      return subs?.length ? method(subs) : undefined
    }

    if (queryKey === '$not') {
      // Negate the whole condition object at the DB level: NOT (k1 AND k2 ...).
      // Operator-agnostic and correct for multi-key conditions, unlike a
      // per-property inversion.
      //
      const result = this.handleQuery(eb, queryProperty, options)
      return result?.length ? eb.not(eb.and(result)) : undefined
    }

    // An explicit `tableName` wins (including `null`, meaning "already
    // qualified"); otherwise a scope qualifies with its own alias.
    const tableName =
      options && 'tableName' in options
        ? options.tableName
        : options?.scope?.alias

    const col = this.col(queryKey, { tableName })

    // Opt-in, type-aware date coercion: when the column is declared temporal
    // (via `getPropertyType` or an `x-db-type` schema annotation), normalize
    // Date / ISO-string / epoch-ms / "YYYY-MM-DD" query values into the
    // canonical string the driver compares correctly.
    const dbType = this.getPropertyType(queryKey)
    const kind = temporalKind(dbType)
    const property = kind
      ? coerceTemporalQueryProperty(queryProperty, kind)
      : queryProperty

    return this.buildPropertyExpression(eb, col, property, dbType)
  }

  /**
   * Resolve what a `$sort` key should order by, adding whatever the ordering
   * needs to the builder.
   *
   * A to-one hop is only joined when the adapter can *prove* the target column
   * is unique — `keyThere` is the target service's id. Otherwise, and for every
   * to-many hop, the ordering value comes from a `GROUP BY` derived table:
   * exactly one row per key, so it cannot duplicate parent rows the way a JOIN
   * on a non-unique column does.
   *
   * Returns a map from sort key to the reference `applySort` should order by;
   * keys absent from it are ordered by their own column.
   */
  private applyJoinsForOrderBy<Q extends Record<string, any>>(
    q: Q,
    $sort: SortFilter,
    options: {
      alreadyJoined: string[]
    },
  ): { q: Q; sortRefs: Map<string, string> } {
    const sortRefs = new Map<string, string>()
    if (!this.options.relations || !$sort) return { q, sortRefs }

    for (const key in $sort) {
      if (!key.includes('.')) continue

      const parts = key.split('.')
      const scope = this.rootScope()
      const target = this.walkRelationPath(parts, scope)

      if (!target) {
        // Mirrors the filter rules: a path that starts at a declared relation
        // has to resolve; anything else may be a legitimate qualified ref.
        if (scope.relations?.[parts[0]]) {
          throw new BadRequest(
            `Invalid $sort: '${key}' does not resolve to a column through the relations of '${scope.alias}'`,
            { $sort: key },
          )
        }
        continue
      }

      const agg = this.sortAggregate($sort[key])

      if (target.kind === 'column') {
        if (target.steps.length === 0) continue

        if (target.steps.every((step) => this.isProvablyUniqueToOne(step))) {
          for (const step of target.steps) {
            if (options.alreadyJoined.includes(step.alias)) continue

            q = q.leftJoin(
              `${step.databaseTableName} as ${step.alias}`,
              `${step.alias}.${step.targetKey}`,
              `${step.sourceAlias}.${step.sourceKey}`,
            )

            options.alreadyJoined.push(step.alias)
          }

          sortRefs.set(key, `${target.columnAlias}.${target.columnName}`)
          continue
        }

        // At least one hop is not provably unique — aggregate the chain.
        const [first, ...rest] = target.steps
        const result = this.addSortDerivedTable(q, {
          fromTable: first.databaseTableName,
          fromAlias: first.alias,
          groupKey: first.targetKey,
          outerRef: `${first.sourceAlias}.${first.sourceKey}`,
          innerJoins: rest,
          valueRef: `${target.columnAlias}.${target.columnName}`,
          agg,
        })
        q = result.q
        sortRefs.set(key, result.ref)
        continue
      }

      // hasMany. Only a to-many directly on this service is supported: behind a
      // to-one prefix the derived table would have to correlate to an alias the
      // outer query may not be able to join.
      if (target.steps.length > 0) {
        throw new BadRequest(
          `Invalid $sort: '${key}' sorts by a hasMany relation behind another relation, which is not supported`,
          { $sort: key },
        )
      }

      const childScope: RelationScope = {
        alias: target.relationKey,
        relations: this.lookupRelationsForService(target.relation.service),
      }

      const inner = target.rest.length
        ? this.walkRelationPath(target.rest, childScope)
        : null

      if (!inner || inner.kind !== 'column') {
        throw new BadRequest(
          `Invalid $sort: '${key}' does not resolve to a column of '${target.relation.service}'`,
          { $sort: key },
        )
      }

      const value = $sort[key]
      const filter =
        typeof value === 'object' && value !== null && 'filter' in value
          ? (value as { filter?: Record<string, any> }).filter
          : undefined

      const result = this.addSortDerivedTable(q, {
        fromTable: target.relation.databaseTableName!,
        fromAlias: target.relationKey,
        groupKey: target.relation.keyThere,
        outerRef: `${target.sourceAlias}.${target.relation.keyHere}`,
        innerJoins: inner.steps,
        valueRef: `${inner.columnAlias}.${inner.columnName}`,
        agg,
        filter,
        filterScope: childScope,
      })
      q = result.q
      sortRefs.set(key, result.ref)
    }

    return { q, sortRefs }
  }

  /** MIN for an ascending sort, MAX for a descending one. */
  private sortAggregate(value: SortFilter[string]): 'min' | 'max' {
    const direction = getSortDirection(value)
    return direction === -1 ||
      direction === 'desc' ||
      direction === 'desc nulls first' ||
      direction === 'desc nulls last'
      ? 'max'
      : 'min'
  }

  private lookupIdFieldForService(serviceName: string): string | undefined {
    if (!this.app) return undefined
    try {
      return this.app.service(serviceName)?.options?.id
    } catch {
      return undefined
    }
  }

  /**
   * Whether a to-one hop is guaranteed to match at most one row, which is what
   * makes a plain JOIN safe. `asArray: false` is the caller's intent, not a
   * guarantee — but `keyThere` being the target service's id is one, and it is
   * how belongsTo is declared in practice.
   *
   * Falls back to this service's own id when the relation points at this very
   * table, so a self-referencing hop stays on the fast path without an app.
   */
  private isProvablyUniqueToOne(step: JoinStep): boolean {
    const idField =
      this.lookupIdFieldForService(step.relation.service) ??
      (step.databaseTableName === this.options.name
        ? this.options.id
        : undefined)

    return !!idField && step.targetKey === idField
  }

  /**
   * `LEFT JOIN (SELECT <key>, MIN|MAX(<value>) ... GROUP BY <key>)`.
   *
   * One row per key by construction, so it cannot multiply the rows it is
   * joined to. It also replaces a correlated aggregate evaluated once per
   * candidate row with a single aggregate pass.
   */
  private addSortDerivedTable<Q extends Record<string, any>>(
    q: Q,
    spec: {
      fromTable: string
      fromAlias: string
      groupKey: string
      outerRef: string
      innerJoins: JoinStep[]
      valueRef: string
      agg: 'min' | 'max'
      filter?: Record<string, any>
      filterScope?: RelationScope
    },
  ): { q: Q; ref: string } {
    const derivedAlias = `${SORT_ALIAS_PREFIX}${spec.fromAlias}`

    q = q.leftJoin(
      (eb: any) => {
        let sub = eb
          .selectFrom(`${spec.fromTable} as ${spec.fromAlias}`)
          .select((seb: any) => [
            seb.ref(`${spec.fromAlias}.${spec.groupKey}`).as(SORT_KEY),
            seb.fn[spec.agg](spec.valueRef).as(SORT_VALUE),
          ])
          .groupBy(`${spec.fromAlias}.${spec.groupKey}`)

        for (const step of spec.innerJoins) {
          sub = sub.innerJoin(
            `${step.databaseTableName} as ${step.alias}`,
            `${step.alias}.${step.targetKey}`,
            `${step.sourceAlias}.${step.sourceKey}`,
          )
        }

        if (spec.filter && Object.keys(spec.filter).length) {
          sub = sub.where((seb: any) => {
            const conditions =
              this.handleQuery(seb, spec.filter as Query, {
                scope: spec.filterScope,
              }) ?? []
            return seb.and(conditions)
          })
        }

        return sub.as(derivedAlias)
      },
      (join: any) =>
        join.onRef(`${derivedAlias}.${SORT_KEY}`, '=', spec.outerRef),
    ) as Q

    return { q, ref: `${derivedAlias}.${SORT_VALUE}` }
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

  applyWhere<Q extends Record<string, any>>(
    q: Q,
    query: Query,
    options?: HandleQueryOptions,
  ) {
    // loop through params and call the where filters

    if (!query || Object.keys(query).length === 0) {
      return q
    }

    const eb = expressionBuilder()

    const result = this.handleQuery(eb, query, options)

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
    if (FILTERS.has(queryKey)) {
      return undefined
    }

    // A `$`-prefixed key is an operator, never a column. `$and`/`$or`/`$not`
    // are handled in handleQueryPropertyNormal; anything else reaching here is
    // unresolvable — e.g. a collection operator on a belongsTo relation, which
    // would otherwise be qualified into a column ref like `"user"."$some"`.
    if (
      queryKey.startsWith('$') &&
      queryKey !== '$and' &&
      queryKey !== '$or' &&
      queryKey !== '$not'
    ) {
      return undefined
    }

    const scope = options?.scope
    // A `tableName` without a scope is the legacy nested-belongsTo context: we
    // know which table the columns belong to, but not its relations, so
    // relation paths cannot be resolved there.
    const isLegacyScope = !scope && !!options?.tableName

    if (!isLegacyScope) {
      const relation = this.handleRelation(
        eb,
        queryKey,
        queryProperty,
        scope ?? this.rootScope(),
      )

      if (relation) return relation
    }

    // JSON traversal reads this service's column types and qualifies with its
    // table, so it only applies to the service's own row source.
    if (!scope) {
      const json = this.handleJson(eb, queryKey, queryProperty)

      if (json) return json
    }

    // Unresolved dot-paths must not leak into WHERE as raw column refs.
    // A path reaches this point only if none of the handlers above claimed
    // it. We skip it when either:
    //   - the first segment matches a known relation (broken chain, e.g.
    //     'user.bogus.name'), or
    //   - the path has 2+ separators (multi-segment paths are only valid
    //     as relation chains or JSON access, both of which would have been
    //     caught above; anything else is almost certainly unintended).
    // Single-dot paths whose first segment is NOT a known relation are
    // left alone — they may be legitimate qualified refs like
    // `alias.column` added by addToQuery null-protect on a prior hop.
    if (queryKey.includes('.')) {
      if (isLegacyScope) return undefined

      const parts = queryKey.split('.')
      if (parts.length > 2) return undefined

      const relations = scope ? scope.relations : this.options.relations
      if (relations?.[parts[0]]) return undefined

      if (scope) {
        // Inside a subquery only aliases derived from its own scope are in the
        // FROM clause; any other qualified ref would point at a table it never
        // joined. JSON traversal is not available here either, so there is no
        // reading under which such a path is valid.
        if (
          parts[0] !== scope.alias &&
          !parts[0].startsWith(`${scope.alias}__`)
        ) {
          throw new BadRequest(
            `Invalid query: '${queryKey}' does not resolve to a column or relation of '${scope.alias}'`,
            { [queryKey]: queryProperty },
          )
        }

        return this.handleQueryPropertyNormal(eb, queryKey, queryProperty, {
          tableName: null,
        })
      }
    }

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
    sortRefs?: Map<string, string>,
  ) {
    if (!filters.$sort) return q

    for (const key in filters.$sort) {
      const value = filters.$sort[key]

      // `applyJoinsForOrderBy` already decided how each relation path is
      // reached and added the join or derived table it needs.
      const ref = sortRefs?.get(key)

      q = q.orderBy(ref ?? this.col(key), getOrderByModifier(value)) as any
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
      const withWhere = this.applyWhere(q, query)
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
