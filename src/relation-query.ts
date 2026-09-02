import { _ } from '@feathersjs/commons'
import { BadRequest } from '@feathersjs/errors'
import type { Query } from '@feathersjs/feathers'
import { expressionBuilder, sql } from 'kysely'
import type { Expression, ExpressionBuilder, SelectQueryBuilder } from 'kysely'

import type { DialectType, Relation, SortFilter } from './declarations.js'
import {
  buildBetween,
  buildJsonbContainment,
  buildJsonbHasKey,
  buildLikePattern,
  buildRegexMatch,
  coerceTemporalQueryProperty,
  getOperator,
  getOrderByModifier,
  getSortDirection,
  qualifyColumn,
  temporalKind,
  transformOperatorValue,
  traverseJSON,
} from './utils/index.js'

/**
 * Compiles the relation-aware parts of a Feathers query into Kysely: the WHERE
 * clause and the ORDER BY, including paths that walk belongsTo and hasMany
 * relations to any depth.
 *
 * Two rules shape everything here:
 *
 *  - **A filter is a semi-join.** Every relation hop in a filter becomes a
 *    correlated `EXISTS`, so a filter can never duplicate the parent rows it
 *    narrows, needs no null-protect, and stays correct under negation and in
 *    UPDATE/DELETE.
 *  - **A sort needs the value, not its existence.** It joins — plainly when
 *    the hop is provably unique, through a `GROUP BY` derived table otherwise.
 *
 * The instance holds no database and no Feathers app: related services are
 * reached through `lookupService`, so a caller can resolve a multi-hop chain
 * with a plain object.
 */

const FILTERS = new Set<string>(['$select', '$sort', '$limit', '$skip'])

const COLLECTION_OPERATORS = ['$none', '$some', '$every'] as const

// Column names and alias prefix of the GROUP BY derived tables that carry a
// `$sort` value. Never selected into a result — the data query selects either
// the service's own table or an explicit `$select`.
const SORT_KEY = '__fk_sort_key'
const SORT_VALUE = '__fk_sort_value'
const SORT_ALIAS_PREFIX = '__fk_sort_'

/** What the relation layer needs to know about a *related* service. */
export type RelatedService = {
  relations?: Record<string, Relation> | undefined
  id?: string | undefined
}

export type RelationQueryContext = {
  /** Table the query selects from; also the alias for unqualified columns. */
  tableName: string
  /** Primary key column, used to prove a to-one hop matches at most one row. */
  idField: string
  dialectType: DialectType | undefined
  relations?: Record<string, Relation> | undefined
  /** Whether a bare name is a declared column of `tableName`. */
  isOwnColumn: (column: string) => boolean
  /** Database type of a column, for JSON traversal and temporal coercion. */
  getPropertyType: (property: string) => string | undefined
  /**
   * Relation metadata of another service, or `undefined` when it cannot be
   * resolved — not registered, not a Kysely service, or no app at all. An
   * unresolvable hop is reported as a `BadRequest`, never silently dropped.
   */
  lookupService: (name: string) => RelatedService | undefined
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

export class RelationQuery {
  constructor(private readonly ctx: RelationQueryContext) {}

  /**
   * Add the query's WHERE clause, relations included.
   *
   * Nested relation notation is normalized here, so callers hand over whatever
   * shape the client sent.
   */
  applyWhere<Q extends Record<string, any>>(q: Q, query: Query | undefined): Q {
    if (!query || Object.keys(query).length === 0) return q

    const normalized = this.ctx.relations
      ? this.flattenRelationQuery(query)
      : query

    const result = this.handleQuery(expressionBuilder(), normalized)

    return result?.length
      ? q.where((eb: ExpressionBuilder<any, any>) => eb.and(result))
      : q
  }

  /**
   * Add the query's ORDER BY, along with whatever each key needs to be
   * reachable: a JOIN for a provably unique to-one hop, a `GROUP BY` derived
   * table otherwise.
   *
   * Sorting is the one path that still joins — it needs the related value, not
   * just its existence.
   */
  applyOrder<Q extends SelectQueryBuilder<any, string, any>>(
    q: Q,
    $sort: SortFilter | undefined,
  ): Q {
    if (!$sort) return q

    let sortRefs = new Map<string, string>()

    if (this.ctx.relations) {
      const resolved = this.applyJoinsForOrderBy(q, $sort, {
        alreadyJoined: [],
      })
      q = resolved.q
      sortRefs = resolved.sortRefs
    }

    for (const key in $sort) {
      q = q.orderBy(
        sortRefs.get(key) ?? this.col(key),
        getOrderByModifier($sort[key]),
      ) as Q
    }

    return q
  }

  /** `qualifyColumn` bound to the row source this instance resolves against. */
  private col<T>(
    column: T,
    options?: { tableName: string | null | undefined },
  ): T {
    return qualifyColumn(column, {
      tableName: options?.tableName,
      ownTable: this.ctx.tableName,
      isOwnColumn: this.ctx.isOwnColumn,
    })
  }

  private rootScope(): RelationScope {
    return { alias: this.ctx.tableName, relations: this.ctx.relations }
  }

  private lookupRelationsForService(
    serviceName: string,
  ): Record<string, Relation> | undefined {
    return this.ctx.lookupService(serviceName)?.relations
  }

  /** Collection operator keys present on a query property, if it is an object. */
  private collectionOperatorsIn(value: any): string[] {
    if (!_.isObject(value) || Array.isArray(value)) return []
    return Object.keys(value).filter((key) =>
      COLLECTION_OPERATORS.includes(
        key as (typeof COLLECTION_OPERATORS)[number],
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
    relations: Record<string, Relation> | undefined = this.ctx.relations,
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
      scope.alias === this.ctx.tableName ? [] : [scope.alias]

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
      sourceAlias === this.ctx.tableName
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
        COLLECTION_OPERATORS.includes(key as any),
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
          COLLECTION_OPERATORS.includes(
            subKey as (typeof COLLECTION_OPERATORS)[number],
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

  private handleJson(
    eb: ExpressionBuilder<any, any>,
    queryKey: string,
    queryProperty: any,
  ) {
    if (!queryKey.includes('.')) {
      return
    }

    const parts = queryKey.split('.')

    const type = this.ctx.getPropertyType(parts[0])

    if (type !== 'json' && type !== 'jsonb') {
      return
    }

    const column = traverseJSON(
      this.col(parts[0]),
      parts.slice(1),
      this.ctx.dialectType,
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
            buildRegexMatch(column, operator, this.ctx.dialectType, value),
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

        const op = getOperator(operator, value, this.ctx.dialectType)
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

    const op = getOperator('$eq', queryProperty, this.ctx.dialectType)
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
    const dbType = this.ctx.getPropertyType(queryKey)
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
    if (!this.ctx.relations || !$sort) return { q, sortRefs }

    let derivedCount = 0

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
          index: derivedCount++,
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
        index: derivedCount++,
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
    return this.ctx.lookupService(serviceName)?.id
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
      (step.databaseTableName === this.ctx.tableName
        ? this.ctx.idField
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
      /** Position among the sort keys that need one, to keep aliases unique. */
      index: number
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
    // Keyed on the position, not on the relation: two sort keys may aggregate
    // the same relation (`{ 'todos.text': 1, 'todos.userId': -1 }`) and would
    // otherwise join two derived tables under one alias.
    const derivedAlias = `${SORT_ALIAS_PREFIX}${spec.index}__${spec.fromAlias}`

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

      const relations = scope ? scope.relations : this.ctx.relations
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

  /**
   * Add a returning statement alias for each key (bypasses bug in sqlite)
   * @param q kysely query builder
   * @param data data which is expected to be returned
   */
}
