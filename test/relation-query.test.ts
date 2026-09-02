import { Kysely, SqliteDialect } from 'kysely'
import Database from 'better-sqlite3'
import assert from 'node:assert'
import { describe, it } from 'vitest'

import { RelationQuery } from '../src/relation-query.js'
import type {
  RelatedService,
  RelationQueryContext,
} from '../src/relation-query.js'

/**
 * `RelationQuery` holds no database and no Feathers app: related services are
 * reached through `lookupService`. These tests drive it directly with a plain
 * object, so a multi-hop chain can be exercised without registering services,
 * calling `app.setup()`, or creating a schema.
 */

const db = new Kysely<any>({
  dialect: new SqliteDialect({ database: new Database(':memory:') }),
})

const SERVICES: Record<string, RelatedService> = {
  assignments: {
    id: 'id',
    relations: {
      customer: {
        service: 'customers',
        keyHere: 'customerId',
        keyThere: 'id',
        asArray: false,
        databaseTableName: 'customers',
      },
      categories: {
        service: 'categories',
        keyHere: 'id',
        keyThere: 'assignmentId',
        asArray: true,
        databaseTableName: 'categories',
      },
    },
  },
  customers: { id: 'id' },
  categories: {
    id: 'id',
    relations: {
      type: {
        service: 'types',
        keyHere: 'typeId',
        keyThere: 'id',
        asArray: false,
        databaseTableName: 'types',
      },
    },
  },
  types: { id: 'id' },
}

const context = (
  overrides: Partial<RelationQueryContext> = {},
): RelationQueryContext => ({
  tableName: 'events',
  idField: 'id',
  dialectType: 'sqlite',
  relations: {
    assignment: {
      service: 'assignments',
      keyHere: 'assignmentId',
      keyThere: 'id',
      asArray: false,
      databaseTableName: 'assignments',
    },
  },
  isOwnColumn: (column) => ['id', 'name', 'assignmentId'].includes(column),
  getPropertyType: () => undefined,
  lookupService: (name) => SERVICES[name],
  ...overrides,
})

const compileWhere = (
  query: Record<string, any>,
  overrides?: Partial<RelationQueryContext>,
) =>
  new RelationQuery(context(overrides))
    .applyWhere(db.selectFrom('events').selectAll('events'), query)
    .compile().sql

const compileOrder = (
  $sort: Record<string, any>,
  overrides?: Partial<RelationQueryContext>,
) =>
  new RelationQuery(context(overrides))
    .applyOrder(db.selectFrom('events').selectAll('events'), $sort)
    .compile().sql

describe('RelationQuery', () => {
  it('resolves a plain column against the configured table', () => {
    assert.match(compileWhere({ name: 'e-1' }), /where "events"."name" = \?/)
  })

  it('compiles a to-one hop as a semi-join, not a join', () => {
    const sql = compileWhere({ 'assignment.title': 'a-1' })

    assert.match(
      sql,
      /where exists \(select 1 from "assignments" as "assignment"/,
    )
    assert.doesNotMatch(sql, /left join/)
  })

  it('walks a chain through the injected service metadata', () => {
    const sql = compileWhere({ 'assignment.customer.fullName': 'c-1' })

    // second hop resolved from SERVICES.assignments.relations.customer
    assert.match(sql, /inner join "customers" as "assignment__customer"/)
  })

  it('nests a to-many hop inside its to-one prefix', () => {
    const sql = compileWhere({
      assignment: { categories: { $some: { typeId: 1 } } },
    })

    assert.match(sql, /exists \(select 1 from "assignments"/)
    assert.match(
      sql,
      /exists \(select 1 from "categories" as "assignment__categories"/,
    )
  })

  it('rejects a chain whose next hop cannot be resolved', () => {
    // `assignment` resolves, but nothing knows the service it points at
    assert.throws(
      () =>
        compileWhere(
          { 'assignment.customer.fullName': 'c-1' },
          { lookupService: () => undefined },
        ),
      (error: any) => {
        assert.strictEqual(error.name, 'BadRequest')
        return true
      },
    )
  })

  it('joins a to-one sort hop when the target id proves it unique', () => {
    const sql = compileOrder({ 'assignment.title': 1 })

    assert.match(sql, /left join "assignments" as "assignment"/)
    assert.doesNotMatch(sql, /group by/)
  })

  it('aggregates a to-one sort hop that cannot be proven unique', () => {
    // keyThere is not the target's id, so a join could multiply parent rows
    const sql = compileOrder(
      { 'sameName.title': 1 },
      {
        relations: {
          sameName: {
            service: 'assignments',
            keyHere: 'name',
            keyThere: 'title',
            asArray: false,
            databaseTableName: 'assignments',
          },
        },
      },
    )

    assert.match(sql, /group by "sameName"."title"/)
    assert.match(sql, /min\("sameName"."title"\)/)
  })

  it('falls back to aggregating when the target id is unknown', () => {
    // Same relation as the joinable case, but nothing can confirm `id` is the
    // target's key — so it takes the safe path rather than assuming.
    const sql = compileOrder(
      { 'assignment.title': 1 },
      { lookupService: () => undefined },
    )

    assert.match(sql, /group by "assignment"."id"/)
  })
})
