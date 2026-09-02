import type { Generated, Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'

import { KyselyService } from '../src/index.js'
import { getDialect } from './dialect.js'
import { addPrimaryKey } from './test-utils.js'

/**
 * A relation graph, a deterministic seeder and a table of query shapes, shared
 * by the SQL-shape snapshot test and the benchmarks so both cover the same
 * cases.
 *
 * The graph is deeper than the one in `relations.test.ts` on purpose: three
 * belongsTo hops, two hasMany levels and a to-many behind a to-one, so a change
 * in path resolution shows up somewhere.
 *
 *   events ──▶ assignment ──▶ customer ──▶ owner (users)
 *                  │                          │
 *                  └─▶ categories ──▶ type    └─▶ ownedCustomers ──▶ assignments
 */

export interface BenchDB {
  users: { id: Generated<number>; name: string }
  customers: { id: Generated<number>; fullName: string; ownerId: number | null }
  assignments: {
    id: Generated<number>
    title: string
    number: number
    customerId: number | null
  }
  types: { id: Generated<number>; name: string }
  categories: {
    id: Generated<number>
    assignmentId: number
    typeId: number
    active: boolean
  }
  events: { id: Generated<number>; name: string; assignmentId: number }
}

const rel = (
  service: string,
  keyHere: string,
  keyThere: string,
  asArray: boolean,
  databaseTableName: string,
) => ({ service, keyHere, keyThere, asArray, databaseTableName })

export const buildApp = (db: Kysely<any>) => {
  const service = (
    name: string,
    properties: Record<string, true>,
    relations: Record<string, any>,
  ) =>
    new KyselyService<any>({
      Model: db,
      name,
      multi: true,
      paginate: { default: 25, max: 100 },
      properties,
      relations,
    })

  return feathers<any>()
    .use(
      'events',
      service(
        'events',
        { id: true, name: true, assignmentId: true },
        {
          assignment: rel(
            'assignments',
            'assignmentId',
            'id',
            false,
            'assignments',
          ),
        },
      ),
    )
    .use(
      'assignments',
      service(
        'assignments',
        { id: true, title: true, number: true, customerId: true },
        {
          customer: rel('customers', 'customerId', 'id', false, 'customers'),
          categories: rel(
            'categories',
            'id',
            'assignmentId',
            true,
            'categories',
          ),
        },
      ),
    )
    .use(
      'customers',
      service(
        'customers',
        { id: true, fullName: true, ownerId: true },
        {
          owner: rel('users', 'ownerId', 'id', false, 'users'),
          assignments: rel(
            'assignments',
            'id',
            'customerId',
            true,
            'assignments',
          ),
        },
      ),
    )
    .use(
      'categories',
      service(
        'categories',
        { id: true, assignmentId: true, typeId: true, active: true },
        {
          type: rel('types', 'typeId', 'id', false, 'types'),
          assignment: rel(
            'assignments',
            'assignmentId',
            'id',
            false,
            'assignments',
          ),
        },
      ),
    )
    .use('types', service('types', { id: true, name: true }, {}))
    .use(
      'users',
      service(
        'users',
        { id: true, name: true },
        {
          ownedCustomers: rel('customers', 'id', 'ownerId', true, 'customers'),
        },
      ),
    )
}

// MARK: schema

const TABLES = [
  'events',
  'categories',
  'assignments',
  'customers',
  'types',
  'users',
] as const

export const createSchema = async (
  db: Kysely<any>,
  options: { indexes?: boolean } = {},
) => {
  for (const table of TABLES) {
    await db.schema.dropTable(table).ifExists().execute()
  }

  await addPrimaryKey(
    db.schema.createTable('users').addColumn('name', 'text'),
    'id',
  ).execute()

  await addPrimaryKey(
    db.schema
      .createTable('customers')
      .addColumn('fullName', 'text')
      .addColumn('ownerId', 'integer'),
    'id',
  ).execute()

  await addPrimaryKey(
    db.schema
      .createTable('assignments')
      .addColumn('title', 'text')
      .addColumn('number', 'integer')
      .addColumn('customerId', 'integer'),
    'id',
  ).execute()

  await addPrimaryKey(
    db.schema.createTable('types').addColumn('name', 'text'),
    'id',
  ).execute()

  await addPrimaryKey(
    db.schema
      .createTable('categories')
      .addColumn('assignmentId', 'integer')
      .addColumn('typeId', 'integer')
      .addColumn('active', 'boolean'),
    'id',
  ).execute()

  await addPrimaryKey(
    db.schema
      .createTable('events')
      .addColumn('name', 'text')
      .addColumn('assignmentId', 'integer'),
    'id',
  ).execute()

  // Secondary indexes are opt-in. Real schemas are rarely indexed on every
  // filtered column, and an un-indexed table is exactly where the shape of the
  // SQL decides the plan. Primary keys are always present, so a FK referencing
  // one still gets an index lookup on the target side.
  if (!options.indexes) return

  const indexes: [string, string, string][] = [
    ['customers_ownerId', 'customers', 'ownerId'],
    ['customers_fullName', 'customers', 'fullName'],
    ['assignments_customerId', 'assignments', 'customerId'],
    ['assignments_title', 'assignments', 'title'],
    ['assignments_number', 'assignments', 'number'],
    ['categories_assignmentId', 'categories', 'assignmentId'],
    ['categories_typeId', 'categories', 'typeId'],
    ['categories_active', 'categories', 'active'],
    ['events_assignmentId', 'events', 'assignmentId'],
    ['events_name', 'events', 'name'],
    ['types_name', 'types', 'name'],
    ['users_name', 'users', 'name'],
  ]

  for (const [name, table, column] of indexes) {
    await db.schema
      .createIndex(name)
      .on(table)
      .column(column)
      .ifNotExists()
      .execute()
  }
}

// MARK: seed

/** Deterministic PRNG so two runs seed byte-identical data. */
const mulberry32 = (seed: number) => () => {
  seed = (seed + 0x6d2b79f5) | 0
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296
}

export type SeedCounts = {
  users: number
  customers: number
  assignments: number
  types: number
  categories: number
  events: number
}

/**
 * Scaled by `BENCH_SCALE` so a run can be made heavier without code changes.
 *
 * Sized for an un-indexed run: large enough that the query plan dominates the
 * round-trip, small enough that every case stays measurable. Scaling up without
 * `BENCH_INDEXES` grows the correlated-aggregate sort quadratically while the
 * rest grow linearly — see `bench/README.md`.
 */
export const seedCounts = (
  scale = Number(process.env.BENCH_SCALE ?? 1),
): SeedCounts => ({
  users: Math.round(75 * scale),
  customers: Math.round(300 * scale),
  assignments: Math.round(1_500 * scale),
  types: 20,
  categories: Math.round(6_000 * scale),
  events: Math.round(3_000 * scale),
})

// Postgres allows 65535 bind parameters per statement; stay well under it.
const insertChunked = async (
  db: Kysely<any>,
  table: string,
  rows: Record<string, any>[],
) => {
  const columns = rows.length ? Object.keys(rows[0]).length : 0
  const chunkSize = Math.max(1, Math.floor(5_000 / Math.max(columns, 1)))

  for (let i = 0; i < rows.length; i += chunkSize) {
    await db
      .insertInto(table)
      .values(rows.slice(i, i + chunkSize))
      .execute()
  }
}

export const seed = async (db: Kysely<any>, counts = seedCounts()) => {
  const random = mulberry32(42)
  const pick = (max: number) => 1 + Math.floor(random() * max)

  await insertChunked(
    db,
    'users',
    Array.from({ length: counts.users }, (_, i) => ({ name: `u-${i}` })),
  )

  await insertChunked(
    db,
    'types',
    Array.from({ length: counts.types }, (_, i) => ({ name: `t-${i}` })),
  )

  await insertChunked(
    db,
    'customers',
    Array.from({ length: counts.customers }, (_, i) => ({
      fullName: `c-${i}`,
      // Leave a tenth without an owner so to-one hops meet real NULLs.
      ownerId: random() < 0.1 ? null : pick(counts.users),
    })),
  )

  await insertChunked(
    db,
    'assignments',
    Array.from({ length: counts.assignments }, (_, i) => ({
      title: `a-${i}`,
      number: 1 + Math.floor(random() * 100),
      customerId: random() < 0.1 ? null : pick(counts.customers),
    })),
  )

  // sqlite cannot bind booleans; postgres will not take 0/1 for a boolean
  // column. Seed the representation each driver accepts.
  const bool =
    getDialect() === 'sqlite'
      ? (value: boolean) => (value ? 1 : 0)
      : (value: boolean) => value

  await insertChunked(
    db,
    'categories',
    Array.from({ length: counts.categories }, () => ({
      assignmentId: pick(counts.assignments),
      typeId: pick(counts.types),
      active: bool(random() < 0.5),
    })),
  )

  await insertChunked(
    db,
    'events',
    Array.from({ length: counts.events }, (_, i) => ({
      name: `e-${i}`,
      assignmentId: pick(counts.assignments),
    })),
  )
}

// MARK: query cases

export type QueryCase = {
  /** Stable name — also the snapshot key and the benchmark label. */
  name: string
  service: string
  query: Record<string, any>
}

export const QUERY_CASES: QueryCase[] = [
  // --- baseline: no relations involved ---
  { name: 'plain column', service: 'events', query: { name: 'e-1' } },
  {
    name: 'plain column with operator',
    service: 'events',
    query: { assignmentId: { $in: [1, 2, 3] } },
  },

  // --- belongsTo ---
  {
    name: 'belongsTo 1 hop',
    service: 'events',
    query: { 'assignment.title': 'a-1' },
  },
  {
    name: 'belongsTo 2 hops',
    service: 'events',
    query: { 'assignment.customer.fullName': 'c-1' },
  },
  {
    name: 'belongsTo 3 hops',
    service: 'events',
    query: { 'assignment.customer.owner.name': 'u-1' },
  },
  {
    name: 'belongsTo nested notation',
    service: 'events',
    query: { assignment: { customer: { fullName: 'c-1' } } },
  },
  {
    // The trade-off of per-filter semi-joins: two subqueries over one prefix
    // where a shared JOIN would have needed one.
    name: 'belongsTo two filters same prefix',
    service: 'events',
    query: { 'assignment.title': 'a-1', 'assignment.number': { $gt: 3 } },
  },

  // --- hasMany ---
  {
    name: 'hasMany $some',
    service: 'assignments',
    query: { categories: { $some: { typeId: 1 } } },
  },
  {
    name: 'hasMany $none',
    service: 'assignments',
    query: { categories: { $none: { active: true } } },
  },
  {
    name: 'hasMany $every',
    service: 'assignments',
    query: { categories: { $every: { active: true } } },
  },
  {
    name: 'hasMany implicit $some via dot path',
    service: 'assignments',
    query: { 'categories.typeId': 1 },
  },

  // --- mixed chains ---
  {
    name: 'belongsTo then hasMany',
    service: 'events',
    query: {
      assignment: { categories: { $some: { typeId: { $in: [1, 2, 3] } } } },
    },
  },
  {
    name: 'mixed 4 hops via dot path',
    service: 'events',
    query: { 'assignment.categories.type.name': 't-1' },
  },
  {
    name: 'belongsTo inside $some',
    service: 'assignments',
    query: { categories: { $some: { type: { name: 't-1' } } } },
  },
  {
    // Filters on `number`, not on a single title: a case that matches nothing
    // measures nothing, and the cost report's `rows` column makes that visible.
    name: 'nested hasMany two levels',
    service: 'users',
    query: {
      ownedCustomers: {
        $some: { assignments: { $some: { number: { $gt: 50 } } } },
      },
    },
  },

  // --- boolean composition ---
  {
    name: '$not with relation path',
    service: 'events',
    query: { $not: { 'assignment.title': 'a-1' } },
  },
  {
    name: '$or over two relation legs',
    service: 'events',
    query: {
      $or: [
        { 'assignment.title': 'a-1' },
        { assignment: { categories: { $some: { typeId: 1 } } } },
      ],
    },
  },

  // --- sorting (the only path that still joins) ---
  {
    name: '$sort by belongsTo column',
    service: 'events',
    query: { $sort: { 'assignment.title': 1 } },
  },
  {
    name: '$sort by hasMany column',
    service: 'assignments',
    query: { $sort: { 'categories.typeId': 1 } },
  },
  {
    name: 'filter and sort on the same relation',
    service: 'events',
    query: { 'assignment.title': 'a-1', $sort: { 'assignment.number': -1 } },
  },
]

/** The compose options a paginated `find` uses. */
export const FIND_OPTIONS = {
  select: true,
  where: true,
  order: true,
  limit: true,
  offset: true,
} as const
