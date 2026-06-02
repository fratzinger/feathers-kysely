import type { Generated } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import dialect, { getDialect } from './dialect.js'
import type { DialectType } from './dialect.js'
import { transformData } from 'feathers-utils'
import { addPrimaryKey } from './test-utils.js'
import { KyselyService } from '../src/index.js'

/*
 * Verifies the declarative `x-db-type` schema annotation as an alternative to a
 * `getPropertyType` function. The same opt-in, type-aware date coercion (see
 * date-coercion.test.ts) must kick in when a column declares its database type
 * via an `x-db-type` key in the `properties` map. Also pins the precedence
 * rule: an explicit `getPropertyType` wins over the annotation.
 */

const dialectName = getDialect()

interface DB {
  xdates: {
    id: Generated<number>
    tstz: any
    plain: any
  }
  xjson: {
    id: Generated<number>
    data: any
  }
}

type Row = { id: number; tstz: any; plain: any }

const COLUMN_DDL: Record<DialectType, any> = {
  postgres: sql`timestamptz`,
  mysql: sql`timestamp`,
  sqlite: sql`text`,
}

const INSTANTS = [
  '2025-06-01T00:00:00.000Z', // before T
  '2026-01-15T10:30:00.000Z', // == T
  '2027-03-20T12:00:00.000Z', // after T
] as const
const T = INSTANTS[1]

const asMs = (iso: string) => new Date(iso).getTime()

// MySQL wants 'YYYY-MM-DD HH:MM:SS'; postgres + sqlite accept ISO 8601 as-is.
const storedTimestamp = (iso: string) =>
  dialectName === 'mysql' ? iso.slice(0, 19).replace('T', ' ') : iso

const db = new Kysely<DB>({ dialect: dialect() })

const clean = async () => {
  await db.schema.dropTable('xdates').ifExists().execute()
  await addPrimaryKey(db.schema.createTable('xdates'), 'id')
    .addColumn('tstz', COLUMN_DDL[dialectName])
    .addColumn('plain', COLUMN_DDL[dialectName])
    .execute()
}

const app = feathers<{
  annotated: KyselyService<Row>
  fnOverride: KyselyService<Row>
  raw: KyselyService<Row>
}>()
  // Coercion declared purely via the `x-db-type` annotation, no function.
  .use(
    'annotated',
    new KyselyService<Row>({
      Model: db,
      id: 'id',
      name: 'xdates',
      multi: true,
      properties: {
        id: true,
        tstz: {
          type: 'string',
          format: 'date-time',
          'x-db-type': 'timestamptz',
        },
        plain: true,
      },
    }),
  )
  // Explicit function must win over the annotation: it suppresses coercion on
  // `tstz` (returns a non-temporal type) while the annotation says timestamptz.
  .use(
    'fnOverride',
    new KyselyService<Row>({
      Model: db,
      id: 'id',
      name: 'xdates',
      multi: true,
      getPropertyType: (prop) => (prop === 'tstz' ? 'json' : undefined),
      properties: {
        tstz: { 'x-db-type': 'timestamptz' },
        plain: true,
      },
    }),
  )
  // No declared types at all — used to prove coercion is opt-in.
  .use(
    'raw',
    new KyselyService<Row>({
      Model: db,
      id: 'id',
      name: 'xdates',
      multi: true,
    }),
  )

const seed = () =>
  app.service('annotated').create(
    INSTANTS.map((iso) => ({
      tstz: storedTimestamp(iso),
      plain: storedTimestamp(iso),
    })),
  )

const eqCount = async (
  service: 'annotated' | 'fnOverride' | 'raw',
  column: 'tstz' | 'plain',
  value: unknown,
) =>
  (
    (await app.service(service).find({
      query: { [column]: { $eq: value } },
      paginate: false,
    })) as Row[]
  ).length

describe(`x-db-type annotation (${dialectName})`, () => {
  beforeEach(async () => {
    await clean()
    await seed()
  })

  it('coerces epoch-ms via the x-db-type annotation alone', async () => {
    expect(await eqCount('annotated', 'tstz', asMs(T))).toBe(1)
  })

  it('is opt-in: an unannotated column is not coerced', async () => {
    // Same physical column, but `plain` carries no annotation, so the raw
    // epoch-ms value is not normalized: it either throws (Postgres) or matches
    // the wrong rows. Either way it does not behave like a coerced column.
    let result: number | 'throws'
    try {
      result = await eqCount('annotated', 'plain', asMs(T))
    } catch {
      result = 'throws'
    }
    expect(result).not.toBe(1)
  })

  it('is opt-in: a service with no declared types does not coerce', async () => {
    let raw: number | 'throws'
    try {
      raw = await eqCount('raw', 'tstz', asMs(T))
    } catch {
      raw = 'throws'
    }
    expect(raw).not.toBe(1)
  })

  it('explicit getPropertyType takes precedence over x-db-type', async () => {
    // The function maps `tstz` to a non-temporal type, so no coercion happens
    // even though the annotation says timestamptz — proving the function wins.
    let result: number | 'throws'
    try {
      result = await eqCount('fnOverride', 'tstz', asMs(T))
    } catch {
      result = 'throws'
    }
    expect(result).not.toBe(1)
  })
})

/*
 * jsonb dot-notation traversal driven purely by an `x-db-type: 'jsonb'`
 * annotation (no getPropertyType function). jsonb is Postgres-only. Shares the
 * single `db`/pool above; teardown happens in the top-level afterAll.
 */
type JsonRow = { id: number; data: any }

const jsonApp = feathers<{ docs: KyselyService<JsonRow> }>().use(
  'docs',
  new KyselyService<JsonRow>({
    Model: db,
    id: 'id',
    name: 'xjson',
    multi: true,
    properties: {
      id: true,
      data: { type: 'object', 'x-db-type': 'jsonb' },
    },
  }),
)

jsonApp.service('docs').hooks({
  before: {
    create: [
      transformData((data) => {
        if (data.data) data.data = JSON.stringify(data.data)
      }),
    ],
  },
})

describe.skipIf(dialectName !== 'postgres')(
  'x-db-type jsonb dot-notation (postgres)',
  () => {
    beforeEach(async () => {
      await db.schema.dropTable('xjson').ifExists().execute()
      await addPrimaryKey(db.schema.createTable('xjson'), 'id')
        .addColumn('data', 'jsonb')
        .execute()
    })

    it('queries a nested jsonb path via x-db-type annotation', async () => {
      await jsonApp
        .service('docs')
        .create([
          { data: { a: { b: { c: 1 } } } },
          { data: { a: { b: { c: 2 } } } },
          { data: { a: { b: { c: 3 } } } },
        ])

      const queried = (await jsonApp.service('docs').find({
        query: { 'data.a.b.c': { $gte: 2 } },
        paginate: false,
      })) as JsonRow[]

      expect(queried).toHaveLength(2)
      expect(queried.map((r) => r.data.a.b.c).sort()).toEqual([2, 3])
    })

    it('queries a top-level jsonb key via x-db-type annotation', async () => {
      await jsonApp
        .service('docs')
        .create([{ data: { name: 'John' } }, { data: { name: 'Test' } }])

      const queried = (await jsonApp.service('docs').find({
        query: { 'data.name': 'John' },
        paginate: false,
      })) as JsonRow[]

      expect(queried).toHaveLength(1)
      expect(queried[0].data).toEqual({ name: 'John' })
    })
  },
)

afterAll(() => db.destroy())
