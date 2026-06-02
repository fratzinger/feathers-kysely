import type { Generated } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import dialect, { getDialect } from './dialect.js'
import type { DialectType } from './dialect.js'
import { addPrimaryKey } from './test-utils.js'
import { KyselyService } from '../src/index.js'

/*
 * Verifies the opt-in, type-aware date coercion: when `getPropertyType` declares
 * a column as temporal, the adapter normalizes a Date / ISO string / epoch-ms
 * number / "YYYY-MM-DD" string query value into the canonical string the driver
 * compares correctly. This is the fix for the gaps catalogued (uncoerced) in
 * date-queries.test.ts. With coercion on, every input format yields the SAME
 * correct rows on every dialect — see the uniform EXPECT table below.
 */

const dialectName = getDialect()

interface DB {
  dates: {
    id: Generated<number>
    tstz: any
    ts: any
    d: any
  }
}

type Dates = { id: number; tstz: any; ts: any; d: any }

const COLUMN_DDL: Record<DialectType, Record<'tstz' | 'ts' | 'd', any>> = {
  postgres: { tstz: sql`timestamptz`, ts: sql`timestamp`, d: sql`date` },
  mysql: { tstz: sql`timestamp`, ts: sql`datetime`, d: sql`date` },
  sqlite: { tstz: sql`text`, ts: sql`text`, d: sql`text` },
}

const INSTANTS = [
  '2025-06-01T00:00:00.000Z', // r0  well before
  '2026-01-14T23:59:59.000Z', // r1  day before T
  '2026-01-15T10:30:00.000Z', // r2  == T (boundary)
  '2026-01-15T18:00:00.000Z', // r3  later, SAME calendar day as T
  '2027-03-20T12:00:00.000Z', // r4  well after
] as const
const T = INSTANTS[2]

const asDate = (iso: string) => new Date(iso)
const asIso = (iso: string) => iso
const asMs = (iso: string) => new Date(iso).getTime()
const asDateOnly = (iso: string) => iso.slice(0, 10)

const FORMATS = {
  Date: asDate,
  ISO: asIso,
  ms: asMs,
  'YYYY-MM-DD': asDateOnly,
} as const
type Format = keyof typeof FORMATS

const COLUMNS = ['tstz', 'ts', 'd'] as const
type Column = (typeof COLUMNS)[number]

const OPERATORS = ['$gt', '$gte', '$lt', '$lte', '$eq'] as const
type Counts = [number, number, number, number, number]

const INSTANT: Counts = [2, 3, 2, 3, 1] // instant columns: compare absolute instant to T
const DAY: Counts = [1, 3, 2, 4, 2] // date column: compare T's calendar day (r2 & r3 tie)
const MIDNIGHT: Counts = [3, 3, 2, 2, 0] // date-only on an instant column → UTC midnight of T's day

// With coercion enabled the result is identical on every dialect, with no throws
// and no timezone dependence (date-only normalizes to an explicit UTC instant).
const EXPECT: Record<Column, Record<Format, Counts>> = {
  tstz: { Date: INSTANT, ISO: INSTANT, ms: INSTANT, 'YYYY-MM-DD': MIDNIGHT },
  ts: { Date: INSTANT, ISO: INSTANT, ms: INSTANT, 'YYYY-MM-DD': MIDNIGHT },
  d: { Date: DAY, ISO: DAY, ms: DAY, 'YYYY-MM-DD': DAY },
}

// MySQL wants 'YYYY-MM-DD HH:MM:SS'; postgres + sqlite accept ISO 8601 as-is.
const storedTimestamp = (iso: string) =>
  dialectName === 'mysql' ? iso.slice(0, 19).replace('T', ' ') : iso

// Opt-in: declares each column's temporal type so the adapter coerces its values.
const getPropertyType = (prop: string) =>
  prop === 'tstz'
    ? 'timestamptz'
    : prop === 'ts'
      ? 'timestamp'
      : prop === 'd'
        ? 'date'
        : undefined

const db = new Kysely<DB>({ dialect: dialect() })

const clean = async () => {
  await db.schema.dropTable('dates').ifExists().execute()
  await addPrimaryKey(db.schema.createTable('dates'), 'id')
    .addColumn('tstz', COLUMN_DDL[dialectName].tstz)
    .addColumn('ts', COLUMN_DDL[dialectName].ts)
    .addColumn('d', COLUMN_DDL[dialectName].d)
    .execute()
}

const app = feathers<{
  dates: KyselyService<Dates>
  datesRaw: KyselyService<Dates>
}>()
  .use(
    'dates',
    new KyselyService<Dates>({
      Model: db,
      id: 'id',
      name: 'dates',
      multi: true,
      getPropertyType,
    }),
  )
  // Same table, NO declared types — used to prove coercion is opt-in.
  .use(
    'datesRaw',
    new KyselyService<Dates>({
      Model: db,
      id: 'id',
      name: 'dates',
      multi: true,
    }),
  )

const seed = () =>
  app.service('dates').create(
    INSTANTS.map((iso) => ({
      tstz: storedTimestamp(iso),
      ts: storedTimestamp(iso),
      d: asDateOnly(iso),
    })),
  )

const find = (
  service: 'dates' | 'datesRaw',
  column: Column,
  operator: string,
  value: unknown,
) =>
  app.service(service).find({
    query: { [column]: { [operator]: value } },
    paginate: false,
  }) as Promise<Dates[]>

describe(`date coercion (${dialectName})`, () => {
  beforeEach(async () => {
    await clean()
    await seed()
  })
  afterAll(() => db.destroy())

  for (const col of COLUMNS) {
    describe(`${col} column`, () => {
      for (const format of Object.keys(FORMATS) as Format[]) {
        it(`${format} input → correct rows`, async () => {
          const expected = EXPECT[col][format]
          for (let i = 0; i < OPERATORS.length; i++) {
            const rows = await find(
              'dates',
              col,
              OPERATORS[i],
              FORMATS[format](T),
            )
            expect(rows, `${col} ${format} ${OPERATORS[i]}`).toHaveLength(
              expected[i],
            )
          }
        })
      }
    })
  }

  describe('coercion details', () => {
    it('bare equality with a Date instance matches', async () => {
      const rows = (await app
        .service('dates')
        .find({ query: { tstz: asDate(T) }, paginate: false })) as Dates[]
      expect(rows).toHaveLength(1)
    })

    it('$in accepts a mix of input formats', async () => {
      const rows = await find('dates', 'tstz', '$in', [
        asDate(INSTANTS[0]),
        asMs(INSTANTS[2]),
        asIso(INSTANTS[4]),
      ])
      expect(rows).toHaveLength(3)
    })

    it('$ne null still works (null is not coerced)', async () => {
      const rows = await find('dates', 'tstz', '$ne', null)
      expect(rows).toHaveLength(INSTANTS.length)
    })

    it('is opt-in: an undeclared column does not coerce epoch-ms', async () => {
      // Declared column: coerced and correct.
      expect(await find('dates', 'tstz', '$eq', asMs(T))).toHaveLength(1)

      // Same column via a service without declared types: stays broken — the
      // raw value either throws (Postgres) or matches the wrong rows.
      let raw: number | 'throws'
      try {
        raw = (await find('datesRaw', 'tstz', '$eq', asMs(T))).length
      } catch {
        raw = 'throws'
      }
      expect(raw).not.toBe(1)
    })
  })
})
