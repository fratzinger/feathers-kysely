// Pin the Node process timezone to UTC BEFORE the pg/mysql pools serialize any
// Date. node-postgres and mysql2 turn a JS `Date` bind param into a wall-clock
// string in the process timezone; without this pin the `timestamp`-without-tz ×
// Date-instance cells would shift by the local offset and the assertions below
// would be machine-dependent. (That shift is itself the footgun documented below.)
process.env.TZ = 'UTC'

import type { Generated } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import dialect, { getDialect } from './dialect.js'
import type { DialectType } from './dialect.js'
import { addPrimaryKey } from './test-utils.js'
import { KyselyService } from '../src/index.js'

/*
 * Characterizes how a date/timestamp query value behaves when supplied as a
 * Date instance, an ISO 8601 string, an epoch-millisecond number, or a
 * "YYYY-MM-DD" string — across $gt/$gte/$lt/$lte/$eq, on every dialect and on
 * three temporal column types.
 *
 * The adapter does NO date coercion: the value flows straight to the driver as
 * a bind parameter (src/adapter.ts buildPropertyExpression/transformOperatorValue),
 * so behavior is purely (dialect) × (column type) × (JS value type). This file
 * pins the current reality; the ✗/~ cells are the gap list for an optional,
 * type-aware coercion feature (would hook src/declarations.ts `getPropertyType`).
 *
 * Findings (✓ correct · ~ surprising-but-defensible · ✗ gap), observed values:
 *   epoch-ms number  ✗ EVERYWHERE — Postgres throws ("out of range"); SQLite &
 *                      MySQL silently compare number-vs-text/date → wrong rows.
 *                      The single most broken format.
 *   Date instance    ✗ SQLite throws (better-sqlite3 cannot bind a Date).
 *                    ✓ Postgres/MySQL on a real timestamp column.
 *                    ✗ Postgres `timestamp`-without-tz shifts by the process TZ
 *                      (here pinned UTC, so it passes — the footgun is silent).
 *   ISO string       ✓ The portable winner for timestamp columns on all three.
 *   "YYYY-MM-DD"     ✓ The right format for a DATE column on all three.
 *                    ~ On a timestamp column it means midnight; on a tz-aware
 *                      column that midnight is the server session tz (so the
 *                      boundary is environment-dependent — we assert only the
 *                      tz-invariant facts there).
 *   DATE column      ✓ only with a "YYYY-MM-DD" value. A time-bearing value
 *                      (Date/ISO) is correct on Postgres (time discarded) but
 *                      ✗ wrong on SQLite/MySQL (the time-of-day shifts the day
 *                      boundary). SQLite ISO is also wrong because the stored
 *                      date-only text and the full-ISO probe don't line up.
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

// The three temporal column types, mapped to each dialect's real type. SQLite
// has no native temporal type, so its columns are TEXT holding ISO strings.
const COLUMN_DDL: Record<DialectType, Record<'tstz' | 'ts' | 'd', any>> = {
  postgres: { tstz: sql`timestamptz`, ts: sql`timestamp`, d: sql`date` },
  mysql: { tstz: sql`timestamp`, ts: sql`datetime`, d: sql`date` },
  sqlite: { tstz: sql`text`, ts: sql`text`, d: sql`text` },
}

// Fixed instants so every boundary at T is decisive. r2 and r3 deliberately
// share a calendar day, which is what makes the DATE-column $eq return 2.
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
const asDateOnly = (iso: string) => iso.slice(0, 10) // 'YYYY-MM-DD'

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
type Counts = [number, number, number, number, number] // indexed like OPERATORS

// Named expected shapes (see the matrix in the header comment).
const INSTANT: Counts = [2, 3, 2, 3, 1] // tstz/ts: compares absolute instants to T
const DAY: Counts = [1, 3, 2, 4, 2] // DATE col: compares T's calendar day (r2 & r3 tie)
const MIDNIGHT: Counts = [3, 3, 2, 2, 0] // date-only on a timestamp col → midnight of T's day
const NUM_VS_TEXT: Counts = [5, 5, 0, 0, 0] // sqlite/mysql: a number sorts before any text/date
const TIME_SHIFTS_DAY: Counts = [1, 1, 4, 4, 0] // time-bearing probe vs DATE col: time moves the boundary
const NUM_VS_DATE: Counts = [0, 0, 5, 5, 0] // mysql: a number coerced against a DATE column

const THROWS = 'throws' as const
const TZ = 'tzInstant' as const // date-only vs a tz-aware column: eq=0, boundary is server-tz-dependent
type Cell = Counts | typeof THROWS | typeof TZ

const EXPECT: Record<DialectType, Record<Column, Record<Format, Cell>>> = {
  postgres: {
    tstz: { Date: INSTANT, ISO: INSTANT, ms: THROWS, 'YYYY-MM-DD': TZ },
    ts: { Date: INSTANT, ISO: INSTANT, ms: THROWS, 'YYYY-MM-DD': MIDNIGHT },
    d: { Date: DAY, ISO: DAY, ms: THROWS, 'YYYY-MM-DD': DAY },
  },
  sqlite: {
    tstz: {
      Date: THROWS,
      ISO: INSTANT,
      ms: NUM_VS_TEXT,
      'YYYY-MM-DD': MIDNIGHT,
    },
    ts: { Date: THROWS, ISO: INSTANT, ms: NUM_VS_TEXT, 'YYYY-MM-DD': MIDNIGHT },
    d: {
      Date: THROWS,
      ISO: TIME_SHIFTS_DAY,
      ms: NUM_VS_TEXT,
      'YYYY-MM-DD': DAY,
    },
  },
  mysql: {
    tstz: { Date: INSTANT, ISO: INSTANT, ms: NUM_VS_TEXT, 'YYYY-MM-DD': TZ },
    ts: {
      Date: INSTANT,
      ISO: INSTANT,
      ms: NUM_VS_TEXT,
      'YYYY-MM-DD': MIDNIGHT,
    },
    d: {
      Date: TIME_SHIFTS_DAY,
      ISO: TIME_SHIFTS_DAY,
      ms: NUM_VS_DATE,
      'YYYY-MM-DD': DAY,
    },
  },
}

const eqCounts = (a: Counts, b: Counts) => a.every((v, i) => v === b[i])

// Human label for the test name, so `pnpm test` output reads like the matrix.
function classify(col: Column, format: Format, cell: Cell): string {
  if (cell === THROWS) return '✗ rejected'
  if (cell === TZ) return '~ tz-dependent (date→instant)'
  const semantic = col === 'd' ? DAY : INSTANT
  if (eqCounts(cell, semantic)) return '✓ correct'
  if (format === 'YYYY-MM-DD' && eqCounts(cell, MIDNIGHT))
    return '~ date→midnight'
  return '✗ wrong rows'
}

// MySQL wants 'YYYY-MM-DD HH:MM:SS'; postgres + sqlite accept ISO 8601 as-is.
const storedTimestamp = (iso: string) =>
  dialectName === 'mysql' ? iso.slice(0, 19).replace('T', ' ') : iso

const db = new Kysely<DB>({ dialect: dialect() })

const clean = async () => {
  await db.schema.dropTable('dates').ifExists().execute()
  await addPrimaryKey(db.schema.createTable('dates'), 'id')
    .addColumn('tstz', COLUMN_DDL[dialectName].tstz)
    .addColumn('ts', COLUMN_DDL[dialectName].ts)
    .addColumn('d', COLUMN_DDL[dialectName].d)
    .execute()
}

const app = feathers<{ dates: KyselyService<Dates> }>().use(
  'dates',
  new KyselyService<Dates>({ Model: db, id: 'id', name: 'dates', multi: true }),
)

const seed = () =>
  app.service('dates').create(
    INSTANTS.map((iso) => ({
      tstz: storedTimestamp(iso),
      ts: storedTimestamp(iso),
      d: asDateOnly(iso),
    })),
  )

const find = (column: Column, operator: string, value: unknown) =>
  app.service('dates').find({
    query: { [column]: { [operator]: value } },
    paginate: false,
  }) as Promise<Dates[]>

describe(`date queries (${dialectName})`, () => {
  beforeEach(async () => {
    await clean()
    await seed()
  })
  afterAll(() => db.destroy())

  for (const col of COLUMNS) {
    describe(`${col} column`, () => {
      for (const format of Object.keys(FORMATS) as Format[]) {
        const cell = EXPECT[dialectName][col][format]
        const probe = () => FORMATS[format](T)

        it(`${format} input → ${classify(col, format, cell)}`, async () => {
          if (cell === THROWS) {
            for (const op of OPERATORS) {
              await expect(find(col, op, probe())).rejects.toThrow()
            }
            return
          }

          if (cell === TZ) {
            // A "YYYY-MM-DD" value against a tz-aware column is parsed as an
            // instant (midnight in the DB session timezone), NOT a calendar day.
            // The exact gt/lt split depends on the server tz, so assert only the
            // tz-invariant facts.
            const n: Record<string, number> = {}
            for (const op of OPERATORS)
              n[op] = (await find(col, op, probe())).length
            expect(n['$eq']).toBe(0) // never equals a stored time-of-day
            expect(n['$gt']).toBe(n['$gte']) // no row sits exactly on the boundary
            expect(n['$lt']).toBe(n['$lte'])
            expect(n['$gt'] + n['$lt']).toBe(INSTANTS.length) // boundary splits the set
            return
          }

          for (let i = 0; i < OPERATORS.length; i++) {
            const op = OPERATORS[i]
            const rows = await find(col, op, probe())
            expect(rows, `${col} ${format} ${op}`).toHaveLength(cell[i])
          }
        })
      }
    })
  }
})
