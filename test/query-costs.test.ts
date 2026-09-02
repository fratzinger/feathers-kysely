import { CompiledQuery, Kysely } from 'kysely'
import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { afterAll, beforeAll, describe, it } from 'vitest'

import dialect, { getDialect } from './dialect.js'
import {
  buildApp,
  createSchema,
  FIND_OPTIONS,
  QUERY_CASES,
  seed,
  seedCounts,
  type BenchDB,
} from './relation-graph.js'
import type { KyselyService } from '../src/index.js'

/**
 * Deterministic cost report for every relation case, from
 * `EXPLAIN (ANALYZE, BUFFERS)`.
 *
 * Wall-clock timings drift several percent between runs, which drowns out the
 * differences worth catching. Blocks touched and rows returned do not: they are
 * a property of the plan and the data, so a 2% change in them is a real change.
 * `rows` doubles as a correctness signal — a query that starts returning a
 * different number of rows shows up here even when it got faster.
 *
 * Postgres only (`BUFFERS` is postgres syntax) and opt-in:
 *
 *   BENCH_COSTS=1 DB=postgres npx vitest run test/query-costs.test.ts
 *
 * Record and compare a baseline with:
 *
 *   BENCH_COSTS_OUT=bench/costs.json    # write this run
 *   BENCH_COSTS_BASE=bench/costs.json   # diff against a recorded run
 */

type CaseCost = { blocks: number; rows: number; plan: string }

const enabled = !!process.env.BENCH_COSTS && getDialect() === 'postgres'

const db = new Kysely<BenchDB>({ dialect: dialect() })
const app = buildApp(db)

/** Root-node block counters are cumulative over the whole plan. */
const readCost = (plan: Record<string, any>): CaseCost => ({
  blocks:
    (plan['Shared Hit Blocks'] ?? 0) +
    (plan['Shared Read Blocks'] ?? 0) +
    (plan['Local Hit Blocks'] ?? 0) +
    (plan['Local Read Blocks'] ?? 0) +
    (plan['Temp Read Blocks'] ?? 0),
  rows: Math.round(plan['Actual Rows'] ?? 0),
  plan: [
    plan['Node Type'],
    ...(plan.Plans ?? []).map((child: any) => child['Node Type']),
  ].join(' > '),
})

describe.skipIf(!enabled)('query costs', () => {
  beforeAll(async () => {
    await createSchema(db, { indexes: !!process.env.BENCH_INDEXES })
    await seed(db, seedCounts())
    await app.setup()
  }, 600_000)

  afterAll(() => db.destroy())

  it('reports the plan cost of every relation case', async () => {
    const costs: Record<string, CaseCost> = {}

    for (const testCase of QUERY_CASES) {
      const service = app.service(
        testCase.service,
      ) as unknown as KyselyService<any>

      const { sql, parameters } = service
        .composeQuery({ query: testCase.query }, FIND_OPTIONS)
        .compile()

      const { rows } = await db.executeQuery<{ 'QUERY PLAN': any }>(
        CompiledQuery.raw(
          `explain (analyze, buffers, format json) ${sql}`,
          parameters as any[],
        ),
      )

      costs[testCase.name] = readCost(rows[0]['QUERY PLAN'][0].Plan)
    }

    const basePath = process.env.BENCH_COSTS_BASE
    const base: Record<string, CaseCost> | undefined =
      basePath && existsSync(basePath)
        ? JSON.parse(readFileSync(basePath, 'utf8'))
        : undefined

    const header = base
      ? `${'case'.padEnd(42)} ${'blocks'.padStart(9)} ${'was'.padStart(9)} ${'delta'.padStart(8)}  rows  plan`
      : `${'case'.padEnd(42)} ${'blocks'.padStart(9)}  rows  plan`

    const lines = [header]

    for (const [name, cost] of Object.entries(costs)) {
      const previous = base?.[name]
      const label = name.padEnd(42)
      const blocks = String(cost.blocks).padStart(9)
      const rowsChanged =
        previous && previous.rows !== cost.rows
          ? `  ROWS ${previous.rows} → ${cost.rows}`
          : ''

      if (!previous) {
        lines.push(
          `${label} ${blocks} ${String(cost.rows).padStart(5)}  ${cost.plan}`,
        )
        continue
      }

      const delta = previous.blocks
        ? ((cost.blocks - previous.blocks) / previous.blocks) * 100
        : 0

      lines.push(
        `${label} ${blocks} ${String(previous.blocks).padStart(9)} ${`${delta >= 0 ? '+' : ''}${delta.toFixed(1)}%`.padStart(8)} ${String(cost.rows).padStart(5)}  ${cost.plan}${rowsChanged}`,
      )
    }

    console.log(`\n${lines.join('\n')}\n`)

    const outPath = process.env.BENCH_COSTS_OUT
    if (outPath) {
      writeFileSync(outPath, `${JSON.stringify(costs, null, 2)}\n`)
      console.log(`cost report written to ${outPath}`)
    }
  }, 600_000)
})
