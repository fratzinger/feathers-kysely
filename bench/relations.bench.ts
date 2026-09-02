import { Kysely } from 'kysely'
import { bench, describe } from 'vitest'

import dialect, { getDialect } from '../test/dialect.js'
import {
  buildApp,
  createSchema,
  QUERY_CASES,
  seed,
  seedCounts,
  type BenchDB,
} from '../test/relation-graph.js'

/**
 * Executes every relation case against a seeded database, so a change to query
 * building can be compared against a recorded baseline:
 *
 *   pnpm bench --outputJson=bench/baseline.json   # on the reference commit
 *   pnpm bench --compare=bench/baseline.json      # after the change
 *
 * `DB` picks the dialect (see `test/dialect.ts`); `BENCH_SCALE` multiplies the
 * row counts. The data is seeded from a fixed PRNG seed, so two runs measure
 * the same rows.
 */

const db = new Kysely<BenchDB>({ dialect: dialect() })
const app = buildApp(db)

const counts = seedCounts()

// Set up at module scope, not in `beforeAll`: vitest does not run suite hooks
// in benchmark mode, so without this `app.setup()` never happens and every
// multi-hop chain fails with a BadRequest — which benchmarks report as NaN
// rather than as an error.
await createSchema(db)
await seed(db, counts)
await app.setup()

const totalRows = Object.values(counts).reduce((sum, n) => sum + n, 0)
console.log(`seeded ${totalRows.toLocaleString('en-US')} rows`, counts)

describe(`relation queries (${getDialect()})`, () => {
  for (const testCase of QUERY_CASES) {
    bench(
      testCase.name,
      async () => {
        await app.service(testCase.service).find({ query: testCase.query })
      },
      // A round-trip per iteration, so the default 500ms budget can collect
      // zero samples for the heavier chains and report NaN.
      { time: 2_000, warmupTime: 500 },
    )
  }
})
