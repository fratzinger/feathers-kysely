import { Kysely, SqliteDialect } from 'kysely'
// eslint-disable-next-line import-x/no-named-as-default
import Database from 'better-sqlite3'

import type { KyselyService } from '../src/index.js'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import {
  buildApp,
  FIND_OPTIONS,
  QUERY_CASES,
  type BenchDB,
} from './relation-graph.js'

/**
 * Snapshots the SQL the adapter generates for every relation case.
 *
 * Kysely compiles without touching the database, so this needs no schema and
 * no data — and it deliberately pins the dialect to sqlite regardless of `DB`,
 * because the point is our own output (which subqueries, which aliases, how
 * many of them), not per-dialect syntax. One snapshot set, identical in every
 * CI job.
 *
 * A change to query building shows up here as a readable diff. Update with
 * `vitest -u` once the new SQL has been reviewed.
 */

const db = new Kysely<BenchDB>({
  dialect: new SqliteDialect({ database: new Database(':memory:') }),
})

const app = buildApp(db)

describe('sql shape', () => {
  beforeAll(() => app.setup())
  afterAll(() => db.destroy())

  for (const testCase of QUERY_CASES) {
    it(testCase.name, () => {
      const service = app.service(
        testCase.service,
      ) as unknown as KyselyService<any>
      const { sql, parameters } = service
        .composeQuery({ query: testCase.query }, FIND_OPTIONS)
        .compile()

      expect({ sql, parameters }).toMatchSnapshot()
    })
  }
})
