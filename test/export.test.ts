import { describe, expect, it } from 'vitest'
import * as lib from '../src/index.js'

describe('exports', () => {
  it('exports all expected members', () => {
    expect(Object.keys(lib).sort()).toEqual(
      [
        // error-handler.ts
        'ERROR',
        'errorHandler',

        // adapter.ts
        'KyselyAdapter',

        // service.ts
        'KyselyService',

        // transactions/lifecycle.ts
        'getKysely',

        // transactions/legacy.ts
        'trxCommit',
        'trxRollback',
        'trxStart',

        // transactions/with-transaction.ts
        'withTransaction',

        // transactions/run-in-transaction.ts
        'runInTransaction',

        // transactions/after-commit.ts
        'afterCommit',

        // hooks/update-operators.ts
        'updateOperators',
      ].sort(),
    )
  })
})
