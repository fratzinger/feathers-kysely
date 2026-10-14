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

        // hooks/with-transaction.ts
        'getKysely',
        'trxCommit',
        'trxRollback',
        'trxStart',
        'withTransaction',

        // hooks/after-commit.ts
        'afterCommit',

        // hooks/update-operators.ts
        'updateOperators',
      ].sort(),
    )
  })
})
