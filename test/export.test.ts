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

        // hooks.ts
        'getKysely',
        'trxCommit',
        'trxRollback',
        'trxStart',
      ].sort(),
    )
  })
})
