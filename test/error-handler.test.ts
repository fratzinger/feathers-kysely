import assert from 'node:assert'
import { errorHandler, ERROR } from '../src/index.js'

describe('Kysely Error handler', () => {
  it('sqlState', () => {
    assert.throws(
      () =>
        errorHandler({
          sqlState: '#23503',
        }),
      {
        name: 'BadRequest',
      },
    )
  })

  it('sqliteError', () => {
    assert.throws(
      () =>
        errorHandler({
          code: 'SQLITE_ERROR',
          errno: 1,
        }),
      {
        name: 'BadRequest',
      },
    )
    assert.throws(() => errorHandler({ code: 'SQLITE_ERROR', errno: 2 }), {
      name: 'Unavailable',
    })
    assert.throws(() => errorHandler({ code: 'SQLITE_ERROR', errno: 3 }), {
      name: 'Forbidden',
    })
    assert.throws(() => errorHandler({ code: 'SQLITE_ERROR', errno: 12 }), {
      name: 'NotFound',
    })
    assert.throws(() => errorHandler({ code: 'SQLITE_ERROR', errno: 13 }), {
      name: 'GeneralError',
    })
  })

  it('postgresqlError', () => {
    assert.throws(
      () =>
        errorHandler({
          code: '22P02',
          message: 'Key (id)=(1) is not present in table "users".',
          severity: 'ERROR',
          routine: 'ExecConstraints',
        }),
      {
        name: 'NotFound',
      },
    )
    assert.throws(
      () =>
        errorHandler({
          code: '2874',
          message: 'Something',
          severity: 'ERROR',
          routine: 'ExecConstraints',
        }),
      {
        name: 'Forbidden',
      },
    )
    assert.throws(
      () =>
        errorHandler({
          code: '3D74',
          message: 'Something',
          severity: 'ERROR',
          routine: 'ExecConstraints',
        }),
      {
        name: 'Unprocessable',
      },
    )
    assert.throws(
      () =>
        errorHandler({
          code: 'XYZ',
          severity: 'ERROR',
          routine: 'ExecConstraints',
        }),
      {
        name: 'GeneralError',
      },
    )
  })

  it('omits query information from the postgres client message but keeps the raw error', () => {
    const raw = {
      code: '23505',
      message:
        'Failing query: INSERT INTO users (email) VALUES ($1) - duplicate key value violates unique constraint',
      severity: 'ERROR',
      routine: 'exec_stmt',
    }

    try {
      errorHandler(raw)
      assert.fail('errorHandler should have thrown')
    } catch (err: any) {
      assert.strictEqual(err.name, 'BadRequest')
      // The query fragment before the dash must not reach the client message.
      assert.strictEqual(
        err.message,
        'duplicate key value violates unique constraint',
      )
      assert.ok(!err.message.includes('INSERT INTO users'))
      // The original, un-stripped error is preserved for server-side logging.
      assert.strictEqual(err[ERROR], raw)
    }
  })
})
