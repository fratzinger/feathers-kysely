import assert from 'node:assert'
import { BadRequest } from '@feathersjs/errors'
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

  it.each([
    ['02000', 'NotFound'],
    ['28000', 'Forbidden'],
    ['08001', 'Unavailable'],
    ['0A000', 'Unavailable'],
    ['0K000', 'Unavailable'],
    ['20000', 'BadRequest'],
    ['21000', 'BadRequest'],
    ['22001', 'BadRequest'],
    ['23000', 'BadRequest'],
    ['24000', 'BadRequest'],
    ['25000', 'BadRequest'],
    ['40001', 'BadRequest'],
    ['42000', 'BadRequest'],
    ['70100', 'BadRequest'],
    ['HY000', 'GeneralError'],
  ])('maps SQLSTATE %s to %s', (sqlState, name) => {
    assert.throws(() => errorHandler({ sqlState, message: 'Something' }), {
      name,
      message: 'Something',
    })
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
          message: 'invalid input syntax for type integer: "abc"',
          severity: 'ERROR',
          routine: 'ExecConstraints',
        }),
      {
        // class 22 (data exception) → NotFound: a malformed id reads as a
        // missing resource, matching the Feathers adapter convention.
        name: 'NotFound',
      },
    )
    assert.throws(
      () =>
        errorHandler({
          code: '23505',
          message:
            'duplicate key value violates unique constraint "users_email_key"',
          severity: 'ERROR',
          routine: 'ExecConstraints',
        }),
      {
        name: 'Conflict',
      },
    )
    assert.throws(
      () =>
        errorHandler({
          code: '23P01',
          message:
            'conflicting key value violates exclusion constraint "room_reservation_excl"',
          severity: 'ERROR',
          routine: 'ExecConstraints',
        }),
      {
        name: 'Conflict',
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

  describe('postgres message sanitization', () => {
    const notNull = {
      code: '23502',
      message:
        'null value in column "name" of relation "items" violates not-null constraint',
      severity: 'ERROR',
      routine: 'ExecConstraints',
    }

    it('keeps declared column names but strips table/relation names', () => {
      assert.throws(
        () => errorHandler(notNull, new Set(['name'])),
        (err: any) => {
          assert.strictEqual(err.name, 'BadRequest')
          // The declared column survives (useful, and already public)...
          assert.match(err.message, /not-null constraint/)
          assert.ok(err.message.includes('"name"'))
          // ...but the relation/table name does not, and the message is not
          // the mangled `null constraint` the old split('-') produced.
          assert.ok(!err.message.includes('items'))
          assert.notStrictEqual(err.message, 'null constraint')
          // The raw, un-sanitized error is preserved for server-side logging.
          assert.strictEqual(err[ERROR], notNull)
          return true
        },
      )
    })

    it('strips constraint names from unique violations (Conflict)', () => {
      assert.throws(
        () =>
          errorHandler(
            {
              code: '23505',
              message:
                'duplicate key value violates unique constraint "items_email_key"',
              severity: 'ERROR',
              routine: 'ExecConstraints',
            },
            new Set(['name']),
          ),
        (err: any) => {
          assert.strictEqual(err.name, 'Conflict')
          assert.match(err.message, /unique constraint/)
          assert.ok(!err.message.includes('"'))
          return true
        },
      )
    })

    it('strips every identifier when no known columns are provided', () => {
      assert.throws(
        () => errorHandler(notNull),
        (err: any) => {
          assert.strictEqual(err.name, 'BadRequest')
          assert.match(err.message, /not-null constraint/)
          assert.ok(!err.message.includes('"'))
          assert.ok(!err.message.includes('name'))
          assert.ok(!err.message.includes('items'))
          return true
        },
      )
    })
  })

  describe('the columns a unique violation names', () => {
    const violation = (detail: string, code = '23505') => ({
      code,
      message:
        'duplicate key value violates unique constraint "items_email_tenant_key"',
      detail,
      severity: 'ERROR',
      routine: '_bt_check_unique',
    })
    const conflictOf = (error: unknown, exposeConflictColumns?: string[]) => {
      try {
        errorHandler(error, new Set(['email', 'tenantId']), {
          exposeConflictColumns,
        })
      } catch (err: any) {
        return err
      }
    }

    it('names the exposed ones, never their values', () => {
      const err = conflictOf(
        violation('Key (email, "tenantId")=(a@b.c, 7) already exists.'),
        ['email'],
      )

      assert.strictEqual(err.name, 'Conflict')
      assert.deepStrictEqual(err.data, { conflict: ['email'] })
      assert.ok(!JSON.stringify(err.data).includes('a@b.c'))
      assert.ok(!err.message.includes('a@b.c'))
    })

    it('names none without the option, or with none of them exposed', () => {
      const error = violation(
        'Key (email, "tenantId")=(a@b.c, 7) already exists.',
      )

      assert.strictEqual(conflictOf(error).data, undefined)
      assert.strictEqual(conflictOf(error, ['name']).data, undefined)
    })

    it('names the columns of an exclusion violation too', () => {
      const err = conflictOf(
        violation(
          'Key (room, during)=(1, [2020-01-01,2020-01-02)) conflicts with existing key (room, during)=(1, [2020-01-01,2020-01-03)).',
          '23P01',
        ),
        ['room'],
      )

      assert.deepStrictEqual(err.data, { conflict: ['room'] })
    })

    it('names no column of an index on an expression', () => {
      const err = conflictOf(
        violation('Key (lower(email::text))=(a@b.c) already exists.'),
        ['email'],
      )

      assert.strictEqual(err.data, undefined)
    })
  })

  describe('non-database and Feathers errors', () => {
    it('passes an existing FeathersError through unchanged', () => {
      assert.throws(() => errorHandler(new BadRequest('nope')), {
        name: 'BadRequest',
        message: 'nope',
      })
    })

    it('wraps an unknown (non-database) error as GeneralError', () => {
      assert.throws(() => errorHandler(new Error('boom')), {
        name: 'GeneralError',
      })
    })
  })
})
