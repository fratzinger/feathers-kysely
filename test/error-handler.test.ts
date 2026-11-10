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

  describe('mysql message sanitization', () => {
    const foreignKey =
      ' (`test`.`children`, CONSTRAINT `children_parent_fk` FOREIGN KEY (`parent_id`) REFERENCES `items` (`id`))'

    it.each([
      [
        'ER_NO_DEFAULT_FOR_FIELD',
        'HY000',
        "Field 'name' doesn't have a default value",
        "Field 'name' doesn't have a default value",
      ],
      [
        'ER_CHECK_CONSTRAINT_VIOLATED',
        'HY000',
        "Check constraint 'items_chk_1' is violated.",
        'Check constraint is violated.',
      ],
      [
        'ER_TRUNCATED_WRONG_VALUE_FOR_FIELD',
        'HY000',
        "Incorrect integer value: 'abc' for column 'name' at row 1",
        "Incorrect integer value for column 'name' at row 1",
      ],
      [
        'WARN_DATA_TRUNCATED',
        '01000',
        "Data truncated for column 'secret' at row 1",
        'Data truncated for column at row 1',
      ],
      [
        'ER_NO_REFERENCED_ROW_2',
        '23000',
        `Cannot add or update a child row: a foreign key constraint fails${foreignKey}`,
        'Cannot add or update a child row: a foreign key constraint fails',
      ],
      [
        'ER_ROW_IS_REFERENCED_2',
        '23000',
        `Cannot delete or update a parent row: a foreign key constraint fails${foreignKey}`,
        'Cannot delete or update a parent row: a foreign key constraint fails',
      ],
      [
        'ER_INVALID_JSON_TEXT',
        '22032',
        'Invalid JSON text: "Invalid value." at position 0 in value for column \'items.name\'.',
        'Invalid JSON text: "Invalid value." at position 0 in value for column \'name\'.',
      ],
      [
        'ER_NO_SUCH_TABLE',
        '42S02',
        "Table 'test.items' doesn't exist",
        "Table doesn't exist",
      ],
      [
        'ER_BAD_FIELD_ERROR',
        '42S22',
        "Unknown column 'nope' in 'field list'",
        'Unknown column',
      ],
    ])('%s → BadRequest: %s', (code, sqlState, message, safe) => {
      const error = { code, sqlState, sqlMessage: message, message }

      assert.throws(
        () => errorHandler(error, new Set(['name'])),
        (err: any) => {
          assert.strictEqual(err.name, 'BadRequest')
          assert.strictEqual(err.message, safe)
          assert.strictEqual(err[ERROR], error)
          return true
        },
      )
    })
  })

  describe('sqlite message sanitization', () => {
    it.each([
      [
        'SQLITE_CONSTRAINT_NOTNULL',
        'NOT NULL constraint failed: items.name',
        'NOT NULL constraint failed: name',
      ],
      [
        'SQLITE_CONSTRAINT_NOTNULL',
        'NOT NULL constraint failed: items.secret',
        'NOT NULL constraint failed',
      ],
      [
        'SQLITE_CONSTRAINT_CHECK',
        'CHECK constraint failed: age >= 0',
        'CHECK constraint failed',
      ],
      [
        'SQLITE_CONSTRAINT_CHECK',
        'CHECK constraint failed: items_age_check',
        'CHECK constraint failed',
      ],
      [
        'SQLITE_CONSTRAINT_FOREIGNKEY',
        'FOREIGN KEY constraint failed',
        'FOREIGN KEY constraint failed',
      ],
      [
        'SQLITE_CONSTRAINT_DATATYPE',
        'cannot store TEXT value in INTEGER column items.name',
        'cannot store TEXT value in INTEGER column name',
      ],
      [
        // a trigger's `RAISE(ABORT, ...)`, worded by the schema's author
        'SQLITE_CONSTRAINT_TRIGGER',
        'price must be positive',
        'price must be positive',
      ],
      ['SQLITE_MISMATCH', 'datatype mismatch', 'datatype mismatch'],
    ])('%s → BadRequest: %s', (code, message, safe) => {
      const error = { code, message }

      assert.throws(
        () => errorHandler(error, new Set(['name'])),
        (err: any) => {
          assert.strictEqual(err.name, 'BadRequest')
          assert.strictEqual(err.message, safe)
          assert.strictEqual(err[ERROR], error)
          return true
        },
      )
    })

    it('strips the table from a SQLITE_ERROR', () => {
      assert.throws(
        () =>
          errorHandler({
            code: 'SQLITE_ERROR',
            message: 'no such table: items',
          }),
        { name: 'GeneralError', message: 'no such table' },
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
      assert.deepStrictEqual(err.data, { columns: ['email'] })
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

      assert.deepStrictEqual(err.data, { columns: ['room'] })
    })

    it('names no column of an index on an expression', () => {
      const err = conflictOf(
        violation('Key (lower(email::text))=(a@b.c) already exists.'),
        ['email'],
      )

      assert.strictEqual(err.data, undefined)
    })

    it('names the exposed ones on SQLite, without their table', () => {
      const error = {
        code: 'SQLITE_CONSTRAINT_UNIQUE',
        message: 'UNIQUE constraint failed: items.email, items.tenantId',
      }
      const err = conflictOf(error, ['email'])

      assert.strictEqual(err.name, 'Conflict')
      assert.strictEqual(
        err.message,
        'duplicate key value violates unique constraint',
      )
      assert.deepStrictEqual(err.data, { columns: ['email'] })
      assert.strictEqual(err[ERROR], error)
      assert.strictEqual(conflictOf(error).data, undefined)
    })

    it('maps a SQLite primary key violation to Conflict', () => {
      const err = conflictOf({
        code: 'SQLITE_CONSTRAINT_PRIMARYKEY',
        message: 'UNIQUE constraint failed: items.id',
      })

      assert.strictEqual(err.name, 'Conflict')
      assert.ok(!err.message.includes('items'))
    })

    it('names no column of a SQLite index on an expression', () => {
      const err = conflictOf(
        {
          code: 'SQLITE_CONSTRAINT_UNIQUE',
          message: "UNIQUE constraint failed: index 'items_lower_email'",
        },
        ['email'],
      )

      assert.strictEqual(err.name, 'Conflict')
      assert.strictEqual(err.data, undefined)
    })

    it('names none on MySQL, nor the value or the table', () => {
      const error = {
        code: 'ER_DUP_ENTRY',
        errno: 1062,
        sqlState: '23000',
        sqlMessage: "Duplicate entry 'a@b.c' for key 'items.email'",
        message: "Duplicate entry 'a@b.c' for key 'items.email'",
      }
      const err = conflictOf(error, ['email'])

      assert.strictEqual(err.name, 'Conflict')
      assert.strictEqual(
        err.message,
        'duplicate key value violates unique constraint',
      )
      assert.strictEqual(err.data, undefined)
      assert.strictEqual(err[ERROR], error)
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
