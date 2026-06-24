import { _ } from '@feathersjs/commons'
import { BadRequest } from '@feathersjs/errors'
import type { ExpressionBuilder } from 'kysely'
import type { DialectType } from '../declarations.js'
import { buildArrayUpdate } from './build-array-update.js'

type Eb = ExpressionBuilder<any, any>

/**
 * Replace the column with `value` only when the current value is `null` or
 * compares to `value` under `cmp` — the portable `$min`/`$max` clamp. `$min`
 * keeps the smaller value (`cmp = '>'`: replace when current is greater);
 * `$max` keeps the larger (`cmp = '<'`). Standard SQL `CASE`, all dialects.
 */
const clamp = (eb: Eb, key: string, cmp: '>' | '<', value: number) => {
  const ref = eb.ref(key)
  return eb
    .case()
    .when(eb.or([eb(ref, 'is', null), eb(ref, cmp, value)]))
    .then(eb.val(value))
    .else(ref)
    .end()
}

// Numeric operators: map a column + finite-number value to a SET expression.
const NUMERIC_BUILDERS = {
  $inc: (eb: Eb, key: string, value: number) => eb(eb.ref(key), '+', value),
  $mul: (eb: Eb, key: string, value: number) => eb(eb.ref(key), '*', value),
  $min: (eb: Eb, key: string, value: number) => clamp(eb, key, '>', value),
  $max: (eb: Eb, key: string, value: number) => clamp(eb, key, '<', value),
} as const

// Array operators: handled by buildArrayUpdate (dialect + column-type aware).
const ARRAY_OPERATORS = ['$push', '$pull'] as const

type NumericOperator = keyof typeof NUMERIC_BUILDERS
type ArrayOperator = (typeof ARRAY_OPERATORS)[number]
type UpdateOperator = NumericOperator | ArrayOperator

export const UPDATE_OPERATOR_KEYS = [
  ...(Object.keys(NUMERIC_BUILDERS) as NumericOperator[]),
  ...ARRAY_OPERATORS,
] as UpdateOperator[]

/** True when `data` carries at least one update operator (`$inc`/`$mul`/`$min`/`$max`/`$push`/`$pull`). */
export const containsUpdateOperator = (data: Record<string, any>): boolean =>
  UPDATE_OPERATOR_KEYS.some((op) => op in data)

export interface ApplyUpdateOperatorsOptions {
  /** SQL dialect — required for the `$push`/`$pull` array operators. */
  dialectType?: DialectType
  /**
   * Resolve a column's database type (e.g. `'jsonb'`, `'text[]'`) so `$push` /
   * `$pull` can pick native-array vs. JSON SQL. Typically the adapter's own
   * `getPropertyType`.
   */
  getColumnType?: (column: string) => string | undefined
}

/**
 * Rewrites MongoDB-style update operators into atomic Kysely SET expressions:
 *
 *   { $inc: { views: 1 }, $mul: { price: 2 } }
 *     -> { views: (eb) => eb('views', '+', 1), price: (eb) => eb('price', '*', 2) }
 *
 * Supported:
 * - `$inc` (add), `$mul` (multiply), `$min`/`$max` (clamp to the smaller/larger
 *   of the current value and `value`) — portable, no options needed;
 * - `$push`/`$pull` (append to / remove from an array column) — these need
 *   `options.dialectType` and `options.getColumnType` to choose native-array
 *   vs. JSON SQL per column (see {@link buildArrayUpdate}).
 *
 * The resulting per-column factories are passed straight through to Kysely's
 * `.set()` (the adapter never touches function values). A new object is returned
 * only when an operator was present; otherwise the input is returned untouched.
 *
 * Throws `BadRequest` when an operator payload is not a plain object, a numeric
 * target value is not finite, or an array target is `undefined`.
 */
export function applyUpdateOperators<D extends Record<string, any>>(
  data: D,
  options: ApplyUpdateOperatorsOptions = {},
): D {
  let result = data
  let copied = false

  for (const op of UPDATE_OPERATOR_KEYS) {
    if (!(op in data)) {
      continue
    }

    const values = data[op]
    if (!_.isObject(values) || Array.isArray(values)) {
      throw new BadRequest(`The value for '${op}' must be an object`)
    }

    if (!copied) {
      result = { ...data }
      copied = true
    }

    for (const key of Object.keys(values)) {
      const value = (values as Record<string, unknown>)[key]

      if (op === '$push' || op === '$pull') {
        if (value === undefined) {
          throw new BadRequest(
            `The value for '${op}.${key}' must not be undefined`,
          )
        }
        ;(result as Record<string, any>)[key] = () =>
          buildArrayUpdate({
            key,
            operator: op,
            value,
            dialectType: options.dialectType,
            columnType: options.getColumnType?.(key),
          })
        continue
      }

      if (typeof value !== 'number' || !Number.isFinite(value)) {
        throw new BadRequest(
          `The value for '${op}.${key}' must be a finite number`,
        )
      }

      const build = NUMERIC_BUILDERS[op]
      ;(result as Record<string, any>)[key] = (eb: Eb) => build(eb, key, value)
    }

    delete (result as Record<string, any>)[op]
  }

  return result
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    SqliteAdapter,
    SqliteIntrospector,
    SqliteQueryCompiler,
  } = await import('kysely')

  const mk = (Adapter: any, Introspector: any, Compiler: any) =>
    new Kysely<any>({
      dialect: {
        createAdapter: () => new Adapter(),
        createDriver: () => new DummyDriver(),
        createIntrospector: (db: any) => new Introspector(db),
        createQueryCompiler: () => new Compiler(),
      },
    })
  const pg = mk(PostgresAdapter, PostgresIntrospector, PostgresQueryCompiler)
  const sqlite = mk(SqliteAdapter, SqliteIntrospector, SqliteQueryCompiler)

  const compile = (db: any, data: any) =>
    db.updateTable('t').set(applyUpdateOperators(data)).compile()

  describe('applyUpdateOperators', () => {
    it('rewrites $inc into an atomic SET (Postgres)', () => {
      const { sql, parameters } = compile(pg, { $inc: { age: 1 } })
      expect(sql).toMatch(/set "age" = \(?"age" \+ \$1\)?/)
      expect(parameters).toEqual([1])
    })

    it('rewrites $mul into an atomic SET (Postgres)', () => {
      const { sql, parameters } = compile(pg, { $mul: { price: 2 } })
      expect(sql).toMatch(/set "price" = \(?"price" \* \$1\)?/)
      expect(parameters).toEqual([2])
    })

    it('binds a negative $inc as a parameter (decrement)', () => {
      const { sql, parameters } = compile(pg, { $inc: { age: -5 } })
      expect(sql).toMatch(/set "age" = \(?"age" \+ \$1\)?/)
      expect(parameters).toEqual([-5])
    })

    it('compiles on SQLite (standard arithmetic, ? placeholders)', () => {
      const { sql, parameters } = compile(sqlite, { $inc: { age: 1 } })
      expect(sql).toMatch(/set "age" = \(?"age" \+ \?\)?/)
      expect(parameters).toEqual([1])
    })

    it('rewrites $min into a portable clamping CASE (Postgres)', () => {
      const { sql, parameters } = compile(pg, { $min: { stock: 5 } })
      expect(sql).toContain(
        'set "stock" = case when ("stock" is null or "stock" > $1) then $2 else "stock" end',
      )
      expect(parameters).toEqual([5, 5])
    })

    it('rewrites $max into a portable clamping CASE (Postgres)', () => {
      const { sql, parameters } = compile(pg, { $max: { peak: 100 } })
      expect(sql).toContain(
        'set "peak" = case when ("peak" is null or "peak" < $1) then $2 else "peak" end',
      )
      expect(parameters).toEqual([100, 100])
    })

    it('$min / $max compile on SQLite (? placeholders)', () => {
      const { sql, parameters } = compile(sqlite, { $max: { peak: 7 } })
      expect(sql).toContain(
        'set "peak" = case when ("peak" is null or "peak" < ?) then ? else "peak" end',
      )
      expect(parameters).toEqual([7, 7])
    })

    it('keeps literal columns and drops the operator key', () => {
      const result = applyUpdateOperators({
        name: 'x',
        $inc: { age: 1 },
      }) as Record<string, any>
      expect(result.name).toBe('x')
      expect('$inc' in result).toBe(false)
      expect(typeof result.age).toBe('function')
    })

    it('returns the same reference when no operator is present', () => {
      const data = { name: 'x', age: 5 }
      expect(applyUpdateOperators(data)).toBe(data)
    })

    it('throws BadRequest when the operator payload is not an object', () => {
      expect(() => applyUpdateOperators({ $inc: 5 as any })).toThrow(
        "The value for '$inc' must be an object",
      )
      expect(() => applyUpdateOperators({ $inc: [1, 2] as any })).toThrow(
        /must be an object/,
      )
    })

    it('throws BadRequest for non-finite / non-numeric values', () => {
      expect(() => applyUpdateOperators({ $inc: { age: 'x' as any } })).toThrow(
        "The value for '$inc.age' must be a finite number",
      )
      expect(() => applyUpdateOperators({ $inc: { age: Number.NaN } })).toThrow(
        /must be a finite number/,
      )
      expect(() =>
        applyUpdateOperators({ $mul: { age: Number.POSITIVE_INFINITY } }),
      ).toThrow(/must be a finite number/)
    })

    it('detects operators via containsUpdateOperator', () => {
      expect(containsUpdateOperator({ $inc: { a: 1 } })).toBe(true)
      expect(containsUpdateOperator({ $mul: { a: 1 } })).toBe(true)
      expect(containsUpdateOperator({ a: 1 })).toBe(false)
    })
  })
}
