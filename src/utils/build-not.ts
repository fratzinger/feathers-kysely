import { sql } from 'kysely'
import type {
  Expression,
  OperationNode,
  OperatorNode,
  ParensNode,
  UnaryOperationNode,
} from 'kysely'

// An EXISTS is TRUE or FALSE, never UNKNOWN — and so is the NOT of one.
const isNeverUnknown = (node: OperationNode): boolean => {
  if (node.kind === 'ParensNode') {
    return isNeverUnknown((node as ParensNode).node)
  }
  if (node.kind !== 'UnaryOperationNode') return false

  const { operator, operand } = node as UnaryOperationNode
  if (operator.kind !== 'OperatorNode') return false

  const op = (operator as OperatorNode).operator
  return (
    op === 'exists' ||
    op === 'not exists' ||
    (op === 'not' && isNeverUnknown(operand))
  )
}

/**
 * Negate a condition so that the result is its exact complement.
 *
 * A bare `NOT (age > 1)` is UNKNOWN wherever `age > 1` is — for a NULL age —
 * so that row matches neither the condition nor its negation. `IS NOT TRUE`
 * maps UNKNOWN to true: `(age > 1) is not true` keeps the NULL row, as a
 * Mongo `$not` does. Standard SQL — all dialects.
 *
 * The condition is parenthesized here: MySQL and SQLite give `IS` the same
 * precedence as `=` / `LIKE` / `IN`, so an unparenthesized `a = 1 is not true`
 * would bind differently.
 *
 * An EXISTS (a relation path) is never UNKNOWN, so it keeps a bare `NOT`:
 * the planner turns `not exists (...)` into an anti-join, which
 * `(exists (...)) is not true` would hide from it.
 */
export function buildNot(condition: Expression<any>) {
  const node = condition.toOperationNode()

  if (isNeverUnknown(node)) {
    return sql<boolean>`not ${condition}`
  }

  // `eb.and` / `eb.or` over several conditions come parenthesized already.
  return node.kind === 'ParensNode'
    ? sql<boolean>`${condition} is not true`
    : sql<boolean>`(${condition}) is not true`
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    expressionBuilder,
  } = await import('kysely')

  const pg = new Kysely<any>({
    dialect: {
      createAdapter: () => new PostgresAdapter(),
      createDriver: () => new DummyDriver(),
      createIntrospector: (db) => new PostgresIntrospector(db),
      createQueryCompiler: () => new PostgresQueryCompiler(),
    },
  })
  const eb = expressionBuilder<any, any>()

  const compile = (condition: Expression<any>) => {
    const { sql: text, parameters } = pg
      .selectFrom('users')
      .select('id')
      .where(buildNot(condition))
      .compile()
    // strip the constant prefix so the assertions read as just the predicate
    return {
      sql: text.replace('select "id" from "users" where ', ''),
      parameters,
    }
  }

  const exists = () =>
    eb.exists(eb.selectFrom('todos').select('id').where('done', '=', true))

  describe('buildNot', () => {
    it('negates with IS NOT TRUE around the parenthesized condition', () => {
      expect(compile(eb('age', '>', 1))).toEqual({
        sql: '("age" > $1) is not true',
        parameters: [1],
      })
      expect(compile(eb.and([eb('age', '>', 1), exists()])).sql).toBe(
        '("age" > $1 and exists (select "id" from "todos" where "done" = $2)) is not true',
      )
    })

    it('keeps a bare NOT for an EXISTS, which is never UNKNOWN', () => {
      expect(compile(exists()).sql).toBe(
        'not exists (select "id" from "todos" where "done" = $1)',
      )
      expect(compile(eb.not(exists())).sql).toBe(
        'not not exists (select "id" from "todos" where "done" = $1)',
      )
    })
  })
}
