import { createDebug } from '@feathersjs/commons'
import { GeneralError } from '@feathersjs/errors'
import type { HookContext, NextFunction } from '@feathersjs/feathers'
import type { ControlledTransaction, Kysely, KyselyPlugin } from 'kysely'
import type {
  KyselyAdapterParams,
  KyselyAdapterTransaction,
} from '../declarations.js'
import { deferEvent } from './defer-event.js'

const debug = createDebug('feathers-kysely-transaction')

export const getKysely = (context: HookContext): Kysely<any> | undefined => {
  const db =
    typeof context.service.getModel === 'function' &&
    context.service.getModel(context.params)

  return db && typeof db.startTransaction === 'function' ? db : undefined
}

let savepointSeq = 0

/** Per savepoint: how many events the root had queued when it opened. */
const queuedBefore = new WeakMap<KyselyAdapterTransaction, number>()

/** Roots whose COMMIT or ROLLBACK went out. */
const ended = new WeakSet<KyselyAdapterTransaction>()

/** Transactions a joined call failed in: they may only roll back. */
const rollbackOnly = new WeakSet<KyselyAdapterTransaction>()

/** Transactions rolled back already. */
const rolledBack = new WeakSet<KyselyAdapterTransaction>()

const PARALLEL_MESSAGE =
  "withTransaction(): nested calls ran in parallel. Savepoints of one transaction form a stack, so nested calls with savepoints of their own must run one after another — or let them join the transaction, the default without { nested: 'savepoint' }."

const ROLLBACK_ONLY_MESSAGE =
  "withTransaction(): a call that joined this transaction failed, so it rolls back rather than commit what that call wrote. Let the error roll back the transaction, or give the nested call a savepoint of its own to recover from: withTransaction({ nested: 'savepoint' })."

const OUTLIVED_MESSAGE =
  'withTransaction(): a nested call outlived its transaction, which already committed or rolled back. Await every nested call before the call that started the transaction returns.'

const STILL_RUNNING_MESSAGE =
  'withTransaction(): a nested call is still running, so the transaction rolls back rather than commit what that call wrote so far. Await every nested call before the call that started the transaction returns.'

const rootOf = (
  transaction: KyselyAdapterTransaction,
): KyselyAdapterTransaction =>
  transaction.parent ? rootOf(transaction.parent) : transaction

/**
 * Throws unless `transaction` may go on: its root still open, its savepoint
 * still there and the innermost one. Otherwise a call that outlived its root
 * would write on the released connection — outside any transaction — and one
 * running in parallel would release or roll back a sibling's savepoint along
 * with its own.
 */
const assertInnermost = (transaction: KyselyAdapterTransaction): void => {
  const { savepoint, savepoints = [] } = transaction
  if (
    ended.has(rootOf(transaction)) ||
    (savepoint && !savepoints.includes(savepoint))
  ) {
    throw new GeneralError(OUTLIVED_MESSAGE)
  }
  if (savepoints.at(-1) !== savepoint) {
    throw new GeneralError(PARALLEL_MESSAGE)
  }
}

/**
 * Refuses every query once the root's COMMIT or ROLLBACK went out. Kysely's own
 * check flips only once that statement returned, and never for the instances
 * `savepoint()` returns.
 */
const refuseAfterEnd = (root: KyselyAdapterTransaction): KyselyPlugin => ({
  transformQuery: ({ node }) => {
    if (ended.has(root)) {
      throw new GeneralError(OUTLIVED_MESSAGE)
    }
    return node
  },
  transformResult: async ({ result }) => result,
})

/**
 * Starts a root transaction (`startTransaction()`) or, when an existing
 * `params.transaction` is found, a nested transaction backed by a savepoint
 * (`savepoint()`). Returns `undefined` (caller should pass through) when no
 * Kysely instance / transaction is available.
 */
const startTransaction = async (
  context: HookContext,
  withDeferred: boolean,
): Promise<KyselyAdapterTransaction | undefined> => {
  const { transaction: parent } = context.params as KyselyAdapterParams

  let trx: ControlledTransaction<any>
  let savepoint: string | undefined

  if (parent) {
    // a transaction passed in by hand starts the stack
    parent.savepoints ??= []
    assertInnermost(parent)
    savepoint = `fk_sp_${++savepointSeq}`
    // claimed before the statement goes out, so a parallel call sees it
    parent.savepoints.push(savepoint)
    try {
      await (parent.trx as any).savepoint(savepoint).execute()
    } catch (error) {
      parent.savepoints.pop()
      throw error
    }
    // Not the instance `savepoint()` returns: that one never learns the root
    // ended. The root's does, and carries the guard of `refuseAfterEnd()`.
    trx = parent.trx
  } else {
    const db = getKysely(context)
    if (!db) {
      return undefined
    }
    trx = await db.startTransaction().execute()
  }

  const transaction: KyselyAdapterTransaction = {
    trx,
    id: Date.now(),
    starting: false,
  }

  if (parent) {
    transaction.parent = parent
    transaction.savepoint = savepoint
    transaction.savepoints = parent.savepoints
    if (parent.committed) {
      // settled on rollback, or once released and the parent committed
      transaction.committed = new Promise((resolve) => {
        transaction.resolve = resolve
      })
    }
    if (withDeferred) {
      // Share the root's queue so every nesting level pushes into one array.
      transaction.deferredEvents = parent.deferredEvents
      if (parent.deferredEvents) {
        queuedBefore.set(transaction, parent.deferredEvents.length)
      }
    }
  } else {
    transaction.trx = trx.withPlugin(refuseAfterEnd(transaction))
    transaction.committed = new Promise((resolve) => {
      transaction.resolve = resolve
    })
    if (withDeferred) {
      transaction.deferredEvents = []
    }
    transaction.savepoints = []
  }

  return transaction
}

const commitTransaction = async (
  transaction: KyselyAdapterTransaction,
): Promise<void> => {
  const { trx, id, savepoint, savepoints, parent, resolve } = transaction

  if (rollbackOnly.has(transaction)) {
    await rollbackTransaction(transaction)
    throw new GeneralError(ROLLBACK_ONLY_MESSAGE)
  }

  try {
    if (savepoint) {
      // releasing an outer savepoint would release an inner one along with it
      assertInnermost(transaction)
      await (trx as any).releaseSavepoint(savepoint).execute()
      savepoints!.pop()
    } else {
      if (savepoints?.length) {
        throw new GeneralError(STILL_RUNNING_MESSAGE)
      }
      ended.add(transaction)
      await trx.commit().execute()
    }
  } catch (error) {
    // A failing COMMIT / RELEASE SAVEPOINT must still release anything awaiting
    // `transaction.committed`, otherwise those awaiters hang forever.
    if (resolve) {
      resolve(false)
    }
    throw error
  }

  if (resolve) {
    if (parent?.committed) {
      // a released savepoint commits only along with its parent
      void parent.committed.then((isCommitted) => resolve(!!isCommitted))
    } else {
      resolve(true)
    }
  }

  // NOTE: deferred events are intentionally NOT flushed here. The caller flushes
  // them after the commit succeeds (see flushDeferredEvents), so that a throwing
  // event listener cannot bubble up and trigger a rollback of an already
  // committed transaction.

  debug('ended transaction %s', id)
}

const rollbackTransaction = async (
  transaction: KyselyAdapterTransaction,
): Promise<void> => {
  const { trx, id, savepoint, savepoints } = transaction

  if (rolledBack.has(transaction)) {
    return
  }
  rolledBack.add(transaction)

  try {
    if (savepoint) {
      // Gone already when an outer savepoint or the root rolled back, taking
      // this one along. Otherwise the database drops every savepoint opened
      // after it, too.
      const index = savepoints!.lastIndexOf(savepoint)
      if (index !== -1) {
        savepoints!.length = index
        if (!ended.has(rootOf(transaction))) {
          await (trx as any).rollbackToSavepoint(savepoint).execute()
        }
      }
    } else {
      ended.add(transaction)
      await trx.rollback().execute()
    }
  } finally {
    // Always release awaiters and discard deferred events, even if the
    // rollback statement itself fails.
    if (transaction.resolve) {
      transaction.resolve(false)
    }

    // Only the root owns the queue; discard everything that was deferred.
    if (!transaction.parent && transaction.deferredEvents) {
      transaction.deferredEvents.length = 0
    }

    // A savepoint takes the events of the calls within it along — the root may
    // still commit.
    const before = queuedBefore.get(transaction)
    if (transaction.deferredEvents && before !== undefined) {
      transaction.deferredEvents.length = before
    }
  }

  debug('rolled back transaction %s', id)
}

/**
 * Flush the root transaction's deferred Feathers events after a successful
 * commit. Each emitter is isolated so a throwing listener can neither abort the
 * remaining events nor bubble up and roll back the already-committed
 * transaction. No-op for nested (savepoint) transactions and for legacy
 * transactions that never deferred events.
 */
const flushDeferredEvents = (transaction: KyselyAdapterTransaction): void => {
  if (transaction.parent || !transaction.deferredEvents) {
    return
  }

  const queue = transaction.deferredEvents
  transaction.deferredEvents = []
  for (const emit of queue) {
    try {
      emit()
    } catch (error) {
      debug('deferred event listener threw %o', error)
    }
  }
}

/**
 * @deprecated Use the `withTransaction()` around hook instead. Kept for
 * backward compatibility; this legacy hook does NOT defer cross-service events.
 */
export const trxStart =
  () =>
  async (context: HookContext): Promise<void> => {
    const transaction = await startTransaction(context, false)

    if (!transaction) {
      return
    }

    context.params = { ...context.params, transaction }
    debug('started a new transaction %s', transaction.id)
  }

/**
 * @deprecated Use the `withTransaction()` around hook instead. Kept for
 * backward compatibility; this legacy hook does NOT defer cross-service events.
 */
export const trxCommit = () => async (context: HookContext) => {
  const { transaction } = context.params as KyselyAdapterParams

  if (!transaction) {
    return
  }

  context.params = { ...context.params, transaction: transaction.parent }

  await commitTransaction(transaction)

  return context
}

/**
 * @deprecated Use the `withTransaction()` around hook instead. Kept for
 * backward compatibility; this legacy hook does NOT defer cross-service events.
 */
export const trxRollback = () => async (context: HookContext) => {
  const { transaction } = context.params as KyselyAdapterParams

  if (!transaction) {
    return
  }

  context.params = { ...context.params, transaction: transaction.parent }

  await rollbackTransaction(transaction)

  return context
}

const MUTATING_METHODS = new Set(['create', 'update', 'patch', 'remove'])

export type WithTransactionOptions = {
  /**
   * Without a transaction: start one (default), or run without one — for a
   * service only ever called within another call's transaction.
   *
   * @default true
   */
  start?: boolean
  /**
   * Within a transaction (a call forwarding `params.transaction`): `join` it
   * (default) — parallel-safe; a failure leaves the transaction rollback-only,
   * even when the caller catches it — or a `savepoint` of its own: a failure
   * rolls back only this call and the caller may go on, but nested calls can't
   * run in parallel. Either way the call's event waits for the root's commit.
   *
   * @default 'join'
   */
  nested?: 'savepoint' | 'join'
}

/**
 * Around hook that wraps a service method in a Kysely `ControlledTransaction`:
 * start → commit on success → rollback on error. Nested calls that forward
 * `params.transaction` join it, or open a savepoint (`nested: 'savepoint'`). Feathers
 * service events (`created`/`updated`/`patched`/`removed`) are deferred and only
 * emitted once the root transaction commits — and discarded on rollback —
 * including events from nested cross-service calls.
 *
 * Register per method (`around: { create: [withTransaction()] }`) or, for full
 * cross-service deferral, app-wide (`app.hooks({ around: [withTransaction()] })`).
 * Only engages for `create`/`update`/`patch`/`remove`; every other method and
 * any non-Kysely / transaction-incapable service is a transparent passthrough.
 */
export const withTransaction =
  (options?: WithTransactionOptions) =>
  async (context: HookContext, next: NextFunction): Promise<void> => {
    if (!MUTATING_METHODS.has(context.method)) {
      await next()
      return
    }

    const parent = (context.params as KyselyAdapterParams).transaction

    if (parent && options?.nested !== 'savepoint') {
      try {
        await next()
      } catch (error) {
        // There is no savepoint to roll back to. Whether the caller catches
        // the error or not, the transaction must not commit what this call
        // wrote — nor, on PostgreSQL, end in a COMMIT that silently rolls back
        // a transaction aborted by a failed statement.
        rollbackOnly.add(parent)
        throw error
      }
      deferEvent(context, parent)
      return
    }

    if (!parent && options?.start === false) {
      await next()
      return
    }

    const transaction = await startTransaction(context, true)

    if (!transaction) {
      await next()
      return
    }

    context.params = { ...context.params, transaction }
    debug('started a new transaction %s', transaction.id)

    try {
      await next()

      // The call that started the root leaves its event to Feathers, which
      // emits it once every hook of the call is done — after the commit, and
      // not at all after a rollback.
      if (parent) {
        deferEvent(context, transaction)
      }

      context.params = { ...context.params, transaction: parent }
      await commitTransaction(transaction)
    } catch (error) {
      context.params = { ...context.params, transaction: parent }
      try {
        await rollbackTransaction(transaction)
      } catch (rollbackError) {
        debug('rollback after error failed %o', rollbackError)
      }
      throw error
    }

    // Reached only on the success path (the catch above rethrows). Emit the
    // deferred events now that the (root) transaction is durably committed.
    flushDeferredEvents(transaction)
  }
