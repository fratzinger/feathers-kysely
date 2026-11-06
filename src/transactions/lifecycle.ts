import { createDebug } from '@feathersjs/commons'
import { GeneralError } from '@feathersjs/errors'
import type { HookContext } from '@feathersjs/feathers'
import type { ControlledTransaction, Kysely, KyselyPlugin } from 'kysely'
import type {
  KyselyAdapterParams,
  KyselyAdapterTransaction,
} from '../declarations.js'

// The lifecycle of a transaction, shared by `withTransaction()` and the legacy
// hooks. Internal: `index.ts` exports only `getKysely`.

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

export const markRollbackOnly = (
  transaction: KyselyAdapterTransaction,
): void => {
  rollbackOnly.add(transaction)
}

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
export const startTransaction = async (
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

export const commitTransaction = async (
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

export const rollbackTransaction = async (
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
export const flushDeferredEvents = (
  transaction: KyselyAdapterTransaction,
): void => {
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
