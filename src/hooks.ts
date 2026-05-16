import { createDebug } from '@feathersjs/commons'
import type { HookContext, NextFunction } from '@feathersjs/feathers'
import { getServiceOptions } from '@feathersjs/feathers'
import type { ControlledTransaction, Kysely } from 'kysely'
import type {
  KyselyAdapterParams,
  KyselyAdapterTransaction,
} from './declarations.js'

const debug = createDebug('feathers-kysely-transaction')

export const getKysely = (context: HookContext): Kysely<any> | undefined => {
  const db =
    typeof context.service.getModel === 'function' &&
    context.service.getModel(context.params)

  return db && typeof db.startTransaction === 'function' ? db : undefined
}

let savepointSeq = 0

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
    savepoint = `fk_sp_${++savepointSeq}`
    trx = await (parent.trx as any).savepoint(savepoint).execute()
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
    transaction.committed = parent.committed
    if (withDeferred) {
      // Share the root's queue so every nesting level pushes into one array.
      transaction.deferredEvents = parent.deferredEvents
    }
  } else {
    transaction.committed = new Promise((resolve) => {
      transaction.resolve = resolve
    })
    if (withDeferred) {
      transaction.deferredEvents = []
    }
  }

  return transaction
}

const commitTransaction = async (
  transaction: KyselyAdapterTransaction,
): Promise<void> => {
  const { trx, id, savepoint } = transaction

  if (savepoint) {
    await (trx as any).releaseSavepoint(savepoint).execute()
  } else {
    await trx.commit().execute()
  }

  if (transaction.resolve) {
    transaction.resolve(true)
  }

  // Only the root flushes the shared deferred-event queue.
  if (!transaction.parent && transaction.deferredEvents) {
    const queue = transaction.deferredEvents
    transaction.deferredEvents = []
    for (const emit of queue) {
      emit()
    }
  }

  debug('ended transaction %s', id)
}

const rollbackTransaction = async (
  transaction: KyselyAdapterTransaction,
): Promise<void> => {
  const { trx, id, savepoint } = transaction

  if (savepoint) {
    await (trx as any).rollbackToSavepoint(savepoint).execute()
  } else {
    await trx.rollback().execute()
  }

  if (transaction.resolve) {
    transaction.resolve(false)
  }

  // Only the root owns the queue; discard everything that was deferred.
  if (!transaction.parent && transaction.deferredEvents) {
    transaction.deferredEvents.length = 0
  }

  debug('rolled back transaction %s', id)
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

/**
 * Around hook that wraps a service method in a Kysely `ControlledTransaction`:
 * start → commit on success → rollback on error. Nested calls that forward
 * `params.transaction` automatically use savepoints. Feathers service events
 * (`created`/`updated`/`patched`/`removed`) are deferred and only emitted once
 * the root transaction commits — and discarded on rollback — including events
 * from nested cross-service calls.
 *
 * Register per method (`around: { create: [withTransaction()] }`) or, for full
 * cross-service deferral, app-wide (`app.hooks({ around: [withTransaction()] })`).
 * Only engages for `create`/`update`/`patch`/`remove`; every other method and
 * any non-Kysely / transaction-incapable service is a transparent passthrough.
 */
export const withTransaction =
  () =>
  async (context: HookContext, next: NextFunction): Promise<void> => {
    if (!MUTATING_METHODS.has(context.method)) {
      await next()
      return
    }

    const parent = (context.params as KyselyAdapterParams).transaction
    const transaction = await startTransaction(context, true)

    if (!transaction) {
      await next()
      return
    }

    context.params = { ...context.params, transaction }
    debug('started a new transaction %s', transaction.id)

    try {
      await next()

      // Capture this call's pending event into the shared root queue and
      // suppress the outer Feathers `eventHook` so nothing is emitted before
      // the root commits. Mirrors @feathersjs/feathers/lib/events.js exactly.
      const self = context.self
      const event = context.event
      if (typeof event === 'string' && transaction.deferredEvents) {
        const events = getServiceOptions(self).events ?? []
        if (!events.includes(event)) {
          const results = Array.isArray(context.result)
            ? context.result
            : [context.result]
          for (const element of results) {
            transaction.deferredEvents.push(() =>
              self.emit(event, element, context),
            )
          }
        }
        context.event = null
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
    } finally {
      if ((context.params as KyselyAdapterParams).transaction === transaction) {
        context.params = { ...context.params, transaction: parent }
      }
    }
  }
