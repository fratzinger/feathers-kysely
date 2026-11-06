import { createDebug } from '@feathersjs/commons'
import type { HookContext, NextFunction } from '@feathersjs/feathers'
import type { KyselyAdapterParams } from '../declarations.js'
import { deferEvent } from './defer-event.js'
import {
  commitTransaction,
  flushDeferredEvents,
  getKysely,
  markRollbackOnly,
  rollbackTransaction,
  startTransaction,
} from './lifecycle.js'

const debug = createDebug('feathers-kysely-transaction')

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
        markRollbackOnly(parent)
        throw error
      }
      deferEvent(context, parent)
      return
    }

    if (!parent && options?.start === false) {
      await next()
      return
    }

    const transaction = await startTransaction(
      parent,
      () => getKysely(context),
      true,
    )

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
