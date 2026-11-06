import { createDebug } from '@feathersjs/commons'
import { GeneralError } from '@feathersjs/errors'
import type { Kysely } from 'kysely'
import type { KyselyAdapterTransaction } from '../declarations.js'
import {
  commitTransaction,
  flushDeferredEvents,
  rollbackTransaction,
  startTransaction,
} from './lifecycle.js'

const debug = createDebug('feathers-kysely-transaction')

/**
 * Runs `action` in a root transaction of its own — for work no hook wraps: a
 * custom method, or one that does slow work first and opens the transaction only
 * once it is done. The same transaction as `withTransaction()` starts: calls
 * that forward it as `params.transaction` run in it, and with `withTransaction()`
 * on their service their events go out after the commit. Commits once `action`
 * resolves; rolls back when it throws, or when a call that joined failed.
 */
export const runInTransaction = async <T>(
  db: Kysely<any>,
  action: (transaction: KyselyAdapterTransaction) => Promise<T>,
): Promise<T> => {
  const transaction = await startTransaction(undefined, () => db, true)
  if (!transaction) {
    throw new GeneralError(
      'runInTransaction(): no Kysely instance to start one on.',
    )
  }
  debug('started a new transaction %s', transaction.id)

  let result: T
  try {
    result = await action(transaction)
    await commitTransaction(transaction)
  } catch (error) {
    try {
      await rollbackTransaction(transaction)
    } catch (rollbackError) {
      debug('rollback after error failed %o', rollbackError)
    }
    throw error
  }

  flushDeferredEvents(transaction)
  return result
}
