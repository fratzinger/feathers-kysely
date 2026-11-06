import { createDebug } from '@feathersjs/commons'
import type { HookContext } from '@feathersjs/feathers'
import type { KyselyAdapterParams } from '../declarations.js'
import {
  commitTransaction,
  rollbackTransaction,
  startTransaction,
} from './lifecycle.js'

const debug = createDebug('feathers-kysely-transaction')

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
