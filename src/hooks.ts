import { createDebug } from '@feathersjs/commons'
import type { HookContext } from '@feathersjs/feathers'
import type { Kysely } from 'kysely'
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

export const trxStart =
  () =>
  async (context: HookContext): Promise<void> => {
    const { transaction: parent } = context.params as KyselyAdapterParams
    const db: Kysely<any> | undefined = parent ? parent.trx : getKysely(context)

    if (!db) {
      return
    }

    const trx = await db.startTransaction().execute()

    const transaction: KyselyAdapterTransaction = {
      trx,
      id: Date.now(),
      starting: false,
    }

    if (parent) {
      transaction.parent = parent
      transaction.committed = parent.committed
    } else {
      transaction.committed = new Promise((resolve) => {
        transaction.resolve = resolve
      })
    }

    context.params = { ...context.params, transaction }
    debug('started a new transaction %s', transaction.id)
  }

export const trxCommit = () => async (context: HookContext) => {
  const { transaction } = context.params as KyselyAdapterParams

  if (!transaction) {
    return
  }

  const { trx, id, parent } = transaction

  context.params = { ...context.params, transaction: parent }

  await trx.commit().execute()

  if (transaction.resolve) {
    transaction.resolve(true)
  }

  debug('ended transaction %s', id)

  return context
}

export const trxRollback = () => async (context: HookContext) => {
  const { transaction } = context.params as KyselyAdapterParams

  if (!transaction) {
    return
  }

  const { trx, id, parent } = transaction

  context.params = { ...context.params, transaction: parent }

  await trx.rollback().execute()

  if (transaction.resolve) {
    transaction.resolve(false)
  }

  debug('rolled back transaction %s', id)

  return context
}
