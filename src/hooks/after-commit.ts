import { createDebug } from '@feathersjs/commons'
import type { Params } from '@feathersjs/feathers'
import type { KyselyAdapterParams } from '../declarations.js'

const debug = createDebug('feathers-kysely-transaction')

export type AfterCommitOptions = {
  /**
   * Called when `action` fails after the commit. Without it, the error is only
   * debug-logged — the caller has long returned.
   */
  onError?: (error: unknown) => void
}

/**
 * Runs `action` once the transaction of `params` committed — in the background,
 * and not at all after a rollback, a savepoint's included. Without a
 * transaction, or with one passed in by hand, right away. For what a rollback
 * can't take back: a push, a cache invalidation, a job in another system.
 * Within a transaction it returns right away, before the action ran.
 */
export const afterCommit = async (
  params: Params | undefined,
  action: () => unknown,
  options?: AfterCommitOptions,
): Promise<void> => {
  const committed = (params as KyselyAdapterParams | undefined)?.transaction
    ?.committed
  if (!committed) {
    await action()
    return
  }

  void committed
    .then(async (isCommitted) => {
      if (isCommitted) {
        await action()
      }
    })
    .catch((error) => {
      if (options?.onError) {
        options.onError(error)
      } else {
        debug('action after commit failed %o', error)
      }
    })
}
