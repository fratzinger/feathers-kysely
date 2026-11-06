import { createDebug } from '@feathersjs/commons'
import type { HookContext } from '@feathersjs/feathers'
import type { KyselyAdapterTransaction } from '../declarations.js'

const debug = createDebug('feathers-kysely-transaction')

/**
 * Moves the call's pending event into the root's queue and clears
 * `context.event`, so Feathers' event hook doesn't emit it before the root
 * commits. Mirrors @feathersjs/feathers/lib/events.js: the queue reads
 * `context.result` only when it flushes — once every hook of the call is done,
 * resolvers and `dispatch` included. A no-op for a transaction that doesn't
 * defer (legacy hooks, one passed in by hand).
 */
export const deferEvent = (
  context: HookContext,
  transaction: KyselyAdapterTransaction,
): void => {
  const self = context.self
  const event = context.event
  if (typeof event !== 'string' || !transaction.deferredEvents) {
    return
  }

  // Mirrors @feathersjs/feathers' eventHook: skip the default emit when the
  // service declares this event itself. We read `self.events` directly instead
  // of `getServiceOptions()` to avoid a value import of the CJS-only
  // @feathersjs/feathers, which breaks our ESM consumers.
  const events: string[] = (self as any).events ?? []
  if (!events.includes(event)) {
    transaction.deferredEvents.push(() => {
      const results = Array.isArray(context.result)
        ? context.result
        : [context.result]
      for (const element of results) {
        // one throwing listener must not keep the others from their events
        try {
          self.emit(event, element, context)
        } catch (error) {
          debug('deferred event listener threw %o', error)
        }
      }
    })
  }
  context.event = null
}
