import { describe, expect, it, vi } from 'vitest'
import { deferEvent } from '../src/hooks/defer-event.js'
import type { KyselyAdapterTransaction } from '../src/index.js'

/** A call's context after the method ran, as far as `deferEvent` reads it. */
const contextOf = (options: {
  event: string | null
  result: unknown
  events?: string[]
}) =>
  ({
    self: { emit: vi.fn(), events: options.events },
    event: options.event,
    result: options.result,
  }) as any

const deferring = () =>
  ({ deferredEvents: [] as Array<() => void> }) as KyselyAdapterTransaction & {
    deferredEvents: Array<() => void>
  }

describe('deferEvent', () => {
  it("queues the call's event and keeps Feathers from emitting it now", () => {
    const context = contextOf({ event: 'created', result: { id: 1 } })
    const transaction = deferring()

    deferEvent(context, transaction)

    expect(context.event).toBeNull()
    expect(context.self.emit).not.toHaveBeenCalled()

    transaction.deferredEvents.forEach((emit) => emit())
    expect(context.self.emit).toHaveBeenCalledWith(
      'created',
      { id: 1 },
      context,
    )
  })

  it('reads the result only when the queue flushes, once every hook is done', () => {
    const context = contextOf({ event: 'created', result: { id: 1 } })
    const transaction = deferring()

    deferEvent(context, transaction)
    // an outer hook — a resolver, say — finishes after withTransaction()
    context.result = { id: 1, resolved: true }
    context.dispatch = { id: 1 }
    transaction.deferredEvents.forEach((emit) => emit())

    // As in Feathers: listeners get the result, the channels read `dispatch`
    // off the context for the clients.
    expect(context.self.emit).toHaveBeenCalledWith(
      'created',
      { id: 1, resolved: true },
      expect.objectContaining({ dispatch: { id: 1 } }),
    )
  })

  it('queues one event per item of a multi call, in order', () => {
    const context = contextOf({
      event: 'created',
      result: [{ id: 1 }, { id: 2 }],
    })
    const transaction = deferring()

    deferEvent(context, transaction)
    transaction.deferredEvents.forEach((emit) => emit())

    expect(context.self.emit.mock.calls.map(([, item]: any[]) => item)).toEqual(
      [{ id: 1 }, { id: 2 }],
    )
  })

  it('leaves an event the service declares itself to the service', () => {
    const context = contextOf({
      event: 'status',
      result: { id: 1 },
      events: ['status'],
    })
    const transaction = deferring()

    deferEvent(context, transaction)

    // Feathers wouldn't emit it either: the service emits its own events
    expect(transaction.deferredEvents).toEqual([])
    expect(context.event).toBeNull()
  })

  it('does nothing without an event, e.g. for find', () => {
    const context = contextOf({ event: null, result: [{ id: 1 }] })
    const transaction = deferring()

    deferEvent(context, transaction)

    expect(transaction.deferredEvents).toEqual([])
    expect(context.event).toBeNull()
  })

  it('lets Feathers emit right away in a transaction that defers nothing', () => {
    const context = contextOf({ event: 'created', result: { id: 1 } })
    const transaction = {} as KyselyAdapterTransaction

    deferEvent(context, transaction)

    expect(context.event).toBe('created')
  })
})
