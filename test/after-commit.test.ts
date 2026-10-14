import { describe, expect, it, vi } from 'vitest'
import { afterCommit } from '../src/index.js'

describe('afterCommit', () => {
  const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

  /** Params of a call in a transaction, as far as `afterCommit` reads them. */
  const inTransaction = (committed: Promise<boolean>) =>
    ({ transaction: { committed } }) as any

  it('runs right away without a transaction', async () => {
    const action = vi.fn()

    await afterCommit({}, action)

    expect(action).toHaveBeenCalledOnce()
  })

  it('waits for the commit, without holding up the caller', async () => {
    let commit!: (isCommitted: boolean) => void
    const committed = new Promise<boolean>((resolve) => (commit = resolve))
    const action = vi.fn()

    await afterCommit(inTransaction(committed), action)
    expect(action).not.toHaveBeenCalled()

    commit(true)
    await tick()
    expect(action).toHaveBeenCalledOnce()
  })

  it('never runs after a rollback', async () => {
    const action = vi.fn()

    await afterCommit(inTransaction(Promise.resolve(false)), action)
    await tick()

    expect(action).not.toHaveBeenCalled()
  })

  it('runs right away in a transaction passed in by hand — its commit is unknown', async () => {
    const action = vi.fn()

    await afterCommit({ transaction: { trx: {} } } as any, action)

    expect(action).toHaveBeenCalledOnce()
  })

  it('without a transaction, a failing action rejects for the caller', async () => {
    const onError = vi.fn()

    await expect(
      afterCommit(
        {},
        () => {
          throw new Error('push failed')
        },
        { onError },
      ),
    ).rejects.toThrow('push failed')
    expect(onError).not.toHaveBeenCalled()
  })

  it('without onError, swallows an action failing after the commit', async () => {
    const unhandled = vi.fn()
    process.on('unhandledRejection', unhandled)

    try {
      await afterCommit(inTransaction(Promise.resolve(true)), () => {
        throw new Error('push failed')
      })
      await tick()
      await tick()

      expect(unhandled).not.toHaveBeenCalled()
    } finally {
      process.off('unhandledRejection', unhandled)
    }
  })

  it('hands a failing action to onError', async () => {
    const onError = vi.fn()
    const error = new Error('push failed')

    await afterCommit(
      inTransaction(Promise.resolve(true)),
      () => {
        throw error
      },
      { onError },
    )
    await tick()

    expect(onError).toHaveBeenCalledWith(error)
  })
})
