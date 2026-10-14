import { EventEmitter } from 'node:events'
import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import type { HookContext, NextFunction } from '@feathersjs/feathers'
import { feathers } from '@feathersjs/feathers'
import { socket } from '@feathersjs/transport-commons'
import dialect, { getDialect } from './dialect.js'
import {
  afterCommit,
  KyselyService,
  trxStart,
  trxCommit,
  trxRollback,
  withTransaction,
} from '../src/index.js'
import type { KyselyAdapterTransaction } from '../src/index.js'
import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

interface AccountsTable {
  id: Generated<number>
  name: string
  balance: number
}

interface DB {
  accounts: AccountsTable
}

type Account = {
  id: number
  name: string
  balance: number
}

const db = new Kysely<DB>({
  dialect: dialect(),
})

const clean = async () => {
  await db.schema.dropTable('accounts').ifExists().execute()

  await addPrimaryKey(
    db.schema
      .createTable('accounts')
      .addColumn('name', 'text', (col) => col.notNull())
      .addColumn('balance', 'real', (col) => col.notNull()),
    'id',
  ).execute()
}

function createApp() {
  const app = feathers<{
    accounts: KyselyService<Account>
  }>().use(
    'accounts',
    new KyselyService<Account>({
      Model: db,
      name: 'accounts',
      multi: true,
    }),
  )

  return app
}

const app = createApp()
const accounts = app.service('accounts')

const dialectName = getDialect()

// SQLite in-memory does not support controlled transactions across connections
describe.skipIf(dialectName === 'sqlite')('transactions', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  describe('manual transaction via params', () => {
    it('committed transaction persists data', async () => {
      const trx = await db.startTransaction().execute()

      const transaction: KyselyAdapterTransaction = {
        trx,
        id: Date.now(),
        starting: false,
      }

      await accounts.create({ name: 'Alice', balance: 100 }, {
        transaction,
      } as any)

      await trx.commit().execute()

      const all = await accounts.find({ paginate: false })
      expect(all).toHaveLength(1)
      expect(all[0].name).toBe('Alice')
      expect(all[0].balance).toBe(100)
    })

    it('rolled back transaction does not persist data', async () => {
      const trx = await db.startTransaction().execute()

      const transaction: KyselyAdapterTransaction = {
        trx,
        id: Date.now(),
        starting: false,
      }

      await accounts.create({ name: 'Bob', balance: 200 }, {
        transaction,
      } as any)

      await trx.rollback().execute()

      const all = await accounts.find({ paginate: false })
      expect(all).toHaveLength(0)
    })

    it('find within a transaction sees uncommitted data', async () => {
      const trx = await db.startTransaction().execute()

      const transaction: KyselyAdapterTransaction = {
        trx,
        id: Date.now(),
        starting: false,
      }

      await accounts.create({ name: 'Charlie', balance: 300 }, {
        transaction,
      } as any)

      const withinTrx = await accounts.find({
        paginate: false,
        transaction,
      } as any)
      expect(withinTrx).toHaveLength(1)
      expect(withinTrx[0].name).toBe('Charlie')

      await trx.rollback().execute()

      const afterRollback = await accounts.find({ paginate: false })
      expect(afterRollback).toHaveLength(0)
    })

    it('patch within a transaction is rolled back', async () => {
      const created = await accounts.create({ name: 'Dave', balance: 500 })

      const trx = await db.startTransaction().execute()

      const transaction: KyselyAdapterTransaction = {
        trx,
        id: Date.now(),
        starting: false,
      }

      await accounts.patch(created.id, { balance: 0 }, { transaction } as any)

      const withinTrx = await accounts.get(created.id, {
        transaction,
      } as any)
      expect(withinTrx.balance).toBe(0)

      await trx.rollback().execute()

      const afterRollback = await accounts.get(created.id)
      expect(afterRollback.balance).toBe(500)
    })

    it('remove within a transaction is rolled back', async () => {
      const created = await accounts.create({ name: 'Eve', balance: 100 })

      const trx = await db.startTransaction().execute()

      const transaction: KyselyAdapterTransaction = {
        trx,
        id: Date.now(),
        starting: false,
      }

      await accounts.remove(created.id, { transaction } as any)

      const withinTrx = await accounts.find({
        paginate: false,
        transaction,
      } as any)
      expect(withinTrx).toHaveLength(0)

      await trx.rollback().execute()

      const afterRollback = await accounts.find({ paginate: false })
      expect(afterRollback).toHaveLength(1)
      expect(afterRollback[0].name).toBe('Eve')
    })
  })

  describe('transaction hooks', () => {
    it('trxStart + trxCommit commits data', async () => {
      const hookApp = createApp()
      const service = hookApp.service('accounts')

      service.hooks({
        before: { create: [trxStart()] },
        after: { create: [trxCommit()] },
        error: { create: [trxRollback()] },
      })

      await service.create({ name: 'Frank', balance: 400 })

      const all = await accounts.find({ paginate: false })
      expect(all).toHaveLength(1)
      expect(all[0].name).toBe('Frank')
    })

    it('trxStart + trxRollback rolls back on error', async () => {
      const hookApp = createApp()
      const service = hookApp.service('accounts')

      service.hooks({
        before: { create: [trxStart()] },
        after: {
          create: [
            async () => {
              throw new Error('Intentional error')
            },
          ],
        },
        error: { create: [trxRollback()] },
      })

      try {
        await service.create({ name: 'Ghost', balance: 999 })
      } catch (e: any) {
        expect(e.message).toBe('Intentional error')
      }

      const all = await accounts.find({ paginate: false })
      expect(all).toHaveLength(0)
    })
  })

  describe('withTransaction around hook', () => {
    type Audit = { id: number; message: string }

    const auditClean = async () => {
      await db.schema.dropTable('audit').ifExists().execute()
      await addPrimaryKey(
        (db as Kysely<any>).schema
          .createTable('audit')
          .addColumn('message', 'text', (col) => col.notNull()),
        'id',
      ).execute()
    }
    beforeEach(auditClean)

    function createCrossApp() {
      return feathers<{
        accounts: KyselyService<Account>
        audit: KyselyService<Audit>
      }>()
        .use(
          'accounts',
          new KyselyService<Account>({
            Model: db,
            name: 'accounts',
            multi: true,
          }),
        )
        .use(
          'audit',
          new KyselyService<Audit>({ Model: db, name: 'audit', multi: true }),
        )
    }

    it('commits data on success', async () => {
      const svc = createApp().service('accounts')
      svc.hooks({ around: { create: [withTransaction()] } })

      await svc.create({ name: 'Wt1', balance: 1 })

      const all = await accounts.find({ paginate: false })
      expect(all).toHaveLength(1)
    })

    it('rolls back on error', async () => {
      const svc = createApp().service('accounts')
      svc.hooks({
        around: { create: [withTransaction()] },
        after: {
          create: [
            () => {
              throw new Error('boom')
            },
          ],
        },
      })

      await expect(svc.create({ name: 'Wt2', balance: 2 })).rejects.toThrow(
        'boom',
      )

      const all = await accounts.find({ paginate: false })
      expect(all).toHaveLength(0)
    })

    it('commits all rows for a multi create', async () => {
      const svc = createApp().service('accounts')
      svc.hooks({ around: { create: [withTransaction()] } })

      await svc.create([
        { name: 'M1', balance: 1 },
        { name: 'M2', balance: 2 },
      ])

      const all = await accounts.find({ paginate: false })
      expect(all).toHaveLength(2)
    })

    it('emits created once, only after commit', async () => {
      const svc = createApp().service('accounts')
      svc.hooks({ around: { create: [withTransaction()] } })

      let count = 0
      let visibleOutsideTrx = -1
      const fired = new Promise<void>((resolve) => {
        svc.on('created', async () => {
          count++
          const rows = await accounts.find({ paginate: false })
          visibleOutsideTrx = rows.length
          resolve()
        })
      })

      await svc.create({ name: 'Evt', balance: 1 })
      await fired

      expect(count).toBe(1)
      // visible from a separate connection => already committed when emitted
      expect(visibleOutsideTrx).toBe(1)
    })

    it('find/get within the transaction see uncommitted rows (mutating-only engagement keeps the trx)', async () => {
      const cross = createCrossApp()
      // app-wide: find/get pass through withTransaction(), create starts a trx
      cross.hooks({ around: [withTransaction()] })
      const a = cross.service('accounts')

      let findWithinTrx = -1
      let getWithinTrx = false
      a.hooks({
        after: {
          create: [
            async (context) => {
              const trx = (context.params as any).transaction
              const rows = await a.find({
                paginate: false,
                transaction: trx,
              } as any)
              findWithinTrx = (rows as Account[]).length
              const one = await a.get((context.result as any).id, {
                transaction: trx,
              } as any)
              getWithinTrx = !!one
              // not visible from a separate connection yet (still open)
              expect(await accounts.find({ paginate: false })).toHaveLength(0)
            },
          ],
        },
      })

      await a.create({ name: 'TrxRead', balance: 1 })

      expect(findWithinTrx).toBe(1)
      expect(getWithinTrx).toBe(true)
      expect(await accounts.find({ paginate: false })).toHaveLength(1)
    })

    it('defers a nested cross-service event until root commit', async () => {
      const cross = createCrossApp()
      const a = cross.service('accounts')
      const b = cross.service('audit')

      a.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              await cross.service('audit').create({ message: 'audited' }, {
                transaction: (context.params as any).transaction,
              } as any)
            },
          ],
        },
      })
      b.hooks({ around: { create: [withTransaction()] } })

      let auditVisibleAtEmit = -1
      const auditCreated = new Promise<void>((resolve) => {
        b.on('created', async () => {
          const rows = await b.find({ paginate: false })
          auditVisibleAtEmit = rows.length
          resolve()
        })
      })

      await a.create({ name: 'Cross', balance: 1 })
      await auditCreated

      expect(auditVisibleAtEmit).toBe(1)
      expect(await accounts.find({ paginate: false })).toHaveLength(1)
      expect(await b.find({ paginate: false })).toHaveLength(1)
    })

    it('discards a nested cross-service event on rollback', async () => {
      const cross = createCrossApp()
      const a = cross.service('accounts')
      const b = cross.service('audit')

      a.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              await cross.service('audit').create({ message: 'audited' }, {
                transaction: (context.params as any).transaction,
              } as any)
            },
          ],
        },
        after: {
          create: [
            () => {
              throw new Error('rollback-it')
            },
          ],
        },
      })
      b.hooks({ around: { create: [withTransaction()] } })

      const spy = vi.fn()
      b.on('created', spy)

      await expect(a.create({ name: 'CrossFail', balance: 1 })).rejects.toThrow(
        'rollback-it',
      )

      expect(spy).not.toHaveBeenCalled()
      expect(await accounts.find({ paginate: false })).toHaveLength(0)
      expect(await b.find({ paginate: false })).toHaveLength(0)
    })

    /**
     * `accounts` creates two audits in parallel within its transaction, in the
     * order of `messages`; the one named `slow` takes a moment. `pending` holds
     * the nested calls, so a test can wait for both to settle.
     */
    function createParallelApp(
      auditHooks: { around: any },
      messages: readonly string[] = ['fast', 'slow'],
    ) {
      const cross = createCrossApp()
      const a = cross.service('accounts')
      const b = cross.service('audit')
      const pending: Promise<unknown>[] = []
      a.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              const params = {
                transaction: (context.params as any).transaction,
              } as any
              pending.push(
                ...messages.map((message) => b.create({ message }, params)),
              )
              await Promise.all(pending)
            },
          ],
        },
      })
      b.hooks({
        ...auditHooks,
        before: {
          create: [
            async (context) => {
              if ((context.data as Audit).message === 'slow') {
                await new Promise((resolve) => setTimeout(resolve, 20))
              }
            },
          ],
        },
      })
      return { a, b, pending }
    }

    it('joins nested calls in parallel: committed with the root, their events after it', async () => {
      const { a, b } = createParallelApp({
        around: { all: [withTransaction({ start: false, nested: 'join' })] },
      })
      const visibleAtEmit: number[] = []
      b.on('created', async () => {
        visibleAtEmit.push((await b.find({ paginate: false })).length)
      })

      await a.create({ name: 'Joined', balance: 1 })

      await vi.waitFor(() => expect(visibleAtEmit).toEqual([2, 2]))
      expect(await accounts.find({ paginate: false })).toHaveLength(1)
    })

    it('a joined call outliving the root it joined writes nothing', async () => {
      const { a, b, pending } = createParallelApp(
        {
          around: { all: [withTransaction({ start: false, nested: 'join' })] },
        },
        ['bad', 'slow'],
      )
      b.hooks({
        after: {
          create: [
            (context) => {
              if ((context.result as Audit).message === 'bad') {
                throw new Error('bad audit')
              }
            },
          ],
        },
      })

      // `bad` fails, so the root rolls back while `slow` is still on its way
      await expect(a.create({ name: 'Joined', balance: 1 })).rejects.toThrow(
        'bad audit',
      )
      const [, slow] = await Promise.allSettled(pending)

      expect(slow).toMatchObject({
        status: 'rejected',
        reason: { message: expect.stringContaining('outlived') },
      })
      expect(await accounts.find({ paginate: false })).toHaveLength(0)
      expect(await b.find({ paginate: false })).toHaveLength(0)
    })

    it('discards joined rows and their events with the root', async () => {
      const { a, b } = createParallelApp({
        around: { all: [withTransaction({ start: false, nested: 'join' })] },
      })
      a.hooks({
        after: {
          create: [
            () => {
              throw new Error('rollback-it')
            },
          ],
        },
      })
      const spy = vi.fn()
      b.on('created', spy)

      await expect(a.create({ name: 'Joined', balance: 1 })).rejects.toThrow(
        'rollback-it',
      )

      expect(spy).not.toHaveBeenCalled()
      expect(await b.find({ paginate: false })).toHaveLength(0)
    })

    it("nested: 'join' app-wide: every mutating call is transactional, nested ones join", async () => {
      const cross = createCrossApp()
      cross.hooks({ around: [withTransaction({ nested: 'join' })] })
      const a = cross.service('accounts')
      const b = cross.service('audit')
      a.hooks({
        before: {
          create: [
            async (context) => {
              const params = {
                transaction: (context.params as any).transaction,
              } as any
              await Promise.all([
                b.create({ message: 'one' }, params),
                b.create({ message: 'two' }, params),
              ])
              // not visible outside before the commit
              expect(await accounts.find({ paginate: false })).toHaveLength(0)
            },
          ],
        },
      })
      const events: string[] = []
      a.on('created', () => events.push('account'))
      b.on('created', (audit: Audit) => events.push(audit.message))

      await a.create({ name: 'AppWide', balance: 1 })
      // a call of its own starts its own transaction
      await b.create({ message: 'alone' })

      expect(await b.find({ paginate: false })).toHaveLength(3)
      expect(events.sort()).toEqual(['account', 'alone', 'one', 'two'])
    })

    it('start: false with savepoints: a nested failure the caller catches is rolled back alone', async () => {
      const cross = createCrossApp()
      const a = cross.service('accounts')
      const b = cross.service('audit')
      a.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              const params = {
                transaction: (context.params as any).transaction,
              } as any
              await b.create({ message: 'bad' }, params).catch(() => {})
              await b.create({ message: 'good' }, params)
            },
          ],
        },
      })
      b.hooks({
        around: { create: [withTransaction({ start: false })] },
        after: {
          create: [
            (context) => {
              if ((context.result as Audit).message === 'bad') {
                throw new Error('bad audit')
              }
            },
          ],
        },
      })
      const spy = vi.fn()
      b.on('created', spy)

      await a.create({ name: 'Savepoint', balance: 1 })

      expect(await accounts.find({ paginate: false })).toHaveLength(1)
      const audits = (await b.find({ paginate: false })) as Audit[]
      expect(audits.map((audit) => audit.message)).toEqual(['good'])
      expect(spy.mock.calls.map(([audit]) => audit.message)).toEqual(['good'])
    })

    it("nested: 'join' undoes nothing on its own: a failure the caller catches keeps what it wrote", async () => {
      const cross = createCrossApp()
      const a = cross.service('accounts')
      const b = cross.service('audit')
      a.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              await b
                .create({ message: 'bad' }, {
                  transaction: (context.params as any).transaction,
                } as any)
                .catch(() => {})
            },
          ],
        },
      })
      b.hooks({
        around: { create: [withTransaction({ nested: 'join' })] },
        after: {
          create: [
            () => {
              throw new Error('bad audit')
            },
          ],
        },
      })

      await a.create({ name: 'Joined', balance: 1 })

      // no savepoint to roll back to: rethrow, so the root rolls back
      expect(await b.find({ paginate: false })).toHaveLength(1)
    })

    it("nested: 'join' in a transaction passed in by hand: its event goes out right away", async () => {
      const b = createCrossApp().service('audit')
      b.hooks({ around: { create: [withTransaction({ nested: 'join' })] } })
      const spy = vi.fn()
      b.on('created', spy)
      const trx = await db.startTransaction().execute()
      const transaction: KyselyAdapterTransaction = {
        trx,
        id: Date.now(),
        starting: false,
      }

      await b.create({ message: 'manual' }, { transaction } as any)

      expect(spy).toHaveBeenCalledTimes(1)
      expect(await b.find({ paginate: false })).toHaveLength(0)
      await trx.commit().execute()
      expect(await b.find({ paginate: false })).toHaveLength(1)
    })

    it('withTransaction() in a transaction passed in by hand: a savepoint of its own, its event right away', async () => {
      const b = createCrossApp().service('audit')
      b.hooks({
        around: { create: [withTransaction()] },
        after: {
          create: [
            (context) => {
              if ((context.result as Audit).message === 'bad') {
                throw new Error('bad audit')
              }
            },
          ],
        },
      })
      const spy = vi.fn()
      b.on('created', spy)
      const trx = await db.startTransaction().execute()
      const transaction: KyselyAdapterTransaction = {
        trx,
        id: Date.now(),
        starting: false,
      }

      await b.create({ message: 'good' }, { transaction } as any)
      await expect(
        b.create({ message: 'bad' }, { transaction } as any),
      ).rejects.toThrow('bad audit')

      // no root of its own whose commit it could wait for
      expect(spy.mock.calls.map(([audit]) => audit.message)).toEqual(['good'])
      expect(await b.find({ paginate: false })).toHaveLength(0)
      await trx.commit().execute()
      const audits = (await b.find({ paginate: false })) as Audit[]
      expect(audits.map((audit) => audit.message)).toEqual(['good'])
    })

    it('drops the events of calls within a savepoint rolled back, though the root commits', async () => {
      const cross = createCrossApp()
      const a = cross.service('accounts')
      const b = cross.service('audit')
      a.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              await b
                .create({ message: 'outer' }, {
                  transaction: (context.params as any).transaction,
                } as any)
                .catch(() => {})
            },
          ],
        },
      })
      b.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              if ((context.data as Audit).message === 'outer') {
                await b.create({ message: 'inner' }, {
                  transaction: (context.params as any).transaction,
                } as any)
              }
            },
          ],
        },
        after: {
          create: [
            (context) => {
              if ((context.result as Audit).message === 'outer') {
                throw new Error('outer fails after inner')
              }
            },
          ],
        },
      })
      const spy = vi.fn()
      b.on('created', spy)

      await a.create({ name: 'Survives', balance: 1 })

      expect(await accounts.find({ paginate: false })).toHaveLength(1)
      expect(await b.find({ paginate: false })).toHaveLength(0)
      expect(spy).not.toHaveBeenCalled()
    })

    it.each([
      ['the first one started', ['fast', 'slow']],
      ['the last one started', ['slow', 'fast']],
    ])(
      'reports nested withTransaction() calls run in parallel, and rolls back — %s finishing first',
      async (_, messages) => {
        const { a, b, pending } = createParallelApp(
          { around: { create: [withTransaction()] } },
          messages,
        )

        await expect(
          a.create({ name: 'Parallel', balance: 1 }),
        ).rejects.toThrow('nested calls ran in parallel')
        await Promise.allSettled(pending)

        expect(await accounts.find({ paginate: false })).toHaveLength(0)
        expect(await b.find({ paginate: false })).toHaveLength(0)
      },
    )

    it('reports nested calls run in parallel also when the caller settles them, the database intact', async () => {
      const cross = createCrossApp()
      const a = cross.service('accounts')
      const b = cross.service('audit')
      const failures: string[] = []
      a.hooks({
        around: { create: [withTransaction()] },
        before: {
          create: [
            async (context) => {
              const params = {
                transaction: (context.params as any).transaction,
              } as any
              const settled = await Promise.allSettled([
                b.create({ message: 'bad' }, params),
                b.create({ message: 'good' }, params),
              ])
              for (const result of settled) {
                if (result.status === 'rejected') {
                  failures.push(result.reason.message)
                }
              }
            },
          ],
        },
      })
      b.hooks({
        around: { create: [withTransaction()] },
        after: {
          create: [
            (context) => {
              if ((context.result as Audit).message === 'bad') {
                throw new Error('bad audit')
              }
            },
          ],
        },
      })

      await a.create({ name: 'Settled', balance: 1 })

      expect(failures).toEqual([
        'bad audit',
        expect.stringContaining('nested calls ran in parallel'),
      ])
      expect(await accounts.find({ paginate: false })).toHaveLength(1)
      expect(await b.find({ paginate: false })).toHaveLength(0)
    })

    describe('afterCommit()', () => {
      const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

      it('runs once the root committed, and sees its rows', async () => {
        const b = createCrossApp().service('audit')
        const visible: number[] = []
        b.hooks({
          around: { create: [withTransaction()] },
          after: {
            create: [
              async (context) => {
                await afterCommit(context.params, async () => {
                  visible.push((await b.find({ paginate: false })).length)
                })
                // still within the transaction
                expect(visible).toEqual([])
              },
            ],
          },
        })

        await b.create({ message: 'one' })

        await vi.waitFor(() => expect(visible).toEqual([1]))
      })

      it('never runs when the root rolls back', async () => {
        const b = createCrossApp().service('audit')
        const action = vi.fn()
        b.hooks({
          around: { create: [withTransaction()] },
          after: {
            create: [
              async (context) => {
                await afterCommit(context.params, action)
                throw new Error('rollback-it')
              },
            ],
          },
        })

        await expect(b.create({ message: 'one' })).rejects.toThrow(
          'rollback-it',
        )
        await tick()

        expect(action).not.toHaveBeenCalled()
      })

      it('never runs within a savepoint rolled back, though the root commits', async () => {
        const cross = createCrossApp()
        const a = cross.service('accounts')
        const b = cross.service('audit')
        const action = vi.fn()
        a.hooks({
          around: { create: [withTransaction()] },
          before: {
            create: [
              async (context) => {
                await b
                  .create({ message: 'nested' }, {
                    transaction: (context.params as any).transaction,
                  } as any)
                  .catch(() => {})
              },
            ],
          },
        })
        b.hooks({
          around: { create: [withTransaction()] },
          after: {
            create: [
              async (context) => {
                await afterCommit(context.params, action)
                throw new Error('nested fails')
              },
            ],
          },
        })

        await a.create({ name: 'Survives', balance: 1 })
        await tick()

        expect(await accounts.find({ paginate: false })).toHaveLength(1)
        expect(await b.find({ paginate: false })).toHaveLength(0)
        expect(action).not.toHaveBeenCalled()
      })
    })

    describe('events arrive as they would without a transaction', () => {
      /**
       * Like `resolveResult` + `resolveExternal` of @feathersjs/schema: async
       * work once the method returned, and a `dispatch` for the clients.
       */
      const resolve = async (context: HookContext, next: NextFunction) => {
        await next()
        await new Promise((resolve) => setTimeout(resolve, 5))
        const resolveAudit = (audit: Audit) => ({ ...audit, resolved: true })
        const external = ({ message }: Audit) => ({ message, external: true })
        context.result = Array.isArray(context.result)
          ? context.result.map(resolveAudit)
          : resolveAudit(context.result)
        context.dispatch = Array.isArray(context.result)
          ? context.result.map(external)
          : external(context.result)
      }

      /**
       * Creates `data` in `audit` — by itself, or within a call of `accounts` —
       * and records what an `audit` listener and a socket client get. Ids are
       * left out: they differ between runs.
       */
      async function record(
        data: Partial<Audit> | Partial<Audit>[],
        options: { nested: boolean; transactional: boolean },
      ) {
        const seen: unknown[] = []
        const received: unknown[] = []
        const app = feathers<any>()
          .configure(
            socket({
              done: Promise.resolve(new EventEmitter()),
              emit: 'emit',
              socketMap: new WeakMap(),
              socketKey: 'socket',
              getParams: () => ({}),
            }),
          )
          .use(
            'accounts',
            new KyselyService<Account>({ Model: db, name: 'accounts' }),
          )
          .use(
            'audit',
            new KyselyService<Audit>({ Model: db, name: 'audit', multi: true }),
          )
        const client = {
          socket: {
            id: 'client',
            emit: (event: string, payload: unknown) => {
              if (event === 'audit created') received.push(payload)
            },
          },
        }
        app.channel('everyone').join(client as any)
        app.publish(() => app.channel('everyone'))
        app
          .service('audit')
          .on('created', ({ id: _id, ...audit }: Audit, context: HookContext) =>
            seen.push({ audit, dispatch: context.dispatch }),
          )

        const transaction = options.transactional ? [withTransaction()] : []
        app
          .service('audit')
          .hooks({ around: { all: [resolve], create: transaction } })

        if (options.nested) {
          app.service('accounts').hooks({
            around: { create: transaction },
            before: {
              create: [
                async (context: HookContext) => {
                  await app.service('audit').create(
                    data as any,
                    {
                      transaction: (context.params as any).transaction,
                    } as any,
                  )
                },
              ],
            },
          })
          await app.service('accounts').create({ name: 'Root', balance: 1 })
        } else {
          await app.service('audit').create(data)
        }

        const count = Array.isArray(data) ? data.length : 1
        await vi.waitFor(() => expect(received).toHaveLength(count))
        return { seen, received }
      }

      it.each([
        ['the call starting the transaction', false, { message: 'one' }],
        [
          'the call starting the transaction, multi',
          false,
          [{ message: 'one' }, { message: 'two' }],
        ],
        ['a nested call', true, { message: 'one' }],
        [
          'a nested call, multi',
          true,
          [{ message: 'one' }, { message: 'two' }],
        ],
      ])('%s', async (_, nested, data) => {
        const expected = await record(data, { nested, transactional: false })
        // the resolver ran before the event, as Feathers emits after every hook
        expect(expected.seen).toEqual(
          (Array.isArray(data) ? data : [data]).map(() => ({
            audit: expect.objectContaining({ resolved: true }),
            dispatch: expect.anything(),
          })),
        )

        expect(await record(data, { nested, transactional: true })).toEqual(
          expected,
        )
      })
    })
  })
})

// These exercise the deferral / suppression / queue mechanics with a stubbed
// ControlledTransaction and plain services, so they run on every dialect
// (including in-memory SQLite) without needing a real cross-connection trx.
describe('withTransaction event deferral (stubbed transaction)', () => {
  /** `savepoint` fails only the first SAVEPOINT. */
  type Failures = { commit?: boolean; rollback?: boolean; savepoint?: boolean }

  function makeTrx(log: string[], label: string, fail: Failures = {}) {
    const trx: any = {
      startTransaction: () => ({
        execute: async () => makeTrx(log, label),
      }),
      commit: () => ({
        execute: async () => {
          log.push(`commit:${label}`)
          if (fail.commit) throw new Error('commit failed')
        },
      }),
      rollback: () => ({
        execute: async () => {
          log.push(`rollback:${label}`)
          if (fail.rollback) throw new Error('rollback failed')
        },
      }),
      withPlugin: () => trx,
      savepoint: () => ({
        execute: async () => {
          if (fail.savepoint) {
            fail.savepoint = false
            throw new Error('savepoint failed')
          }
          return makeTrx(log, `${label}>sp`)
        },
      }),
      // nested calls issue their savepoint statements on the root's instance
      releaseSavepoint: () => ({
        execute: async () => {
          log.push(`commit:${label}>sp`)
        },
      }),
      rollbackToSavepoint: () => ({
        execute: async () => {
          log.push(`rollback:${label}>sp`)
        },
      }),
    }
    return trx
  }

  class StubService {
    log: string[]
    failCreate = false
    /** Statements of its root transaction that fail. */
    fail: Failures = {}
    withModel: boolean
    onCreate?: (data: any, params: any) => Promise<void>

    constructor(log: string[], withModel = true) {
      this.log = log
      this.withModel = withModel
    }

    getModel() {
      if (!this.withModel) return undefined
      const log = this.log
      return {
        startTransaction: () => ({
          execute: async () => makeTrx(log, 'root', this.fail),
        }),
      }
    }

    async find() {
      return []
    }

    async create(data: any, params: any) {
      this.log.push('create')
      if (this.onCreate) await this.onCreate(data, params)
      if (this.failCreate) throw new Error('create failed')
      return { id: 1, ...data }
    }
  }

  function appWith(...services: [string, any][]) {
    const app = feathers()
    for (const [name, svc] of services) app.use(name, svc)
    return app
  }

  it('passes non-mutating methods through untouched', async () => {
    const log: string[] = []
    const app = appWith(['s', new StubService(log)])
    const s = app.service('s')
    s.hooks({
      around: { find: [withTransaction()], create: [withTransaction()] },
    })

    await s.find()

    expect(log).toEqual([]) // no transaction started for find
  })

  it('passes through and still emits when no Kysely db is available', async () => {
    const log: string[] = []
    const app = appWith(['s', new StubService(log, /* withModel */ false)])
    const s = app.service('s')
    s.hooks({ around: { create: [withTransaction()] } })

    const events: string[] = []
    s.on('created', () => events.push('created'))

    await s.create({ a: 1 })

    expect(log).toEqual(['create']) // no commit/rollback
    expect(events).toEqual(['created']) // normal (non-deferred) emission
  })

  it('commits and emits the event only after commit', async () => {
    const log: string[] = []
    const app = appWith(['s', new StubService(log)])
    const s = app.service('s')
    s.hooks({ around: { create: [withTransaction()] } })

    s.on('created', () => log.push('event'))

    await s.create({ a: 1 })

    expect(log).toEqual(['create', 'commit:root', 'event'])
  })

  it('a throwing listener of the root call rejects it as in Feathers, the transaction committed', async () => {
    const log: string[] = []
    const app = appWith(['s', new StubService(log)])
    const s = app.service('s')
    s.hooks({ around: { create: [withTransaction()] } })

    s.on('created', () => {
      log.push('event')
      throw new Error('listener boom')
    })

    // Feathers itself emits the root call's event, once the commit is done
    await expect(s.create({ a: 1 })).rejects.toThrow('listener boom')

    expect(log).toEqual(['create', 'commit:root', 'event'])
  })

  it('a throwing listener of a nested call neither rejects nor rolls back, nor keeps other events', async () => {
    const log: string[] = []
    const a = new StubService(log)
    // a multi create
    const b = Object.assign(new StubService(log), {
      async create(data: any[]) {
        log.push('create')
        return data.map((item, index) => ({ id: index + 1, ...item }))
      },
    })
    const app = appWith(['a', a], ['b', b])
    a.onCreate = async (_data, params) => {
      await (app.service('b') as any).create([{ x: 1 }, { x: 2 }], {
        transaction: params.transaction,
      })
    }
    app.service('a').hooks({ around: { create: [withTransaction()] } })
    app.service('b').hooks({ around: { create: [withTransaction()] } })
    app.service('b').on('created', (item: { x: number }) => {
      log.push(`event:b${item.x}`)
      throw new Error('listener boom')
    })
    app.service('a').on('created', () => log.push('event:a'))

    await expect(app.service('a').create({ a: 1 })).resolves.toMatchObject({
      id: 1,
    })

    expect(log).toEqual([
      'create',
      'create',
      'commit:root>sp',
      'commit:root',
      'event:b1',
      'event:b2',
      'event:a',
    ])
  })

  it('rolls back and emits nothing when an after hook throws', async () => {
    const log: string[] = []
    const app = appWith(['s', new StubService(log)])
    const s = app.service('s')
    s.hooks({
      around: { create: [withTransaction()] },
      after: {
        create: [
          () => {
            throw new Error('after-fail')
          },
        ],
      },
    })

    const spy = vi.fn()
    s.on('created', spy)

    await expect(s.create({ a: 1 })).rejects.toThrow('after-fail')

    expect(log).toEqual(['create', 'rollback:root'])
    expect(spy).not.toHaveBeenCalled()
  })

  it('rolls back when the method itself throws', async () => {
    const log: string[] = []
    const stub = new StubService(log)
    stub.failCreate = true
    const app = appWith(['s', stub])
    const s = app.service('s')
    s.hooks({ around: { create: [withTransaction()] } })

    const spy = vi.fn()
    s.on('created', spy)

    await expect(s.create({ a: 1 })).rejects.toThrow('create failed')

    expect(log).toEqual(['create', 'rollback:root'])
    expect(spy).not.toHaveBeenCalled()
  })

  it('defers nested cross-service events until the root commits', async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const app = appWith(['a', a], ['b', b])
    const aSvc = app.service('a')
    const bSvc = app.service('b')

    a.onCreate = async (_data, params) => {
      await (app.service('b') as any).create(
        { x: 1 },
        { transaction: params.transaction },
      )
    }

    aSvc.hooks({ around: { create: [withTransaction()] } })
    bSvc.hooks({ around: { create: [withTransaction()] } })

    aSvc.on('created', () => log.push('event:a'))
    bSvc.on('created', () => log.push('event:b'))

    await aSvc.create({ a: 1 })

    // b runs first (savepoint), a commits root, then queue flushes b then a
    expect(log).toEqual([
      'create', // a.create begins, calls b
      'create', // b.create
      'commit:root>sp', // b's savepoint commit
      'commit:root', // a's root commit
      'event:b', // flushed in push order (b captured first)
      'event:a',
    ])
  })

  it('discards nested cross-service events when the root rolls back', async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const app = appWith(['a', a], ['b', b])
    const aSvc = app.service('a')
    const bSvc = app.service('b')

    a.onCreate = async (_data, params) => {
      await (app.service('b') as any).create(
        { x: 1 },
        { transaction: params.transaction },
      )
    }

    aSvc.hooks({
      around: { create: [withTransaction()] },
      after: {
        create: [
          () => {
            throw new Error('nope')
          },
        ],
      },
    })
    bSvc.hooks({ around: { create: [withTransaction()] } })

    const spyA = vi.fn()
    const spyB = vi.fn()
    aSvc.on('created', spyA)
    bSvc.on('created', spyB)

    await expect(aSvc.create({ a: 1 })).rejects.toThrow('nope')

    expect(spyA).not.toHaveBeenCalled()
    expect(spyB).not.toHaveBeenCalled()
    expect(log).toEqual([
      'create',
      'create',
      'commit:root>sp', // b's savepoint committed
      'rollback:root', // a rolls back the root => everything discarded
    ])
  })

  it('handles three levels of nesting (a -> b -> c)', async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const c = new StubService(log)
    const app = appWith(['a', a], ['b', b], ['c', c])
    const aSvc = app.service('a')
    const bSvc = app.service('b')
    const cSvc = app.service('c')

    a.onCreate = async (_d, params) => {
      await (app.service('b') as any).create(
        { n: 1 },
        { transaction: params.transaction },
      )
    }
    b.onCreate = async (_d, params) => {
      await (app.service('c') as any).create(
        { n: 1 },
        { transaction: params.transaction },
      )
    }

    aSvc.hooks({ around: { create: [withTransaction()] } })
    bSvc.hooks({ around: { create: [withTransaction()] } })
    cSvc.hooks({ around: { create: [withTransaction()] } })

    aSvc.on('created', () => log.push('event:a'))
    bSvc.on('created', () => log.push('event:b'))
    cSvc.on('created', () => log.push('event:c'))

    await aSvc.create({})

    // only the root (a) flushes; innermost captured first
    expect(log.slice(-3)).toEqual(['event:c', 'event:b', 'event:a'])
    expect(log).toContain('commit:root')
    expect(log.indexOf('commit:root')).toBeLessThan(log.indexOf('event:c'))
  })

  it('handles recursive same-service calls within the transaction', async () => {
    const log: string[] = []
    const stub = new StubService(log)
    const app = appWith(['s', stub])
    const s = app.service('s')

    stub.onCreate = async (data, params) => {
      if (data?.depth) return
      await (app.service('s') as any).create(
        { depth: 1 },
        { transaction: params.transaction },
      )
    }

    s.hooks({ around: { create: [withTransaction()] } })

    const spy = vi.fn()
    s.on('created', spy)

    await s.create({ depth: 0 })

    expect(spy).toHaveBeenCalledTimes(2)
    // both emitted after the root commit
    expect(log.filter((l) => l === 'commit:root')).toHaveLength(1)
  })

  it("nested: 'join' runs a nested call in the root transaction, its event after the commit", async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const app = appWith(['a', a], ['b', b])
    const aSvc = app.service('a')
    const bSvc = app.service('b')

    let rootTransaction: unknown
    let joinedTransaction: unknown
    a.onCreate = async (_data, params) => {
      rootTransaction = params.transaction
      await (app.service('b') as any).create(
        { x: 1 },
        { transaction: params.transaction },
      )
    }
    b.onCreate = async (_data, params) => {
      joinedTransaction = params.transaction
    }

    aSvc.hooks({ around: { create: [withTransaction()] } })
    bSvc.hooks({
      around: { create: [withTransaction({ start: false, nested: 'join' })] },
    })
    aSvc.on('created', () => log.push('event:a'))
    bSvc.on('created', () => log.push('event:b'))

    await aSvc.create({ a: 1 })

    // no savepoint of its own: one commit, of the root
    expect(joinedTransaction).toBe(rootTransaction)
    expect(log).toEqual([
      'create',
      'create',
      'commit:root',
      'event:b',
      'event:a',
    ])
  })

  it("nested: 'join' discards the nested call's event when the root rolls back", async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const app = appWith(['a', a], ['b', b])
    const aSvc = app.service('a')
    const bSvc = app.service('b')

    a.onCreate = async (_data, params) => {
      await (app.service('b') as any).create(
        { x: 1 },
        { transaction: params.transaction },
      )
    }
    a.failCreate = true

    aSvc.hooks({ around: { create: [withTransaction()] } })
    bSvc.hooks({
      around: { create: [withTransaction({ start: false, nested: 'join' })] },
    })
    const spy = vi.fn()
    bSvc.on('created', spy)

    await expect(aSvc.create({ a: 1 })).rejects.toThrow('create failed')

    expect(spy).not.toHaveBeenCalled()
    expect(log).toEqual(['create', 'create', 'rollback:root'])
  })

  it("nested: 'join' joins a parent also when it may start one", async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const app = appWith(['a', a], ['b', b])

    let rootTransaction: unknown
    let joinedTransaction: unknown
    a.onCreate = async (_data, params) => {
      rootTransaction = params.transaction
      await (app.service('b') as any).create(
        { x: 1 },
        { transaction: params.transaction },
      )
    }
    b.onCreate = async (_data, params) => {
      joinedTransaction = params.transaction
    }
    app.service('a').hooks({ around: { create: [withTransaction()] } })
    app
      .service('b')
      .hooks({ around: { create: [withTransaction({ nested: 'join' })] } })

    await app.service('a').create({ a: 1 })

    expect(joinedTransaction).toBe(rootTransaction)
    expect(log).toEqual(['create', 'create', 'commit:root'])
  })

  it('drops the events queued within a savepoint rolled back, the root committing', async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const c = new StubService(log)
    const app = appWith(['a', a], ['b', b], ['c', c])

    a.onCreate = async (_data, params) => {
      await (app.service('b') as any)
        .create({ x: 1 }, { transaction: params.transaction })
        .catch(() => {})
    }
    b.onCreate = async (_data, params) => {
      await (app.service('c') as any).create(
        { x: 1 },
        { transaction: params.transaction },
      )
    }
    b.failCreate = true

    for (const name of ['a', 'b', 'c']) {
      app.service(name).hooks({ around: { create: [withTransaction()] } })
      app.service(name).on('created', () => log.push(`event:${name}`))
    }

    await app.service('a').create({ a: 1 })

    // c's savepoint was released, but rolled back with b's
    expect(log.filter((entry) => entry.startsWith('event:'))).toEqual([
      'event:a',
    ])
  })

  it('start: false runs a call without a transaction as usual', async () => {
    const log: string[] = []
    const app = appWith(['b', new StubService(log)])
    const bSvc = app.service('b')
    bSvc.hooks({ around: { create: [withTransaction({ start: false })] } })
    bSvc.on('created', () => log.push('event:b'))

    await bSvc.create({ x: 1 })

    expect(log).toEqual(['create', 'event:b'])
  })

  it("nested: 'join' still starts a transaction for a call without one", async () => {
    const log: string[] = []
    const app = appWith(['b', new StubService(log)])
    const bSvc = app.service('b')
    bSvc.hooks({ around: { create: [withTransaction({ nested: 'join' })] } })
    bSvc.on('created', () => log.push('event:b'))

    await bSvc.create({ x: 1 })

    expect(log).toEqual(['create', 'commit:root', 'event:b'])
  })

  it('a failing COMMIT rejects the call, rolls back and emits nothing', async () => {
    const log: string[] = []
    const s = new StubService(log)
    s.fail.commit = true
    let committed: Promise<unknown> | undefined
    s.onCreate = async (_data, params) => {
      committed = params.transaction.committed
    }
    const app = appWith(['s', s])
    app.service('s').hooks({ around: { create: [withTransaction()] } })
    app.service('s').on('created', () => log.push('event'))

    await expect(app.service('s').create({ x: 1 })).rejects.toThrow(
      'commit failed',
    )

    expect(await committed).toBe(false)
    expect(log).toEqual(['create', 'commit:root', 'rollback:root'])
  })

  it('a failing ROLLBACK still rejects with the error that caused it', async () => {
    const log: string[] = []
    const s = new StubService(log)
    s.failCreate = true
    s.fail.rollback = true
    let committed: Promise<unknown> | undefined
    s.onCreate = async (_data, params) => {
      committed = params.transaction.committed
    }
    const app = appWith(['s', s])
    app.service('s').hooks({ around: { create: [withTransaction()] } })

    await expect(app.service('s').create({ x: 1 })).rejects.toThrow(
      'create failed',
    )

    expect(await committed).toBe(false)
    expect(log).toEqual(['create', 'rollback:root'])
  })

  it('a failing SAVEPOINT rejects the nested call and leaves no savepoint behind', async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    a.fail.savepoint = true
    const app = appWith(['a', a], ['b', b])
    let failure: unknown
    a.onCreate = async (_data, params) => {
      const nested = { transaction: params.transaction }
      failure = await (app.service('b') as any)
        .create({ x: 1 }, nested)
        .catch((error: Error) => error.message)
      // would be "in parallel" with the failed one's savepoint still listed
      await (app.service('b') as any).create({ x: 2 }, nested)
    }
    app.service('a').hooks({ around: { create: [withTransaction()] } })
    app.service('b').hooks({ around: { create: [withTransaction()] } })

    await app.service('a').create({ a: 1 })

    expect(failure).toBe('savepoint failed')
    expect(log).toEqual(['create', 'create', 'commit:root>sp', 'commit:root'])
  })

  it('a nested call still running at the commit rolls the root back, then is refused', async () => {
    const log: string[] = []
    const a = new StubService(log)
    const b = new StubService(log)
    const app = appWith(['a', a], ['b', b])
    let straggler: Promise<unknown> | undefined
    let finishB!: () => void
    b.onCreate = () =>
      new Promise<void>((resolve) => {
        finishB = resolve
      })
    a.onCreate = async (_data, params) => {
      // not awaited
      straggler = (app.service('b') as any).create(
        { x: 1 },
        { transaction: params.transaction },
      )
      // b opened its savepoint and waits
      await new Promise((resolve) => setTimeout(resolve, 0))
    }
    app.service('a').hooks({ around: { create: [withTransaction()] } })
    app.service('b').hooks({ around: { create: [withTransaction()] } })

    await expect(app.service('a').create({ a: 1 })).rejects.toThrow(
      'still running',
    )
    finishB()
    await expect(straggler).rejects.toThrow('outlived its transaction')

    // neither released nor rolled back on the ended transaction
    expect(log).toEqual(['create', 'create', 'rollback:root'])
  })

  it('legacy hooks pass through without a Kysely db or a transaction', async () => {
    const log: string[] = []
    const s = new StubService(log, false)
    const app = appWith(['s', s])
    app.service('s').hooks({
      before: { create: [trxStart()] },
      after: { create: [trxCommit()] },
      error: { create: [trxRollback()] },
    })

    await app.service('s').create({ x: 1 })
    s.failCreate = true
    await expect(app.service('s').create({ x: 2 })).rejects.toThrow(
      'create failed',
    )

    expect(log).toEqual(['create', 'create'])
  })

  describe('committed of a nested call', () => {
    /** a → b, both with savepoints; returns what each call's `committed` resolved to. */
    async function run(options: { failA?: boolean; failB?: boolean }) {
      const log: string[] = []
      const a = new StubService(log)
      const b = new StubService(log)
      const app = appWith(['a', a], ['b', b])
      let committedA: Promise<unknown> | undefined
      let committedB: Promise<unknown> | undefined

      a.onCreate = async (_data, params) => {
        committedA = params.transaction.committed
        await (app.service('b') as any)
          .create({ x: 1 }, { transaction: params.transaction })
          .catch(() => {})
      }
      b.onCreate = async (_data, params) => {
        committedB = params.transaction.committed
      }
      a.failCreate = !!options.failA
      b.failCreate = !!options.failB
      app.service('a').hooks({ around: { create: [withTransaction()] } })
      app.service('b').hooks({ around: { create: [withTransaction()] } })

      await app
        .service('a')
        .create({ a: 1 })
        .catch(() => {})

      return { a: await committedA, b: await committedB }
    }

    it('resolves true once its savepoint is released and the root committed', async () => {
      expect(await run({})).toEqual({ a: true, b: true })
    })

    it('resolves false when its savepoint rolls back, though the root commits', async () => {
      expect(await run({ failB: true })).toEqual({ a: true, b: false })
    })

    it('resolves false when the root rolls back after its savepoint was released', async () => {
      expect(await run({ failA: true })).toEqual({ a: false, b: false })
    })
  })
})
