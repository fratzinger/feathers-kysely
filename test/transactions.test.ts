import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'
import {
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
  })
})

// These exercise the deferral / suppression / queue mechanics with a stubbed
// ControlledTransaction and plain services, so they run on every dialect
// (including in-memory SQLite) without needing a real cross-connection trx.
describe('withTransaction event deferral (stubbed transaction)', () => {
  function makeTrx(log: string[], label: string) {
    const trx: any = {
      startTransaction: () => ({
        execute: async () => makeTrx(log, label),
      }),
      commit: () => ({
        execute: async () => {
          log.push(`commit:${label}`)
        },
      }),
      rollback: () => ({
        execute: async () => {
          log.push(`rollback:${label}`)
        },
      }),
      savepoint: () => ({
        execute: async () => makeTrx(log, `${label}>sp`),
      }),
      releaseSavepoint: () => ({
        execute: async () => {
          log.push(`commit:${label}`)
        },
      }),
      rollbackToSavepoint: () => ({
        execute: async () => {
          log.push(`rollback:${label}`)
        },
      }),
    }
    return trx
  }

  class StubService {
    log: string[]
    failCreate = false
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
          execute: async () => makeTrx(log, 'root'),
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
})
