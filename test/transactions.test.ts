import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'
import {
  KyselyService,
  trxStart,
  trxCommit,
  trxRollback,
} from '../src/index.js'
import type { KyselyAdapterTransaction } from '../src/index.js'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
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
})
