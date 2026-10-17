import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'

import { KyselyService } from '../src/index.js'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

/**
 * A statement binds at most a dialect-specific number of parameters (Postgres
 * and MySQL 65535, SQLite 32766, Cloudflare D1 100). A multi create binds one
 * per row and column, an `$in` one per element — both have to stay under it.
 */

interface ItemsTable {
  id: Generated<number>
  sku: string
  name: string
  qty: number | null
}

interface DB {
  items: ItemsTable
}

type Item = { id: number; sku: string; name: string; qty: number | null }

const dialectType = getDialect()
const textType = dialectType === 'mysql' ? 'varchar(255)' : 'text'

const db = new Kysely<DB>({ dialect: dialect() })

const clean = async () => {
  await db.schema.dropTable('items').ifExists().execute()
  await addPrimaryKey(
    db.schema
      .createTable('items')
      .addColumn('sku', textType, (col) => col.notNull().unique())
      .addColumn('name', textType, (col) => col.notNull())
      .addColumn('qty', 'integer'),
    'id',
  ).execute()
}

const options = {
  Model: db,
  name: 'items',
  multi: true,
  properties: { id: true, sku: true, name: true, qty: true },
} as const

const app = feathers<{
  items: KyselyService<Item>
  // 3 columns per row → 3 rows per statement
  small: KyselyService<Item>
}>()
  .use('items', new KyselyService<Item>(options))
  .use('small', new KyselyService<Item>({ ...options, maxParameters: 10 }))

const rows = (count: number, offset = 0) =>
  Array.from({ length: count }, (_, i) => ({
    sku: `sku-${i + offset}`,
    name: `item ${i + offset}`,
    qty: i + offset,
  }))

const count = async () =>
  Number(
    (
      await db
        .selectFrom('items')
        .select((eb) => eb.fn.countAll().as('n'))
        .executeTakeFirstOrThrow()
    ).n,
  )

describe('parameter limit', () => {
  beforeEach(clean)
  afterAll(() => db.destroy())

  describe('multi create', () => {
    it('creates more rows than one statement can bind', async () => {
      // 25000 rows x 3 columns = 75000 parameters — over every dialect's limit
      const data = rows(25000)
      const result = await app.service('items').create(data)

      expect(result).toHaveLength(data.length)
      expect(result[0].sku).toBe('sku-0')
      expect(result.at(-1)!.sku).toBe('sku-24999')
      expect(await count()).toBe(data.length)
    })

    it('splits into statements under maxParameters, in input order', async () => {
      const result = await app.service('small').create(rows(10))

      expect(result.map((item) => item.sku)).toEqual(
        rows(10).map((item) => item.sku),
      )
      expect(new Set(result.map((item) => item.id)).size).toBe(10)
    })

    it('is atomic: a failing chunk rolls back the chunks before it', async () => {
      const data: any[] = rows(10)
      data[8] = { sku: 'broken', name: null, qty: 1 } // NOT NULL violation

      await expect(app.service('small').create(data)).rejects.toThrow()
      expect(await count()).toBe(0)
    })

    it('runs within the caller transaction', async () => {
      const trx = await db.startTransaction().execute()

      await app
        .service('small')
        .create(rows(10), { transaction: { trx, starting: false } })
      await trx.rollback().execute()

      expect(await count()).toBe(0)
    })

    it('returns existing rows of ignored conflicts across chunks', async () => {
      await app.service('items').create(rows(3, 4))

      const result = await app.service('small').create(rows(10), {
        kysely: { onConflictFields: ['sku'], onConflictAction: 'ignore' },
      })

      // with conflict handling the order of the returned rows is not defined
      expect(result.map((item) => item.sku).sort()).toEqual(
        rows(10)
          .map((item) => item.sku)
          .sort(),
      )
      expect(await count()).toBe(10)
    })

    it('rejects a duplicate conflict key in different chunks', async () => {
      const data = [...rows(5), ...rows(1)] // sku-0 in chunk 1 and chunk 2

      await expect(
        app.service('small').create(data, {
          kysely: { onConflictFields: ['sku'], onConflictAction: 'merge' },
        }),
      ).rejects.toThrow(/share the conflict key/)
      expect(await count()).toBe(0)
    })

    it('honors returning: false', async () => {
      const result = await app
        .service('small')
        .create(rows(10), { kysely: { returning: false } })

      expect(result).toEqual([])
      expect(await count()).toBe(10)
    })
  })

  describe.runIf(dialectType === 'postgres')('$in on postgres', () => {
    it('binds the list as one array parameter', () => {
      const { sql, parameters } = app
        .service('items')
        .composeQuery(
          { query: { id: { $in: [1, 2, 3] }, sku: { $nin: ['a', 'b'] } } },
          { select: true, where: true },
        )
        .compile()

      expect(sql).toMatch(/"id" = any\(\$1\)/)
      expect(sql).toMatch(/"sku" <> all\(\$2\)/)
      expect(parameters).toEqual([
        [1, 2, 3],
        ['a', 'b'],
      ])
    })

    it('matches with more elements than one statement can bind', async () => {
      await app.service('items').create(rows(3))
      const ids = Array.from({ length: 70000 }, (_, i) => i + 1)

      const result = await app
        .service('items')
        .find({ query: { id: { $in: ids } }, paginate: false })

      expect(result).toHaveLength(3)
    })
  })
})
