import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'

import { KyselyService } from '../src/index.js'
import { afterAll, beforeEach, describe, expect, it } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

interface ProductsTable {
  id: Generated<number>
  sku: string
  name: string
  price: number
  stock?: number
}

interface DB {
  products: ProductsTable
}

type Product = {
  id: number
  sku: string
  name: string
  price: number
  stock: number | null
}

const dialectType = getDialect()

function setup() {
  const db = new Kysely<DB>({
    dialect: dialect(),
  })

  const clean = async () => {
    await db.schema.dropTable('products').ifExists().execute()

    const textType = dialectType === 'mysql' ? 'varchar(255)' : 'text'

    const builder = addPrimaryKey(
      db.schema
        .createTable('products')
        .addColumn('sku', textType, (col) => col.notNull().unique())
        .addColumn('name', textType, (col) => col.notNull())
        .addColumn('price', 'real', (col) => col.notNull())
        .addColumn('stock', 'real'),
      'id',
    )

    await builder.execute()
  }

  const app = feathers<{
    products: KyselyService<Product>
  }>().use(
    'products',
    new KyselyService<Product>({
      Model: db,
      name: 'products',
      multi: true,
      properties: {
        sku: { type: 'string' },
        name: { type: 'string' },
        price: { type: 'number' },
        stock: { type: 'number' },
      },
    }),
  )

  return {
    db,
    clean,
    products: app.service('products'),
    app,
  }
}

const { app, db, clean } = setup()
const service = () => app.service('products')

const noReturn = { kysely: { returning: false } }

describe('params.kysely.returning: false', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  describe('create', () => {
    it('single resolves to undefined but inserts the row', async () => {
      const result = await service().create(
        { sku: 'C-1', name: 'One', price: 10, stock: 1 },
        noReturn,
      )

      expect(result).toBeUndefined()

      const rows = (await service().find({
        query: { sku: 'C-1' },
      })) as Product[]
      expect(rows).toHaveLength(1)
      expect(rows[0]).toMatchObject({ sku: 'C-1', name: 'One', price: 10 })
    })

    it('multi resolves to [] but inserts every row', async () => {
      const result = await service().create(
        [
          { sku: 'C-2', name: 'Two', price: 20, stock: 2 },
          { sku: 'C-3', name: 'Three', price: 30, stock: 3 },
        ],
        noReturn,
      )

      expect(result).toEqual([])

      const all = (await service().find({})) as Product[]
      expect(all).toHaveLength(2)
    })

    it('empty array still resolves to [] without touching the db', async () => {
      const result = await service().create([], noReturn)
      expect(result).toEqual([])
    })

    it('overrides onConflictReturning and still upserts', async () => {
      const initial = await service().create({
        sku: 'C-4',
        name: 'Original',
        price: 10,
        stock: 1,
      })

      const result = await service().create(
        { sku: 'C-4', name: 'Merged', price: 99, stock: 9 },
        {
          kysely: {
            onConflictFields: ['sku'],
            onConflictAction: 'merge',
            onConflictReturning: 'all',
            returning: false,
          },
        },
      )

      expect(result).toBeUndefined()

      const rows = (await service().find({
        query: { sku: 'C-4' },
      })) as Product[]
      expect(rows).toHaveLength(1)
      expect(rows[0]).toMatchObject({
        id: initial.id,
        name: 'Merged',
        price: 99,
      })
    })

    it("emits 'created' with undefined payload", async () => {
      const events: unknown[] = []
      const handler = (product: unknown) => events.push(product)
      service().on('created', handler)

      try {
        const result = await service().create(
          { sku: 'C-5', name: 'Five', price: 5, stock: 1 },
          noReturn,
        )
        expect(result).toBeUndefined()
      } finally {
        service().removeListener('created', handler)
      }

      expect(events).toHaveLength(1)
      expect(events[0]).toBeUndefined()
    })
  })

  describe('patch', () => {
    it('single resolves to undefined but updates the row', async () => {
      const created = await service().create({
        sku: 'P-1',
        name: 'Original',
        price: 10,
        stock: 1,
      })

      const result = await service().patch(
        created.id,
        { name: 'Patched', price: 50 },
        noReturn,
      )

      expect(result).toBeUndefined()

      const row = await service().get(created.id)
      expect(row).toMatchObject({ name: 'Patched', price: 50 })
    })

    it('single throws NotFound when the id does not exist', async () => {
      await expect(
        service().patch(999999, { name: 'Nope' }, noReturn),
      ).rejects.toMatchObject({ name: 'NotFound' })
    })

    it('multi resolves to [] but updates every matching row', async () => {
      await service().create([
        { sku: 'P-2', name: 'A', price: 10, stock: 1 },
        { sku: 'P-3', name: 'B', price: 10, stock: 1 },
      ])

      const result = await service().patch(
        null,
        { price: 42 },
        { ...noReturn, query: { price: 10 } },
      )

      expect(result).toEqual([])

      const all = (await service().find({})) as Product[]
      expect(all.every((r) => r.price === 42)).toBe(true)
    })

    it('single no-op patch (empty data) still throws NotFound for a missing id', async () => {
      await expect(
        service().patch(999999, {}, noReturn),
      ).rejects.toMatchObject({ name: 'NotFound' })
    })

    it("emits 'patched' with undefined payload", async () => {
      const created = await service().create({
        sku: 'P-4',
        name: 'Original',
        price: 10,
        stock: 1,
      })

      const events: unknown[] = []
      const handler = (product: unknown) => events.push(product)
      service().on('patched', handler)

      try {
        await service().patch(created.id, { name: 'Patched' }, noReturn)
      } finally {
        service().removeListener('patched', handler)
      }

      expect(events).toHaveLength(1)
      expect(events[0]).toBeUndefined()
    })
  })

  describe('update', () => {
    it('resolves to undefined but replaces the row', async () => {
      const created = await service().create({
        sku: 'U-1',
        name: 'Original',
        price: 10,
        stock: 5,
      })

      const result = await service().update(
        created.id,
        { sku: 'U-1', name: 'Replaced', price: 99 } as any,
        noReturn,
      )

      expect(result).toBeUndefined()

      const row = await service().get(created.id)
      expect(row).toMatchObject({ name: 'Replaced', price: 99 })
      // stock omitted from the replacement → nulled out
      expect(row.stock).toBeNull()
    })

    it('throws NotFound when the id does not exist', async () => {
      await expect(
        service().update(999999, { sku: 'X', name: 'X', price: 1 } as any, noReturn),
      ).rejects.toMatchObject({ name: 'NotFound' })
    })
  })

  describe('remove', () => {
    it('single resolves to undefined but deletes the row', async () => {
      const created = await service().create({
        sku: 'R-1',
        name: 'One',
        price: 10,
        stock: 1,
      })

      const result = await service().remove(created.id, noReturn)

      expect(result).toBeUndefined()

      const rows = (await service().find({
        query: { sku: 'R-1' },
      })) as Product[]
      expect(rows).toHaveLength(0)
    })

    it('single throws NotFound when the id does not exist', async () => {
      await expect(
        service().remove(999999, noReturn),
      ).rejects.toMatchObject({ name: 'NotFound' })
    })

    it('multi resolves to [] but deletes every matching row', async () => {
      await service().create([
        { sku: 'R-2', name: 'A', price: 5, stock: 1 },
        { sku: 'R-3', name: 'B', price: 5, stock: 1 },
        { sku: 'R-4', name: 'C', price: 99, stock: 1 },
      ])

      const result = await service().remove(null, {
        ...noReturn,
        query: { price: 5 },
      })

      expect(result).toEqual([])

      const all = (await service().find({})) as Product[]
      expect(all).toHaveLength(1)
      expect(all[0].sku).toBe('R-4')
    })

    it("emits 'removed' with undefined payload", async () => {
      const created = await service().create({
        sku: 'R-5',
        name: 'One',
        price: 10,
        stock: 1,
      })

      const events: unknown[] = []
      const handler = (product: unknown) => events.push(product)
      service().on('removed', handler)

      try {
        await service().remove(created.id, noReturn)
      } finally {
        service().removeListener('removed', handler)
      }

      expect(events).toHaveLength(1)
      expect(events[0]).toBeUndefined()
    })
  })
})
