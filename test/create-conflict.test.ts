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
  description?: string
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
  description: string | null
}

const dialectType = getDialect()

function setup() {
  const db = new Kysely<DB>({
    dialect: dialect(),
  })

  const clean = async () => {
    await db.schema.dropTable('products').ifExists().execute()

    // Use varchar for MySQL (text can't have unique index without prefix length)
    const textType = dialectType === 'mysql' ? 'varchar(255)' : 'text'

    const builder = addPrimaryKey(
      db.schema
        .createTable('products')
        .addColumn('sku', textType, (col) => col.notNull().unique())
        .addColumn('name', textType, (col) => col.notNull())
        .addColumn('price', 'real', (col) => col.notNull())
        .addColumn('stock', 'real')
        .addColumn('description', textType),
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
        description: { type: 'string' },
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

describe('create with params.kysely conflict handling', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  describe('plain create (no kysely options)', () => {
    it('still inserts a record unchanged', async () => {
      const result = await app.service('products').create({
        sku: 'PLAIN-001',
        name: 'Plain Product',
        price: 9.99,
        stock: 3,
      })

      expect(result).toMatchObject({
        sku: 'PLAIN-001',
        name: 'Plain Product',
        price: 9.99,
        stock: 3,
      })
      expect(result.id).toBeDefined()
    })
  })

  describe('single record', () => {
    it('inserts when there is no conflict', async () => {
      const result = await app
        .service('products')
        .create(
          { sku: 'C-001', name: 'Test Product', price: 99.99, stock: 10 },
          { kysely: { onConflictFields: ['sku'] } },
        )

      expect(result).toMatchObject({
        sku: 'C-001',
        name: 'Test Product',
        price: 99.99,
        stock: 10,
      })
      expect(result.id).toBeDefined()
    })

    it('ignores the conflict and returns the existing row (default action)', async () => {
      const initial = await app.service('products').create({
        sku: 'C-002',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
      })

      const result = await app
        .service('products')
        .create(
          { sku: 'C-002', name: 'Updated Product', price: 75.0, stock: 15 },
          { kysely: { onConflictFields: ['sku'] } },
        )

      expect(result).toMatchObject({
        id: initial.id,
        sku: 'C-002',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
      })
    })

    it('merges all fields on conflict', async () => {
      const initial = await app.service('products').create({
        sku: 'C-003',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
        description: 'Original description',
      })

      const result = await app.service('products').create(
        {
          sku: 'C-003',
          name: 'Updated Product',
          price: 75.0,
          stock: 15,
          description: 'New description',
        },
        { kysely: { onConflictFields: ['sku'], onConflictAction: 'merge' } },
      )

      expect(result).toMatchObject({
        id: initial.id,
        sku: 'C-003',
        name: 'Updated Product',
        price: 75.0,
        stock: 15,
        description: 'New description',
      })
    })

    it('merges only the specified onConflictMergeFields', async () => {
      const initial = await app.service('products').create({
        sku: 'C-004',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
      })

      const result = await app.service('products').create(
        { sku: 'C-004', name: 'Updated Product', price: 75.0, stock: 15 },
        {
          kysely: {
            onConflictFields: ['sku'],
            onConflictAction: 'merge',
            onConflictMergeFields: ['price'],
          },
        },
      )

      expect(result).toMatchObject({
        id: initial.id,
        sku: 'C-004',
        name: 'Original Product', // unchanged
        price: 75.0, // merged
        stock: 5, // unchanged
      })
    })

    it('excludes onConflictExcludeFields from the merge', async () => {
      const initial = await app.service('products').create({
        sku: 'C-005',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
      })

      const result = await app.service('products').create(
        { sku: 'C-005', name: 'Updated Product', price: 75.0, stock: 15 },
        {
          kysely: {
            onConflictFields: ['sku'],
            onConflictAction: 'merge',
            onConflictExcludeFields: ['name'],
          },
        },
      )

      expect(result).toMatchObject({
        id: initial.id,
        sku: 'C-005',
        name: 'Original Product', // excluded from merge
        price: 75.0, // merged
        stock: 15, // merged
      })
    })
  })

  describe('multiple records', () => {
    it('inserts new and ignores existing on a mixed batch', async () => {
      await app.service('products').create({
        sku: 'M-001',
        name: 'Existing',
        price: 10,
        stock: 1,
      })

      const result = (await app.service('products').create(
        [
          { sku: 'M-001', name: 'Existing Updated', price: 99, stock: 9 },
          { sku: 'M-002', name: 'Fresh', price: 20, stock: 2 },
        ],
        { kysely: { onConflictFields: ['sku'], onConflictAction: 'ignore' } },
      )) as Product[]

      expect(result).toHaveLength(2)
      const bySku = Object.fromEntries(result.map((r) => [r.sku, r]))
      // existing returned unchanged
      expect(bySku['M-001']).toMatchObject({ name: 'Existing', price: 10 })
      // fresh inserted
      expect(bySku['M-002']).toMatchObject({ name: 'Fresh', price: 20 })
    })

    it('merges existing on a batch', async () => {
      await app.service('products').create({
        sku: 'M-010',
        name: 'Existing',
        price: 10,
        stock: 1,
      })

      const result = (await app.service('products').create(
        [
          { sku: 'M-010', name: 'Existing Updated', price: 99, stock: 9 },
          { sku: 'M-011', name: 'Fresh', price: 20, stock: 2 },
        ],
        { kysely: { onConflictFields: ['sku'], onConflictAction: 'merge' } },
      )) as Product[]

      expect(result).toHaveLength(2)
      const bySku = Object.fromEntries(result.map((r) => [r.sku, r]))
      expect(bySku['M-010']).toMatchObject({
        name: 'Existing Updated',
        price: 99,
        stock: 9,
      })
      expect(bySku['M-011']).toMatchObject({ name: 'Fresh', price: 20 })
    })
  })

  describe('feathers events (the reason to prefer create over upsert)', () => {
    it('emits "created" when inserting via create + conflict options', async () => {
      const events: Product[] = []
      const handler = (product: Product) => events.push(product)
      app.service('products').on('created', handler)

      try {
        await app
          .service('products')
          .create(
            { sku: 'E-001', name: 'Eventful', price: 5, stock: 1 },
            { kysely: { onConflictFields: ['sku'] } },
          )
      } finally {
        app.service('products').removeListener('created', handler)
      }

      expect(events).toHaveLength(1)
      expect(events[0]).toMatchObject({ sku: 'E-001', name: 'Eventful' })
    })

    it('still emits "created" even when the conflict is ignored', async () => {
      await app.service('products').create({
        sku: 'E-002',
        name: 'Original',
        price: 5,
        stock: 1,
      })

      const events: Product[] = []
      const handler = (product: Product) => events.push(product)
      app.service('products').on('created', handler)

      try {
        const result = await app.service('products').create(
          { sku: 'E-002', name: 'Ignored Update', price: 99, stock: 9 },
          {
            kysely: { onConflictFields: ['sku'], onConflictAction: 'ignore' },
          },
        )
        // returns the pre-existing row...
        expect(result).toMatchObject({ name: 'Original' })
      } finally {
        app.service('products').removeListener('created', handler)
      }

      // ...but the standard pipeline still emits `created` (documented caveat)
      expect(events).toHaveLength(1)
      expect(events[0]).toMatchObject({ sku: 'E-002', name: 'Original' })
    })
  })

  describe('empty array edge case', () => {
    it('create([], { kysely }) resolves to [] without hitting the database', async () => {
      const result = await app
        .service('products')
        .create([], { kysely: { onConflictFields: ['sku'] } })

      expect(result).toEqual([])
    })

    it('deprecated upsert([]) resolves to [] (delegates to create)', async () => {
      // The old `_upsert` had no empty-array guard and would emit an invalid
      // empty multi-INSERT; delegating to `_create` makes this a safe no-op.
      const result = await app
        .service('products')
        .upsert([], { onConflictFields: ['sku'] } as any)

      expect(result).toEqual([])
    })
  })
})
