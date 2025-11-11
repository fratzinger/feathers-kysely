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

function setup() {
  const db = new Kysely<DB>({
    dialect: dialect(),
  })

  const clean = async () => {
    // drop and recreate the products table
    await db.schema.dropTable('products').ifExists().execute()

    // Use varchar for MySQL (text can't have unique index without prefix length)
    // Use text for other databases
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
const dialectType = getDialect()

describe('upsert', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  describe('basic upsert functionality', () => {
    it('should insert a new record when no conflict exists', async () => {
      const product = {
        sku: 'PROD-001',
        name: 'Test Product',
        price: 99.99,
        stock: 10,
      }

      const result = await app.service('products').upsert(product, {
        onConflictFields: ['sku'],
      } as any)

      expect(result).toMatchObject({
        sku: 'PROD-001',
        name: 'Test Product',
        price: 99.99,
        stock: 10,
      })
      expect(result.id).toBeDefined()
    })

    it('should ignore conflict when onConflictAction is ignore', async () => {
      // Create initial product
      const initial = await app.service('products').create({
        sku: 'PROD-002',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
      })

      // Try to upsert with same SKU but different data
      const result = await app.service('products').upsert(
        {
          sku: 'PROD-002',
          name: 'Updated Product',
          price: 75.0,
          stock: 15,
        },
        {
          onConflictFields: ['sku'],
          onConflictAction: 'ignore',
        } as any,
      )

      // Should return the original product unchanged
      expect(result).toMatchObject({
        id: initial.id,
        sku: 'PROD-002',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
      })
    })

    it('should merge all fields on conflict when onConflictAction is merge', async () => {
      // Create initial product
      const initial = await app.service('products').create({
        sku: 'PROD-003',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
        description: 'Original description',
      })

      // Upsert with same SKU but different data
      const result = await app.service('products').upsert(
        {
          sku: 'PROD-003',
          name: 'Updated Product',
          price: 75.0,
          stock: 15,
          description: 'Updated description',
        },
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any,
      )

      // Should return the updated product
      expect(result).toMatchObject({
        id: initial.id,
        sku: 'PROD-003',
        name: 'Updated Product',
        price: 75.0,
        stock: 15,
        description: 'Updated description',
      })
    })

    it('should merge only specified fields with onConflictMergeFields', async () => {
      // Create initial product
      const initial = await app.service('products').create({
        sku: 'PROD-004',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
        description: 'Original description',
      })

      // Upsert with same SKU but only update price and stock
      const result = await app.service('products').upsert(
        {
          sku: 'PROD-004',
          name: 'Updated Product',
          price: 75.0,
          stock: 15,
          description: 'Updated description',
        },
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
          onConflictMergeFields: ['price', 'stock'],
        },
      )

      // Should only update price and stock, keeping original name and description
      expect(result).toMatchObject({
        id: initial.id,
        sku: 'PROD-004',
        name: 'Original Product', // unchanged
        price: 75.0, // updated
        stock: 15, // updated
        description: 'Original description', // unchanged
      })
    })

    it('should exclude specified fields with onConflictExcludeFields', async () => {
      // Create initial product
      const initial = await app.service('products').create({
        sku: 'PROD-005',
        name: 'Original Product',
        price: 50.0,
        stock: 5,
        description: 'Original description',
      })

      // Upsert with same SKU, excluding description from merge
      const result = await app.service('products').upsert(
        {
          sku: 'PROD-005',
          name: 'Updated Product',
          price: 75.0,
          stock: 15,
          description: 'Updated description',
        },
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
          onConflictExcludeFields: ['description'],
        } as any,
      )

      // Should update all fields except description
      expect(result).toMatchObject({
        id: initial.id,
        sku: 'PROD-005',
        name: 'Updated Product', // updated
        price: 75.0, // updated
        stock: 15, // updated
        description: 'Original description', // excluded from merge
      })
    })
  })

  describe('multiple record upsert', () => {
    it('should upsert multiple records with ignore action', async () => {
      // Create some initial products
      await app.service('products').create([
        { sku: 'PROD-100', name: 'Product 100', price: 10.0, stock: 10 },
        { sku: 'PROD-101', name: 'Product 101', price: 20.0, stock: 20 },
      ])

      // Upsert multiple products, some new and some existing
      const results = await app.service('products').upsert(
        [
          { sku: 'PROD-100', name: 'Updated 100', price: 15.0, stock: 15 },
          { sku: 'PROD-102', name: 'Product 102', price: 30.0, stock: 30 },
          { sku: 'PROD-101', name: 'Updated 101', price: 25.0, stock: 25 },
        ],
        {
          onConflictFields: ['sku'],
          onConflictAction: 'ignore',
        } as any,
      )

      expect(results).toHaveLength(3)

      // Check that existing products were not updated
      const product100 = results.find((p) => p.sku === 'PROD-100')
      expect(product100).toMatchObject({
        sku: 'PROD-100',
        name: 'Product 100', // original name
        price: 10.0, // original price
      })

      // Check that new product was inserted
      const product102 = results.find((p) => p.sku === 'PROD-102')
      expect(product102).toMatchObject({
        sku: 'PROD-102',
        name: 'Product 102',
        price: 30.0,
      })
    })

    it('should upsert multiple records with merge action', async () => {
      // Create some initial products
      await app.service('products').create([
        {
          sku: 'PROD-200',
          name: 'Product 200',
          price: 10.0,
          stock: 10,
          description: 'Desc 200',
        },
        {
          sku: 'PROD-201',
          name: 'Product 201',
          price: 20.0,
          stock: 20,
          description: 'Desc 201',
        },
      ])

      // Upsert multiple products with merge
      const results = await app.service('products').upsert(
        [
          {
            sku: 'PROD-200',
            name: 'Updated 200',
            price: 15.0,
            stock: 15,
            description: 'New Desc 200',
          },
          {
            sku: 'PROD-202',
            name: 'Product 202',
            price: 30.0,
            stock: 30,
            description: 'Desc 202',
          },
        ],
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any,
      )

      expect(results).toHaveLength(2)

      // Check that existing product was updated
      const product200 = results.find((p) => p.sku === 'PROD-200')
      expect(product200).toMatchObject({
        sku: 'PROD-200',
        name: 'Updated 200', // updated
        price: 15.0, // updated
        stock: 15, // updated
        description: 'New Desc 200', // updated
      })

      // Check that new product was inserted
      const product202 = results.find((p) => p.sku === 'PROD-202')
      expect(product202).toMatchObject({
        sku: 'PROD-202',
        name: 'Product 202',
        price: 30.0,
      })
    })
  })

  describe('bulk upsert (additional tests)', () => {
    it('should bulk upsert all new records efficiently', async () => {
      const results = await app.service('products').upsert(
        [
          { sku: 'BULK-001', name: 'Bulk 1', price: 100.0, stock: 50 },
          { sku: 'BULK-002', name: 'Bulk 2', price: 200.0, stock: 60 },
          { sku: 'BULK-003', name: 'Bulk 3', price: 300.0, stock: 70 },
          { sku: 'BULK-004', name: 'Bulk 4', price: 400.0, stock: 80 },
          { sku: 'BULK-005', name: 'Bulk 5', price: 500.0, stock: 90 },
        ],
        {
          onConflictFields: ['sku'],
          onConflictAction: 'ignore',
        } as any,
      )

      expect(results).toHaveLength(5)
      expect(results.every((r) => r.id)).toBe(true)
      expect(results.map((r) => r.sku)).toEqual([
        'BULK-001',
        'BULK-002',
        'BULK-003',
        'BULK-004',
        'BULK-005',
      ])
    })

    it('should bulk upsert all existing records with ignore action', async () => {
      // Create initial products
      await app.service('products').create([
        { sku: 'EXIST-001', name: 'Existing 1', price: 10.0, stock: 1 },
        { sku: 'EXIST-002', name: 'Existing 2', price: 20.0, stock: 2 },
        { sku: 'EXIST-003', name: 'Existing 3', price: 30.0, stock: 3 },
      ])

      // Try to upsert all existing with different data
      const results = await app.service('products').upsert(
        [
          { sku: 'EXIST-001', name: 'Updated 1', price: 100.0, stock: 100 },
          { sku: 'EXIST-002', name: 'Updated 2', price: 200.0, stock: 200 },
          { sku: 'EXIST-003', name: 'Updated 3', price: 300.0, stock: 300 },
        ],
        {
          onConflictFields: ['sku'],
          onConflictAction: 'ignore',
        } as any,
      )

      expect(results).toHaveLength(3)

      // All should have original values
      const sorted = results.sort((a, b) => a.sku.localeCompare(b.sku))
      expect(sorted[0]).toMatchObject({
        sku: 'EXIST-001',
        name: 'Existing 1',
        price: 10.0,
      })
      expect(sorted[1]).toMatchObject({
        sku: 'EXIST-002',
        name: 'Existing 2',
        price: 20.0,
      })
      expect(sorted[2]).toMatchObject({
        sku: 'EXIST-003',
        name: 'Existing 3',
        price: 30.0,
      })
    })

    it('should bulk upsert all existing records with merge action', async () => {
      // Create initial products
      await app.service('products').create([
        {
          sku: 'MERGE-001',
          name: 'Original 1',
          price: 10.0,
          stock: 1,
          description: 'Desc 1',
        },
        {
          sku: 'MERGE-002',
          name: 'Original 2',
          price: 20.0,
          stock: 2,
          description: 'Desc 2',
        },
        {
          sku: 'MERGE-003',
          name: 'Original 3',
          price: 30.0,
          stock: 3,
          description: 'Desc 3',
        },
      ])

      // Upsert all existing with different data
      const results = await app.service('products').upsert(
        [
          {
            sku: 'MERGE-001',
            name: 'Updated 1',
            price: 100.0,
            stock: 100,
            description: 'New Desc 1',
          },
          {
            sku: 'MERGE-002',
            name: 'Updated 2',
            price: 200.0,
            stock: 200,
            description: 'New Desc 2',
          },
          {
            sku: 'MERGE-003',
            name: 'Updated 3',
            price: 300.0,
            stock: 300,
            description: 'New Desc 3',
          },
        ],
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any,
      )

      expect(results).toHaveLength(3)

      // All should have updated values
      const sorted = results.sort((a, b) => a.sku.localeCompare(b.sku))
      expect(sorted[0]).toMatchObject({
        sku: 'MERGE-001',
        name: 'Updated 1',
        price: 100.0,
        description: 'New Desc 1',
      })
      expect(sorted[1]).toMatchObject({
        sku: 'MERGE-002',
        name: 'Updated 2',
        price: 200.0,
        description: 'New Desc 2',
      })
      expect(sorted[2]).toMatchObject({
        sku: 'MERGE-003',
        name: 'Updated 3',
        price: 300.0,
        description: 'New Desc 3',
      })
    })

    it('should bulk upsert with partial merge fields', async () => {
      // Create initial products
      await app.service('products').create([
        {
          sku: 'PARTIAL-001',
          name: 'Original 1',
          price: 10.0,
          stock: 1,
          description: 'Original Desc 1',
        },
        {
          sku: 'PARTIAL-002',
          name: 'Original 2',
          price: 20.0,
          stock: 2,
          description: 'Original Desc 2',
        },
      ])

      // Upsert with only price and stock updates (merge mode with specific fields)
      const results = await app.service('products').upsert(
        [
          {
            sku: 'PARTIAL-001',
            name: 'New Name 1',
            price: 100.0,
            stock: 100,
            description: 'New Desc 1',
          },
          {
            sku: 'PARTIAL-002',
            name: 'New Name 2',
            price: 200.0,
            stock: 200,
            description: 'New Desc 2',
          },
          {
            sku: 'PARTIAL-003',
            name: 'New 3',
            price: 300.0,
            stock: 300,
            description: 'New Desc 3',
          },
        ],
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
          onConflictMergeFields: ['price', 'stock'],
        } as any,
      )

      expect(results).toHaveLength(3)

      // Check existing records - only price and stock should be updated
      const partial1 = results.find((r) => r.sku === 'PARTIAL-001')
      expect(partial1).toMatchObject({
        sku: 'PARTIAL-001',
        name: 'Original 1', // unchanged
        price: 100.0, // updated
        stock: 100, // updated
        description: 'Original Desc 1', // unchanged
      })

      const partial2 = results.find((r) => r.sku === 'PARTIAL-002')
      expect(partial2).toMatchObject({
        sku: 'PARTIAL-002',
        name: 'Original 2', // unchanged
        price: 200.0, // updated
        stock: 200, // updated
        description: 'Original Desc 2', // unchanged
      })

      // New record should have all fields
      const partial3 = results.find((r) => r.sku === 'PARTIAL-003')
      expect(partial3).toMatchObject({
        sku: 'PARTIAL-003',
        name: 'New 3',
        price: 300.0,
        stock: 300,
        description: 'New Desc 3',
      })
    })

    it('should handle large bulk upsert efficiently', async () => {
      const largeDataset = Array.from({ length: 100 }, (_, i) => ({
        sku: `LARGE-${String(i).padStart(3, '0')}`,
        name: `Large Product ${i}`,
        price: (i + 1) * 10.0,
        stock: i * 5,
      }))

      const results = await app.service('products').upsert(largeDataset, {
        onConflictFields: ['sku'],
        onConflictAction: 'ignore',
      } as any)

      expect(results).toHaveLength(100)
      expect(results.every((r) => r.id)).toBe(true)
      expect(results.every((r) => r.sku.startsWith('LARGE-'))).toBe(true)

      // Update half of them
      const updateDataset = largeDataset.slice(0, 50).map((item) => ({
        ...item,
        price: item.price * 2,
      }))

      const updateResults = await app
        .service('products')
        .upsert(updateDataset, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any)

      expect(updateResults).toHaveLength(50)
      expect(updateResults[0].price).toBe(largeDataset[0].price * 2)
    })

    it('should handle bulk upsert with excludeFields', async () => {
      // Create initial products
      await app.service('products').create([
        {
          sku: 'EXCLUDE-001',
          name: 'Name 1',
          price: 10.0,
          stock: 100,
          description: 'Keep this',
        },
        {
          sku: 'EXCLUDE-002',
          name: 'Name 2',
          price: 20.0,
          stock: 200,
          description: 'Keep this too',
        },
      ])

      // Upsert excluding description field
      const results = await app.service('products').upsert(
        [
          {
            sku: 'EXCLUDE-001',
            name: 'New Name 1',
            price: 100.0,
            stock: 1000,
            description: 'Should not update',
          },
          {
            sku: 'EXCLUDE-002',
            name: 'New Name 2',
            price: 200.0,
            stock: 2000,
            description: 'Should not update',
          },
        ],
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
          onConflictExcludeFields: ['description'],
        } as any,
      )

      expect(results).toHaveLength(2)

      const sorted = results.sort((a, b) => a.sku.localeCompare(b.sku))
      expect(sorted[0]).toMatchObject({
        sku: 'EXCLUDE-001',
        name: 'New Name 1',
        price: 100.0,
        stock: 1000,
        description: 'Keep this', // not updated
      })
      expect(sorted[1]).toMatchObject({
        sku: 'EXCLUDE-002',
        name: 'New Name 2',
        price: 200.0,
        stock: 2000,
        description: 'Keep this too', // not updated
      })
    })
  })

  describe('edge cases', () => {
    it('should handle upsert with null values', async () => {
      // Create initial product with all fields
      const initial = await app.service('products').create({
        sku: 'PROD-300',
        name: 'Product 300',
        price: 50.0,
        stock: 10,
        description: 'Has description',
      })

      // Upsert with null values
      const result = await app.service('products').upsert(
        {
          sku: 'PROD-300',
          name: 'Product 300',
          price: 50.0,
          stock: null,
          description: null,
        },
        {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any,
      )

      expect(result).toMatchObject({
        id: initial.id,
        sku: 'PROD-300',
        stock: null,
        description: null,
      })
    })

    it('should work with $select parameter', async () => {
      await app.service('products').create({
        sku: 'PROD-400',
        name: 'Product 400',
        price: 100.0,
        stock: 5,
      })

      const result = await app.service('products').upsert(
        {
          sku: 'PROD-400',
          name: 'Updated Product 400',
          price: 150.0,
          stock: 10,
        },
        {
          query: {
            $select: ['id', 'sku', 'price'],
          },
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any,
      )

      // Should only return selected fields
      expect(result).toHaveProperty('id')
      expect(result).toHaveProperty('sku')
      expect(result).toHaveProperty('price')
      expect(result.price).toBe(150.0)
    })
  })

  describe('error handling', () => {
    it('should handle upsert without onConflictFields parameter', async () => {
      // Should behave like regular insert
      const result = await app
        .service('products')
        .upsert(
          { sku: 'NO-CONFLICT-FIELD', name: 'Test', price: 10 },
          {} as any,
        )
      expect(result).toHaveProperty('id')
      expect(result.sku).toBe('NO-CONFLICT-FIELD')
    })

    it('should throw error on duplicate without conflict handling', async () => {
      await app.service('products').create({
        sku: 'DUP-ERROR',
        name: 'Test',
        price: 10,
      })

      await expect(
        app
          .service('products')
          .upsert({ sku: 'DUP-ERROR', name: 'Test 2', price: 20 }, {} as any),
      ).rejects.toThrow()
    })
  })

  describe('data type handling', () => {
    it('should handle upsert with zero values', async () => {
      const result = await app
        .service('products')
        .upsert({ sku: 'ZERO', name: 'Zero Product', price: 0, stock: 0 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any)
      expect(result.price).toBe(0)
      expect(result.stock).toBe(0)
    })

    it('should handle upsert with negative numbers', async () => {
      const result = await app
        .service('products')
        .upsert({ sku: 'NEG', name: 'Negative', price: -10.5, stock: -5 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any)
      expect(result.price).toBe(-10.5)
    })

    it('should handle upsert with very large numbers', async () => {
      const result = await app
        .service('products')
        .upsert({ sku: 'LARGE', name: 'Large', price: 999999999.99 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any)
      // Use 0 decimal places for comparison due to 'real' type precision limits
      expect(result.price).toBeCloseTo(999999999.99, 0)
    })

    it('should handle upsert with empty string', async () => {
      const result = await app
        .service('products')
        .upsert({ sku: 'EMPTY', name: '', price: 10, description: '' }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any)
      expect(result.name).toBe('')
      expect(result.description).toBe('')
    })

    it('should handle upsert with special characters', async () => {
      const result = await app.service('products').upsert(
        {
          sku: 'SPECIAL-123',
          name: 'Test\'s "Product" & More',
          price: 10,
          description: 'Special chars: émojis 🚀',
        },
        { onConflictFields: ['sku'], onConflictAction: 'merge' } as any,
      )
      expect(result.name).toContain("Test's")
      expect(result.description).toContain('🚀')
    })
  })

  describe('mixed insert and update', () => {
    it('should handle mixed new and existing records with ignore', async () => {
      // Create some existing records
      await app.service('products').create([
        { sku: 'EXIST-1', name: 'Existing 1', price: 10 },
        { sku: 'EXIST-3', name: 'Existing 3', price: 30 },
      ])

      // Upsert mix of new and existing
      const results = await app.service('products').upsert(
        [
          { sku: 'EXIST-1', name: 'Updated 1', price: 100 }, // existing - should ignore
          { sku: 'NEW-2', name: 'New 2', price: 200 }, // new - should insert
          { sku: 'EXIST-3', name: 'Updated 3', price: 300 }, // existing - should ignore
          { sku: 'NEW-4', name: 'New 4', price: 400 }, // new - should insert
        ],
        { onConflictFields: ['sku'], onConflictAction: 'ignore' } as any,
      )

      expect(results).toHaveLength(4)

      // Check existing records kept original values
      const exist1 = results.find((r) => r.sku === 'EXIST-1')
      expect(exist1!.name).toBe('Existing 1')
      expect(exist1!.price).toBe(10)

      // Check new records were inserted
      const new2 = results.find((r) => r.sku === 'NEW-2')
      expect(new2!.name).toBe('New 2')
      expect(new2!.price).toBe(200)
    })

    it('should handle mixed new and existing records with merge', async () => {
      await app
        .service('products')
        .create([{ sku: 'MIX-1', name: 'Original 1', price: 10, stock: 100 }])

      const results = await app.service('products').upsert(
        [
          { sku: 'MIX-1', name: 'Updated 1', price: 15, stock: 150 }, // update
          { sku: 'MIX-2', name: 'New 2', price: 25, stock: 250 }, // insert
        ],
        { onConflictFields: ['sku'], onConflictAction: 'merge' } as any,
      )

      expect(results).toHaveLength(2)

      const mix1 = results.find((r) => r.sku === 'MIX-1')
      expect(mix1!.name).toBe('Updated 1')
      expect(mix1!.price).toBe(15)

      const mix2 = results.find((r) => r.sku === 'MIX-2')
      expect(mix2!.id).toBeDefined()
    })
  })

  describe('conflict field variations', () => {
    it('should handle single conflict field on id column', async () => {
      const created = await app.service('products').create({
        sku: 'ID-TEST',
        name: 'Original',
        price: 10,
      })

      const result = await app
        .service('products')
        .upsert(
          { id: created.id, sku: 'ID-TEST', name: 'Updated', price: 20 },
          { onConflictFields: ['id'], onConflictAction: 'merge' } as any,
        )

      expect(result.id).toBe(created.id)
      expect(result.name).toBe('Updated')
    })

    it('should not update conflict fields themselves', async () => {
      await app.service('products').create({
        sku: 'ORIG-SKU',
        name: 'Original',
        price: 10,
      })

      const result = await app
        .service('products')
        .upsert({ sku: 'ORIG-SKU', name: 'Updated', price: 20 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any)

      // SKU should remain the same (it's the conflict field)
      expect(result.sku).toBe('ORIG-SKU')
      expect(result.name).toBe('Updated')
    })
  })

  describe('merge fields edge cases', () => {
    it('should handle empty onConflictMergeFields array', async () => {
      await app.service('products').create({
        sku: 'EMPTY-MERGE',
        name: 'Original',
        price: 10,
        stock: 5,
      })

      const result = await app
        .service('products')
        .upsert({ sku: 'EMPTY-MERGE', name: 'Updated', price: 20, stock: 10 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
          onConflictMergeFields: [],
        } as any)

      // Nothing should be updated (no fields to merge)
      expect(result.name).toBe('Original')
      expect(result.price).toBe(10)
      expect(result.stock).toBe(5)
    })

    it('should handle overlapping merge and exclude fields', async () => {
      await app.service('products').create({
        sku: 'OVERLAP',
        name: 'Original',
        price: 10,
        stock: 5,
      })

      const result = await app
        .service('products')
        .upsert({ sku: 'OVERLAP', name: 'Updated', price: 20, stock: 10 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
          onConflictMergeFields: ['name', 'price', 'stock'],
          onConflictExcludeFields: ['price'], // Exclude should win
        } as any)

      expect(result.name).toBe('Updated')
      expect(result.price).toBe(10) // Should not be updated (excluded)
      expect(result.stock).toBe(10)
    })
  })

  describe('return value verification', () => {
    it('should return correct IDs for bulk upsert', async () => {
      const results = await app.service('products').upsert(
        [
          { sku: 'ID-1', name: 'Prod 1', price: 10 },
          { sku: 'ID-2', name: 'Prod 2', price: 20 },
          { sku: 'ID-3', name: 'Prod 3', price: 30 },
        ],
        { onConflictFields: ['sku'], onConflictAction: 'ignore' } as any,
      )

      // All should have unique IDs
      const ids = results.map((r) => r.id)
      const uniqueIds = new Set(ids)
      expect(uniqueIds.size).toBe(3)
    })

    it('should preserve original IDs when using ignore', async () => {
      const created = await app.service('products').create({
        sku: 'PRESERVE-ID',
        name: 'Original',
        price: 10,
      })

      const result = await app
        .service('products')
        .upsert({ sku: 'PRESERVE-ID', name: 'Updated', price: 20 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'ignore',
        } as any)

      expect(result.id).toBe(created.id)
    })

    it('should return all fields when $select is not specified', async () => {
      const result = await app.service('products').upsert(
        {
          sku: 'ALL-FIELDS',
          name: 'Test',
          price: 10,
          stock: 5,
          description: 'Desc',
        },
        { onConflictFields: ['sku'], onConflictAction: 'merge' } as any,
      )

      expect(result).toHaveProperty('id')
      expect(result).toHaveProperty('sku')
      expect(result).toHaveProperty('name')
      expect(result).toHaveProperty('price')
      expect(result).toHaveProperty('stock')
      expect(result).toHaveProperty('description')
    })
  })

  describe('null handling variations', () => {
    it('should update non-null to null', async () => {
      await app.service('products').create({
        sku: 'NULL-TEST',
        name: 'Product',
        price: 10,
        stock: 5,
        description: 'Has desc',
      })

      const result = await app.service('products').upsert(
        {
          sku: 'NULL-TEST',
          name: 'Product',
          price: 10,
          stock: null,
          description: null,
        },
        { onConflictFields: ['sku'], onConflictAction: 'merge' } as any,
      )

      expect(result.stock).toBeNull()
      expect(result.description).toBeNull()
    })

    it('should handle all optional fields as null', async () => {
      const result = await app.service('products').upsert(
        {
          sku: 'ALL-NULL',
          name: 'Product',
          price: 10,
          stock: null,
          description: null,
        },
        { onConflictFields: ['sku'], onConflictAction: 'merge' } as any,
      )

      expect(result.stock).toBeNull()
      expect(result.description).toBeNull()
    })
  })

  describe('performance tests', () => {
    it('should handle upsert with very long strings', async () => {
      const longString = 'A'.repeat(200) // Test near varchar limit

      const result = await app
        .service('products')
        .upsert({ sku: 'LONG-STR', name: longString, price: 10 }, {
          onConflictFields: ['sku'],
          onConflictAction: 'merge',
        } as any)

      expect(result.name).toBe(longString)
    })

    it('should handle upsert with maximum batch size', async () => {
      const batch = Array.from({ length: 500 }, (_, i) => ({
        sku: `BATCH-${i}`,
        name: `Product ${i}`,
        price: i * 10,
      }))

      const results = await app.service('products').upsert(batch, {
        onConflictFields: ['sku'],
        onConflictAction: 'ignore',
      } as any)

      expect(results).toHaveLength(500)
      expect(results.every((r) => r.id)).toBe(true)
    })
  })

  describe.skipIf(dialectType === 'mysql')('database-specific behavior', () => {
    it('should handle multiple conflict fields (composite unique constraint)', async () => {
      // This test would require a table with composite unique constraints
      // Skipping for now as our test table doesn't have one
    })
  })
})
