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

  describe('onConflictReturning', () => {
    describe("'written'", () => {
      it('single + ignore with conflict resolves to undefined and leaves the row unchanged', async () => {
        const initial = await app.service('products').create({
          sku: 'W-001',
          name: 'Original',
          price: 10,
          stock: 1,
        })

        const result = await app.service('products').create(
          { sku: 'W-001', name: 'Updated', price: 99, stock: 9 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'written',
            },
          },
        )

        expect(result).toBeUndefined()

        const rows = (await app
          .service('products')
          .find({ query: { sku: 'W-001' } })) as Product[]
        expect(rows).toHaveLength(1)
        expect(rows[0]).toMatchObject({
          id: initial.id,
          name: 'Original',
          price: 10,
        })
      })

      it('single + ignore without conflict returns the inserted row', async () => {
        const result = await app.service('products').create(
          { sku: 'W-002', name: 'Fresh', price: 20, stock: 2 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'written',
            },
          },
        )

        expect(result).toMatchObject({ sku: 'W-002', name: 'Fresh' })
        expect(result.id).toBeDefined()
      })

      it('multi mixed batch returns only the fresh rows', async () => {
        await app.service('products').create({
          sku: 'W-010',
          name: 'Existing',
          price: 10,
          stock: 1,
        })

        const result = (await app.service('products').create(
          [
            { sku: 'W-010', name: 'Existing Updated', price: 99, stock: 9 },
            { sku: 'W-011', name: 'Fresh', price: 20, stock: 2 },
          ],
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'written',
            },
          },
        )) as Product[]

        expect(result).toHaveLength(1)
        expect(result[0]).toMatchObject({ sku: 'W-011', name: 'Fresh' })

        const all = (await app.service('products').find({})) as Product[]
        expect(all).toHaveLength(2)
        const existing = all.find((r) => r.sku === 'W-010')
        expect(existing).toMatchObject({ name: 'Existing', price: 10 })
      })

      it('multi all-conflicting batch resolves to an empty array', async () => {
        await app.service('products').create([
          { sku: 'W-020', name: 'One', price: 10, stock: 1 },
          { sku: 'W-021', name: 'Two', price: 20, stock: 2 },
        ])

        const result = (await app.service('products').create(
          [
            { sku: 'W-020', name: 'One Updated', price: 99, stock: 9 },
            { sku: 'W-021', name: 'Two Updated', price: 99, stock: 9 },
          ],
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'written',
            },
          },
        )) as Product[]

        expect(result).toEqual([])
      })

      it("multi + merge writes every row and thus behaves like 'all'", async () => {
        await app.service('products').create({
          sku: 'W-030',
          name: 'Existing',
          price: 10,
          stock: 1,
        })

        const result = (await app.service('products').create(
          [
            { sku: 'W-030', name: 'Existing Updated', price: 99, stock: 9 },
            { sku: 'W-031', name: 'Fresh', price: 20, stock: 2 },
          ],
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'merge',
              onConflictReturning: 'written',
            },
          },
        )) as Product[]

        expect(result).toHaveLength(2)
        const bySku = Object.fromEntries(result.map((r) => [r.sku, r]))
        expect(bySku['W-030']).toMatchObject({
          name: 'Existing Updated',
          price: 99,
        })
        expect(bySku['W-031']).toMatchObject({ name: 'Fresh', price: 20 })
      })

      it('merge with empty onConflictMergeFields (effectively ignored) resolves to undefined on conflict', async () => {
        await app.service('products').create({
          sku: 'W-040',
          name: 'Original',
          price: 10,
          stock: 1,
        })

        const result = await app.service('products').create(
          { sku: 'W-040', name: 'Updated', price: 99, stock: 9 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'merge',
              onConflictMergeFields: [],
              onConflictReturning: 'written',
            },
          },
        )

        expect(result).toBeUndefined()
      })
    })

    describe("'changed'", () => {
      it('single + merge with identical values resolves to undefined and does not write', async () => {
        const initial = await app.service('products').create({
          sku: 'CH-001',
          name: 'Original',
          price: 50,
          stock: 5,
        })

        const result = await app.service('products').create(
          { sku: 'CH-001', name: 'Original', price: 50, stock: 5 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'merge',
              onConflictReturning: 'changed',
            },
          },
        )

        expect(result).toBeUndefined()

        const rows = (await app
          .service('products')
          .find({ query: { sku: 'CH-001' } })) as Product[]
        expect(rows).toHaveLength(1)
        expect(rows[0]).toMatchObject({ id: initial.id, name: 'Original' })
      })

      it('single + merge with changed values returns the merged row', async () => {
        const initial = await app.service('products').create({
          sku: 'CH-002',
          name: 'Original',
          price: 50,
          stock: 5,
        })

        const result = await app.service('products').create(
          { sku: 'CH-002', name: 'Updated', price: 75, stock: 5 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'merge',
              onConflictReturning: 'changed',
            },
          },
        )

        expect(result).toMatchObject({
          id: initial.id,
          sku: 'CH-002',
          name: 'Updated',
          price: 75,
        })
      })

      it("multi returns only fresh and actually changed rows (MySQL: behaves like 'all')", async () => {
        await app.service('products').create([
          { sku: 'CH-101', name: 'Identical', price: 10, stock: 1 },
          { sku: 'CH-102', name: 'Old Name', price: 20, stock: 2 },
        ])

        const result = (await app.service('products').create(
          [
            { sku: 'CH-101', name: 'Identical', price: 10, stock: 1 }, // no-op
            { sku: 'CH-102', name: 'New Name', price: 25, stock: 2 }, // changed
            { sku: 'CH-103', name: 'Fresh', price: 30, stock: 3 }, // insert
          ],
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'merge',
              onConflictReturning: 'changed',
            },
          },
        )) as Product[]

        const skus = result.map((r) => r.sku).sort()
        if (dialectType === 'mysql') {
          // Documented limitation: MySQL has no RETURNING, so the returned
          // rows of a multi merge behave like 'all'.
          expect(skus).toEqual(['CH-101', 'CH-102', 'CH-103'])
        } else {
          expect(skus).toEqual(['CH-102', 'CH-103'])
        }

        const changed = result.find((r) => r.sku === 'CH-102')
        expect(changed).toMatchObject({ name: 'New Name', price: 25 })
      })

      it("ignore action behaves like 'written'", async () => {
        await app.service('products').create({
          sku: 'CH-201',
          name: 'Original',
          price: 10,
          stock: 1,
        })

        const result = await app.service('products').create(
          { sku: 'CH-201', name: 'Updated', price: 99, stock: 9 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'changed',
            },
          },
        )

        expect(result).toBeUndefined()
      })

      it('compares only the merge fields (onConflictMergeFields)', async () => {
        const initial = await app.service('products').create({
          sku: 'CH-301',
          name: 'Original',
          price: 50,
          stock: 5,
        })

        // name and stock differ, but only price is merged — and price is
        // identical, so nothing is written.
        const result = await app.service('products').create(
          { sku: 'CH-301', name: 'Different', price: 50, stock: 99 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'merge',
              onConflictMergeFields: ['price'],
              onConflictReturning: 'changed',
            },
          },
        )

        expect(result).toBeUndefined()

        const rows = (await app
          .service('products')
          .find({ query: { sku: 'CH-301' } })) as Product[]
        expect(rows[0]).toMatchObject({
          id: initial.id,
          name: 'Original',
          stock: 5,
        })
      })
    })

    describe("'none'", () => {
      it('single with conflict resolves to undefined', async () => {
        await app.service('products').create({
          sku: 'N-001',
          name: 'Original',
          price: 10,
          stock: 1,
        })

        const result = await app.service('products').create(
          { sku: 'N-001', name: 'Updated', price: 99, stock: 9 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'none',
            },
          },
        )

        expect(result).toBeUndefined()
      })

      it('single without conflict resolves to undefined but inserts the row', async () => {
        const result = await app.service('products').create(
          { sku: 'N-002', name: 'Fresh', price: 20, stock: 2 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'none',
            },
          },
        )

        expect(result).toBeUndefined()

        const rows = (await app
          .service('products')
          .find({ query: { sku: 'N-002' } })) as Product[]
        expect(rows).toHaveLength(1)
        expect(rows[0]).toMatchObject({ name: 'Fresh', price: 20 })
      })

      it('single + merge resolves to undefined but merges the row', async () => {
        const initial = await app.service('products').create({
          sku: 'N-003',
          name: 'Original',
          price: 10,
          stock: 1,
        })

        const result = await app.service('products').create(
          { sku: 'N-003', name: 'Merged', price: 99, stock: 9 },
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'merge',
              onConflictReturning: 'none',
            },
          },
        )

        expect(result).toBeUndefined()

        const rows = (await app
          .service('products')
          .find({ query: { sku: 'N-003' } })) as Product[]
        expect(rows).toHaveLength(1)
        expect(rows[0]).toMatchObject({
          id: initial.id,
          name: 'Merged',
          price: 99,
        })
      })

      it('multi resolves to an empty array but inserts the fresh rows', async () => {
        await app.service('products').create({
          sku: 'N-010',
          name: 'Existing',
          price: 10,
          stock: 1,
        })

        const result = await app.service('products').create(
          [
            { sku: 'N-010', name: 'Existing Updated', price: 99, stock: 9 },
            { sku: 'N-011', name: 'Fresh', price: 20, stock: 2 },
          ],
          {
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'none',
            },
          },
        )

        expect(result).toEqual([])

        const all = (await app.service('products').find({})) as Product[]
        expect(all).toHaveLength(2)
      })
    })

    describe('scoping & interplay', () => {
      it('is ignored without onConflictFields', async () => {
        const result = await app
          .service('products')
          .create(
            { sku: 'SC-001', name: 'Plain', price: 10, stock: 1 },
            { kysely: { onConflictReturning: 'none' } },
          )

        expect(result).toMatchObject({ sku: 'SC-001', name: 'Plain' })
        expect(result.id).toBeDefined()
      })

      it('honors $select for written rows', async () => {
        await app.service('products').create({
          sku: 'SC-010',
          name: 'Existing',
          price: 10,
          stock: 1,
        })

        const result = (await app.service('products').create(
          [
            { sku: 'SC-010', name: 'Existing Updated', price: 99, stock: 9 },
            { sku: 'SC-011', name: 'Fresh', price: 20, stock: 2 },
          ],
          {
            query: { $select: ['id', 'sku'] },
            kysely: {
              onConflictFields: ['sku'],
              onConflictAction: 'ignore',
              onConflictReturning: 'written',
            },
          },
        )) as Product[]

        expect(result).toHaveLength(1)
        expect(result[0].sku).toBe('SC-011')
        expect(Object.keys(result[0]).sort()).toEqual(['id', 'sku'])
      })

      it('create([]) still resolves to []', async () => {
        const result = await app.service('products').create([], {
          kysely: {
            onConflictFields: ['sku'],
            onConflictReturning: 'written',
          },
        })

        expect(result).toEqual([])
      })

      it("emits 'created' with undefined when a single ignored create returns nothing", async () => {
        await app.service('products').create({
          sku: 'SC-020',
          name: 'Original',
          price: 5,
          stock: 1,
        })

        const events: unknown[] = []
        const handler = (product: unknown) => events.push(product)
        app.service('products').on('created', handler)

        try {
          const result = await app.service('products').create(
            { sku: 'SC-020', name: 'Ignored', price: 9, stock: 2 },
            {
              kysely: {
                onConflictFields: ['sku'],
                onConflictAction: 'ignore',
                onConflictReturning: 'written',
              },
            },
          )
          expect(result).toBeUndefined()
        } finally {
          app.service('products').removeListener('created', handler)
        }

        // The standard pipeline still emits `created` — with undefined as
        // payload (documented caveat).
        expect(events).toHaveLength(1)
        expect(events[0]).toBeUndefined()
      })

      it('deprecated upsert forwards onConflictReturning', async () => {
        await app.service('products').create({
          sku: 'SC-030',
          name: 'Original',
          price: 5,
          stock: 1,
        })

        const result = await app
          .service('products')
          .upsert({ sku: 'SC-030', name: 'Ignored', price: 9, stock: 2 }, {
            onConflictFields: ['sku'],
            onConflictAction: 'ignore',
            onConflictReturning: 'written',
          } as any)

        expect(result).toBeUndefined()
      })
    })
  })
})
