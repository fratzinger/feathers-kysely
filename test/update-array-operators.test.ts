import type { Generated } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'
import { KyselyService, updateOperators } from '../src/index.js'
import { describe, it, expect, beforeEach, afterAll } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

const dialectName = getDialect()

// --- Postgres: native arrays (text[]) and jsonb arrays -----------------------
describe.skipIf(dialectName !== 'postgres')(
  'update array operators — Postgres (native arrays + jsonb)',
  () => {
    interface DB {
      arr_test: {
        id: Generated<number>
        tags: string[]
        labels: any
      }
    }
    const db = new Kysely<DB>({ dialect: dialect() })

    const clean = async () => {
      await db.schema.dropTable('arr_test').ifExists().execute()
      await db.schema
        .createTable('arr_test')
        .addColumn('id', 'serial', (c) => c.primaryKey())
        .addColumn('tags', sql`text[]`)
        .addColumn('labels', 'jsonb')
        .execute()
    }

    const service = new KyselyService<any>({
      Model: db,
      id: 'id',
      name: 'arr_test',
      multi: true,
      properties: {
        tags: { type: 'array', 'x-db-type': 'text[]' },
        labels: { type: 'array', 'x-db-type': 'jsonb' },
      },
    })
    const app = feathers().use('arr_test', service)
    app.service('arr_test').hooks({ before: { patch: [updateOperators()] } })

    beforeEach(clean)
    afterAll(() => db.destroy())

    const seed = async (tags: string[], labels: any[]) => {
      const row = await db
        .insertInto('arr_test')
        .values({
          tags: tags as any,
          labels: sql`${JSON.stringify(labels)}::jsonb` as any,
        })
        .returning('id')
        .executeTakeFirstOrThrow()
      return row.id
    }
    const read = (id: number) =>
      db
        .selectFrom('arr_test')
        .selectAll()
        .where('id', '=', id)
        .executeTakeFirstOrThrow()

    it('$push appends a scalar to a native text[] column', async () => {
      const id = await seed(['a'], [])
      await app.service('arr_test').patch(id, { $push: { tags: 'b' } } as any)
      expect((await read(id)).tags).toEqual(['a', 'b'])
    })

    it('$push appends several to a native text[] column', async () => {
      const id = await seed(['a'], [])
      await app
        .service('arr_test')
        .patch(id, { $push: { tags: ['b', 'c'] } } as any)
      expect((await read(id)).tags).toEqual(['a', 'b', 'c'])
    })

    it('$pull removes every occurrence from a native text[] column', async () => {
      const id = await seed(['a', 'b', 'a', 'c'], [])
      await app.service('arr_test').patch(id, { $pull: { tags: 'a' } } as any)
      expect((await read(id)).tags).toEqual(['b', 'c'])
    })

    it('$pull removes several values from a native text[] column', async () => {
      const id = await seed(['a', 'b', 'c', 'd'], [])
      await app
        .service('arr_test')
        .patch(id, { $pull: { tags: ['a', 'c'] } } as any)
      expect((await read(id)).tags).toEqual(['b', 'd'])
    })

    it('$push appends to a jsonb array column', async () => {
      const id = await seed([], ['x'])
      await app.service('arr_test').patch(id, { $push: { labels: 'y' } } as any)
      expect((await read(id)).labels).toEqual(['x', 'y'])
    })

    it('$pull removes from a jsonb array column', async () => {
      const id = await seed([], ['x', 'y', 'x', 'z'])
      await app.service('arr_test').patch(id, { $pull: { labels: 'x' } } as any)
      expect((await read(id)).labels).toEqual(['y', 'z'])
    })
  },
)

// --- SQLite: JSON arrays stored as text --------------------------------------
describe.skipIf(dialectName !== 'sqlite')(
  'update array operators — SQLite (JSON)',
  () => {
    interface DB {
      arr_test: { id: Generated<number>; tags: string; plain: string | null }
    }
    const db = new Kysely<DB>({ dialect: dialect() })

    const clean = async () => {
      await db.schema.dropTable('arr_test').ifExists().execute()
      await addPrimaryKey(
        db.schema
          .createTable('arr_test')
          .addColumn('tags', 'text', (c) => c.notNull().defaultTo('[]'))
          .addColumn('plain', 'text'),
        'id',
      ).execute()
    }

    const service = new KyselyService<any>({
      Model: db,
      id: 'id',
      name: 'arr_test',
      multi: true,
      properties: {
        tags: { type: 'array', 'x-db-type': 'json' },
        // `plain` is intentionally left without an x-db-type annotation
        plain: true,
      },
    })
    const app = feathers().use('arr_test', service)
    app.service('arr_test').hooks({
      before: { patch: [updateOperators()], update: [updateOperators()] },
    })

    beforeEach(clean)
    afterAll(() => db.destroy())

    const seed = async (tags: any[]) => {
      const row = await db
        .insertInto('arr_test')
        .values({ tags: JSON.stringify(tags) })
        .returning('id')
        .executeTakeFirstOrThrow()
      return row.id
    }
    const readTags = async (id: number) => {
      const row = await db
        .selectFrom('arr_test')
        .selectAll()
        .where('id', '=', id)
        .executeTakeFirstOrThrow()
      return JSON.parse(row.tags)
    }

    it('$push appends a scalar to a JSON array', async () => {
      const id = await seed(['a'])
      await app.service('arr_test').patch(id, { $push: { tags: 'b' } } as any)
      expect(await readTags(id)).toEqual(['a', 'b'])
    })

    it('$push appends several to a JSON array', async () => {
      const id = await seed(['a'])
      await app
        .service('arr_test')
        .patch(id, { $push: { tags: ['b', 'c'] } } as any)
      expect(await readTags(id)).toEqual(['a', 'b', 'c'])
    })

    it('$pull on a JSON column is rejected on SQLite', async () => {
      const id = await seed(['a', 'b'])
      await expect(
        app.service('arr_test').patch(id, { $pull: { tags: 'a' } } as any),
      ).rejects.toMatchObject({ name: 'BadRequest' })
    })

    it('$push on a column with no detectable type is rejected', async () => {
      const id = await seed([])
      await expect(
        app.service('arr_test').patch(id, { $push: { plain: 'x' } } as any),
      ).rejects.toMatchObject({ name: 'BadRequest' })
    })
  },
)
