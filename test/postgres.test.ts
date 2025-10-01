import type { Generated } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'

import { KyselyService } from '../src/index.js'
import { describe } from 'vitest'
import { transformData } from 'feathers-utils'

interface DB {
  postgres: {
    id: Generated<number>
    str: string
    intArr: number[]
    strArr: string[]
    jsonb: any
  }
}

type Postgres = {
  id: number
  str: string
  intArr: number[]
  strArr: string[]
  jsonb: any
}

function setup() {
  const db = new Kysely<DB>({
    dialect: dialect(),
    // log(event) {
    //   console.log(event.query.sql, event.query.parameters)
    // },
  })

  const clean = async () => {
    // drop and recreate the todos table
    await db.schema.dropTable('postgres').ifExists().execute()

    await db.schema
      .createTable('postgres')
      .addColumn('id', 'serial', (col) => col.primaryKey())
      .addColumn('str', 'varchar')
      .addColumn('intArr', sql`integer[]`)
      .addColumn('strArr', sql`varchar[]`)
      .addColumn('jsonb', 'jsonb')
      .execute()
  }

  const app = feathers<{
    postgres: KyselyService<Postgres>
  }>().use(
    'postgres',
    new KyselyService<Postgres>({
      Model: db,
      id: 'id',
      name: 'postgres',
      multi: true,
    }),
  )

  const postgresService = app.service('postgres')

  postgresService.hooks({
    before: {
      all: [],
      find: [],
      get: [],
      create: [
        transformData((data) => {
          if (data.jsonb) {
            data.jsonb = JSON.stringify(data.jsonb)
          }
        }),
      ],
      update: [
        transformData((data) => {
          if (data.jsonb) {
            data.jsonb = JSON.stringify(data.jsonb)
          }
        }),
      ],
      patch: [
        transformData((data) => {
          if (data.jsonb) {
            data.jsonb = JSON.stringify(data.jsonb)
          }
        }),
      ],
      remove: [],
    },
  })

  return {
    db,
    clean,
    postgres: app.service('postgres'),
    app,
  }
}

const { app, db, clean } = setup()

const dialectName = getDialect()

describe.skipIf(dialectName !== 'postgres')('postgres', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  describe('datatypes', () => {
    it('create special datatypes', async () => {
      const created = await app.service('postgres').create({
        str: 'test',
        intArr: [1, 2, 3],
        strArr: ['a', 'b', 'c'],
        jsonb: [{ a: 1 }, { b: 2 }],
      })
      // console.log('created', created)
    })

    it('patch special datatypes', async () => {
      const created = await app.service('postgres').create({
        str: 'test',
        intArr: [1, 2, 3],
        strArr: ['a', 'b', 'c'],
        jsonb: [{ a: 1 }, { b: 2 }],
      })

      const patchedIntArr = await app.service('postgres').patch(created.id, {
        intArr: [4, 5, 6],
      })

      const patchedStrArr = await app.service('postgres').patch(created.id, {
        strArr: ['d', 'e', 'f'],
      })

      const patchedJsonObject = await app
        .service('postgres')
        .patch(created.id, {
          jsonb: { e: 5, f: 6 },
        })

      const patchedJson = await app.service('postgres').patch(created.id, {
        jsonb: [{ c: 3 }, { d: 4 }],
      })
    })
  })
})
