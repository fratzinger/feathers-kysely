import type { Generated, RawBuilder } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'
import { alterItems } from 'feathers-hooks-common'

import { KyselyService } from '../src/index.js'
import { describe } from 'vitest'

interface DB {
  array_contains_test: {
    id: Generated<number>
    str: string
    intArr: number[]
    strArr: string[]
    jsonArr: any[]
  }
}

type ContainsTest = {
  id: number
  str: string
  intArr: number[]
  strArr: string[]
  jsonArr: any[]
}

function json<T>(value: T): RawBuilder<T> {
  return sql`CAST(${JSON.stringify(value)} AS JSONB)`
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
    await db.schema.dropTable('contains_test').ifExists().execute()

    await db.schema
      .createTable('contains_test')
      .addColumn('id', 'serial', (col) => col.primaryKey())
      .addColumn('str', 'varchar')
      .addColumn('intArr', sql`integer[]`)
      .addColumn('strArr', sql`varchar[]`)
      .addColumn('jsonArr', 'jsonb')
      .execute()
  }

  const app = feathers<{
    'contains-test': KyselyService<ContainsTest>
  }>().use(
    'contains-test',
    new KyselyService<ContainsTest>({
      Model: db,
      id: 'id',
      name: 'contains_test',
      multi: true,
    }),
  )

  const containsTest = app.service('contains-test')

  containsTest.hooks({
    before: {
      create: [
        alterItems((item) => {
          if (item.jsonArr) {
            item.jsonArr = json(item.jsonArr)
          }
        }),
      ],
    },
  })

  return {
    db,
    clean,
    containsTest: app.service('contains-test'),
    app,
  }
}

const { app, db, clean } = setup()

const dialectName = getDialect()

describe.skipIf(dialectName !== 'postgres' || true /** TODO */)(
  'special operators',
  () => {
    beforeEach(clean)

    afterAll(() => db.destroy())

    it('$like works', async () => {
      const [item1, item2] = await app.service('contains-test').create([
        {
          str: 'hello world',
        },
        { str: 'goodbye world' },
      ])

      const findResult = await app.service('contains-test').find({
        query: {
          str: { $like: '%hello%' },
        },
        paginate: false,
      })

      expect(findResult.length).toBe(1)
      expect(findResult[0].id).toBe(item1.id)

      const findResult2 = await app.service('contains-test').find({
        query: {
          str: { $like: '%world' },
        },
        paginate: false,
      })

      expect(findResult2.length).toBe(2)
    })

    it('%iLike works', async () => {
      const [item1, item2] = await app.service('contains-test').create([
        {
          str: 'Hello World',
        },
        { str: 'Goodbye World' },
      ])

      const findResult = await app.service('contains-test').find({
        query: {
          str: { $iLike: '%hello%' },
        },
        paginate: false,
      })

      expect(findResult.length).toBe(1)
      expect(findResult[0].id).toBe(item1.id)

      const findResult2 = await app.service('contains-test').find({
        query: {
          str: { $iLike: '%world' },
        },
        paginate: false,
      })

      expect(findResult2.length).toBe(2)
    })

    it('array contains/contained/overlaps integer[] works', async () => {
      const [item1, item2] = await app.service('contains-test').create([
        {
          intArr: [1, 2, 3],
        },
        {
          intArr: [3, 4, 5],
        },
      ])

      expect(item1.intArr).toEqual([1, 2, 3])

      // contains subset
      const findResult = await app.service('contains-test').find({
        query: {
          intArr: { $contains: [1, 2] },
        },
        paginate: false,
      })

      expect(findResult.length).toBe(1)
      expect(findResult[0].id).toBe(item1.id)

      // contains bigger
      const findResult2 = await app.service('contains-test').find({
        query: {
          intArr: { $contains: [1, 2, 3, 4] },
        },
        paginate: false,
      })

      expect(findResult2.length).toBe(0)

      // contained
      const findResult3 = await app.service('contains-test').find({
        query: {
          intArr: { $contained: [1, 2, 3, 4] },
        },
        paginate: false,
      })

      expect(findResult3.length).toBe(1)
      expect(findResult3[0].id).toBe(item1.id)

      // overlaps
      const findResult4 = await app.service('contains-test').find({
        query: {
          intArr: { $overlap: [2, 3, 4] },
        },
        paginate: false,
      })

      expect(findResult4.length).toBe(2)
    })

    it('array contains/contained/overlaps varchar[] works', async () => {
      const [item1, item2] = await app.service('contains-test').create([
        {
          strArr: ['a', 'b', 'c'],
        },
        {
          strArr: ['c', 'd', 'e'],
        },
      ])

      expect(item1.strArr).toEqual(['a', 'b', 'c'])

      // contains subset
      const findResult = await app.service('contains-test').find({
        query: {
          strArr: { $contains: ['a', 'b'] },
        },
        paginate: false,
      })

      expect(findResult.length).toBe(1)
      expect(findResult[0].id).toBe(item1.id)

      // contains bigger
      const findResult2 = await app.service('contains-test').find({
        query: {
          strArr: { $contains: ['a', 'b', 'c', 'd'] },
        },
        paginate: false,
      })

      expect(findResult2.length).toBe(0)

      // contained
      const findResult3 = await app.service('contains-test').find({
        query: {
          strArr: { $contained: ['a', 'b', 'c', 'd'] },
        },
        paginate: false,
      })

      expect(findResult3.length).toBe(1)
      expect(findResult3[0].id).toBe(item1.id)

      // overlaps
      const findResult4 = await app.service('contains-test').find({
        query: {
          strArr: { $overlap: ['b', 'c', 'd'] },
        },
        paginate: false,
      })

      expect(findResult4.length).toBe(2)
    })

    it('array contains/contained/overlaps jsonb works', async () => {
      const [item1, item2] = await app.service('contains-test').create([
        {
          jsonArr: ['a', 'b', 'c'],
        },
        {
          jsonArr: ['c', 'd', 'e'],
        },
      ])

      expect(item1.jsonArr).toEqual(['a', 'b', 'c'])

      // contains subset
      const findResult = await app.service('contains-test').find({
        query: {
          jsonArr: { $contains: ['a', 'b'] },
        },
        paginate: false,
      })

      expect(findResult.length).toBe(1)
      expect(findResult[0].id).toBe(item1.id)

      // contains bigger
      const findResult2 = await app.service('contains-test').find({
        query: {
          jsonArr: { $contains: ['a', 'b', 'c', 'd'] },
        },
        paginate: false,
      })

      expect(findResult2.length).toBe(0)

      // contained
      const findResult3 = await app.service('contains-test').find({
        query: {
          jsonArr: { $contained: ['a', 'b', 'c', 'd'] },
        },
        paginate: false,
      })

      expect(findResult3.length).toBe(1)
      expect(findResult3[0].id).toBe(item1.id)

      // overlaps
      const findResult4 = await app.service('contains-test').find({
        query: {
          jsonArr: { $overlap: ['b', 'c', 'd'] },
        },
        paginate: false,
      })

      expect(findResult4.length).toBe(2)
    })
  },
)
