import type { Generated, RawBuilder } from 'kysely'
import { Kysely, sql } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'
import { transformData } from 'feathers-utils'

import { KyselyService } from '../src/index.js'
import { describe } from 'vitest'

interface DB {
  array_contains_test: {
    id: Generated<number>
    str: string
    intArr: number[]
    strArr: string[]
    bigintArr: string[]
    floatArr: number[]
    numericArr: string[]
    charArr: string[]
    varcharArr: string[]
    dateArr: string[]
    jsonArr: any[]
  }
}

type ContainsTest = {
  id: number
  str: string
  intArr: number[]
  strArr: string[]
  bigintArr: (number | string)[]
  floatArr: number[]
  numericArr: (number | string)[]
  charArr: string[]
  varcharArr: string[]
  dateArr: string[]
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
      .addColumn('strArr', sql`text[]`)
      .addColumn('bigintArr', sql`bigint[]`)
      .addColumn('floatArr', sql`float8[]`)
      .addColumn('numericArr', sql`numeric[]`)
      .addColumn('charArr', sql`char(4)[]`)
      .addColumn('varcharArr', sql`varchar[]`)
      .addColumn('dateArr', sql`date[]`)
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
      properties: {
        // Declares the column as jsonb so containment/overlap operators emit
        // jsonb operands instead of the native-array codegen used for
        // genuine integer[]/varchar[] columns.
        jsonArr: { type: 'array', 'x-db-type': 'jsonb' },
        // For non-text[]/integer[] array columns the declared array type drives
        // the cast of the literal so it matches the column's element type.
        bigintArr: { type: 'array', 'x-db-type': 'bigint[]' },
        floatArr: { type: 'array', 'x-db-type': 'float8[]' },
        numericArr: { type: 'array', 'x-db-type': 'numeric[]' },
        charArr: { type: 'array', 'x-db-type': 'char(4)[]' },
        varcharArr: { type: 'array', 'x-db-type': 'varchar[]' },
        // `date[]` does not trigger temporal coercion (temporalKind matches only
        // an exact "date"), and array-operator values are excluded from temporal
        // coercion anyway, so the date strings are cast straight to ::date[].
        dateArr: { type: 'array', 'x-db-type': 'date[]' },
      },
    }),
  )

  const containsTest = app.service('contains-test')

  containsTest.hooks({
    before: {
      create: [
        transformData((item: Record<string, any>) => {
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

describe.skipIf(dialectName !== 'postgres')('special operators', () => {
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

  it('array contains/contained/overlaps text[] works', async () => {
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

  // The native ::text[]/::integer[] codegen only matches genuine text[]/integer[]
  // columns. Other element types (bigint[], float8[], numeric[], char(n)[],
  // varchar[]) require both operands to share the exact array type, which is
  // driven by the column's `x-db-type` annotation.
  it('array operators honor the declared array type (varchar/bigint/float/numeric/char)', async () => {
    const [item1, item2] = await app.service('contains-test').create([
      {
        varcharArr: ['a', 'b', 'c'],
        bigintArr: [1, 2, 3],
        floatArr: [1.5, 2.5, 3.5],
        numericArr: [1, 2, 3],
        charArr: ['aaaa', 'bbbb', 'cccc'],
        dateArr: ['2026-06-17', '2026-06-18'],
      },
      {
        varcharArr: ['c', 'd', 'e'],
        bigintArr: [3, 4, 5],
        floatArr: [3.5, 4.5, 5.5],
        numericArr: [3, 4, 5],
        charArr: ['cccc', 'dddd', 'eeee'],
        dateArr: ['2026-06-19', '2026-06-20'],
      },
    ])

    // varchar[] - was the originally reported `varchar[] @> text[]` failure
    const vc = await app.service('contains-test').find({
      query: { varcharArr: { $contains: ['a', 'b'] } },
      paginate: false,
    })
    expect(vc.map((r) => r.id)).toEqual([item1.id])

    // bigint[]
    const bi = await app.service('contains-test').find({
      query: { bigintArr: { $overlap: [3, 4] } },
      paginate: false,
    })
    expect(bi.map((r) => r.id).sort()).toEqual([item1.id, item2.id].sort())

    // float8[]
    const fl = await app.service('contains-test').find({
      query: { floatArr: { $contains: [1.5, 2.5] } },
      paginate: false,
    })
    expect(fl.map((r) => r.id)).toEqual([item1.id])

    // numeric[]
    const nu = await app.service('contains-test').find({
      query: { numericArr: { $contained: [1, 2, 3, 4] } },
      paginate: false,
    })
    expect(nu.map((r) => r.id)).toEqual([item1.id])

    // char(4)[] - parenthesized type qualifier
    const ch = await app.service('contains-test').find({
      query: { charArr: { $contains: ['aaaa'] } },
      paginate: false,
    })
    expect(ch.map((r) => r.id)).toEqual([item1.id])

    // date[] - no temporal coercion interference for array operators
    const dt = await app.service('contains-test').find({
      query: { dateArr: { $contains: ['2026-06-17'] } },
      paginate: false,
    })
    expect(dt.map((r) => r.id)).toEqual([item1.id])
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

    // contains single element -> column @> '["a"]'::jsonb
    const findResultSingle = await app.service('contains-test').find({
      query: {
        jsonArr: { $contains: ['a'] },
      },
      paginate: false,
    })

    expect(findResultSingle.length).toBe(1)
    expect(findResultSingle[0].id).toBe(item1.id)

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

  it('array contains/contained/overlaps numeric jsonb works', async () => {
    const [item1, item2] = await app.service('contains-test').create([
      {
        jsonArr: [1, 2, 3],
      },
      {
        jsonArr: [3, 4, 5],
      },
    ])

    expect(item1.jsonArr).toEqual([1, 2, 3])

    // contains subset
    const findResult = await app.service('contains-test').find({
      query: {
        jsonArr: { $contains: [1, 2] },
      },
      paginate: false,
    })

    expect(findResult.length).toBe(1)
    expect(findResult[0].id).toBe(item1.id)

    // contained
    const findResult2 = await app.service('contains-test').find({
      query: {
        jsonArr: { $contained: [1, 2, 3, 4] },
      },
      paginate: false,
    })

    expect(findResult2.length).toBe(1)
    expect(findResult2[0].id).toBe(item1.id)

    // overlaps - validates the OR-of-@> fallback for numeric jsonb arrays
    const findResult3 = await app.service('contains-test').find({
      query: {
        jsonArr: { $overlap: [2, 3, 4] },
      },
      paginate: false,
    })

    expect(findResult3.length).toBe(2)
  })
})
