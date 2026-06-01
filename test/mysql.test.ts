import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import dialect, { getDialect } from './dialect.js'

import { KyselyService } from '../src/index.js'
import { describe } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

interface DB {
  mysql_users: {
    id: Generated<number>
    name: string
    age: number | null
  }
  // Non-auto-increment, application-supplied string primary key.
  mysql_items: {
    code: string
    label: string | null
  }
}

type MysqlUser = { id: number; name: string; age: number | null }
type MysqlItem = { code: string; label: string | null }

function setup() {
  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    await db.schema.dropTable('mysql_users').ifExists().execute()
    await addPrimaryKey(
      db.schema
        .createTable('mysql_users')
        .addColumn('name', 'varchar(255)', (col) => col.notNull())
        .addColumn('age', 'integer'),
      'id',
    ).execute()

    await db.schema.dropTable('mysql_items').ifExists().execute()
    await db.schema
      .createTable('mysql_items')
      .addColumn('code', 'varchar(64)', (col) => col.primaryKey())
      .addColumn('label', 'varchar(255)')
      .execute()
  }

  const app = feathers<{
    mysql_users: KyselyService<MysqlUser>
    mysql_items: KyselyService<MysqlItem>
  }>()
    .use(
      'mysql_users',
      new KyselyService<MysqlUser>({
        Model: db,
        name: 'mysql_users',
        multi: true,
        properties: { id: true, name: true, age: true },
      }),
    )
    .use(
      'mysql_items',
      new KyselyService<MysqlItem>({
        Model: db,
        id: 'code',
        name: 'mysql_items',
        multi: true,
        properties: { code: true, label: true },
      }),
    )

  return { app, db, clean }
}

const { app, db, clean } = setup()

const dialectName = getDialect()

// MySQL has no RETURNING, so the adapter re-fetches written rows by their id.
// These tests guard that path (see executeAndReturn in src/adapter.ts).
describe.skipIf(dialectName !== 'mysql')('mysql write path', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  it('create(single) returns the inserted row with its generated id', async () => {
    const created = await app
      .service('mysql_users')
      .create({ name: 'Dave', age: 40 })

    expect(typeof created.id).toBe('number')
    expect(created.name).toBe('Dave')
    expect(created.age).toBe(40)
  })

  it('create([...]) returns every inserted row (auto-increment ids)', async () => {
    const created = await app.service('mysql_users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    expect(created).toHaveLength(3)

    // Order of the IN(...) re-fetch is not guaranteed, so match set-wise.
    const names = created.map((u) => u.name).sort()
    expect(names).toEqual(['Alice', 'Bob', 'Charlie'])

    // Each returned row carries a real, distinct numeric id paired with its row.
    const ids = created.map((u) => u.id)
    expect(ids.every((id) => typeof id === 'number')).toBe(true)
    expect(new Set(ids).size).toBe(3)

    const alice = created.find((u) => u.name === 'Alice')
    expect(alice?.age).toBe(30)
  })

  it('create([...]) honors $select on the MySQL re-fetch', async () => {
    const created = await app
      .service('mysql_users')
      .create([{ name: 'Eve', age: 22 }], { query: { $select: ['name'] } })

    expect(created).toHaveLength(1)
    expect(created[0].name).toBe('Eve')
    // id is force-added by applySelectId so the adapter can re-fetch.
    expect('id' in created[0]).toBe(true)
  })

  it('create([...]) with application-supplied (non-numeric) primary keys re-fetches by those keys', async () => {
    // Non-sequential, non-numeric ids: the old "firstId + i" guess would return
    // the wrong rows (or none). The fix re-fetches by the supplied codes.
    const created = await app.service('mysql_items').create([
      { code: 'z-100', label: 'first' },
      { code: 'a-001', label: 'second' },
    ])

    expect(created).toHaveLength(2)

    const byCode = Object.fromEntries(created.map((i) => [i.code, i.label]))
    expect(byCode['z-100']).toBe('first')
    expect(byCode['a-001']).toBe('second')
  })

  it('create(single) with an application-supplied primary key returns that row', async () => {
    const created = await app
      .service('mysql_items')
      .create({ code: 'single-1', label: 'only' })

    expect(created.code).toBe('single-1')
    expect(created.label).toBe('only')
  })
})
