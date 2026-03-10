import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import assert from 'node:assert'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { describe, it } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

function setup() {
  interface UsersTable {
    id: Generated<number>
    name: string
    age: number | null
  }

  interface TodosTable {
    id: Generated<number>
    text: string
    userId: number
  }

  interface DB {
    users: UsersTable
    todos: TodosTable
  }

  const db = new Kysely<DB>({ dialect: dialect() })

  const clean = async () => {
    await db.schema.dropTable('todos').ifExists().execute()
    await addPrimaryKey(
      db.schema
        .createTable('todos')
        .addColumn('text', 'text', (col) => col.notNull())
        .addColumn('userId', 'integer', (col) => col.notNull()),
      'id',
    ).execute()

    await db.schema.dropTable('users').ifExists().execute()
    await addPrimaryKey(
      db.schema
        .createTable('users')
        .addColumn('name', 'text', (col) => col.notNull())
        .addColumn('age', 'real'),
      'id',
    ).execute()
  }

  const users = new KyselyService({
    Model: db,
    name: 'users',
    multi: true,
    properties: { id: true, name: true, age: true },
    relations: {
      todos: {
        service: 'todos',
        keyHere: 'id',
        keyThere: 'userId',
        asArray: true,
        databaseTableName: 'todos',
      },
    },
  })

  const todos = new KyselyService({
    Model: db,
    name: 'todos',
    multi: true,
    properties: { id: true, text: true, userId: true },
    relations: {
      user: {
        service: 'users',
        keyHere: 'userId',
        keyThere: 'id',
        asArray: false,
        databaseTableName: 'users',
      },
    },
  })

  type ServiceTypes = {
    users: typeof users
    todos: typeof todos
  }

  const app = feathers<ServiceTypes>().use('users', users).use('todos', todos)
  return { app, db, clean }
}

const { app, db, clean } = setup()

async function assertTablesIntact(
  expectedUsers: number,
  expectedTodos: number,
) {
  const users = await app.service('users').find({ paginate: false })
  const todos = await app.service('todos').find({ paginate: false })
  assert.strictEqual(
    users.length,
    expectedUsers,
    'users table row count mismatch',
  )
  assert.strictEqual(
    todos.length,
    expectedTodos,
    'todos table row count mismatch',
  )
}

async function seed() {
  const users = await app.service('users').create([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
  ])
  await app.service('todos').create([
    { text: 'First todo', userId: users[0].id },
    { text: 'Second todo', userId: users[1].id },
  ])
  return users
}

describe('security - SQL injection', () => {
  beforeEach(clean)
  afterAll(() => db.destroy())

  // MARK: Value injection

  it('injection via query value string is parameterized', async () => {
    const injectionStr = "'; DROP TABLE users;--"
    await app.service('users').create({ name: injectionStr, age: 1 })
    await seed()

    const result = await app.service('users').find({
      query: { name: injectionStr },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, injectionStr)
    await assertTablesIntact(3, 2)
  })

  it('injection via $gt operator value', async () => {
    await seed()
    try {
      await app.service('users').find({
        query: { age: { $gt: '1; DROP TABLE users;--' } },
        paginate: false,
      })
    } catch {
      // error is acceptable — injection string as number comparison
    }
    await assertTablesIntact(2, 2)
  })

  it('injection via $like operator value', async () => {
    await seed()
    const result = await app.service('users').find({
      query: { name: { $like: "%'; DROP TABLE users;--" } },
      paginate: false,
    })
    assert.strictEqual(result.length, 0)
    await assertTablesIntact(2, 2)
  })

  it('injection via $in array values', async () => {
    await seed()
    try {
      await app.service('users').find({
        query: { id: { $in: ['1; DROP TABLE users;--'] } },
        paginate: false,
      })
    } catch {
      // error is acceptable
    }
    await assertTablesIntact(2, 2)
  })

  // MARK: Field name injection

  it('injection via query field name', async () => {
    await seed()
    try {
      await app.service('users').find({
        query: { 'id; DROP TABLE users;--': 1 },
        paginate: false,
      })
    } catch {
      // SQL syntax error expected
    }
    await assertTablesIntact(2, 2)
  })

  it('injection via $select field name', async () => {
    await seed()
    try {
      await app.service('users').find({
        query: { $select: ['id; DROP TABLE users;--'] },
        paginate: false,
      })
    } catch {
      // SQL syntax error expected
    }
    await assertTablesIntact(2, 2)
  })

  it('injection via $sort key', async () => {
    await seed()
    try {
      await app.service('users').find({
        query: { $sort: { 'id; DROP TABLE users;--': 1 } },
        paginate: false,
      })
    } catch {
      // SQL syntax error expected
    }
    await assertTablesIntact(2, 2)
  })

  // MARK: Relation injection

  it('injection via relation dot notation column', async () => {
    const users = await seed()
    try {
      await app.service('users').find({
        query: { 'todos.text; DROP TABLE users;--': 'test' },
        paginate: false,
      })
    } catch {
      // SQL error from invalid column expected
    }
    await assertTablesIntact(2, 2)
  })

  it('injection via relation key in dot notation', async () => {
    await seed()
    try {
      await app.service('users').find({
        query: { 'users;DROP TABLE users;--.name': 'test' },
        paginate: false,
      })
    } catch {
      // no matching relation, falls through to normal handling — error or ignored
    }
    await assertTablesIntact(2, 2)
  })

  // MARK: Logical operator injection

  it('injection nested in $or is parameterized', async () => {
    await seed()
    const result = await app.service('users').find({
      query: { $or: [{ name: "'; DROP TABLE users;--" }] },
      paginate: false,
    })
    assert.strictEqual(result.length, 0)
    await assertTablesIntact(2, 2)
  })

  it('injection nested in $and is parameterized', async () => {
    await seed()
    try {
      const result = await app.service('users').find({
        query: {
          $and: [
            { name: "'; DROP TABLE users;--" },
            { age: { $gt: '1; DROP TABLE users;--' } },
          ],
        },
        paginate: false,
      })
    } catch {
      // error from invalid $gt comparison is acceptable
    }
    await assertTablesIntact(2, 2)
  })

  // MARK: Comprehensive integrity check

  it('tables remain intact after a battery of injection attempts', async () => {
    const users = await seed()

    const injections = [
      () =>
        app
          .service('users')
          .find({ query: { name: "'; DROP TABLE users;--" }, paginate: false }),
      () =>
        app
          .service('users')
          .find({ query: { name: '1 OR 1=1' }, paginate: false }),
      () =>
        app.service('users').find({
          query: { name: { $like: '%; DROP TABLE users;--' } },
          paginate: false,
        }),
      () =>
        app
          .service('users')
          .find({ query: { 'id OR 1=1--': 1 }, paginate: false }),
      () =>
        app.service('users').find({
          query: { $or: [{ name: "' UNION SELECT * FROM users;--" }] },
          paginate: false,
        }),
      () =>
        app.service('todos').find({
          query: { 'user.name; DROP TABLE todos;--': 'x' },
          paginate: false,
        }),
    ]

    for (const attempt of injections) {
      try {
        await attempt()
      } catch {
        // errors are expected for some injection attempts
      }
    }

    await assertTablesIntact(2, 2)

    // Verify original data is still intact
    const allUsers = await app.service('users').find({ paginate: false })
    assert.ok(allUsers.find((u) => u.name === 'Alice'))
    assert.ok(allUsers.find((u) => u.name === 'Bob'))
  })
})
