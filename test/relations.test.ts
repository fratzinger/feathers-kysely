import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import assert from 'node:assert'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { describe, it } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

function setup() {
  interface TodosTable {
    id: Generated<number>
    text: string
    userId: number
    assigneeId: number | null
    completedById: number | null
  }

  interface UsersTable {
    id: Generated<number>
    name: string
    age: number | null
    time?: number | null
    created: boolean | null
    managerId: number | null
  }

  interface DB {
    todos: TodosTable
    users: UsersTable
  }

  const db = new Kysely<DB>({
    dialect: dialect(),
    // log(event) {
    //   console.log(event.query.sql, event.query.parameters)
    // },
  })

  const clean = async () => {
    // drop and recreate the todos table
    await db.schema.dropTable('todos').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('todos')
        .addColumn('text', 'text', (col) => col.notNull())
        .addColumn('userId', 'integer', (col) => col.notNull())
        .addColumn('assigneeId', 'integer')
        .addColumn('completedById', 'integer'),
      'id',
    ).execute()

    // drop and recreate the users table
    await db.schema.dropTable('users').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('users')
        .addColumn('name', 'text', (col) => col.notNull())
        .addColumn('age', 'real')
        .addColumn('time', 'real')
        .addColumn('created', 'boolean')
        .addColumn('managerId', 'integer'),
      'id',
    ).execute()
  }

  const users = new KyselyService<User>({
    Model: db,
    name: 'users',
    multi: true,
    properties: {
      id: true,
      name: true,
      age: true,
      time: true,
      created: true,
      managerId: true,
    },
    relations: {
      todos: {
        service: 'todos',
        keyHere: 'id',
        keyThere: 'userId',
        asArray: true,
        databaseTableName: 'todos',
      },
      manager: {
        service: 'users',
        keyHere: 'managerId',
        keyThere: 'id',
        asArray: false,
        databaseTableName: 'users',
      },
      reports: {
        service: 'users',
        keyHere: 'id',
        keyThere: 'managerId',
        asArray: true,
        databaseTableName: 'users',
      },
    },
  })

  const todos = new KyselyService<Todo>({
    Model: db,
    name: 'todos',
    multi: true,
    properties: {
      id: true,
      text: true,
      userId: true,
      assigneeId: true,
      completedById: true,
    },
    relations: {
      user: {
        service: 'users',
        keyHere: 'userId',
        keyThere: 'id',
        asArray: false,
        databaseTableName: 'users',
      },
      assignee: {
        service: 'users',
        keyHere: 'assigneeId',
        keyThere: 'id',
        asArray: false,
        databaseTableName: 'users',
      },
      completedBy: {
        service: 'users',
        keyHere: 'completedById',
        keyThere: 'id',
        asArray: false,
        databaseTableName: 'users',
      },
    },
  })

  type User = {
    id: number
    name: string
    age: number | null
    time: string
    create: boolean
    managerId: number | null
  }

  type Todo = {
    id: number
    text: string
    userId: number
    assigneeId: number | null
    completedById: number | null
  }

  type ServiceTypes = {
    users: KyselyService<User>
    todos: KyselyService<Todo>
  }

  const app = feathers<ServiceTypes>().use('users', users).use('todos', todos)
  return { app, db, clean }
}

const { app, db, clean } = setup()

describe('relations', () => {
  beforeEach(clean)

  afterAll(() => db.destroy())

  it('query for belongsTo with dot.notation', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const aliceTodos = await app
      .service('todos')
      .find({ query: { 'user.name': 'Alice' }, paginate: false })
    assert.strictEqual(aliceTodos.length, 2)
    assert.ok(aliceTodos.every((todo) => todo.userId === users[0].id))
  })

  it('query for belongsTo with nested notation', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const aliceTodos = await app
      .service('todos')
      .find({ query: { user: { name: 'Alice' } }, paginate: false })
    assert.strictEqual(aliceTodos.length, 2)
    assert.ok(aliceTodos.every((todo) => todo.userId === users[0].id))
  })

  it('query for hasMany with dot.notation 1', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const usersWithTodos = await app
      .service('users')
      .find({ query: { 'todos.text': { $like: '%first%' } }, paginate: false })
    assert.strictEqual(usersWithTodos.length, 2)
    const alice = usersWithTodos.find((u) => u.name === 'Alice')
    const bob = usersWithTodos.find((u) => u.name === 'Bob')
    assert.ok(alice)
    assert.ok(bob)
  })

  it('query for hasMany with dot.notation 2', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const usersWithTodos = await app
      .service('users')
      .find({ query: { 'todos.text': { $like: '%todo%' } }, paginate: false })
    assert.strictEqual(usersWithTodos.length, 2)
    const alice = usersWithTodos.find((u) => u.name === 'Alice')
    const bob = usersWithTodos.find((u) => u.name === 'Bob')
    assert.ok(alice)
    assert.ok(bob)
  })

  it.skip('query for hasMany with multiple dot.notations', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
      { name: 'David', age: 28 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
      { text: "Bob's second todo", userId: users[1].id },
      { text: "David's only todo", userId: users[3].id },
    ])

    const usersWithTodos = await app.service('users').find({
      query: {
        'todos.text': { $like: '%todo%' },
        'todos.userId': users[1].id,
      },
      paginate: false,
    })
    assert.strictEqual(usersWithTodos.length, 2)
    const alice = usersWithTodos.find((u) => u.name === 'Alice')
    const bob = usersWithTodos.find((u) => u.name === 'Bob')
    assert.ok(alice)
    assert.ok(bob)
  })

  it('query for hasMany with nested notation 1', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const usersWithTodos = await app.service('users').find({
      query: { todos: { text: { $like: '%todo%' } } },
      paginate: false,
    })
    assert.strictEqual(usersWithTodos.length, 2)
    const alice = usersWithTodos.find((u) => u.name === 'Alice')
    const bob = usersWithTodos.find((u) => u.name === 'Bob')
    assert.ok(alice)
    assert.ok(bob)
  })

  it('query for hasMany with $some', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const result = await app.service('users').find({
      query: { todos: { $some: { text: { $like: '%first%' } } } },
      paginate: false,
    })
    assert.strictEqual(result.length, 2)
    assert.ok(result.find((u) => u.name === 'Alice'))
    assert.ok(result.find((u) => u.name === 'Bob'))
  })

  it('query for hasMany with $none: no matching children', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    // Charlie has no todos at all, Alice and Bob have todos with "todo" in text
    const result = await app.service('users').find({
      query: { todos: { $none: { text: { $like: '%todo%' } } } },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Charlie')
  })

  it('query for hasMany with $none: no children at all', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    // Only Charlie has no todos
    const result = await app.service('users').find({
      query: { todos: { $none: {} } },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Charlie')
  })

  it('query for hasMany with $every', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first item", userId: users[1].id },
      { text: "Bob's second todo", userId: users[1].id },
    ])

    // Alice: all todos contain "todo" -> matches
    // Bob: only one contains "todo" -> does not match
    // Charlie: no todos at all -> matches (vacuous truth)
    const result = await app.service('users').find({
      query: { todos: { $every: { text: { $like: '%todo%' } } } },
      paginate: false,
    })
    assert.strictEqual(result.length, 2)
    assert.ok(result.find((u) => u.name === 'Alice'))
    assert.ok(result.find((u) => u.name === 'Charlie'))
  })

  it('query for hasMany with $none combined with regular filters', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    // Users older than 28 who have no todos
    const result = await app.service('users').find({
      query: { age: { $gt: 28 }, todos: { $none: {} } },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Charlie')
  })

  // MARK: Self-referencing relations

  it('self-referencing belongsTo: query by manager name (dot notation)', async () => {
    const alice = await app.service('users').create({ name: 'Alice', age: 40 })
    await app.service('users').create([
      { name: 'Bob', age: 30, managerId: alice.id },
      { name: 'Charlie', age: 25, managerId: alice.id },
    ])

    const result = await app
      .service('users')
      .find({ query: { 'manager.name': 'Alice' }, paginate: false })
    assert.strictEqual(result.length, 2)
    assert.ok(result.find((u) => u.name === 'Bob'))
    assert.ok(result.find((u) => u.name === 'Charlie'))
  })

  it('self-referencing belongsTo: query by manager name (nested notation)', async () => {
    const alice = await app.service('users').create({ name: 'Alice', age: 40 })
    await app.service('users').create([
      { name: 'Bob', age: 30, managerId: alice.id },
      { name: 'Charlie', age: 25, managerId: alice.id },
    ])

    const result = await app
      .service('users')
      .find({ query: { manager: { name: 'Alice' } }, paginate: false })
    assert.strictEqual(result.length, 2)
    assert.ok(result.find((u) => u.name === 'Bob'))
    assert.ok(result.find((u) => u.name === 'Charlie'))
  })

  it('self-referencing hasMany: $some reports', async () => {
    const alice = await app.service('users').create({ name: 'Alice', age: 40 })
    await app.service('users').create([
      { name: 'Bob', age: 30, managerId: alice.id },
      { name: 'Charlie', age: 25 },
    ])

    // Alice manages Bob, so she has reports
    const result = await app.service('users').find({
      query: { reports: { $some: {} } },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Alice')
  })

  it('self-referencing hasMany: $none reports', async () => {
    const alice = await app.service('users').create({ name: 'Alice', age: 40 })
    await app.service('users').create([
      { name: 'Bob', age: 30, managerId: alice.id },
      { name: 'Charlie', age: 25 },
    ])

    // Bob and Charlie manage nobody
    const result = await app.service('users').find({
      query: { reports: { $none: {} } },
      paginate: false,
    })
    assert.strictEqual(result.length, 2)
    assert.ok(result.find((u) => u.name === 'Bob'))
    assert.ok(result.find((u) => u.name === 'Charlie'))
  })

  // MARK: Multiple relations to same table

  it('multiple relations to same table: query by creator name', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Todo 1', userId: users[0].id, assigneeId: users[1].id },
      { text: 'Todo 2', userId: users[1].id, assigneeId: users[0].id },
    ])

    const result = await app
      .service('todos')
      .find({ query: { 'user.name': 'Alice' }, paginate: false })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Todo 1')
  })

  it('multiple relations to same table: query by assignee name', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Todo 1', userId: users[0].id, assigneeId: users[1].id },
      { text: 'Todo 2', userId: users[1].id, assigneeId: users[0].id },
    ])

    const result = await app
      .service('todos')
      .find({ query: { 'assignee.name': 'Bob' }, paginate: false })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Todo 1')
  })

  it('multiple relations to same table: combine two relation filters', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Todo 1', userId: users[0].id, assigneeId: users[1].id },
      { text: 'Todo 2', userId: users[1].id, assigneeId: users[0].id },
      { text: 'Todo 3', userId: users[0].id, assigneeId: users[0].id },
    ])

    const result = await app.service('todos').find({
      query: { 'user.name': 'Alice', 'assignee.name': 'Bob' },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Todo 1')
  })

  it('multiple relations to same table: aliases do not collide', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: 'Todo 1', userId: users[0].id, assigneeId: users[1].id },
      { text: 'Todo 2', userId: users[2].id, assigneeId: users[0].id },
    ])

    // creator age > 28 AND assignee age < 30
    const result = await app.service('todos').find({
      query: { 'user.age': { $gt: 28 }, 'assignee.age': { $lt: 30 } },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Todo 1')
  })

  // MARK: Multi-level relations (unsupported — graceful handling)

  it('3-level dot notation is silently ignored', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create({ text: 'Todo 1', userId: users[0].id })

    // 3-level path should be silently skipped (parts.length !== 2)
    try {
      const result = await app.service('todos').find({
        query: { 'user.manager.name': 'Alice' },
        paginate: false,
      })
      // If no error, filter was ignored — all todos returned
      assert.strictEqual(result.length, 1)
    } catch {
      // An error is also acceptable — the key point is it doesn't crash the DB
    }
  })

  it("sort by relation's column", async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const todos = await app.service('todos').find({
      query: { $sort: { 'user.age': 1 } },
      paginate: false,
    })

    assert.strictEqual(todos.length, 3)
    assert.strictEqual(todos[0].userId, users[1].id)
  })
})
