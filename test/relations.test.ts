import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import assert from 'node:assert'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { beforeAll, describe, it } from 'vitest'
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
  beforeAll(() => app.setup())
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

  it('query for hasMany with $some and $or', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: 'urgent task', userId: users[0].id },
      { text: 'normal todo', userId: users[1].id },
      { text: 'boring note', userId: users[2].id },
    ])

    // Users who have a todo with text matching 'urgent' OR 'todo'
    const result = await app.service('users').find({
      query: {
        todos: {
          $some: {
            $or: [
              { text: { $like: '%urgent%' } },
              { text: { $like: '%todo%' } },
            ],
          },
        },
      },
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

  // MARK: $some/$none/$every on belongsTo relations

  it('$some/$none/$every on a belongsTo relation throw', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Todo 1', userId: users[0].id },
      { text: 'Todo 2', userId: users[1].id },
    ])

    // `user` is a belongsTo relation (asArray: false) — a collection operator
    // cannot apply to it. Dropping it silently would widen the result set.
    for (const query of [
      { user: { $some: { name: 'Alice' } } },
      { user: { $none: {} } },
      { user: { $every: { name: 'Alice' } } },
    ]) {
      await assert.rejects(
        () => app.service('todos').find({ query, paginate: false }),
        (error: any) => {
          assert.strictEqual(error.name, 'BadRequest')
          assert.match(error.message, /only valid on a hasMany relation/)
          return true
        },
      )
    }
  })

  // MARK: $some/$none/$every on non-existent relations

  it('$some/$none/$every on a non-existent relation throw', async () => {
    await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    // `nonExistent` is not a defined relation
    for (const query of [
      { nonExistent: { $some: { name: 'Alice' } } },
      { nonExistent: { $none: {} } },
      { nonExistent: { $every: { name: 'Alice' } } },
    ]) {
      await assert.rejects(
        () => app.service('users').find({ query, paginate: false }),
        (error: any) => {
          assert.strictEqual(error.name, 'BadRequest')
          return true
        },
      )
    }
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

  // MARK: Multi-level belongsTo

  it('3-level dot notation filters through chained belongsTo', async () => {
    const managers = await app
      .service('users')
      .create([{ name: 'Manager-A' }, { name: 'Manager-B' }])
    const workers = await app.service('users').create([
      { name: 'Alice', managerId: managers[0].id },
      { name: 'Bob', managerId: managers[1].id },
    ])
    await app.service('todos').create([
      { text: 'Alice todo', userId: workers[0].id },
      { text: 'Bob todo', userId: workers[1].id },
    ])

    const result = await app.service('todos').find({
      query: { 'user.manager.name': 'Manager-A' },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Alice todo')
  })

  it('3-level nested notation produces the same result as dot notation', async () => {
    const managers = await app
      .service('users')
      .create([{ name: 'Manager-A' }, { name: 'Manager-B' }])
    const workers = await app.service('users').create([
      { name: 'Alice', managerId: managers[0].id },
      { name: 'Bob', managerId: managers[1].id },
    ])
    await app.service('todos').create([
      { text: 'Alice todo', userId: workers[0].id },
      { text: 'Bob todo', userId: workers[1].id },
    ])

    const result = await app.service('todos').find({
      query: { user: { manager: { name: 'Manager-B' } } },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Bob todo')
  })

  it('3-level belongsTo with operator ($gt)', async () => {
    const managers = await app.service('users').create([
      { name: 'Manager-Old', age: 55 },
      { name: 'Manager-Young', age: 30 },
    ])
    const workers = await app.service('users').create([
      { name: 'Alice', managerId: managers[0].id },
      { name: 'Bob', managerId: managers[1].id },
    ])
    await app.service('todos').create([
      { text: 'Alice todo', userId: workers[0].id },
      { text: 'Bob todo', userId: workers[1].id },
    ])

    const result = await app.service('todos').find({
      query: { 'user.manager.age': { $gt: 40 } },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Alice todo')
  })

  it('3-level combined with 1-level sharing the same relation prefix', async () => {
    const managers = await app.service('users').create([{ name: 'Manager-A' }])
    const workers = await app.service('users').create([
      { name: 'Alice', managerId: managers[0].id },
      { name: 'Alicia', managerId: managers[0].id },
    ])
    await app.service('todos').create([
      { text: 'Alice todo', userId: workers[0].id },
      { text: 'Alicia todo', userId: workers[1].id },
    ])

    const result = await app.service('todos').find({
      query: {
        'user.manager.name': 'Manager-A',
        'user.name': 'Alice',
      },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Alice todo')
  })

  it('sort by 3-level belongsTo column', async () => {
    const managers = await app
      .service('users')
      .create([{ name: 'Manager-Z' }, { name: 'Manager-A' }])
    const workers = await app.service('users').create([
      { name: 'Alice', managerId: managers[0].id },
      { name: 'Bob', managerId: managers[1].id },
    ])
    await app.service('todos').create([
      { text: 'Alice todo', userId: workers[0].id },
      { text: 'Bob todo', userId: workers[1].id },
    ])

    const result = await app.service('todos').find({
      query: { $sort: { 'user.manager.name': 1 } },
      paginate: false,
    })
    assert.strictEqual(result.length, 2)
    // Bob's manager is Manager-A (first), Alice's is Manager-Z
    assert.strictEqual(result[0].text, 'Bob todo')
    assert.strictEqual(result[1].text, 'Alice todo')
  })

  it('3-level with $and filters', async () => {
    const managers = await app.service('users').create([
      { name: 'Manager-A', age: 50 },
      { name: 'Manager-B', age: 30 },
    ])
    const workers = await app.service('users').create([
      { name: 'Alice', managerId: managers[0].id },
      { name: 'Bob', managerId: managers[1].id },
    ])
    await app.service('todos').create([
      { text: 'Alice todo', userId: workers[0].id },
      { text: 'Bob todo', userId: workers[1].id },
    ])

    const result = await app.service('todos').find({
      query: {
        $and: [
          { 'user.manager.name': 'Manager-A' },
          { 'user.manager.age': { $gt: 40 } },
        ],
      },
      paginate: false,
    })
    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Alice todo')
  })

  it('3-level path with unknown middle segment throws', async () => {
    await app.service('users').create([{ name: 'Alice' }, { name: 'Bob' }])
    await app.service('todos').create({ text: 'Todo 1', userId: 1 })

    // The path starts at a declared relation but breaks further along — a
    // broken chain, not a column.
    await assert.rejects(
      () =>
        app.service('todos').find({
          query: { 'user.bogus.name': 'Alice' },
          paginate: false,
        }),
      (error: any) => {
        assert.strictEqual(error.name, 'BadRequest')
        assert.match(error.message, /does not resolve to a column/)
        return true
      },
    )
  })

  // MARK: relation chains through hasMany

  /**
   * Alice ← Bob ← Carol (manager chain), one todo per user.
   * Alice.reports = [Bob], Bob.reports = [Carol], Carol.reports = [].
   * The orphan todo has no user at all, so it also covers the LEFT JOIN
   * null-protect on a belongsTo hop.
   */
  const seedChain = async () => {
    const alice = await app.service('users').create({ name: 'Alice' })
    const bob = await app
      .service('users')
      .create({ name: 'Bob', managerId: alice.id })
    const carol = await app
      .service('users')
      .create({ name: 'Carol', managerId: bob.id })

    const todos = await app.service('todos').create([
      { text: 'Alice todo', userId: alice.id, assigneeId: carol.id },
      { text: 'Bob todo', userId: bob.id },
      { text: 'Carol todo', userId: carol.id },
      { text: 'Orphan todo', userId: 9999 },
    ])

    return { alice, bob, carol, todos }
  }

  it('3-level path through hasMany filters via EXISTS', async () => {
    await seedChain()

    // users who have at least one todo whose owner is Alice
    const result = await app.service('users').find({
      query: { 'todos.user.name': 'Alice' },
      paginate: false,
    })

    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Alice')
  })

  it('hasMany behind a belongsTo hop ($some, both notations)', async () => {
    await seedChain()

    // todos whose owner has a direct report named Carol → Bob's todo
    for (const query of [
      { user: { reports: { $some: { name: 'Carol' } } } },
      { 'user.reports': { $some: { name: 'Carol' } } },
      { 'user.reports.name': 'Carol' },
    ]) {
      const result = await app.service('todos').find({ query, paginate: false })
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0].text, 'Bob todo')
    }
  })

  it('hasMany behind a belongsTo hop ($none) excludes rows without a parent', async () => {
    await seedChain()

    // todos whose owner has no reports → Carol's todo. The orphan todo must not
    // slip through: its LEFT JOIN'd user is NULL, so NOT EXISTS would be
    // vacuously true without the null-protect on the hop.
    const result = await app.service('todos').find({
      query: { user: { reports: { $none: {} } } },
      paginate: false,
    })

    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].text, 'Carol todo')
  })

  it('belongsTo inside $some joins within the subquery', async () => {
    await seedChain()

    // users owning at least one todo that is assigned to Carol → Alice
    for (const query of [
      { todos: { $some: { assignee: { name: 'Carol' } } } },
      { todos: { $some: { 'assignee.name': 'Carol' } } },
      { 'todos.assignee.name': 'Carol' },
    ]) {
      const result = await app.service('users').find({ query, paginate: false })
      assert.strictEqual(result.length, 1)
      assert.strictEqual(result[0].name, 'Alice')
    }
  })

  it('nested hasMany inside $some does not shadow the outer alias', async () => {
    await seedChain()

    // users with a report that itself has a report named Carol → Alice
    const result = await app.service('users').find({
      query: { reports: { $some: { reports: { $some: { name: 'Carol' } } } } },
      paginate: false,
    })

    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Alice')
  })

  it('self-referencing belongsTo inside $some resolves in the child scope', async () => {
    await seedChain()

    // users with a report whose manager is Alice → Alice herself
    const result = await app.service('users').find({
      query: { reports: { $some: { 'manager.name': 'Alice' } } },
      paginate: false,
    })

    assert.strictEqual(result.length, 1)
    assert.strictEqual(result[0].name, 'Alice')

    // Bob's reports are managed by Bob, not Alice → no match for Bob
    const none = await app.service('users').find({
      query: { reports: { $some: { 'manager.name': 'Nobody' } } },
      paginate: false,
    })
    assert.strictEqual(none.length, 0)
  })

  it('relation chain combined with $or and a regular filter', async () => {
    await seedChain()

    const result = await app.service('todos').find({
      query: {
        $or: [{ 'user.reports.name': 'Carol' }, { text: 'Orphan todo' }],
        $sort: { text: 1 },
      },
      paginate: false,
    })

    assert.deepStrictEqual(
      result.map((todo: any) => todo.text),
      ['Bob todo', 'Orphan todo'],
    )
  })

  it('broken relation chain past a hasMany hop throws', async () => {
    await seedChain()

    // `bogus` is neither a relation nor a resolvable ref inside the subquery
    await assert.rejects(
      () =>
        app.service('users').find({
          query: { 'todos.bogus.name': 'Alice' },
          paginate: false,
        }),
      (error: any) => {
        assert.strictEqual(error.name, 'BadRequest')
        return true
      },
    )
  })

  it('unresolvable 3-level path inside $or throws', async () => {
    const users = await app
      .service('users')
      .create([{ name: 'Alice' }, { name: 'Bob' }])
    await app.service('todos').create([
      { text: 'Alice todo', userId: users[0].id },
      { text: 'Bob todo', userId: users[1].id },
    ])

    // Both legs reference a path that cannot be resolved. Neither may leak
    // into SQL as "a"."b"."c", and neither may be silently dropped — an $or
    // whose legs vanish matches every row.
    await assert.rejects(
      () =>
        app.service('todos').find({
          query: {
            $or: [
              { 'user.bogus.name': { $iLike: '%Alice%' } },
              { 'user.bogus.age': { $gt: 0 } },
            ],
          },
          paginate: false,
        }),
      (error: any) => {
        assert.strictEqual(error.name, 'BadRequest')
        return true
      },
    )
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

  // MARK: hasMany sort

  it('sort by hasMany relation column ascending (MIN)', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Z-todo', userId: users[0].id },
      { text: 'A-todo', userId: users[0].id },
      { text: 'M-todo', userId: users[1].id },
    ])

    const result = await app.service('users').find({
      query: { $sort: { 'todos.text': 1 } },
      paginate: false,
    })

    assert.strictEqual(result.length, 2)
    // Alice has MIN(text)='A-todo', Bob has MIN(text)='M-todo' → Alice first
    assert.strictEqual(result[0].name, 'Alice')
    assert.strictEqual(result[1].name, 'Bob')
  })

  it('sort by hasMany relation column descending (MAX)', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Z-todo', userId: users[0].id },
      { text: 'A-todo', userId: users[0].id },
      { text: 'M-todo', userId: users[1].id },
    ])

    const result = await app.service('users').find({
      query: { $sort: { 'todos.text': -1 } },
      paginate: false,
    })

    assert.strictEqual(result.length, 2)
    // Alice has MAX(text)='Z-todo', Bob has MAX(text)='M-todo' → Alice first (desc)
    assert.strictEqual(result[0].name, 'Alice')
    assert.strictEqual(result[1].name, 'Bob')
  })

  it('sort by hasMany with filter (extended form)', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Z-important', userId: users[0].id, assigneeId: 1 },
      { text: 'A-other', userId: users[0].id, assigneeId: 2 },
      { text: 'B-important', userId: users[1].id, assigneeId: 1 },
    ])

    // Sort by todos.text ascending, but only consider todos where assigneeId = 1
    const result = await app.service('users').find({
      query: {
        $sort: {
          'todos.text': { direction: 1, filter: { assigneeId: 1 } },
        } as any,
      },
      paginate: false,
    })

    assert.strictEqual(result.length, 2)
    // Alice: MIN(text where assigneeId=1) = 'Z-important'
    // Bob: MIN(text where assigneeId=1) = 'B-important'
    // 'B-important' < 'Z-important' → Bob first
    assert.strictEqual(result[0].name, 'Bob')
    assert.strictEqual(result[1].name, 'Alice')
  })

  it('sort by hasMany combined with regular sort', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'C-todo', userId: users[0].id },
      { text: 'A-todo', userId: users[1].id },
    ])

    const result = await app.service('users').find({
      query: { $sort: { 'todos.text': 1, name: 1 } },
      paginate: false,
    })

    assert.strictEqual(result.length, 2)
    // Bob has MIN(text)='A-todo', Alice has MIN(text)='C-todo' → Bob first
    assert.strictEqual(result[0].name, 'Bob')
    assert.strictEqual(result[1].name, 'Alice')
  })

  it('hasMany sort does not duplicate rows', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    // Alice has 5 todos, Bob has 1
    await app.service('todos').create([
      { text: 'A1', userId: users[0].id },
      { text: 'A2', userId: users[0].id },
      { text: 'A3', userId: users[0].id },
      { text: 'A4', userId: users[0].id },
      { text: 'A5', userId: users[0].id },
      { text: 'B1', userId: users[1].id },
    ])

    const result = await app.service('users').find({
      query: { $sort: { 'todos.text': 1 } },
      paginate: false,
    })

    // Must return exactly 2 users, not 6 (JOIN would duplicate)
    assert.strictEqual(result.length, 2)
  })

  it('hasMany sort with users that have no related records', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    // Only Alice and Bob have todos, Charlie has none
    await app.service('todos').create([
      { text: 'B-todo', userId: users[0].id },
      { text: 'A-todo', userId: users[1].id },
    ])

    const result = await app.service('users').find({
      query: { $sort: { 'todos.text': 1 } },
      paginate: false,
    })

    assert.strictEqual(result.length, 3)
    // Users with todos should be sorted; Charlie (NULL) position is dialect-dependent
    // but all 3 users must be present
    const names = result.map((u: any) => u.name)
    assert.ok(names.includes('Alice'))
    assert.ok(names.includes('Bob'))
    assert.ok(names.includes('Charlie'))
  })

  it('hasMany sort with filter excludes non-matching related records from sort', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    // Alice: assigneeId=1 → 'Z-task', assigneeId=2 → 'A-task'
    // Bob: assigneeId=1 → 'B-task'
    await app.service('todos').create([
      { text: 'Z-task', userId: users[0].id, assigneeId: 1 },
      { text: 'A-task', userId: users[0].id, assigneeId: 2 },
      { text: 'B-task', userId: users[1].id, assigneeId: 1 },
    ])

    // Without filter: Alice MIN='A-task' < Bob MIN='B-task' → Alice first
    const withoutFilter = await app.service('users').find({
      query: { $sort: { 'todos.text': 1 } },
      paginate: false,
    })
    assert.strictEqual(withoutFilter[0].name, 'Alice')

    // With filter assigneeId=1: Alice MIN='Z-task' > Bob MIN='B-task' → Bob first
    const withFilter = await app.service('users').find({
      query: {
        $sort: {
          'todos.text': { direction: 1, filter: { assigneeId: 1 } },
        } as any,
      },
      paginate: false,
    })
    assert.strictEqual((withFilter as any[])[0].name, 'Bob')
    assert.strictEqual((withFilter as any[])[1].name, 'Alice')
  })

  it('hasMany sort with filter where no records match filter', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'A-todo', userId: users[0].id, assigneeId: 1 },
      { text: 'B-todo', userId: users[1].id, assigneeId: 1 },
    ])

    // Filter by assigneeId=999 which matches nothing → all NULLs
    const result = await app.service('users').find({
      query: {
        $sort: {
          'todos.text': { direction: 1, filter: { assigneeId: 999 } },
        } as any,
      },
      paginate: false,
    })

    // Both users returned, both have NULL sort value
    assert.strictEqual(result.length, 2)
  })

  it('hasMany sort descending with filter', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'X-task', userId: users[0].id, assigneeId: 1 },
      { text: 'A-task', userId: users[0].id, assigneeId: 1 },
      { text: 'M-task', userId: users[1].id, assigneeId: 1 },
    ])

    // DESC uses MAX: Alice MAX='X-task', Bob MAX='M-task' → Alice first
    const result = await app.service('users').find({
      query: {
        $sort: {
          'todos.text': { direction: -1, filter: { assigneeId: 1 } },
        } as any,
      },
      paginate: false,
    })

    assert.strictEqual(result[0].name, 'Alice')
    assert.strictEqual(result[1].name, 'Bob')
  })

  it('hasMany sort combined with where filter on same relation', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ])

    await app.service('todos').create([
      { text: 'C-todo', userId: users[0].id },
      { text: 'A-todo', userId: users[1].id },
    ])
    // Charlie has no todos

    // Filter to only users who have todos, then sort by todo text
    const result = await app.service('users').find({
      query: {
        todos: { $some: {} },
        $sort: { 'todos.text': 1 },
      },
      paginate: false,
    })

    assert.strictEqual(result.length, 2)
    assert.strictEqual(result[0].name, 'Bob') // A-todo
    assert.strictEqual(result[1].name, 'Alice') // C-todo
  })

  it('hasMany sort on self-referencing relation', async () => {
    const boss = await app.service('users').create({ name: 'Boss', age: 50 })
    await app.service('users').create([
      { name: 'Zara', age: 25, managerId: boss.id },
      { name: 'Aaron', age: 30, managerId: boss.id },
    ])
    await app.service('users').create({ name: 'Lone', age: 40 })

    // Sort by reports' names ascending (MIN)
    const result = await app.service('users').find({
      query: { $sort: { 'reports.name': 1 } },
      paginate: false,
    })

    assert.strictEqual(result.length, 4)
    // Boss has MIN(reports.name)='Aaron', others have NULL
    // Boss should appear among the results with non-null sort value
    const bossIdx = result.findIndex((u: any) => u.name === 'Boss')
    assert.ok(bossIdx >= 0, 'Boss should be in results')
  })

  it('hasMany sort with multiple sort keys on different relations', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Same', userId: users[0].id },
      { text: 'Same', userId: users[1].id },
    ])

    // Both have same todo text, tiebreak by age
    const result = await app.service('users').find({
      query: { $sort: { 'todos.text': 1, age: 1 } },
      paginate: false,
    })

    assert.strictEqual(result.length, 2)
    // Same MIN(text), so tiebreak by age asc → Bob (25) first
    assert.strictEqual(result[0].name, 'Bob')
    assert.strictEqual(result[1].name, 'Alice')
  })

  it('hasMany sort with extended direction strings', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'Z-todo', userId: users[0].id },
      { text: 'A-todo', userId: users[1].id },
    ])

    const result = await app.service('users').find({
      query: { $sort: { 'todos.text': 'asc' } as any },
      paginate: false,
    })

    const res = result as any[]
    assert.strictEqual(res.length, 2)
    // Bob MIN='A-todo' < Alice MIN='Z-todo' → Bob first
    assert.strictEqual(res[0].name, 'Bob')
    assert.strictEqual(res[1].name, 'Alice')
  })

  it('belongsTo sort still uses JOIN (not subquery)', async () => {
    const users = await app.service('users').create([
      { name: 'Zara', age: 30 },
      { name: 'Aaron', age: 25 },
    ])

    await app.service('todos').create([
      { text: 'First', userId: users[0].id },
      { text: 'Second', userId: users[1].id },
    ])

    // belongsTo sort (user.name on todos) should still work via JOIN
    const result = await app.service('todos').find({
      query: { $sort: { 'user.name': 1 } },
      paginate: false,
    })

    assert.strictEqual(result.length, 2)
    // Aaron (user) first
    assert.strictEqual(result[0].userId, users[1].id)
    assert.strictEqual(result[1].userId, users[0].id)
  })
})
