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
  }

  interface UsersTable {
    id: Generated<number>
    name: string
    age: number | null
    time?: number | null
    created: boolean | null
  }

  interface DB {
    todos: TodosTable
    users: UsersTable
  }

  const db = new Kysely<DB>({
    dialect: dialect(),
    // log(event) {
    //   console.log(event.query.sql)
    // },
  })

  const clean = async () => {
    // drop and recreate the todos table
    await db.schema.dropTable('todos').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('todos')
        .addColumn('text', 'text', (col) => col.notNull())
        .addColumn('userId', 'integer', (col) => col.notNull()),
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
        .addColumn('created', 'boolean'),
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
    },
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

  const todos = new KyselyService<Todo>({
    Model: db,
    name: 'todos',
    multi: true,
    properties: {
      id: true,
      text: true,
      userId: true,
    },
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

  type User = {
    id: number
    name: string
    age: number | null
    time: string
    create: boolean
  }

  type Todo = {
    id: number
    text: string
    userId: number
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

  it('query for belongsTo', async () => {
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

  it('query for hasMany 1', async () => {
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

  it('query for hasMany 2', async () => {
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
