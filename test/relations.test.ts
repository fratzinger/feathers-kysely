import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import assert from 'node:assert'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { describe, it, beforeEach } from 'vitest'

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
  //   log(event) {
  //     console.log(event.query.sql)
  //   }
})

const clean = async () => {
  // drop and recreate the todos table
  await db.schema.dropTable('todos').ifExists().execute()
  await db.schema
    .createTable('todos')
    .addColumn('id', 'integer', (col) => col.primaryKey().autoIncrement())
    .addColumn('text', 'text', (col) => col.notNull())
    .addColumn('userId', 'integer', (col) => col.notNull())
    .execute()

  // drop and recreate the users table
  await db.schema.dropTable('users').ifExists().execute()
  await db.schema
    .createTable('users')
    .addColumn('id', 'integer', (col) => col.primaryKey().autoIncrement())
    .addColumn('name', 'text', (col) => col.notNull())
    .addColumn('age', 'real')
    .addColumn('time', 'real')
    .addColumn('created', 'boolean')
    .execute()
}

const users = new KyselyService<User>({
  Model: db,
  dialectType: 'sqlite',
  name: 'users',
  multi: true,
})

const todos = new KyselyService<Todo>({
  Model: db,
  dialectType: 'sqlite',
  name: 'todos',
  multi: true,
})

// @ts-expect-error TODO: add to options
todos.queryMap = {
  user: {
    service: 'users',
    keyHere: 'userId',
    keyThere: 'id',
    asArray: false,
    db: 'users',
  },
}

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

describe('relations', () => {
  beforeEach(clean)

  it('query for relation', async () => {
    const users = await app.service('users').create([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ])

    const createdTodos = await app.service('todos').create([
      { text: "Alice's first todo", userId: users[0].id },
      { text: "Alice's second todo", userId: users[0].id },
      { text: "Bob's first todo", userId: users[1].id },
    ])

    const aliceTodos = await app.service('todos').find({ query: { 'user.name': 'Alice' }, paginate: false })
    assert.strictEqual(aliceTodos.length, 2)
    assert.ok(aliceTodos.every((todo) => todo.userId === users[0].id))
  })
})
