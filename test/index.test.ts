import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { defineTestSuite } from 'feathers-adapter-vitest'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { afterAll, beforeAll, describe } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

const testSuite = defineTestSuite({
  blacklist: [
    '.get + NotFound (string)',
    '.patch + NotFound (string)',
    '.remove + NotFound (string)',
    '.update + NotFound (string)',
  ],
  // only: ['.get + NotFound (integer)'],
})

interface PeopleTable {
  id: Generated<number>
  name: string
  age: number
  time?: string
  created?: boolean
}
interface TodosTable {
  id: Generated<number>
  text: string
  personId: number
}

interface DB {
  todos: TodosTable
  people: PeopleTable
  users: PeopleTable
}

function setup() {
  const db = new Kysely<DB>({
    dialect: dialect(),
  })

  const clean = async () => {
    // drop and recreate the todos table
    await db.schema.dropTable('todos').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('todos')
        .addColumn('text', 'text', (col) => col.notNull())
        .addColumn('personId', 'real', (col) => col.notNull()),
      'id',
    ).execute()

    // drop and create the people table
    await db.schema.dropTable('people').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('people')
        .addColumn('name', 'text', (col) => col.notNull())
        .addColumn('age', 'real')
        .addColumn('time', 'real')
        .addColumn('created', 'boolean'),
      'id',
    ).execute()

    // drop and create the people-customid table
    await db.schema.dropTable('people-customid').ifExists().execute()

    await addPrimaryKey(
      db.schema
        .createTable('people-customid')
        .addColumn('name', 'text', (col) => col.notNull())
        .addColumn('age', 'real')
        .addColumn('time', 'real')
        .addColumn('created', 'boolean'),
      'customid',
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

  type Person = {
    id: number
    name: string
    age: number | null
    time: string
    created: boolean
  }

  type Todo = {
    id: number
    text: string
    personId: number
    personName: string
  }

  type ServiceTypes = {
    people: KyselyService<Person>
    'people-customid': KyselyService<Person>
    users: KyselyService<Person>
    todos: KyselyService<Todo>
  }

  class TodoService extends KyselyService<Todo> {
    // createQuery(params: KyselyAdapterParams<AdapterQuery>) {
    //   const query = super.createQuery(params)
    //   query.join('people as person', 'todos.personId', 'person.id').select('person.name as personName')
    //   return query
    // }
  }

  const people = new KyselyService<Person>({
    Model: db,
    name: 'people',
    events: ['testing'],
  })

  const peopleId = new KyselyService<Person>({
    Model: db,
    id: 'customid',
    name: 'people-customid',
    events: ['testing'],
  })

  const users = new KyselyService<Person>({
    Model: db,
    name: 'users',
    events: ['testing'],
  })

  const todos = new TodoService({
    Model: db,
    name: 'todos',
  })

  const app = feathers<ServiceTypes>()
    .use('people', people)
    .use('people-customid', peopleId)
    .use('users', users)
    .use('todos', todos)

  return {
    db,
    clean,
    people,
    peopleId,
    users,
    todos,
    app,
  }
}

const { app, db, clean } = setup()

describe('Feathers Kysely Service', () => {
  beforeAll(clean)

  afterAll(() => db.destroy())

  testSuite({ app, serviceName: 'users', idProp: 'id' })
  testSuite({ app, serviceName: 'people', idProp: 'id' })
  testSuite({ app, serviceName: 'people-customid', idProp: 'customid' })
})
