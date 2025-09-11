import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { defineTestSuite } from 'feathers-adapter-vitest'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { afterAll, beforeAll, describe } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

const testSuite = defineTestSuite({
  '.options': true,
  '.events': true,
  '._get': true,
  '._find': true,
  '._create': true,
  '._update': true,
  '._patch': true,
  '._remove': true,
  '.get': true,
  '.get + $select': true,
  '.get + id + query': true,
  '.get + NotFound': true,
  '.get + id + query id': true,
  '.find': true,
  '.remove': true,
  '.remove + $select': true,
  '.remove + id + query': true,
  '.remove + multi': true,
  '.remove + multi no pagination': true,
  '.remove + id + query id': true,
  '.update': true,
  '.update + $select': true,
  '.update + id + query': true,
  '.update + NotFound': true,
  '.update + query + NotFound': true,
  '.update + id + query id': true,
  '.patch': true,
  '.patch + $select': true,
  '.patch + id + query': true,
  '.patch multiple': true,
  '.patch multiple no pagination': true,
  '.patch multi query same': true,
  '.patch multi query changed': true,
  '.patch + NotFound': true,
  '.patch + query + NotFound': true,
  '.patch + id + query id': true,
  '.create': true,
  '.create ignores query': true,
  '.create + $select': true,
  '.create multi': true,
  'internal .find': true,
  'internal .get': true,
  'internal .create': true,
  'internal .update': true,
  'internal .patch': true,
  'internal .remove': true,
  '.find + equal': true,
  '.find + equal multiple': true,
  '.find + $sort': true,
  '.find + $sort + string': true,
  '.find + $limit': true,
  '.find + $limit 0': true,
  '.find + $skip': true,
  '.find + $select': true,
  '.find + $or': true,
  '.find + $and': true,
  '.find + $in': true,
  '.find + $nin': true,
  '.find + $lt': true,
  '.find + $lte': true,
  '.find + $gt': true,
  '.find + $gte': true,
  '.find + $ne': true,
  '.find + $gt + $lt + $sort': true,
  '.find + $or nested + $sort': true,
  '.find + $and + $or': true,
  'params.adapter + paginate': true,
  'params.adapter + multi': true,
  '.find + paginate': true,
  '.find + paginate + query': true,
  '.find + paginate + $limit + $skip': true,
  '.find + paginate + $limit 0': true,
  '.find + paginate + params': true,
  '.$create': false,
  '.$find': false,
  '.$get': false,
  '.$remove': false,
  '.$update': false,
  '.$patch': false,
  '.id': true,
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
    create: boolean
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
    dialectType: 'sqlite',
    name: 'people',
    events: ['testing'],
  })

  const peopleId = new KyselyService<Person>({
    Model: db,
    id: 'customid',
    dialectType: 'sqlite',
    name: 'people-customid',
    events: ['testing'],
  })

  const users = new KyselyService<Person>({
    Model: db,
    dialectType: 'sqlite',
    name: 'users',
    events: ['testing'],
  })

  const todos = new TodoService({
    dialectType: 'sqlite',
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
