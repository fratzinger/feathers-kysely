import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import { feathers } from '@feathersjs/feathers'
import { defineTestSuite } from 'feathers-adapter-vitest'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { afterAll, beforeAll, describe } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

const all = true

const testSuite = defineTestSuite({
  '.options': all,
  '.events': all,
  '._get': all,
  '._find': all,
  '._create': all,
  '._update': all,
  '._patch': all,
  '._remove': all,
  '.get': all,
  '.get + $select': all,
  '.get + id + query': all,
  '.get + NotFound': all,
  '.get + id + query id': all,
  '.find': all,
  '.remove': all,
  '.remove + $select': all,
  '.remove + id + query': all,
  '.remove + multi': all,
  '.remove + multi no pagination': all,
  '.remove + id + query id': all,
  '.update': all,
  '.update + $select': all,
  '.update + id + query': all,
  '.update + NotFound': all,
  '.update + query + NotFound': all,
  '.update + id + query id': all,
  '.patch': all,
  '.patch + $select': all,
  '.patch + id + query': all,
  '.patch multiple': all,
  '.patch multiple no pagination': all,
  '.patch multi query same': all,
  '.patch multi query changed': all,
  '.patch + NotFound': all,
  '.patch + query + NotFound': all,
  '.patch + id + query id': all || true,
  '.create': all,
  '.create ignores query': all,
  '.create + $select': all,
  '.create multi': all,
  'internal .find': all,
  'internal .get': all,
  'internal .create': all,
  'internal .update': all,
  'internal .patch': all,
  'internal .remove': all,
  '.find + equal': all,
  '.find + equal multiple': all,
  '.find + $sort': all,
  '.find + $sort + string': all,
  '.find + $limit': all,
  '.find + $limit 0': all,
  '.find + $skip': all,
  '.find + $select': all,
  '.find + $or': all,
  '.find + $and': all,
  '.find + $in': all,
  '.find + $nin': all,
  '.find + $lt': all,
  '.find + $lte': all,
  '.find + $gt': all,
  '.find + $gte': all,
  '.find + $ne': all,
  '.find + $gt + $lt + $sort': all,
  '.find + $or nested + $sort': all,
  '.find + $and + $or': all,
  'params.adapter + paginate': all,
  'params.adapter + multi': all,
  '.find + paginate': all,
  '.find + paginate + query': all,
  '.find + paginate + $limit + $skip': all,
  '.find + paginate + $limit 0': all,
  '.find + paginate + params': all,
  '.$create': false,
  '.$find': false,
  '.$get': false,
  '.$remove': false,
  '.$update': false,
  '.$patch': false,
  '.id': all,
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
