import type { Generated } from 'kysely'
import { Kysely } from 'kysely'
import assert from 'node:assert'
import { feathers } from '@feathersjs/feathers'
import dialect from './dialect.js'

import { KyselyService } from '../src/index.js'
import { afterAll, beforeEach, describe, it } from 'vitest'
import { addPrimaryKey } from './test-utils.js'

/**
 * A relation chain is walked at query time through `app.service(name)`. These
 * tests cover what happens when that lookup cannot produce relations: no app,
 * an unregistered service, or a service that is not a KyselyService. In every
 * case the chain is unresolvable and must be rejected, never silently dropped.
 */

interface UsersTable {
  id: Generated<number>
  name: string
  managerId: number | null
}

interface DB {
  users: UsersTable
}

const db = new Kysely<DB>({ dialect: dialect() })

const relations = (service: string) => ({
  manager: {
    service,
    keyHere: 'managerId',
    keyThere: 'id',
    asArray: false,
    databaseTableName: 'users',
  },
})

const makeService = (service: string) =>
  new KyselyService<any>({
    Model: db,
    name: 'users',
    multi: true,
    properties: { id: true, name: true, managerId: true },
    relations: relations(service),
  })

const expectBadRequest = (promise: Promise<any>) =>
  assert.rejects(promise, (error: any) => {
    assert.strictEqual(error.name, 'BadRequest')
    return true
  })

const clean = async () => {
  await db.schema.dropTable('users').ifExists().execute()
  await addPrimaryKey(
    db.schema
      .createTable('users')
      .addColumn('name', 'text', (col) => col.notNull())
      .addColumn('managerId', 'integer'),
    'id',
  ).execute()
}

describe('relation resolution', () => {
  beforeEach(clean)
  afterAll(() => db.destroy())

  it('without app.setup(): one hop works, deeper chains are rejected', async () => {
    // No app is ever handed to the service, so `lookupRelationsForService`
    // cannot resolve the relations of the hop's target.
    const users = makeService('users')

    const alice = await users.create({ name: 'Alice' })
    await users.create({ name: 'Bob', managerId: alice.id })

    // One hop only needs this service's own relation definition
    const oneHop = await users.find({
      query: { 'manager.name': 'Alice' },
      paginate: false,
    })
    assert.deepStrictEqual(
      oneHop.map((user: any) => user.name),
      ['Bob'],
    )

    await expectBadRequest(
      users.find({
        query: { 'manager.manager.name': 'Alice' },
        paginate: false,
      }),
    )
  })

  it('relation pointing at an unregistered service is rejected', async () => {
    const users = makeService('nope')
    const app = feathers<any>().use('users', users)
    await app.setup()

    const alice = await app.service('users').create({ name: 'Alice' })
    await app.service('users').create({ name: 'Bob', managerId: alice.id })

    // The first hop still resolves from `databaseTableName`
    const oneHop = await app.service('users').find({
      query: { 'manager.name': 'Alice' },
      paginate: false,
    })
    assert.strictEqual(oneHop.length, 1)

    // The second hop needs app.service('nope'), which throws
    await expectBadRequest(
      app.service('users').find({
        query: { 'manager.manager.name': 'Alice' },
        paginate: false,
      }),
    )
  })

  it('relation pointing at a non-Kysely service is rejected', async () => {
    const users = makeService('plain')
    const app = feathers<any>()
      .use('users', users)
      // A service without adapter `options`, so it exposes no relations
      .use('plain', {
        async find() {
          return []
        },
      })
    await app.setup()

    const alice = await app.service('users').create({ name: 'Alice' })
    await app.service('users').create({ name: 'Bob', managerId: alice.id })

    await expectBadRequest(
      app.service('users').find({
        query: { 'manager.manager.name': 'Alice' },
        paginate: false,
      }),
    )
  })
})
