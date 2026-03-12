# @fratzinger/feathers-kysely

[![npm](https://img.shields.io/npm/v/@fratzinger/feathers-kysely)](https://www.npmjs.com/package/@fratzinger/feathers-kysely)
[![Download Status](https://img.shields.io/npm/dm/@fratzinger/feathers-kysely.svg?style=flat-square)](https://www.npmjs.com/package/@fratzinger/feathers-kysely)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label)](https://discord.gg/qa8kez8QBx)

> A [FeathersJS](https://feathersjs.com/) database adapter for [Kysely](https://kysely.dev/) — the type-safe SQL query builder.

Supports **PostgreSQL**, **MySQL**, **SQLite**, and **MSSQL**.

## Installation

```bash
npm install @fratzinger/feathers-kysely kysely
```

You also need a database driver for your dialect:

```bash
# PostgreSQL
npm install pg

# MySQL
npm install mysql2

# SQLite
npm install better-sqlite3
```

## Usage

```ts
import { feathers } from '@feathersjs/feathers'
import { Kysely, Generated } from 'kysely'
import { KyselyService } from '@fratzinger/feathers-kysely'

// 1. Define your database types
interface UsersTable {
  id: Generated<number>
  name: string
  age: number
}

interface DB {
  users: UsersTable
}

type User = { id: number; name: string; age: number }

// 2. Create a Kysely instance
const db = new Kysely<DB>({
  dialect: /* your dialect */,
})

// 3. Register the service
const app = feathers()
  .use('users', new KyselyService<User>({
    Model: db,
    name: 'users', // table name
    id: 'id',      // primary key (default: 'id')
    multi: true,    // allow multi create/patch/remove
    paginate: { default: 10, max: 100 },
  }))

// 4. Use the service
const user = await app.service('users').create({ name: 'Alice', age: 30 })
const users = await app.service('users').find({ query: { age: { $gte: 18 } } })
```

## API

### `KyselyService(options)`

A full Feathers service (`find`, `get`, `create`, `update`, `patch`, `remove`) backed by Kysely.

### `KyselyAdapter(options)`

The underlying adapter class if you want to build a custom service.

#### Options

| Option      | Type                  | Default    | Description                            |
| ----------- | --------------------- | ---------- | -------------------------------------- |
| `Model`     | `Kysely<any>`         | _required_ | The Kysely database instance           |
| `name`      | `string`              | _required_ | The database table name                |
| `id`        | `string`              | `'id'`     | The primary key field                  |
| `multi`     | `boolean \| string[]` | `false`    | Allow multi create/patch/remove        |
| `paginate`  | `object`              | —          | `{ default, max }` pagination settings |
| `operators` | `string[]`            | —          | Additional query operators to allow    |
| `filters`   | `object`              | —          | Additional query filters               |

## Query Operators

All standard [Feathers query operators](https://feathersjs.com/api/databases/querying.html) are supported, plus:

| Operator     | SQL             | Description                                    |
| ------------ | --------------- | ---------------------------------------------- |
| `$lt`        | `<`             | Less than                                      |
| `$lte`       | `<=`            | Less than or equal                             |
| `$gt`        | `>`             | Greater than                                   |
| `$gte`       | `>=`            | Greater than or equal                          |
| `$in`        | `IN`            | In a list of values                            |
| `$nin`       | `NOT IN`        | Not in a list of values                        |
| `$eq`        | `=` / `IS`      | Equal (handles `null`)                         |
| `$ne`        | `!=` / `IS NOT` | Not equal (handles `null`)                     |
| `$like`      | `LIKE`          | Pattern matching                               |
| `$notLike`   | `NOT LIKE`      | Negated pattern matching                       |
| `$iLike`     | `ILIKE`         | Case-insensitive pattern matching (PostgreSQL) |
| `$contains`  | `@>`            | Array contains (PostgreSQL)                    |
| `$contained` | `<@`            | Array contained by (PostgreSQL)                |
| `$overlap`   | `&&`            | Array overlap (PostgreSQL)                     |

Logical operators `$and` and `$or` are also supported.

## Upsert

The service provides an `upsert` method using `ON CONFLICT` (PostgreSQL/SQLite) or `ON DUPLICATE KEY UPDATE` (MySQL):

```ts
const result = await app.service("users").upsert(
  { name: "Alice", age: 31 },
  {
    onConflictFields: ["name"],
    onConflictAction: "merge", // 'merge' (default) or 'ignore'
    // onConflictMergeFields: ['age'],     // specific fields to update
    // onConflictExcludeFields: ['name'],  // fields to exclude from update
  },
);
```

## Transactions

Transaction hooks are provided to wrap service calls in database transactions using Kysely's `ControlledTransaction` API.

### Using hooks

```ts
import { trxStart, trxCommit, trxRollback } from "@fratzinger/feathers-kysely";

app.service("users").hooks({
  before: {
    create: [trxStart()],
  },
  after: {
    create: [trxCommit()],
  },
  error: {
    create: [trxRollback()],
  },
});
```

### Using params directly

You can also manage transactions manually by passing a `transaction` object in `params`:

```ts
import type { KyselyAdapterTransaction } from "@fratzinger/feathers-kysely";

const trx = await db.startTransaction().execute();

const transaction: KyselyAdapterTransaction = {
  trx,
  id: Date.now(),
  starting: false,
};

try {
  await app
    .service("users")
    .create({ name: "Alice", age: 30 }, { transaction });
  await app
    .service("posts")
    .create({ title: "Hello", userId: 1 }, { transaction });
  await trx.commit().execute();
} catch (error) {
  await trx.rollback().execute();
  throw error;
}
```

Nested transactions (savepoints) are supported by passing a transaction's `trx` to `startTransaction()` again.

## Relations

Define relations between services to enable powerful query operators and sorting across related tables.

### Configuration

```ts
const app = feathers().use(
  "users",
  new KyselyService<User>({
    Model: db,
    name: "users",
    id: "id",
    relations: {
      todos: {
        service: "todos",
        keyHere: "id",
        keyThere: "userId",
        asArray: true,
        databaseTableName: "todos",
      },
      manager: {
        service: "users",
        keyHere: "managerId",
        keyThere: "id",
        asArray: false,
        databaseTableName: "users",
      },
    },
  }),
);
```

| Relation Option     | Type      | Description                                                |
| ------------------- | --------- | ---------------------------------------------------------- |
| `service`           | `string`  | The name of the related Feathers service                   |
| `keyHere`           | `string`  | The local column that references the related table         |
| `keyThere`          | `string`  | The column in the related table that matches `keyHere`     |
| `asArray`           | `boolean` | `true` for hasMany (one-to-many), `false` for belongsTo   |
| `databaseTableName` | `string`  | The actual database table name of the related entity       |

### Filtering with `$none`, `$some`, `$every`

For `asArray: true` (hasMany) relations, you can filter parent records based on conditions on their children:

- **`$some`** — at least one related record matches the filter
- **`$none`** — no related record matches the filter
- **`$every`** — all related records match the filter

```ts
// Users who have at least one todo with text 'A-todo'
await app.service("users").find({
  query: { todos: { $some: { text: "A-todo" } } },
});

// Users who have no completed todos
await app.service("users").find({
  query: { todos: { $none: { completed: true } } },
});

// Users where every todo is completed
await app.service("users").find({
  query: { todos: { $every: { completed: true } } },
});
```

### Sorting by hasMany Relations

You can sort parent records by a column in a hasMany relation using dot notation in `$sort`. The adapter uses a subquery with `MIN()` (ascending) or `MAX()` (descending) to aggregate the related values without duplicating rows.

```ts
// Sort users by the MIN of their todos' text (ascending)
await app.service("users").find({
  query: { $sort: { "todos.text": 1 } },
});

// Sort users by the MAX of their todos' text (descending)
await app.service("users").find({
  query: { $sort: { "todos.text": -1 } },
});
```

You can also filter which related records are considered for sorting by using the extended form:

```ts
// Sort users by the MIN text of only their todos assigned to user 1
await app.service("users").find({
  query: {
    $sort: {
      "todos.text": { direction: 1, filter: { assigneeId: 1 } },
    },
  },
});
```

All standard sort directions are supported: `1`, `-1`, `'asc'`, `'desc'`, `'asc nulls first'`, `'asc nulls last'`, `'desc nulls first'`, `'desc nulls last'`.

## Custom Queries

The underlying Kysely instance is available at `service.Model`. Note that it is the full `Kysely` instance, not scoped to a table — so provide the table name in each query:

```ts
const service = app.service("users");

const results = await service.Model.selectFrom("users")
  .select(["id", "name"])
  .where("age", ">", 18)
  .execute();
```

## License

Copyright (c) 2026 [Feathers contributors](https://github.com/feathersjs/feathers/graphs/contributors)

Licensed under the [MIT license](LICENSE).
