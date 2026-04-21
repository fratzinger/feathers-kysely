# Relation Setup

Relations are defined in the service options using the `relations` property. The convention follows [feathers-graph-populate](https://github.com/marshallswain/feathers-graph-populate).

## Configuration

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

## Relation Options

| Option            | Type      | Description                                              |
| ----------------- | --------- | -------------------------------------------------------- |
| `service`         | `string`  | The name of the related Feathers service                 |
| `keyHere`         | `string`  | The local column that references the related table       |
| `keyThere`        | `string`  | The column in the related table that matches `keyHere`   |
| `asArray`         | `boolean` | `true` for hasMany (one-to-many), `false` for belongsTo |
| `databaseTableName` | `string` | The actual database table name of the related entity    |

## Relation Types

### belongsTo (`asArray: false`)

A record belongs to one related record. The foreign key is on the current table.

Example: A user **belongs to** a manager (another user).

```
users.managerId → users.id
```

See [belongsTo](./belongs-to) for details.

### hasMany (`asArray: true`)

A record has many related records. The foreign key is on the related table.

Example: A user **has many** todos.

```
users.id ← todos.userId
```

See [hasMany](./has-many) for details.

## App Setup

If you plan to query or sort by [multi-level belongsTo chains](./belongs-to#multi-level-chains) (e.g. `event.assignment.customer.fullName`), make sure `app.setup()` runs before the first query. Feathers calls it automatically when you start the server via `app.listen()`. In tests or programmatic usage without `listen()`, invoke it explicitly:

```ts
await app.setup();
```

This is how the adapter discovers the relation definitions of _other_ services when resolving a chained path. Single-level queries work without it.
