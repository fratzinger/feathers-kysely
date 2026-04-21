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

If you plan to query or sort by [multi-level belongsTo chains](./belongs-to#multi-level-chains) (e.g. `event.assignment.customer.fullName`), the adapter needs a reference to the Feathers app so it can resolve relation definitions on _other_ services. There are two ways to provide it:

### 1. Automatic (via `app.setup()`)

Feathers calls `service.setup(app, path)` automatically when you start the server with `app.listen()`. In tests or programmatic usage without `listen()`, invoke it explicitly:

```ts
await app.setup();
```

### 2. Explicit (via the constructor)

You can pass the app as a second constructor argument — useful in tests or when you want the adapter fully wired before the Feathers lifecycle runs:

```ts
const events = new KyselyService<Event>(
  {
    Model: db,
    name: "events",
    relations: {
      /* ... */
    },
  },
  app,
);
app.use("events", events);
```

If both are provided, the constructor argument takes precedence. Single-level queries work without either.
