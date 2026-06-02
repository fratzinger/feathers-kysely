# Service & Options

## `KyselyService(options)`

A full Feathers service (`find`, `get`, `create`, `update`, `patch`, `remove`) backed by Kysely.

```ts
import { KyselyService } from "@fratzinger/feathers-kysely";

app.use(
  "users",
  new KyselyService<User>({
    Model: db,
    name: "users",
  }),
);
```

## `KyselyAdapter(options)`

The underlying adapter class if you want to build a custom service.

```ts
import { KyselyAdapter } from "@fratzinger/feathers-kysely";

class MyService extends KyselyAdapter {
  // custom methods
}
```

## Options

| Option      | Type                  | Default    | Description                            |
| ----------- | --------------------- | ---------- | -------------------------------------- |
| `Model`     | `Kysely<any>`         | _required_ | The Kysely database instance           |
| `name`      | `string`              | _required_ | The database table name                |
| `id`        | `string`              | `'id'`     | The primary key field                  |
| `multi`     | `boolean \| string[]` | `false`    | Allow multi create/patch/remove        |
| `paginate`  | `object`              | —          | `{ default, max }` pagination settings |
| `operators` | `string[]`            | —          | Additional query operators to allow    |
| `filters`   | `object`              | —          | Additional query filters               |
| `relations` | `object`              | —          | Relation definitions (see [Relations](../relations/setup)) |
| `properties` | `object`             | —          | Map of column name → JSON schema property object (typically your service's schema `properties`). Used as the set of known columns and as a declarative source for a column's database type via an [`x-db-type`](#declaring-column-types) annotation |
| `getPropertyType` | `function`       | —          | Resolve a column's type. Returns `'json'`/`'jsonb'` for JSON columns, or a temporal type (`'date'`, `'timestamp'`, `'timestamptz'`, `'datetime'`) to enable [date coercion](./operators#querying-dates-timestamps). Takes precedence over `x-db-type` |

## Declaring column types

Two features need to know a column's underlying database type:

- **JSON columns** — to query into a `json`/`jsonb` column with [dot notation](./operators#querying-json-columns).
- **Temporal columns** — to enable [type-aware date coercion](./operators#querying-dates-timestamps).

You can declare the type in either of two ways. The declarative `x-db-type`
annotation lives on the column's entry in `properties` (which is typically your
service's JSON schema `properties` block), so the type sits next to the field
definition:

```ts
new KyselyService({
  Model: db,
  name: "events",
  properties: {
    id: true,
    startsAt: { type: "string", format: "date-time", "x-db-type": "timestamptz" },
    day: { type: "string", "x-db-type": "date" },
    payload: { type: "object", "x-db-type": "jsonb" },
  },
});
```

The imperative `getPropertyType` function is the alternative (and escape hatch).
It takes precedence over `x-db-type`; return `undefined` to fall back to the
annotation:

```ts
new KyselyService({
  Model: db,
  name: "events",
  getPropertyType: (property) => {
    if (property === "startsAt") return "timestamptz";
    if (property === "payload") return "jsonb";
  },
});
```

Recognized types are `'json'`, `'jsonb'`, `'date'`, `'timestamp'`,
`'timestamptz'`, and `'datetime'`.
