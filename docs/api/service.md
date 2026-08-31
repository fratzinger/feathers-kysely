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

## Skipping the return value

The mutating methods (`create`, `update`, `patch`, `remove`) return the written
rows by default. For fire-and-forget writes you can opt out with
`params.kysely.returning: false`:

```ts
await app.service("logs").create(rows, { kysely: { returning: false } });
```

The adapter then skips the `RETURNING` clause and every post-fetch. A single
call resolves to `undefined`, a multi call to `[]` — and the emitted Feathers
event (`created`/`patched`/`updated`/`removed`) carries that same empty payload.

| Call                                                                 | Resolves to |
| -------------------------------------------------------------------- | ----------- |
| `create(data)`, `update(id, data)`, `patch(id, data)`, `remove(id)`   | `undefined` |
| `create([...])`, `patch(null, data)`, `remove(null)`                  | `[]`        |

`NotFound` is still enforced: a single `update`, `patch` or `remove` that matches
no row rejects as usual. Existence is derived from the statement's affected-row
count on PostgreSQL/SQLite, and from the pre-fetch MySQL performs anyway — so
you keep the error semantics without paying for the read.

For `create`, `returning: false` also overrides
[`onConflictReturning`](../guides/upsert#controlling-the-returned-rows),
forcing `'none'`.

::: tip What it actually saves
- **`create`** — the entire read is gone. The biggest win is on MySQL, which has
  no `RETURNING` and otherwise needs a separate `SELECT` round-trip to read the
  inserted rows back.
- **`patch` / `remove`** — the post-fetch is gone. On MySQL the pre-fetch
  `SELECT` that builds the `WHERE` clause still runs, so only one of the two
  round-trips is saved there.
- **`update`** — replaces a row, so it always reads the existing row first (only
  the id when [`properties`](#options) is configured). Only the post-fetch is
  saved.
:::

::: warning TypeScript
The service types still declare `Promise<Result>`. With `returning: false` a
single mutation resolves to `undefined` at runtime.
:::
