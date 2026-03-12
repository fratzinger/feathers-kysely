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
| `properties` | `object`             | —          | Column property definitions            |
| `getPropertyType` | `function`       | —          | Function to resolve property types (e.g. for JSON columns) |
