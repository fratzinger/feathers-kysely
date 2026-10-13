# Update Operators

MongoDB-style atomic update operators for `patch`, provided by the
`updateOperators()` hook. Unlike [query operators](/api/operators) (which build
`WHERE` clauses), these build `SET` clauses — they modify a column based on its
current value instead of overwriting it, computed in the database in a single
statement.

| Operator    | SQL                            | Description                           |
| ----------- | ------------------------------ | ------------------------------------- |
| `$inc`      | `col = col + value`            | Increment (or decrement)              |
| `$mul`      | `col = col * value`            | Multiply                              |
| `$min`      | `col = least(col, value)`\*    | Keep the smaller of current and value |
| `$max`      | `col = greatest(col, value)`\* | Keep the larger of current and value  |
| `$push`     | append to an array column      | Add element(s) to an array            |
| `$pull`     | remove from an array column    | Remove element(s) from an array       |
| `$addToSet` | append if not yet present      | Add element(s) an array lacks, once   |

\* `$min` / `$max` compile to a portable `CASE` expression
(`case when col is null or col > value then value else col end`) rather than
`LEAST`/`GREATEST`, so they work identically on PostgreSQL, MySQL and SQLite. A
`NULL` column is treated as "no value yet" and initialized to `value`.

## Setup

`updateOperators()` is an opt-in hook. Register it on `patch` — and on `update`
too, so misuse there fails loudly (see [below](#patch-only)). It works as a
`before` **or** an `around` hook:

```ts
import { updateOperators } from "@fratzinger/feathers-kysely";

app.service("users").hooks({
  before: {
    patch: [updateOperators()],
    update: [updateOperators()],
  },
});
```

## Usage

`$inc` increments a column atomically (a negative value decrements):

```ts
// SET views = views + 1
await app.service("articles").patch(id, { $inc: { views: 1 } });

// SET stock = stock - 5
await app.service("products").patch(id, { $inc: { stock: -5 } });
```

`$mul` multiplies a column:

```ts
// SET price = price * 2
await app.service("products").patch(id, { $mul: { price: 2 } });
```

`$min` / `$max` clamp a column to the smaller / larger of its current value and
the given value — useful for low-water / high-water marks (lowest price seen,
highest score). A `NULL` column is initialized to the value:

```ts
// only lowers the floor, never raises it
await app.service("products").patch(id, { $min: { lowestPrice: 9.99 } });

// only raises the high score, never lowers it
await app.service("games").patch(id, { $max: { highScore: 4200 } });
```

Operators combine with each other and with plain values in a single patch:

```ts
// SET name = 'Sale', price = price * 0.8, sold = sold + 1
await app.service("products").patch(id, {
  name: "Sale",
  $mul: { price: 0.8 },
  $inc: { sold: 1 },
});
```

Because the work happens in the database, it is safe under concurrency — two
simultaneous `{ $inc: { views: 1 } }` patches both count, with no read-modify-write
race. It also applies to **multi-patch** (every matched row is updated):

```ts
// every active user's loginCount goes up by 1
await app
  .service("users")
  .patch(null, { $inc: { loginCount: 1 } }, { query: { active: true } });
```

## Array operators (`$push` / `$pull` / `$addToSet`)

`$push` appends to an array column, `$pull` removes matching elements, and
`$addToSet` appends only the elements the array doesn't contain yet. A scalar
value affects one element; an array value affects each:

```ts
// append one tag, or several
await app.service("posts").patch(id, { $push: { tags: "kysely" } });
await app.service("posts").patch(id, { $push: { tags: ["sql", "node"] } });

// remove one value (all occurrences), or several
await app.service("posts").patch(id, { $pull: { tags: "draft" } });
await app.service("posts").patch(id, { $pull: { tags: ["draft", "wip"] } });

// tags ["sql"] -> ["sql", "node"]: "sql" is already there, "node" is added once
await app
  .service("posts")
  .patch(id, { $addToSet: { tags: ["sql", "node", "node"] } });
```

`$addToSet` keeps the input order, adds each new value only once, and never
removes duplicates the array already has. Values are compared exactly: on
`jsonb`, `{ "a": 1 }` and `{ "a": 1, "b": 2 }` are different elements.

A `NULL` column is treated like a missing field in MongoDB: `$push` and
`$addToSet` start a new array, `$pull` leaves it `NULL`.

::: info Difference from MongoDB
In MongoDB, an array value pushes the array as **one** nested element, and
`{ $each: [...] }` appends several. Here an array value always means "each
element" (like Prisma's `push`), since a nested array can't go into a native
`text[]` column anyway. For the same reason `$pull` with an array removes each
value, which MongoDB calls `$pullAll`.
:::

### Column type detection

The SQL differs completely between a **native Postgres array** (`text[]`,
`integer[]`, …) and a **`json` / `jsonb`** column, so the operator must know the
column's storage. It resolves the type per column the same way the query
operators do — from a `getPropertyType` option, or an `x-db-type` annotation in
the service's `properties`:

```ts
new KyselyService({
  Model: db,
  name: "posts",
  properties: {
    tags: { type: "array", "x-db-type": "text[]" }, // native Postgres array
    labels: { type: "array", "x-db-type": "jsonb" }, // jsonb array
  },
});
```

If a column's storage can't be determined, the array operators throw a
`BadRequest` rather than guessing.

### Support matrix

| Column storage        | `$push`          | `$pull`              | `$addToSet`                  |
| --------------------- | ---------------- | -------------------- | ---------------------------- |
| Postgres native array | ✅ Postgres only | ✅ Postgres only     | ✅ Postgres only             |
| `json` / `jsonb`      | ✅ all dialects  | ✅ **Postgres only** | ✅ Postgres, SQLite (≥ 3.44) |

- Native array ops use `array_append` / `array_remove` / `||` and exist only on
  PostgreSQL (other dialects throw `BadRequest`).
- JSON `$push` works on all dialects (`||` on Postgres, `json_array_append` on
  MySQL, `json_insert` on SQLite).
- JSON `$pull` is **Postgres-only** (a `jsonb_agg` filter); on MySQL / SQLite it
  throws `BadRequest`, as it can't be expressed as a single statement.
- JSON `$addToSet` works on Postgres and SQLite. SQLite rebuilds the array with
  `json_group_array(... order by ...)`, which needs SQLite 3.44 or newer, and
  compares elements by their JSON text, so `{"a":1,"b":2}` and `{"b":2,"a":1}`
  count as different there. On MySQL it throws `BadRequest`.

## patch only {#patch-only}

Operators are only meaningful for `patch` (a partial update). `update` replaces
the entire record, so an operator there is almost certainly a mistake — using one
on `update` throws a `BadRequest`. Register the hook on `update` as well to get
that clear error rather than a confusing database failure.

## Validation

Invalid payloads throw a `BadRequest`:

- the operator value must be an object (`{ $inc: 5 }` is rejected);
- for `$inc` / `$mul` / `$min` / `$max`, each target value must be a finite
  number (`{ $inc: { age: "x" } }`, `NaN`, and `Infinity` are rejected);
- for `$push` / `$pull` / `$addToSet`, the value must be defined, and the
  column's storage must be detectable (see [above](#column-type-detection)).

## TypeScript

`PatchData<T>` is `Partial<T>`, so the operator keys are not part of the static
type. Cast at the call site when needed:

```ts
await app.service("products").patch(id, { $inc: { stock: -1 } } as any);
```

## Dialects

`$inc` / `$mul` / `$min` / `$max` work on all dialects (PostgreSQL, MySQL,
SQLite) — the generated arithmetic / `CASE` is standard SQL, and every value is
bound as a parameter. `$push` / `$pull` / `$addToSet` are dialect- and
storage-specific; see the [support matrix](#support-matrix) above.
