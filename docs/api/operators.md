# Query Operators

All standard [Feathers query operators](https://feathersjs.com/api/databases/querying.html) are supported, plus the following:

## Comparison Operators

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

## Pattern Matching

| Operator     | SQL             | Description                                    |
| ------------ | --------------- | ---------------------------------------------- |
| `$like`      | `LIKE`          | Pattern matching                               |
| `$notLike`   | `NOT LIKE`      | Negated pattern matching                       |
| `$iLike`     | `ILIKE` / `LIKE`| Case-insensitive pattern matching (Postgres uses `ILIKE`; MySQL/SQLite fall back to `LIKE`, which is case-insensitive for ASCII) |

## Array Operators (PostgreSQL)

| Operator     | SQL  | Description        |
| ------------ | ---- | ------------------ |
| `$contains`  | `@>` | Array contains     |
| `$contained` | `<@` | Array contained by |
| `$overlap`   | `&&` | Array overlap      |

## Logical Operators

`$and` and `$or` are supported for combining conditions:

```ts
// Users named Alice who are at least 18
await app.service("users").find({
  query: {
    $and: [{ name: "Alice" }, { age: { $gte: 18 } }],
  },
});

// Users named Alice or Bob
await app.service("users").find({
  query: {
    $or: [{ name: "Alice" }, { name: "Bob" }],
  },
});
```

## Querying JSON Columns

Once a column is [declared as `json` or `jsonb`](./service#declaring-column-types),
you can query into it with dot notation. Each segment after the column name is a
key in the JSON document, and all comparison operators work on the resolved
value:

```ts
const service = new KyselyService({
  Model: db,
  name: "events",
  properties: {
    payload: { type: "object", "x-db-type": "jsonb" },
  },
});

// Nested path: payload -> a -> b -> c
await service.find({ query: { "payload.a.b.c": { $gte: 2 } } });

// Top-level key
await service.find({ query: { "payload.name": "John" } });
```

Key segments are always parameterized, so they are safe against injection.

## Querying Dates & Timestamps

By default a date/timestamp query value is passed straight to the database
driver, so what "works" depends on the dialect, the column type, and the value's
JavaScript type. The combinations are inconsistent and some fail outright — for
example an epoch-millisecond number throws on Postgres (no implicit cast), a
`Date` instance throws on SQLite (better-sqlite3 cannot bind it), and a number
silently matches the wrong rows on SQLite/MySQL.

Opt in to **type-aware date coercion** by [declaring a column's temporal
type](./service#declaring-column-types) — either with an `x-db-type` annotation
in `properties` or with `getPropertyType`. The adapter then normalizes any of a
`Date`, an ISO-8601 string, an epoch-millisecond number, or a `"YYYY-MM-DD"`
string into the representation every supported driver compares correctly — a
full ISO string for `timestamp` / `timestamptz` / `datetime` columns, and a
`"YYYY-MM-DD"` string for `date` columns. With it enabled, all four formats
return the same rows on every dialect:

```ts
const service = new KyselyService({
  Model: db,
  name: "events",
  properties: {
    startsAt: { "x-db-type": "timestamptz" },
    day: { "x-db-type": "date" },
  },
});

// All of these are equivalent now:
await service.find({ query: { startsAt: { $gt: new Date("2026-01-15T10:30:00Z") } } });
await service.find({ query: { startsAt: { $gt: "2026-01-15T10:30:00.000Z" } } });
await service.find({ query: { startsAt: { $gt: 1768473000000 } } });

// A "YYYY-MM-DD" value is the right format for a `date` column:
await service.find({ query: { day: { $gte: "2026-01-15" } } });
```

::: tip Notes

- Normalization is done in **UTC**. A `"YYYY-MM-DD"` value against a `timestamp`
  column is interpreted as that day's UTC midnight.
- Coercion is applied to `$lt`, `$lte`, `$gt`, `$gte`, `$eq`, `$ne`, `$in`, and
  `$nin` values; `null` and pattern operators are left untouched.
- It only affects **query** values. Stored values must already be in a comparable
  format (e.g. ISO strings for SQLite text columns).

:::
