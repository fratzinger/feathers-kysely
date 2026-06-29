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

| Operator     | SQL                    | Description                                    |
| ------------ | ---------------------- | ---------------------------------------------- |
| `$like`      | `LIKE`                 | Pattern matching                               |
| `$notLike`   | `NOT LIKE`             | Negated pattern matching                       |
| `$iLike`     | `ILIKE` / `LIKE`       | Case-insensitive pattern matching (Postgres uses `ILIKE`; MySQL/SQLite fall back to `LIKE`, which is case-insensitive for ASCII) |
| `$notILike`  | `NOT ILIKE` / `NOT LIKE` | Negated case-insensitive pattern matching (same dialect fallback as `$iLike`) |

With `$like` and friends you supply the wildcards (`%`, `_`) yourself:

```ts
await app.service("users").find({ query: { name: { $like: "A%" } } });
await app.service("users").find({ query: { email: { $notILike: "%@spam.com" } } });
```

### `$startsWith` / `$endsWith`

Convenience prefix/suffix matching. The value is matched **literally** — any
`%`, `_`, or `\` in it is escaped — and a single wildcard is appended/prepended.
Compiles to `LIKE 'value%' ESCAPE '\'` / `LIKE '%value' ESCAPE '\'`. Available on
all dialects (case-sensitive, except SQLite's `LIKE` is ASCII-case-insensitive).

```ts
// name LIKE 'Jo%'
await app.service("users").find({ query: { name: { $startsWith: "Jo" } } });

// name LIKE '%son' — a value of "10%" matches the literal "10%", not a wildcard
await app.service("users").find({ query: { name: { $endsWith: "son" } } });
```

## Range Operators

| Operator      | SQL           | Description                          |
| ------------- | ------------- | ----------------------------------- |
| `$between`    | `BETWEEN`     | Value is within `[min, max]` (inclusive) |
| `$notBetween` | `NOT BETWEEN` | Value is outside `[min, max]`       |

The value must be a `[min, max]` tuple; both bounds are inclusive. Available on
all dialects.

```ts
// age BETWEEN 18 AND 65
await app.service("users").find({ query: { age: { $between: [18, 65] } } });

await app.service("users").find({
  query: { createdAt: { $notBetween: ["2026-01-01", "2026-02-01"] } },
});
```

## Regular Expressions

| Operator     | SQL (Postgres) | SQL (MySQL)  | Description       |
| ------------ | -------------- | ------------ | ----------------- |
| `$regex`     | `~`            | `REGEXP`     | Matches a regex   |
| `$notRegex`  | `!~`           | `NOT REGEXP` | Does not match    |

::: warning Dialect support
`$regex` / `$notRegex` are available on **PostgreSQL and MySQL only**. SQLite has
no built-in `REGEXP`, so these operators are rejected there with a `BadRequest`
rather than emitting SQL that fails at runtime.
:::

```ts
// PostgreSQL: name ~ '^Jo.*n$'
await app.service("users").find({ query: { name: { $regex: "^Jo.*n$" } } });
```

## Array Operators (PostgreSQL)

| Operator     | SQL  | Description        |
| ------------ | ---- | ------------------ |
| `$contains`  | `@>` | Array contains     |
| `$contained` | `<@` | Array contained by |
| `$overlap`   | `&&` | Array overlap      |

These work on native array columns (`text[]`, `integer[]`, …) and on `json` /
`jsonb` array columns. For non-`text[]`/`integer[]` element types, declare the
column's array type (e.g. `"x-db-type": "varchar[]"`) so the literal is cast to
the column's exact element type — see [Declaring column types](./service#declaring-column-types).

```ts
await app.service("posts").find({ query: { tags: { $contains: ["news"] } } });
```

## JSON Key Existence (PostgreSQL)

For `json` / `jsonb` columns, test whether top-level keys (or array string
elements) exist. **PostgreSQL only** — rejected with a `BadRequest` on other
dialects.

| Operator      | SQL                  | Description                       |
| ------------- | -------------------- | --------------------------------- |
| `$hasKey`     | `jsonb_exists`       | The key exists                    |
| `$hasKeyAny`  | `jsonb_exists_any`   | **Any** of the listed keys exists |
| `$hasKeyAll`  | `jsonb_exists_all`   | **All** of the listed keys exist  |

```ts
// payload ? 'userId'
await app.service("events").find({ query: { payload: { $hasKey: "userId" } } });

// payload ?| array['a','b']
await app.service("events").find({
  query: { payload: { $hasKeyAny: ["a", "b"] } },
});
```

## Relation Operators

`$some`, `$none`, and `$every` filter parent records by conditions on their
`hasMany` children (`EXISTS` / `NOT EXISTS` subqueries). See
[Querying Relations → hasMany](../relations/querying#hasmany).

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

### `$not`

`$not` negates an entire condition object at the database level — it compiles to
`NOT (...)` around whatever the inner query produces. It is **operator-agnostic**:
the inner condition can use any operator, nested `$and` / `$or`, or multiple keys.

```ts
// NOT (age = 20)
await app.service("users").find({ query: { $not: { age: 20 } } });

// NOT (age > 15) — works with any operator, not just equality
await app.service("users").find({ query: { $not: { age: { $gt: 15 } } } });

// De Morgan: NOT (age = 10 OR age = 20) === age != 10 AND age != 20
await app.service("users").find({
  query: { $not: { $or: [{ age: 10 }, { age: 20 }] } },
});
```

Because the whole object is negated as a unit, a **multi-key** condition negates
the conjunction — `$not: { age: 20, name: "b" }` is `NOT (age = 20 AND name = "b")`,
not a per-property inversion. An empty `$not: {}` is a no-op.

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
