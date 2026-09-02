# Sorting by Relations

You can sort parent records by columns in related tables using dot notation in `$sort`.

## belongsTo Sorting

```ts
// Sort users by their manager's name
await app.service("users").find({
  query: { $sort: { "manager.name": 1 } },
});
```

For belongsTo (`asArray: false`) relations, sorting uses a `LEFT JOIN` — but only
when the adapter can *prove* the hop matches at most one row, which is the case
when `keyThere` is the target service's `id`. That covers how belongsTo is
declared in practice, and no aggregation is needed there.

`asArray: false` is a statement of intent, not a guarantee: nothing stops a
relation from pointing at a non-unique column. A `LEFT JOIN` on one would
multiply the parent rows and inflate the paginated `total`, so such a hop is
resolved through an aggregate instead — see
[Non-unique to-one relations](#non-unique-to-one-relations).

### Multi-level belongsTo

You can sort by a column reached through any number of belongsTo hops:

```ts
// Sort events by their assignment's customer's full name
await app.service("events").find({
  query: { $sort: { "assignment.customer.fullName": 1 } },
});
```

Requires `app.setup()` to have run so the adapter can look up related services — see [Setup → App Setup](./setup#app-setup). For more on chained paths, see [belongsTo → Multi-level chains](./belongs-to#multi-level-chains).

## hasMany Sorting

For hasMany (`asArray: true`) relations, sorting uses an aggregate so the sort
cannot duplicate parent rows:

- **Ascending** — uses `MIN()` to pick the smallest value among related records
- **Descending** — uses `MAX()` to pick the largest value among related records

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

::: info
Parents with no related records get `NULL` values, which sort according to your database's default NULL ordering.
:::

### Filtering Related Records

You can narrow which related records are considered for sorting by providing a `filter`:

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

Only todos where `assigneeId = 1` are included in the `MIN()` aggregation. The
filter accepts the same operators as a regular query, not just equality.

### Extended Sort Syntax

The full sort value can be either a direction or an object:

```ts
// Simple form
$sort: { "todos.text": 1 }

// Extended form with filter
$sort: { "todos.text": { direction: 1, filter: { assigneeId: 1 } } }
```

### Supported Sort Directions

| Value                | Direction  |
| -------------------- | ---------- |
| `1` or `'1'`        | Ascending  |
| `-1` or `'-1'`      | Descending |
| `'asc'`             | Ascending  |
| `'desc'`            | Descending |
| `'asc nulls first'` | Ascending, nulls first  |
| `'asc nulls last'`  | Ascending, nulls last   |
| `'desc nulls first'`| Descending, nulls first |
| `'desc nulls last'`  | Descending, nulls last  |

## Non-unique to-one relations

When a hop declared `asArray: false` cannot be proven unique, the ordering value
comes from a `GROUP BY` derived table rather than a plain join:

```sql
LEFT JOIN (
  SELECT "age" AS "__fk_sort_key", MIN("name") AS "__fk_sort_value"
  FROM "users" GROUP BY "age"
) AS "__fk_sort__sameAge" ON "__fk_sort__sameAge"."__fk_sort_key" = "users"."age"
ORDER BY "__fk_sort__sameAge"."__fk_sort_value" ASC
```

One row per key by construction, so the parent row count and `total` stay
correct. The aggregate follows the sort direction — `MIN()` ascending, `MAX()`
descending — the same rule hasMany sorting uses.

This is also the mechanism behind hasMany sorting, which replaced a correlated
aggregate evaluated once per candidate row. On the benchmark's un-indexed data
that change took `$sort` by a hasMany column from ~220ms to ~1.3ms.

## Errors

A `$sort` path that starts at a declared relation has to resolve, or the query
is rejected with a `BadRequest` rather than silently ordering by nothing:

- a broken chain (`'user.bogus.name'`)
- a path that does not end on a column of the related service
- a hasMany relation reached through another relation
  (`'user.todos.text'`) — not supported; sort by a to-many declared on the
  service you are querying

## Combining Sorts

You can mix regular column sorts with relation sorts:

```ts
await app.service("users").find({
  query: {
    $sort: {
      "todos.text": 1, // sort by related todo text
      name: -1, // then by user name descending
    },
  },
});
```
