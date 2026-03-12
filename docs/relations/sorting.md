# Sorting by Relations

You can sort parent records by columns in related tables using dot notation in `$sort`.

## belongsTo Sorting

For belongsTo (`asArray: false`) relations, sorting uses a `LEFT JOIN`:

```ts
// Sort users by their manager's name
await app.service("users").find({
  query: { $sort: { "manager.name": 1 } },
});
```

Since there is at most one related record, no aggregation is needed.

## hasMany Sorting

For hasMany (`asArray: true`) relations, sorting uses a subquery with an aggregate function to avoid duplicating parent rows:

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

Only todos where `assigneeId = 1` are included in the `MIN()` aggregation.

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

### Combining Sorts

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
