# Querying Relations

## belongsTo

For `asArray: false` (belongsTo) relations, filter by the related record's columns using dot notation or nested notation. This translates to a `LEFT JOIN`. Chains of any depth are supported — see [belongsTo → Multi-level chains](./belongs-to#multi-level-chains) for details.

```ts
// 1-level (either form works)
await app.service("todos").find({
  query: { "user.name": "Alice" },
});

// Multi-level
await app.service("events").find({
  query: { "assignment.customer.fullName": "Acme Corp" },
});
```

## hasMany

For `asArray: true` (hasMany) relations, you can filter parent records based on conditions on their children using `$some`, `$none`, and `$every`.

## `$some`

Returns parent records where **at least one** related record matches the filter.

```ts
// Users who have at least one todo with text 'A-todo'
await app.service("users").find({
  query: { todos: { $some: { text: "A-todo" } } },
});
```

Translates to `WHERE EXISTS (SELECT 1 FROM todos WHERE ...)`.

## `$none`

Returns parent records where **no** related record matches the filter.

```ts
// Users who have no completed todos
await app.service("users").find({
  query: { todos: { $none: { completed: true } } },
});
```

Translates to `WHERE NOT EXISTS (SELECT 1 FROM todos WHERE ...)`.

## `$every`

Returns parent records where **all** related records match the filter.

```ts
// Users where every todo is completed
await app.service("users").find({
  query: { todos: { $every: { completed: true } } },
});
```

Implemented as "no child exists that does NOT match" — `WHERE NOT EXISTS (SELECT 1 FROM todos WHERE NOT ...)`.

## Combining with Other Queries

Relation operators can be combined with regular query filters:

```ts
// Active users who have at least one high-priority todo
await app.service("users").find({
  query: {
    active: true,
    todos: { $some: { priority: "high" } },
  },
});
```
