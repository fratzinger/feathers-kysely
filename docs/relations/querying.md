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

## Mixed chains

A relation path may mix belongsTo and hasMany hops in any order and to any depth. Each belongsTo hop becomes a `LEFT JOIN`; each hasMany hop opens an `EXISTS` subquery, and the rest of the path is resolved inside it against the related service's own relations.

```ts
// assignmentEvents → assignment (belongsTo) → assignmentCategories (hasMany)
await app.service("assignment-events").find({
  query: {
    assignment: {
      assignmentCategories: {
        $some: { assignmentCategoryTypeId: { $in: [1, 2, 3] } },
      },
    },
  },
});
```

```sql
SELECT assignment_events.* FROM assignment_events
LEFT JOIN assignments AS assignment ON assignment.id = assignment_events.assignmentId
WHERE EXISTS (
  SELECT 1 FROM assignment_categories AS assignment__assignmentCategories
  WHERE assignment__assignmentCategories.assignmentId = assignment.id
    AND assignment__assignmentCategories.assignmentCategoryTypeId IN (1, 2, 3)
) AND assignment.id IS NOT NULL
```

Relations of the related service are available inside `$some` / `$none` / `$every` too — a belongsTo there is joined **within** the subquery:

```ts
// ... → assignmentCategories (hasMany) → type (belongsTo)
await app.service("assignment-events").find({
  query: {
    assignment: {
      assignmentCategories: { $some: { type: { name: "urgent" } } },
    },
  },
});
```

### Dot notation

Dot notation is equivalent, with one restriction: every hasMany hop in a dot path means `$some`. `$none` and `$every` are only expressible in nested notation.

```ts
// same as the $some query above
await app.service("assignment-events").find({
  query: { "assignment.assignmentCategories.type.name": "urgent" },
});
```

### Semantics of a hasMany behind a belongsTo

A belongsTo hop is joined with a `LEFT JOIN`, so the adapter adds `<alias>.<key> IS NOT NULL` for the hop. This matters for the negating operators: `{ assignment: { assignmentCategories: { $none: {} } } }` means _"has an assignment, and that assignment has no categories"_ — rows without an assignment at all are **not** returned.

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

## Limits

- **`app.setup()` must have run** for any chain longer than one hop — the adapter resolves each hop through `app.service(name)`. See [Setup → App Setup](./setup#app-setup).
- **JSON column traversal is not available inside `$some` / `$none` / `$every`** — column types are read from the queried service's own `properties`, so a dot path in a sub-filter is treated as a relation path, not as JSON access.
- **Unresolvable paths are dropped**, never turned into SQL. A typo in a relation or column name silently widens the result set instead of throwing.
