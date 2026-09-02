# Querying Relations

## belongsTo

For `asArray: false` (belongsTo) relations, filter by the related record's columns using dot notation or nested notation. This translates to a correlated `EXISTS` subquery — a semi-join, so it can never duplicate parent rows. Chains of any depth are supported — see [belongsTo → Multi-level chains](./belongs-to#multi-level-chains) for details.

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

A relation path may mix belongsTo and hasMany hops in any order and to any depth. Every hop becomes a correlated `EXISTS` subquery, and the rest of the path is resolved inside it against the related service's own relations.

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
WHERE EXISTS (
  SELECT 1 FROM assignments AS assignment
  WHERE assignment.id = assignment_events.assignmentId
    AND EXISTS (
      SELECT 1 FROM assignment_categories AS assignment__assignmentCategories
      WHERE assignment__assignmentCategories.assignmentId = assignment.id
        AND assignment__assignmentCategories.assignmentCategoryTypeId IN (1, 2, 3)
    )
)
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

### Filters are semi-joins, never joins

Every relation filter compiles to `EXISTS`, at every hop and every depth. Three consequences worth knowing:

- **Parent rows are never duplicated.** `$limit`, `$skip` and the paginated `total` stay correct even when a relation declared `asArray: false` points at a non-unique column.
- **Negation composes.** `$not`, `$none` and `$every` wrap the whole subquery, so there is no join predicate left outside the negation to reason about.
- **A missing parent excludes the row.** `{ assignment: { assignmentCategories: { $none: {} } } }` reads as _"has an assignment, and that assignment has no categories"_ — a row without an assignment at all does not match, because the outer `EXISTS` is already false.

`$sort` is the exception: ordering by a related column needs the value itself, not just its existence, so it still uses a `LEFT JOIN` (or an aggregate subquery for hasMany). See [Sorting](./sorting).

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

## Errors

A filter that cannot be resolved is rejected with a `BadRequest` instead of being dropped. A dropped filter widens the result set, which is how an authorization filter turns into a data leak — so anything the adapter can prove is wrong fails loudly:

- a collection operator (`$some` / `$none` / `$every`) on a belongsTo relation, on a plain column, or on an unknown key
- a path that starts at a declared relation but breaks further along — `'user.bogus.name'`, or a hop whose target service is missing, is not a `KyselyService`, or is unreachable because `app.setup()` never ran
- a dot path inside `$some` / `$none` / `$every` that is neither a relation of the related service nor one of its own columns

Two cases stay quiet, because the adapter cannot prove they are wrong:

- a dot path whose **first** segment is not a declared relation — indistinguishable from an already-qualified column ref or a JSON access. An unknown column there surfaces as a database error.
- an empty condition object (`{ user: {} }`), which is a no-op, like `$not: {}`
