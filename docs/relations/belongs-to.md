# belongsTo

A belongsTo relation (`asArray: false`) represents a record that references one related record. The foreign key lives on the current table.

## Definition

```ts
new KyselyService<User>({
  Model: db,
  name: "users",
  id: "id",
  relations: {
    manager: {
      service: "users",
      keyHere: "managerId", // column on the users table
      keyThere: "id", // column on the related table
      asArray: false,
      databaseTableName: "users",
    },
  },
});
```

In this example, each user can optionally belong to a manager (a self-referencing relation).

## Querying

Filter parent records by a belongsTo relation's column using dot notation or nested notation — both produce the same SQL.

```ts
// Dot notation
await app.service("todos").find({
  query: { "user.name": "Alice" },
});

// Nested notation
await app.service("todos").find({
  query: { user: { name: "Alice" } },
});
```

Operators work on the leaf column:

```ts
await app.service("todos").find({
  query: { "user.age": { $gt: 30 } },
});
```

## Sorting

You can sort by a belongsTo relation's column using dot notation:

```ts
// Sort users by their manager's name
await app.service("users").find({
  query: { $sort: { "manager.name": 1 } },
});
```

For belongsTo relations, this translates to a simple `LEFT JOIN` — no aggregation is needed since there is at most one related record.

## Multi-level chains

You can chain belongsTo relations across any number of hops. Each hop is resolved through the target service's own `relations` definition.

Given an `events` service that belongsTo `assignments`, which belongsTo `customers`:

```ts
// Dot notation
await app.service("events").find({
  query: { "assignment.customer.fullName": "Acme Corp" },
});

// Nested notation (equivalent)
await app.service("events").find({
  query: { assignment: { customer: { fullName: "Acme Corp" } } },
});
```

Each service declares only its own direct relations — the adapter walks the chain at query time by looking up the target service via `app.service(name)`.

### Operators and sorting work at any depth

```ts
// Filter with an operator at the leaf
await app.service("events").find({
  query: { "assignment.customer.createdAt": { $gt: "2026-01-01" } },
});

// Sort by a deep column
await app.service("events").find({
  query: { $sort: { "assignment.customer.fullName": 1 } },
});
```

### SQL output

A filter compiles to one correlated `EXISTS`; further hops become `INNER JOIN`s inside it, with aliases built by joining the relation keys with `__`:

```sql
SELECT events.* FROM events
WHERE EXISTS (
  SELECT 1 FROM assignments AS assignment
  INNER JOIN customers AS assignment__customer
          ON assignment__customer.id = assignment.customerId
  WHERE assignment.id = events.assignmentId
    AND assignment__customer.fullName = 'Acme Corp'
)
```

Because this is a semi-join, a chain can never duplicate the parent row — not even when a relation declared `asArray: false` points at a non-unique column.

Each filter builds its own subquery: `'assignment.customer.fullName'` and `'assignment.number'` in the same query produce two `EXISTS` clauses rather than sharing one join. `$sort` still joins, and there JOINs sharing a prefix are deduplicated.

### Requirements and limits

- **`app.setup()` must have run** — the adapter needs the Feathers app to look up related services. See [Setup → App Setup](./setup#app-setup).
- **hasMany hops open a subquery** — a chain may pass through hasMany relations (e.g. `'user.todos.text'`); each one becomes an `EXISTS` subquery instead of a JOIN. See [Querying Relations → Mixed chains](./querying#mixed-chains).
- **Same-adapter services** — the related service must also be a `KyselyService`. Paths through foreign adapters are silently skipped.
- **Broken paths throw** — if a path starts at a declared relation but any later segment doesn't resolve (typo, missing `app.setup()`, non-Kysely service), the query is rejected with a `BadRequest` rather than silently dropping the filter. See [Querying Relations → Errors](./querying#errors).
