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

## Sorting

You can sort by a belongsTo relation's column using dot notation:

```ts
// Sort users by their manager's name
await app.service("users").find({
  query: { $sort: { "manager.name": 1 } },
});
```

For belongsTo relations, this translates to a simple `LEFT JOIN` — no aggregation is needed since there is at most one related record.
