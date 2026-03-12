# hasMany

A hasMany relation (`asArray: true`) represents a record that has multiple related records. The foreign key lives on the related table.

## Definition

```ts
new KyselyService<User>({
  Model: db,
  name: "users",
  id: "id",
  relations: {
    todos: {
      service: "todos",
      keyHere: "id", // column on the users table
      keyThere: "userId", // column on the todos table
      asArray: true,
      databaseTableName: "todos",
    },
  },
});
```

In this example, each user can have many todos.

## Querying

hasMany relations support the `$some`, `$none`, and `$every` operators. See [Querying Relations](./querying) for details.

## Sorting

You can sort parent records by a hasMany relation's column. See [Sorting](./sorting) for details.
