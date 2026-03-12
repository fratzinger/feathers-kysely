# Custom Queries

The underlying Kysely instance is available at `service.Model`. It is the full `Kysely` instance, not scoped to a table — so provide the table name in each query:

```ts
const service = app.service("users");

const results = await service.Model.selectFrom("users")
  .select(["id", "name"])
  .where("age", ">", 18)
  .execute();
```
