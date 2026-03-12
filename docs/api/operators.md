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

| Operator     | SQL             | Description                                    |
| ------------ | --------------- | ---------------------------------------------- |
| `$like`      | `LIKE`          | Pattern matching                               |
| `$notLike`   | `NOT LIKE`      | Negated pattern matching                       |
| `$iLike`     | `ILIKE`         | Case-insensitive pattern matching (PostgreSQL) |

## Array Operators (PostgreSQL)

| Operator     | SQL  | Description        |
| ------------ | ---- | ------------------ |
| `$contains`  | `@>` | Array contains     |
| `$contained` | `<@` | Array contained by |
| `$overlap`   | `&&` | Array overlap      |

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
