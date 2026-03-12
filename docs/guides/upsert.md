# Upsert

The service provides an `upsert` method using `ON CONFLICT` (PostgreSQL/SQLite) or `ON DUPLICATE KEY UPDATE` (MySQL).

```ts
const result = await app.service("users").upsert(
  { name: "Alice", age: 31 },
  {
    onConflictFields: ["name"],
    onConflictAction: "merge", // 'merge' (default) or 'ignore'
  },
);
```

## Options

| Option                   | Type       | Default   | Description                              |
| ------------------------ | ---------- | --------- | ---------------------------------------- |
| `onConflictFields`       | `string[]` | _required_ | Fields to use in the `ON CONFLICT` clause |
| `onConflictAction`       | `string`   | `'merge'` | `'merge'` or `'ignore'`                  |
| `onConflictMergeFields`  | `string[]` | —         | Specific fields to update on conflict     |
| `onConflictExcludeFields`| `string[]` | —         | Fields to exclude from update             |

### Merge vs Ignore

- **`merge`** — updates the existing row with the new values (`ON CONFLICT DO UPDATE`)
- **`ignore`** — keeps the existing row unchanged (`ON CONFLICT DO NOTHING`)

### Controlling Merged Fields

```ts
// Only update the age field on conflict
await app.service("users").upsert(
  { name: "Alice", age: 31 },
  {
    onConflictFields: ["name"],
    onConflictMergeFields: ["age"],
  },
);

// Update everything except the name field
await app.service("users").upsert(
  { name: "Alice", age: 31 },
  {
    onConflictFields: ["name"],
    onConflictExcludeFields: ["name"],
  },
);
```
