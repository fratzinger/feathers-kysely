# Upsert

`create` can perform an upsert using `ON CONFLICT` (PostgreSQL/SQLite) or
`ON DUPLICATE KEY UPDATE` (MySQL). Pass the conflict options under
`params.kysely`:

```ts
const result = await app.service("users").create(
  { name: "Alice", age: 31 },
  {
    kysely: {
      onConflictFields: ["name"],
      onConflictAction: "merge", // 'merge' or 'ignore' (default)
    },
  },
);
```

Because this is a regular `create`, it runs through the standard Feathers
pipeline: it emits the `created` event, runs your hooks, and participates in
transaction event deferral.

::: warning Event semantics
`create` always emits `created`, even when a conflict occurs. With
`onConflictAction: 'ignore'` and an existing row, the call returns that row
without inserting — but a `created` event is still emitted. This is inherent to
the standard pipeline; if you need different event semantics, handle it in a hook.
:::

## Options

All options live under `params.kysely`:

| Option                    | Type       | Default    | Description                               |
| ------------------------- | ---------- | ---------- | ----------------------------------------- |
| `onConflictFields`        | `string[]` | _required_ | Fields to use in the `ON CONFLICT` clause |
| `onConflictAction`        | `string`   | `'ignore'` | `'merge'` or `'ignore'`                    |
| `onConflictMergeFields`   | `string[]` | —          | Specific fields to update on conflict     |
| `onConflictExcludeFields` | `string[]` | —          | Fields to exclude from update             |

Conflict handling only kicks in when `onConflictFields` is set. Without it,
`create` is a plain insert.

### Merge vs Ignore

- **`merge`** — updates the existing row with the new values (`ON CONFLICT DO UPDATE`)
- **`ignore`** — keeps the existing row unchanged (`ON CONFLICT DO NOTHING`), returns the existing row

### Controlling Merged Fields

```ts
// Only update the age field on conflict
await app.service("users").create(
  { name: "Alice", age: 31 },
  {
    kysely: {
      onConflictFields: ["name"],
      onConflictAction: "merge",
      onConflictMergeFields: ["age"],
    },
  },
);

// Update everything except the name field
await app.service("users").create(
  { name: "Alice", age: 31 },
  {
    kysely: {
      onConflictFields: ["name"],
      onConflictAction: "merge",
      onConflictExcludeFields: ["name"],
    },
  },
);
```

## Deprecated: `upsert` method

The dedicated `upsert` method still works but is **deprecated**. It takes the
same options as top-level params and forwards them to `create`. Unlike `create`,
it does **not** emit events or run through the standard pipeline.

```ts
// Deprecated — prefer create(data, { kysely: { ... } })
await app.service("users").upsert(
  { name: "Alice", age: 31 },
  { onConflictFields: ["name"], onConflictAction: "merge" },
);
```
