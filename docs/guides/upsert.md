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

With `onConflictReturning: 'written' | 'changed' | 'none'`, a single create
that returns nothing emits `created` with `undefined` as payload.
:::

## Options

All options live under `params.kysely`:

| Option                    | Type       | Default    | Description                                  |
| ------------------------- | ---------- | ---------- | -------------------------------------------- |
| `onConflictFields`        | `string[]` | _required_ | Fields to use in the `ON CONFLICT` clause    |
| `onConflictAction`        | `string`   | `'ignore'` | `'merge'` or `'ignore'`                       |
| `onConflictMergeFields`   | `string[]` | —          | Specific fields to update on conflict        |
| `onConflictExcludeFields` | `string[]` | —          | Fields to exclude from update                |
| `onConflictReturning`     | `string`   | `'all'`    | `'all'`, `'written'`, `'changed'` or `'none'` |

Conflict handling only kicks in when `onConflictFields` is set. Without it,
`create` is a plain insert and `onConflictReturning` is ignored as well.

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

## Controlling the returned rows

By default (`onConflictReturning: 'all'`), `create` behaves like
get-or-create: every input row comes back, including rows whose conflict was
ignored — those are fetched with an extra `SELECT` after the insert.
`onConflictReturning` changes that:

| Mode        | `ignore` (or merge with zero fields to update)                  | `merge`                                            |
| ----------- | --------------------------------------------------------------- | -------------------------------------------------- |
| `'all'`     | every input row (ignored conflicts are post-fetched)             | every input row                                    |
| `'written'` | only inserted rows; single create resolves to `undefined`        | every row is written, so this equals `'all'`       |
| `'changed'` | like `'written'`                                                 | only inserted rows + rows whose merge fields differ |
| `'none'`    | nothing: `undefined` for single, `[]` for multi                  | same                                               |

```ts
// Bulk import: insert what's new, skip duplicates, return only the new rows
const inserted = await app.service("users").create(rows, {
  kysely: {
    onConflictFields: ["email"],
    onConflictAction: "ignore",
    onConflictReturning: "written",
  },
});

// Sync job: merge incoming data, but only get back what actually changed
const changed = await app.service("users").create(rows, {
  kysely: {
    onConflictFields: ["email"],
    onConflictAction: "merge",
    onConflictReturning: "changed",
  },
});

// Fire-and-forget bulk insert: no RETURNING, no post-fetch — the fastest path
await app.service("users").create(rows, {
  kysely: {
    onConflictFields: ["email"],
    onConflictAction: "ignore",
    onConflictReturning: "none",
  },
});
```

::: tip `'changed'` skips no-op writes
With `'changed'`, a merge only writes rows whose merge fields actually differ.
On PostgreSQL/SQLite this adds `DO UPDATE ... WHERE ... IS DISTINCT FROM ...`,
so no-op merges are not written at all — no triggers fire, no `updated_at`
bump, no dead tuples. MySQL natively skips identical writes with
`ON DUPLICATE KEY UPDATE`.
:::

::: warning Caveats
- **TypeScript**: the service types still declare `Promise<Result>`. With
  `'written'`, `'changed'` or `'none'`, a single create may resolve to
  `undefined` at runtime.
- **MySQL**: MySQL has no `RETURNING`. To tell written rows apart, the
  adapter runs one extra `SELECT` over the conflict fields _before_ the
  insert (for `'written'`/`'changed'` with ignored conflicts and for
  single-row `'changed'` merges) — outside a transaction this is best-effort
  under concurrent writers. For a multi-row `merge` with `'changed'`, the
  returned rows behave like `'all'` (there is no way to tell changed rows
  apart).
:::

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
