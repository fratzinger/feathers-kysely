# Transactions

Wrap service calls in database transactions using Kysely's `ControlledTransaction`
API. The recommended way is the `withTransaction()` **around hook**.

## Using the `withTransaction()` around hook (recommended)

A single around hook handles the whole lifecycle: start → commit on success →
rollback on error. It also **defers Feathers service events** until the
transaction commits (and discards them on rollback), so listeners never react to
data that was rolled back — including events from nested cross-service calls.

Per method:

```ts
import { withTransaction } from "@fratzinger/feathers-kysely";

app.service("users").hooks({
  around: {
    create: [withTransaction()],
  },
});
```

For **full cross-service event deferral**, register it app-wide. App-level
around hooks run for every service and execute *inside* Feathers' internal
event hook, which is exactly what makes deferral work:

```ts
app.hooks({
  around: [withTransaction()],
});
```

`withTransaction()` only engages for `create` / `update` / `patch` / `remove`.
Every other method, and any non-Kysely or transaction-incapable service (e.g.
in-memory SQLite), is a transparent passthrough — so app-level registration is
safe across a mixed app.

Root vs. nested is detected automatically: if `params.transaction` is already
set, a savepoint is opened instead of a new root transaction. Forward
`context.params.transaction` into nested service calls to include them:

```ts
app.service("users").hooks({
  around: { create: [withTransaction()] },
  before: {
    create: [
      async (context) => {
        await context.app
          .service("audit")
          .create(
            { message: "user created" },
            { transaction: context.params.transaction },
          );
      },
    ],
  },
});
```

### Event timing

- On a successful **root** commit, all collected events
  (`created`/`updated`/`patched`/`removed`), including those of nested
  cross-service calls, are emitted in call order.
- On rollback, no events are emitted at all.
- Only the root transaction flushes; nested savepoints just queue onto it.

For a nested service's event to be deferred it must also run through
`withTransaction()` (per-service or app-level) and the call must forward
`params.transaction`.

## How event deferral works

Feathers emits `created`/`updated`/… in an internal app-level *around* hook that
wraps every method. `withTransaction()` runs inside it. After the wrapped method
resolves, the hook captures the pending event into a root-transaction-scoped
queue and clears `context.event` so the internal hook does not emit immediately.
When the root transaction commits, the queue is flushed; on rollback it is
discarded.

## Legacy hooks (deprecated)

::: warning Deprecated
`trxStart` / `trxCommit` / `trxRollback` are kept for backward compatibility but
are deprecated. They do **not** defer cross-service events. Prefer
`withTransaction()`.
:::

```ts
import { trxStart, trxCommit, trxRollback } from "@fratzinger/feathers-kysely";

app.service("users").hooks({
  before: {
    create: [trxStart()],
  },
  after: {
    create: [trxCommit()],
  },
  error: {
    create: [trxRollback()],
  },
});
```

## Using Params Directly

You can manage transactions manually by passing a `transaction` object in
`params`. This interoperates with `withTransaction()` — forward
`context.params.transaction` into nested service calls and they join the same
transaction:

```ts
import type { KyselyAdapterTransaction } from "@fratzinger/feathers-kysely";

const trx = await db.startTransaction().execute();

const transaction: KyselyAdapterTransaction = {
  trx,
  id: Date.now(),
  starting: false,
};

try {
  await app
    .service("users")
    .create({ name: "Alice", age: 30 }, { transaction });
  await app
    .service("posts")
    .create({ title: "Hello", userId: 1 }, { transaction });
  await trx.commit().execute();
} catch (error) {
  await trx.rollback().execute();
  throw error;
}
```

## Nested Transactions (Savepoints)

Nested transactions are backed by savepoints. When a `params.transaction` is
already present, `withTransaction()` (and the legacy `trxStart()`) opens a
savepoint via `trx.savepoint()` instead of a new root transaction, releasing it
on success and rolling back to it on error. This happens automatically — just
forward `context.params.transaction` into the nested service call.
