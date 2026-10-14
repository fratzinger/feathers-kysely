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
around hooks run for every service and execute _inside_ Feathers' internal
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
- Only the root transaction flushes; nested savepoints just queue onto it. A
  savepoint rolled back drops the events queued within it, even when the root
  commits after all.
- Every event carries what it would without a transaction: the call's result
  and `dispatch` once all its hooks are done — resolvers such as
  `resolveResult` / `resolveExternal` included, wherever they sit around
  `withTransaction()`. So realtime clients get what the channels would send them
  anyway.
- A listener that throws rejects the root call, as in Feathers — the transaction
  stays committed. The events of nested calls are isolated from each other and
  from the call.

For a nested service's event to be deferred it must also run through
`withTransaction()` (per-service or app-level) and the call must forward
`params.transaction`.

## Options: `start` and `nested`

Two decisions are independent of each other:

| Option   | Default       |                                                                      |
| -------- | ------------- | -------------------------------------------------------------------- |
| `start`  | `true`        | Without a transaction: start one, or (`false`) run without one.      |
| `nested` | `'savepoint'` | Within a transaction: a savepoint of its own, or (`'join'`) join it. |

A savepoint lets a nested call fail on its own — the caller can catch the error
and go on. But savepoints of one transaction form a stack, so nested calls with
savepoints can't run in parallel. Joining needs no savepoint, so nested calls
can run in parallel; a failure in one aborts the whole transaction. Either way
the call's event waits for the root's commit.

- `withTransaction({ nested: 'join' })` app-wide makes every mutating call
  transactional, and nested calls join the caller's transaction.
- `withTransaction({ start: false, nested: 'join' })` suits a service only ever
  called within another call's transaction: it joins that one and starts none
  of its own.

```ts
import { withTransaction } from "@fratzinger/feathers-kysely";

app.service("orders").hooks({
  around: { create: [withTransaction()] },
  before: {
    create: [
      async (context) => {
        const { transaction } = context.params;
        // in parallel: fine when they join, not with savepoints of their own
        await Promise.all([
          context.app.service("order-items").create(items, { transaction }),
          context.app.service("audit").create(entry, { transaction }),
        ]);
      },
    ],
  },
});

const joining = withTransaction({ start: false, nested: "join" });
app.service("order-items").hooks({ around: { all: [joining] } });
app.service("audit").hooks({ around: { all: [joining] } });
```

A joined call has no savepoint to roll back to: if the caller catches its error
and goes on, what it wrote before failing commits with the root — and after a
database error, the transaction is aborted altogether. Let the error roll back
the root. To recover from a nested failure, use savepoints and call one after
another.

A nested call must not outlive the transaction it runs in. When one fails in
`Promise.all`, the root rolls back right away, while the others may still be
running. Those are refused from then on — they reject with "outlived its
transaction" instead of writing outside of it. And a nested call still in its
savepoint when the root is about to commit — one nobody awaited — rolls the root
back ("still running") rather than commit half its work. Await every nested call
before the call that started the transaction returns.

## After the commit: `afterCommit()`

Some side effects can't be rolled back — a push notification, a cache
invalidation, a job in another system. `afterCommit()` runs them once the
transaction in `params` committed, in the background, and not at all after a
rollback. Without a transaction it runs the action right away:

```ts
import { afterCommit } from "@fratzinger/feathers-kysely";

const sendPush = async (context) => {
  await afterCommit(context.params, () => push(context.result), {
    onError: (error) => logger.error(error),
  });
};
```

Within a transaction `afterCommit()` returns before the action ran — the commit
comes later. Called within a savepoint, the action waits for the savepoint to be
released and the root to commit: a savepoint rolled back takes it along, even
when the root commits after all.

Without a transaction, the action runs right away, and an error reaches the
caller — `onError` covers only an action run after the commit. The same goes
for a transaction passed in by hand: there is no telling when it commits.

## How event deferral works

Feathers emits `created`/`updated`/… in an internal app-level _around_ hook that
wraps every method, once all other hooks of the call are done.
`withTransaction()` runs inside it.

- The call that starts the root transaction leaves its event to that hook: by
  the time it emits, `withTransaction()` has committed — and after a rollback
  the call fails, so it emits nothing.
- A nested call moves its pending event into a queue of the root transaction
  and clears `context.event`, so the internal hook does not emit it right away.
  When the root commits, the queue is flushed, reading each call's
  `context.result` only then — every nested call is done by that time. On
  rollback, the queue is discarded.

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

::: warning Not in parallel
Savepoints of one transaction form a stack: releasing one releases every
savepoint opened after it. So a nested call opens its savepoint only on top of
its caller's: of nested `withTransaction()` calls in parallel (`Promise.all`),
every one but the first rejects with "nested calls ran in parallel" before it
runs, rather than let the database fail. Run them one after another, or let the
nested services join: `withTransaction({ nested: 'join' })`.
:::
