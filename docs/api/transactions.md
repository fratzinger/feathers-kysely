# Transactions

Transaction hooks wrap service calls in database transactions using Kysely's `ControlledTransaction` API.

## Using Hooks

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

You can manage transactions manually by passing a `transaction` object in `params`:

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

Nested transactions are supported by passing a transaction's `trx` to `startTransaction()` again.
