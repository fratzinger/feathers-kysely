# Getting Started

## Installation

```bash
npm install @fratzinger/feathers-kysely kysely
```

You also need a database driver for your dialect:

```bash
# PostgreSQL
npm install pg

# MySQL
npm install mysql2

# SQLite
npm install better-sqlite3
```

## Basic Setup

```ts
import { feathers } from "@feathersjs/feathers";
import { Kysely, Generated } from "kysely";
import { KyselyService } from "@fratzinger/feathers-kysely";

// 1. Define your database types
interface UsersTable {
  id: Generated<number>;
  name: string;
  age: number;
}

interface DB {
  users: UsersTable;
}

type User = { id: number; name: string; age: number };

// 2. Create a Kysely instance
const db = new Kysely<DB>({
  dialect: /* your dialect */,
});

// 3. Register the service
const app = feathers().use(
  "users",
  new KyselyService<User>({
    Model: db,
    name: "users",
    id: "id",
    multi: true,
    paginate: { default: 10, max: 100 },
  }),
);

// 4. Use the service
const user = await app.service("users").create({ name: "Alice", age: 30 });
const users = await app
  .service("users")
  .find({ query: { age: { $gte: 18 } } });
```
