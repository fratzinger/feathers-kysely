# @fratzinger/feathers-kysely

[![npm](https://img.shields.io/npm/v/@fratzinger/feathers-kysely)](https://www.npmjs.com/package/@fratzinger/feathers-kysely)
[![Download Status](https://img.shields.io/npm/dm/@fratzinger/feathers-kysely.svg?style=flat-square)](https://www.npmjs.com/package/@fratzinger/feathers-kysely)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label)](https://discord.gg/qa8kez8QBx)

> A [FeathersJS](https://feathersjs.com/) database adapter for [Kysely](https://kysely.dev/) — the type-safe SQL query builder.

Supports **PostgreSQL**, **MySQL**, and **SQLite**.

## Installation

```bash
npm install @fratzinger/feathers-kysely kysely
```

## Documentation

[Read the full documentation](https://feathers-kysely.fratzinger.workers.dev/)

## Contributing

`pnpm test` runs the suite (`DB=sqlite|postgres|mysql`, see `test/dialect.ts`).
`pnpm bench` runs the query benchmarks — see [`bench/README.md`](bench/README.md)
for the regression workflow.

SQLite runs in memory and needs no setup. For postgres and mysql, put the
connection details in a gitignored `.env`, which `vite.config.ts` loads via
`loadEnvFile` before the tests run; anything you leave out falls back to the
defaults in `test/dialect.ts`.

```ini
DB = postgres
POSTGRES_DB = feathers_kysely_test
POSTGRES_USER = postgres
POSTGRES_PASSWORD = ""
```

Quote empty values. `POSTGRES_PASSWORD = ` with nothing after it makes
`loadEnvFile` treat the *next* line as the value, so that variable is silently
lost — a missing `POSTGRES_DB` then falls back to the default and the tests
connect to a different database than the one you configured.

## License

Copyright (c) 2026 [Feathers contributors](https://github.com/feathersjs/feathers/graphs/contributors)

Licensed under the [MIT license](LICENSE).
