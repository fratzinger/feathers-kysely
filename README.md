# @fratzinger/feathers-kysely

[![Download Status](https://img.shields.io/npm/dm/feathers-kysely.svg?style=flat-square)](https://www.npmjs.com/package/feathers-kysely)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label)](https://discord.gg/qa8kez8QBx)

> Feathers SQL service adapter built with Kysely

## Installation

```bash
npm install @fratzinger/feathers-kysely --save
```

## Note about custom queries

Like all Feathers services, you can access the underlying database adapter at `service.Model`. One thing worth noting in `feathers-kysely` is that `service.Model` is the full Kysely instance and not locked down to the current table. So you have to provide the table name in each of the methods that you use, like

- `service.Model.selectFrom('my-table')...`
- `service.Model.insertInto('my-table')...`
- `service.Model.updateTable('my-table')...`

## License

Copyright (c) 2025 [Feathers contributors](https://github.com/feathersjs/feathers/graphs/contributors)

Licensed under the [MIT license](LICENSE).
