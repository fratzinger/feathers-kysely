---
name: other-orms
description: Prior art from other ORMs and query builders. Use when naming an operator, option or config key, designing relation/filter semantics, or deciding an edge case (null, empty array, error vs. silent no-op) — anything shaped like "how does X solve this?".
---

# Other ORMs

Prior art for API decisions in this adapter. Consult it before inventing a name or a semantic; being boringly familiar to users of Prisma/Drizzle/Mongo beats being clever.

## Candidates

Default pair: **Prisma** + **Drizzle**. Add others by branch.

| Candidate                                                     | Best prior art for                                                                        | Docs                                                                       |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Prisma                                                        | relation filters (`some`/`none`/`every`), `AND`/`OR`/`NOT`, nested writes, upsert         | https://www.prisma.io/docs/orm/prisma-client/queries/filtering-and-sorting |
| Drizzle                                                       | relational queries, `with`, operator naming, type inference                               | https://orm.drizzle.team/docs/rqb                                          |
| Kysely                                                        | what our own layer already offers — check before building it ourselves                    | https://kysely.dev/docs/recipes                                            |
| MongoDB query operators                                       | semantics of `$`-operators; the tie-breaker, since Feathers' query syntax is Mongo-shaped | https://www.mongodb.com/docs/manual/reference/operator/query/              |
| Feathers adapters (`@feathersjs/knex`, `@feathersjs/mongodb`) | what Feathers users already expect from `params`/`query`                                  | https://github.com/feathersjs/feathers/tree/dove/packages                  |
| Objection.js                                                  | `$relatedQuery`, graph insert/upsert, eager expressions                                   | https://vincit.github.io/objection.js/                                     |
| TypeORM                                                       | `find` options, eager/lazy relations                                                      | https://typeorm.io/find-options                                            |
| MikroORM                                                      | query conditions, filters, populate                                                       | https://mikro-orm.io/docs/query-conditions                                 |
| Sequelize                                                     | `Op.*` naming, nested `include`/`where`                                                   | https://sequelize.org/docs/v6/core-concepts/model-querying-basics/         |

## How to consult

1. Pick the candidates whose column above matches the decision — two or three, not the whole table.
2. Read the primary docs (WebFetch/WebSearch on the URLs above), not blog posts, and note the version. For Kysely and Feathers, `node_modules` holds the exact version we ship against — read the types there.
3. Report per candidate: the exact name/signature, the semantics, and where they disagree with each other. Then one recommendation with the reason.

## Constraints that outrank prior art

- **Feathers common query syntax** (`$limit`, `$skip`, `$sort`, `$select`, `$or`, `$and`) is fixed by `@feathersjs/adapter-commons` — prior art may not rename it.
- **Mongo semantics before Prisma naming** where the two collide, because Feathers queries are Mongo-shaped.
- **Kysely must express it type-safely** across postgres, mysql and sqlite. A borrowed feature that only works on one dialect needs a documented dialect note (see `docs/api/operators.md`) or it does not ship.
