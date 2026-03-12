---
layout: home

hero:
  image:
    src: /logo.svg
    alt: feathers-kysely logo
  name: feathers-kysely
  text: Type-safe SQL for FeathersJS
  tagline: A database adapter for Kysely — the type-safe SQL query builder. Supports PostgreSQL, MySQL, SQLite, and MSSQL.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: View on GitHub
      link: https://github.com/fratzinger/feathers-kysely

features:
  - title: Type-safe Queries
    details: Built on Kysely for full TypeScript support across your database queries.
  - title: Relations
    details: Define belongsTo and hasMany relations with filtering ($some, $none, $every) and sorting support.
  - title: Transactions
    details: Full transaction support with hooks or manual control, including nested savepoints.
  - title: Multi-dialect
    details: Works with PostgreSQL, MySQL, SQLite, and MSSQL out of the box.
---
