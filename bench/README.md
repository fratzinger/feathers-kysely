# Benchmarks

Two complementary guards against performance and shape regressions in query
building.

## `test/sql-shape.test.ts` — what SQL we emit

Snapshots the compiled SQL and parameters for every case in
`test/relation-graph.ts`. Kysely compiles without touching a database, so this
runs in the normal test suite, needs no schema and no data, and is pinned to
sqlite regardless of `DB` — the point is *our* output (which subqueries, which
aliases, how many of them), not per-dialect syntax.

This is the cheap, deterministic guard: a change to query building shows up as
a readable diff instead of as a timing wobble. Review the diff, then accept it
with `vitest -u`.

## `bench/relations.bench.ts` — how fast that SQL runs

Executes every case against a seeded database.

```bash
# on the reference commit
pnpm bench --outputJson=bench/baseline.json

# after the change
pnpm bench --compare=bench/baseline.json
```

The comparison prints a factor per case (`[1.02x] ⇑`). Baselines are
machine-specific and gitignored — generate one locally rather than trusting a
committed file.

### Reading the numbers

**Compare a case against itself across runs, never against another case.** The
cases differ in selectivity, so `belongsTo 2 hops` being faster than
`belongsTo 1 hop` says nothing about hop count — it says the two filters match
different numbers of rows.

Treat anything under roughly 1.1x as noise, and look at `hz` and `mean` rather
than the summary factor, which a single outlier in `max` can distort. Re-run a
suspicious case before believing it.

### Knobs

| Variable | Effect |
| --- | --- |
| `DB` | `sqlite` (default), `postgres`, `mysql` — see `test/dialect.ts` |
| `BENCH_SCALE` | Multiplies all row counts (default `1` ≈ 72k rows) |

Data is generated from a fixed PRNG seed, so two runs measure the same rows.

### The graph

`test/relation-graph.ts` defines a deliberately deep graph — three belongsTo
hops, two hasMany levels, and a to-many behind a to-one — so a change in path
resolution shows up somewhere:

```
events ──▶ assignment ──▶ customer ──▶ owner (users)
               │                          │
               └─▶ categories ──▶ type     └─▶ ownedCustomers ──▶ assignments
```

Every column any case filters or sorts on is indexed. That is not incidental:
without an index, a chained filter degrades to a sequential scan inside a
correlated subquery and the timing tells you about table size instead of about
the SQL. If you add a case that filters a new column, add its index too.

## Gotchas

- **Suite hooks do not run in benchmark mode.** `beforeAll` is silently skipped,
  so `bench/relations.bench.ts` seeds and calls `app.setup()` at module scope
  with top-level await. Setup in a hook leaves the app without its Feathers
  reference, every multi-hop chain fails with a `BadRequest`, and benchmarks
  report the failure as `NaN` rather than as an error.
- **A too-small time budget also reports `NaN`.** Each iteration is a database
  round-trip; the default 500ms budget can complete zero samples for the
  heavier chains, hence the explicit `{ time, warmupTime }` per benchmark.
