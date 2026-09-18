---
name: relational-query-processor
description: Specialized skill for working in the fdb-relational-core SQL processing layer — parser, plan generator, and Cascades planner. Use when debugging query plans, writing planner rules, or understanding the SQL execution path.
  Some example usages:
  "Why is this query doing a full table scan?"
  "Explain this EXPLAIN output"
  "How do I write a new Cascades rule?"
  "Why is my query not using the index?"
---

This skill provides context for working in `fdb-relational-core`, the SQL processing layer
of the Record Layer — covering the parser, plan generator, and Cascades planner.

## Key entry points

| Class | Role |
|---|---|
| `EmbeddedRelationalStatement` | Main SQL statement implementation. `executeGet` → point read via `source.get()`; `executeScan` → range scan via `source.openScan()`. |
| `RelationalDirectAccessStatement` | Bypasses the SQL planner entirely — goes directly to FDB. |
| `RecordLayerEngine` | Engine setup; manages the connection between JDBC and the record layer. |
| `RelationalPlanCache` | Caches compiled query plans. **Must be initialized** for SQL performance — `RelationalPlanCache.buildWithDefaults()`. Without it, every query runs the full Cascades planner. |

## SQL execution path (normal case)

```
JDBC call
  → EmbeddedRelationalStatement
    → SQL parse + Cascades planner (on cache miss)
      → LogicalQueryPlan.optimize()
        → Simplification.executeRuleSetIteratively()   ← only on cold miss
    → RelationalPlanCache (warm hit: wraps existing plan)
      → PhysicalQueryPlan.withExecutionContext()
        → executePhysicalPlan()
          → FDB read
```

On a **warm cache hit**, simplification is skipped entirely. The planner overhead is
~0.3–0.5 ms per call.

## Reading EXPLAIN output

Run `EXPLAIN <query>` via JDBC or in a yaml test:

```sql
EXPLAIN SELECT * FROM t WHERE id = 1
```

Common plan shapes:
- `COVERING(idx_name [EQUALS ?] → [...])` — index-only scan, no fetch needed.
- `FETCH(COVERING(...))` — index scan followed by a record fetch.
- `SCAN(<,>) | FILTER ...` — full table scan with a post-scan filter. This usually means the
  planner could not match a primary key or index scan candidate.
- `MAP (_.col AS col)` — projection step.

## Writing a yaml test that checks the plan

```yaml
- query: explain select * from t where id = 1
  explain: "COVERING(idx_id [EQUALS ?] → [id: KEY[0], name: VALUE[0]])"
```

Use `@MaintainYamlTestConfig(YamlTestConfigFilters.CORRECT_EXPECTATIONS)` on the test method
to have the framework fill in the `explain:` value automatically on first run.

## Debugging a plan interactively

Add `@DebugPlanner` to the `@YamlTest` method to launch `PlannerRepl`, which lets you step
through the Cascades optimization phases.

## Cascades planner overview

The planner is a Cascades-style rule-based optimizer:

1. **Logical rules** transform the logical plan (e.g., push predicates into index scans).
2. **Physical rules** convert logical operators to physical plans (e.g., select index vs. scan).
3. **Match candidates** (e.g., `PrimaryScanMatchCandidate`, `ValueIndexScanMatchCandidate`)
   determine which access paths are available for a given predicate.
4. **Simplification** runs `Derivations.simplifyLocalValues()` — only on cold cache misses.

If a query is doing a full table scan when you expect an index scan, common causes:
- The predicate is on a non-indexed column.
- The index exists but the match candidate is not recognizing the predicate shape (check
  that the column names and types match exactly).
- The `RelationalPlanCache` is not initialized (every call looks like a cold miss).

## Transaction lifecycle in tests

With `autoCommit=true`, `ensureTransactionActive()` rolls back and restarts the transaction
on every statement, clearing per-transaction caches. Both SQL and direct-access paths share
this overhead in benchmarks.

## Schema templates

Schema templates persist in FDB across JVM runs. If you see unexpected state in tests, the
template may be stale. Creation code should be idempotent (drop-then-create pattern).
