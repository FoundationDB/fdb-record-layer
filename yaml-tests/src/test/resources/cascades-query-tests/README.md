# `cascades-query-tests`

Query-planning coverage ported from the Cascades tests in `fdb-record-layer-core`'s
`com.apple.foundationdb.record.provider.foundationdb.query` package.

Those tests drive the Cascades planner through the `RecordQuery` builder API. The SQL layer drives
the *same* planner from SQL text, so most of that coverage can live here instead, where plans,
planner metrics and result metadata are machine-maintained.

Registered by `yaml-tests/src/test/java/CascadesQueryTests.java`. **The suite class is the only
registry** — a `.yamsql` file nobody references is silently never run.

`MIGRATION.md` in this directory maps every source test method to its new location and current state,
and records the product divergences found while porting.

## Status

115 files. 338 of 350 `explain:` directives are filled. Of the 12 empty ones, 5 belong to queries parked
by a product defect, which never execute; the other 7 are `or-query-to-union-extra-repeated-column.yamsql`
and `in-query-constant-value.yamsql`, added after a stale-deferral review and **not yet run**.
Files added later land with an `- explain: ""`
placeholder on every query — run the correction pass below to fill them in, then verify each against the
source's Cascades assertion (see *Verifying a generated plan against the source assertion*).
`MIGRATION.md` records the state of every source method. The core tests are untouched; retiring them is a
separate, later step.

## Authoring conventions

### One file per source index configuration — and no `USE INDEX`

The core tests keep a deliberately minimal per-test index set (via `RecordMetaDataHook`) so the
planner is forced into one plan. **Reproduce that set exactly, in its own file when it differs.**

Read the whole hook before writing the template. Many hooks are not additive — they call
`complexQuerySetupHook()` and then *remove* indexes, e.g.
`FDBAndQueryToIntersectionTest.testComplexQueryAndWithTwoChildren3` removes `multi_index`,
`MySimpleRecord$str_value_indexed` and `MySimpleRecord$num_value_3_indexed` before adding its own two.
Missing a `removeIndex` leaves an extra candidate in play and silently changes the plan.

**Do not use `USE INDEX` to stand in for a smaller index set.** None of the source tests hint; the
ones that restrict access paths do it with `setAllowedIndexes(...)` or
`planGraph(..., allowedIndexes)`, which are recorded as not portable. A hint also changes behaviour
in its own right: `AccessHints.satisfies`
(`fdb-record-layer-core/.../cascades/AccessHints.java`) returns `false` when the query carries hints
and the candidate carries none, so a hinted query cannot fall back to a scan — it fails with
`UNSUPPORTED_QUERY`.

Variant files are named with a suffix describing the index configuration, e.g.
`and-query-to-intersection-two-covering.yamsql`.

### One `schema_template` per file when the file uses `include:`

Multiple `schema_template` blocks per file are supported and get connection indexes 1, 2, 3…, but
an included `setup:` with no explicit `connect:` resolves to *the* single registered connection URI
and fails outright when several exist
(`YamlExecutionContext.getConnectionFromConnectionURIList`). If a source class needs two index
configurations, make two files rather than two templates.

### Index naming mirrors the core names

Index names appear verbatim in explain strings, so pick them once and never rename:

| Core | Here |
|---|---|
| `MySimpleRecord$str_value_indexed` | `MY_SIMPLE_RECORD_STR_VALUE_INDEXED` |
| `MySimpleRecord$num_value_unique` | `MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE` (unique) |
| `MySimpleRecord$num_value_3_indexed` | `MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED` |
| `multi_index` | `MULTI_INDEX` |
| `repeater$fanout` | `REPEATER_FANOUT` |

### Copy the source's expected plan as a comment

The core classes already carry the expected plan in a comment, e.g.
`// Index(multi_index [[even, 0, 3],[even, 0, 3]])`. Copy it above the query. That is what the
reviewer diffs the generated `explain:` against.

### Mirror the source test

These ports must be as truthful to the record-layer-core test they came from as possible. In
particular:

- The `ORDER BY` mirrors the source's `setSort(...)` **exactly** — the same keys, the same
  directions, nothing added and nothing dropped. Do **not** append the primary key as a tie-break to
  make an ordered result deterministic, and do not drop a key because it happens to be
  equality-bound. If the source declares no sort, there is no `ORDER BY`.
- The index set for each test matches the source's `RecordMetaDataHook`.
- Predicates and literals are as the source has them.
- Where SQL cannot express something, it is left out and the reason is recorded in the file header
  and in the mapping table below — never approximated.

Expected rows use ordered `result:` only where the row *sequence* is actually determined, namely:

- the block has at most one row; or
- the query's `ORDER BY` keys are unique across the expected rows; or
- rows that tie on those keys are identical, so the sequence is invariant anyway (e.g. a query that
  projects only its sort column); or
- the query has no `ORDER BY` but the source asserts a specific order and the plan the run produced
  determines it — currently `multi-field-index-selection-prefix-scalar.yamsql`,
  `repeated-field-query-only-index.yamsql` and `covering-index-header-compound-pk.yamsql`; or
- the block is paged with `maxRows:`, where page membership depends on order regardless.

Everywhere else use `unorderedResult:`. **Tie order within equal sort keys is not derivable by
reasoning** — an in-union under `ORDER BY col DESC` iterates the IN list descending but still scans
each leg forward, giving `(col desc, pk asc)`, whereas a reverse index scan gives `(col desc, pk
desc)`. Asserting a guessed tie order is stronger than the source and produces failures that say
nothing about the product.

Ordering by a column outside the projection is fine — see `orderby.yamsql:247`,
`select c from t1 order by b`.

### PERMUTED_MIN / PERMUTED_MAX: the permuted size is derived, not declared

There is no option to set `IndexOptions.PERMUTED_SIZE_OPTION`. It is computed from where the aggregate
sits in the index's `ORDER BY` — `permutedSize = fieldValues.size() - aggregateOrderIndex`
(`MaterializedViewIndexGenerator:238-240`), where `fieldValues` are the GROUP BY values:

| Core index | yamsql |
|---|---|
| `field("num_value_unique").groupBy(concatenateFields("num_value_2", "num_value_3_indexed"))`, PERMUTED_SIZE 1 | `select num_value_2, max(num_value_unique), num_value_3_indexed from t group by num_value_2, num_value_3_indexed order by num_value_2, max(num_value_unique), num_value_3_indexed` |
| the same over three grouping columns with PERMUTED_SIZE 2 | `... group by a, b, c order by a, max(x), b, c` |

The ORDER BY must list the grouping values in their GROUP BY order with the aggregate inserted exactly
once, or the generator rejects it as "an attempt to create a covering aggregate index"
(`MaterializedViewIndexGenerator:229-231`). Everything after the aggregate is permuted behind it in the
index key, which is what makes `ORDER BY max(...)` free — and what turns an equality on one of those
trailing columns into a residual filter rather than a scan bound.

### Nested messages, fan-out indexes and `oneOfThem()`

A `repeated` nested message becomes a `STRUCT ARRAY`, and every construct the core tests build over it
has a SQL spelling:

| Core key expression / filter | yamsql |
|---|---|
| `field("reviews", FanOut).nest("rating")` | `create index review_rating as select sq.rating from restaurant_record as r, (select rating from r.reviews) sq order by sq.rating` |
| `field("tags", FanOut).nest(concatenateFields("value", "weight"))` | `select sq.value, sq.weight from restaurant_record as r, (select value, weight from r.tags) sq order by sq.value, sq.weight` |
| `field("customer", FanOut)` (repeated scalar) | `select sq.c from restaurant_record as r, (select c from r.customer as c) sq order by sq.c` |
| `concat(field("customer", FanOut), field("name"))` | `select sq.c, r.name from restaurant_record as r, (select c from r.customer as c) sq order by sq.c, r.name` |
| `concat(field("other_id"), field("map").nest(field("entry", FanOut).nest("key")))` | `select r.other_id, sq.key from outer_record as r, (select key from r.map.entry) sq order by r.other_id, sq.key` |
| `field("stats").nest(concatenateFields("school_name", "start_date"))` | `select stats.school_name, stats.start_date from restaurant_reviewer order by stats.school_name, stats.start_date` |
| `Query.field("reviews").oneOfThem().matches(Query.field("rating").greaterThan(5))` | `where exists (select 1 from r.reviews as rv where rv.rating > 5)` |
| `setSort(field("stats").nest("start_date"))` | `order by stats.start_date` |
| `setPrimaryKey(concat(field("header").nest("path"), field("header").nest("rec_no")))` | `primary key(header.path, header.rec_no)` |

`MaterializedViewIndexGenerator` builds a trie over the projected field paths, marking the accessor a
lateral explodes over (`AnnotatedAccessor`); only a marked array accessor gets `FanType.FanOut`
(`MaterializedViewIndexGenerator.java:810-826`). So a plain nested path stays a `nest`, a projection
can mix parent columns with an exploded one in either order, and the ORDER BY fixes the key order.

Two naming traps:

- **A projection cannot produce two columns with the same name.** `concat(alarmIndex.version,
  eventIndex.version)` has to alias them apart; the ORDER BY still names the paths, which is what
  determines the key.
- **`UUID` is a primitive type name in the grammar**, so a column called `uuid` needs quoting.
  `key`, `value`, `start`, `end`, `name` and `version` do not — they are in `keywordsCanBeId` or
  `functionNameBase`, both of which `simpleId` accepts.

### One table per file, even when the source store has two

`nestedMetaData` puts `RestaurantRecord` and `RestaurantReviewer` in one store, but every query pins a
`setRecordType`, so splitting them across files loses nothing — and it is what lets both files keep
`INTERMINGLE_TABLES=true` (see below): both tables key on a `bigint`, so without the record-type prefix
their primary keys would collide.

### Schema templates use `INTERMINGLE_TABLES=true`

Every `schema_template` here ends with `WITH OPTIONS(INTERMINGLE_TABLES=true)`. Without it the
Relational layer prefixes each table's primary key with the record-type key
(`RecordLayerSchemaTemplate.Builder.addTable` asserts
`Key.Expressions.recordType().isPrefixKey(...)`), which puts that designator *inside* every index
key. The core tests use a bare primary key, so intermingling both matches the source schema and
keeps index orderings contiguous — with the type key in the middle, an ordering like
`(num_value_3_indexed, rec_no)` is not reachable and the query fails to plan. Every file here
declares exactly one table, so sharing the primary-key space is harmless.

### `equalsParameter` needs no special treatment — but not for the reason you might assume

Write the literal. A source test's `equalsParameter(...)` is covered because **`AstNormalizer` lifts every
literal into a `ConstantObjectValue`** regardless of statement type — which is why every generated explain
shows `promote(@c7 AS INT)` rather than an inline constant. The plan is parameterized either way.

It is **not** because both statement types run. `statement_type` defaults to `BOTH`, but
`preset: single_repetition_ordered` sets `repetition = 1`, and `TestBlock.getRunAsPreparedMix`
(`block/TestBlock.java:517-521`) then returns a single `random.nextBoolean()` — so exactly *one* of simple
or prepared runs, and which one varies per run because the seed defaults to `System.currentTimeMillis()`
(`TestBlock.java:195`). Even in prepared mode, only explicit `!!…!!` injections become `?`
(`QueryInterpreter.getInjections`), and none of these files use them, so both modes send byte-identical
SQL.

### `preset: single_repetition_ordered` on every block

These are deterministic plan-shape tests; the default repetition of 5 buys only plan-cache churn.

## Shared fixtures

`includes/simple-record-100.yamsql` is the SQL equivalent of
`FDBRecordStoreQueryTestBase.complexQuerySetup` — 100 `MY_SIMPLE_RECORD` rows with

```
rec_no = i, str_value_indexed = (i & 1) == 1 ? 'odd' : 'even', num_value_unique = 1000 - i,
num_value_2 = i % 3, num_value_3_indexed = i % 5, repeater = [0 .. i % 10)
```

Include it after the `schema_template` and before the `test_block`:

```yaml
---
include:
    cascades-query-tests/includes/simple-record-100.yamsql
```

The includer must declare exactly one `schema_template` containing a `MY_SIMPLE_RECORD` table with
those six columns.

## Known differences from the core tests

- **Record-type prefix — eliminated.** `WITH OPTIONS(INTERMINGLE_TABLES=true)` on every schema
  template gives each table the bare primary key the core tests use, so this is no longer a
  divergence. See *Schema templates use `INTERMINGLE_TABLES=true`* above for why it is required.
- **Unset `ARRAY` columns are `NULL` here, empty lists in the record layer.** A `repeated` proto
  field that was never set reads back as an empty list; a SQL `ARRAY` column omitted from an
  `INSERT` reads back as `NULL`.
- **`startsWith` becomes `LIKE 'x%'`, which does not bound a scan.** `Query.field(f).startsWith("x")`
  plans as the prefix range `{[x],[x]}`; `LIKE` plans as a residual filter. The knock-on effect is
  bigger than the lost range: with nothing left to bound the record scan, the planner may prefer a
  different access path altogether and the rows then arrive in *that* path's order. Seen in
  `nested-field-query-hierarchical.yamsql`, where a covering index scan wins and the result had to be
  relaxed to `unorderedResult`.
- **The record layer's blanket duplicate removal is gone.** `RecordQuery.removeDuplicates` defaults to
  `true`, so `RelationalExpression.fromRecordQuery:163` wraps every record-layer graph in a
  `LogicalDistinctExpression`. The SQL front end adds no such thing — correctly, since SQL does not
  deduplicate rows. So an `unorderedPrimaryKeyDistinctPlan` in a source assertion is usually *not* a
  planner decision to reproduce, and where the ported plan lacks it the result can be wrong: see
  `.out/issue-exists-fanout-no-distinct.md`.
- **Aggregate indexes over a repeated field always come out as a cross product.** The grouping key is
  built from the GROUP BY values alone and the aggregated column is appended with `groupBy`, which
  concatenates the two (`MaterializedViewIndexGenerator:233,438`, `GroupingKeyExpression.of:59-69`).
  With a fan-out on both sides that is a cross product, so the *entries-together* shape — each key
  paired with the value from its own entry — has no SQL spelling, however the DDL is written.
- **Synthetic (unnested) record types have no DDL.** `RecordLayerSchemaTemplate.computeIndexes:285`
  records the gap, so `addUnnestedRecordType` and `addNestedRecordType` index variants cannot be
  reproduced. Same reason `FDBCrossRecordQueryTest` stays in JUnit.
- **Direct-access aggregate reads have no SQL surface.** `evaluateAggregateFunction` with an
  `IndexAggregateFunction` becomes a `GROUP BY` query, which is a different (and stronger) test:
  it goes through the planner rather than reading the index directly.
- **Per-index options are mostly unreachable.** `WITH ATTRIBUTES` offers only
  `LEGACY_EXTREMUM_EVER` (`RelationalParser.g4` `indexAttribute`), so an `IndexOptions` value like
  `BITMAP_VALUE_ENTRY_SIZE_OPTION` cannot be set and takes its default. That is what blocks
  `GroupByTest`'s three bitmap tests, whose expectations are all about a bucket width of 4.
- **A SQL `ARRAY` is always the *wrapped* proto form.** `NullableArrayUtils.wrapArray` is applied to
  every generated index key, so a fan-out index ported from a plain `repeated` field is structurally
  the `…List { repeated X values }` variant. Behaviour is identical and the key expression gains a
  `values` hop. It also means a core test that exists to contrast the wrapped and unwrapped shapes —
  `FDBSimpleQueryGraphTest.testSimplePlanGraphWithNullableArray` — has no independent SQL content.
- **`<struct array> = []` throws at execution.** Comparing an array-of-struct column to an array
  literal plans fine and then fails per row with an internal `VerifyException`
  (`PromoteValue.java:265`); only *primitive*-element arrays work. Use an unrestricted select or a
  scalar predicate instead. See `.out/issue-promotevalue-struct-array-comparison.md`.
- **`setRequiredResults` is not a projection.** It only steers the covering-index decision; the record
  layer still returns whole records. So a source method that sets it still wants `select *`, and the
  covering difference is unavoidable. Contrast the rule above: it is the *absence* of
  `setRequiredResults` that makes a narrow SQL projection wrong, because that changes the plan.
- **ORDER BY over an expression containing a literal does not plan.** `order by <c & 4>` against an
  index over `bitand(c, 4)` gives `UnableToPlanException`
  (`long-arithmetic-functions-complex.yamsql`, `complexIndex`, both entries parked). Root cause:
  `AbstractDataAccessRule.satisfiesRequestedOrdering` compares ordering values with plain `equals`
  (`:801`, `:814`), and SQL's `ConstantObjectValue` is never structurally equal to the index's
  `LiteralValue` — so every match in the partition is discarded and planning fails outright rather
  than falling back. Computed values in *predicates* match fine, because that path goes through
  `Value.semanticEquals` under `ValueEquivalence.constantEquivalenceWithEvaluationContext`. It is the
  *literal* that breaks it, not the computation and not `select *`: `order by <a + b>` alone plans,
  because `add(a, b)` holds no constant. Filed upstream 2026-09-07. **Do not drop the sort to make
  such a query plan** — the sort is what the source tests, so keep it and park the query.
- **A source test that asserts a full record scan will get an index scan instead.** The relational layer
  hard-wires `IndexScanPreference.PREFER_INDEX` (`PlannerConfiguration.java:160`), and the Cascades cost
  model consults it when comparing an index scan against a primary scan — `PREFER_SCAN` prefers the scan,
  anything else prefers the index (`PlanningCostModel.java:497-501`). Nothing on the SQL surface sets that
  preference. So `typeFilterPlan(scanPlan(unbounded))` and `typeFilter(anything(), scan(unbounded()))`
  assertions are unreachable whenever *any* index could serve the query, even a worse one: an unfiltered
  `select *` picks an arbitrary index plus a fetch, and a filter on an unindexed column scans an unrelated
  index with the predicate residual. Affects `returned-record-limit-no-filter.yamsql` (where it costs the
  whole plan assertion) and `repeated-field-query-prefix-repeated.yamsql` query1 (where the load-bearing
  half — a *particular* index is not used — survives). A source test does still get the record scan when
  no index can serve it at all, as `repeated-field-query-prefix-repeated-nested.yamsql` shows. Watch for
  it as a row-order surprise: the expected order becomes the index key order, not the primary key order.
  `select * from my_simple_record` with no filter plans as `ISCAN(MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED <,>)`
  and emits rows in `(num_value_3_indexed, rec_no)` order.
- **Store-counter assertions are lost.** `assertDiscardedNone` / `assertDiscardedAtMost` /
  `assertLoadRecord` have no yamsql equivalent.
- **Structural plan matchers become explain strings.** `assertMatchesExactly(plan, unionOnValuesPlan(...))`
  degrades to a string comparison, which is weaker and drifts with unrelated planner work. Prefer
  `explainContains:` on the load-bearing fragment where the point is only "index X is used".
- **Plan hashes are weaker.** `planHash:` exists but is skipped in multi-server configs;
  `QueryPlanHashTest` remains the home for hash stability.

## Gotchas

- **`SELECT DISTINCT` parses but is silently ignored** — no visitor reads `DISTINCT()`. Do not use
  it to stand in for `setRemoveDuplicates(true)`; it produces wrong results with no error.
- **`USE INDEX` excludes the primary-scan candidate** (see above).
- **`LIMIT`/`OFFSET` are rejected.** Row limits are expressed with `maxRows:` plus one `result:`
  block per page.
- The runner **reads** from `build/resources/test/` but **writes** corrections to
  `src/test/resources/`. Re-run after a correction pass before comparing.

### Index states: `set schema state`, and its one-shot limitation

An index can be put in `WRITE_ONLY` or `DISABLED` state, which is how `markIndexWriteOnly` /
`markIndexDisabled` port. The directive needs the `database_id` and schema `name` in its JSON
(`Command.java:189-190`), so the file cannot use an implicit `schema_template` — create the
template, database and schema by hand and give the test block an explicit `connect:`. See
`restricted-index-write-only.yamsql`. `store_info.formatVersion` is required (`Command.java:186`);
pass 2, as every existing user does. Every `setup:` block in such a file also needs
`connect: "jdbc:embed:/__SYS?schema=CATALOG"` — with no `schema_template` there is no default
connection URI and `SetupBlock$ManualSetupBlock.parse:115` resolves one eagerly.

**It is setup-only.** All seven pre-existing users call it exactly once, before any query, and there
is no per-query form — so a source test that changes an index state *mid-test* (restrict, assert,
re-enable, assert again) can only have its first phase ported.

### Parking a query that a defect breaks

**Do not `@Disabled` the whole suite method** — that switches off every working query in the file as
collateral, and twice it hid the very entries that would have narrowed the defect. There is no
"disabled" directive, but `supported_version` is accepted per query and gives exactly the right
granularity: `QueryConfig.getSupportedVersionConfig` returns a `SkipConfig` when the check fails, and
`QueryCommand.parse:101-108` turns that command into a `SkippedCommand` — skipping **that query only**.

Use a sentinel version no server will ever reach:

```yaml
      - query: select … where rest_no > 1 and true
      # ── SKIPPED, NOT PASSING ─────────────────────────────────────────────────────────────────────
      # <what fails, the root cause with file:line, and the issue draft path>
      #
      # `supported_version: "!max_version"` is a *sentinel*, not a real version floor …
      - supported_version: "!max_version"
      - explain: ""
      - result: [ … the correct answer, left as it is … ]
```

**It must be the quoted MAX singleton.** A numeric floor does *not* work, and this cost a wasted run:
`SemanticVersionType` orders `MIN < NORMAL < CURRENT < MAX` and `SemanticVersion.compareTo` compares **by
type first** (`SemanticVersion.java:110-115, 295-301`), so `!current_version` outranks *every* numeric
version. `SupportedVersionCheck.parse` asks `supportedVersion.lesserVersions(versionsUnderTest)`, which
comes back empty for a numeric sentinel against `[!current_version]` — reported supported, nothing
skipped. `"!max_version"` outranks `CURRENT`, so it always skips. Quote it: only `!current_version` has a
registered YAML tag (`tags/CurrentVersionTag.java`), so a bare `!max_version` fails construction, while
the quoted string reaches `SemanticVersion.parse`, which resolves singleton texts before the numeric
pattern (`:219-223`).

Place it after the `query:` and before the first result config — a non-result config cannot follow a
result one (`QueryConfig.parseConfigs`).

**The sentinel only works if the block keeps at least one runnable query.** A `SkippedCommand`
contributes no executable, and `TestBlock.parse:409` asserts
`!executables.isEmpty()` — a block whose every query is skipped fails to parse outright with "have no
tests to execute". So:

| the file has | use |
|---|---|
| other working queries | the per-query `supported_version` sentinel |
| only the failing query | `@Disabled` on the suite method — there is no collateral to protect |

`and-query-to-intersection-two-covering`, `in-query-or-overlap` and `nested-field-query-doubly-nested` are
the second kind, and each says so in the note on its query.

**Verify the skip actually happened.** A sentinel that silently fails to skip looks exactly like a normal
failing test, and the file will report the real defect as a fresh failure. After adding one, confirm the
run logs the query as skipped rather than executed.

Rules:

- **Keep the correct expectation.** Never rewrite it to match the defect, and never weaken the query
  (dropping a sort, narrowing a projection) to make it plan — that produces a file which passes while no
  longer testing what its source tests.
- **Say what breaks and why, next to the query**, not in the Java driver: the reader is in the yamsql.
- Mark the method **ported, FAILING** in `MIGRATION.md` and add the analysis under *Known product
  divergences found while porting*.
- Delete the sentinel and the note when the defect is fixed; the `explain: ""` then fills in on the next
  correction pass.

The two things `supported_version` costs: the query never runs, so its `explain:` stays empty and the
file reports as passing. Neither is visible from a test report — `MIGRATION.md`'s FAILING count is the
only place the total is tracked.

## Verifying a generated plan against the source assertion

Run after each correction pass. **Compare against the source's `assertMatchesExactly` for the *Cascades*
branch, never against its `//` comment.** Tests parameterized on deferred fetch carry two plan comments —
`FDBAndQueryToIntersectionTest.testComplexQueryAndWithTwoChildren` has `Index(…) ∩ Index(…)` on one line
and `Fetch(Covering(…) ∩ Covering(…))` on the next — and the Cascades assertion is the second. Reading the
first produces spurious "divergences" in every covering plan.

Plan-hash assertions are out of scope: `planHash:` is skipped in multi-server configs, and
`QueryPlanHashTest` is the home for hash stability.

What to compare: the operator set (scan kind, covering, fetch, union/intersection kind, in-join vs
in-union, distinct, aggregate), the index names, and whether each scan bound is an equality or a range.
Ignore `MAP` — it is a projection artefact, never an access-path decision.

### Systematic divergences — expected, do not re-investigate per file

| divergence | why |
|---|---|
| SQL plans a full **index** scan where the source plans a full **record** scan | any index whose entries cover the projection is cheaper than reading records; seen in `in-query.yamsql` (`testInQueryNoIndex`), `in-query-or.yamsql` (`testInQueryEmptyList`), `sparse-index-not-implied.yamsql`. The load-bearing assertion in those tests — that a *particular* index is not used — still holds |
| covering + fetch where the source has a plain index scan, or the reverse | follows the projection. A source query with no `setRequiredResults` returns whole records; write `select *`, not a narrow column list, or the plan legitimately changes. This was a real port error in `multi-field-index-selection-prefix-scalar.yamsql`, fixed 2026-09-07 |
| in-join where the source has an in-union, or the reverse | follows `attemptFailedInJoinAsUnionMaxSize`. The SQL layer pins it at 24; the core tests set it per test (10, 20, …). Not exposed, so not reproducible |
| the source has `unorderedPrimaryKeyDistinctPlan` and the SQL plan has no distinct | `RecordQuery.removeDuplicates` defaults to `true`, so `RelationalExpression.fromRecordQuery:163` wraps **every** record-layer graph in a `LogicalDistinctExpression`. It is not a planner decision. Sound wherever the fan-out component is equality-bound; where it is range-bounded the rows are actually wrong — see `.out/issue-exists-fanout-no-distinct.md` |
| unordered-union + distinct where the SQL plan has an ordered union | same root cause as the row above |
| a residual `FILTER … LIKE` where the source has a prefix range | `startsWith` → `LIKE 'x%'`; `LIKE` cannot bound a scan. Knock-on: with nothing left to bound the record scan the planner may pick a different access path entirely (`nested-field-query-hierarchical.yamsql`) |
| "not covering because a required field is missing" cannot be reproduced | proto2 `required` has no SQL equivalent — every column is nullable, so nothing defeats the covering scan. `covering-index-header-not-covering.yamsql` keeps the index-choice coverage but has lost the property its source tested |

## Filling in explains

Every query already has an `- explain: ""` placeholder, so use **`CORRECT_EXPECTATIONS`** — not
`ADD_EXPLAINS`, which only inserts into blocks that have no explain at all. `CORRECT_EXPECTATIONS`
also switches on metrics correction, which matters here because no `.metrics.binpb` companions exist
yet and an exact `explain:` enforces planner metrics.

Locally only — CI rejects a committed `@MaintainYamlTestConfig`:

1. Add `@MaintainYamlTestConfig(YamlTestConfigFilters.CORRECT_EXPECTATIONS)` to the `@TestTemplate`
   method (or to the class, to do the whole suite at once).
2. `./gradlew :yaml-tests:quickTest --tests 'CascadesQueryTests'`
3. The framework rewrites each `- explain: ""` line in place with the actual plan, and writes
   `<file>.metrics.binpb` and `<file>.metrics.yaml`.
4. Review each generated explain against the `#` plan comment above the query — that comment is the
   plan the source test asserted, and the two should describe the same shape.
5. **Remove the annotation** before committing.

Note that the runner reads from `build/resources/test/` but writes corrections to
`src/test/resources/`, so re-run before comparing.

## Source-class mapping

Per-method detail — every source method, the file it landed in, and its current state — is in
`MIGRATION.md`. This table is the summary; the counts are of `@DualPlannerTest` methods only.

Ported so far:

| Source class | File(s) | Cases | Deferred |
|---|---|---|---|
| `FDBMultiFieldIndexSelectionTest` | `multi-field-index-selection`, `-prefix-scalar`, `-wider-covering` | 7 of 7 | — |
| `FDBFilterCoalescingQueryTest` | `filter-coalescing` | 3 of 4 | `versionRangeCoalesce` is a plain `@Test` over a VERSION index built from `Query.version()` |
| `FDBOrderingQueryTest` | `ordering-query`, `-two-indexes` | 4 of 4 | `testSortOnly` / `testSortAndFilter` are `@ParameterizedTest` without `@DualPlannerTest` |
| `FDBAndQueryToIntersectionTest` | `and-query-to-intersection`, `-choice`, `-choice-unique`, `-compound-pk`, `-num-value-2`, `-two-covering` | 10 of 13 | `testComplexQuery1g` (RANK index); `sortedIntersectionUnbounded` / `sortedIntersectionBounded` (need `CREATE TYPE AS ENUM`) |
| `FDBCoveringIndexQueryTest` | `covering-index`, `-concatenated`, `-additional-filter`, `-multi`, `-multi-value`, `-primary-key`, `-header`, `-header-compound-pk`, `-header-value`, `-header-concatenated-value`, `-header-multi`, `-header-not-covering` | 15 of 17 | `coveringOff` (needs the `DISABLED_PLANNER_RULES` connection option; rule naming unconfirmed); `coveringRedundant` (index repeats a column) |
| `FDBOrQueryToUnionTest` | `or-query-to-union`, `-ordered`, `-no-index`, `-proto-only`, `-compound-pk`, `-compound-pk-ordered`, `-ordered-and`, `-unorderable-and`, `-covering`, `-nested-predicates`, `-extra-repeated-column` | 30 of 32 | `testOrderedUnionHasRepeatedColumns*` (an index key naming `num_value_2` twice), `orderByIncludingValuePortion` (`keyWithValue` sort key), `testOrQuerySplitContinuations` |
| `FDBInQueryTest` | `in-query`, `-compound-index`, `-multi-index`, `-header`, `-or`, `-or-overlap`, `-or-compound`, `-sorted-covering`, `-sorted-extra-unordered-columns`, `-constant-value` | 33 of 38 | the `testTupleInList*` trio (row-constructor IN lists, built via `planGraph`); `enumIn`; `testInQueryParameterBad`; the `InAsOrUnionMode` / `maxReplans` parameterizations |
| `FDBRepeatedFieldQueryTest` | `repeated-field-query`, `-only-index`, `-prefix-repeated`, `-prefix-repeated-nested` | 4 of 4 | — |
| `FDBRecordStoreRepeatedQueryTest` | `record-store-repeated-query`, `record-store-repeated-query-sum-by-repeated` | 3 of 3 | — |
| `FDBReturnedRecordLimitQueryTest` | `returned-record-limit`, `-no-filter` | 3 of 4 | `testComplexLimits6` uses `setAllowedIndexes(emptyList())` to forbid every index; `USE INDEX` cannot express that |
| `FDBSortQueryIndexSelectionTest` | `sort-index-selection`, `-nested`, `-uncommon-pk` | 7 of 10 | three cases turn on `assertDiscardedAtMost`, which has no yamsql equivalent |
| `SparseIndexTest` | `sparse-index-or-predicate`, `-implied`, `-not-implied` | 3 of 4 | `sparseIndexIsNotPickedWhenDoingFullScan` needs "restrict to this index *and* still allow the scan", which `USE INDEX` cannot express |
| `FDBNestedFieldQueryTest` | `nested-field-query`, `-complex`, `-hierarchical`, `-nested-map`, `-nested-pk`, `-doubly-nested`, `-concat-nested`, `-reviewer`, `-reviewer-between`, `-reviewer-email-hometown`, `-reviewer-hometown-email`, `-reviewer-school-concat`, `-reviewer-sorted`, `-reviewer-sorted-inequality` | 17 of 17 | — |
| `GroupByTest` | `group-by-query`, `-no-index`, `-aggregate-index`, `-both-indexes` | 9 of 14 | two are `@Disabled` upstream; the three bitmap tests need `BITMAP_VALUE_ENTRY_SIZE_OPTION` = 4, which has no SQL surface. Overlaps `groupby-tests.yamsql`; the new directory owns its port |
| `FDBPermutedMinMaxQueryTest` | `permuted-min-max`, `-num-value-2`, `-repeater`, `-str-value`, `-ordering-keys` | 11 of 11 | — |
| `FDBSimpleQueryGraphTest` | `simple-query-graph`, `-hints`, `-joins`, `-tag-index`, `-name-tag-index` | 13 of 22 | 2 are `@Disabled` upstream (issue #3431); 4 `testMediumJoinTypeEvolution*` mutate `RecordMetaData` between plan and execute; 3 build null-on-empty or `LogicalFilterExpression` quantifiers that no SQL text produces |
| `FDBRestrictedIndexQueryTest` | `restricted-index-write-only`, `-disabled` | 2 of 5 | `queryAllowedIndexes` needs the `allowedForQuery` index option (no DDL surface); `queryAllowedUniversalIndex` a universal index; `indexQueryabilityFilter` a non-TRUE filter (`QueryPlan.java:649` hard-wires TRUE). Only the first phase of the two that port is reproducible — `set schema state` is setup-only, so the re-enable half has no form |
| `FDBModificationQueryTest` | `modification-query` | 5 of 7 | `testPlanUpsertGraph` needs ON CONFLICT/MERGE, absent from the grammar; `testPlanInsertExpressionBadNullAssignments` turns on proto2 `repeated`-cannot-be-null, and every SQL ARRAY is nullable |
| `RecursiveQueriesTest` | `recursive-queries`, `-forest`, `-multiples` | 6 of 13 | 7 not portable: out-of-band scan limits, DML interleaved into a paged query, runtime-generated random hierarchies, and three that assert plan-object identity/hashCode |
| `FDBVersionsQueryTest` | `versions-query-long-record`, `versions-query-other-record`, `versions-query` | 8 of 13 | 5 need a version as a query literal or bound parameter, which SQL has no spelling for — `versions-tests.yamsql` carries the attempt commented out |
| `FDBRecordStoreQueryTest` | `record-store-query-continuation`, `record-store-query-even-odd`, `record-store-query-exclude-null`, `record-store-query-null`, `record-store-query` | 6 of 12 | `enumFields*` blocked on `CREATE TYPE AS ENUM`; `queryWithShortTimeLimit`, `testPartialRecordScan`, `testUncommonPrimaryKey`, `enumFieldsWithWrongTypes` not portable |
| `FDBLongArithmeticFunctionQueryTest` | `long-arithmetic-functions-binary`, `long-arithmetic-functions-complex`, `long-arithmetic-functions-mask-1`, `long-arithmetic-functions-mask-2`, `long-arithmetic-functions-two-column`, `long-arithmetic-functions-unique-mask-4`, `long-arithmetic-functions` | 8 of 9 | the three unary cases have no SQL operator (`unaryOperator` is reachable only from a column DEFAULT clause). `complexIndex` is FAILING on a real defect |
| `RecordTypeKeyTest` | `record-type-key` | 3 of 5 | the only file using `INTERMINGLE_TABLES=false`. `testIndexScan` needs a per-table prefix mix (the option is template-wide); `testWithExplicitRecordTypeKeyComparison` a bare record-type-key predicate |
| `FDBQueryCompatibilityTest` | `record-store-query` | 1 of 1 | its single dual method folded into `record-store-query.yamsql` |

**Nothing left to start.** Every class in scope is ported or ruled out. `MIGRATION.md`
carries the per-method state, including the methods still marked `deferred` inside
ported classes.

## Stays in JUnit, and why

| Class | Why |
|---|---|
| `FDBNestedRepeatedQueryTest` | Its 11 in-scope methods are all aggregate-index tests. Each needs a synthetic (unnested) record type and the direct-access `evaluateAggregateFunction`, neither of which has a SQL surface, and the *entries-together* aggregate index shape they turn on cannot be built from SQL at all — see the two aggregate entries under *Known differences* above. **Decision: 2026-09-07, not ported.** |
| `FDBCrossRecordQueryTest` | Multi-record-type indexes over a proto union. A SQL index must have exactly one base table; `RecordLayerSchemaTemplate.computeIndexes` has an explicit TODO for universal, multi-type and synthetic indexes. |
| `QueryPlanFullySortedTest` | Asserts `RecordQueryPlan.isStrictlySorted()` — a plan property with no yamsql directive. |
| `FDBCollateJREQueryTest`, `FDBCollateQueryTestBase` | `collate_jre` is not in the SQL function grammar; collation indexes are unreachable. |
| `TempTableTest`, `TempTableTestBase` | Temp tables are a Cascades primitive; the SQL surface is `CREATE TEMPORARY FUNCTION` and recursive CTEs. |
| `FDBRecordStoreNullQueryTest` | proto2 `optional`/`required` null semantics. |
| `QueryPlanResultTest` | Asserts on the `QueryPlanInfo` the planner returns. |
| `indexes/RankIndexTest` | `IndexTypes.RANK` has no SQL surface. |
| `plan/plans/FDBComparatorPlanTest`, `FDBSelectorPlanTest` | `RecordQueryComparatorPlan` / `RecordQuerySelectorPlan` are constructed directly and are not producible from SQL. |
| `indexes/OutsideValueLikeIndexQueryTest` | Registers a custom out-of-tree index maintainer. |
| `cascades/TranslateGraphTest` | Operates on `RelationalExpression` graphs directly. |

Roughly 90 Cascades cases stay as JUnit. This is a partial migration by design.
