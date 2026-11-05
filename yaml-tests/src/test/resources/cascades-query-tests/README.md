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

17 files. `MIGRATION.md` records the state of every source method. The core tests are untouched; retiring them is a
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
`in-query-union-intersection-no-nv3.yamsql`.

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
  determines it; or
- the block is paged with `maxRows:`, where page membership depends on order regardless.

Everywhere else use `unorderedResult:`. **Tie order within equal sort keys is not derivable by
reasoning** — an in-union under `ORDER BY col DESC` iterates the IN list descending but still scans
each leg forward, giving `(col desc, pk asc)`, whereas a reverse index scan gives `(col desc, pk
desc)`. Asserting a guessed tie order is stronger than the source and produces failures that say
nothing about the product.

Ordering by a column outside the projection is fine — see `orderby.yamsql:256`,
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

### Parameters are injected, in a `statement_type: prepared` block

A value the source writes as a literal is written as a literal. **`AstNormalizer` lifts every literal into
a `ConstantObjectValue`** regardless of statement type, which is why every generated explain shows
`promote(@c7 AS INT)` rather than an inline constant.

A value the source binds as a parameter — `equalsParameter(...)`, `in("name")`, a list passed through the
`EvaluationContext` — is written as an explicit injection: `!! 0 !!` or `!! 'les aliments' !!` for a
scalar, `!! !in {[1, 3]} !!` for an `IN` list. In prepared mode `QueryInterpreter.getInjections` turns each
injection into a `?` and binds the value, so the query reaches the planner as a parameter, as in the source.

Those queries go in a test block of their own, named `<file>-prepared`, or in the file's only block when
every query in the file is parameterized. Either way the block sets

```yaml
  options:
    statement_type: prepared
```

The option has to be pinned. `statement_type` defaults to `BOTH`, but `preset: single_repetition_ordered`
sets `repetition = 1`, and `TestBlock.getRunAsPreparedMix` (`block/TestBlock.java:517-521`) then returns a
single `random.nextBoolean()` — so exactly *one* of simple or prepared runs, and which one varies per run
because the seed defaults to `System.currentTimeMillis()` (`TestBlock.java:195`). In simple mode an
injection is spliced back into the SQL text as a literal, so a `BOTH` block would test the parameter only
half the time. Literal queries stay in the main block: a source test that has both a literal and a
parameter variant becomes one query in each block.

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
- **The record layer's blanket duplicate removal is gone.** `RecordQuery.removeDuplicates` defaults to
  `true`, so `RelationalExpression.fromRecordQuery:163` wraps every record-layer graph in a
  `LogicalDistinctExpression`. The SQL front end adds no such thing — correctly, since SQL does not
  deduplicate rows. So an `unorderedPrimaryKeyDistinctPlan` in a source assertion is usually *not* a
  planner decision to reproduce.
- **A SQL `ARRAY` is always the *wrapped* proto form.** `NullableArrayUtils.wrapArray` is applied to
  every generated index key, so a fan-out index ported from a plain `repeated` field is structurally
  the `…List { repeated X values }` variant. Behaviour is identical and the key expression gains a
  `values` hop. It also means a core test that exists to contrast the wrapped and unwrapped shapes —
  `FDBSimpleQueryGraphTest.testSimplePlanGraphWithNullableArray` — has no independent SQL content.
- **`setRequiredResults` is not a projection.** It only steers the covering-index decision; the record
  layer still returns whole records. So a source method that sets it still wants `select *`, and the
  covering difference is unavoidable. Contrast the rule above: it is the *absence* of
  `setRequiredResults` that makes a narrow SQL projection wrong, because that changes the plan.
- **A source test that asserts a full record scan will get an index scan instead.** The relational layer
  hard-wires `IndexScanPreference.PREFER_INDEX` (`PlannerConfiguration.java:160`), and the Cascades cost
  model consults it when comparing an index scan against a primary scan — `PREFER_SCAN` prefers the scan,
  anything else prefers the index (`PlanningCostModel.java:497-501`). Nothing on the SQL surface sets that
  preference. So `typeFilterPlan(scanPlan(unbounded))` and `typeFilter(anything(), scan(unbounded()))`
  assertions are unreachable whenever *any* index could serve the query, even a worse one: an unfiltered
  `select *` picks an arbitrary index plus a fetch, and a filter on an unindexed column scans an unrelated
  index with the predicate residual. Affects `in-query.yamsql` (`testInQueryNoIndex`), where the IN on the
  unindexed `NUM_VALUE_2` is a residual filter over a full scan of `MY_SIMPLE_RECORD_STR_VALUE_INDEXED`; the
  load-bearing half — that no index serves the IN — survives. Watch for it as a row-order surprise: the
  expected order becomes the index key order, not the primary key order.
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
template, database and schema by hand and give the test block an explicit `connect:`. `store_info.formatVersion` is required (`Command.java:186`);
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
| SQL plans a full **index** scan where the source plans a full **record** scan | any index whose entries cover the projection is cheaper than reading records; seen in `in-query.yamsql` (`testInQueryNoIndex`) and `in-query-or.yamsql` (`testInQueryEmptyList`). The load-bearing assertion in those tests — that a *particular* index is not used — still holds |
| covering + fetch where the source has a plain index scan, or the reverse | follows the projection. A source query with no `setRequiredResults` returns whole records; write `select *`, not a narrow column list, or the plan legitimately changes |
| in-join where the source has an in-union, or the reverse | follows `attemptFailedInJoinAsUnionMaxSize`. The SQL layer pins it at 24; the core tests set it per test (10, 20, …). Not exposed, so not reproducible |
| the source has `unorderedPrimaryKeyDistinctPlan` and the SQL plan has no distinct | `RecordQuery.removeDuplicates` defaults to `true`, so `RelationalExpression.fromRecordQuery:163` wraps **every** record-layer graph in a `LogicalDistinctExpression`. It is not a planner decision. Sound wherever the fan-out component is equality-bound; where it is range-bounded the rows can differ |
| unordered-union + distinct where the SQL plan has an ordered union | same root cause as the row above |

## Filling in and updating explains

Every query has an exact `explain:`, and every file has its `<file>.metrics.binpb` and
`<file>.metrics.yaml` companions, because an exact `explain:` also enforces the planner metrics. A new
query starts with an `- explain: ""` placeholder; a planner change that moves a plan or its metrics shows
up as a failure on the existing expectation. Either way, use **`CORRECT_EXPECTATIONS`** — not
`ADD_EXPLAINS`, which only inserts into blocks that have no explain at all, and does not correct metrics.

Locally only — CI rejects a committed `@MaintainYamlTestConfig`:

1. Add `@MaintainYamlTestConfig(YamlTestConfigFilters.CORRECT_EXPECTATIONS)` to the `@TestTemplate`
   method (or to the class, to do the whole suite at once).
2. `./gradlew :yaml-tests:quickTest --tests 'CascadesQueryTests'`
3. The framework rewrites each `explain:` that differs (or is empty) in place with the actual plan, and
   rewrites `<file>.metrics.binpb` and `<file>.metrics.yaml`.
4. Review each changed explain against the `#` plan comment above the query — that comment is the
   plan the source test asserted, and the two should describe the same shape.
5. **Remove the annotation** before committing.

Note that the runner reads from `build/resources/test/` but writes corrections to
`src/test/resources/`, so re-run before comparing.

## Source-class mapping

Per-method detail — every source method, the file it landed in, and its current state — is in
`MIGRATION.md`. This table is the summary.

| Source class | File(s) |
|---|---|
| `FDBInQueryTest` | `in-query`, `-compound-index`, `-multi-index`, `-header`, `-or`, `-or-overlap`, `-or-compound`, `-sorted-extra-unordered-columns`, `-union-sargable`, `-union-intersection`, `-union-intersection-no-nv3` |
| `FDBPermutedMinMaxQueryTest` | `permuted-min-max`, `-num-value-2`, `-repeater`, `-ordering-keys` |
| `FDBSimpleQueryGraphTest` | `simple-query-graph-tag-index`, `-name-tag-index` |

## Stays in JUnit, and why

No source class moves wholesale; the core tests stay where they are (see *Status*). Of the methods
`MIGRATION.md` maps, one is **not portable** and stays a JUnit test permanently:

| Method | Why |
|---|---|
| `FDBInQueryTest.testInQueryParameterBad` | binds a non-list to an `IN` parameter; a SQL `IN` list is always a list |

