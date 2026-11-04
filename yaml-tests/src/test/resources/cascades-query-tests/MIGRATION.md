# Migration map: record query tests affected by the `ExplodeValue` change

The subset of the `fdb-record-layer-core` Cascades query tests whose plans, plan-matching code or helpers change when an
explode is represented by `TableFunctionExpression(ExplodeValue)` rather than `ExplodeExpression`. Each is ported to a
declarative yamsql file here, so that the effect of that change on the planner is visible as plan and metric differences
in one place. **The core tests are untouched.** Authoring conventions are in `README.md` in this directory.

## Legend

| state | meaning |
|---|---|
| ported, plan generated | in a yamsql file; has run and its `explain:` is filled in |
| ported, plan generated | in a yamsql file; `explain:` still `""` because the file has not yet completed a run |
| **ported, FAILING** | in a yamsql file, expectation believed correct, product does not match. The query is parked: a `supported_version: "!max_version"` sentinel where the file has other working queries, `@Disabled` on the suite method where it does not — see *Parking a query that a defect breaks* in `README.md` |
| covered indirectly | no dedicated query; exercised by another entry or by a test configuration |
| deferred | portable in principle, not done yet |
| not portable | cannot be expressed in SQL; stays a JUnit test permanently |
| out of scope | plain `@Test` / `@ParameterizedTest` without `@DualPlannerTest`, i.e. old planner only |

`annotation` records how the source method is annotated: `dual` = `@DualPlannerTest`,
`cascades` = `@DualPlannerTest(planner = CASCADES)`, `old` = `@DualPlannerTest(planner = OLD)`.

## `FDBInQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBInQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testInQueryNoIndex` | dual | `in-query.yamsql` | ported, plan generated | expected 67 rows |
| `testInQueryNoIndexWithParameter` | dual | `in-query.yamsql` | ported, plan generated | `valuesThree` is a parameter in the source, so it is `!! !in {[1,3]} !!` in the `in-query-prepared` block (`statement_type: prepared`); the IN stays a residual filter. 33 rows |
| `testInQueryIndex` | dual | `in-query.yamsql` | ported, plan generated | 2 queries (forward and reverse), IN list already in order: `(1, 2, 4)` |
| `testInQueryCoveringIndex` | dual | `in-query.yamsql` | ported, plan generated | 2 queries (forward and reverse); only REC_NO projected |
| `testInQueryParameter` | dual | `in-query.yamsql` | ported, plan generated | own query, no ORDER BY; the IN list is a parameter (`!! !in {[1, 3, 4]} !!`) in the `in-query-prepared` block |
| `testInQuerySortedByParameter` | dual | `in-query.yamsql` | ported, plan generated | own queries, forward and reverse, sorted on the IN column; parameter-injected in the `in-query-prepared` block |
| `testInQueryParameter2` | dual | `in-query-multi-index.yamsql` | ported, plan generated | 10 queries: p in 0..4 x forward/reverse; `p` and the `valueThrees` list are parameter-injected, block is `statement_type: prepared` |
| `testInQueryParameter2UsingCoveringIndexScans` | dual | `in-query-multi-index.yamsql` | ported, plan generated | 10 queries: p in 0..4 x forward/reverse, covering variant; parameter-injected, block is `statement_type: prepared` |
| `testInQueryParameterBad` | dual | — | not portable | binds a non-list to an IN parameter; a SQL IN list is always a list |
| `testInQueryIndexSorted` | dual | `in-query.yamsql` | ported, plan generated | 2 queries (forward and reverse) with the IN list out of order, `(1, 4, 2)`, so the SORTED flag does work; ordered `result` |
| `testInQueryIndexSortedDifferently` | dual | `in-query.yamsql` | ported, plan generated | 2 queries, forward and reverse; the sort is on a different column than the IN |
| `testParameterizedInQuerySortedWithExtraUnorderedColumns` | dual | `in-query-sorted-extra-unordered-columns.yamsql` | ported, plan generated | own queries, forward and reverse, in the `-prepared` block: `num_value_2` and the IN list are parameter-injected. See the file header for the comparison-key divergence |
| `inQueryWithSortBySecondFieldOfCompoundIndex` | dual | `in-query-compound-index.yamsql` | ported, plan generated |  |
| `inQueryWithSortAndRangePredicateOnSecondFieldOfCompoundIndex` | dual | `in-query-compound-index.yamsql` | ported, plan generated |  |
| `testListInQuerySortedWithExtraUnorderedColumns` | dual | `in-query-sorted-extra-unordered-columns.yamsql` | ported, plan generated | own queries, forward and reverse, with a literal IN list. See the file header for the comparison-key divergence |
| `inQueryWithSortAndParameter` | dual | `in-query-compound-index.yamsql` | ported, plan generated | the IN list is a parameter, so it is in the `-prepared` block (`!! !in {[1, 4, 2]} !!`) |
| `testInWithNesting` | dual | `in-query-header.yamsql` | ported, plan generated |  |
| `testMultipleInQueryIndex` | dual | `in-query-header.yamsql` | ported, plan generated |  |
| `testMultipleInQueryIndexSorted` | dual | `in-query-header.yamsql` | ported, plan generated |  |
| `testInWithLimit` | dual | `in-query-header.yamsql` | ported, plan generated | `maxRows: 3` pages (SQL has no LIMIT clause); the source `normalizeNestedFields` parameterization is old-planner only |
| `testInWithContinuation` | dual | `in-query-header.yamsql` | ported, plan generated | `maxRows: 1` pagination across the whole result, so page boundaries fall inside and between IN elements |
| `testInQueryOr` | dual | `in-query-or.yamsql` | ported, plan generated |   |
| `testInQueryOrOverlap` | dual | `in-query-or-overlap.yamsql` | ported, plan generated | OR of two IN lists sharing a value (905): a union of two in-joins, deduplicated. Previously raised a ClassCastException; it passes now |
| `testInQueryOrDifferentCondition` | dual | `in-query-or.yamsql` | ported, plan generated | one leg is a range, the other an AND whose IN stays a residual filter  |
| `testInQueryOrCompound` | dual | `in-query-or-compound.yamsql` | ported, plan generated | IN inside an OR inside an AND, all bound by MULTI_INDEX  |
| `testInQueryOrMultipleIndexes` | dual | `in-query-or.yamsql` | ported, plan generated | the two legs use two different indexes  |
| `enumIn` | dual | — | deferred | needs CREATE TYPE AS ENUM |
| `testInQueryEmptyList` | dual | `in-query-or.yamsql` | ported, plan generated | an empty IN list cannot be a SQL literal; `!! !in {[]} !!` in the `in-query-or-prepared` block (`statement_type: prepared`) |
| `testInUnionWithNonSargable` | dual | `in-query-or.yamsql` | ported, plan generated | NUM_VALUE_2 has no index, so the IN cannot become an in-union; the `maxNumReplansForInToJoin` parameterization collapses  |
| `testInUnionWithSomeSargable` | dual | `in-query-union-sargable.yamsql` | ported, plan generated | 3 queries, one per `str_list` binding; the replan and index parameterizations collapse  Both IN lists are parameters in the source, so they are parameter-injected and the block is `statement_type: prepared`. |
| `testInUnionWithIntersectionOnTwoPredicates` | dual | `in-query-union-intersection.yamsql`, `in-query-union-intersection-no-nv3.yamsql` | ported, plan generated | 3 queries per index variant; the `dropNumValue3Index = true` variant is the `-no-nv3` file, where MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED is absent so the sort must come from one of the two composite indexes  Both IN lists are parameters in the source, so they are parameter-injected and the blocks are `statement_type: prepared`. |

## `FDBPermutedMinMaxQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBPermutedMinMaxQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `selectMaxWithInOrderByMax` | cascades + `@MethodSource` | `permuted-min-max.yamsql` | ported, plan generated | in-union, comparison key (m, num_value_2, num_value_3_indexed). Three representative IN lists including a repeated value, since the source deduplicates its expectation, and an empty one  `byLiteral` and `byConstantObjectValue` are the literal block; `byParameter` is the `-prepared` block. |
| `testMaxWithInAndDupes` | cascades + `@MethodSource` | `permuted-min-max-num-value-2.yamsql` | ported, plan generated | "dupes" is literal: (str_value_indexed, num_value_3_indexed) fixes i mod 10 and those ten records cover every residue mod 3, so max(num_value_2) is 2 in *every* group. Unordered — the whole sort key is tied  `byLiteral` and `byConstantObjectValue` are the literal block; `byParameter` is the `-prepared` block. |
| `testSortedMaxWithInOnRepeater` | cascades + `@MethodSource` | `permuted-min-max-repeater.yamsql` | ported, plan generated | in-union over repeater values; representative lists from the source's 81 pairs  `byLiteral` and `byConstantObjectValue` are the literal block; `byParameter` is the `-prepared` block. |
| `testMaxUniqueByStr2And3WithDifferentOrderingKeys` | cascades + `@MethodSource` | `permuted-min-max-ordering-keys.yamsql` | ported, plan generated | four ORDER BY prefixes, each with a different expected plan shape: in-**join** with no ordering constraint, in-**union** with one. Own file — the hook removes the unique index because the six extra records duplicate NUM_VALUE_UNIQUE. Unordered: the source uses `containsInAnyOrder` plus a separate monotonicity check that yamsql cannot express  `byLiteral` and `byConstantObjectValue` are the literal block; `byParameter` is the `-prepared` block. |

## `FDBSimpleQueryGraphTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBSimpleQueryGraphTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testSimpleExistentialPredicateOnSimpleIndex` | cascades + `@BooleanSource` | `simple-query-graph-tag-index.yamsql` | ported, plan generated | both the EQUALS and IN variants  The tag value is a parameter in the source, so both variants are parameter-injected in a `statement_type: prepared` block. |
| `testEqualityAndSimpleExistentialPredicate` | cascades | `simple-query-graph-name-tag-index.yamsql` | ported, plan generated | IN on a scalar column plus an existential over the array, one index over concat(name, tags.value)  Both `name` and the tag value are parameters in the source, so they are parameter-injected in a `statement_type: prepared` block. |
