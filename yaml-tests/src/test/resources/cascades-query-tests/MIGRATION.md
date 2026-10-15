# Migration map: `fdb-record-layer-core` query tests to `cascades-query-tests`

Generated as part of moving the Cascades-planner query tests out of `fdb-record-layer-core` and into
the declarative yamsql suite. **The core tests are untouched** — retiring them is a separate, later
step, so every source method listed here still exists and still runs.

New tests live in `yaml-tests/src/test/resources/cascades-query-tests/<file>.yamsql`, registered by
`yaml-tests/src/test/java/CascadesQueryTests.java`. Authoring conventions are in `README.md` in the
same directory.

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

## `FDBMultiFieldIndexSelectionTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBMultiFieldIndexSelectionTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testPrefixScalar` | dual | `multi-field-index-selection-prefix-scalar.yamsql` | ported, plan generated | 2 queries; source asserts the ordered list [3, 1] (NULL sorts first). **Corrected 2026-09-07:** was projecting REC_NO alone, which made the plan a COVERING scan where the source asserts a plain index scan; now `select *`, plans need regenerating |
| `testComplexQuery2` | dual | `multi-field-index-selection.yamsql` | ported, plan generated |  |
| `testComplexQuery3` | dual | `multi-field-index-selection.yamsql` | ported, plan generated |  |
| `testComplexQueryDenorm` | dual | `multi-field-index-selection.yamsql` | ported, plan generated | must plan identically to testComplexQuery3; compare the two explains |
| `testComplexQuery4` | dual | `multi-field-index-selection.yamsql` | ported, plan generated |  |
| `testComplexQuery8` | dual | `multi-field-index-selection.yamsql` | ported, plan generated |  |
| `testWiderCoveringIndex` | dual | `multi-field-index-selection-wider-covering.yamsql` | ported, plan generated | empty result: NUM_VALUE_2 is i%3 so never >= 10; source never executes it either |

## `FDBFilterCoalescingQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBFilterCoalescingQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `simpleRangeCoalesce` | dual | `filter-coalescing.yamsql` | ported, plan generated | own file: source hook is NO_HOOK |
| `versionRangeCoalesce` | @Test | — | not portable | VERSION index built from Query.version(); no SQL surface |
| `duplicateFilters` | dual | `filter-coalescing-multi-index.yamsql` | ported, plan generated |  |
| `overlappingFilters` | dual | `filter-coalescing-multi-index.yamsql` | ported, plan generated |  |

## `FDBOrderingQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBOrderingQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testSortOnly` | @ParameterizedTest | — | out of scope | no @DualPlannerTest: old planner only |
| `testSortAndFilter` | @ParameterizedTest | — | out of scope | no @DualPlannerTest: old planner only |
| `testUnionOrdered` | dual | `ordering-query.yamsql` | ported, plan generated | source asserts the ordered list [1,5,4,3,2,6] |
| `testUnionOrdered2` | dual | `ordering-query-two-indexes.yamsql` | ported, plan generated |  |
| `testUnionOrdered3` | dual | `ordering-query-two-indexes.yamsql` | ported, plan generated |  |
| `testCoveringOrdered` | dual | `ordering-query.yamsql` | ported, plan generated | source asserts the ordered list ['b','ab'] |

## `FDBAndQueryToIntersectionTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBAndQueryToIntersectionTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testComplexQueryAndWithTwoChildren` | dual | `and-query-to-intersection.yamsql` | ported, plan generated |  |
| `testComplexQueryAndWithTwoChildren2` | dual | `and-query-to-intersection.yamsql` | ported, plan generated | same query as above; source differs only in old-planner deferred-fetch config |
| `testComplexQueryAndWithTwoChildren3` | dual | `and-query-to-intersection-two-covering.yamsql` | **ported, FAILING** | returns 2 rows instead of 10; see the file header and the planner-bug note below |
| `testComplexQueryAndWithMultipleChildren` | dual | `and-query-to-intersection-num-value-2.yamsql` | ported, plan generated |  |
| `testComplexQueryAndWithIncompatibleFilters` | dual | `and-query-to-intersection.yamsql` | ported, plan generated | startsWith -> LIKE; see divergences below |
| `testComplexQueryAndWithSomeIncompatibleFilters` | dual | `and-query-to-intersection-num-value-2.yamsql` | ported, plan generated | startsWith -> LIKE |
| `testComplexQuery1g` | dual | — | not portable | GROUPED_INDEX is an IndexTypes.RANK index; RANK has no SQL surface |
| `testComplexLimits4` | dual | `and-query-to-intersection.yamsql` | ported, plan generated | row limit not reproduced: no sort in the source, so page membership is not derivable; asserts the full result, paging covered by ForceContinuations |
| `testAndQuery7` | dual | `and-query-to-intersection-compound-pk.yamsql` | ported, plan generated |  |
| `intersectionVersusRange` | dual | `and-query-to-intersection-choice.yamsql` | ported, plan generated | empty result; source never executes it |
| `intersectionVersusRange2` | cascades | `and-query-to-intersection-choice-unique.yamsql` | ported, plan generated | empty result; source never executes it |
| `sortedIntersectionUnbounded` | dual | — | deferred | needs CREATE TYPE AS ENUM (TestRecordsEnum shapes); docs flag ENUM queries as not working |
| `sortedIntersectionBounded` | dual | — | deferred | same as above |
| `intersectionVisitorOnComplexComparisonKey` | @Test | — | out of scope | old planner only |

## `FDBCoveringIndexQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBCoveringIndexQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `coveringSimple` | dual | `covering-index.yamsql` | ported, plan generated |  |
| `coveringOff` | dual | — | deferred | disables individual planner rules; SQL exposes only the DISABLED_PLANNER_RULES connection option, rule naming unconfirmed |
| `coveringSortNoFilter` | dual | `covering-index.yamsql` | ported, plan generated |  |
| `coveringSimpleInsufficient` | dual | `covering-index.yamsql` | ported, plan generated | reverse sort |
| `notCoveringRecordScan` | dual | `covering-index.yamsql` | ported, plan generated | Cascades and the old planner differ here; the port follows Cascades |
| `notCoveringWithAdditionalFilter` | dual | `covering-index.yamsql` | ported, plan generated |  |
| `coveringWithAdditionalFilter` | dual | `covering-index-additional-filter.yamsql` | ported, plan generated |  |
| `coveringWithAdditionalNestedFilter` | dual | `covering-index-header-multi.yamsql` | ported, plan generated | empty result; source never executes it |
| `coveringMulti` | dual | `covering-index-multi.yamsql` | ported, plan generated |  |
| `coveringMultiValue` | dual | `covering-index-multi-value.yamsql` | ported, plan generated | value part via INCLUDE (...); generated plan shows NUM_VALUE_2: VALUE:[0] as the source expects |
| `coveringWithHeaderValue` | dual | `covering-index-header-value.yamsql` | ported, plan generated | nested value part via a partial ORDER BY; INCLUDE cannot take a dotted path |
| `coveringWithHeaderConcatenatedValue` | dual | `covering-index-header-concatenated-value.yamsql` | ported, plan generated | own file: same index name as above but a different value part |
| `coveringWithHeader` | dual | `covering-index-header-compound-pk.yamsql` | ported, plan generated | 2 queries; the only header test that populates the store |
| `coveringConcatenatedFields` | dual | `covering-index-concatenated.yamsql` | ported, plan generated |  |
| `notCoveringWithRequiredFieldsNotAvailable` | dual | `covering-index-header-not-covering.yamsql` | ported, plan generated | **Divergent (2026-09-07):** the source's point is that proto2 `required` `path` is absent from the index so the scan cannot be covering. SQL columns are all nullable, so the plan here *is* covering. Index choice still matches; the property under test does not survive.  |
| `queryCoveringAggregate` | @Test | — | out of scope | calls planCoveringAggregateIndex on the old planner directly |
| `nestedRepeatedSplitCoveringIndex` | @Test | — | out of scope | @Disabled in the source |
| `coveringRedundant` | dual | — | deferred | index repeats NUM_VALUE_2 twice; duplicate column in CREATE INDEX AS SELECT unconfirmed |
| `coveringPrimaryKey` | dual | `covering-index-primary-key.yamsql` | ported, plan generated | keyWithValue via ON ... INCLUDE; generated plan matches the source comment |
| `coveringWideFunctionKey` | @Test | — | out of scope | old planner only |

## `FDBOrQueryToUnionTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBOrQueryToUnionTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testComplexQuery6` | dual | `or-query-to-union.yamsql` | ported, plan generated |  |
| `testComplexQuery6Continuations` | dual | `or-query-to-union.yamsql` | covered indirectly | not a separate entry: the ForceContinuations config re-runs every query through continuations |
| `testOrQuery1` | dual | `or-query-to-union.yamsql` | ported, plan generated |  |
| `testOrQueryPlanEquals` | dual | `or-query-to-union.yamsql` | ported, plan generated | reversed leg order; must plan identically to testOrQuery1 |
| `testOrQuery2` | dual | `or-query-to-union.yamsql` | ported, plan generated |  |
| `testOrQuery3` | dual | `or-query-to-union.yamsql` | ported, plan generated |  |
| `testOrQuery4` | dual | `or-query-to-union.yamsql` | ported, plan generated |  |
| `testOrQuery5` | dual | `or-query-to-union.yamsql` | ported, plan generated | expected 60 rows (the deduplicated count the source asserts as 50+10) |
| `testOrQuery5WithLimits` | dual | `or-query-to-union.yamsql` | covered indirectly | the query is identical to testOrQuery5, which is already asserted there; its only distinguishing feature is the returned-row limit, which is not reproducible without a predictable page order |
| `testOrQuery6` | dual | `or-query-to-union-str-3-index.yamsql` | ported, plan generated | additive hook: STR_VALUE_3_INDEX only, so MULTI_INDEX and the fan-out index are absent |
| `testUnorderableOrQueryWithAnd` | dual | `or-query-to-union-unorderable-and.yamsql` | ported, plan generated | expected 40 (deduplicated); source asserts 40 with dedup and 53 without |
| `testOrQuery7` | dual | `or-query-to-union-compound-pk.yamsql` | ported, plan generated |  |
| `testOrQueryOrdered` | dual | `or-query-to-union-compound-pk-ordered.yamsql` | ported, plan generated |  |
| `testOrderedOrQueryWithMultipleValuesForEarlierColumn` | dual | `or-query-to-union-ordered.yamsql` | ported, plan generated |  |
| `testOrderedOrQueryWithIntermediateUnorderedColumn` | dual | `or-query-to-union-ordered.yamsql` | ported, plan generated | issue #2336 |
| `testNestedPredicates` | dual | `or-query-to-union-nested-predicates.yamsql` | ported, plan generated | Now portable: the RestaurantReviewer schema exists (nested-field-query-reviewer.yamsql). 2 queries, forward and reverse; normalizeNestedFields is old-planner-only so that is the whole Cascades parameterization. Index name mirrors the source typo Reviewer$catgory_stats. Projection is the source setRequiredResults list rather than select *, because the source asserts covering union legs with no fetch. The source saves no records; 7 rows invented, START_DATE distinct so both sorts are total. Lost: plan hashes and the plan.isReverse() assertion. VERIFIED (2026-09-08): plan matches the source exactly - union of two covering REVIEWER_CATGORY_STATS scans, each with two equalities, COMPARE BY (STATS.START_DATE, NAME, STATS.HOMETOWN, ID), which is the source comparison key at :1368 in the same order. Reverse variant carries REVERSE on both legs. |
| `testNestedPredicatesWithExtraUnorderedColumns` | dual | `or-query-to-union-nested-predicates.yamsql` | ported, plan generated | Same file, same index and data as testNestedPredicates; differs only in sorting by stats.start_date alone, leaving NAME an extra unordered column. 2 queries, forward and reverse. VERIFIED (2026-09-08): same union, COMPARE BY (STATS.START_DATE, STATS.HOMETOWN, NAME, ID) - the source key at :1416, in the same order. |
| `testOrderedUnionDoesNotUseIndexWithExtraRepeatedColumn` | dual | `or-query-to-union-extra-repeated-column.yamsql` | ported, plan pending | DEFERRAL WAS STALE (2026-09-08): the recorded blocker was "needs a spike: the index mixes scalar columns with an unnested array element in one composite key". That shape is now proven - PREFIX_REPEATED is (num_value_2, repeater FanOut) and FANOUT_INDEX is (header.path, repeated_int FanOut), both accepted and matched - so STR_3_REPEATER as (str_value_indexed, num_value_3_indexed, repeater FanOut) is the same family. 1 query: baseParams varies only deferFetch and omitPrimaryKeyInOrderingKey, both old-planner-only, and pins sortReverse false. This one executes the hazard rather than only asserting the shape: 10 of the 100 fixture records have an empty REPEATER, so a plan that wrongly used STR_3_REPEATER would return 90 rows instead of 100. Row order is (nv3, rec_no), fixed by the forward index scan rather than by the ORDER BY. Lost: plan hashes. |
| `testOrderedUnionHasRepeatedColumnsInPrefixAndOrdering` | dual | — | deferred | **blocked**: the index key repeats NUM_VALUE_2 twice — `(num_value_2, str_value_indexed, num_value_3_indexed, num_value_2)`. Same unknown as `coveringRedundant`: whether a duplicate column in `CREATE INDEX ... AS SELECT` is accepted |
| `testOrderedUnionHasRepeatedColumnsNotInPrefix` | dual | — | deferred | **blocked**: same duplicated NUM_VALUE_2, ordered `(str_value_indexed, num_value_2, num_value_3_indexed, num_value_2)` |
| `orderByIncludingValuePortion` | dual | `or-query-to-union-key-with-value.yamsql` | ported, plan generated | 4 queries (two sort keys x two directions); keyWithValue split expressed as a partial index ORDER BY |
| `testOrQueryChildReordering` | dual | `or-query-to-union-ordered.yamsql` | ported, plan generated | 2 queries (both leg orders); reverse primary-key sort |
| `testOrQueryChildReordering2` | dual | `or-query-to-union-ordered.yamsql` | ported, plan generated | 2 queries (both leg orders) |
| `testOrderedOrQueryWithAnd` | dual | `or-query-to-union-ordered-and.yamsql` | ported, plan generated | expected 51 rows, matching the source assertion |
| `testComplexLimits5` | dual | `or-query-to-union-ordered.yamsql` | ported, plan generated | row limit not reproduced, as testComplexLimits4 |
| `testOrQueryDenorm` | dual | `or-query-to-union-ordered.yamsql` | ported, plan generated |  |
| `testOrQuerySplitContinuations` | dual | `or-query-to-union-split-continuations.yamsql` | ported, plan generated | own data set: NUM_VALUE_3_INDEXED is i / 10, so each value covers a contiguous block of ten primary keys — that contiguity is the point |
| `testOrQueryNoIndex` | dual | `or-query-to-union-no-index.yamsql` | ported, plan generated |  |
| `unionVisitorOnComplexComparisonKey` | @Test | — | out of scope | old planner only |
| `deferFetchOnUnionWithInnerFilter` | dual | `or-query-to-union-covering.yamsql` | ported, plan generated | startsWith -> LIKE |
| `testOrQueryToDistinctUnion` | dual | `or-query-to-union.yamsql` | ported, plan generated |  |
| `testOrQueryToDistinctUnionWithPartialDefer` | dual | `or-query-to-union-proto-only.yamsql` | ported, plan generated |  |
| `testComplexOrQueryToDistinctUnion` | dual | `or-query-to-union-proto-only.yamsql` | ported, plan generated | nine open-interval legs, none satisfiable; source never executes it |

## `FDBInQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBInQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testInQueryNoIndex` | dual | `in-query.yamsql` | ported, plan generated | expected 67 rows |
| `testInQueryNoIndexWithParameter` | dual | `in-query.yamsql` | ported, plan generated | expected 33 rows |
| `testInQueryIndex` | dual | `in-query.yamsql` | ported, plan generated | 2 queries (forward and reverse) |
| `testInQueryWithConstantValueUnsorted` | cascades | `in-query-constant-value.yamsql` | ported, plan pending | DEFERRAL WAS STALE (2026-09-08): recorded as "ConstantObjectValue via planGraph", but AstNormalizer lifts every SQL literal into a ConstantObjectValue, so a plain IN (1, 2, 4) is already the shape the source builds by hand - the same reason testConstantObjectInQuerySortedWithExtraUnorderedColumns ported cleanly. 4 queries, the source four bindings (60/20/40/40 rows); they normalize to one query and so share a plan-cache entry, which is harmless since the source also asserts one plan for all four. Own file because complexQuerySetup(NO_HOOK) means only the three proto-implied indexes. unorderedResult, the source being LogicalSortExpression.unsorted. Lost: assertDiscardedNone; plan hashes. |
| `testTupleInList` | cascades | — | deferred | row-constructor IN list built via planGraph; SQL support unconfirmed |
| `testTupleInListNoIndex` | cascades | — | deferred | as above |
| `testTupleInListCannotPromote` | cascades | — | deferred | as above |
| `testInQueryCoveringIndex` | dual | `in-query.yamsql` | ported, plan generated |  |
| `testInQueryParameter` | dual | `in-query.yamsql` | ported, plan generated |  |
| `testInQuerySortedByParameter` | dual | `in-query.yamsql` | ported, plan generated | same query as testInQueryParameter after collapsing the reverse parameter |
| `testInQuerySortedByConstantValue` | cascades | `in-query-constant-value.yamsql` | ported, plan pending | Same file and same stale blocker as testInQueryWithConstantValueUnsorted. 2 queries, forward and reverse. Row order is not fixed by the ORDER BY (20 records share each num_value_3_indexed) but by the plan: forward gives (nv3 asc, rec_no asc), and reverse iterates the IN list descending while scanning each leg forward - which is exactly what the source isNotReverse() on the leg asserts - giving (nv3 desc, rec_no asc). setAttemptFailedInJoinAsUnionMaxSize(100) is not on the SQL surface; the relational layer pins 24 and both lists are far shorter, so it cannot matter. Lost: assertDiscardedAtMost(reverse ? 40 : 0); plan hashes. |
| `testInQueryParameter2` | dual | `in-query-multi-index.yamsql` | ported, plan generated | 5 queries, one per p in 0..4 |
| `testInQueryParameter2UsingCoveringIndexScans` | dual | `in-query-multi-index.yamsql` | ported, plan generated | 5 queries, covering variant |
| `testInQueryParameterBad` | dual | — | not portable | binds a non-list to an IN parameter; a SQL IN list is always a list |
| `testNotInQueryParameterBad` | dual | `in-query.yamsql` | ported, plan generated | portable part only: a plain NOT IN, 40 rows |
| `testInQueryIndexSorted` | dual | `in-query.yamsql` | ported, plan generated | same query as testInQueryIndex |
| `testInQueryIndexSortedDifferently` | dual | `in-query.yamsql` | ported, plan generated | sort on a different column than the IN |
| `testParameterizedInQuerySortedWithExtraUnorderedColumns` | dual | `in-query-sorted-extra-unordered-columns.yamsql` | ported, plan generated | One of three methods that collapse into this file: parameter, ConstantObjectValue and literal IN list are the same SQL once AstNormalizer lifts literals. 2 queries here, forward and reverse; the other 10 parameter combinations are RecordQueryPlanner-only. Projection is the source setRequiredResults list, since the source asserts covering scans. 21 rows, NUM_VALUE_UNIQUE unique so both sorts are total. Lost: assertDiscardedNone / assertDiscardedAtMost(79); plan hashes. VERIFIED (2026-09-08) with one divergence. In-union over a covering INDEX_FOR_IN_UNION scan with two equalities, as the source asserts, and the IN list arrives as arrayDistinct(...) so duplicates are removed before the union. But COMPARE BY has three values, not the source four (:1104) - NUM_VALUE_3_INDEXED is missing, because the MAP sits inside the in-union and the key is computed after it, so a projection narrower than the index shrinks the key. Still total, NUM_VALUE_UNIQUE being unique. The source no-mapPlan form is also unreachable, since any SQL projection emits a MAP. |
| `testConstantObjectInQuerySortedWithExtraUnorderedColumns` | cascades | `in-query-sorted-extra-unordered-columns.yamsql` | ported, plan generated | Same file. What survives the collapse is its duplicate IN list (1,1,2,2,4,4) and its four projected columns, which are the second pair of queries. NOTE: the point of the duplicate list is assertDiscardedNone, which yamsql cannot express, so the port checks only that the rows are right. VERIFIED (2026-09-08): COMPARE BY (NUM_VALUE_UNIQUE, STR_VALUE_INDEXED, NUM_VALUE_3_INDEXED, REC_NO) matches the source (:1163) exactly, in the same order, and the covering scan sits under a MAP as the source mapPlan asserts. arrayDistinct on the IN list is the mechanism behind the duplicate-list point. |
| `inQueryWithSortBySecondFieldOfCompoundIndex` | dual | `in-query-compound-index.yamsql` | ported, plan generated |  |
| `inQueryWithSortAndRangePredicateOnSecondFieldOfCompoundIndex` | dual | `in-query-compound-index.yamsql` | ported, plan generated |  |
| `testListInQuerySortedWithExtraUnorderedColumns` | dual | `in-query-sorted-extra-unordered-columns.yamsql` | ported, plan generated | Same file; covered by the same first pair of queries as the parameterized variant. VERIFIED (2026-09-08): shares the first pair of queries with the parameterized variant, so the same result holds - in-union over a covering INDEX_FOR_IN_UNION scan with two equalities, and the same three-value COMPARE BY where the source asserts four (:1475). See that row. |
| `inQueryWithSortAndParameter` | dual | `in-query-compound-index.yamsql` | ported, plan generated | same query as the range-predicate case; kept for a 1:1 mapping |
| `cnfAsInQuery` | @Test | — | out of scope | old planner only |
| `testInWithNesting` | dual | `in-query-header.yamsql` | ported, plan generated |  |
| `testMultipleInQueryIndex` | dual | `in-query-header.yamsql` | ported, plan generated |  |
| `testMultipleInQueryIndexSorted` | dual | `in-query-header.yamsql` | ported, plan generated |  |
| `testInWithLimit` | dual | `in-query-header.yamsql` | covered indirectly | not a separate entry: covered by ForceContinuations |
| `testInWithContinuation` | dual | `in-query-header.yamsql` | covered indirectly | not a separate entry: covered by ForceContinuations |
| `testOneOfThemIn` | @Test | — | out of scope | old planner only |
| `testOneOfThemInParameter` | @Test | — | out of scope | old planner only |
| `testOneOfThemInSorted` | @Test | — | out of scope | old planner only |
| `testRecordFunctionInGrouped` | @Test | — | out of scope | old planner only |
| `testRecordFunctionInUngrouped` | @Test | — | out of scope | old planner only |
| `testInQueryOr` | dual | `in-query-or.yamsql` | ported, plan generated |   |
| `testInQueryOrOverlap` | dual | `in-query-or-overlap.yamsql` | **ported, FAILING** | executing it raises `ClassCastException: ByteString$LiteralByteString cannot be cast to Comparable`. The expectation is correct — the source executes the same plan and asserts these five records under both planners. Isolated into its own file and `@Disabled`; see the file header  |
| `testInQueryOrDifferentCondition` | dual | `in-query-or.yamsql` | ported, plan generated | one leg is a range, the other an AND whose IN stays a residual filter  |
| `testInQueryOrCompound` | dual | `in-query-or-compound.yamsql` | ported, plan generated | IN inside an OR inside an AND, all bound by MULTI_INDEX  |
| `testInQueryOrMultipleIndexes` | dual | `in-query-or.yamsql` | ported, plan generated | the two legs use two different indexes  |
| `enumIn` | dual | — | deferred | needs CREATE TYPE AS ENUM |
| `testInQueryEmptyList` | dual | `in-query-or.yamsql` | ported, plan generated | empty IN list via the `!! !in {[]} !!` idiom empty IN list; !in {[]} idiom |
| `testInQueryWithNonSargable` | @Test | — | out of scope | old planner only |
| `testInUnionWithNonSargable` | dual | `in-query-or.yamsql` | ported, plan generated | NUM_VALUE_2 has no index, so the IN cannot become an in-union; the `maxNumReplansForInToJoin` parameterization collapses  |
| `testInUnionWithSomeSargable` | dual | `in-query-union-sargable.yamsql` | ported, plan generated | 3 queries, one per `str_list` binding; the replan and index parameterizations collapse  |
| `testInUnionWithIntersectionOnTwoPredicates` | dual | `in-query-union-intersection.yamsql`, `in-query-union-intersection-no-nv3.yamsql` | ported, plan generated | 3 queries per index variant; the `dropNumValue3Index = true` variant is the `-no-nv3` file, where MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED is absent so the sort must come from one of the two composite indexes |

## `FDBRepeatedFieldQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBRepeatedFieldQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `doublyRepeated` | @Test | — | out of scope | old planner only |
| `doublyRepeatedComparison` | @Test | — | out of scope | old planner only |
| `sortRepeated` | @Test | — | out of scope | old planner only |
| `sortRepeated2` | @Test | — | out of scope | old planner only |
| `testComplexQuery7` | dual | `repeated-field-query.yamsql` | ported, plan generated | oneOfThem() rendered as a correlated EXISTS over the unnested array |
| `testPrefixRepeated` | dual | `repeated-field-query-prefix-repeated.yamsql` | ported, plan generated | 2 queries. Own file for the (scalar, fan-out) composite PREFIX_REPEATED. Source data verbatim, 3 records; REPEATER written as [] for REC_NO 3. oneOfThem becomes the correlated EXISTS. The SQL plan will lack the source primaryKeyDistinct, which comes from RecordQuery.removeDuplicates, not the fan-out; harmless because query2 binds both index components to equalities. Lost: assertDiscardedAtMost(1) and assertDiscardedNone; plan hashes. VERIFIED (2026-09-08) with one divergence. Query2 matches: ISCAN(PREFIX_REPEATED) with both components equality-bound; the source fetch(coveringIndexScan(...)) is the same access path with a deferred fetch. Query1 diverges: SQL scans the unrelated MY_SIMPLE_RECORD_STR_VALUE_INDEXED with a residual filter where the source plans a full record scan, because the relational layer hard-wires IndexScanPreference.PREFER_INDEX (PlannerConfiguration.java:160) and the Cascades cost model reads it (PlanningCostModel.java:497-501). The load-bearing half - PREFIX_REPEATED is not used - survives. |
| `testOnlyRepeatIndex` | dual | `repeated-field-query-only-index.yamsql` | ported, plan generated | all three proto indexes removed; source asserts the ordered list [0,1,2] |
| `testPrefixRepeatedNested` | dual | `repeated-field-query-prefix-repeated-nested.yamsql` | ported, plan generated | 1 query. TestRecordsWithHeader with the source primary key header.rec_no. startsWith becomes LIKE, which is the documented divergence but invisible here since the source also asserts a residual filter over a type-filtered scan. Source data verbatim, 3 records. Lost: plan hashes. VERIFIED (2026-09-08): SCAN(<,>) + TFILTER MY_RECORD + FILTER HEADER.PATH LIKE, which is the source predicatesFilterPlan(typeFilterPlan(scanPlan())) with the documented startsWith-to-LIKE rendering. The nested primary key and the (nested path, fan-out) index DDL are both accepted. |

## `FDBReturnedRecordLimitQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBReturnedRecordLimitQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testComplexLimits2` | dual | `returned-record-limit.yamsql` | ported, plan generated | maxRows: 10; emission order (str desc, nv3 desc, rec_no desc), derived from a run — the plan is a reverse scan of MULTI_INDEX |
| `testComplexLimits3` | dual | `returned-record-limit.yamsql` | ported, plan generated | row limit not reproduced: no sort in the source; asserts the full result |
| `testComplexLimits6` | dual | — | not portable | setAllowedIndexes(emptyList()) forbids every index; USE INDEX cannot express that |
| `testComplexLimits7` | dual | `returned-record-limit-no-filter.yamsql` | ported, plan generated | 1 query, 10 pages. Two forced deviations: the source query spans MySimpleRecord and MyOtherRecord, which SQL cannot express, so it queries MY_SIMPLE_RECORD alone - the row set is unaffected because complexQuerySetup saves only MySimpleRecords and the base class never saves a MyOtherRecord; and setReturnedRowLimit is a hard stop while maxRows is a page size, so the port pages through all 100 rows instead of stopping at 10. Own file rather than an entry in returned-record-limit.yamsql, to avoid regenerating that file plans. Lost: assertDiscardedNone; plan hashes. VERIFIED (2026-09-08), with the plan assertion recorded as not reproducible. The first run failed: expected primary-key order, actual was index key order (NUM_VALUE_3_INDEXED, REC_NO), because SQL plans an index scan where the source asserts a full record scan - the relational layer hard-wires IndexScanPreference.PREFER_INDEX (PlannerConfiguration.java:160) and the Cascades cost model reads it (PlanningCostModel.java:497-501). So the source plan assertion is NOT reproducible for an unfiltered query; what survives is that a row limit works with no filter. Rows re-ordered to the index key order and a terminating empty page added (100 rows at 10 per page is exactly full); passes. Generated plan is ISCAN(MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED <,>) - an unbounded index scan, no type filter, which is the divergence made concrete. |
| `testComplexLimits8` | @Test | — | out of scope | old planner only |

## `FDBRecordStoreRepeatedQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBRecordStoreRepeatedQueryTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `queryByRepeated` | dual | `record-store-repeated-query.yamsql` | ported, plan generated | `oneOfThem()` rendered as a correlated EXISTS; the source loads no records, so the table is empty here too |
| `querySumOldPlanner` | old | — | out of scope | `Planner.OLD` only |
| `querySumByRepeatedPredicateOnGroup` | cascades | `record-store-repeated-query-sum-by-repeated.yamsql` | ported, plan generated | SUM index grouped by an unnested array element plus a scalar; expressible because only the grouping side fans out. Predicate before the grouping becomes WHERE. unorderedResult, since the source builds LogicalSortExpression.unsorted. Source saves no records; 4 rows invented, one of which would form an extra group if the predicate were dropped. The aggregate column is renamed TOTAL (the source calls it "aggregate"); nothing asserts the name. Lost: plan hashes. VERIFIED (2026-09-08): AISCAN(REPEATED_GROUP_SUM [EQUALS ...] BY_GROUP) under a MAP, which is the source mapPlan(aggregateIndexPlan(range [[42],[42]])). The fan-out grouping DDL is accepted. |
| `querySumByRepeatedPredicateOnHaving` | cascades | `record-store-repeated-query-sum-by-repeated.yamsql` | ported, plan generated | Same file and same asserted plan as the OnGroup variant; the predicate moves to HAVING. VERIFIED (2026-09-08): identical plan to the OnGroup variant, which is what the source asserts for both. HAVING on a grouping column supplied by a lateral works. |

## `FDBSortQueryIndexSelectionTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBSortQueryIndexSelectionTest.java`

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `sortOnly` | dual | `sort-index-selection-unique.yamsql` | ported, plan generated | own data set: REC_NO is a Carter-Wegman hash, so primary-key order differs from NUM_VALUE_UNIQUE order |
| `sortByPrimaryKey` | dual | `sort-index-selection-primary-key.yamsql` | ported, plan generated | 2 queries (forward, reverse); the source asserts a record scan, not an index scan |
| `testComplexQuery5` | dual | `sort-index-selection.yamsql` | ported, plan generated | |
| `testComplexQuery5r` | dual | `sort-index-selection.yamsql` | ported, plan generated | reverse |
| `testComplexQuery8x` | dual | `sort-index-selection.yamsql` | ported, plan generated | predicate stays a residual filter above the sort's index scan |
| `testComplexLimits1` | dual | `sort-index-selection.yamsql` | ported, plan generated | row limit not reproduced: STR_VALUE_INDEXED has two distinct values, so page membership depends on tie order |
| `sortNested` | dual | `sort-index-selection-nested.yamsql` | ported, plan generated | 6 queries; own header data set with the compound primary key (header.path, header.rec_no) |
| `sortOnlyUniqueNull` | dual | — | not portable | NULL_UNIQUE_HOOK rebuilds the unique index with `Key.Evaluated.NullStandin.NULL_UNIQUE`; index null-standin behaviour is not expressible in SQL DDL |
| `sortUniqueNull` | dual | — | not portable | as above |
| `testUncommonMultiIndex` | dual | — | not portable | `setRecordTypes([...])` across three record types with a fan-out sort, expecting planning to fail; a SQL query has one table |
| `sortWithScannableFilterOnIndex` | `@ParameterizedTest` | — | out of scope | no `@DualPlannerTest`; also registers a custom "FAKE_TYPE" index maintainer via `@AutoService` |
| `sortWithNonScannableFilterOnIndex` | `@ParameterizedTest` | — | out of scope | as above |
| `sortWithNonScannableFilterWithAnd` | `@ParameterizedTest` | — | out of scope | as above |
| `testUncommonPrimaryKeyWithSort` | `@Test` | — | out of scope | old planner only |
| `twoSortOneNestedFilter` | `@Test` | — | out of scope | old planner only |

## `SparseIndexTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/SparseIndexTest.java`

All four are `@DualPlannerTest(planner = CASCADES)`. Each attaches a different `IndexPredicate` to a
VALUE index over NUM_VALUE_2, so each needs its own schema. NUM_VALUE_2 is i % 3 in the fixture, so
`> 50` matches nothing and every result is empty — the source asserts only the plan.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `sparseIndexIsUsedWhenItsOrPredicateIsImplied` | cascades | `sparse-index-or-predicate.yamsql` | ported, plan generated | index predicate `num_value_2 > 42 OR num_value_2 < 10`; the query implies the first disjunct |
| `sparseIndexIsUsedWhenItsPredicateIsImplied` | cascades | `sparse-index-implied.yamsql` | ported, plan generated | index predicate `num_value_2 > 42` |
| `sparseIndexIsNotUsedWhenItsPredicateIsNotImplied` | cascades | `sparse-index-not-implied.yamsql` | ported, plan generated | index predicate `num_value_2 > 100`, not implied, so the plan must scan |
| `sparseIndexIsNotPickedWhenDoingFullScan` | cascades | — | not portable | uses `planQueryWithAllowedIndexes(..., Set.of("SparseIndex"), false)` and asserts a *scan* is still chosen. `USE INDEX` cannot express this: it restricts the planner to the named index and drops the primary-scan candidate, so the query would fail rather than scan |

## `FDBNestedFieldQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBNestedFieldQueryTest.java`

The widest-spread class so far: its 17 dual methods run against five different protos
(`test_records_3`, `test_records_4`, `test_records_5`, `test_records_map`,
`test_records_with_header`), and within `test_records_4` almost every method swaps the reviewer index
out for a different one. Hence 14 files, one per index configuration.

Two of them, `nested-field-query.yamsql` and the `nested-field-query-reviewer*` group, split
`nestedMetaData`'s single store into one table each. Every query pins a `setRecordType`, so nothing is
lost, and a single-table template can use `INTERMINGLE_TABLES=true` — with both tables present the
record-type prefix would be load-bearing, since `RestaurantRecord.rest_no` and `RestaurantReviewer.id`
are both `bigint` primary keys.

`addUniversalIndex(globalCountIndex())` is not reproduced anywhere: a universal index spans record
types and `RecordLayerSchemaTemplate.computeIndexes` cannot express one. No query in the class can
match it.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `hierarchical` | dual | `nested-field-query-hierarchical.yamsql` | ported, plan generated | compound PK (parent_path, child_name); NULL in the leading PK column works, as in the source. `startsWith` → `LIKE 'photos%'` leaves the record scan unbounded, so the planner prefers a covering NUM_VALUE_INDEXED scan and the second block had to be relaxed to `unorderedResult` — the rows arrive in NUM_VALUE_INDEXED order, not primary-key order |
| `nested` | dual | `nested-field-query.yamsql` | ported, plan generated | `oneOfThem()` → correlated EXISTS; fan-out index over `reviews[].rating` |
| `nested2` | dual | `nested-field-query.yamsql` | ported, plan generated | two conditions on one array element, both bound by the `tag` index |
| `nested3` | dual | `nested-field-query.yamsql` | ported, plan generated | only `rating` is bound; `reviewer` stays a filter inside the flat-map |
| `nested4` | dual | `nested-field-query-complex.yamsql` | ported, plan generated | duplicated NAME equality must fold. The hook's `duplicates` = concat(name, name) index is dropped: a projection cannot name one column twice, and only the old planner reacts to it |
| `nestedWithAnd` | dual + `@BooleanSource` | `nested-field-query-reviewer.yamsql` | ported, plan generated | the `normalizeNestedFields` parameter collapses — `BooleanNormalizer` is on the old-planner path only. `startsWith("H")` → `LIKE 'H%'` |
| `nestedThenWithAnd` | dual | `nested-field-query-reviewer-email-hometown.yamsql` | ported, plan generated | both spellings ported; the source's `plan1.equals(plan2)` becomes "the two `explain` strings match" |
| `nestedThenWithAndPartial` | dual | `nested-field-query-reviewer-hometown-email.yamsql` | ported, plan generated | gap in the middle of the index key, so EMAIL stays a residual filter |
| `nestedAndOnNestedMap` | dual | `nested-field-query-nested-map.yamsql` | ported, plan generated | source saves no records; four discriminating rows added so the "unsatisfied filter is not dropped" claim is actually executed. `OtherRecord` omitted (no index, never queried, would force the record-type prefix) |
| `nestedWithAndConcat` | dual | `nested-field-query-reviewer-school-concat.yamsql` | ported, plan generated | one query, not two: both source spellings are the same SQL text |
| `nestedWithBetween` | dual | `nested-field-query-reviewer-between.yamsql` | ported, plan generated | two one-sided inequalities must merge into one range |
| `nestedWithAndSorted` | dual | `nested-field-query-reviewer-sorted.yamsql` | ported, plan generated | both queries ported, including the one that over-specifies the sort |
| `nestedWithAndSortedEquality` | dual | `nested-field-query-reviewer-sorted.yamsql` | ported, plan generated | shares the index configuration with `nestedWithAndSorted`, hence the shared file |
| `nestedWithAndSortedInequality` | dual | `nested-field-query-reviewer-sorted-inequality.yamsql` | ported, plan generated | same index *name* as the file above, different key, so a separate template. One query, not two |
| `doublyNested` | dual | `nested-field-query-doubly-nested.yamsql` | **ported, FAILING** | fan-out two levels down; `Recurrence.uuid` omitted (UUID is a primitive type name in the grammar and no test reads it). `EXISTS` over the unnested array returns ev3 twice: a range-bounded fan-out scan with no duplicate elimination. Not SQL-specific — the source's distinct comes from `RecordQuery.removeDuplicates` defaulting to true, which blinds every record-layer test to it. `@Disabled` (single-query file); see `.out/issue-exists-fanout-no-distinct.md` |
| `testConcatNested` | dual | `nested-field-query-concat-nested.yamsql` | ported, plan generated | index components come from two different nested messages, so the projection has to alias the two VERSION columns apart |
| `testNestedPrimaryKeyQuery` | dual | `nested-field-query-nested-pk.yamsql` | ported, plan generated | PK (header.path, header.rec_no); no indexes in the template, which is the point |
| `nestedRankMap` | `@Test` | — | out of scope | old planner only, and an `IndexTypes.RANK` index over a map-like repeated message |

## `GroupByTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/GroupByTest.java`

All 14 methods are `@DualPlannerTest(planner = CASCADES)` and all build their graph with `planGraph`, so
the whole class is in scope. Nine port; two are already `@Disabled` upstream and three are blocked on a
missing index option.

The four `GroupingKind`s of `constructGroupByPlan` map onto SQL directly: REGULAR_GROUPING is
`group by num_value_2, str_value_indexed`, REVERSED_GROUPING is the same projection with the keys the
other way round, `withPredicateInSelectWhere` is `where num_value_2 >= 42` and
`withPredicateInSelectHaving` is `having num_value_2 <= 44`.

Four files, one per index configuration — `setupHookAndAddData(addIndex, addAggregateIndex)`:

| `addIndex` | `addAggregateIndex` | file |
|---|---|---|
| true | false | `group-by-query.yamsql` |
| false | false | `group-by-query-no-index.yamsql` |
| false | true | `group-by-query-aggregate-index.yamsql` |
| true | true | `group-by-query-both-indexes.yamsql` |

NUM_VALUE_2 tops out at 7 in the 14-row data set, so every `>= 42` block is empty. That is faithful — the
source calls `planGraph` and asserts the plan without executing it, so those predicates exist to shape a
scan range, not to select rows.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testSimpleGroupBy` | cascades | `group-by-query.yamsql` | ported, plan generated | streaming aggregation over the value index |
| `attemptToPlanGroupByWithoutCompatiblySortedIndexFails` | cascades | `group-by-query-no-index.yamsql` | ported, plan generated | `UnableToPlanException` → `UNSUPPORTED_QUERY` (`ExceptionUtil:79`). Pins down that Cascades has no sort-then-aggregate or hash-aggregate fallback: MULTI_INDEX supplies the *reversed* order and is no help |
| `testAggregateIndexPlanning` | cascades | `group-by-query-aggregate-index.yamsql` | ported, plan generated | aggregate index matched with the value index withheld |
| `testIndexPlanningWithPredicateInSelectWhere` | cascades | `group-by-query.yamsql` | ported, plan generated | predicate folded into the scan range, `[[42],>` |
| `testIndexPlanningWithPredicateInSelectWhereMatchesAggregateIndex` | cascades | `group-by-query-both-indexes.yamsql` | ported, plan generated | both indexes present; must still pick the aggregate index |
| `testIndexPlanningWithPredicateInSelectWhereAndSelectHavingMatchesAggregateIndex` | cascades | `group-by-query-both-indexes.yamsql` | ported, plan generated | WHERE and HAVING together bound the range to `[[42],[44]]` |
| `testAggregateIndexPlanningReversedGrouping` | cascades | `group-by-query-both-indexes.yamsql` | ported, plan generated | grouping *set* must match, not the written order: AGG_INDEX groups (num_value_2, str_value_indexed), the query the other way |
| `testAggregateIndexPlanningReversedGroupingWithPredicateInSelectWhereMatchesAggregateIndex` | cascades | `group-by-query-both-indexes.yamsql` | ported, plan generated |  |
| `testAggregateIndexPlanningReversedGroupingWithPredicateInSelectWhereAndSelectHavingMatchesAggregateIndex` | cascades | `group-by-query-both-indexes.yamsql` | ported, plan generated |  |
| `testAggregateIndexPlanningReversedGroupingExplicitAndImplicitGrouping` | cascades | — | out of scope | `@Disabled` upstream, "TODO re-enable" |
| `testAggregateIndexPlanningReversedGroupingOnlyImplicitGrouping` | cascades | — | out of scope | `@Disabled` upstream, "TODO re-enable" |
| `testBitmapWithStreamAggregation` | cascades | — | not portable | needs `IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION` set to 4. No SQL surface for it: `indexAttribute` in the grammar offers only `LEGACY_EXTREMUM_EVER`, and the default entry size is 10000. Every expectation in these three is about how NUM_VALUE_2 1..7 splits across buckets of width 4; at width 10000 they all fall in bucket 0 and the structure under test disappears. Bitmap aggregates themselves are reachable — see `bitmap-aggregate-index.yamsql` |
| `testBitmapWithBitmapIndex` | cascades | — | not portable | as above |
| `testBitmapWithBitmapIndexWithEmptyGroup` | cascades | — | not portable | as above |

## `FDBPermutedMinMaxQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBPermutedMinMaxQueryTest.java`

All 11 methods are `@DualPlannerTest(planner = CASCADES)` and all port. PERMUTED_MAX is fully reachable
from SQL, and the permuted size does not have to be stated — it is **derived** from where the aggregate
sits in the index's ORDER BY: `permutedSize = fieldValues.size() - aggregateOrderIndex`
(`MaterializedViewIndexGenerator:238-240`). So `order by a, max(x), b, c` over `group by a, b, c` gives a
permuted size of 2. The idiom is already exercised by `aggregate-index-tests.yamsql:36` and `:50`.

`complexQuerySetup(hook)` does **not** apply `complexQuerySetupHook()`, so the base index set in this
class is only the three proto-declared indexes — no MULTI_INDEX, no repeater fan-out.

The `InComparisonCase` parameterization (byParameter / byLiteral / byConstantObjectValue) collapses to one
SQL query: the first two are the two halves of the default `statement_type: both`, and
byConstantObjectValue is a Cascades internal with no SQL surface. `setAttemptFailedInJoinAsUnionMaxSize`
is likewise not reproduced and not load-bearing — every IN list here is well under the 24 the SQL layer
pins.

Five files, one per index configuration:

| index | permuted size | file |
|---|---|---|
| `maxUniqueBy2And3` | 1 | `permuted-min-max.yamsql` |
| `max2ByStrValueAnd3` | 1 | `permuted-min-max-num-value-2.yamsql` |
| `max3ByStrValueRepeaterAnd2` | 1 | `permuted-min-max-repeater.yamsql` |
| `maxUniqueByStrValueOrderBy2And3` | 2 | `permuted-min-max-str-value.yamsql` |
| the same, minus the unique index, plus 6 rows | 2 | `permuted-min-max-ordering-keys.yamsql` |

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `selectMaxOrderByFirstGroup` | cascades + `@BooleanSource` | `permuted-min-max.yamsql` | ported, plan generated | both directions. ORDER BY names only NUM_VALUE_2, but the permuted key delivers (num_value_2, max, num_value_3_indexed), so the rows arrive ordered by the max within each group — which is what the source asserts |
| `selectMaxByGroupWithFilter` | cascades | `permuted-min-max.yamsql` | ported, plan generated | all six of the source's num_value_2 values, -1..4 |
| `selectMaxByGroupWithOrder` | cascades + `@BooleanSource` | `permuted-min-max.yamsql` | ported, plan generated | ORDER BY the aggregate, both directions. One representative num_value_2 — the source's -1..4 loop is covered by the block above |
| `selectMaxWithInOrderByMax` | cascades + `@MethodSource` | `permuted-min-max.yamsql` | ported, plan generated | in-union, comparison key (m, num_value_2, num_value_3_indexed). Three representative IN lists including a repeated value, since the source deduplicates its expectation, and an empty one |
| `testMaxWithInAndDupes` | cascades + `@MethodSource` | `permuted-min-max-num-value-2.yamsql` | ported, plan generated | "dupes" is literal: (str_value_indexed, num_value_3_indexed) fixes i mod 10 and those ten records cover every residue mod 3, so max(num_value_2) is 2 in *every* group. Unordered — the whole sort key is tied |
| `testSortedMaxWithEqualityOnRepeater` | cascades | `permuted-min-max-repeater.yamsql` | ported, plan generated | fan-out inside the aggregate index's GROUP BY. Own data: `setUpWithRepeaters` writes REPEATER as the set bit positions of i. Representative (str, x) pairs from the source's 18 |
| `testSortedMaxWithInOnRepeater` | cascades + `@MethodSource` | `permuted-min-max-repeater.yamsql` | ported, plan generated | in-union over repeater values; representative lists from the source's 81 pairs |
| `testMaxUniqueByStr2And3WithDifferentOrderingKeys` | cascades + `@MethodSource` | `permuted-min-max-ordering-keys.yamsql` | ported, plan generated | four ORDER BY prefixes, each with a different expected plan shape: in-**join** with no ordering constraint, in-**union** with one. Own file — the hook removes the unique index because the six extra records duplicate NUM_VALUE_UNIQUE. Unordered: the source uses `containsInAnyOrder` plus a separate monotonicity check that yamsql cannot express |
| `selectMaxGroupByWithPredicateOnMax` | cascades | `permuted-min-max.yamsql` | ported, plan generated | HAVING on the aggregate. The source derives the threshold as the mean of that group's maxima; for num_value_2 = 0 that is 994 |
| `selectMaxGroupByWithPredicateAndOrderByOnMax` | cascades + `@BooleanSource` | `permuted-min-max.yamsql` | ported, plan generated | the same plus ORDER BY the aggregate, both directions |
| `maxUniqueFilterOnEntries` | cascades + `@BooleanSource` | `permuted-min-max-str-value.yamsql` | ported, plan generated | permuted size 2 pushes both trailing grouping columns behind the max, so only STR_VALUE_INDEXED stays a scan bound and the NUM_VALUE_2 equality has to survive as a residual filter — the source asserts `predicatesFilterPlan` above the scan |

## `FDBSimpleQueryGraphTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBSimpleQueryGraphTest.java`

All 22 methods are `@DualPlannerTest(planner = CASCADES)`; every query is built as a Cascades graph via
`planGraph`. 13 port, 2 are `@Disabled` upstream, 7 are not portable.

Schema: `openNestedRecordStore` puts **both** TestRecords4 record types in one store with bare primary
keys. `INTERMINGLE_TABLES=true` reproduces that, and here it is load-bearing rather than incidental — it
is why the source's plans carry a `typeFilterPlan` over the scan. Primary-key values do not collide
(reviewers 1-2, restaurants 1000-1001).

One collapse worth recording: `setUpWithNullableArray` rebuilds the same data over
`test_records_4_wrapper.proto`, where each repeated field is wrapped in a `…List` message with a `values`
field. That is precisely how the Relational layer models an `ARRAY` — `NullableArrayUtils.wrapArray` is
applied to every generated index key — so the wrapped and unwrapped variants are the *same* SQL, and
`testSimplePlanGraphWithNullableArray` has no independent content. It follows that every fan-out index in
this migration is structurally the wrapper variant.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testSimplePlanGraph` | cascades | `simple-query-graph.yamsql` | ported, plan generated | `Map(TypeFilter(Scan([([1],>))))` — no index, since neither projected column is indexed with the predicate |
| `testSimplePlanGraphReversed` | cascades | `simple-query-graph.yamsql` | ported, plan generated | the same under a reverse sort |
| `testSimplePlanGraphWithNullableArray` | cascades | `simple-query-graph.yamsql` | ported, plan generated | same SQL as `testSimplePlanGraph`; see the note above |
| `testSimplePlanWithConstantPredicateGraph` | cascades | `simple-query-graph.yamsql` | **ported, FAILING** | `and true` dies at planning with `INTERNAL_ERROR` and the message "null". `AndOrValue.toQueryPredicate:218-219` requires both children to implement `BooleanValue`; a boolean literal is a `ConstantObjectValue` and a boolean column a `FieldValue`. Broader than this query — `WHERE b AND b`, `WHERE a > 1 OR b`, `WHERE NOT b` all fail identically and nothing covers them. `@Disabled` (this file holds only this query, so a sentinel would leave the block with no executables); see `.out/issue-boolean-operand-under-and-or-not.md` |
| `testPlanDifferentWithIndexHintGraph` | cascades | `simple-query-graph-hints.yamsql` | ported, plan generated | the one place in the migration where `USE INDEX` is the *faithful* translation — the source really does attach an `IndexAccessHint` |
| `testFailWithBadIndexHintGraph` | cascades | `simple-query-graph-hints.yamsql` | ported, plan generated | hinting a fan-out index that cannot answer the query → `UNSUPPORTED_QUERY` |
| `testPlanCrossProductJoin` | cascades | `simple-query-graph-joins.yamsql` | ported, plan generated | no join predicate, so the reviewer side is an unbounded type-filtered scan |
| `testSimpleJoin` | cascades | `simple-query-graph-joins.yamsql` | ported, plan generated | join predicate turns the inner side into an equality-bounded scan |
| `testMediumJoinDatabaseObjectDependencies` | cascades | `simple-query-graph-joins.yamsql` | ported, plan generated | the plan ports; its assertion on the plan's *database object dependencies* has no yamsql directive |
| `testMediumJoin` | cascades | `simple-query-graph-joins.yamsql` | ported, plan generated | five quantifiers. The source asserts only `assertInstanceOf(RecordQueryFlatMapPlan.class, …)` plus `verifySerialization`, so the `explain` here is a tighter assertion than the source's |
| `testPlanFiveWayJoin` | cascades | `simple-query-graph-joins.yamsql` | ported, plan generated | builds inline the graph `planMediumJoin` builds, and asserts the same thing — same SQL, kept so the two explains can be compared |
| `testSimpleExistentialPredicateOnSimpleIndex` | cascades + `@BooleanSource` | `simple-query-graph-tag-index.yamsql` | ported, plan generated | both the EQUALS and IN variants |
| `testEqualityAndSimpleExistentialPredicate` | cascades | `simple-query-graph-name-tag-index.yamsql` | ported, plan generated | IN on a scalar column plus an existential over the array, one index over concat(name, tags.value) |
| `testPlanQueryOnRestNoWithNullOnEmpty` | cascades | — | out of scope | `@Disabled` upstream, issue #3431 |
| `testPlanQueryOnNameWithNullOnEmpty` | cascades | — | out of scope | `@Disabled` upstream, issue #3431 |
| `testSubselectHasNullOnEmpty` | cascades | — | not portable | needs a `forEachWithNullOnEmpty` quantifier. SQL has no way to ask a derived table for null-on-empty semantics; the source constructs the operator directly |
| `testSubselectHasNullOnEmptyAndIsNullPredicate` | cascades | — | not portable | same, plus an IS NULL on the null-on-empty result |
| `testLogicalFilterExpression` | cascades | — | not portable | builds `LogicalFilterExpression` directly, which no SQL text produces — the front end always emits a select |
| `testMediumJoinTypeEvolutionIdentical` | cascades | — | not portable | mutates `RecordMetaData` between planning and execution; a yamsql file has one schema per template and no way to evolve it mid-block |
| `testMediumJoinTypeEvolutionCompatible` | cascades | — | not portable | as above |
| `testMediumJoinTypeEvolutionIncompatible1` | cascades | — | not portable | as above |
| `testMediumJoinTypeEvolutionIncompatible2` | cascades | — | not portable | as above |

## `FDBModificationQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBModificationQueryTest.java`

One index configuration, so one file. `openNestedRecordStore(context)` = `nestedMetaData(null)` for all seven methods: TestRecords4's RestaurantRecord (pk rest_no) and RestaurantReviewer (pk id) in one store with bare primary keys, plus the two proto-declared indexes `RestaurantRecord$name` and `RestaurantReviewer$name`, three fan-out indexes over RestaurantRecord's repeated fields (`review_rating` = reviews[].rating, `tag` = tags[].(value, weight), `customers` = customer[], `customers-name` = concat(customer[], name)) and `stats$school` = stats.start_date on RestaurantReviewer. The schema_template is byte-for-byte the one in the existing simple-query-graph.yamsql, which ports the other class built on openNestedRecordStore, with the same index names (RESTAURANT_RECORD_NAME, RESTAURANT_REVIEWER_NAME, REVIEW_RATING, TAG, CUSTOMERS, CUSTOMERS_NAME, STATS_SCHOOL) and `WITH OPTIONS(INTERMINGLE_TABLES=true)`. `addUniversalIndex(globalCountIndex())` is not reproduced (no DDL for a universal index) and no query can match it. Both tables are kept because testStablePlanHash updates the reviewer; primary keys cannot collide because RESTAURANT_REVIEWER is never populated — as in the source — which matters here since rest_no 100 and reviewer id 100 are the same key without the record-type prefix.

Deviations: Stateful file, unique in this directory: the queries mutate the store, so five test_blocks are chained (plus one interleaved `setup:` restoring REST_NO 100 after the delete block) and `preset: single_repetition_ordered` is load-bearing rather than a cost saving. Verified sound: Block.parse dispatches documents in order, ORDERED mode runs tests in written order, and QueryExecutor.isForcedContinuationsEligible only re-runs queries whose text starts with `select`, so each DML statement executes exactly once even under ForceContinuations.; Apostrophe dropped from the update's appended literal: the source sets `name = name + ' McDonald\'s'`; here it is ' McDonalds'. The '' escape only decodes from 4.13.5.0 (b9790b9af), so a mixed-mode older server would read the escape back verbatim.; File-level `options: supported_version: 4.11.1.0` — the only version guard in this directory. Needed because the readback select is the source's own `reviews = []` predicate and comparing an ARRAY column to the empty-array literal is guarded at 4.11.1.0 in arrays-operators.yamsql. Guarding a single query instead would be wrong: skipping one step of a stateful block invalidates every expectation after it.; The wet UPDATE has no RETURNING (so its plan matches the source's updatePlan(unorderedPrimaryKeyDistinctPlan(...)) assertion) while the dry-run UPDATE does (so the value that is computed but deliberately not written is observable). RETURNING adds a MAP above the UPDATE; that is a deliberate, localised divergence and is called out in the file header.; Where the source inspects the record fields the DML plan flows out (delete: old rest_no/name; insert: rest_no/name per row), the port asserts `count:` plus a readback select. Only the dry-run update's old/new pair is asserted directly.; Two plan-hash assertions dropped: `assertEquals(959543174, plan.hashCode())` in testPlanInsertExpression, and the whole subject of testStablePlanHash.; `assertMatchesExactly` structural matchers become `explain:` strings (the documented systematic weakening); all 15 explains are `""` placeholders awaiting the CORRECT_EXPECTATIONS pass.

Blockers: Cannot run gradle, so none of the 15 explains are filled in and none of the riskier SQL constructs above are executed. The file needs one CORRECT_EXPECTATIONS pass; the four things to look at first are the two struct-array-vs-`[]` constructs, `"old".name`, the dotted update target, and whether `EXPLAIN` works on INSERT/DELETE.; testPlanUpsertGraph's coverage of the index name `RestaurantRecord$name` is lost with the method — no other query in the class touches that index, so RESTAURANT_RECORD_NAME is present in the template but never matched. That is faithful (the source's other methods do not match it either) but it means the index cannot be dropped from the template without checking the upsert method's fate first.

Riskiest assumptions: The untyped empty-array literal `[]` inserted into a STRUCT ARRAY column (`reviews restaurant_review array`) round-trips as an empty array rather than NULL. Precedent exists only for primitive arrays: arrays.yamsql:145 does `INSERT INTO G VALUES (1, [])` for a STRING ARRAY and arrays.yamsql:58-64 shows NULL and [] are distinguishable. If `[]` for a struct array instead lands as NULL, every `where reviews = []` readback returns nothing and the file fails on the first readback.; `where reviews = []` — comparing a STRUCT ARRAY column against the empty-array literal. arrays-operators.yamsql:245 proves `WHERE "arr" = []` for an INTEGER ARRAY and :264 proves array-of-tuples equality between two literals, but not the two together on a column. This is the source's own `whereReviewsIsEmptyGraph` predicate, so it was kept rather than replaced with an unconditional select.; `returning "old".name as old_name, "new".name as new_name` — `"new".<col>` is exercised by update-delete-returning.yamsql, `"old"` is not exercised by any yamsql file. UpdateExpression.java:62-63 defines both `old` and `new` as result fields, and the lowercase quoting follows the existing TODO note in that file about identifier case matching.; `update restaurant_reviewer set stats.start_date = 3` — a dotted (nested-struct) update target. Documented in docs/.../DML/UPDATE.rst:213 (`UPDATE T SET C.S1 = 45`) and QueryVisitor:840 casts the resolved target to a FieldValue and takes its field path, but no yamsql or relational-core test exercises it.; `EXPLAIN <insert|delete>` — `describeObjectClause` accepts insertStatement and deleteStatement, and update-delete-returning.yamsql explains UPDATEs, but no existing yamsql file carries an `explain:` on an INSERT or a DELETE. If explain on those is unsupported, 6 queries fail.; `count:` on a dry run reports the rows the plan flowed rather than 0. Confirmed for INSERT by OptionScopeTest (`executeUpdate("... OPTIONS(DRY RUN)")` returns 1 while the table stays empty) and by RecordQueryDeletePlan:99-103 / RecordQueryUpdatePlan:101-104 taking the dry path but still emitting a QueryResult; not confirmed for DELETE/UPDATE through the yamsql `count:` directive.; Cross-block state survives: blocks share one database and `connection_lifecycle` defaults to TEST (a fresh connection per test), so each statement auto-commits. index-ddl.yamsql:217-227 relies on the same cross-block persistence for its UNIQUE_CONSTRAINT_VIOLATION cases.; `update restaurant_reviewer ... where id = 100` matches nothing even though RESTAURANT_RECORD.REST_NO 100 exists at that very key under INTERMINGLE_TABLES, because the type filter discriminates. If it did not, the expectation would be `count: 1` and the restaurant record would be corrupted.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testPlanInsertExpression` | cascades | `modification-query.yamsql` | ported, plan generated | test_block `modification-query-insert`, 4 queries: dry-run INSERT (count 2, nothing written), the source's `whereReviewsIsEmptyGraph` readback (empty), the same INSERT for real (count 2), the readback again (2 rows). Source asserts `insertPlan(explodePlan()).where(target("RestaurantRecord"))`. The companion `assertEquals(959543174, plan.hashCode())` is not reproduced — plan hashes are out of scope for this suite. |
| `testInsertExistingRecordThrowsException` | cascades | `modification-query.yamsql` | ported, plan generated | test_block `modification-query-insert-existing`, 2 queries, both `error: UNIQUE_CONSTRAINT_VIOLATION` (ExceptionUtil:70 maps RecordAlreadyExistsException). Relies on the rows left by the preceding block, exactly as the source relies on its own preceding insert. RecordQueryInsertPlan uses ERROR_IF_EXISTS on both the wet and the dry path, which is why the dry-run half throws too. No `explain:` — the plan is the same as the block above and the point is the execution error (matches the convention in group-by-query-no-index.yamsql). |
| `testPlanDeleteExpression` | cascades | `modification-query.yamsql` | ported, plan generated | test_block `modification-query-delete`, 4 queries: dry-run DELETE (count 1), readback still 2 rows, wet DELETE (count 1), readback 1 row. Written without RETURNING so the plan stays `SCAN / DELETE`, matching the source's `deletePlan(typeFilterPlan(scanPlan()))` — note the source has no distinct above the scan here, unlike its update. |
| `testPlanUpdateExpression` | cascades | `modification-query.yamsql` | ported, plan generated | test_block `modification-query-update`, 3 queries. The wet UPDATE is written without RETURNING so its plan matches the source's `updatePlan(unorderedPrimaryKeyDistinctPlan(typeFilterPlan(scanPlan())))`; the dry run uses `returning "old".name as old_name, "new".name as new_name` because the computed-but-unwritten value is otherwise unobservable, at the cost of a MAP above the UPDATE. Apostrophe dropped: ' McDonald''s' -> ' McDonalds'. Preceded by a `setup:` block restoring REST_NO 100, which the delete block removed. |
| `testStablePlanHash` | cascades | `modification-query.yamsql` | ported, plan generated | PARTIAL. test_block `modification-query-stable-plan-hash` reproduces the two update shapes the source plans — `update restaurant_reviewer set stats.start_date = 3 where id = 100` (nested-struct target, int literal promoted to bigint; RESTAURANT_REVIEWER is empty in the source too, so count 0) and `update restaurant_record set reviews = [(1, 34), (2, 14)] where rest_no = 100` (whole STRUCT ARRAY column replaced) — plus a readback. The property under test, that planning the same graph twice yields the same plan hash, has no yamsql expression: there is no directive for 'plan twice and compare', and `planHash:` is skipped in multi-server configs anyway. |
| `testPlanUpsertGraph` | cascades | — | not portable | The graph nests an UpdateExpression inside a NOT EXISTS inside the *source* of an InsertExpression: `INSERT ... WHERE NOT EXISTS(UPDATE ... SET name = 'McDonald''s') FROM (VALUES (300, ...), (400, ...))`. RelationalParser.g4 has no ON CONFLICT / UPSERT / MERGE (grepped both .g4 files), `insertStatement` takes only `queryExpressionBody` or a VALUES list, and a subquery is always a `query` — never a deleteStatement/insertStatement/updateStatement. No SQL text produces this shape. It is also the class's only index-naming assertion (`RestaurantRecord$name`), so that coverage is lost with it. |
| `testPlanInsertExpressionBadNullAssignments` | cascades | — | not portable | Asserts a RecordCoreException when NULL is assigned to `reviews`/`tags`/`customer`, which holds only because a proto2 `repeated` field cannot be null. Every SQL ARRAY column in the ported schema is nullable (the documented 'unset ARRAY columns are NULL here' divergence), so the same INSERT succeeds and the property under test does not exist. `ARRAY NOT NULL` is the nearest equivalent (arrays.yamsql:41-43 shows NULL into a NOT NULL array -> INTERNAL_ERROR) but a non-nullable array is stored as a bare `repeated` field rather than the wrapped `…List { repeated values }` form every other file in this directory relies on, so it would be a different schema, not this one — and no yamsql file anywhere declares a STRUCT ARRAY NOT NULL, so it is unproven. |

## `RecursiveQueriesTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/RecursiveQueriesTest.java`

Two schemas. The hierarchy files (recursive-queries, recursive-queries-forest) reproduce `setupRecordStoreMetadata(context, true)` exactly: `create table simple_hierarchical_record(id bigint, parent bigint, primary key(id))` plus `parent_id_idx` (order by parent, id) for concat(parent, id) and `id_parent_idx` (order by id, parent) for concat(id, parent) — nothing else, so the planner has only the two access paths the source gives it. Data is sampleHierarchy() (9 edges) and sampleForest() (13 edges) respectively, one data set per file. recursive-queries-multiples has its own schema, `create table seed(id bigint, primary key(id))` with rows 2 and 5 and no indexes, because that recursion touches no hierarchy at all — the table exists only because a recursive CTE's initial leg must be a query where the source used a seeding temp table. All three templates end with `WITH OPTIONS(INTERMINGLE_TABLES=true)` and declare exactly one table, so the shared primary-key space cannot collide.

Deviations: Seeding temp tables have no SQL surface, so every initial leg is a query over the table instead. Every seed the source uses is a real edge of the hierarchy, so `where id = 250` returns precisely the row the source inserts into the temp table.; The source's {300 -> 300} ancestors seed (a node absent from the hierarchy) is replaced by seeding node 1, whose parent -1 is likewise not a node: same property under test, and a literal (300, 300) row in the table would be a self-parent edge and recurse forever. Marked in the file with a comment.; Continuation row-limit schedules ([1,2,1], [1,1,2], [1,-1], [1,2,4,-1]) collapse to `maxRows: 1` with every page listed. `maxRows` is one value per block — QueryConfig.parseConfigs refuses to interleave directives after the first result — so the split points differ from the source's while the asserted sequence does not.; The two unpaged level_order descendants blocks assert the multiset with `unorderedResult` rather than the source's exact sequence, because order within a level is a property of the recursive leg's plan. The level structure the source asserts is instead asserted by the paged level_order blocks, where page membership forces it.; The two-seed ancestors blocks use `unorderedResult`: which seed row the IN list emits first is not derivable, and the multiset is the same for all three traversals the source exercises.; multiplesOf's `Assertions.assertEquals(7, plan.getComplexity())` is dropped — no yamsql directive for plan complexity.; No `supported_version` directives, matching the rest of the directory. recursive-cte.yamsql guards traversal-order queries at 4.7.3.0, but mixed-mode resolves only the most recent released versions (4.13.x), so the guard would be a no-op.

Blockers: Every query carries `- explain: ""`; the plans still have to be generated by a local CORRECT_EXPECTATIONS run, and the generated explains reviewed. The source methods ported here assert results only — none of the six calls assertMatchesExactly — so there is no source plan comment to diff against, and the review has to be a sanity check on the operator shape (RUNION-DFS PREORDER/POSTORDER vs RUNION for level order) instead.; Registration in CascadesQueryTests.java and the MIGRATION.md/README.md entries are left to the central pass, per the file-ownership rule.

Riskiest assumptions: That `traversal order pre_order` over the multiplesOf recursion surfaces as `UNSUPPORTED_QUERY`. The source asserts `UnableToPlanException` and MIGRATION.md records that mapping for GroupByTest, but the SQL front end could reject the statement earlier with a different code.; That the multiplesOf recursion plans at all through SQL. Its recursive leg is a temp-table scan plus a map and a filter with no base-table access — a shape the SQL layer's recursive-CTE tests have never exercised (every query in recursive-cte.yamsql joins the hierarchy table in the recursive leg).; That `select id from (select id * 2 as id from multiples) as sq where id < 50` is classified as the recursive branch (containsReferencesTo does search the subtree, so the nested reference should count) and that the derived table's `id` alias resolves in the WHERE to the product rather than the pre-multiplication value.; That within-level emission order in the paged `level_order` blocks matches the source's sequence. Two candidate orders exist — by the parent that produced the row, and by ascending id from a full `id_parent_idx` scan — and they coincide for both of these data sets; recursive-cte.yamsql's pre-4.7.1 pages give the same sequence for sampleHierarchy. Paging forces an order to be committed to, so if the product picks a third order these four blocks fail.; That the seed `where parent = -1` in the forest file emits root 1 before root 500 (equality on parent via `parent_id_idx`, then ascending id). Every ordered forest expectation depends on it.; That the pre-order/post-order sequences hold for the seed `where id = 10` (descendants of a subtree), which recursive-cte.yamsql does not cover — only the whole-hierarchy seed is empirically confirmed there.; That `seed` is usable as a table name (not in the lexer's keyword list, and `simpleId` accepts keywordsCanBeId anyway) and that a single-column table with `primary key(id)` under INTERMINGLE_TABLES=true is accepted.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `multiplesOfTest` | cascades + @ParameterizedTest(@MethodSource "multiplesOfSuccessParameters") | `recursive-queries-multiples.yamsql` | ported, plan generated | recursive-queries-multiples.yamsql, 8 queries (4 parameter pairs x ANY/level_order). Own file and own schema: this recursion touches no hierarchy table, and its recursive leg has no table access at all — that is the property the traversal cases turn on. The seeding temp table becomes a one-column SEED table (2, 5); the empty-initial case is a seed query that selects no rows (`where id > 1000`). The predicate is written as the source builds it — inner `id * 2 as id`, outer `id < limit` on the product — which is why the seed rows survive limit 0. `assertEquals(7, plan.getComplexity())` has no yamsql surface and is dropped. |
| `multiplesOfTestFailsWithPreorderTraversal` | cascades | `recursive-queries-multiples.yamsql` | ported, plan generated | recursive-queries-multiples.yamsql, last block: same recursion plus `traversal order pre_order`, expecting `- error: UNSUPPORTED_QUERY` (UnableToPlanException maps there via ExceptionUtil). No explain directive, since no plan is produced. The reason the source gives — the DFS implementation cannot match this plan — survives the port intact, because the recursive leg still has no correlated scan to match on. |
| `ancestorsOfNodeTest` | cascades + @ParameterizedTest(@MethodSource "ancestorsOfNodeParameters") | `recursive-queries.yamsql` | ported, plan generated | recursive-queries.yamsql, 9 queries. Seeds {250->50} (`where id = 250`, ordered chain result) and {250->50, 40->10} (`where id in (250, 40)`, unorderedResult: 7 rows with (10,1) and (1,-1) twice — which seed row comes first out of the IN is a plan property, and the multiset is traversal-invariant). The source's third seed {300->300} names a node absent from the hierarchy; a literal (300,300) row would be a self-parent edge and recurse forever, so that trio is covered by seeding node 1, whose parent (-1) is likewise not a node — same property, 'a seed with no matching parent yields just the seed'. Documented in the file header. |
| `descendantsOfNodeTest` | cascades + @ParameterizedTest(@MethodSource "descendantsOfNodeParameters") | `recursive-queries.yamsql` | ported, plan generated | recursive-queries.yamsql, 8 queries: seeds {1->-1} and {10->1} x ANY/pre_order/post_order/level_order. Pre/post/ANY use ordered `result:` — recursive-cte.yamsql asserts the same sequences over the same hierarchy, so the order is observed rather than reasoned. The two level_order blocks use unorderedResult (within-level order is a plan property); the source's exact level sequence is asserted instead by the paged level_order block, where page membership pins it down. |
| `ancestorsOfNodeAcrossContinuation` | cascades + @ParameterizedTest(@MethodSource "ancestorsOfNodeParametersAcrossContinuationParameters") | `recursive-queries.yamsql` | ported, plan generated | recursive-queries.yamsql, `recursive-queries-continuations` block, 3 queries (ANY, pre_order, level_order; the source's [1,2,1] LEVEL case is commented out upstream). The source's three limit schedules [1,2,1], [1,1,2], [1,-1] all concatenate to the same four rows and collapse into one `maxRows: 1` block each, since maxRows is one value per block (QueryConfig.parseConfigs forbids interleaving directives after the first result). Split points differ from the source; the asserted sequence does not. |
| `descendantsOfNodeAcrossContinuations` | cascades + @ParameterizedTest(@MethodSource "descendantsOfNodeAcrossContinuationParameters") | `recursive-queries.yamsql` | ported, plan generated | Split by data set: the sampleHierarchy half is 4 paged queries in recursive-queries.yamsql's continuations block, the sampleForest half is recursive-queries-forest.yamsql (4 paged queries, two roots seeded by `where parent = -1`). Mirrors this method's reversed join-predicate spelling (`h.parent = a.id`) rather than descendantsOf's (`a.id = h.parent`) so the two explains can be compared. Limit schedules [1,-1] and [1,2,4,-1] collapse to `maxRows: 1`; every page is listed plus the terminating empty page. |
| `descendantsWithScanLimitOutOfBand` | cascades + @ParameterizedTest(@MethodSource "descendantsWithScanLimitParameters") | — | not portable | Turns on `ExecuteProperties.setScannedRecordsLimit(n)` and asserts, per returned row, `getRecordsScanned() <= depth + 4` (DFS) or `<= maxLevelSize` (level order). Neither an out-of-band scan limit nor any scanned-records assertion has a yamsql directive; README already records that assertDiscardedNone/assertLoadRecord-style counters are lost. What is left of the method — the traversal equals the unbounded traversal — is what the ported blocks already assert. The wideHierarchy() 401-node data set (and the `useSecondaryIndexes=false` variant) is only there to make the scan-count bound meaningful, so it was not reproduced. |
| `descendantsOfHierarchyWithReparenting` | cascades + @ParameterizedTest(@MethodSource "descendantsOfNodeAfterReparentingAcrossContinuationParameters") | — | not portable | Saves a new `child -> parent` edge *between* continuation resumptions, six scenarios' worth. A yamsql query block is a query plus result directives; there is no way to interleave DML into the page sequence of a single paged query, and the whole point is what a continuation does when the hierarchy moves under it. |
| `randomizedDescendantsTest` | cascades + @ParameterizedTest(@MethodSource "randomizedDescendantsTestParameters") | — | not portable | Generates a random 10-level hierarchy (`Hierarchy.generateRandomHierarchy`) at parameter-provision time and computes the expectation from it with `calculateDescendants`. A yamsql file states data and expectations literally; there is no generator hook, and pinning one instance of the random hierarchy would be a different (much larger) test. |
| `randomizedAncestorsTest` | cascades + @ParameterizedTest(@MethodSource "randomizedAncestorsTestParameters") | — | not portable | As randomizedDescendantsTest, plus `getRandomLeaf()` choosing the seed at run time. |
| `testRecursivePlanEquality` | cascades | — | not portable | Asserts `plan1.equals(plan2)`, equal hashCode, equal toString, equal getComplexity and equal isReverse for two plans built from identical input, and `assertInstanceOf(RecordQueryRecursiveDfsJoinPlan.class, ...)`. Java plan-object identity has no yamsql surface, and unlike the nestedThenWithAnd case there are no two SQL spellings to compare — the input is the same twice. The one externally visible claim, that PREORDER yields a DFS join plan, shows up in the `explain` of every pre_order block in recursive-queries.yamsql. |
| `testRecursiveUnionExpressionEquality` | cascades | — | not portable | `RecursiveUnionExpression.equalsWithoutChildren` under an explicit `AliasMap`, including that two expressions differing only in traversal strategy are unequal. Operates on Cascades expressions directly; no SQL text produces a bare expression to compare. |
| `testRecursiveLevelUnionPlanHashCode` | cascades | — | not portable | `RecordQueryRecursiveLevelUnionPlan.computeHashCodeWithoutChildren()` against `Objects.hash(scanAlias, insertAlias)`, over plans built from `RecordQueryScanPlan(ScanComparisons.EMPTY, false)`. Plan internals; `planHash:` is the nearest directive and is unrelated (and skipped in multi-server configs). |

## `FDBVersionsQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBVersionsQueryTest.java`

Three templates, one per source index configuration, all `WITH OPTIONS(INTERMINGLE_TABLES=true, STORE_ROW_VERSIONS=true)`.

1. `versions-query.yamsql` — MY_SIMPLE_RECORD(rec_no bigint pk, str_value_indexed string, num_value_unique integer, num_value_2 integer, num_value_3_indexed integer, repeater integer array). Reproduces VERSIONS_HOOK exactly: the three indexes test_records_1.proto declares (MY_SIMPLE_RECORD_STR_VALUE_INDEXED, unique MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE, MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED) plus VERSION_INDEX (`select "__ROW_VERSION"`, = `version()`) and VERSION_BY_NUM_VALUE_2_INDEX (`select num_value_2, "__ROW_VERSION" order by num_value_2, "__ROW_VERSION"`, = `concat(field("num_value_2"), version())`). Data: ten INSERT statements of ten rows, mirroring populateRecords().

2. `versions-query-other-record.yamsql` — MY_OTHER_RECORD(rec_no bigint pk, num_value_2 integer, num_value_3_indexed integer), no indexes (MyOtherRecord declares none and VERSIONS_HOOK adds none). Ten INSERTs mirroring populateOtherRecords().

3. `versions-query-long-record.yamsql` — MY_LONG_RECORD(rec_no bigint pk, bytes_value bytes), no indexes. test_records_2.proto's `split_long_records = true` needs no option because yamsql's `enableLongRows` already defaults to true (RecordLayerSchemaTemplate.Builder:444).

MAX_EVER_VERSION verified as a non-issue: it does not appear anywhere in FDBVersionsQueryTest (only in `indexes/VersionIndexTest`). Both indexes in this class are plain IndexTypes.VERSION and both are reachable from SQL DDL — the migration-lead hint applies to a different class.

Deviations: `setRequiredResults` becomes a narrow SQL projection. The record layer still returns whole records when setRequiredResults is set — it is only a planner hint — so `orderByVersionWithSelectiveResults` and `requestVersionWhenQueryIsOnOtherFields` project `rec_no, "__ROW_VERSION"` where the source returns records. This is the closest expressible form and it preserves the asserted plan shape, because projecting the row version defeats covering.; The version *value* cannot be asserted, only its presence. Every version column is matched with `!not_null _`, so the source's `FDBRecordVersion.fromBytes(...)` round-trip, its `assertThat(version, greaterThan(previousVersion))` monotonicity check and `assertVersionsByIdMatch`'s map equality are all lost. What survives is the pinned REC_NO sequence under `order by "__ROW_VERSION"`, which is an indirect monotonicity assertion.; Ordered results are stronger than the source. `assertInVersionOrder` only checks that versions increase; the ordered `result:` blocks pin the exact REC_NO sequence, which is derivable because each INSERT is one of the source's ten transactions.; MyOtherRecord and MyLongRecord live in their own files, so the type filter above the scan is vacuous in the MyOtherRecord case where the source's store really does hold MySimpleRecord records too. Merging them was impossible: all three tables key on the same bigint values, and INTERMINGLE_TABLES=true shares the primary-key space.; REPEATER is declared to mirror MySimpleRecord but never written, so it reads back as NULL rather than the record layer's empty list (the documented directory-wide ARRAY divergence).; yamsql's `enableLongRows` defaults to true, so MY_SIMPLE_RECORD and MY_OTHER_RECORD get `split_long_records = true` where test_records_1.proto does not set it. Directory-wide, not specific to this port, and not plan-affecting at these record sizes.; Store-counter and cursor-level assertions have no yamsql equivalent, as elsewhere in the migration.

Blockers: No run: gradle was out of scope, so all 8 queries carry `- explain: ""` and no `.metrics.binpb`/`.metrics.yaml` companions exist. A CORRECT_EXPECTATIONS pass is needed, after which each generated explain should be checked against the source's Cascades assertion: VERSION_INDEX unbounded for orderByVersion / orderByVersionWithSelectiveResults / versionGraphQuery, VERSION_BY_NUM_VALUE_2_INDEX [[1],[1]] for sortAndFilterWithSingleIndex, MY_SIMPLE_RECORD_STR_VALUE_INDEXED [[even],[even]] for requestVersionWhenQueryIsOnOtherFields / versionsInProjectionOnly, and a filtered type-restricted scan for the two scan files.; Registration is pending: none of the three files is referenced by CascadesQueryTests.java (I was told not to edit it), so they are currently never run.

Riskiest assumptions: Row-version order equals write order — across setup statements and within a single multi-row VALUES list. Every ordered `result:` in versions-query.yamsql depends on this. It is inferred from versions-tests.yamsql, whose ordered `order by "__ROW_VERSION"` blocks (lines 351, 359, 367) hold today, but I could not run anything to confirm it for a ten-statement setup. If it is wrong, the three ordered blocks fail and must become unorderedResult (which would lose the point of orderByVersion).; `order by version` resolves the select alias to the projected row-version expression *and* still lets VERSION_INDEX satisfy the sort, giving the source's map-over-index-scan rather than a sort above the MAP. QueryVisitor.visitOrderByClauseForSelect does the alias lookup, but no existing yamsql file orders by an alias. `order by "__ROW_VERSION"` is the safe fallback and is noted in a comment above the query.; The combination `INTERMINGLE_TABLES=true` + `STORE_ROW_VERSIONS=true` is untested anywhere in the tree: versions-tests.yamsql uses store_row_versions without intermingling, and every cascades-query-tests file uses intermingling without store_row_versions. The two flags are independent fields forwarded to RecordMetaDataBuilder, so this should be fine, but it is unverified — including whether a VERSION index builds cleanly over a bare (non-type-prefixed) primary key.; Hex bytes literals (`x'626c61685f305f30'`) work inside a multi-row VALUES list in a setup step. bytes.yamsql uses them in a single-row-per-tuple insert, which is the same construct, but I did not execute it.; `VERSION` and `NUMBER` are usable as unquoted column aliases (`VERSION` is in `functionNameBase`, `NUMBER` in `keywordsCanBeId`, both accepted by `simpleId`). Checked against the grammar, not run.; All five not-portable methods share one blocker. I classified them "not portable" rather than "deferred" because there is no version literal or version parameter in the grammar today; the lead may prefer "deferred" given versions-tests.yamsql records it as a known TODO.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `orderByVersion` | dual | `versions-query.yamsql` | ported, plan generated | `select * from my_simple_record order by "__ROW_VERSION"`; no setRequiredResults in the source, so `select *`. Ordered result of all 100 rows in write order. 100 rows. |
| `orderByVersionWithSelectiveResults` | dual | `versions-query.yamsql` | ported, plan generated | setRequiredResults(rec_no, version) rendered as `select rec_no, "__ROW_VERSION"`. Projecting the row version defeats covering (versions-tests.yamsql:326), so this should stay the plain VERSION_INDEX scan the source asserts. 100 rows, ordered. |
| `filterByVersion` | dual | — | not portable | `Query.version().greaterThan(records.get(50).getVersion())` needs a version literal or bound parameter. SQL has neither: versions-tests.yamsql carries the attempt commented out with "Need to have a solution for casting versions as query arguments for this one". A scalar subquery would not reproduce the asserted `versionIndex ([<v>],>` scan bound. Unblocks if a version literal/cast lands. |
| `residualVersionFilter` | dual | — | not portable | Same blocker as filterByVersion — the version predicate has no SQL spelling. The interesting property (the version predicate demoted to a residual filter above a NUM_VALUE_UNIQUE scan) cannot be reached without it. |
| `residualVersionFilterWithSelectiveResults` | dual | — | not portable | Same blocker as residualVersionFilter. |
| `sortAndFilterWithSingleIndex` | dual | `versions-query.yamsql` | ported, plan generated | `where num_value_2 = 1 order by "__ROW_VERSION"` — VERSION_BY_NUM_VALUE_2_INDEX bounds the equality and supplies the version order. NUM_VALUE_2 = j % 3 so this is j in {1,4,7} from all ten transactions: 30 rows, ordered. |
| `sortFilterOnVersionIndexEntries` | dual | — | not portable | `Query.and(num_value_2 = 1, version() != excludedVersion)` — the notEquals needs a version literal. Same blocker as filterByVersion. |
| `requestVersionWhenQueryIsOnOtherFields` | dual | `versions-query.yamsql` | ported, plan generated | `select rec_no, "__ROW_VERSION" ... where str_value_indexed = 'even'`. 50 rows, unorderedResult — the source has no setSort (it sorts its expectation by primary key only because that is the plan's emission order). |
| `versionGraphQuery` | cascades | `versions-query.yamsql` | ported, plan generated | `select "__ROW_VERSION" as version, rec_no as number ... order by version` — ORDER BY resolves the select alias (QueryVisitor.visitOrderByClauseForSelect:940-968), matching the source's sort on the projected column. 100 rows, ordered. The source's byte-level version decode and monotonicity check degrade to `!not_null _` plus the pinned NUMBER sequence. |
| `versionInSubSelectQuery` | cascades | — | not portable | `version <= versionForQuery` applied outside a sub-select. Same version-literal blocker; the sub-select nesting itself is expressible (versions-tests.yamsql:276) but the predicate is not. |
| `versionsInProjectionOnly` | cascades | `versions-query.yamsql` | ported, plan generated | `select "__ROW_VERSION" as version, rec_no ... where str_value_indexed = 'even'`. Same access path as requestVersionWhenQueryIsOnOtherFields with the version projected first and aliased, which is how the source's graph names it. 50 rows, unordered. |
| `versionsInProjectionOnlyFromScan` | cascades | `versions-query-other-record.yamsql` | ported, plan generated | Own file: MyOtherRecord has no indexes (that is the point — the plan is a filter over a type-filtered scan), and its primary keys `j*100+i` collide with MySimpleRecord's under INTERMINGLE_TABLES=true, so the two tables cannot share a template. 30 rows (num_value_2 = 1 → j in {1,4,7}), unordered. |
| `versionsInProjectionOnlyFromScanOfOnlyType` | cascades | `versions-query-long-record.yamsql` | ported, plan generated | Own file: the source switches to TestRecords2Proto with only MyLongRecord and a hook that just sets storeRecordVersions. rec_no = i*100+j, bytes_value = "blah_<i>_<j>" written as hex literals `x'626c61685f…'`. 100 rows, unordered (the source asserts a rec_no→version map). |

## `FDBRecordStoreQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBRecordStoreQueryTest.java`

All five files declare the same single MY_SIMPLE_RECORD table (rec_no bigint pk, str_value_indexed string, num_value_unique integer, num_value_2 integer, num_value_3_indexed integer, repeater integer array) with WITH OPTIONS(INTERMINGLE_TABLES=true), mirroring test_records_1.proto. Two index configurations: (a) record-store-query.yamsql reproduces complexQuerySetupHook() — the three proto indexes MY_SIMPLE_RECORD_STR_VALUE_INDEXED, MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE (unique), MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED, plus MULTI_INDEX over (str_value_indexed, num_value_2, num_value_3_indexed) and the REPEATER_FANOUT fan-out index — and uses the shared cascades-query-tests/includes/simple-record-100.yamsql fixture; (b) the four -even-odd / -continuation / -exclude-null / -null files carry only the three proto indexes (openSimpleRecordStore with no hook). Those four share one index set but differ in data, and yamsql has one data set per file, so they are split by data rather than by index configuration — each header says so. No USE INDEX anywhere. 10 query blocks total, every one with an `- explain: ""` placeholder.

Deviations: queryWithContinuation's first query drops setAllowedIndexes(Collections.emptyList()): it is written as an unrestricted `select * from my_simple_record`, so the plan may be a full index scan where the source asserts `Scan(<,>) | [MySimpleRecord]`. This is the documented systematic divergence (an index that covers the projection is cheaper than reading records); USE INDEX cannot express 'forbid every index'. Recorded in the file header.; queryWithContinuation drops all three returned-row limits (10, 5, 15) and the source's assertion that REC_NO comes back ascending. maxRows: needs a predictable page order and none of the three source queries declares a sort; adding an ORDER BY would deviate from the source. Same precedent as testComplexLimits3/4/5 elsewhere in the migration. Continuation coverage instead rides on the ForceContinuations configuration.; Every result block except nullQuery's single-row one is unorderedResult. None of the six ported methods calls setSort, and none of the sources asserts an order except queryWithContinuation (see above), so per the suite's convention tie order is not asserted.; Store-counter assertions (assertDiscardedNone / assertDiscardedAtMost) in query, queryWithContinuation, testParameterQuery1, nullQuery, queryExcludeNull and testParameterBindings are lost — no yamsql equivalent.; testParameterBindings' boundQuery.isCompatible(recordStore, bindings) assertions (one compatible, one incompatible binding set) are not reproduced: that is a BoundRecordQuery API property, not a planning or execution property.; The source's unset proto `repeater` field reads back as an empty list in the record layer but as NULL for an omitted SQL ARRAY column, so every row explicitly inserts `[]` (the same choice the shared include and nested-field-query-nested-pk.yamsql make). All other unset scalar fields are inserted as explicit `null` and expected as `!null _`.; No README.md-style `#` plan comment was copied above any query — the task brief overrides that README convention ("Do NOT transcribe the source's plan into a comment"). Where a source plan shape is load-bearing for explaining a divergence (queryWithContinuation's record scan, queryExcludeNull's `([null],[2])` range) it is described in the file header instead.; No `record-store-query-uncommon-pk.yamsql` was created, although README.md's 'Still to port' table anticipates one: testUncommonPrimaryKey turned out to need a multi-record-type index and a two-table query, so it is not portable.

Blockers: CREATE TYPE AS ENUM blocks enumFields and enumFieldsWithoutIndex — the two methods that would otherwise port cleanly (both need MyShapeRecord with size/color/shape enum columns; enumFields adds one index per column, enumFieldsWithoutIndex adds none).; Registration is still needed: none of the five files is referenced by yaml-tests/src/test/java/CascadesQueryTests.java, which I was told not to edit. Until they are registered they will silently never run.; All 10 explain values are `""` placeholders and no .metrics.binpb companions exist; a CORRECT_EXPECTATIONS pass is required before these files pass, and the resulting plans should be reviewed against the Cascades branch of each source assertion (notably the divergence expected on queryWithContinuation's unrestricted first query).

Riskiest assumptions: record-store-query-continuation.yamsql inserts 100 rows with NUM_VALUE_UNIQUE NULL and record-store-query-exclude-null.yamsql inserts 100 rows with both NUM_VALUE_UNIQUE and STR_VALUE_INDEXED NULL, under a UNIQUE index on NUM_VALUE_UNIQUE. I verified this is safe in the record layer — StandardIndexMaintainer:471 skips checkUniqueness when indexEntry.keyContainsNonUniqueNull(), and the default NullStandin is NULL — and the source tests do exactly this. I did NOT verify the Relational insert path adds no uniqueness pre-check of its own. If it does, both files fail in setup.; I assumed `<>` and `is not null` are accepted by the SQL grammar based on their use in boolean.yamsql and uuid-non-prepared.yamsql, not by running anything.; queryWithContinuation's first query is `select * from my_simple_record` with no predicate. I assumed the planner accepts an unfiltered select and does not, e.g., reject it for lack of a bound; nothing in the suite exercises exactly that shape today.; The 100-row expected result for that same query is the largest single expectation in these files. It is machine-derived from the source's setupSimpleRecordStore loop, but if the Relational layer represents an inserted `[]` array as NULL rather than an empty array on read-back, all 300-odd REPEATER cells in the -continuation file would mismatch. I relied on filter-coalescing.yamsql expecting `REPEATER: []` for the same insert form.; For testParameterBindings I assumed the duplicated `str_value_indexed = 'even' and str_value_indexed = 'even'` is accepted and folds, since that folding is precisely what the source's compatible-binding case exercises. If the SQL front end instead keeps both predicates, the plan will differ from the source's while the rows stay right.; enumFields / enumFieldsWithoutIndex are marked deferred rather than not portable on the strength of MIGRATION.md's existing ENUM entries; I did not independently test whether CREATE TYPE AS ENUM plus an equality on the enum column works today.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `query` | dual | `record-store-query-even-odd.yamsql` | ported, plan generated | 1 query: str_value_indexed = 'even'. Own 100-row data set — num_value_unique = i + 1000 (counting up, unlike the shared fixture's 1000 - i), num_value_2/num_value_3_indexed unset. 50 rows, unordered (source asserts a count and a per-row property, no sort, no order assertion). |
| `queryWithContinuation` | dual | `record-store-query-continuation.yamsql` | ported, plan generated | 3 queries (no predicate; str_value_indexed = 'odd'; num_value_2 = 0) over own data with num_value_2 = i % 2. Two source features not reproduced and noted in the header: the first query's setAllowedIndexes(emptyList()) has no yamsql equivalent so its plan may be a full index scan rather than a record scan; the returned-row limits (10/5/15) and the ascending REC_NO assertion are dropped because none of the three queries declares a sort, so maxRows page membership is not derivable. Continuation coverage comes from the ForceContinuations config. All three expectations unordered (100/50/50 rows). |
| `testParameterQuery1` | dual | `record-store-query.yamsql` | ported, plan generated | 2 queries, one per source binding set: ('even', 1) and ('odd', 2). Uses the shared simple-record-100 include with the complexQuerySetupHook index set. 16 rows each, matching the source's assertEquals(16, i). equalsParameter needs no special treatment — statement_type defaults to both. |
| `nullQuery` | dual | `record-store-query-null.yamsql` | ported, plan generated | 2 queries over three hand-written rows, the third with STR_VALUE_INDEXED unset. `<> 'yes'` excludes the NULL row under three-valued logic exactly as the record layer's notEquals does, so 1 row (ordered result, single row); `is not null` gives 2 rows (unordered). |
| `queryExcludeNull` | dual | `record-store-query-exclude-null.yamsql` | ported, plan generated | 1 query: num_value_3_indexed < 2. Own data — only even REC_NOs set NUM_VALUE_3_INDEXED (to i % 5); the other 50 rows leave it NULL, which is the point. 20 rows, unordered. |
| `FDBQueryCompatibilityTest.testParameterBindings` | dual | `record-store-query.yamsql` | ported, plan generated | 1 query with the duplicated STR_VALUE_INDEXED equality kept verbatim (the source binds $p1 and $p2 to the same value). 3 rows (REC_NO 18, 48, 78), matching assertEquals(3, i). The boundQuery.isCompatible(...) assertions are a BoundRecordQuery API property with no yamsql surface and are not reproduced. |
| `queryWithShortTimeLimit` | dual | — | not portable | Asserts RecordCursor.NoNextReason.TIME_LIMIT_REACHED and the number of transactions it took (11) under setTimeLimit(1). Neither the time limit nor the no-next-reason has a yamsql directive, and the behaviour is timing-dependent across configurations. Stays in JUnit (confirmed by the migration lead). |
| `testPartialRecordScan` | dual | — | not portable | setAllowedIndexes(emptyList()) plus assertions on plan.hasRecordScan() / !plan.hasFullRecordScan(). Plan predicates have no yamsql directive, and USE INDEX cannot express 'no index at all' (it drops the primary-scan candidate rather than the indexes). |
| `testUncommonPrimaryKey` | dual | — | not portable | setRecordTypes over two of three TestRecordsMulti types that share no primary key, against addMultiTypeIndex('onetwo$element'). A SQL index has exactly one base table (RecordLayerSchemaTemplate.computeIndexes has an explicit TODO for multi-type indexes) and a SQL query one table per leg, so both queries in the method would become UNIONs and plan differently. This is why no `record-store-query-uncommon-pk.yamsql` was created despite the placeholder name in README.md. |
| `enumFieldsWithWrongTypes` | dual + @ParameterizedTest(@MethodSource) | — | not portable | Passes an EnumValueDescriptor, its proto form, a builder, or an int as the comparand and expects RecordCoreException 'Comparison value of incorrect type'. SQL cannot present a protobuf descriptor as a literal, so the wrong-type case has no spelling. |
| `enumFields` | dual | — | deferred | TestRecordsEnum MyShapeRecord with three single-column indexes over size/color/shape. Needs CREATE TYPE AS ENUM, which the docs flag as not working for queries; same blocker as FDBAndQueryToIntersectionTest.sortedIntersection* and FDBInQueryTest.enumIn. |
| `enumFieldsWithoutIndex` | dual | — | deferred | Same schema with NO_HOOK, asserting the enum equality survives as a residual filter over a type-filtered scan. Same CREATE TYPE AS ENUM blocker. |
| `queryByteString` | @Test | — | out of scope | Plain @Test — old planner only. |
| `queryUuid` | @Test | — | out of scope | Plain @Test — old planner only. |
| `queryByteStringWithZero` | @Test | — | out of scope | Plain @Test — old planner only. |
| `queryComplexityLimitsForInUnion` | @Test | — | out of scope | Plain @Test — old planner only; also asserts RecordQueryPlanComplexityException log keys. |
| `queryOrderedUnionComplexityLimit` | @Test | — | out of scope | Plain @Test — old planner only. |
| `queryUnorderedUnionComplexityLimit` | @Test | — | out of scope | Plain @Test — old planner only. |
| `queryIntersectionComplexityLimit` | @Test | — | out of scope | Plain @Test — old planner only. |
| `uuidPrimaryKey` | @Test | — | out of scope | Plain @Test — old planner only. |
| `nullableInt32` | @Test | — | out of scope | Plain @Test — old planner only. |
| `doesNotNormalizeLargeCnf` | @Test | — | out of scope | Plain @Test — old planner only; asserts BooleanNormalizer behaviour, which is on the old-planner path. |
| `doesNotNormalizeBigExpression` | @Test | — | out of scope | Plain @Test — old planner only. |
| `FDBQueryCompatibilityTest.testQueryCompatibilityDifferentStructure` | @Test | — | out of scope | Plain @Test; compares BoundRecordQuery equality/hashCode and never plans or executes. |
| `FDBQueryCompatibilityTest.testQueryCompatibilityUniqueUsages` | @Test | — | out of scope | As above. |
| `FDBQueryCompatibilityTest.testQueryCompatibilityDifferentParameters` | @Test | — | out of scope | As above. |
| `FDBQueryCompatibilityTest.testQueryCompatibilityNestedStructure` | @Test | — | out of scope | As above. |
| `FDBQueryCompatibilityTest.testQueryCompatibilityNestedStructure2` | @Test | — | out of scope | As above. |
| `FDBQueryCompatibilityTest.testQueryCompatibilityConstraintSimple` | @Test | — | out of scope | As above. |
| `FDBQueryCompatibilityTest.testQueryCompatibilityConstraintComplex` | @Test | — | out of scope | As above. |

## `FDBQueryCompatibilityTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBQueryCompatibilityTest.java`

One file, one schema template, one index configuration. The template reproduces `complexQuerySetupHook()` exactly: table MY_SIMPLE_RECORD(REC_NO bigint pk, STR_VALUE_INDEXED string, NUM_VALUE_UNIQUE integer, NUM_VALUE_2 integer, NUM_VALUE_3_INDEXED integer, REPEATER integer array) with the three `test_records_1.proto` indexes (MY_SIMPLE_RECORD_STR_VALUE_INDEXED, unique MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE, MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED) plus MULTI_INDEX ordered by (STR_VALUE_INDEXED, NUM_VALUE_2, NUM_VALUE_3_INDEXED) and REPEATER_FANOUT over the unnested array. `WITH OPTIONS(INTERMINGLE_TABLES=true)`; data comes from `includes/simple-record-100.yamsql`, the SQL equivalent of `complexQuerySetup`. This is byte-for-byte the same schema as `and-query-to-intersection.yamsql`, kept as a separate file per the prefix-ownership rule; no USE INDEX anywhere.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testParameterBindings` | dual | `record-store-query.yamsql` | ported, plan generated | folded into `record-store-query.yamsql` — its template is byte-identical and its query duplicated that file's third entry; the standalone `query-compatibility.yamsql` was removed. One query, one block. Ported as the executable half only: the query as bound by the source's `compatibleBindings` (p1 = p2 = 'even', p3 = 3, num_value_2 = 0 as a literal). The duplicated STR_VALUE_INDEXED equality is transcribed as written, since the point is that the redundant conjunct must not change the plan. Expected 3 rows (REC_NO 18, 48, 78), which is exactly the source's `assertEquals(3, i)` and its per-row checks. `unorderedResult` because the source sets no sort. Lost: `BoundRecordQuery.isCompatible` (true for p1 = p2 = 'even', false for p1 = 'even' / p2 = 'odd') — yamsql cannot plan against one binding set and execute against another, and there is no plan-cache surface; also lost is `TestHelpers.assertDiscardedNone`. The source binds p3 to the string "-10" at plan time and the integer 3 at execute time, which is likewise only observable through BoundRecordQuery. No source plan assertion exists (the test calls `planQuery` and executes but never matches the plan), so the `explain: ""` placeholder has no `#` plan comment to be diffed against. |
| `testQueryCompatibilityDifferentStructure` | @Test | — | out of scope | Plain @Test, so useCascadesPlanner is false and only the old planner runs it. Also asserts only BoundRecordQuery.equals/hashCode across two record types ("MySimpleRecord1" vs "MySimpleRecord"), which has no SQL surface. |
| `testQueryCompatibilityUniqueUsages` | @Test | — | out of scope | Plain @Test. Asserts two identically built and identically bound BoundRecordQuery values are equal — a Java object contract, not query behaviour. |
| `testQueryCompatibilityDifferentParameters` | @Test | — | out of scope | Plain @Test. Asserts BoundRecordQuery inequality when one parameter is renamed p1 -> pa; parameter naming is not observable through SQL. |
| `testQueryCompatibilityNestedStructure` | @Test | — | out of scope | Plain @Test. BoundRecordQuery equality over `nested.oneOfThem()` bindings; nothing is planned or executed. |
| `testQueryCompatibilityNestedStructure2` | @Test | — | out of scope | Plain @Test. Same as above with three nested parameters, asserting that equality of two of them is part of the binding constraint. |
| `testQueryCompatibilityConstraintSimple` | @Test | — | out of scope | Plain @Test. Exercises the equal-parameters constraint (p1 = p2) directly through BoundRecordQuery equality; the same constraint is what testParameterBindings' isCompatible check reads, and it is not reproducible in yamsql. |
| `testQueryCompatibilityConstraintComplex` | @Test | — | out of scope | Plain @Test. Eight-way equal-parameters constraint, again asserted only via BoundRecordQuery equality. |

## `FDBLongArithmeticFunctionQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBLongArithmeticFunctionQueryTest.java`

Every file declares one MY_SIMPLE_RECORD table mirroring TestRecords1Proto.MySimpleRecord (rec_no bigint primary key, str_value_indexed string, num_value_unique integer, num_value_2 integer, num_value_3_indexed integer, repeater integer array) WITH OPTIONS(INTERMINGLE_TABLES=true), plus the three indexes test_records_1.proto declares (MY_SIMPLE_RECORD_STR_VALUE_INDEXED, unique MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE, MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED). openSimpleRecordStore does not apply complexQuerySetupHook, so MULTI_INDEX and the repeater fan-out index are correctly absent. Each file then adds the one index set its source method builds: (1) add(num_value_2, num_value_3_indexed); (2) bitand(num_value_2, 2); (3) bitand(num_value_2, 1); (4) concat(str_value_indexed, add(nv2, nv3), bitand(num_value_unique, 4)); (5) bitand(num_value_unique, 4); (6) a plain value index (num_value_2, num_value_3_indexed); (7) eight single-component function indexes, one per binary operator. Function indexes come from CREATE INDEX ... AS SELECT <arith expr>: MaterializedViewIndexGenerator.toKeyExpression (:567-575) turns an ArithmeticValue into function(<LogicalOperator name lowercased>, concat(args)) and a LiteralValue into Key.Expressions.value(...), which reproduces the source's Key.Expressions.function("add"/"bitand", ...) exactly. Index names are transliterated because $, + and & are not identifier characters (MySimpleRecord$num_value_2&2 becomes MY_SIMPLE_RECORD_NUM_VALUE_2_BITAND_2, etc.). Data sets are per-file, each 100 rows reproducing that method's setupSimpleRecordStore lambda; columns the lambda leaves unset are NULL and REPEATER is inserted as [] (the record layer reads an unset repeated field back as an empty list).

Deviations: basicFunctionPlans: the source rebuilds the store with a single MySimpleRecord$binaryFunction index per parameter; long-arithmetic-functions-binary.yamsql carries all eight function indexes in one template instead of eight near-identical files. Sound here because the source's assertion is indexPlan().where(scanComparisons(range("[EQUALS $p]"))) with no index name - any equality-bounded index scan satisfies it - and no other function index can match a given operator's predicate. indexed-functions.yamsql already keeps DMASK1 and DMASK2 side by side the same way. Documented in the file header.; basicFunctionPlans: two representative predicate values per operator instead of every distinct function value over the data (55 across the eight operators). Values chosen for small result sets.; complexIndexGraphQueryWithMaskInResults: 3 of the source's 130 ($str, $sumLowerBound, $sumUpperBound) bindings.; sum2And3Query and numValue2MaskQuery/doesNotMatchIncorrectMask port every one of the source's bindings (12 and 3 respectively), including the empty ones.; complexIndex: setRequiredResults(bitand(num_value_unique, 4)) not reproduced; the query is select *. Reason in the file header - the record layer returns whole records regardless, and a narrow projection would flip the plan to the covering scan the source asserts does not occur. The masked projection is covered by complexIndexGraphQueryWithMaskInResults in the same file.; complexIndex and complexIndexGraphQueryWithMaskInResults use unorderedResult although the source asserts an order: their ORDER BY keys tie across many rows and the source's tie order is the forward index scan's trailing primary key, which the query does not state.; TestHelpers.assertDiscardedNone (numValue2MaskQuery, complexIndexGraphQueryWithMaskInResults) has no yamsql equivalent.; Plan-hash assertions dropped (out of scope per README).; REPEATER is inserted as [] rather than omitted, matching the record layer's read-back of an unset repeated field; expected rows carry REPEATER: [].; long-arithmetic-functions-two-column.yamsql is named for its index configuration rather than its source method (calculateFunctionFromCoveringIndexScan), because the source asserts a *non*-covering plan and a -covering suffix would mislead.; The index literal's boxed type differs: the source writes Key.Expressions.value(4L), SQL parses a bare 4 as an Integer (ParseHelpers.parseDecimal). Not observable - LongArithmethicFunctionKeyExpression reads arguments through Key.Evaluated.getNullableLong.

Blockers: basicFunctionPlans' three unary parameterizations are permanently not portable: the grammar has no unary expression operator (unaryOperator is reachable only from a column DEFAULT clause, RelationalParser.g4:977) and SqlFunctionCatalogImpl.createSynonyms has no entry for ~ / bitnot. Also missing from the catalog: << and >> (they are in bitOperator but unmapped), so shift-based function indexes are unreachable. Nothing is lost for Cascades - the source expects those cases to throw "unknown function".; Registration in CascadesQueryTests.java and the MIGRATION.md rows are left to the central pass, per the file-ownership rule. All seven files still carry - explain: "" placeholders and have no .metrics.binpb companions, so they need a CORRECT_EXPECTATIONS run before they can pass.

Riskiest assumptions: The one existing precedent for arithmetic/mask indexes, indexed-functions.yamsql, uses BIGINT columns; these files use INTEGER to mirror the proto's int32. Both sides of the match are then ADD_II/BITAND_II so they should agree, and the index value itself is stored as a Tuple long either way - but this exact type combination is unproven.; matchConstantMaskValue and the mask files rely on a literal mask inside the index key still matching when the *query's* mask literal is lifted to a parameter in the prepared-statement half of statement_type: both. indexed-functions.yamsql demonstrates this for `d & 1 = 1`, so I believe it holds, but it is the load-bearing mechanism for 12 of the 49 queries.; complex_index is a three-component index whose 2nd and 3rd components are arithmetic expressions, and the ORDER BY has to repeat both. Precedent (bPlusCByE) covers one arithmetic component in a two-component index; three is an extrapolation. If reorderValues' Value.equals comparison fails on either expression the DDL will be rejected as "order by must be a subset of projection list".; complexIndex's query asks for a two-key ordering where both keys are arithmetic expressions (order by nv2 + nv3, num_value_unique & 4) behind an equality on str_value_indexed. Precedent covers a single arithmetic sort key. If the second key does not match, the plan gains a sort or fails to plan.; Putting eight function indexes in one template (binary file) does not perturb any plan. Argued from "a different function is a different Value, hence no candidate" plus "select * needs REPEATER, so no covering competition", not from a run.; `as sum` as a column alias parses (SUM is in keywordsCanBeId, and indexed-functions.yamsql:53 uses it); `as mask` and `as id` are plain identifiers.; 100 rows with NULL NUM_VALUE_UNIQUE do not trip the unique index. Verified in source: StandardIndexMaintainer.updateOneKeyAsync guards checkUniqueness with !indexEntry.keyContainsNonUniqueNull().; Expected rows for `/` and `%` assume SQL's DIV_II/MOD_II match Java semantics, which they do verbatim in ArithmeticValue.PhysicalOperator; num_value_3_indexed = (i % 5) + 1 is never 0 so there is no divide-by-zero.; Validation was YAML-parse only (SafeLoader plus a multi_constructor for the ! tags): 3 documents, one test_block with preset single_repetition_ordered, an - explain: "" on every query, trailing newline. No Gradle run, so nothing about DDL acceptance, plans or rows has been executed.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `sum2And3Query` | dual | `long-arithmetic-functions.yamsql` | ported, plan generated | long-arithmetic-functions.yamsql, 12 queries (one per $sum in -1..10, five of them empty). Index add(num_value_2, num_value_3_indexed) + the three proto indexes. select * (source sets no setRequiredResults); unorderedResult (source uses containsInAnyOrder). |
| `numValue2MaskQuery` | dual | `long-arithmetic-functions-mask-2.yamsql` | ported, plan generated | long-arithmetic-functions-mask-2.yamsql, 3 queries ($mask 0..2; mask=1 empty because (i & 2) is only 0 or 2). Own file: index bitand(num_value_2, 2). assertDiscardedNone is not reproducible. |
| `doesNotMatchIncorrectMask` | dual | `long-arithmetic-functions-mask-1.yamsql` | ported, plan generated | long-arithmetic-functions-mask-1.yamsql, 3 queries. Own file because the index set is the discriminating detail: bitand(num_value_2, 1) only, queried with mask 2, so nothing can match. Same data and same rows as the mask-2 sibling; the pair exists so the two explains can be diffed. |
| `complexIndex` | dual | `long-arithmetic-functions-complex.yamsql` | **ported, FAILING** | `UnableToPlanException` with the source's sort. **Root cause found:** `AbstractDataAccessRule.satisfiesRequestedOrdering:801,814` compares ordering values with plain `Object.equals`, so the SQL side's `ConstantObjectValue(@21)` never matches the index's `LiteralValue(4)`; all matches are discarded at `:319-321`. `ValueEquivalence.constantEquivalenceWithEvaluationContext` exists for this and is what the predicate path uses. Queries skipped with a `supported_version: "!max_version"` sentinel; ORDER BY kept. See `.out/issue-ordering-match-constant-object-value.md` |
| `complexIndexGraphQueryWithMaskInResults` | cascades | `long-arithmetic-functions-complex.yamsql` | ported, plan generated | Same file as complexIndex (same index, same data). 3 of the source's 130 (str, lower, upper) bindings ported - the plan is identical for all of them: ('even',1,4), ('odd',1,4), ('even',-1,9). This is the projection that surfaces the masked value, which is why complexIndex could stay select *. |
| `matchConstantMaskValue` | cascades | `long-arithmetic-functions-unique-mask-4.yamsql` | ported, plan generated | long-arithmetic-functions-unique-mask-4.yamsql, 9 queries (masks 1, 2, 4 x right-hand sides 0, mask, mask+1). The source's ConstantObjectValue mask is covered by statement_type: both, whose prepared half lifts the mask literal to a parameter - the same shape indexed-functions.yamsql already relies on. OUTCOME (2026-09-08): all *nine* queries get one plan, `ISCAN(MY_SIMPLE_RECORD_STR_VALUE_INDEXED <,>) + FILTER _.NUM_VALUE_UNIQUE & @c7 EQUALS promote(@c9 AS INT) + MAP`, byte-identical down to the constant ordinals - including the three mask-4 queries that ought to match MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE_BITAND_4. This is **not** a matching defect: the mask-1/mask-2 sibling pair proves a COV mask matches a literal index mask (byte-identical query `num_value_2 & 2 = 0`, matched in mask-2.yamsql, correctly unmatched in mask-1.yamsql). It is a *port* artifact - after AstNormalizer lifts the literals, all nine queries are the same normalized query, so they share one plan-cache entry and all reuse the first-planned one (mask 1, which correctly cannot match). CONSEQUENCE: the *positive* half of this method - that mask 4 matches - is currently NOT covered by this file. Restoring it needs the mask-4 queries in their own file (own database, own cache). The three negative masks are covered. |
| `calculateFunctionFromCoveringIndexScan` | cascades | `long-arithmetic-functions-two-column.yamsql` | ported, plan generated | long-arithmetic-functions-two-column.yamsql, 1 query, 100 rows. Despite the class, the index is a plain two-column value index, so it needs a top-level ORDER BY naming both columns. The SQL projection (num_value_2, num_value_3_indexed, rec_no) is fully covered by the index, so the generated plan may be the covering scan the source's comment says it should have been rather than the record scan it asserts; rows are the same either way. |
| `basicFunctionPlans (binary parameters)` | dual | `long-arithmetic-functions-binary.yamsql` | ported, plan generated | long-arithmetic-functions-binary.yamsql, 16 queries. The source's eleven binary names collapse to eight SQL operators because subtract/sub, multiply/mul and divide/div are aliases of one function; +, -, *, /, %, &, /, ^ map onto add, sub, mul, div, mod, bitand, bitor, bitxor in SqlFunctionCatalogImpl.createSynonyms, which is exactly the binary set LongArithmethicFunctionKeyExpressionFactory registers. Two representative predicate values per operator instead of all 55 distinct function values. |
| `basicFunctionPlans (unary parameters)` | dual | — | not portable | The three unary cases (subtract/sub as negation, bitnot) have no SQL spelling. There is no unary expression operator in the grammar at all - unaryOperator is reachable only from a column DEFAULT clause (RelationalParser.g4:977), never from expressionAtom - and no ~ / bitnot entry in SqlFunctionCatalogImpl.createSynonyms. Nothing is lost: under Cascades the source itself expects planning to fail with "unknown function" and returns early. |
| `basicFunctionPlans() (MethodSource supplier)` | none | — | out of scope | Argument supplier, not a test. |

## `RecordTypeKeyTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/RecordTypeKeyTest.java`

One file, one schema template, one index configuration. `record-type-key.yamsql` reproduces `simpleMetaData` + `RecordTypeKeyTest.BASIC_HOOK`: tables MY_SIMPLE_RECORD (rec_no bigint, str_value_indexed string, num_value_unique integer, num_value_2 integer, num_value_3_indexed integer, repeater integer array, primary key(rec_no)) and MY_OTHER_RECORD (rec_no bigint, num_value_2 integer, num_value_3_indexed integer, primary key(rec_no)), with the three proto-declared MySimpleRecord indexes (MY_SIMPLE_RECORD_STR_VALUE_INDEXED, MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE unique, MY_SIMPLE_RECORD_NUM_VALUE_3_INDEXED) and no index on MY_OTHER_RECORD. The template ends `WITH OPTIONS(INTERMINGLE_TABLES=false)` — the documented exception for this class: with intermingling off, RecordLayerTable.Builder seeds primaryKeyParts with Key.Expressions.recordType() and appends the declared columns, yielding concat(recordType(), field("rec_no")) for both tables, exactly BASIC_HOOK's pkey. Data is `saveSomeRecords` verbatim: MY_SIMPLE_RECORD (123, 'abc', nv2 1, nv3 1) and (456, 'xyz', nv2 2, nv3 2), plus MY_OTHER_RECORD (123, nv2 2, nv3 2) — the deliberate REC_NO 123 collision that only the record-type prefix resolves. NUM_VALUE_UNIQUE and REPEATER are unset in the source and therefore NULL here (expected as `!null _`). BASIC_HOOK's three universal COUNT indexes (globalRecordCount / globalRecordUpdateCount removed, countByRecordType added) are not reproducible — a universal index spans record types — and no in-scope query can match them. All three explains are `""` placeholders awaiting a CORRECT_EXPECTATIONS pass.

Deviations: Unset ARRAY column: the source's `repeater` reads back as an empty list in the record layer; here REPEATER is NULL (expected as `!null _`). Already a known, catalogued difference in README.md.; BASIC_HOOK's universal COUNT indexes are absent: `globalRecordCount` and `globalRecordUpdateCount` (which the hook removes anyway) and `countByRecordType` (which it adds). A universal index spans record types and `RecordLayerSchemaTemplate.computeIndexes` cannot express one. No in-scope query can match `countByRecordType`, so no coverage is lost.; `INTERMINGLE_TABLES=false` instead of the directory-wide `true`. This is the sanctioned exception for this class, not an oversight — the record-type prefix is the subject under test.

Riskiest assumptions: `INTERMINGLE_TABLES=false` with two tables whose declared primary keys are both `rec_no` and whose values collide (123) is accepted and correctly disambiguated. Evidence: the existing `yaml-tests/src/test/resources/record-type-key-tests.yamsql` uses `WITH OPTIONS(INTERMINGLE_TABLES=false)` over two tables and gets `SCAN([IS T1, EQUALS ...])` plans, but its primary-key ranges do not overlap (10-12 vs 100-102). I am relying on the record-type prefix, not on distinct values.; The unique index over NUM_VALUE_UNIQUE tolerates two NULL rows. Evidence: `sort-index-selection-primary-key.yamsql` inserts 100 rows with NUM_VALUE_UNIQUE unset under the same unique index and has generated metrics, so it ran; the record layer's default null standin makes null index entries non-unique (`containsNonUniqueNull`). Unverified that INTERMINGLE_TABLES=false changes nothing here — it should not, the standin is unrelated.; Ordered `result:` for `testScan` despite no ORDER BY. I argued the sequence is plan-independent for this 2-row data set (REC_NO, STR_VALUE_INDEXED and NUM_VALUE_3_INDEXED all order 123 before 456; NUM_VALUE_UNIQUE ties and breaks on the primary key). If the generated plan turns out to be something I did not anticipate and the order flips, relax to `unorderedResult` — the source does assert the order, so I kept it.; `testScan` may plan an index scan + fetch rather than the record scan the source asserts, per the documented systematic divergence. I expect the record scan to win because no index here covers `select *` over six columns, but I did not run it.; `testIndexScan` marked not portable rather than ported-with-a-note. The judgement is that reproducing it with INTERMINGLE_TABLES=false yields a byte-identical duplicate of `testScan` under a schema that is not the source's hook, and with intermingling on it is neither hook. If the migration lead would rather have a second entry documenting the Cascades expectation, that is a one-line addition.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `testScan` | dual | `record-type-key.yamsql` | ported, plan generated | BASIC_HOOK; `select * from my_simple_record`, no filter, no sort. Ordered `result:` with 2 rows although the query has no ORDER BY: every candidate access path agrees on (123, 456) because REC_NO, STR_VALUE_INDEXED and NUM_VALUE_3_INDEXED all rank them the same way and NUM_VALUE_UNIQUE is NULL in both rows. The MY_OTHER_RECORD row with the same REC_NO must not appear. Possible plan divergence: the source asserts a record scan bounded by the record-type range; SQL may prefer an index scan + fetch (documented systematic divergence in README.md), though no index here covers `select *`. |
| `testSinglyBoundedScan` | dual | `record-type-key.yamsql` | ported, plan generated | BASIC_HOOK; `where rec_no < 400`, one row (123). Should plan the record-type key plus a one-sided range on the second primary-key column, matching the source's `[IS MySimpleRecord, [LESS_THAN 400]]`; the precedent `record-type-key-tests.yamsql` shows exactly that shape (`SCAN([IS T1, [LESS_THAN ...]])`). |
| `testDoublyBoundedScan` | dual | `record-type-key.yamsql` | ported, plan generated | BASIC_HOOK; `where rec_no > 200 and rec_no < 500`, one row (456). The two one-sided inequalities must coalesce into a single range behind the record-type key. |
| `testIndexScan` | dual | — | not portable | Its hook installs `concat(recordType(), rec_no)` on MySimpleRecord ONLY and leaves MyOtherRecord with the bare `rec_no` primary key. `INTERMINGLE_TABLES` is a schema-template-wide option (`RecordLayerSchemaTemplate.Builder.setIntermingleTables`, consumed by `RecordLayerTable.newBuilder(boolean)` which seeds primaryKeyParts with `recordType()`), so a per-table mix has no DDL: false prefixes both tables (= BASIC_HOOK, i.e. a duplicate of testScan), true prefixes neither. The property under test is also old-planner-only — the Cascades branch of the assertion is byte-identical to testScan's `scanPlan(range("[IS MySimpleRecord]"))`. |
| `testWithExplicitRecordTypeKeyComparison` | dual | — | not portable | The filter is a bare `RecordTypeKeyComparison("MySimpleRecord")` QueryComponent. The record-type key is not a column, so no SQL text produces it; and under Cascades the source asserts that *planning throws* `Comparisons.EvaluationContextRequiredException` (issue #3813), an internal planner exception with no yamsql `error:` surface. |
| `testExplicitKeys` | @Test | — | out of scope | Plain @Test: metadata-builder assertion on getExplicitRecordTypeKey, no query. |
| `testIllegalKey` | @Test | — | out of scope | Plain @Test: expects MetaDataException from setRecordTypeKey with a non-tuple value. |
| `testDuplicateRecordTypeKeys` | @Test | — | out of scope | Plain @Test: two types given the same explicit record type key must fail metadata validation. |
| `testOverlappingRecordTypeKeys` | @Test | — | out of scope | Plain @Test: explicit key colliding with another type's default must fail metadata validation. |
| `testWriteRead` | @Test | — | out of scope | Plain @Test: asserts the literal primary-key Tuples (1,123)/(1,456)/(2,123) and loadRecord round-trips; Tuple-level assertions have no yamsql surface. |
| `testIndexScanOnSecondColumn` | @Test + @Disabled | — | out of scope | @Disabled upstream, and builds a RecordQueryIndexPlan by hand over the universal index `recno-type` = concat(num_value_2, recordType()). |
| `testDoublyBoundedScanWithSort` | @ParameterizedTest | — | out of scope | No @DualPlannerTest, so old planner only (useCascadesPlanner defaults to false). It also `Assumptions.assumeTrue`s away the concat(recordType(), rec_no) sort case, issue #744. |
| `testSortOnSingleRecordType` | @ParameterizedTest + @Disabled | — | out of scope | @Disabled upstream (issue #744) and not @DualPlannerTest. |
| `testSortOnIndexWithComparisonOnSecondColumn` | @ParameterizedTest + @Disabled | — | out of scope | @Disabled upstream, not @DualPlannerTest, and needs the universal index concat(num_value_2, recordType()). |
| `testSingleton` | @Test | — | out of scope | Plain @Test. Also needs `t2.setPrimaryKey(recordType())` — a primary key that is *only* the record-type key, i.e. a zero-column SQL PRIMARY KEY, which the DDL cannot declare. |
| `testDeleteType` | @Test | — | out of scope | Plain @Test: deleteRecordsWhere plus getSnapshotRecordCount / raw scanIndex assertions. |
| `testDeletePartial` | @Test | — | out of scope | Plain @Test: deleteRecordsWhere with a predicate, asserted via raw index-entry Tuples. |
| `testBuildIndexIndexThreshold` | @Test | — | out of scope | Plain @Test: FDBStoreTimer counter assertions around checkVersion-time index rebuild. |
| `testOnlineIndexBuilder` | @Test | — | out of scope | Plain @Test: OnlineIndexer driving, index states and timer counters. |
| `testOnlineIndexMultiTargetBuilder` | @Test | — | out of scope | Plain @Test: multi-target OnlineIndexer over SUM and MAX_EVER_TUPLE indexes. |
| `testOnlineIndexBuilderRecordTypeKeyZero` | @Test | — | out of scope | Plain @Test: explicit record type key 0 plus getTotalRecordsScanned on the OnlineIndexer. |

## `FDBRestrictedIndexQueryTest`

`fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/provider/foundationdb/query/FDBRestrictedIndexQueryTest.java`

11 methods, **5 in scope** (`@DualPlannerTest`); the other 6 are plain `@Test`, so old-planner only. Two
port.

The two that do are the only files in the directory that do **not** use a `schema_template` block. They
cannot: `set schema state` needs the `database_id` and schema `name` in its JSON (`Command.java:189-190`),
and with an implicit template those are framework-generated. So they create the template, database and
schema by hand and connect explicitly — the shape of `disabled-index-tests-proto.yamsql`, with the template
built by inline DDL (`create-drop.yamsql:52`) rather than loaded from a proto.

Only the **first** phase of each ports. Both source methods restrict an index, check the planner avoids it,
then `uncheckedMarkIndexReadable` and check the planner uses it again — deliberately without a rebuild, so
the index is stale. That needs the state changed *mid-test*, and `set schema state` is a setup-block
directive: all seven existing users call it exactly once, before any query. There is no per-query or
mid-file form.

| source method | annotation | new file | state | note |
|---|---|---|---|---|
| `queryWithWriteOnly` | dual | `restricted-index-write-only.yamsql` | ported, plan generated | index marked `WRITE_ONLY` via `set schema state`, so it holds no entries. Verified: the plan scans MY_SIMPLE_RECORD_STR_VALUE_INDEXED unbounded and keeps NUM_VALUE_3_INDEXED as a residual filter — a *different* index — the source's `hasNoDescendant(indexScan(indexName(containsString("num_value_3_indexed"))))`. The row assertion is what guards the property: if the planner used the index the result would be empty. Re-enable phase not portable |
| `queryWithDisabled` | dual | `restricted-index-disabled.yamsql` | ported, plan generated | as above with `DISABLED`. Verified: the plan scans MY_SIMPLE_RECORD_NUM_VALUE_UNIQUE unbounded with STR_VALUE_INDEXED as a residual filter, avoiding the disabled index as the source requires. Re-enable phase not portable |
| `queryAllowedIndexes` | dual | — | not portable | turns on `IndexOptions.NOT_ALLOWED_FOR_QUERY_OPTIONS` (`allowedForQuery=false`) on the index, and its second half then names that index explicitly to force its use. The option has no SQL DDL surface — nothing in `RelationalParser.g4` or `fdb-relational-core` mentions `allowedForQuery`, and `WITH ATTRIBUTES` offers only `LEGACY_EXTREMUM_EVER` |
| `queryAllowedUniversalIndex` | dual | — | not portable | `addUniversalIndex`; a SQL index has exactly one base table (`RecordLayerSchemaTemplate.computeIndexes:285`) |
| `indexQueryabilityFilter` | dual | — | not portable | needs `setIndexQueryabilityFilter(FALSE)`; the SQL path hard-wires `IndexQueryabilityFilter.TRUE` (`QueryPlan.java:649`) |
| `queryAggregateWithWriteOnly` | `@Test` | — | out of scope | old planner only |
| `queryAggregateWithDisabled` | `@Test` | — | out of scope | old planner only |
| `queryAggregateWithFilteredIndex` | `@Test` | — | out of scope | old planner only |
| `snapshotRecordCountForRecordTypeFiltered` | `@Test` | — | out of scope | old planner only |
| `snapshotRecordCountFiltered` | `@Test` | — | out of scope | old planner only |
| `snapshotUpdateCountFiltered` | `@Test` | — | out of scope | old planner only |

## Verification status (2026-09-07)

Every generated plan has been compared against the *Cascades* branch of its source test's
`assertMatchesExactly` / `assertThat(plan, …)`. Plan hashes are out of scope. Of 195 query blocks:

| | blocks |
|---|---|
| congruent with the source assertion | 127 |
| divergent for a systematic, documented reason | ~14 |
| no source plan assertion to compare against | 28 |
| source assertion not mechanically extractable | 5 |
| `@Disabled` pending a product fix | 3 |

The systematic divergences are tabulated in `README.md` under *Verifying a generated plan against the
source assertion* — read that before investigating any apparent mismatch, and note that the source's `//`
plan comment is **not** a valid baseline: tests parameterized on deferred fetch carry two, and the
Cascades assertion is the second.

Two findings came out of the pass. `multi-field-index-selection-prefix-scalar.yamsql` was projecting a
narrow column list where its source sets no `setRequiredResults`, which turned a plain index scan into a
covering one — fixed. `covering-index-header-not-covering.yamsql` cannot test what its source tests,
because proto2 `required` has no SQL equivalent.

Where a source method holds several plan assertions (`sortNested`) or builds its matcher in a helper
(`FDBOrQueryToUnionTest.queryPlanMatcher`), the comparison was done by hand.

## Totals

| state | methods |
|---|---|
| ported, plan generated | 199 |
| ported, plan pending | 3 |
| **ported, FAILING** | 5 |
| covered indirectly | 4 |
| deferred | 12 |
| not portable | 42 |
| out of scope | 78 |
| **total catalogued** | **343** |

## Classes not yet started

**None.** Every class in scope has been ported or ruled out; see *Totals* below and the `deferred` rows
inside the class sections for the methods that remain.

## Classes that stay in JUnit permanently

| class | why |
|---|---|
| `FDBIncarnationQueryTest` | All 4 methods are `@DualPlannerTest(CASCADES)` and none can run. `FDBRecordStore.getIncarnation():3617` and `updateIncarnation():3631` both gate on `FormatVersion.INCARNATION`, while `RecordLayerConfig.RecordLayerConfigBuilder:69` seeds the default format version `CACHEABLE_STATE` (`FormatVersion.java:221`). Nothing on the yamsql path raises it: the one lever that parses a format version is dead code — `RecordLayerSetStoreStateConstantAction.execute()` builds the store without ever calling `setFormatVersion`, so the value `Command.java:186` computes is discarded. A file that appeared to test `COPY … INCREMENT INCARNATION` would silently do nothing, which is worse than not porting it. **Decision: 2026-09-07, not ported.** |
| `FDBNestedRepeatedQueryTest` | Only 11 of its 35 methods are in scope (the rest are plain `@Test`/`@ParameterizedTest`, so old-planner only), and all 11 are aggregate-index tests whose useful half cannot be expressed. Each builds its aggregate twice — once over `OuterRecord`, once over a synthetic record type from `addUnnestedRecordType`, for which there is no DDL — and reads the values through the direct-access `evaluateAggregateFunction`, which has no SQL surface. Worse, the *entries-together* index shape (each map key paired with the value from its own entry) has no SQL spelling at all: the grouping key is built from the GROUP BY values alone (`MaterializedViewIndexGenerator:233`) and the aggregated column is appended with `groupBy` (`:438`), which `GroupingKeyExpression.of:59-69` concatenates — and two fan-outs concatenated is a cross product. So SQL can only build the shape the source itself calls "the kind of index one might accidentally construct". What is left after all that is not worth a yamsql file. **Decision: 2026-09-07, not ported.** |
| `FDBCrossRecordQueryTest` | multi-record-type indexes over a proto union; a SQL index must have exactly one base table |
| `QueryPlanFullySortedTest` | asserts `RecordQueryPlan.isStrictlySorted()`, a plan property with no yamsql directive |
| `FDBCollateJREQueryTest / FDBCollateQueryTestBase` | `collate_jre` is not in the SQL function grammar |
| `TempTableTest / TempTableTestBase` | temp tables are a Cascades primitive; the SQL surface is CREATE TEMPORARY FUNCTION and recursive CTEs |
| `FDBRecordStoreNullQueryTest` | proto2 optional/required null semantics |
| `QueryPlanResultTest` | asserts on the QueryPlanInfo the planner returns |
| `indexes/RankIndexTest` | **Excluded from this task by decision (2026-09-06).** Rank indexes are not yet supported in the Relational layer; `IndexTypes.RANK` has no SQL surface. Revisit only if rank support lands. |
| `plan/plans/FDBComparatorPlanTest, FDBSelectorPlanTest` | those plans are constructed directly and are not producible from SQL |
| `indexes/OutsideValueLikeIndexQueryTest` | registers a custom out-of-tree index maintainer |
| `cascades/TranslateGraphTest` | operates on RelationalExpression graphs directly |

## Result-ordering convention

Ordered `result:` is used only where the row sequence is genuinely determined (unique `ORDER BY` keys,
identical tying rows, at most one row, `maxRows:` paging, or the files where the source asserts a
specific order that the observed plan determines). All other blocks use `unorderedResult:` — 16 blocks
were relaxed on 2026-09-06 after two wrong guesses at tie order showed it is not derivable by
reasoning. Current split: 80 blocks ordered, 79 unordered.

A third source of unassertable order showed up on 2026-09-07 in `nested-field-query-hierarchical.yamsql`:
the order can be determined by the plan and still not be worth asserting, when *which plan wins* is the
thing in question. There `startsWith` → `LIKE` leaves the record scan unbounded, so a covering index scan
wins on cost and the rows arrive in index order rather than primary-key order. The set is right and the
sequence is fully determined — but it flips the moment the cost model changes its mind, and the source's
own ordering is equally an artefact of *its* access path. Assert the set.

`maxRows:` paging is only used where the source query has a sort, since each result directive consumes
one page and page membership therefore has to be predictable. `testComplexLimits3`, `testComplexLimits4`
and `testComplexLimits5` have no sort in the source, so their row limit is not reproduced — they assert
the full result and rely on the ForceContinuations configuration for paging coverage. This is a
deliberate weakening: the source asserts "with a limit of n you get n rows", which yamsql cannot express
without a predictable order.

## Known product divergences found while porting

**Ordering match uses `Object.equals`, so extracted constants never match index literals (2026-09-07).**
`AbstractDataAccessRule.satisfiesRequestedOrdering` compares the requested ordering's values against a
match's `matchedOrderingParts` with plain `equals` — at `:814`, and at `:801` through an `ImmutableSet`
lookup that also depends on `hashCode`. SQL lifts every literal into a `ConstantObjectValue`, while an
index key expression necessarily holds a `LiteralValue`, so `ORDER BY num_value_unique & 4` against an
index over `bitand(num_value_unique, 4)` is structurally unequal. Every match in the partition is then
dropped and `dataAccessForMatchPartition` returns empty (`:319-321`); with no data access and no sort
operator, planning fails outright. `ValueEquivalence.constantEquivalenceWithEvaluationContext`
(`ValueEquivalence.java:346-400`) exists precisely to relate the two and returns a `QueryPlanConstraint` so
the plan cache stays sound — the predicate path uses it via `Value.semanticEquals`, and the ordering check
is the one place that does not. Asserted supported behaviour upstream:
`FDBLongArithmeticFunctionQueryTest.matchConstantMaskValue`. (An earlier note here claimed
`long-arithmetic-functions-unique-mask-4.yamsql` was a second, unexplained symptom. It is not a defect
at all — see that file's row below.) **Filed upstream by the user (2026-09-07); no draft needs filing.** The
local write-up `.out/issue-ordering-match-constant-object-value.md` is kept only as the analysis trail.
This entry subsumes what was previously catalogued separately as "ORDER BY over computed values does not
plan" — that was the same defect seen before the root cause was known.

**`<struct array> = []` fails at execution (2026-09-07).** Comparing an array-of-struct column to an array
literal plans fine and then throws `VerifyException` per row. `PromoteValue.eval` case-analyses the target
type over {simple promotion, enum, record} and `Verify.verify(promoteToType.isRecord())`
(`PromoteValue.java:265`) fails for an *array* target; `isSimplePromotion` (`:224-226`) admits an array only
when its element type is primitive. That is why `arrays-operators.yamsql`'s `INTEGER ARRAY = []` passes
(`:245`) and a struct array does not — no test compares a struct array to a literal. The coercion machinery
already handles array-to-array (`:394-403`, `:472-474`); only the descriptor computation in `eval` was never
extended. An empty table hides it, since the filter is never evaluated. `modification-query.yamsql`
substitutes an unrestricted select for the source's `whereReviewsIsEmptyGraph` predicate, which the source
uses only as "find the rows I just wrote". Draft:
`.out/issue-promotevalue-struct-array-comparison.md`.

**Boolean operands under AND / OR / NOT (2026-09-07).** `where <predicate> and true` fails at planning with
`INTERNAL_ERROR` and a message that is literally `null`. `AndOrValue.toQueryPredicate:218-219` and
`NotValue.toQueryPredicate:85` require both operands to implement the `BooleanValue` marker interface, but the
SQL front end legitimately produces boolean-*typed* non-`BooleanValue` operands: a literal becomes a
`ConstantObjectValue`, a boolean column a `FieldValue`. The relational layer's
`Expression.Utils.toUnderlyingPredicate` ladder (`Expression.java:416-448`) already bridges this, but only for
the top-level WHERE expression — never for a conjunct. Guava's no-arg `Verify.verify` supplies the null
message. `WHERE b AND b`, `WHERE a > 1 OR b` and `WHERE NOT b` fail identically, and no yamsql covers any of
them (`boolean.yamsql` only exercises booleans in the select list). Draft:
`.out/issue-boolean-operand-under-and-or-not.md`.

1. **OR of two overlapping `IN` lists throws** — `in-query-or-overlap.yamsql`. Executing
   `num_value_unique IN (903, 905, 901) OR num_value_unique IN (906, 905, 904)` raises
   `ClassCastException: ByteString$LiteralByteString cannot be cast to Comparable`. The same plan runs
   correctly through the record-layer API: `FDBInQueryTest.testInQueryOrOverlap` asserts exactly five
   records under both planners. The sibling `testInQueryOr` — an `IN` list OR'd with a *range* over the
   same index — plans a union and works, so it is specifically OR-of-two-IN-lists. A ByteString
   reaching a `Comparable` cast suggests a serialized per-leg continuation, or a packed IN-list value,
   being fed to the union's comparison key `(num_value_unique, rec_no)`. Draft issue in
   `.out/issue-in-or-bytestring-cce.md`.
2. **Intersection with an unsound comparison key** — `and-query-to-intersection-two-covering.yamsql`
   returns 2 rows where 10 are correct. The planner judges the two legs compatibly ordered by
   reasoning from all of the query's equality-bound predicates rather than the ones each leg applies.
   Full analysis in that file's header; draft issue in `.out/issue-intersection-comparison-key.md`.
   `FDBAndQueryToIntersectionTest.testComplexQueryAndWithTwoChildren3` asserts the faulty plan but
   never executes it.
3. **`startsWith` is not `LIKE 'x%'`** — the core planner turns `Query.field(x).startsWith("l")`
   into a prefix range scan (`{[l],[l]}`); the SQL `LIKE 'l%'` translation yields an unbounded scan
   plus a `FILTER ... LIKE`. Affects `and-query-to-intersection.yamsql` (x1),
   `and-query-to-intersection-num-value-2.yamsql` (x1), `or-query-to-union-covering.yamsql` (x1) and
   `covering-index-header-compound-pk.yamsql` (x1). Accepted as a divergence, not a defect.

