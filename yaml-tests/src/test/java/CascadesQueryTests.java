/*
 * CascadesQueryTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.apple.foundationdb.relational.yamltests.MaintainYamlTestConfig;
import com.apple.foundationdb.relational.yamltests.YamlTest;
import com.apple.foundationdb.relational.yamltests.YamlTestConfigFilters;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;

/**
 * Query-planning coverage ported from the Cascades tests in {@code fdb-record-layer-core}'s
 * {@code com.apple.foundationdb.record.provider.foundationdb.query} package.
 *
 * <p>
 * Those tests drive the Cascades planner through the {@code RecordQuery} builder API; the SQL layer
 * drives the same planner from SQL text, so most of them can be expressed declaratively here
 * instead. Each yamsql file corresponds to one source test class, and every query carries a comment
 * naming the source test method it came from. Where a source class needs more than one index
 * configuration the file is split, with the variant named in a suffix — the planner has to be forced
 * into a plan by the schema it is given, and {@code USE INDEX} cannot stand in for a missing index
 * because a hinted query drops the primary-scan candidate entirely.
 * </p>
 *
 * <p>
 * See {@code cascades-query-tests/README.md} for the source-class mapping, the authoring
 * conventions, and the list of tests that cannot be ported.
 * </p>
 */
@YamlTest
@MaintainYamlTestConfig(YamlTestConfigFilters.CORRECT_EXPECTATIONS)
class CascadesQueryTests {
    private static final String PREFIX = "cascades-query-tests";

    @TestTemplate
    void andQueryToIntersectionChoiceUnique(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/and-query-to-intersection-choice-unique.yamsql");
    }

    @TestTemplate
    void andQueryToIntersectionChoice(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/and-query-to-intersection-choice.yamsql");
    }

    @TestTemplate
    void andQueryToIntersectionCompoundPk(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/and-query-to-intersection-compound-pk.yamsql");
    }

    @TestTemplate
    void andQueryToIntersectionNumValue2(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/and-query-to-intersection-num-value-2.yamsql");
    }

    @TestTemplate
    @Disabled("Intersection of two covering scans returns 2 rows instead of 10; the planner treats the legs as "
              + "compatibly ordered by reasoning from all the query's equality-bound predicates rather than the ones "
              + "each leg applies. This file holds only that one query, so a per-query supported_version sentinel "
              + "would leave the test block with no executables (TestBlock.parse:409). See the file header.")
    void andQueryToIntersectionTwoCovering(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/and-query-to-intersection-two-covering.yamsql");
    }

    @TestTemplate
    void andQueryToIntersection(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/and-query-to-intersection.yamsql");
    }

    @TestTemplate
    void coveringIndexAdditionalFilter(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-additional-filter.yamsql");
    }

    @TestTemplate
    void coveringIndexConcatenated(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-concatenated.yamsql");
    }

    @TestTemplate
    void coveringIndexHeaderCompoundPk(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-header-compound-pk.yamsql");
    }

    @TestTemplate
    void coveringIndexHeaderConcatenatedValue(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-header-concatenated-value.yamsql");
    }

    @TestTemplate
    void coveringIndexHeaderMulti(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-header-multi.yamsql");
    }

    @TestTemplate
    void coveringIndexHeaderNotCovering(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-header-not-covering.yamsql");
    }

    @TestTemplate
    void coveringIndexHeaderValue(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-header-value.yamsql");
    }

    @TestTemplate
    void coveringIndexMultiValue(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-multi-value.yamsql");
    }

    @TestTemplate
    void coveringIndexMulti(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-multi.yamsql");
    }

    @TestTemplate
    void coveringIndexPrimaryKey(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index-primary-key.yamsql");
    }

    @TestTemplate
    void coveringIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/covering-index.yamsql");
    }

    @TestTemplate
    void filterCoalescingMultiIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/filter-coalescing-multi-index.yamsql");
    }

    @TestTemplate
    void filterCoalescing(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/filter-coalescing.yamsql");
    }

    @TestTemplate
    void inQueryCompoundIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-compound-index.yamsql");
    }

    @TestTemplate
    void inQueryHeader(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-header.yamsql");
    }

    @TestTemplate
    void inQueryMultiIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-multi-index.yamsql");
    }

    @TestTemplate
    void inQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query.yamsql");
    }

    @TestTemplate
    void multiFieldIndexSelectionPrefixScalar(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/multi-field-index-selection-prefix-scalar.yamsql");
    }

    @TestTemplate
    void multiFieldIndexSelectionWiderCovering(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/multi-field-index-selection-wider-covering.yamsql");
    }

    @TestTemplate
    void multiFieldIndexSelection(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/multi-field-index-selection.yamsql");
    }

    @TestTemplate
    void orQueryToUnionCompoundPkOrdered(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-compound-pk-ordered.yamsql");
    }

    @TestTemplate
    void orQueryToUnionCompoundPk(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-compound-pk.yamsql");
    }

    @TestTemplate
    void orQueryToUnionCovering(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-covering.yamsql");
    }

    @TestTemplate
    void orQueryToUnionNoIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-no-index.yamsql");
    }

    @TestTemplate
    void orQueryToUnionOrderedAnd(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-ordered-and.yamsql");
    }

    @TestTemplate
    void orQueryToUnionOrdered(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-ordered.yamsql");
    }

    @TestTemplate
    void orQueryToUnionProtoOnly(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-proto-only.yamsql");
    }

    @TestTemplate
    void orQueryToUnionUnorderableAnd(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-unorderable-and.yamsql");
    }

    @TestTemplate
    void orQueryToUnion(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union.yamsql");
    }

    @TestTemplate
    void orderingQueryTwoIndexes(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/ordering-query-two-indexes.yamsql");
    }

    @TestTemplate
    void orderingQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/ordering-query.yamsql");
    }

    @TestTemplate
    void repeatedFieldQueryOnlyIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/repeated-field-query-only-index.yamsql");
    }

    @TestTemplate
    void repeatedFieldQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/repeated-field-query.yamsql");
    }

    @TestTemplate
    void returnedRecordLimit(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/returned-record-limit.yamsql");
    }

    @TestTemplate
    void sortIndexSelectionNested(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sort-index-selection-nested.yamsql");
    }

    @TestTemplate
    void sortIndexSelectionPrimaryKey(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sort-index-selection-primary-key.yamsql");
    }

    @TestTemplate
    void sortIndexSelectionUnique(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sort-index-selection-unique.yamsql");
    }

    @TestTemplate
    void sortIndexSelection(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sort-index-selection.yamsql");
    }

    @TestTemplate
    void sparseIndexImplied(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sparse-index-implied.yamsql");
    }

    @TestTemplate
    void sparseIndexNotImplied(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sparse-index-not-implied.yamsql");
    }

    @TestTemplate
    void sparseIndexOrPredicate(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/sparse-index-or-predicate.yamsql");
    }

    @TestTemplate
    void recordStoreRepeatedQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-store-repeated-query.yamsql");
    }

    @TestTemplate
    void inQueryOrCompound(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-or-compound.yamsql");
    }

    @TestTemplate
    void inQueryOr(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-or.yamsql");
    }

    @TestTemplate
    void inQueryUnionIntersectionNoNv3(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-union-intersection-no-nv3.yamsql");
    }

    @TestTemplate
    void inQueryUnionIntersection(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-union-intersection.yamsql");
    }

    @TestTemplate
    void inQueryUnionSargable(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-union-sargable.yamsql");
    }

    @TestTemplate
    void orQueryToUnionKeyWithValue(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-key-with-value.yamsql");
    }

    @TestTemplate
    void orQueryToUnionSplitContinuations(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-split-continuations.yamsql");
    }

    @TestTemplate
    void orQueryToUnionStr3Index(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-str-3-index.yamsql");
    }

    @TestTemplate
    @Disabled("OR of two overlapping IN lists raises ClassCastException: ByteString cannot be cast to Comparable. "
              + "This file holds only that one query, so a per-query supported_version sentinel would leave the test "
              + "block with no executables (TestBlock.parse:409). See the file header.")
    void inQueryOrOverlap(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-or-overlap.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryComplex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-complex.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryConcatNested(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-concat-nested.yamsql");
    }

    @TestTemplate
    @Disabled("EXISTS over an unnested array returns the outer row once per matching element: ev1, ev3, ev3 where "
              + "the answer is ev1, ev3. This file holds only that one query, so a per-query supported_version "
              + "sentinel would leave the test block with no executables (TestBlock.parse:409). See the file header.")
    void nestedFieldQueryDoublyNested(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-doubly-nested.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryHierarchical(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-hierarchical.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryNestedMap(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-nested-map.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryNestedPk(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-nested-pk.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryReviewerBetween(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-reviewer-between.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryReviewerEmailHometown(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-reviewer-email-hometown.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryReviewerHometownEmail(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-reviewer-hometown-email.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryReviewerSchoolConcat(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-reviewer-school-concat.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryReviewerSortedInequality(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-reviewer-sorted-inequality.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryReviewerSorted(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-reviewer-sorted.yamsql");
    }

    @TestTemplate
    void nestedFieldQueryReviewer(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query-reviewer.yamsql");
    }

    @TestTemplate
    void nestedFieldQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/nested-field-query.yamsql");
    }

    @TestTemplate
    void groupByQueryAggregateIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/group-by-query-aggregate-index.yamsql");
    }

    @TestTemplate
    void groupByQueryBothIndexes(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/group-by-query-both-indexes.yamsql");
    }

    @TestTemplate
    void groupByQueryNoIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/group-by-query-no-index.yamsql");
    }

    @TestTemplate
    void groupByQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/group-by-query.yamsql");
    }

    @TestTemplate
    void permutedMinMaxNumValue2(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max-num-value-2.yamsql");
    }

    @TestTemplate
    void permutedMinMaxOrderingKeys(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max-ordering-keys.yamsql");
    }

    @TestTemplate
    void permutedMinMaxRepeater(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max-repeater.yamsql");
    }

    @TestTemplate
    void permutedMinMaxStrValue(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max-str-value.yamsql");
    }

    @TestTemplate
    void permutedMinMax(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/permuted-min-max.yamsql");
    }

    @TestTemplate
    void simpleQueryGraphHints(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/simple-query-graph-hints.yamsql");
    }

    @TestTemplate
    void simpleQueryGraphJoins(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/simple-query-graph-joins.yamsql");
    }

    @TestTemplate
    void simpleQueryGraphNameTagIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/simple-query-graph-name-tag-index.yamsql");
    }

    @TestTemplate
    void simpleQueryGraphTagIndex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/simple-query-graph-tag-index.yamsql");
    }

    @TestTemplate
    void simpleQueryGraph(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/simple-query-graph.yamsql");
    }

    @TestTemplate
    void longArithmeticFunctionsBinary(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/long-arithmetic-functions-binary.yamsql");
    }

    @TestTemplate
    void longArithmeticFunctionsComplex(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/long-arithmetic-functions-complex.yamsql");
    }

    @TestTemplate
    void longArithmeticFunctionsMask1(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/long-arithmetic-functions-mask-1.yamsql");
    }

    @TestTemplate
    void longArithmeticFunctionsMask2(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/long-arithmetic-functions-mask-2.yamsql");
    }

    @TestTemplate
    void longArithmeticFunctionsTwoColumn(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/long-arithmetic-functions-two-column.yamsql");
    }

    @TestTemplate
    void longArithmeticFunctionsUniqueMask4(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/long-arithmetic-functions-unique-mask-4.yamsql");
    }

    @TestTemplate
    void longArithmeticFunctions(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/long-arithmetic-functions.yamsql");
    }

    @TestTemplate
    void modificationQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/modification-query.yamsql");
    }

    @TestTemplate
    void recordStoreQueryContinuation(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-store-query-continuation.yamsql");
    }

    @TestTemplate
    void recordStoreQueryEvenOdd(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-store-query-even-odd.yamsql");
    }

    @TestTemplate
    void recordStoreQueryExcludeNull(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-store-query-exclude-null.yamsql");
    }

    @TestTemplate
    void recordStoreQueryNull(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-store-query-null.yamsql");
    }

    @TestTemplate
    void recordStoreQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-store-query.yamsql");
    }

    @TestTemplate
    void recordTypeKey(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-type-key.yamsql");
    }

    @TestTemplate
    void recursiveQueriesForest(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/recursive-queries-forest.yamsql");
    }

    @TestTemplate
    void recursiveQueriesMultiples(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/recursive-queries-multiples.yamsql");
    }

    @TestTemplate
    void recursiveQueries(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/recursive-queries.yamsql");
    }

    @TestTemplate
    void versionsQueryLongRecord(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/versions-query-long-record.yamsql");
    }

    @TestTemplate
    void versionsQueryOtherRecord(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/versions-query-other-record.yamsql");
    }

    @TestTemplate
    void versionsQuery(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/versions-query.yamsql");
    }

    @TestTemplate
    void restrictedIndexDisabled(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/restricted-index-disabled.yamsql");
    }

    @TestTemplate
    void restrictedIndexWriteOnly(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/restricted-index-write-only.yamsql");
    }

    @TestTemplate
    void orQueryToUnionNestedPredicates(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-nested-predicates.yamsql");
    }

    @TestTemplate
    void repeatedFieldQueryPrefixRepeated(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/repeated-field-query-prefix-repeated.yamsql");
    }

    @TestTemplate
    void repeatedFieldQueryPrefixRepeatedNested(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/repeated-field-query-prefix-repeated-nested.yamsql");
    }

    @TestTemplate
    void recordStoreRepeatedQuerySumByRepeated(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/record-store-repeated-query-sum-by-repeated.yamsql");
    }

    @TestTemplate
    void returnedRecordLimitNoFilter(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/returned-record-limit-no-filter.yamsql");
    }

    @TestTemplate
    void inQuerySortedExtraUnorderedColumns(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-sorted-extra-unordered-columns.yamsql");
    }

    @TestTemplate
    void orQueryToUnionExtraRepeatedColumn(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/or-query-to-union-extra-repeated-column.yamsql");
    }

    @TestTemplate
    void inQueryConstantValue(YamlTest.Runner runner) throws Exception {
        runner.runYamsql(PREFIX + "/in-query-constant-value.yamsql");
    }
}
