/*
 * RecursiveQueriesTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestHierarchiesProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.UnableToPlanException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableInsertExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveUnionPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Stopwatch;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression.Traversal.ANY;
import static com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression.Traversal.LEVEL;
import static com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression.Traversal.PREORDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test suite for {@link RecordQueryRecursiveUnionPlan} planning and execution.
 */
class RecursiveQueriesTest extends TempTableTestBase {

    static Stream<Arguments> multiplesOfSuccessParameters() {
        return Stream.of(
            Arguments.of(List.of(2L, 5L), 50L, LEVEL, List.of(2L, 5L, 4L, 10L, 8L, 20L, 16L, 40L, 32L)),
            Arguments.of(List.of(2L, 5L), 50L, ANY, List.of(2L, 5L, 4L, 10L, 8L, 20L, 16L, 40L, 32L)),
            Arguments.of(List.of(2L, 5L), 6L, LEVEL, List.of(2L, 5L, 4L)),
            Arguments.of(List.of(2L, 5L), 6L, ANY, List.of(2L, 5L, 4L)),
            Arguments.of(List.of(2L, 5L), 0L, LEVEL, List.of(2L, 5L)),
            Arguments.of(List.of(2L, 5L), 0L, ANY, List.of(2L, 5L)),
            Arguments.of(List.of(), 1000L, LEVEL, List.of()),
            Arguments.of(List.of(), 1000L, ANY, List.of())
        );
    }


    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "recursive union with initial {0}, limit {1}, and {2} traversal produces {3}")
    @MethodSource("multiplesOfSuccessParameters")
    void multiplesOfTest(List<Long> initial, long limit, RecursiveUnionExpression.Traversal traversal, List<Long> expectedResult) throws Exception {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        var result = multiplesOf(initial, limit, traversal);
        assertEquals(expectedResult, result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void multiplesOfTestFailsWithPreorderTraversal() {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        // PREORDER traversal is expected to fail because the constructed plan for calculating
        // multiplesOf is currently not matchable for the DFS implementation. We can implement
        // this if necessary, but for now this is not needed.
        assertThrows(UnableToPlanException.class, () -> multiplesOf(List.of(2L, 5L), 50L, PREORDER));
    }

    /**
     * Sample hierarchy, visually looking like the following.
     * <pre>
     * {@code
     *                    1
     *                 ┌─────┐
     *                ┌┘     └┐
     *               10       20
     *            ┌──┼──┐   ┌──┼──┐
     *           40  50  70 100  210
     *               │
     *              250
     * }
     * </pre>
     * @return a sample hierarchy represented by a list of {@code child -> parent} edges.
     */
    @Nonnull
    private static Map<Long, Long> sampleHierarchy() {
        return ImmutableMap.of(
                1L, -1L,
                10L, 1L,
                20L, 1L,
                40L, 10L,
                50L, 10L,
                70L, 10L,
                100L, 20L,
                210L, 20L,
                250L, 50L
         );
    }

    /**
     * Sample forest, visually looking like the following.
     * <pre>
     * {@code
     *               1                    500
     *            ┌─────┐              ┌─────┐
     *           ┌┘     └┐            ┌┘     └┐
     *          10       20          510      520
     *       ┌──┼──┐   ┌──┼──┐        │
     *      40  50  70 100  210      550
     *          │
     *         250
     * }
     * </pre>
     * @return a sample forest represented by a list of {@code child -> parent} edges.
     */
    @Nonnull
    private static Map<Long, Long> sampleForest() {
        return ImmutableMap.<Long, Long>builder()
                .put(1L, -1L)
                .put(10L, 1L)
                .put(20L, 1L)
                .put(40L, 10L)
                .put(50L, 10L)
                .put(70L, 10L)
                .put(100L, 20L)
                .put(210L, 20L)
                .put(250L, 50L)
                .put(500L, -1L)
                .put(510L, 500L)
                .put(520L, 500L)
                .put(550L, 510L)
                .build();
    }

    static Stream<Arguments> ancestorsOfNodeParameters() {
        return Stream.of(
            Arguments.of(ImmutableMap.of(250L, 50L), LEVEL, List.of(250L, 50L, 10L, 1L)),
            Arguments.of(ImmutableMap.of(250L, 50L), PREORDER, List.of(250L, 50L, 10L, 1L)),
            Arguments.of(ImmutableMap.of(250L, 50L), ANY, List.of(250L, 50L, 10L, 1L)),
            Arguments.of(ImmutableMap.of(250L, 50L, 40L, 10L), LEVEL, List.of(250L, 40L, 50L, 10L, 10L, 1L, 1L)),
            Arguments.of(ImmutableMap.of(250L, 50L, 40L, 10L), PREORDER, List.of(250L, 50L, 10L, 1L, 40L, 10L, 1L)),
            Arguments.of(ImmutableMap.of(250L, 50L, 40L, 10L), ANY, List.of(250L, 50L, 10L, 1L, 40L, 10L, 1L)),
            Arguments.of(ImmutableMap.of(300L, 300L), LEVEL, List.of(300L)),
            Arguments.of(ImmutableMap.of(300L, 300L), PREORDER, List.of(300L)),
            Arguments.of(ImmutableMap.of(300L, 300L), ANY, List.of(300L))
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "ancestors of {0} with {1} traversal are {2}")
    @MethodSource("ancestorsOfNodeParameters")
    void ancestorsOfNodeTest(Map<Long, Long> initial, RecursiveUnionExpression.Traversal traversal, List<Long> expectedResult) {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        var result = ancestorsOf(sampleHierarchy(), initial, traversal);
        assertEquals(expectedResult, result);
    }

    static Stream<Arguments> descendantsOfNodeParameters() {
        return Stream.of(
            Arguments.of(ImmutableMap.of(1L, -1L), LEVEL, List.of(1L, 10L, 20L, 40L, 50L, 70L, 100L, 210L, 250L)),
            Arguments.of(ImmutableMap.of(1L, -1L), PREORDER, List.of(1L, 10L, 40L, 50L, 250L, 70L, 20L, 100L, 210L)),
            Arguments.of(ImmutableMap.of(1L, -1L), ANY, List.of(1L, 10L, 40L, 50L, 250L, 70L, 20L, 100L, 210L)),
            Arguments.of(ImmutableMap.of(10L, 1L), LEVEL, List.of(10L, 40L, 50L, 70L, 250L)),
            Arguments.of(ImmutableMap.of(10L, 1L), PREORDER, List.of(10L, 40L, 50L, 250L, 70L)),
            Arguments.of(ImmutableMap.of(10L, 1L), ANY, List.of(10L, 40L, 50L, 250L, 70L))
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "descendants of {0} with {1} traversal are {2}")
    @MethodSource("descendantsOfNodeParameters")
    void descendantsOfNodeTest(Map<Long, Long> initial, RecursiveUnionExpression.Traversal traversal, List<Long> expectedResult) {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        var result = descendantsOf(sampleHierarchy(), initial, traversal);
        assertEquals(expectedResult, result);
    }

    static Stream<Arguments> ancestorsOfNodeParametersAcrossContinuationParameters() {
        return Stream.of(
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, 2, 1), LEVEL, List.of(List.of(250L), List.of(50L, 10L), List.of(1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, 2, 1), PREORDER, List.of(List.of(250L), List.of(50L, 10L), List.of(1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, 2, 1), ANY, List.of(List.of(250L), List.of(50L, 10L), List.of(1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, 1, 2), LEVEL, List.of(List.of(250L), List.of(50L), List.of(10L, 1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, 1, 2), PREORDER, List.of(List.of(250L), List.of(50L), List.of(10L, 1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, 1, 2), ANY, List.of(List.of(250L), List.of(50L), List.of(10L, 1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, -1), LEVEL, List.of(List.of(250L), List.of(50L, 10L, 1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, -1), PREORDER, List.of(List.of(250L), List.of(50L, 10L, 1L))),
            Arguments.of(ImmutableMap.of(250L, 50L), List.of(1, -1), ANY, List.of(List.of(250L), List.of(50L, 10L, 1L)))
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "ancestors of nodes {0} with row limits {1} and {2} traversal are {3}")
    @MethodSource("ancestorsOfNodeParametersAcrossContinuationParameters")
    void ancestorsOfNodeAcrossContinuation(Map<Long, Long> initial, List<Integer> successiveRowLimits,
                                           RecursiveUnionExpression.Traversal traversal, List<List<Long>> expectedResult) {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        var result = ancestorsOfAcrossContinuations(sampleHierarchy(), initial, successiveRowLimits, false, traversal);
        assertEquals(expectedResult, result);
    }

    static Stream<Arguments> descendantsOfNodeAcrossContinuationParameters() {
        return Stream.of(
            Arguments.of(sampleHierarchy(), ImmutableMap.of(1L, -1L), List.of(1, -1), LEVEL, List.of(List.of(1L), List.of(10L, 20L, 40L, 50L, 70L, 100L, 210L, 250L))),
            Arguments.of(sampleHierarchy(), ImmutableMap.of(1L, -1L), List.of(1, -1), PREORDER, List.of(List.of(1L), List.of(10L, 40L, 50L, 250L, 70L, 20L, 100L, 210L))),
            Arguments.of(sampleHierarchy(), ImmutableMap.of(1L, -1L), List.of(1, -1), ANY, List.of(List.of(1L), List.of(10L, 40L, 50L, 250L, 70L, 20L, 100L, 210L))),
            Arguments.of(sampleHierarchy(), ImmutableMap.of(1L, -1L), List.of(1, 2, 4, -1), LEVEL, List.of(List.of(1L), List.of(10L, 20L), List.of(40L, 50L, 70L, 100L), List.of(210L, 250L))),
            Arguments.of(sampleHierarchy(), ImmutableMap.of(1L, -1L), List.of(1, 2, 4, -1), PREORDER, List.of(List.of(1L), List.of(10L, 40L), List.of(50L, 250L, 70L, 20L), List.of(100L, 210L))),
            Arguments.of(sampleHierarchy(), ImmutableMap.of(1L, -1L), List.of(1, 2, 4, -1), ANY, List.of(List.of(1L), List.of(10L, 40L), List.of(50L, 250L, 70L, 20L), List.of(100L, 210L))),
            Arguments.of(sampleForest(), ImmutableMap.of(1L, -1L, 500L, -1L), List.of(1, 2, 4, -1), LEVEL, List.of(List.of(1L), List.of(500L, 10L), List.of(20L, 510L, 520L, 40L), List.of(50L, 70L, 100L, 210L, 550L, 250L))),
            Arguments.of(sampleForest(), ImmutableMap.of(1L, -1L, 500L, -1L), List.of(1, 2, 4, -1), PREORDER, List.of(List.of(1L), List.of(10L, 40L), List.of(50L, 250L, 70L, 20L), List.of(100L, 210L, 500L, 510L, 550L, 520L))),
            Arguments.of(sampleForest(), ImmutableMap.of(1L, -1L, 500L, -1L), List.of(1, 2, 4, -1), ANY, List.of(List.of(1L), List.of(10L, 40L), List.of(50L, 250L, 70L, 20L), List.of(100L, 210L, 500L, 510L, 550L, 520L)))
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "descendants of nodes {1} with row limits {2} and {3} traversal in hierarchy are {4}")
    @MethodSource("descendantsOfNodeAcrossContinuationParameters")
    void descendantsOfNodeAcrossContinuations(Map<Long, Long> hierarchy, Map<Long, Long> initial, List<Integer> successiveRowLimits,
                                              RecursiveUnionExpression.Traversal traversal, List<List<Long>> expectedResult) {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        var result = descendantsOfAcrossContinuations(hierarchy, initial, successiveRowLimits, false, traversal);
        assertEquals(expectedResult, result);
    }

    @Nonnull
    static Stream<Arguments> randomizedDescendantsTestParameters() {
        final int maxChildrenCountPerLevel = 1000;
        final int maxDepth = 10;
        final var randomHierarchy = Hierarchy.generateRandomHierarchy(maxChildrenCountPerLevel, maxDepth);
        final var splits = ListPartitioner.getSplitsUsingNormalDistribution(10, randomHierarchy.size());
        return Stream.of(
            Arguments.of(randomHierarchy, LEVEL, splits),
            Arguments.of(randomHierarchy, PREORDER, splits),
            Arguments.of(randomHierarchy, ANY, splits)
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "randomized descendants test with {1} traversal")
    @MethodSource("randomizedDescendantsTestParameters")
    void randomizedDescendantsTest(Hierarchy randomHierarchy, RecursiveUnionExpression.Traversal traversal,
                                   List<Integer> splits) {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        final var descendants = randomHierarchy.calculateDescendants(traversal);
        final var randomContinuationScenario = ListPartitioner.randomPartition(descendants, splits);
        final var continuationSnapshots = randomContinuationScenario.getKey();
        final var expectedResults = randomContinuationScenario.getValue();
        var result = descendantsOfAcrossContinuations(randomHierarchy.getEdges(), ImmutableMap.of(1L, -1L), continuationSnapshots, false, traversal);
        assertEquals(expectedResults, result);
    }

    @Nonnull
    static Stream<Arguments> randomizedAncestorsTestParameters() {
        final int maxChildrenCountPerLevel = 100;
        final int maxDepth = 100;
        final var randomHierarchy = Hierarchy.generateRandomHierarchy(maxChildrenCountPerLevel, maxDepth);
        final var leaf = randomHierarchy.getRandomLeaf();
        final var parent = randomHierarchy.getEdges().get(leaf);
        final var ancestors = randomHierarchy.calculateAncestors(leaf);
        final var splits = ListPartitioner.getSplitsUsingNormalDistribution(10, randomHierarchy.size());
        return Stream.of(
            Arguments.of(randomHierarchy, leaf, parent, ancestors, splits, LEVEL),
            Arguments.of(randomHierarchy, leaf, parent, ancestors, splits, PREORDER),
            Arguments.of(randomHierarchy, leaf, parent, ancestors, splits, ANY)
        );
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    @ParameterizedTest(name = "randomized ancestors test with {5} traversal")
    @MethodSource("randomizedAncestorsTestParameters")
    void randomizedAncestorsTest(Hierarchy randomHierarchy, Long leaf, Long parent, List<Long> ancestors,
                                List<Integer> splits, RecursiveUnionExpression.Traversal traversal) {
        Assumptions.assumeTrue(isUseCascadesPlanner());
        final var randomContinuationScenario = ListPartitioner.randomPartition(ancestors, splits);
        final var continuationSnapshots = randomContinuationScenario.getKey();
        final var expectedResults = randomContinuationScenario.getValue();
        var result = ancestorsOfAcrossContinuations(randomHierarchy.getEdges(), ImmutableMap.of(leaf, parent), continuationSnapshots, false, traversal);
        assertEquals(expectedResults, result);
    }

    /**
     * Creates and executes a recursive union plan that calculates multiple series recursively {@code F(X) = F(X-1) * 2}
     * up until a given limit.
     *
     * <p>For example, given initial values [2, 5] and limit 50:
     * <ul>
     *   <li>Start with: [2, 5]</li>
     *   <li>First iteration: 2*2=4, 5*2=10 → add [4, 10] if < 50</li>
     *   <li>Second iteration: 4*2=8, 10*2=20 → add [8, 20] if < 50</li>
     *   <li>Third iteration: 8*2=16, 20*2=40 → add [16, 40] if < 50</li>
     *   <li>Fourth iteration: 16*2=32, 40*2=80 → add [32] (80 >= 50, so excluded)</li>
     *   <li>Result: [2, 5, 4, 10, 8, 20, 16, 40, 32]</li>
     * </ul>
     *
     * @param initial The initial elements in the series, used to seed the recursion.
     * @param limit The (exclusive) limit of the series.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     * @return A series of by-two multiples starting with {@code initial} items up until the given {@code limit}. The Note
     * that the initial items are still included in the final result even if they violate the limit.
     * @throws Exception If the execution of the recursive union plan fails.
     */
    @Nonnull
    private List<Long> multiplesOf(@Nonnull final List<Long> initial, long limit,
                                   @Nonnull final RecursiveUnionExpression.Traversal traversal) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final var seedingTempTable = tempTableInstance();
            final var seedingTempTableAlias = CorrelationIdentifier.of("Seeding");
            initial.forEach(value -> seedingTempTable.add(queryResult(value)));

            final var ttScanBla = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(seedingTempTableAlias, getHierarchyType())));
            var selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(List.of(getIdCol(ttScanBla))).addQuantifier(ttScanBla)
                    .build().buildSelect();
            final var seedingSelectQun = Quantifier.forEach(Reference.initialOf(selectExpression));
            final var insertTempTableAlias = CorrelationIdentifier.of("Insert");
            final var initInsertQun = Quantifier.forEach(Reference.initialOf(TempTableInsertExpression.ofCorrelated(seedingSelectQun,
                    insertTempTableAlias, getInnerType(seedingSelectQun))));
            final var scanTempTableAlias = CorrelationIdentifier.of("Scan");

            final var ttScanRecuQun = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(scanTempTableAlias, getHierarchyType())));
            var idField = getIdCol(ttScanRecuQun);
            final var multByTwo = Column.of(Optional.of("id"), (Value)new ArithmeticValue.MulFn().encapsulate(List.of(idField.getValue(), LiteralValue.ofScalar(2L))));
            selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(List.of(multByTwo))
                    .addQuantifier(ttScanRecuQun)
                    .build().buildSelect();
            final var selectQun = Quantifier.forEach(Reference.initialOf(selectExpression));
            idField = getIdCol(selectQun);
            final var lessThanForty = new ValuePredicate(idField.getValue(), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, limit));
            selectExpression = GraphExpansion.builder()
                    .addPredicate(lessThanForty)
                    .addQuantifier(selectQun)
                    .build().buildSimpleSelectOverQuantifier(selectQun);
            final var recuSelectQun = Quantifier.forEach(Reference.initialOf(selectExpression));

            final var recuInsertQun = Quantifier.forEach(Reference.initialOf(TempTableInsertExpression.ofCorrelated(recuSelectQun, insertTempTableAlias, getInnerType(recuSelectQun))));

            final var recursiveUnionPlan = new RecursiveUnionExpression(initInsertQun, recuInsertQun, scanTempTableAlias, insertTempTableAlias, traversal);

            final var logicalPlan = Reference.initialOf(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.initialOf(recursiveUnionPlan))));
            final var cascadesPlanner = (CascadesPlanner)planner;
            final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();

            var evaluationContext = putTempTableInContext(seedingTempTableAlias, seedingTempTable, null);
            return extractResultsAsIds(context, plan, evaluationContext).stream().collect(ImmutableList.toImmutableList());
        }
    }

    /**
     * Creates and executes a recursive union plan that calculates the ancestors of a node (or multiple nodes) in a
     * given hierarchy modelled with an adjacency list.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s ancestors are to be calculated.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     * @return A list of nodes representing the path from the given path from child(ren) to the parent.
     */
    @Nonnull
    private List<Long> ancestorsOf(@Nonnull final Map<Long, Long> hierarchy, @Nonnull final Map<Long, Long> initial,
                                   @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        return hierarchicalQuery(hierarchy, initial, (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(hierarchyScanQun);
            final var parentField = getParentField(ttSelectQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        }, traversal);
    }

    /**
     * Creates a recursive union plan that calculates the descendants of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s descendants are to be calculated.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     * @return A list of nodes representing the path from the given parent(s) to the children.
     */
    @Nonnull
    private List<Long> descendantsOf(@Nonnull final Map<Long, Long> hierarchy, @Nonnull final Map<Long, Long> initial,
                                     @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        return hierarchicalQuery(hierarchy, initial, (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(ttSelectQun);
            final var parentField = getParentField(hierarchyScanQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        }, traversal);
    }

    /**
     * Creates a recursive union plan that calculates the ancestors of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list given a list of execution resumptions by means of result offsets.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s ancestors are to be calculated.
     * @param successiveRowLimits execution resumption points by means of result offsets.\
     * @param reportExecutionTime if {@code true}, outputs the execution time to standard out.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     *
     * @return A chunked list of nodes representing the path from the given path from child(ren) to the parent across
     * the given list of execution resumptions.
     */
    @Nonnull
    private List<List<Long>> ancestorsOfAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                            @Nonnull final Map<Long, Long> initial,
                                                            @Nonnull final List<Integer> successiveRowLimits,
                                                            final boolean reportExecutionTime,
                                                            @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate = (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(hierarchyScanQun);
            final var parentField = getParentField(ttSelectQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        };
        return hierarchyQueryAcrossContinuations(hierarchy, initial, predicate, successiveRowLimits, reportExecutionTime, traversal);
    }

    /**
     * Creates a recursive union plan that calculates the descendants of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list given a list of execution resumptions by means of result offsets.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s descendants are to be calculated.
     * @param successiveRowLimits execution resumption points by means of result offsets.
     * @param reportExecutionTime if {@code true}, outputs the execution time to standard out.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     *
     * @return A chunked list of nodes representing the path from the given parent(s) to the children across
     * the given list of execution resumptions.
     */
    @Nonnull
    private List<List<Long>> descendantsOfAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                              @Nonnull final Map<Long, Long> initial,
                                                              @Nonnull final List<Integer> successiveRowLimits,
                                                              final boolean reportExecutionTime,
                                                              @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate = (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(ttSelectQun);
            final var parentField = getParentField(hierarchyScanQun);
            return new ValuePredicate(parentField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, idField));
        };
        return hierarchyQueryAcrossContinuations(hierarchy, initial, predicate, successiveRowLimits, reportExecutionTime, traversal);
    }

    /**
     * Creates a recursive union plan that calculates the ancestors of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list given a list of execution resumptions by means of result offsets.
     *
     * @param hierarchy the hierarchy represented as a list of {@code child -> parent} edges.
     * @param initial the initial state passed to the recursive union.
     * @param predicate a predicate that is put on the recursive state determining the fix point.
     * @param successiveRowLimits execution resumption points by means of result offsets.
     * @param reportExecutionTime if {@code True}, outputs the execution time to standard out.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     * @return A chunked list of nodes representing the execution results of the recursive query.
     */
    @Nonnull
    private List<List<Long>> hierarchyQueryAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                               @Nonnull final Map<Long, Long> initial,
                                                               @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate,
                                                               @Nonnull final List<Integer> successiveRowLimits,
                                                               final boolean reportExecutionTime,
                                                               @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        final ImmutableList.Builder<List<Long>> resultBuilder = ImmutableList.builder();
        try (FDBRecordContext context = openContext()) {
            var executionTimesMs =  ImmutableList.<Long>builder();
            var continuations = ImmutableList.<byte[]>builder();
            var executionResult = hierarchicalQuery(hierarchy, initial, predicate, context, null, successiveRowLimits.get(0), traversal);
            executionTimesMs.add(executionResult.getExecutionTimeMillis());
            continuations.add(new byte[]{});
            final var plan = executionResult.getPlan();
            var continuation = executionResult.getContinuation();
            resultBuilder.add(executionResult.getExecutionResult());
            final var seedingTempTableAlias = CorrelationIdentifier.of("Seeding");

            for (final var rowLimit : successiveRowLimits.stream().skip(1).collect(ImmutableList.toImmutableList())) {
                final var seedingTempTable = tempTableInstance();
                initial.forEach((id, parent) -> seedingTempTable.add(queryResult(id, parent)));
                var evaluationContext = setUpPlanContext(plan, seedingTempTableAlias, seedingTempTable);
                timer.reset();
                executionResult = executeHierarchyPlan(plan, continuation, evaluationContext, rowLimit);
                executionTimesMs.add(executionResult.getExecutionTimeMillis());
                continuations.add(continuation == null ? new byte[]{} : continuation);
                continuation = executionResult.getContinuation();
                resultBuilder.add(Objects.requireNonNull(executionResult.getExecutionResult()));
            }

            if (reportExecutionTime) {
                reportExecutionTimes(executionTimesMs.build(), continuations.build());
            }
        }
        return resultBuilder.build();
    }

    /**
     * Creates and executes a hierarchical query with the specified traversal order.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial The initial state passed to the recursive union.
     * @param queryPredicate A predicate that determines the fix point.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     * @return A list of nodes representing the execution results of the recursive query.
     */
    @Nonnull
    private List<Long> hierarchicalQuery(@Nonnull final Map<Long, Long> hierarchy,
                                         @Nonnull final Map<Long, Long> initial,
                                         @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate,
                                         @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        try (FDBRecordContext context = openContext()) {
            return hierarchicalQuery(hierarchy, initial, queryPredicate, context, null, -1, traversal).getExecutionResult();
        }
    }

    /**
     * Creates and executes a hierarchical query with full control over execution parameters.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial The initial state passed to the recursive union.
     * @param queryPredicate A predicate that determines the fix point.
     * @param context The FDB record context to use.
     * @param continuation An optional continuation from a previous execution.
     * @param numberOfItemsToReturn The number of items to return, or -1 for all items.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     * @return The execution result including the plan, results, and continuation.
     */
    @Nonnull
    private HierarchyExecutionResult hierarchicalQuery(@Nonnull final Map<Long, Long> hierarchy,
                                                       @Nonnull final Map<Long, Long> initial,
                                                       @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate,
                                                       @Nonnull final FDBRecordContext context,
                                                       @Nullable final byte[] continuation,
                                                       int numberOfItemsToReturn,
                                                       @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestHierarchiesProto.getDescriptor());
        builder.getRecordType("SimpleHierarchicalRecord").setPrimaryKey(field("id"));
        builder.addIndex("SimpleHierarchicalRecord", "parentIdIdx", concat(field("parent"), field("id")));
        builder.addIndex("SimpleHierarchicalRecord", "idParentIdx", concat(field("id"), field("parent")));
        RecordMetaData metaData = builder.getRecordMetaData();
        createOrOpenRecordStore(context, metaData);
        for (final var entry : hierarchy.entrySet()) {
            final var message = item(entry.getKey(), entry.getValue());
            recordStore.saveRecord(message);
        }
        final var seedingTempTableAlias = CorrelationIdentifier.of("Seeding");
        final var insertTempTableAlias = CorrelationIdentifier.of("Insert");
        final var scanTempTableAlias = CorrelationIdentifier.of("Scan");

        final var plan = createAndOptimizeHierarchyQuery(seedingTempTableAlias, insertTempTableAlias, scanTempTableAlias, queryPredicate, traversal);

        final var seedingTempTable = tempTableInstance();

        initial.forEach((id, parent) -> seedingTempTable.add(queryResult(id, parent)));
        var evaluationContext = setUpPlanContext(plan, seedingTempTableAlias, seedingTempTable);
        return executeHierarchyPlan(plan, continuation, evaluationContext, numberOfItemsToReturn);
    }

    /**
     * Executes a hierarchical plan, created by invoking {@link RecursiveQueriesTest#hierarchicalQuery(Map, Map, BiFunction, RecursiveUnionExpression.Traversal)},
     * or resumes a previous execution given its continuation. The execution can be bounded to return a specific number
     * of elements only.
     *
     * @param hierarchyPlan The hierarchy plan, created by invoking {@link RecursiveQueriesTest#hierarchicalQuery(Map, Map, BiFunction, RecursiveUnionExpression.Traversal)}.
     * @param continuation An optional continuation belonging to previous interrupted execution.
     * @param numberOfItemsToReturn An optional number of items to return, {@code -1} to get all items.
     * @return the {@code id} portion of plan execution results
     */
    @Nonnull
    private HierarchyExecutionResult executeHierarchyPlan(@Nonnull final RecordQueryPlan hierarchyPlan,
                                                          @Nullable final byte[] continuation,
                                                          @Nonnull EvaluationContext evaluationContext,
                                                          int numberOfItemsToReturn) {
        int counter = 0;
        final var resultBuilder = ImmutableList.<Pair<Long, Long>>builder();
        final Stopwatch timer = Stopwatch.createStarted();
        try (RecordCursorIterator<QueryResult> cursor = hierarchyPlan.executePlan(
                recordStore, evaluationContext, continuation,
                ExecuteProperties.newBuilder().build()).asIterator()) {
            while (cursor.hasNext()) {
                Message message = Verify.verifyNotNull(cursor.next()).getMessage();
                resultBuilder.add(asIdParent(message));
                if (counter != -1 && ++counter == numberOfItemsToReturn) {
                    final var elapsedTimeMs = timer.elapsed(TimeUnit.MILLISECONDS);
                    return new HierarchyExecutionResult(hierarchyPlan,
                            resultBuilder.build().stream().map(Pair::getKey).collect(ImmutableList.toImmutableList()),
                            cursor.getContinuation(), elapsedTimeMs);
                }
            }
            // todo: check if the continuation here is an END continuation.
            final var elapsedTimeMs = timer.elapsed(TimeUnit.MILLISECONDS);
            return new HierarchyExecutionResult(hierarchyPlan,
                    resultBuilder.build().stream().map(Pair::getKey).collect(ImmutableList.toImmutableList()),
                    cursor.getContinuation(), elapsedTimeMs);
        }
    }

    /**
     * Creates and optimizes a hierarchy query plan.
     *
     * @param seedingTempTableAlias The alias for the seeding temp table.
     * @param insertTempTableAlias The alias for the insert temp table.
     * @param scanTempTableAlias The alias for the scan temp table.
     * @param queryPredicate The predicate that determines the fix point.
     * @param traversal The traversal order to use (LEVEL, PREORDER, or ANY).
     * @return The optimized query plan.
     */
    @Nonnull
    private RecordQueryPlan createAndOptimizeHierarchyQuery(@Nonnull final CorrelationIdentifier seedingTempTableAlias,
                                                            @Nonnull final CorrelationIdentifier insertTempTableAlias,
                                                            @Nonnull final CorrelationIdentifier scanTempTableAlias,
                                                            @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate,
                                                            @Nonnull final RecursiveUnionExpression.Traversal traversal) {
        final var ttScanSeeding = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(seedingTempTableAlias, getHierarchyType())));
        var selectExpression = GraphExpansion.builder()
                .addAllResultColumns(List.of(getIdCol(ttScanSeeding), getParentCol(ttScanSeeding))).addQuantifier(ttScanSeeding)
                .build().buildSelect();
        final var seedingSelectQun = Quantifier.forEach(Reference.initialOf(selectExpression));
        final var tempTableInsertExpression = TempTableInsertExpression.ofCorrelated(seedingSelectQun,
                insertTempTableAlias, getInnerType(seedingSelectQun));
        final var initInsertQun = Quantifier.forEach(Reference.initialOf(tempTableInsertExpression));
        final var tempTableScanExpression = TempTableScanExpression.ofCorrelated(scanTempTableAlias, getHierarchyType());
        final var ttScanRecuQun = Quantifier.forEach(Reference.initialOf(tempTableScanExpression));
        selectExpression = GraphExpansion.builder()
                .addAllResultColumns(List.of(getIdCol(ttScanRecuQun), getParentCol(ttScanRecuQun)))
                .addQuantifier(ttScanRecuQun)
                .build().buildSelect();
        final var ttSelectQun = Quantifier.forEach(Reference.initialOf(selectExpression));
        final var hierarchyScanQun = generateHierarchyScan(queryPredicate, ttSelectQun);
        final var joinExpression = GraphExpansion.builder()
                .addAllResultColumns(List.of(getIdCol(hierarchyScanQun), getParentCol(hierarchyScanQun)))
                .addQuantifier(hierarchyScanQun)
                .addQuantifier(ttSelectQun)
                .build().buildSelect();

        final var joinQun = Quantifier.forEach(Reference.initialOf(joinExpression));
        final var recuInsertQun = Quantifier.forEach(Reference.initialOf(TempTableInsertExpression.ofCorrelated(joinQun,
                insertTempTableAlias, getInnerType(joinQun))));
        final var recursiveUnionPlan = new RecursiveUnionExpression(initInsertQun, recuInsertQun, scanTempTableAlias, insertTempTableAlias, traversal);

        final var logicalPlan = Reference.initialOf(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.initialOf(recursiveUnionPlan))));
        final var cascadesPlanner = (CascadesPlanner)planner;
        return cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
    }

    @Nonnull
    private Quantifier.ForEach generateHierarchyScan(@Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate,
                                                     @Nonnull final Quantifier.ForEach ttSelectQun) {
        var qun = Quantifier.forEach(Reference.initialOf(
                new FullUnorderedScanExpression(ImmutableSet.of("SimpleHierarchicalRecord"),
                        new Type.AnyRecord(false),
                        new AccessHints())));

        qun = Quantifier.forEach(Reference.initialOf(new LogicalTypeFilterExpression(ImmutableSet.of("SimpleHierarchicalRecord"), qun, getHierarchyType())));

        final var predicate = queryPredicate.apply(qun, ttSelectQun);

        final var selectBuilder = GraphExpansion.builder()
                .addAllResultColumns(List.of(getIdCol(qun), getParentCol(qun)))
                .addPredicate(predicate)
                .addQuantifier(qun);
        qun = Quantifier.forEach(Reference.initialOf(selectBuilder.build().buildSelect()));
        return qun;
    }

    private void reportExecutionTimes(@Nonnull final List<Long> executionTimesMillis, @Nonnull final List<byte[]> continuations) {
        Verify.verify(executionTimesMillis.size() == continuations.size());
        System.out.println("executions count:\t" + executionTimesMillis.size());
        System.out.println("total execution duration:\t" + executionTimesMillis.stream().reduce(0L, Long::sum) + " ms");
        for (int i = 0; i < executionTimesMillis.size(); i++) {
            System.out.println("execution " + i + " duration:\t" + executionTimesMillis.get(i) + " ms");
            System.out.println("execution " + i + " continuation size:\t" + continuations.get(i).length + " bytes");
        }
    }

    private static final class HierarchyExecutionResult {

        @Nonnull
        private final RecordQueryPlan plan;

        @Nonnull
        private final List<Long> executionResult;

        @Nullable
        private final byte[] continuation;

        private final long executionTimeMillis;

        HierarchyExecutionResult(@Nonnull final RecordQueryPlan plan,
                                 @Nonnull final List<Long> executionResult,
                                 @Nullable final byte[] continuation,
                                 final long executionTimeMillis) {
            this.plan = plan;
            this.executionResult = executionResult;
            this.continuation = continuation;
            this.executionTimeMillis = executionTimeMillis;
        }

        @Nonnull
        public RecordQueryPlan getPlan() {
            return plan;
        }

        @Nonnull
        public List<Long> getExecutionResult() {
            return executionResult;
        }

        @Nullable
        public byte[] getContinuation() {
            return continuation;
        }

        public long getExecutionTimeMillis() {
            return executionTimeMillis;
        }
    }
}
