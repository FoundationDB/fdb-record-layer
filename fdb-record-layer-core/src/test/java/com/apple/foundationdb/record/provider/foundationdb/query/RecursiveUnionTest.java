/*
 * RecursiveUnionTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link RecordQueryRecursiveUnionPlan} planning and execution.
 */
public class RecursiveUnionTest extends TempTableTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase1() throws Exception {
        var result = multiplesOf(ImmutableList.of(2L, 5L), 50L);
        assertEquals(ImmutableList.of(2L, 5L, 4L, 10L, 8L, 20L, 16L, 40L, 32L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase2() throws Exception {
        var result = multiplesOf(ImmutableList.of(2L, 5L), 6L);
        assertEquals(ImmutableList.of(2L, 5L, 4L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase3() throws Exception {
        var result = multiplesOf(ImmutableList.of(2L, 5L), 0L);
        assertEquals(ImmutableList.of(2L, 5L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase4() throws Exception {
        var result = multiplesOf(ImmutableList.of(), 1000L);
        assertEquals(ImmutableList.of(), result);
    }

    /**
     * Sample hierarchy, visually looking like the following.
     * <pre>
     * {@code
     *                              1
     *                            /   \
     *                         /        \
     *                       /            \
     *                     10             20
     *                    / | \          /  \
     *                  /   |  \        /    \
     *                 /    |   \      /      \
     *                40   50   70   100      210
     *                      |
     *                      |
     *                     250
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
     *                              1                         500
     *                            /   \                      /   \
     *                         /        \                  /      \
     *                       /            \               /        \
     *                     10             20            510        520
     *                    / | \          /  \             |
     *                  /   |  \        /    \            |
     *                 /    |   \      /      \           |
     *                40   50   70   100      210       550
     *                      |
     *                      |
     *                     250
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

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase5() {
        var result = ancestorsOf(sampleHierarchy(), ImmutableMap.of(250L, 50L));
        assertEquals(ImmutableList.of(250L, 50L, 10L, 1L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase6() {
        var result = ancestorsOf(sampleHierarchy(), ImmutableMap.of(250L, 50L, 40L, 10L));
        assertEquals(ImmutableList.of(250L, 40L, 50L, 10L, 10L, 1L, 1L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase7() {
        var result = ancestorsOf(sampleHierarchy(), ImmutableMap.of(300L, 300L));
        assertEquals(ImmutableList.of(300L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase8() {
        var result = descendantsOf(sampleHierarchy(), ImmutableMap.of(1L, -1L));
        assertEquals(ImmutableList.of(1L, 10L, 20L, 40L, 50L, 70L, 100L, 210L, 250L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase9() {
        var result = descendantsOf(sampleHierarchy(), ImmutableMap.of(10L, 1L));
        assertEquals(ImmutableList.of(10L, 40L, 50L, 70L, 250L), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase10() {
        var result = ancestorsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(250L, 50L), ImmutableList.of(1, 2, 1), false);
        assertEquals(ImmutableList.of(ImmutableList.of(250L), ImmutableList.of(50L, 10L), ImmutableList.of(1L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase11() {
        var result = ancestorsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(250L, 50L), ImmutableList.of(1, 1, 2), false);
        assertEquals(ImmutableList.of(ImmutableList.of(250L), ImmutableList.of(50L), ImmutableList.of(10L, 1L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase12() {
        var result = ancestorsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(250L, 50L), ImmutableList.of(1, -1), false);
        assertEquals(ImmutableList.of(ImmutableList.of(250L), ImmutableList.of(50L, 10L, 1L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase13() {
        var result = descendantsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(1L, -1L), ImmutableList.of(1, -1), false);
        assertEquals(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(10L, 20L, 40L, 50L, 70L, 100L, 210L, 250L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase14() {
        var result = descendantsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(1L, -1L), ImmutableList.of(1, 2, 4, -1), false);
        assertEquals(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(10L, 20L), ImmutableList.of(40L, 50L, 70L, 100L), ImmutableList.of(210L, 250L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase15() {
        var result = descendantsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(1L, -1L), ImmutableList.of(1, 2, 4, -1), false);
        assertEquals(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(10L, 20L), ImmutableList.of(40L, 50L, 70L, 100L), ImmutableList.of(210L, 250L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase16() {
        var result = descendantsOfAcrossContinuations(sampleForest(), ImmutableMap.of(1L, -1L, 500L, -1L), ImmutableList.of(1, 2, 4, -1), false);
        assertEquals(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(500L, 10L), ImmutableList.of(20L, 510L, 520L, 40L), ImmutableList.of(50L, 70L, 100L, 210L, 550L, 250L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void randomizedHierarchyTestCase1() {
        final var randomHierarchy = Hierarchy.generateRandomHierarchy(1000, 10);
        final var descendants = randomHierarchy.calculateDescendants();
        final var randomContinuationScenario = ListPartitioner.partitionUsingPowerDistribution(1, descendants);
        final var continuationSnapshots = randomContinuationScenario.getKey();
        final var expectedResults = randomContinuationScenario.getValue();
        var result = descendantsOfAcrossContinuations(randomHierarchy.getEdges(), ImmutableMap.of(1L, -1L), continuationSnapshots, false);
        assertEquals(expectedResults, result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void randomizedHierarchyTestCase2() {
        final var randomHierarchy = Hierarchy.generateRandomHierarchy(1000, 10);
        final var leaf = randomHierarchy.getRandomLeaf();
        final var parent = randomHierarchy.getEdges().get(leaf);
        final var ancestors = randomHierarchy.calculateAncestors(leaf);
        final var randomContinuationScenario = ListPartitioner.partitionUsingPowerDistribution(5, ancestors);
        final var continuationSnapshots = randomContinuationScenario.getKey();
        final var expectedResults = randomContinuationScenario.getValue();
        var result = ancestorsOfAcrossContinuations(randomHierarchy.getEdges(), ImmutableMap.of(leaf, parent), continuationSnapshots, false);
        assertEquals(expectedResults, result);
    }

    /**
     * Creates and executes a recursive union plan that calculates multiple series recursively {@code F(X) = F(X-1) * 2}
     * up until a given limit.
     *
     * @param initial The initial elements in the series, used to seed the recursion.
     * @param limit The (exclusive) limit of the series.
     * @return A series of by-two multiples starting with {@code initial} items up until the given {@code limit}. The Note
     * that the initial items are still included in the final result even if they violate the limit.
     * @throws Exception If the execution of the recursive union plan fails.
     */
    @Nonnull
    private List<Long> multiplesOf(@Nonnull final List<Long> initial, long limit) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final var seedingTempTable = tempTableInstance();
            final var seedingTempTableAlias = CorrelationIdentifier.of("Seeding");
            initial.forEach(value -> seedingTempTable.add(queryResult(value)));

            final var ttScanBla = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(seedingTempTableAlias, getHierarchyType())));
            var selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(getIdCol(ttScanBla))).addQuantifier(ttScanBla)
                    .build().buildSelect();
            final var seedingSelectQun = Quantifier.forEach(Reference.initialOf(selectExpression));
            final var insertTempTableAlias = CorrelationIdentifier.of("Insert");
            final var initInsertQun = Quantifier.forEach(Reference.initialOf(TempTableInsertExpression.ofCorrelated(seedingSelectQun,
                    insertTempTableAlias, getInnerType(seedingSelectQun))));
            final var scanTempTableAlias = CorrelationIdentifier.of("Scan");

            final var ttScanRecuQun = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(scanTempTableAlias, getHierarchyType())));
            var idField = getIdCol(ttScanRecuQun);
            final var multByTwo = Column.of(Optional.of("id"), (Value)new ArithmeticValue.MulFn().encapsulate(ImmutableList.of(idField.getValue(), LiteralValue.ofScalar(2L))));
            selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(multByTwo))
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

            final var recursiveUnionPlan = new RecursiveUnionExpression(initInsertQun, recuInsertQun, scanTempTableAlias, insertTempTableAlias, RecursiveUnionExpression.Traversal.LEVEL);

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
     * @return A list of nodes representing the path from the given path from child(ren) to the parent.
     */
    @Nonnull
    private List<Long> ancestorsOf(@Nonnull final Map<Long, Long> hierarchy, @Nonnull final Map<Long, Long> initial) {
        return hierarchicalQuery(hierarchy, initial, (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(hierarchyScanQun);
            final var parentField = getParentField(ttSelectQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        });
    }

    /**
     * Creates a recursive union plan that calculates the descendants of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s descendants are to be calculated.
     * @return A list of nodes representing the path from the given parent(s) to the children.
     */
    @Nonnull
    private List<Long> descendantsOf(@Nonnull final Map<Long, Long> hierarchy, @Nonnull final Map<Long, Long> initial) {
        return hierarchicalQuery(hierarchy, initial, (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(ttSelectQun);
            final var parentField = getParentField(hierarchyScanQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        });
    }

    /**
     * Creates a recursive union plan that calculates the ancestors of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list given a list of execution resumptions by means of result offsets.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s ancestors are to be calculated.
     * @param successiveRowLimits execution resumption points by means of result offsets.\
     *
     * @return A chunked list of nodes representing the path from the given path from child(ren) to the parent across
     * the given list of execution resumptions.
     */
    @Nonnull
    private List<List<Long>> ancestorsOfAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                            @Nonnull final Map<Long, Long> initial,
                                                            @Nonnull final List<Integer> successiveRowLimits,
                                                            final boolean reportExecutionTime) {
        final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate = (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(hierarchyScanQun);
            final var parentField = getParentField(ttSelectQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        };
        return hierarchyQueryAcrossContinuations(hierarchy, initial, predicate, successiveRowLimits, reportExecutionTime);
    }

    /**
     * Creates a recursive union plan that calculates the ancestors of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list given a list of execution resumptions by means of result offsets.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s ancestors are to be calculated.
     * @param successiveRowLimits execution resumption points by means of result offsets.
     *
     * @return A chunked list of nodes representing the path from the given path from child(ren) to the parent across
     * the given list of execution resumptions.
     */
    @Nonnull
    private List<List<Long>> descendantsOfAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                              @Nonnull final Map<Long, Long> initial,
                                                              @Nonnull final List<Integer> successiveRowLimits,
                                                              final boolean reportExecutionTime) {
        final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate = (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(ttSelectQun);
            final var parentField = getParentField(hierarchyScanQun);
            return new ValuePredicate(parentField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, idField));
        };
        return hierarchyQueryAcrossContinuations(hierarchy, initial, predicate, successiveRowLimits, reportExecutionTime);
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
     * @return A chunked list of nodes representing the execution results of the recursive query.
     */
    @Nonnull
    private List<List<Long>> hierarchyQueryAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                               @Nonnull final Map<Long, Long> initial,
                                                               @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate,
                                                               @Nonnull final List<Integer> successiveRowLimits,
                                                               final boolean reportExecutionTime) {
        final ImmutableList.Builder<List<Long>> resultBuilder = ImmutableList.builder();
        try (FDBRecordContext context = openContext()) {
            var executionTimesMs =  ImmutableList.<Long>builder();
            var executionResult = hierarchicalQuery(hierarchy, initial, predicate, context, null, successiveRowLimits.get(0));
            executionTimesMs.add(executionResult.getExecutionTimeMillis());
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
                continuation = executionResult.getContinuation();
                resultBuilder.add(Objects.requireNonNull(executionResult.getExecutionResult()));
            }

            if (reportExecutionTime) {
                reportExecutionTimes(executionTimesMs.build());
            }
        }
        return resultBuilder.build();
    }

    @Nonnull
    private List<Long> hierarchicalQuery(@Nonnull final Map<Long, Long> hierarchy,
                                         @Nonnull final Map<Long, Long> initial,
                                         @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate) {
        try (FDBRecordContext context = openContext()) {
            return hierarchicalQuery(hierarchy, initial, queryPredicate, context, null, -1).getExecutionResult();
        }
    }

    @Nonnull
    private HierarchyExecutionResult hierarchicalQuery(@Nonnull final Map<Long, Long> hierarchy,
                                                                         @Nonnull final Map<Long, Long> initial,
                                                                         @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate,
                                                                         @Nonnull final FDBRecordContext context,
                                                                         @Nullable final byte[] continuation,
                                                                         int numberOfItemsToReturn) {
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

        final var plan = createAndOptimizeHierarchyQuery(seedingTempTableAlias, insertTempTableAlias, scanTempTableAlias, queryPredicate);

        System.out.println(ExplainPlanVisitor.prettyExplain(plan));

        final var seedingTempTable = tempTableInstance();

        initial.forEach((id, parent) -> seedingTempTable.add(queryResult(id, parent)));
        var evaluationContext = setUpPlanContext(plan, seedingTempTableAlias, seedingTempTable);
        return executeHierarchyPlan(plan, continuation, evaluationContext, numberOfItemsToReturn);
    }

    /**
     * Executes a hierarchical plan, created by invoking {@link RecursiveUnionTest#hierarchicalQuery(Map, Map, BiFunction)},
     * or resumes a previous execution given its continuation. The execution can be bounded to return a specific number
     * of elements only.
     *
     * @param hierarchyPlan The hierarchy plan, created by invoking {@link RecursiveUnionTest#hierarchicalQuery(Map, Map, BiFunction)}.
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

    @Nonnull
    private RecordQueryPlan createAndOptimizeHierarchyQuery(@Nonnull final CorrelationIdentifier seedingTempTableAlias,
                                                            @Nonnull final CorrelationIdentifier insertTempTableAlias,
                                                            @Nonnull final CorrelationIdentifier scanTempTableAlias,
                                                            @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate) {
        final var ttScanSeeding = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(seedingTempTableAlias, getHierarchyType())));
        var selectExpression = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(ttScanSeeding), getParentCol(ttScanSeeding))).addQuantifier(ttScanSeeding)
                .build().buildSelect();
        final var seedingSelectQun = Quantifier.forEach(Reference.initialOf(selectExpression));
        final var tempTableInsertExpression = TempTableInsertExpression.ofCorrelated(seedingSelectQun,
                insertTempTableAlias, getInnerType(seedingSelectQun));
        final var initInsertQun = Quantifier.forEach(Reference.initialOf(tempTableInsertExpression));
        final var tempTableScanExpression = TempTableScanExpression.ofCorrelated(scanTempTableAlias, getHierarchyType());
        final var ttScanRecuQun = Quantifier.forEach(Reference.initialOf(tempTableScanExpression));
        selectExpression = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(ttScanRecuQun), getParentCol(ttScanRecuQun)))
                .addQuantifier(ttScanRecuQun)
                .build().buildSelect();
        final var ttSelectQun = Quantifier.forEach(Reference.initialOf(selectExpression));
        final var hierarchyScanQun = generateHierarchyScan(queryPredicate, ttSelectQun);
        final var joinExpression = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(hierarchyScanQun), getParentCol(hierarchyScanQun)))
                .addQuantifier(hierarchyScanQun)
                .addQuantifier(ttSelectQun)
                .build().buildSelect();

        final var joinQun = Quantifier.forEach(Reference.initialOf(joinExpression));
        final var recuInsertQun = Quantifier.forEach(Reference.initialOf(TempTableInsertExpression.ofCorrelated(joinQun,
                insertTempTableAlias, getInnerType(joinQun))));
        final var recursiveUnionPlan = new RecursiveUnionExpression(initInsertQun, recuInsertQun, scanTempTableAlias, insertTempTableAlias, RecursiveUnionExpression.Traversal.LEVEL);

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
                .addAllResultColumns(ImmutableList.of(getIdCol(qun), getParentCol(qun)))
                .addPredicate(predicate)
                .addQuantifier(qun);
        qun = Quantifier.forEach(Reference.initialOf(selectBuilder.build().buildSelect()));
        return qun;
    }

    private void reportExecutionTimes(@Nonnull final List<Long> executionTimesMillis) {
        System.out.println("executions count:\t" + executionTimesMillis.size());
        System.out.println("total execution duration:\t" + executionTimesMillis.stream().reduce(0L, Long::sum) + " ms");
        for (int i = 0; i < executionTimesMillis.size(); i++) {
            System.out.println("execution " + i + " duration:\t" + executionTimesMillis.get(i) + " ms");
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
