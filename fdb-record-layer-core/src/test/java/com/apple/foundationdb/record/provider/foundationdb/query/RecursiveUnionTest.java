/*
 * RecursiveUnionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link com.apple.foundationdb.record.query.plan.plans.RecursiveUnionQueryPlan} planning and execution.
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
        var result = ancestorsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(250L, 50L), ImmutableList.of(1, 2, 1));
        assertEquals(ImmutableList.of(ImmutableList.of(250L), ImmutableList.of(50L, 10L), ImmutableList.of(1L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase11() {
        var result = ancestorsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(250L, 50L), ImmutableList.of(1, 1, 2));
        assertEquals(ImmutableList.of(ImmutableList.of(250L), ImmutableList.of(50L), ImmutableList.of(10L, 1L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase12() {
        var result = ancestorsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(250L, 50L), ImmutableList.of(1, -1));
        assertEquals(ImmutableList.of(ImmutableList.of(250L), ImmutableList.of(50L, 10L, 1L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase13() {
        var result = descendantsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(1L, -1L), ImmutableList.of(1, -1));
        assertEquals(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(10L, 20L, 40L, 50L, 70L, 100L, 210L, 250L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase14() {
        var result = descendantsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(1L, -1L), ImmutableList.of(1, 2, 4, -1));
        assertEquals(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(10L, 20L), ImmutableList.of(40L, 50L, 70L, 100L), ImmutableList.of(210L, 250L)), result);
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void recursiveUnionWorksCorrectlyCase15() {
        var result = descendantsOfAcrossContinuations(sampleHierarchy(), ImmutableMap.of(1L, -1L), ImmutableList.of(1, 2, 4, -1));
        assertEquals(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(10L, 20L), ImmutableList.of(40L, 50L, 70L, 100L), ImmutableList.of(210L, 250L)), result);
    }

    @Tag(Tags.Performance)
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void randomizedHierarchyTestCase1() {
        final var randomHierarchy = Hierarchy.generateRandomHierarchy(1000, 4);
        final var descendants = randomHierarchy.calculateDescendants();
        final var randomContinuationScenario = ListPartitioner.partitionUsingPowerDistribution(10, descendants);
        final var continuationSnapshots = randomContinuationScenario.getKey();
        final var expectedResults = randomContinuationScenario.getValue();
        var result = descendantsOfAcrossContinuations(randomHierarchy.getEdges(), ImmutableMap.of(1L, -1L), continuationSnapshots);
        assertEquals(expectedResults, result);
    }

    /**
     * Creates a recursive union plan that calculates multiple series recursively {@code F(X) = F(X-1) * 2} up until
     * a given limit.
     *
     * @param initial The initial elements in the series, used to seed the recursion.
     * @param limit The (exclusive) limit of the series.
     * @return A series of by-two multiples starting with {@code initial} items up until the given {@code limit}. Note that the
     * initial items are still included in the final result even if they violate the limit.
     * @throws Exception If the execution of the recursive union plan fails.
     */
    @Nonnull
    private List<Long> multiplesOf(@Nonnull final List<Long> initial, long limit) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final var seedingTempTable = tempTableInstance();
            final var seedingTempTableAlias = CorrelationIdentifier.of("Seeding");
            initial.forEach(value -> seedingTempTable.add(queryResult(value)));

            final var ttScanBla = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(seedingTempTableAlias, seedingTempTableAlias.getId(), getType())));
            var selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(getIdCol(ttScanBla), getValueCol(ttScanBla))).addQuantifier(ttScanBla)
                    .build().buildSelect();
            final var seedingSelectQun = Quantifier.forEach(Reference.of(selectExpression));

            final var insertTempTableAlias = CorrelationIdentifier.of("Insert");
            final var insertTempTableReferenceValue = ConstantObjectValue.of(insertTempTableAlias, insertTempTableAlias.getId(), new Type.Relation(getType()));

            final var initInsertQun = Quantifier.forEach(Reference.of(TempTableInsertExpression.ofConstant(seedingSelectQun,
                    insertTempTableAlias, insertTempTableAlias.getId(), getType(seedingSelectQun))));

            final var scanTempTableAlias = CorrelationIdentifier.of("Scan");
            final var scanTempTableReferenceValue = ConstantObjectValue.of(scanTempTableAlias, scanTempTableAlias.getId(), new Type.Relation(getType()));

            final var ttScanRecuQun = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(scanTempTableAlias, scanTempTableAlias.getId(), getType())));
            var idField = getIdCol(ttScanRecuQun);
            final var multByTwo = Column.of(Optional.of("id"), (Value)new ArithmeticValue.MulFn().encapsulate(ImmutableList.of(idField.getValue(), LiteralValue.ofScalar(2L))));
            selectExpression = GraphExpansion.builder()
                    .addAllResultColumns(ImmutableList.of(multByTwo, getValueCol(ttScanRecuQun)))
                    .addQuantifier(ttScanRecuQun)
                    .build().buildSelect();
            final var selectQun = Quantifier.forEach(Reference.of(selectExpression));
            idField = getIdCol(selectQun);
            final var lessThanForty = new ValuePredicate(idField.getValue(), new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, limit));
            selectExpression = GraphExpansion.builder()
                    .addPredicate(lessThanForty)
                    .addQuantifier(selectQun)
                    .build().buildSimpleSelectOverQuantifier(selectQun);
            final var recuSelectQun = Quantifier.forEach(Reference.of(selectExpression));

            final var recuInsertQun = Quantifier.forEach(Reference.of(TempTableInsertExpression.ofConstant(recuSelectQun,
                    insertTempTableAlias, insertTempTableAlias.getId(), getType(recuSelectQun))));

            final var recursiveUnionPlan = new RecursiveUnionExpression(initInsertQun, recuInsertQun, scanTempTableReferenceValue, insertTempTableReferenceValue);

            final var logicalPlan = Reference.of(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.of(recursiveUnionPlan))));
            final var cascadesPlanner = (CascadesPlanner)planner;
            final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();

            var evaluationContext = putTempTableInContext(seedingTempTableAlias, seedingTempTable, null);
            return extractResultsAsIdValuePairs(context, plan, evaluationContext).stream().map(Pair::getKey).collect(ImmutableList.toImmutableList());
        }
    }

    /**
     * Creates a recursive union plan that calculates the ancestors of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list.
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
     * modelled with an adjacency list.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s ancestors are to be calculated.
     * @return A list of nodes representing the path from the given path from child(ren) to the parent.
     */
    @Nonnull
    private List<List<Long>> ancestorsOfAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                            @Nonnull final Map<Long, Long> initial,
                                                            @Nonnull final List<Integer> successiveRowLimits) {
        final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate = (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(hierarchyScanQun);
            final var parentField = getParentField(ttSelectQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        };
        return hierarchyQueryAcrossContinuations(hierarchy, initial, predicate, successiveRowLimits);
    }

    /**
     * Creates a recursive union plan that calculates the ancestors of a node (or multiple nodes) in a given hierarchy
     * modelled with an adjacency list.
     *
     * @param hierarchy The hierarchy, represented a list of {@code child -> parent} edges.
     * @param initial List of edges whose {@code child}'s ancestors are to be calculated.
     * @return A list of nodes representing the path from the given path from child(ren) to the parent.
     */
    @Nonnull
    private List<List<Long>> descendantsOfAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                            @Nonnull final Map<Long, Long> initial,
                                                            @Nonnull final List<Integer> successiveRowLimits) {
        final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate = (hierarchyScanQun, ttSelectQun) -> {
            final var idField = getIdField(ttSelectQun);
            final var parentField = getParentField(hierarchyScanQun);
            return new ValuePredicate(idField, new Comparisons.ValueComparison(Comparisons.Type.EQUALS, parentField));
        };
        return hierarchyQueryAcrossContinuations(hierarchy, initial, predicate, successiveRowLimits);
    }

    @Nonnull
    private List<List<Long>> hierarchyQueryAcrossContinuations(@Nonnull final Map<Long, Long> hierarchy,
                                                               @Nonnull final Map<Long, Long> initial,
                                                               @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> predicate,
                                                               @Nonnull final List<Integer> successiveRowLimits) {
        final ImmutableList.Builder<List<Long>> resultBuilder = ImmutableList.builder();
        try (FDBRecordContext context = openContext()) {
            var planAndResultAndContinuation = hierarchicalQuery(hierarchy, initial, predicate, context, null, successiveRowLimits.get(0));
            final var plan = planAndResultAndContinuation.getPlan();
            var continuation = planAndResultAndContinuation.getContinuation();
            resultBuilder.add(planAndResultAndContinuation.getExecutionResult());
            final var seedingTempTableAlias = CorrelationIdentifier.of("Seeding");
            for (final var rowLimit : successiveRowLimits.stream().skip(1).collect(ImmutableList.toImmutableList())) {
                final var seedingTempTable = tempTableInstance();
                initial.forEach((id, parent) -> seedingTempTable.add(queryResult(id, parent)));
                var evaluationContext = setUpPlanContext(plan, seedingTempTableAlias, seedingTempTable);
                final var pair = executeHierarchyPlan(plan, continuation, evaluationContext, rowLimit);
                continuation = pair.getRight();
                resultBuilder.add(Objects.requireNonNull(pair.getLeft()));
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
        createOrOpenRecordStore(context, RecordMetaData.build(TestHierarchiesProto.getDescriptor()));
        for (final var entry : hierarchy.entrySet()) {
            final var message = item(entry.getKey(), entry.getValue());
            recordStore.saveRecord(message);
        }
        final var seedingTempTableAlias = CorrelationIdentifier.of("Seeding");
        final var insertTempTableAlias = CorrelationIdentifier.of("Insert");
        final var scanTempTableAlias = CorrelationIdentifier.of("Scan");

        final var plan = createAndOptimizeHierarchyQuery(seedingTempTableAlias, insertTempTableAlias, scanTempTableAlias, queryPredicate);

        final var seedingTempTable = tempTableInstance();

        initial.forEach((id, parent) -> seedingTempTable.add(queryResult(id, parent)));
        var evaluationContext = setUpPlanContext(plan, seedingTempTableAlias, seedingTempTable);
        final var resultAndContinuation = executeHierarchyPlan(plan, continuation, evaluationContext, numberOfItemsToReturn);
        return new HierarchyExecutionResult(plan, resultAndContinuation.getKey(), resultAndContinuation.getRight());
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
    private Pair<List<Long>, byte[]> executeHierarchyPlan(@Nonnull final RecordQueryPlan hierarchyPlan,
                                                          @Nullable final byte[] continuation,
                                                          @Nonnull EvaluationContext evaluationContext,
                                                          int numberOfItemsToReturn) {
        int counter = 0;
        final var resultBuilder = ImmutableList.<Pair<Long, Long>>builder();
        try (RecordCursorIterator<QueryResult> cursor = hierarchyPlan.executePlan(
                recordStore, evaluationContext, continuation,
                ExecuteProperties.newBuilder().build()).asIterator()) {
            while (cursor.hasNext()) {
                Message message = Verify.verifyNotNull(cursor.next()).getMessage();
                resultBuilder.add(asIdParent(message));
                if (counter != -1 && ++counter == numberOfItemsToReturn) {
                    return Pair.of(resultBuilder.build().stream().map(Pair::getKey).collect(ImmutableList.toImmutableList()),
                            cursor.getContinuation());
                }
            }
            // todo: check if the continuation here is an END continuation.
            return Pair.of(resultBuilder.build().stream().map(Pair::getKey).collect(ImmutableList.toImmutableList()),
                    cursor.getContinuation());
        }
    }

    @Nonnull
    private RecordQueryPlan createAndOptimizeHierarchyQuery(@Nonnull final CorrelationIdentifier seedingTempTableAlias,
                                                            @Nonnull final CorrelationIdentifier insertTempTableAlias,
                                                            @Nonnull final CorrelationIdentifier scanTempTableAlias,
                                                            @Nonnull final BiFunction<Quantifier.ForEach, Quantifier.ForEach, QueryPredicate> queryPredicate) {
        final var ttScanSeeding = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(seedingTempTableAlias, seedingTempTableAlias.getId(), getType())));
        var selectExpression = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(ttScanSeeding), getValueCol(ttScanSeeding), getParentCol(ttScanSeeding))).addQuantifier(ttScanSeeding)
                .build().buildSelect();
        final var seedingSelectQun = Quantifier.forEach(Reference.of(selectExpression));
        final var tempTableInsertExpression = TempTableInsertExpression.ofConstant(seedingSelectQun,
                insertTempTableAlias, insertTempTableAlias.getId(), getType(seedingSelectQun));
        final var initInsertQun = Quantifier.forEach(Reference.of(tempTableInsertExpression));
        final var insertTempTableReferenceValue = tempTableInsertExpression.getTempTableReferenceValue();

        final var hierarchyScanQun = generateHierarchyScan();

        final var tempTableScanExpression = TempTableScanExpression.ofConstant(scanTempTableAlias, scanTempTableAlias.getId(), getType());
        final var scanTempTableReferenceValue = tempTableScanExpression.getTempTableReferenceValue();
        final var ttScanRecuQun = Quantifier.forEach(Reference.of(tempTableScanExpression));
        selectExpression = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(ttScanRecuQun), getValueCol(ttScanRecuQun), getParentCol(ttScanRecuQun)))
                .addQuantifier(ttScanRecuQun)
                .build().buildSelect();
        final var ttSelectQun = Quantifier.forEach(Reference.of(selectExpression));
        final var predicate = queryPredicate.apply(hierarchyScanQun, ttSelectQun);
        final var joinExpression = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(hierarchyScanQun), getValueCol(hierarchyScanQun), getParentCol(hierarchyScanQun)))
                .addPredicate(predicate)
                .addQuantifier(hierarchyScanQun)
                .addQuantifier(ttSelectQun)
                .build().buildSelect();

        final var joinQun = Quantifier.forEach(Reference.of(joinExpression));
        final var recuInsertQun = Quantifier.forEach(Reference.of(TempTableInsertExpression.ofConstant(joinQun,
                insertTempTableAlias, insertTempTableAlias.getId(), getType(joinQun))));
        final var recursiveUnionPlan = new RecursiveUnionExpression(initInsertQun, recuInsertQun, scanTempTableReferenceValue, insertTempTableReferenceValue);

        final var logicalPlan = Reference.of(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.of(recursiveUnionPlan))));
        final var cascadesPlanner = (CascadesPlanner)planner;
        return cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
    }

    @Nonnull
    private Quantifier.ForEach generateHierarchyScan() {
        var qun = Quantifier.forEach(Reference.of(
                new FullUnorderedScanExpression(ImmutableSet.of("SimpleHierarchicalRecord"),
                        new Type.AnyRecord(false),
                        new AccessHints())));

        qun = Quantifier.forEach(Reference.of(new LogicalTypeFilterExpression(ImmutableSet.of("SimpleHierarchicalRecord"), qun, getType())));
        final var selectBuilder = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(qun), getValueCol(qun), getParentCol(qun)))
                .addQuantifier(qun);
        qun = Quantifier.forEach(Reference.of(selectBuilder.build().buildSelect()));
        return qun;
    }

    private static final class HierarchyExecutionResult {

        @Nonnull
        private final RecordQueryPlan plan;

        @Nonnull
        private final List<Long> executionResult;

        @Nullable
        private final byte[] continuation;

        HierarchyExecutionResult(@Nonnull final RecordQueryPlan plan,
                                 @Nonnull final List<Long> executionResult,
                                 @Nullable final byte[] continuation) {
            this.plan = plan;
            this.executionResult = executionResult;
            this.continuation = continuation;
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
    }
}
