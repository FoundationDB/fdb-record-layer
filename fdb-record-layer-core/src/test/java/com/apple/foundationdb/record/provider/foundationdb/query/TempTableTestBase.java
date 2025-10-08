/*
 * TempTableTestBase.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.TestHierarchiesProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableScanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty.usedTypes;

/**
 * Contains utility methods around {@link TempTable}s to make testing temp table planning and execution easier.
 */
public abstract class TempTableTestBase extends FDBRecordStoreQueryTestBase {

    @Nonnull
    private static final Random random = new Random(42L);

    @BeforeEach
    void setupPlanner() {
        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);
        }
    }

    /**
     * executes the plans, with the passed in {@link TempTable} as input to the upper operator added to a newly created
     * {@link EvaluationContext}.
     *
     * @param context The transaction used to execute the plan.
     * @param plan The plan to execute.
     * @param inputTempTable The input temp table, possibly containing the expected plan results.
     * @param tempTableId The id of the temp table, used to register it correctly in the {@link EvaluationContext}.
     *
     * @return The execution results of the {@code plan} mapped to a pair of the {@code id} and {@code value}.
     *
     * @throws Exception If the execution fails.
     */
    @Nonnull
    List<Pair<Long, String>> collectResults(@Nonnull final FDBRecordContext context,
                                            @Nonnull final RecordQueryPlan plan,
                                            @Nonnull final TempTable inputTempTable,
                                            @Nonnull final CorrelationIdentifier tempTableId) throws Exception {
        final var evaluationContext = putTempTableInContext(tempTableId, inputTempTable, null);
        return extractResultsAsIdValuePairs(context, plan, evaluationContext);
    }

    @Nonnull
    List<Pair<Long, String>> collectResults(@Nonnull final TempTable inputTempTable) {
        return inputTempTable.getList().stream().map(TempTableTestBase::asIdValue).collect(ImmutableList.toImmutableList());
    }

    /**
     * Utility method that executes a {@code plan} returning its results.
     *
     * @param context The transaction used to execute the plan.
     * @param plan The plan to be executed.
     * @param evaluationContext The evaluation context.
     *
     * @return The execution results of the {@code plan} mapped to a pair of the {@code id} and {@code value}.
     *
     * @throws Exception If the execution fails.
     */
    @Nonnull
    List<Pair<Long, String>> extractResultsAsIdValuePairs(@Nonnull final FDBRecordContext context,
                                                          @Nonnull final RecordQueryPlan plan,
                                                          @Nonnull final EvaluationContext evaluationContext) throws Exception {
        ImmutableList.Builder<Pair<Long, String>> resultBuilder = ImmutableList.builder();
        fetchResultValues(context, plan, record -> {
            resultBuilder.add(asIdValue(record));
            return record;
        }, evaluationContext, c -> {
        }, ExecuteProperties.newBuilder().build());
        return resultBuilder.build();
    }

    /**
     * Utility method that executes a {@code plan} returning its results.
     *
     * @param context The transaction used to execute the plan.
     * @param plan The plan to be executed.
     * @param evaluationContext The evaluation context.
     *
     * @return The execution results of the {@code plan} mapped to a pair of the {@code id} and {@code value}.
     *
     * @throws Exception If the execution fails.
     */
    @Nonnull
    List<Long> extractResultsAsIds(@Nonnull final FDBRecordContext context,
                                   @Nonnull final RecordQueryPlan plan,
                                   @Nonnull final EvaluationContext evaluationContext) throws Exception {
        ImmutableList.Builder<Long> resultBuilder = ImmutableList.builder();
        fetchResultValues(context, plan, record -> {
            resultBuilder.add(asId(record));
            return record;
        }, evaluationContext, c -> {
        }, ExecuteProperties.newBuilder().build());
        return resultBuilder.build();
    }

    static long asId(@Nonnull final Message message) {
        final var descriptor = message.getDescriptorForType();
        return (long)message.getField(descriptor.findFieldByName("id"));
    }

    @Nonnull
    static Pair<Long, String> asIdValue(@Nonnull final Message message) {
        final var descriptor = message.getDescriptorForType();
        return Pair.of((Long)message.getField(descriptor.findFieldByName("id")),
                (String)message.getField(descriptor.findFieldByName("value")));
    }

    @Nonnull
    static Pair<Long, String> asIdValue(@Nonnull final QueryResult queryResult) {
        final Message message = queryResult.getMessage();
        return asIdValue(message);
    }

    @Nonnull
    static Pair<Long, Long> asIdParent(@Nonnull final Message message) {
        final var descriptor = message.getDescriptorForType();
        return Pair.of((Long)message.getField(descriptor.findFieldByName("id")),
                (Long)message.getField(descriptor.findFieldByName("parent")));
    }

    @Nonnull
    static QueryResult queryResult(long id, @Nonnull final String value) {
        return QueryResult.ofComputed(item(id, value));
    }

    @Nonnull
    static QueryResult queryResult(long id) {
        return queryResult(id, id + "Value");
    }

    @Nonnull
    static QueryResult queryResult(long id, long parent) {
        return QueryResult.ofComputed(item(id, parent));
    }

    @Nonnull
    static Message item(long id, @Nonnull final String value) {
        return TestHierarchiesProto.TempTableRecord.newBuilder()
                .setId(id)
                .setValue(value)
                .build();
    }

    @Nonnull
    static Message item(long id) {
        return TestHierarchiesProto.SimpleHierarchicalRecord.newBuilder()
                .setId(id)
                .build();
    }

    @Nonnull
    static Message item(long id, long parent) {
        return TestHierarchiesProto.SimpleHierarchicalRecord.newBuilder()
                .setId(id)
                .setParent(parent)
                .build();
    }

    @Nonnull
    private static String arrow(long child, long parent) {
        return child + " -> " + parent;
    }

    @Nonnull
    static RecordConstructorValue rcv(long id, @Nonnull final String value) {
        return RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG),
                                Optional.of("id")), LiteralValue.ofScalar(id)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING),
                                Optional.of("value")), LiteralValue.ofScalar(value))));
    }

    @Nonnull
    static RecordConstructorValue rcv(long id, long parent, @Nonnull final String value) {
        return RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG),
                                Optional.of("id")), LiteralValue.ofScalar(id)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG),
                                Optional.of("parent")), LiteralValue.ofScalar(parent)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING),
                                Optional.of("value")), LiteralValue.ofScalar(value))));
    }

    @Nonnull
    RecordQueryPlan createAndOptimizeTempTableScanPlan(@Nonnull final CorrelationIdentifier tempTableId) {
        final var tempTableScanQun = Quantifier.forEach(Reference.initialOf(TempTableScanExpression.ofCorrelated(tempTableId, getTempTableType())));
        final var selectExpressionBuilder = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(tempTableScanQun), getValueCol(tempTableScanQun)))
                .addQuantifier(tempTableScanQun);
        final var logicalPlan = Reference.initialOf(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.initialOf(selectExpressionBuilder.build().buildSelect()))));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();
        assertMatchesExactly(plan, mapPlan(tempTableScanPlan()));
        return plan;
    }

    static void addSampleDataToTempTable(@Nonnull final TempTable tempTable) {
        tempTable.add(queryResult(42L, "fortySecondValue"));
        tempTable.add(queryResult(45L, "fortyFifthValue"));
    }

    @Nonnull
    static EvaluationContext putTempTableInContext(@Nonnull final CorrelationIdentifier tempTableAlias,
                                                   @Nonnull final TempTable tempTable,
                                                   @Nullable EvaluationContext parentContext) {
        final var actualParentContext = parentContext == null ? EvaluationContext.empty() : parentContext;
        return actualParentContext.withBinding(Bindings.Internal.CORRELATION, tempTableAlias, tempTable);
    }

    @Nonnull
    static EvaluationContext setUpPlanContext(@Nonnull final RecordQueryPlan recordQueryPlan,
                                              @Nonnull final CorrelationIdentifier tempTableAlias,
                                              @Nonnull final TempTable tempTable) {
        final var evaluationContext = setUpPlanContextTypes(recordQueryPlan);
        return putTempTableInContext(tempTableAlias, tempTable, evaluationContext);
    }

    @Nonnull
    static EvaluationContext setUpPlanContextTypes(@Nonnull final RecordQueryPlan recordQueryPlan) {
        final var usedTypes = usedTypes().evaluate(recordQueryPlan);
        return EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(usedTypes).build());
    }

    @Nonnull
    static FieldValue getIdField(@Nonnull final Quantifier.ForEach quantifier) {
        return FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "id");
    }

    @Nonnull
    static FieldValue getParentField(@Nonnull final Quantifier.ForEach quantifier) {
        return FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "parent");
    }

    @Nonnull
    static Column<Value> getIdCol(@Nonnull final Quantifier.ForEach quantifier) {
        return Column.of(Optional.of("id"), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "id"));
    }

    @Nonnull
    static Column<Value> getIdColAs(@Nonnull final Quantifier.ForEach quantifier, @Nonnull final String name) {
        return Column.of(Optional.of(name), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "id"));
    }

    @Nonnull
    static Column<Value> getValueCol(@Nonnull final Quantifier.ForEach quantifier) {
        return Column.of(Optional.of("value"), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "value"));
    }

    @Nonnull
    static Column<Value> getParentCol(@Nonnull final Quantifier.ForEach quantifier) {
        return Column.of(Optional.of("parent"), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "parent"));
    }

    @Nonnull
    static Type getHierarchyType() {
        return Type.Record.fromDescriptor(getHierarchyDescriptor()).notNullable();
    }

    @Nonnull
    static Type getInnerType(@Nonnull final Quantifier.ForEach forEach) {
        return Objects.requireNonNull(((Type.Relation)forEach.getRangesOver().getResultType()).getInnerType());
    }

    @Nonnull
    static Type getTempTableType() {
        return Type.Record.fromDescriptor(getTempTableDescriptor()).notNullable();
    }

    @Nonnull
    static Descriptors.Descriptor getHierarchyDescriptor() {
        return TestHierarchiesProto.SimpleHierarchicalRecord.getDescriptor();
    }

    @Nonnull
    static Descriptors.Descriptor getTempTableDescriptor() {
        return TestHierarchiesProto.TempTableRecord.getDescriptor();
    }

    @Nonnull
    public static TempTable tempTableInstance() {
        return TempTable.Factory.instance().createTempTable();
    }

    static final class ListPartitioner {

        /**
         * Generates a list of partition sizes using a power distribution.
         * This method creates random split points that follow a power law distribution,
         * which tends to create more smaller partitions and fewer larger ones.
         *
         * @param numSplits The number of partitions to generate
         * @param end The total size of the input to be partitioned
         * @return A list of integers representing partition sizes
         */
        @Nonnull
        public static List<Integer> getSplitsUsingPowerDistribution(int numSplits, int end) {
            double alpha = 0.4;
            double randomnessFactor = 0.3;
            Random random = new Random();
            int totalRange = end + 1;

            // Generate inverse power distribution weights with randomness
            double[] weights = new double[numSplits];
            double totalWeight = 0;

            for (int i = 0; i < numSplits; i++) {
                double baseWeight = Math.pow(numSplits - i, -alpha);
                double randomVariance = 1 + (random.nextGaussian() * randomnessFactor);
                weights[i] = baseWeight * Math.max(0.1, randomVariance);
                totalWeight += weights[i];
            }

            // Calculate sizes
            int[] sizes = new int[numSplits];
            int assignedTotal = 0;

            Assertions.assertTrue(totalWeight > 0);
            for (int i = 0; i < numSplits - 1; i++) {
                double normalizedWeight = weights[i] / totalWeight;
                sizes[i] = Math.max(1, (int) Math.round(totalRange * normalizedWeight));
                assignedTotal += sizes[i];
            }

            // Assign remaining elements to the last split
            sizes[numSplits - 1] = Math.max(1, totalRange - assignedTotal);

            // Create boundaries
            List<Integer> boundaries = new ArrayList<>();

            int currentPos = 0;
            for (int i = 0; i < numSplits - 1; i++) {
                currentPos += sizes[i];
                boundaries.add(Math.min(currentPos, end + 1));
            }

            return boundaries;
        }

        /**
         * Generates a list of partition sizes using a normal (uniform) distribution.
         * This method creates random split points with equal probability across
         * the entire range, resulting in more evenly distributed partition sizes.
         *
         * @param continuationCount The number of partitions to generate
         * @param inputSize The total size of the input to be partitioned
         * @return A list of integers representing partition sizes
         */
        @Nonnull
        public static List<Integer> getSplitsUsingNormalDistribution(int continuationCount, int inputSize) {
            if (continuationCount == 0) {
                return List.of();
            }
            int splitCount = continuationCount + 2; // two extra splits for boundaries.
            if (splitCount == 0) {
                return List.of();
            }
            Verify.verify(inputSize > 0);
            Random random = new Random();

            int baseSize = inputSize / splitCount;
            int remainder = inputSize % splitCount;

            // Generate random split points while maintaining similar sizes
            List<Integer> boundaries = new ArrayList<>();

            int currentPos = 0;
            for (int i = 0; i < splitCount - 1; i++) {
                // Calculate target size for this split
                int targetSize = baseSize + (i < remainder ? 1 : 0);

                // Add some randomness (±20% of base size, but ensure minimum size of 1)
                int maxVariance = Math.max(1, baseSize / 5);
                int variance = random.nextInt(2 * maxVariance + 1) - maxVariance;
                int actualSize = Math.max(1, targetSize + variance);

                currentPos += actualSize;

                // Ensure we don't exceed the end boundary
                int remaining = inputSize - currentPos;
                int splitsLeft = splitCount - i - 1;
                if (remaining < splitsLeft) {
                    currentPos = inputSize - splitsLeft + 1;
                }

                boundaries.add(Math.min(currentPos, inputSize));
            }

            return boundaries;
        }

        /**
         * Partitions an input list into multiple sublists based on the provided split sizes.
         * This method creates continuation snapshots and their corresponding expected results
         * for testing pagination scenarios with random partition sizes.
         *
         * <p>The method ensures that:
         * <ul>
         *   <li>At least one partition is created (minimum numberOfPartitions = 1)</li>
         *   <li>No more partitions than input elements are created</li>
         *   <li>The last partition gets a continuation value of -1 (indicating end)</li>
         *   <li>Each partition contains consecutive elements from the input</li>
         * </ul>
         *
         * @param input The list of Long values to be partitioned
         * @param splits The list of partition sizes to use for splitting
         * @return A pair containing:
         *         <ul>
         *           <li>Left: List of continuation snapshots (integers indicating next start position, -1 for end)</li>
         *           <li>Right: List of sublists representing the partitioned data</li>
         *         </ul>
         */
        @Nonnull
        public static Pair<List<Integer>, List<List<Long>>> randomPartition(@Nonnull final List<Long> input,
                                                                             @Nonnull final List<Integer> splits) {
            Verify.verify(splits.size() < input.size());
            var numberOfPartitions = splits.size();
            numberOfPartitions = Math.min(numberOfPartitions, input.size());
            numberOfPartitions = Math.max(numberOfPartitions, 1);
            if (numberOfPartitions == 1) {
                return NonnullPair.of(ImmutableList.of(-1), ImmutableList.of(ImmutableList.copyOf(input)));
            }
            final var left = ImmutableList.<Integer>builder();
            final var right = ImmutableList.<List<Long>>builder();


            int l = 0;
            for (int i = 0; i < numberOfPartitions; i++) {
                int r = splits.get(i);
                left.add(r - l);
                right.add(ImmutableList.copyOf(input.subList(l, r)));
                l = r;
            }
            return NonnullPair.of(left.build(), right.build());
        }
    }

    static final class Hierarchy {

        private static final long ROOT = 1;

        private static final long SENTINEL = -1;

        @Nonnull
        private final Map<Long, Long> edges;

        @Nonnull
        private final Supplier<Multimap<Long, Long>> reverseLookup;

        private Hierarchy(@Nonnull final Map<Long, Long> edges) {
            this.edges = edges;
            this.reverseLookup = Suppliers.memoize(this::calculateReverseLookup);
        }

        @Nonnull
        public List<Long> calculateAncestors(long vertex) {
            Verify.verify(vertex >= ROOT);
            final var result = ImmutableList.<Long>builder();

            var current = vertex;

            do {
                result.add(current);
                current = edges.get(current);
            } while (current != SENTINEL);

            return result.build();
        }

        public long getRandomLeaf() {
            final var leafNodes = new ArrayList<>(Sets.difference(edges.keySet(), reverseLookup.get().keySet()));
            final var index = random.nextInt(leafNodes.size());
            return leafNodes.get(index);
        }

        /**
         * Calculates all descendants of the ROOT vertex in level-order (breadth-first) traversal.
         * This is a convenience method that calls {@link #calculateDescendantsLevelOrder(long)} with ROOT.
         *
         * @return A list of all descendants from ROOT in level-order traversal.
         */
        @Nonnull
        public List<Long> calculateDescendants(@Nonnull final RecursiveUnionExpression.TraversalStrategy traversalStrategy) {
            return calculateDescendants(ROOT, traversalStrategy);
        }

        /**
         * Calculates all descendants of a given vertex in the specified traversal order.
         * <br>
         * For example, given the hierarchy:
         * <pre>
         * {@code
         *         1
         *       /   \
         *      10    20
         *    / | \   / \
         *   40 50 70 100 210
         *      |
         *     250
         * }
         * </pre>
         * Starting from vertex 1:
         * - LEVEL traversal would return: [1, 10, 20, 40, 50, 70, 100, 210, 250]
         * - PREORDER traversal would return: [1, 10, 40, 50, 250, 70, 20, 100, 210]
         * - POSTORDER traversal would return: [40, 250, 50, 70, 10, 100, 210, 20, 1]
         * - ANY traversal returns pre-order because the optimizer always prefers pre-order when given ANY order
         *
         * @param vertex The starting vertex to find descendants from. Must be >= ROOT.
         * @param traversalStrategy The traversal order to use (LEVEL, PREORDER, POSTORDER, or ANY).
         * @return A list of all descendants (including the starting vertex) in the specified traversal order.
         */
        @Nonnull
        public List<Long> calculateDescendants(long vertex, @Nonnull final RecursiveUnionExpression.TraversalStrategy traversalStrategy) {
            switch (traversalStrategy) {
                case LEVEL:
                    return calculateDescendantsLevelOrder(vertex);
                case PREORDER:
                case ANY: // Optimizer always prefers pre-order when given ANY order
                    Verify.verify(vertex >= ROOT);
                    final var preResult = ImmutableList.<Long>builder();
                    calculateDescendantsPreOrder(vertex, preResult);
                    return preResult.build();
                case POSTORDER:
                    Verify.verify(vertex >= ROOT);
                    final var postResult = ImmutableList.<Long>builder();
                    calculateDescendantsPostOrder(vertex, postResult);
                    return postResult.build();
                default:
                    throw new IllegalArgumentException("Unsupported traversal type: " + traversalStrategy);
            }
        }

        /**
         * Calculates all descendants of a given vertex in level-order (breadth-first) traversal.
         * This method visits all nodes at depth 0, then all nodes at depth 1, then all nodes at depth 2, etc.
         * Uses a queue-based iterative approach for breadth-first traversal.
         * <br>
         * For example, given the hierarchy:
         * <pre>
         * {@code
         *         1
         *       /   \
         *      10    20
         *    / | \   / \
         *   40 50 70 100 210
         *      |
         *     250
         * }
         * </pre>
         * Starting from vertex 1, this method would return: [1, 10, 20, 40, 50, 70, 100, 210, 250]
         * <br>
         * @param vertex The starting vertex to find descendants from. Must be >= ROOT.
         * @return A list of all descendants (including the starting vertex) in level-order traversal.
         */
        @Nonnull
        public List<Long> calculateDescendantsLevelOrder(long vertex) {
            Verify.verify(vertex >= ROOT);
            final var result = ImmutableList.<Long>builder();
            final var reverseLookup = reverseLookup();

            final Queue<Long> level = new LinkedList<>();
            level.add(vertex);
            while (!level.isEmpty()) {
                final var current = level.poll();
                result.add(current);
                level.addAll(reverseLookup.get(current));
            }
            return result.build();
        }

        /**
         * Helper method for pre-order (depth-first) traversal of descendants.
         * This method visits the current vertex first, then recursively visits all of its children from left to right.
         * Uses a recursive approach for depth-first traversal.
         * <br>
         * For example, given the hierarchy:
         * <pre>
         * {@code
         *         1
         *       /   \
         *      10    20
         *    / | \   / \
         *   40 50 70 100 210
         *      |
         *     250
         * }
         * </pre>
         * Starting from vertex 1, this method would visit nodes in this order: [1, 10, 40, 50, 250, 70, 20, 100, 210]
         * <br>
         * @param vertex The current vertex being visited.
         * @param result The builder to accumulate the traversal results.
         */
        private void calculateDescendantsPreOrder(long vertex,
                                                  @Nonnull final ImmutableList.Builder<Long> result) {
            result.add(vertex);
            final var children = reverseLookup.get().get(vertex);
            for (final var child : children) {
                calculateDescendantsPreOrder(child, result);
            }
        }

        /**
         * Helper method for post-order (depth-first) traversal of descendants.
         * This method recursively visits all of its children from left to right first, then visits the current vertex.
         * Uses a recursive approach for depth-first traversal where children are processed before their parent.
         * <br>
         * For example, given the hierarchy:
         * <pre>
         * {@code
         *         1
         *       /   \
         *      10    20
         *    / | \   / \
         *   40 50 70 100 210
         *      |
         *     250
         * }
         * </pre>
         * Starting from vertex 1, this method would visit nodes in this order: [40, 250, 50, 70, 10, 100, 210, 20, 1]
         * <br>
         * @param vertex The current vertex being visited.
         * @param result The builder to accumulate the traversal results.
         */
        private void calculateDescendantsPostOrder(long vertex,
                                                   @Nonnull final ImmutableList.Builder<Long> result) {
            final var children = reverseLookup.get().get(vertex);
            for (final var child : children) {
                calculateDescendantsPostOrder(child, result);
            }
            result.add(vertex);
        }

        @Nonnull
        public Multimap<Long, Long> reverseLookup() {
            return reverseLookup.get();
        }

        @Nonnull
        private Multimap<Long, Long> calculateReverseLookup() {
            final var reverseLookup = ArrayListMultimap.<Long, Long>create();
            for (final var entry : edges.entrySet()) {
                reverseLookup.put(entry.getValue(), entry.getKey());
            }
            return reverseLookup;
        }

        @Nonnull
        public Map<Long, Long> getEdges() {
            return edges;
        }

        @Nonnull
        public static Hierarchy fromEdges(@Nonnull final Map<Long, Long> edges) {
            return new Hierarchy(edges);
        }

        public static double generateWithVariance(double x, double variancePercent) {
            // Calculate the variance range
            double variance = x * (variancePercent / 100.0);

            // Generate random value between -variance and +variance
            double randomVariance = (random.nextDouble() * 2 - 1) * variance;

            return x + randomVariance;
        }

        public static int generateWithPercentVariance(int x, int percentage) {
            Verify.verify(percentage >= 0 && percentage <= 100);
            double result = generateWithVariance(x, percentage);
            return (int) Math.round(result);
        }

        @Nonnull
        public static Hierarchy generateRandomHierarchy(int maxChildrenCountPerLevel, int maxDepth, int parentsCount) {
            var result = new LinkedHashMap<Long, Long>();
            result.put(ROOT, SENTINEL);
            long firstItemCurrentLevel = 1;
            long firstItemNextLevel = 2;
            for (int i = 0; i < maxDepth; i++) {
                final var listOfItems = new ArrayList<Long>(maxChildrenCountPerLevel);
                int j = 0;
                while (j < maxChildrenCountPerLevel) {
                    listOfItems.add(firstItemNextLevel + j++);
                }
                var numSplits = generateWithPercentVariance(parentsCount, 20);
                if (numSplits >= maxChildrenCountPerLevel) {
                    numSplits = maxChildrenCountPerLevel - 1;
                }
                final var splits = ListPartitioner.getSplitsUsingPowerDistribution(numSplits, listOfItems.size());
                final var levelPartitions = ListPartitioner.randomPartition(listOfItems, splits);
                int newLevelSize = levelPartitions.getValue().stream().map(List::size).reduce(0, Integer::sum);
                for (int partition = 0; partition < levelPartitions.getValue().size(); partition++) {
                    final var currentPartition = levelPartitions.getValue().get(partition);
                    for (int k = 0; k < currentPartition.size(); k++) {
                        result.put(currentPartition.get(k), (long)partition + firstItemCurrentLevel);
                    }
                }
                firstItemCurrentLevel = firstItemNextLevel;
                firstItemNextLevel = firstItemNextLevel + newLevelSize;
            }
            return Hierarchy.fromEdges(result);
        }

        public int size() {
            final var vertices = ImmutableSet.<Long>builder();
            for (final var edge : edges.entrySet()) {
                vertices.add(edge.getKey(), edge.getKey());
            }
            return vertices.build().size();
        }

        public void print() {
            print(1, ROOT, reverseLookup());
        }

        private void print(int level, long value, @Nonnull final Multimap<Long, Long> reverseLookup) {
            for (int i = 0; i < level; i++) {
                System.out.print("\t");
            }
            System.out.println(value);
            final var children = reverseLookup.get(value);
            for (final var child : children) {
                print(level + 1, child, reverseLookup);
            }
        }
    }
}

