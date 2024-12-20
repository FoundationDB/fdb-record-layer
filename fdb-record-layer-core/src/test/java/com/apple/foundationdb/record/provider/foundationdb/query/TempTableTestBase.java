/*
 * TempTableTestBase.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.TempTableScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
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
import java.util.function.Function;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableScanPlan;

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
        final var tempTableScanQun = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(tempTableId, tempTableId.getId(), getTempTableType())));
        final var selectExpressionBuilder = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(getIdCol(tempTableScanQun), getValueCol(tempTableScanQun)))
                .addQuantifier(tempTableScanQun);
        final var logicalPlan = Reference.of(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.of(selectExpressionBuilder.build().buildSelect()))));
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
        final ImmutableMap.Builder<String, Object> constants = ImmutableMap.builder();
        constants.put(tempTableAlias.getId(), tempTable);
        return actualParentContext.withBinding(Bindings.Internal.CONSTANT, tempTableAlias, constants.build());
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
        final var usedTypes = UsedTypesProperty.evaluate(recordQueryPlan);
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
         * Partitions a list of numbers into a number of sub-lists defined, each sub-list start and end is defined
         * pseudo-randomly according to a random size chosen with a uniform probability distribution.
         * @param numberOfPartitions The number of partitions.
         * @param input The input list.
         * @return A number of sub-lists, each sub-list start and end is defined pseudo-randomly according to a random
         * size chosen with a uniform probability distribution
         */
        @Nonnull
        public static Pair<List<Integer>, List<List<Long>>> partitionUsingNormalDistribution(int numberOfPartitions,
                                                                                          @Nonnull final List<Long> input) {
            return randomPartition(numberOfPartitions, input, random::nextInt);
        }

        /**
         * Partitions a list of numbers into a number of sub-lists defined, each sub-list start and end is defined
         * pseudo-randomly according to a random size chosen with a power probability distribution.
         * @param numberOfPartitions The number of partitions.
         * @param input The input list.
         * @return A number of sub-lists, each sub-list start and end is defined pseudo-randomly according to a random
         * size chosen with a power probability distribution
         */
        @Nonnull
        public static Pair<List<Integer>, List<List<Long>>> partitionUsingPowerDistribution(int numberOfPartitions,
                                                                                         @Nonnull final List<Long> input) {
            return randomPartition(numberOfPartitions, input, ListPartitioner::nextIntWithPowerDistribution);
        }

        @Nonnull
        private static Pair<List<Integer>, List<List<Long>>> randomPartition(int numberOfPartitions,
                                                                                @Nonnull final List<Long> input,
                                                                                @Nonnull final Function<Integer, Integer> randomGenerator) {
            numberOfPartitions = Math.min(numberOfPartitions, input.size());
            numberOfPartitions = Math.max(numberOfPartitions, 1);
            if (numberOfPartitions == 1) {
                return NonnullPair.of(ImmutableList.of(-1), ImmutableList.of(ImmutableList.copyOf(input)));
            }
            final var left = ImmutableList.<Integer>builder();
            final var right = ImmutableList.<List<Long>>builder();

            int size = 0;
            for (int i = 0; i < numberOfPartitions; i++) {
                int partitionSize = size + randomGenerator.apply(input.size());
                if (size + partitionSize >= input.size() || (i == numberOfPartitions - 1 && size + partitionSize < input.size())) {
                    left.add(-1);
                    right.add(input.subList(size, input.size()));
                    break;
                }
                left.add(partitionSize + 1);
                right.add(ImmutableList.copyOf(input.subList(size, partitionSize + 1 + size)));
                size += partitionSize + 1;
            }
            return NonnullPair.of(left.build(), right.build());
        }

        /**
         * Returns a random integer number between [1, {@code upperBound}] according to a power distribution.
         * <br>
         * This transforms the normal distribution as defined in standard Java {@link Random}
         * to power distribution, for more information, see <a href="https://mathworld.wolfram.com/RandomNumber.html">https://mathworld.wolfram.com/RandomNumber.html</a>
         * @param upperBound The upper bound (inclusive), must be larger than 1
         * @return a random integer number between [1, {@code upperBound}] according to a power distribution.
         * <br>
         */
        private static int nextIntWithPowerDistribution(int upperBound) {
            Verify.verify(upperBound > 1);
            double lowerRange = 1.0;
            double upperRange = upperBound * 1.0d;
            double temperature = -2.3;
            double randomValue = random.nextDouble();
            double leftTerm = Math.pow(upperRange, temperature + 1);
            double rightTerm = Math.pow(lowerRange, temperature + 1);
            double exponent = (1.0d / (temperature + 1));
            double base = (leftTerm - rightTerm) * randomValue + rightTerm;
            return (int)Math.round(Math.pow(base, exponent)) - 1;
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

        @Nonnull
        public List<Long> calculateDescendants() {
            return calculateDescendants(ROOT);
        }

        @Nonnull
        public List<Long> calculateDescendants(long vertex) {
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

        @Nonnull
        public static Hierarchy generateRandomHierarchy(int maxChildrenCountPerLevel, int maxDepth) {
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
                final var levelPartitions = ListPartitioner.partitionUsingPowerDistribution((int)(firstItemNextLevel - firstItemCurrentLevel), listOfItems);
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

