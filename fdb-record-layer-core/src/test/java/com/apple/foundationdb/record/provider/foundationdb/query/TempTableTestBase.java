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
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.tempTableScanPlan;

/**
 * Contains utility methods around {@link TempTable}s to make testing temp table planning and execution easier.
 */
public abstract class TempTableTestBase extends FDBRecordStoreQueryTestBase {

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
     * @return The execution results of the {@code plan} mapped to a pair of the {@code id} and {@code value}.
     * @throws Exception If the execution fails.
     */
    @Nonnull
    Set<Pair<Long, String>> collectResults(@Nonnull final FDBRecordContext context,
                                           @Nonnull final RecordQueryPlan plan,
                                           @Nonnull final TempTable inputTempTable,
                                           @Nonnull final CorrelationIdentifier tempTableId) throws Exception {
        final var evaluationContext = putTempTableInContext(tempTableId, inputTempTable, null);
        return extractResultsAsIdValuePairs(context, plan, evaluationContext);
    }

    /**
     * Utility method that executes a {@code plan} returning its results.
     * @param context The transaction used to execute the plan.
     * @param plan The plan to be executed.
     * @param evaluationContext The evaluation context.
     * @return The execution results of the {@code plan} mapped to a pair of the {@code id} and {@code value}.
     * @throws Exception If the execution fails.
     */
    @Nonnull
    Set<Pair<Long, String>> extractResultsAsIdValuePairs(@Nonnull final FDBRecordContext context,
                                                         @Nonnull final RecordQueryPlan plan,
                                                         @Nonnull final EvaluationContext evaluationContext) throws Exception {
        ImmutableSet.Builder<Pair<Long, String>> resultBuilder = ImmutableSet.builder();
        fetchResultValues(context, plan, record -> {
            resultBuilder.add(asIdValue(record));
            return record;
        }, evaluationContext, c -> {
        }, ExecuteProperties.newBuilder().build());
        return resultBuilder.build();
    }

    /**
     * Utility method that executes a {@code plan} returning its results.
     * @param context The transaction used to execute the plan.
     * @param plan The plan to be executed.
     * @param evaluationContext The evaluation context.
     * @return The execution results of the {@code plan} mapped to a pair of the {@code id} and {@code parent}.
     * @throws Exception If the execution fails.
     */
    @Nonnull
    Set<Pair<Long, Long>> extractResultsAsIdParentPairs(@Nonnull final FDBRecordContext context,
                                                         @Nonnull final RecordQueryPlan plan,
                                                         @Nonnull final EvaluationContext evaluationContext) throws Exception {
        ImmutableSet.Builder<Pair<Long, Long>> resultBuilder = ImmutableSet.builder();
        fetchResultValues(context, plan, record -> {
            resultBuilder.add(asIdParent(record));
            return record;
        }, evaluationContext, c -> {
        }, ExecuteProperties.newBuilder().build());
        return resultBuilder.build();
    }

    @Nonnull
    static Pair<Long, String> asIdValue(@Nonnull final Message message) {
        final var descriptor = message.getDescriptorForType();
        return Pair.of((Long)message.getField(descriptor.findFieldByName("id")),
                (String)message.getField(descriptor.findFieldByName("value")));
    }


    @Nonnull
    static Pair<Long, Long> asIdParent(@Nonnull final Message message) {
        final var descriptor = message.getDescriptorForType();
        return Pair.of((Long)message.getField(descriptor.findFieldByName("id")),
                (Long)message.getField(descriptor.findFieldByName("parent")));
    }

    @Nonnull
    static QueryResult queryResult(long id, @Nonnull final String value, long parent) {
        return QueryResult.ofComputed(item(id, value, parent));
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
        return queryResult(id, arrow(id, parent), parent);
    }

    @Nonnull
    static Message item(long id, @Nonnull final String value) {
        return TestHierarchiesProto.SimpleHierarchicalRecord.newBuilder()
                .setId(id)
                .setValue(value)
                .build();
    }

    @Nonnull
    static Message item(long id, @Nonnull final String value, long parent) {
        return TestHierarchiesProto.SimpleHierarchicalRecord.newBuilder()
                .setId(id)
                .setParent(parent)
                .setValue(value)
                .build();
    }

    @Nonnull
    static Message item(long id, long parent) {
        return TestHierarchiesProto.SimpleHierarchicalRecord.newBuilder()
                .setId(id)
                .setParent(parent)
                .setValue(arrow(id, parent))
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
        final var tempTableScanQun = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(tempTableId, tempTableId.getId(), getType())));
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
    static EvaluationContext setUpPlanContext(@Nonnull RecordQueryPlan recordQueryPlan,
                                              @Nonnull final CorrelationIdentifier tempTableAlias,
                                              @Nonnull final TempTable tempTable) {
        final var usedTypes = UsedTypesProperty.evaluate(recordQueryPlan);
        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(usedTypes).build());
        return putTempTableInContext(tempTableAlias, tempTable, evaluationContext);
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
    static Column<Value> getValueCol(@Nonnull final Quantifier.ForEach quantifier) {
        return Column.of(Optional.of("value"), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "value"));
    }

    @Nonnull
    static Column<Value> getParentCol(@Nonnull final Quantifier.ForEach quantifier) {
        return Column.of(Optional.of("parent"), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "parent"));
    }

    @Nonnull
    static Type getType() {
        return Type.Record.fromDescriptor(getDescriptor());
    }

    @Nonnull
    static Type getType(@Nonnull final Quantifier.ForEach forEach) {
        return Objects.requireNonNull(((Type.Relation)forEach.getRangesOver().getResultType()).getInnerType());
    }

    @Nonnull
    static Descriptors.Descriptor getDescriptor() {
        return TestHierarchiesProto.SimpleHierarchicalRecord.getDescriptor();
    }
}
