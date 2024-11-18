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
import com.apple.foundationdb.record.TestRecords7Proto;
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
     * @return The execution results of the {@code plan} mapped to a pair of the {@code rec_no} and {@code str_value}.
     * @throws Exception If the execution fails.
     */
    @Nonnull
    Set<Pair<Long, String>> collectResults(@Nonnull final FDBRecordContext context,
                                           @Nonnull final RecordQueryPlan plan,
                                           @Nonnull final TempTable<QueryResult> inputTempTable,
                                           @Nonnull final CorrelationIdentifier tempTableId) throws Exception {
        final ImmutableMap.Builder<String, Object> constants = ImmutableMap.builder();
        constants.put(tempTableId.getId(), inputTempTable);
        final var evaluationContext = EvaluationContext.empty().withBinding(Bindings.Internal.CONSTANT, tempTableId, constants.build());
        return extractResultsAsPairs(context, plan, evaluationContext);
    }

    /**
     * Utility method that executes a {@code plan} returning its results.
     * @param context The transaction used to execute the plan.
     * @param plan The plan to be executed.
     * @param evaluationContext The evaluation context.
     * @return The execution results of the {@code plan} mapped to a pair of the {@code rec_no} and {@code str_value}.
     * @throws Exception If the execution fails.
     */
    @Nonnull
    Set<Pair<Long, String>> extractResultsAsPairs(@Nonnull final FDBRecordContext context,
                                                  @Nonnull final RecordQueryPlan plan,
                                                  @Nonnull final EvaluationContext evaluationContext) throws Exception {
        ImmutableSet.Builder<Pair<Long, String>> resultBuilder = ImmutableSet.builder();
        fetchResultValues(context, plan, record -> {
            resultBuilder.add(asPair(record));
            return record;
        }, evaluationContext, c -> {
        }, ExecuteProperties.newBuilder().build());
        return resultBuilder.build();
    }

    @Nonnull
    static Pair<Long, String> asPair(@Nonnull final Message message) {
        final var descriptor = message.getDescriptorForType();
//       return Pair.of((Long)message.getField(descriptor.findFieldByName("rec_no")), "");
        return Pair.of((Long)message.getField(descriptor.findFieldByName("rec_no")),
                (String)message.getField(descriptor.findFieldByName("str_value")));
    }

    @Nonnull
    static QueryResult queryResult(long recNo, @Nonnull final String strValue) {
        return QueryResult.ofComputed(item(recNo, strValue));
    }

    @Nonnull
    static QueryResult queryResult(long recNo) {
        return queryResult(recNo, recNo + "Value");
    }

    @Nonnull
    static Message item(long recNo, @Nonnull final String strValue) {
        return TestRecords7Proto.MyRecord1.newBuilder()
                .setRecNo(recNo)
                .setStrValue(strValue)
                .build();
    }

    @Nonnull
    static RecordConstructorValue rcv(long recNo, @Nonnull final String strValue) {
        return RecordConstructorValue.ofColumns(
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG),
                                Optional.of("rec_no")), LiteralValue.ofScalar(recNo)),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING),
                                Optional.of("str_value")), LiteralValue.ofScalar(strValue))));
    }

    @Nonnull
    RecordQueryPlan createAndOptimizeTempTableScanPlan(@Nonnull final CorrelationIdentifier tempTableId) {
        final var type = Type.Record.fromDescriptor(TestRecords7Proto.MyRecord1.getDescriptor());
        final var tempTableScanQun = Quantifier.forEach(Reference.of(TempTableScanExpression.ofConstant(tempTableId, tempTableId.getId(), type)));
        final var recNoField = getRecNoCol(tempTableScanQun);
        final var strValueIndexedField = getStrValueCol(tempTableScanQun);
        final var selectExpressionBuilder = GraphExpansion.builder()
                .addAllResultColumns(ImmutableList.of(recNoField, strValueIndexedField))
                .addQuantifier(tempTableScanQun);
        final var logicalPlan = Reference.of(LogicalSortExpression.unsorted(Quantifier.forEach(Reference.of(selectExpressionBuilder.build().buildSelect()))));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(() -> logicalPlan, Optional.empty(), IndexQueryabilityFilter.TRUE, EvaluationContext.empty()).getPlan();
        assertMatchesExactly(plan, mapPlan(tempTableScanPlan()));
        return plan;
    }

    static void addSampleDataToTempTable(@Nonnull final TempTable<QueryResult> tempTable) {
        tempTable.add(queryResult(42L, "fortySecondValue"));
        tempTable.add(queryResult(45L, "fortyFifthValue"));
    }

    @Nonnull
    static EvaluationContext putTempTableInContext(@Nonnull final CorrelationIdentifier tempTableAlias,
                                                   @Nonnull final TempTable<QueryResult> tempTable,
                                                   @Nullable EvaluationContext parentContext) {
        final var actualParentContext = parentContext == null ? EvaluationContext.empty() : parentContext;
        final ImmutableMap.Builder<String, Object> constants = ImmutableMap.builder();
        constants.put(tempTableAlias.getId(), tempTable);
        return actualParentContext.withBinding(Bindings.Internal.CONSTANT, tempTableAlias, constants.build());
    }

    @Nonnull
    static EvaluationContext setUpPlanContext(@Nonnull RecordQueryPlan recordQueryPlan,
                                              @Nonnull final CorrelationIdentifier tempTableAlias,
                                              @Nonnull final TempTable<QueryResult> tempTable) {
        final var usedTypes = UsedTypesProperty.evaluate(recordQueryPlan);
        final var evaluationContext = EvaluationContext.forTypeRepository(TypeRepository.newBuilder().addAllTypes(usedTypes).build());
        return putTempTableInContext(tempTableAlias, tempTable, evaluationContext);
    }

    @Nonnull
    static FieldValue getRecNoField(@Nonnull final Quantifier.ForEach quantifier) {
        return FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "rec_no");
    }

    @Nonnull
    static Column<Value> getRecNoCol(@Nonnull final Quantifier.ForEach quantifier) {
        return Column.of(Optional.of("rec_no"), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "rec_no"));
    }

    @Nonnull
    static Column<Value> getStrValueCol(@Nonnull final Quantifier.ForEach quantifier) {
        return Column.of(Optional.of("str_value"), FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), "str_value"));
    }

    @Nonnull
    static Type getType() {
        return Type.Record.fromDescriptor(getDescriptor());
    }

    @Nonnull
    static Descriptors.Descriptor getDescriptor() {
        return TestRecords7Proto.MyRecord1.getDescriptor();
    }
}
