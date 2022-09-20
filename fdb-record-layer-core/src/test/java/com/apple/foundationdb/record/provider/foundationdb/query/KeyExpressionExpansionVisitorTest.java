/*
 * KeyExpressionExpansionVisitorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Tests for {@link com.apple.foundationdb.record.query.plan.cascades.KeyExpressionExpansionVisitor}.
 */
public class KeyExpressionExpansionVisitorTest extends FDBRecordStoreQueryTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void generateMatchIndexFromGroupingKeyExpression() {
        setupHookAndAddData(true);
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                this::constructSimpleSelect,
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());
        Assertions.assertNotNull(plan);
    }

    @Nonnull
    private GroupExpressionRef<RelationalExpression> constructSimpleSelect() {
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var allRecordTypes = ImmutableSet.of("MySimpleRecord", "MyOtherRecord");
        var qun = Quantifier.forEach(GroupExpressionRef.of(new FullUnorderedScanExpression(allRecordTypes,
                Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)), new AccessHints())));
        qun = Quantifier.forEach(GroupExpressionRef.of(new LogicalTypeFilterExpression(ImmutableSet.of("MySimpleRecord"), qun, Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()))));
        final var num3Value = FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_3_indexed");
        final var result = GraphExpansion.builder().addQuantifier(qun).addAllResultColumns(ImmutableList.of(Column.unnamedOf(num3Value))).build().buildSelect();
        return GroupExpressionRef.of(result);
    }

    @Nonnull
    private GroupExpressionRef<RelationalExpression> constructGroupByPlan() {
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var allRecordTypes = ImmutableSet.of("MySimpleRecord", "MyOtherRecord");
        var qun =
                Quantifier.forEach(GroupExpressionRef.of(
                        new FullUnorderedScanExpression(allRecordTypes,
                                Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                new AccessHints())));

        qun = Quantifier.forEach(GroupExpressionRef.of(
                new LogicalTypeFilterExpression(ImmutableSet.of("MySimpleRecord"),
                        qun,
                        Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()))));

        final var num2Value = FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_2");

        final var scanAlias = qun.getAlias();
        final var groupByColAlias = CorrelationIdentifier.of("select_grouping_cols");
        final var groupingCol = Column.of(Type.Record.Field.of(num2Value.getResultType(), Optional.of("num_value_2")), num2Value);
        final var groupingColGroup = RecordConstructorValue.ofColumns(ImmutableList.of(groupingCol));

        // 1. build the underlying select, result expr = ( (num_value_2 as GB) <group1>, ($qun as qun) <group2>)
        {
            final var selectBuilder = GraphExpansion.builder();

            // <group1>
            final var col1 = Column.of(Type.Record.Field.of(groupingColGroup.getResultType(), Optional.of(groupByColAlias.getId())), groupingColGroup);

            // <group2>
            final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
            final var col2 = Column.of(Type.Record.Field.of(quantifiedValue.getResultType(), Optional.of(qun.getAlias().getId())), quantifiedValue);

            selectBuilder.addQuantifier(qun).addAllResultColumns(List.of(col1, col2));
            qun = Quantifier.forEach(GroupExpressionRef.of(selectBuilder.build().buildSelect()));
        }

        CorrelationIdentifier groupingExprAlias;

        // 2. build the group by expression, for that we need the aggregation expression and the grouping expression.
        {
            // 2.1. construct aggregate expression.
            final var aggCol = Column.of(Type.Record.Field.unnamedOf(Type.primitiveType(Type.TypeCode.LONG)),
                    new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.of(scanAlias.getId(), "num_value_3_indexed"))));
            final var aggregationExpr = RecordConstructorValue.ofColumns(ImmutableList.of(aggCol));

            // 2.2. construct grouping columns expression.
            final var groupingExpr = FieldValue.ofFieldName(qun.getFlowedObjectValue(), groupByColAlias.getId());

            // 2.3. construct the group by expression
            final var groupByExpression = new GroupByExpression(aggregationExpr, groupingExpr, qun);
            groupingExprAlias = groupByExpression.getGroupingValueAlias();
            qun = Quantifier.forEach(GroupExpressionRef.of(groupByExpression));
        }

        // 3. construct the select expression on top containing the final result set
        {
            // construct a result set that makes sense.
            final var numValue2Reference = Column.of(Type.Record.Field.of(num2Value.getResultType(), Optional.of("num_value_2")),
                    FieldValue.ofFieldNames(QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), ImmutableList.of(groupingExprAlias.getId(), "num_value_2")));
            final var aggregateReference = Column.unnamedOf(FieldValue.ofOrdinalNumber(FieldValue.ofOrdinalNumber(ObjectValue.of(qun.getAlias(), qun.getFlowedObjectType()), 0), 0));

            final var result = GraphExpansion.builder().addQuantifier(qun).addAllResultColumns(ImmutableList.of(numValue2Reference,  aggregateReference)).build().buildSelect();
            return GroupExpressionRef.of(result);
        }
    }

    protected void setupHookAndAddData(boolean addIndex) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStoreTestBase.RecordMetaDataHook hook = (metaDataBuilder) -> {
                complexQuerySetupHook().apply(metaDataBuilder);
                if (addIndex) {
                    metaDataBuilder.addIndex("MySimpleRecord", new Index("sumIdx", field("rec_no").groupBy(field("num_value_3_indexed")), IndexTypes.SUM));
                }
                metaDataBuilder.removeIndex(COUNT_INDEX.getName());
            };
            openSimpleRecordStore(context, hook);
            var rec = TestRecords1Proto.MySimpleRecord.newBuilder();
            /*
                 0 -> 0
                 1 -> 1
                 2 -> 2
                 3 -> 3
                 4 -> 4
                 5 -> 0
                 6 -> 1
                 7 -> 2
                 8 -> 3
                 9 -> 4
                 0 -> {0, 5} = 5
                 1 -> {1, 6} = 7
                 2 -> {2, 7} = 9
                 3 -> {3, 8} = 11
                 4 -> {4, 9} = 13
            */
            for (int i = 0; i < 10; ++i) {
                rec.setRecNo(i).setNumValue3Indexed(i % 5);
                recordStore.saveRecord(rec.build());
            }
            commit(context);
        }
    }
}
