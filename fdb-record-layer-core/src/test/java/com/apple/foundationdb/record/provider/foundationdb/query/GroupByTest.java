/*
 * GroupByTest.java
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
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue.ofScalar;

/**
 * test suite for {@code GROUP BY} expression planning and execution.
 */
@Tag(Tags.RequiresFDB)
public class GroupByTest extends FDBRecordStoreQueryTestBase {

    /**
     * message MySimpleRecord {
     *   optional int64 rec_no = 1 [(field).primary_key = true];
     *   optional string str_value_indexed = 2 [(field).index = {}];
     *   optional int32 num_value_unique = 3 [(field).index = { unique: true }];
     *   optional int32 num_value_2 = 4;
     *   optional int32 num_value_3_indexed = 5 [(field).index = {}];
     *   repeated int32 repeater = 6;
     * }
     */

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimpleGroupBy() throws Exception {
        setupHookAndAddData();
        final var cascadesPlanner = (CascadesPlanner)planner;
        cascadesPlanner.planGraph(
                () -> {
                    final var allRecordTypes =
                            ImmutableSet.of("MySimpleRecord");
                    var qun =
                            Quantifier.forEach(GroupExpressionRef.of(
                                    new FullUnorderedScanExpression(allRecordTypes,
                                            Type.Record.fromFieldDescriptorsMap(cascadesPlanner.getRecordMetaData().getFieldDescriptorMapFromNames(allRecordTypes)),
                                            new AccessHints())));

                    qun = Quantifier.forEach(GroupExpressionRef.of(
                            new LogicalTypeFilterExpression(ImmutableSet.of("MySimpleRecord"),
                                    qun,
                                    Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()))));

                    final var selectBuilder = GraphExpansion.builder();
                    final var num2Value = new FieldValue(qun.getFlowedObjectValue(), ImmutableList.of("num_value_2"));
                    final var num3Value = new FieldValue(qun.getFlowedObjectValue(), ImmutableList.of("num_value_3_indexed"));
                    selectBuilder.addQuantifier(qun)
                            .addResultValue(RecordConstructorValue.ofColumns(List.of(
                                    Column.of(Type.Record.Field.of(num2Value.getResultType(), Optional.of("num_value_2")), num2Value),
                                    Column.of(Type.Record.Field.of(num3Value.getResultType(), Optional.of("num_value_3_indexed")), num3Value))));
                    qun = Quantifier.forEach(GroupExpressionRef.of(selectBuilder.build().buildSelect()));

                    final var groupingCol = Column.of(Type.Record.Field.of(num2Value.getResultType(), Optional.of("num_value_2")), num2Value);
                    final var aggCol = Column.of(Type.Record.Field.unnamedOf(Type.primitiveType(Type.TypeCode.LONG)),
                            new NumericAggregationValue(NumericAggregationValue.PhysicalOperator.SUM_I, new FieldValue(qun.getFlowedObjectValue(), ImmutableList.of("num_value_3_indexed"))));
                    final var groupingCols = RecordConstructorValue.ofColumns(List.of(groupingCol));
                    final var aggregationCols = RecordConstructorValue.ofColumns(List.of(aggCol));
                    final var resultValue = RecordConstructorValue.ofColumns(List.of(aggCol, groupingCol));
                    final var groupByExpression = new GroupByExpression(groupingCols, aggregationCols, resultValue, qun);
                    qun = Quantifier.forEach(GroupExpressionRef.of(groupByExpression));

                    final var topSelectBuilder = GraphExpansion.builder();
                    final var groupByQuantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
                    topSelectBuilder.addQuantifier(qun).addResultValue(groupByQuantifiedValue);
                    qun = Quantifier.forEach(GroupExpressionRef.of(topSelectBuilder.build().buildSelect()));
                    return GroupExpressionRef.of(new LogicalSortExpression(null, false, qun));
                },
                Optional.of(ImmutableSet.of("RestaurantRecord")),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());
    }

    protected void setupHookAndAddData() throws Exception {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStoreTestBase.RecordMetaDataHook hook = (metaDataBuilder) -> {
                complexQuerySetupHook().apply(metaDataBuilder);
                metaDataBuilder.addIndex("MySimpleRecord", "MySimpleRecord$num_value_2", field("num_value_2"));
            };
            openSimpleRecordStore(context, hook);
            var rec = TestRecords1Proto.MySimpleRecord.newBuilder();
            rec.setRecNo(1).setStrValueIndexed("1").setNumValueUnique(1).setNumValue2(1).setNumValue3Indexed(10); recordStore.saveRecord(rec.build());
            rec.setRecNo(2).setStrValueIndexed("2").setNumValueUnique(2).setNumValue2(1).setNumValue3Indexed(20); recordStore.saveRecord(rec.build());
            rec.setRecNo(3).setStrValueIndexed("3").setNumValueUnique(3).setNumValue2(1).setNumValue3Indexed(30); recordStore.saveRecord(rec.build());
            rec.setRecNo(4).setStrValueIndexed("4").setNumValueUnique(4).setNumValue2(2).setNumValue3Indexed(5); recordStore.saveRecord(rec.build());
            rec.setRecNo(5).setStrValueIndexed("5").setNumValueUnique(5).setNumValue2(2).setNumValue3Indexed(5); recordStore.saveRecord(rec.build());
            rec.setRecNo(6).setStrValueIndexed("6").setNumValueUnique(6).setNumValue2(2).setNumValue3Indexed(5); recordStore.saveRecord(rec.build());
            rec.setRecNo(7).setStrValueIndexed("7").setNumValueUnique(7).setNumValue2(3).setNumValue3Indexed(-10); recordStore.saveRecord(rec.build());
            rec.setRecNo(8).setStrValueIndexed("8").setNumValueUnique(8).setNumValue2(3).setNumValue3Indexed(-20); recordStore.saveRecord(rec.build());
            rec.setRecNo(9).setStrValueIndexed("9").setNumValueUnique(9).setNumValue2(3).setNumValue3Indexed(-30); recordStore.saveRecord(rec.build());
            rec.setRecNo(10).setStrValueIndexed("10").setNumValueUnique(10).setNumValue2(4).setNumValue3Indexed(100); recordStore.saveRecord(rec.build());
            rec.setRecNo(11).setStrValueIndexed("11").setNumValueUnique(11).setNumValue2(4).setNumValue3Indexed(2000); recordStore.saveRecord(rec.build());
            commit(context);
        }
    }
}
