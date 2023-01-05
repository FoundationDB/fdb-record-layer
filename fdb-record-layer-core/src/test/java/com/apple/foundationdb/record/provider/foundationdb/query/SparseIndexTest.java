/*
 * SparseIndexConstructionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProto;
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.aggregations;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.groupings;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.streamingAggregationPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.sumAggregationValue;

/**
 * Tests filtered (sparse) index construction.
 */
@Tag(Tags.RequiresFDB)
public class SparseIndexTest extends FDBRecordStoreQueryTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void testSimpleGroupBy() throws Exception {
        setupHookAndAddData();
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = cascadesPlanner.planGraph(
                this::constructGroupByPlan,
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());

        assertMatchesExactly(plan,
                mapPlan(
                        streamingAggregationPlan(
                                mapPlan(
                                        indexPlan()
                                                .where(scanComparisons(range("<,>")))
                                )).where(aggregations(recordConstructorValue(exactly(sumAggregationValue())))
                                .and(groupings(ValueMatchers.fieldValueWithFieldNames("select_grouping_cols"))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void attemptToPlanGroupByWithoutCompatiblySortedIndexFails() throws Exception {
        setupHookAndAddData();
        final var cascadesPlanner = (CascadesPlanner)planner;
        Assertions.assertThrows(RecordCoreException.class, () -> cascadesPlanner.planGraph(
                this::constructGroupByPlan,
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty()), "Cascades planner could not plan query");
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
                    new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_I, FieldValue.ofFieldNames(qun.getFlowedObjectValue(), ImmutableList.of(scanAlias.getId(), "num_value_3_indexed"))));
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
            qun = Quantifier.forEach(GroupExpressionRef.of(result));
            return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
        }
    }

    protected void setupHookAndAddData() throws Exception {

        /**
         message MySimpleRecord {
           optional int64 rec_no = 1 [(field).primary_key = true];
           optional string str_value_indexed = 2 [(field).index = {}];
           optional int32 num_value_unique = 3 [(field).index = { unique: true }];
           optional int32 num_value_2 = 4;
           optional int32 num_value_3_indexed = 5 [(field).index = {}];
           repeated int32 repeater = 6;
         }

         index: (num_value_2, num_value_3_indexed) where num_value_2 > 42

         */


        try (FDBRecordContext context = openContext()) {
            FDBRecordStoreTestBase.RecordMetaDataHook hook = (metaDataBuilder) -> {
                complexQuerySetupHook().apply(metaDataBuilder);

                final var typeRepositoryBuilder = TypeRepository.newBuilder();

                final var recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
                final var predicate = (Value.SerializableValue)new RelOpValue.GtFn().encapsulate(typeRepositoryBuilder, List.of(
                        FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "num_value_2"),
                        LiteralValue.ofScalar(42)
                ));

                final var protoIndexBuilder = RecordMetaDataProto.Index.newBuilder()
                        .setName("SparseIndex")
                        .addRecordType("MySimpleRecord")
                        .setType(IndexTypes.VALUE)
                        .setRootExpression(concat(field("num_value_2"), field("num_value_3_indexed")).toKeyExpression())
                        .setPredicate(predicate.toProto())
                        .build();
                final var index = new Index(protoIndexBuilder);
                metaDataBuilder.addIndex("MySimpleRecord", index);
            };
            openSimpleRecordStore(context, hook);
            var rec = TestRecords1Proto.MySimpleRecord.newBuilder();

            // (hatyo) assume all indexed records satisfy the predicate, because this test does not concern itself with
            //         index maintenance logic.
            rec.setRecNo(1).setStrValueIndexed("1").setNumValueUnique(1).setNumValue2(43).setNumValue3Indexed(10);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(2).setStrValueIndexed("2").setNumValueUnique(2).setNumValue2(44).setNumValue3Indexed(20);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(3).setStrValueIndexed("3").setNumValueUnique(3).setNumValue2(45).setNumValue3Indexed(30);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(4).setStrValueIndexed("4").setNumValueUnique(4).setNumValue2(46).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(5).setStrValueIndexed("5").setNumValueUnique(5).setNumValue2(47).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(6).setStrValueIndexed("6").setNumValueUnique(6).setNumValue2(48).setNumValue3Indexed(5);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(7).setStrValueIndexed("7").setNumValueUnique(7).setNumValue2(49).setNumValue3Indexed(-10);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(8).setStrValueIndexed("8").setNumValueUnique(8).setNumValue2(50).setNumValue3Indexed(-20);
            recordStore.saveRecord(rec.build());
            rec.setRecNo(9).setStrValueIndexed("9").setNumValueUnique(9).setNumValue2(51).setNumValue3Indexed(-30);
            recordStore.saveRecord(rec.build());
            commit(context);
        }
    }

}
