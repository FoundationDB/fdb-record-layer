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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompileTimeEvaluableRange;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueRangesPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;

/**
 * Tests filtered (sparse) index construction.
 */
@Tag(Tags.RequiresFDB)
public class SparseIndexTest extends FDBRecordStoreQueryTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void sparseIndexIsUsedWhenItsPredicateIsImplied() throws Exception {
        final var compileTimeRange = CompileTimeEvaluableRange.newBuilder();
        compileTimeRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 42));
        final var recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        complexQuerySetup(metaData -> setupIndex(metaData, new ValueRangesPredicate.Sargable(FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "num_value_2"),
                compileTimeRange.build().orElseThrow()).toResidualPredicate()));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = planQuery(cascadesPlanner, 50);
        assertMatchesExactly(plan,
                mapPlan(indexPlan()
                        .where(RecordQueryPlanMatchers.indexName("SparseIndex"))
                        .and(RecordQueryPlanMatchers.indexScanType(IndexScanType.BY_VALUE))
                        .and(scanComparisons(range("([50],>")))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void sparseIndexIsNotUsedWhenItsPredicateIsNotImplied() throws Exception {
        final var compileTimeRange = CompileTimeEvaluableRange.newBuilder();
        compileTimeRange.tryAdd(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 100));
        final var recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        complexQuerySetup(metaData -> setupIndex(metaData, new ValueRangesPredicate.Sargable(FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "num_value_2"),
                compileTimeRange.build().orElseThrow()).toResidualPredicate()));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = planQuery(cascadesPlanner, 50);
        assertMatchesExactly(plan, mapPlan(descendantPlans(scanPlan())));
    }

    private static void setupIndex(@Nonnull final RecordMetaDataBuilder metaData, final QueryPredicate predicate) {

        final QueryPredicate.Serializable normalized = (QueryPredicate.Serializable)BooleanPredicateNormalizer.getDefaultInstanceForDnf().normalize(predicate).orElse(predicate);

        final var protoIndexBuilder = RecordMetaDataProto.Index.newBuilder()
                .setName("SparseIndex")
                .addRecordType("MySimpleRecord")
                .setType(IndexTypes.VALUE)
                .setRootExpression(field("num_value_2").toKeyExpression())
                .setPredicate(normalized.toProto())
                .build();
        final var index = new Index(protoIndexBuilder);
        // index: (num_value_2, num_value_3_indexed) where num_value_2 > <boundary>
        metaData.addIndex("MySimpleRecord", index);
    }

    @Nonnull
    private static GroupExpressionRef<RelationalExpression> constructQueryWithPredicate(@Nonnull final RecordMetaData metadata, int boundary) {
        final var allRecordTypes = ImmutableSet.of("MySimpleRecord", "MyOtherRecord");
        var qun =
                Quantifier.forEach(GroupExpressionRef.of(
                        new FullUnorderedScanExpression(allRecordTypes,
                                Type.Record.fromFieldDescriptorsMap(metadata.getFieldDescriptorMapFromNames(allRecordTypes)),
                                new AccessHints())));

        qun = Quantifier.forEach(GroupExpressionRef.of(
                new LogicalTypeFilterExpression(ImmutableSet.of("MySimpleRecord"),
                        qun,
                        Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()))));

        final var num2Value = FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_2");
        final var queryBuilder = GraphExpansion.builder();
        queryBuilder.addPredicate(new ValuePredicate(num2Value, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 50)));
        queryBuilder.addQuantifier(qun);
        queryBuilder.addResultColumn(Column.unnamedOf(num2Value));
        final var query = queryBuilder.build().buildSelect();

        qun = Quantifier.forEach(GroupExpressionRef.of(query));
        return GroupExpressionRef.of(new LogicalSortExpression(ImmutableList.of(), false, qun));
    }

    @Nonnull
    private static RecordQueryPlan planQuery(@Nonnull final CascadesPlanner planner, int boundary) {
        return planner.planGraph(
                () -> constructQueryWithPredicate(planner.getRecordMetaData(), boundary),
                Optional.empty(),
                IndexQueryabilityFilter.TRUE,
                false,
                ParameterRelationshipGraph.empty());
    }
}
