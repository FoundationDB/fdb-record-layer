/*
 * SparseIndexTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.descendantPlans;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.mapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;

/**
 * Tests filtered (sparse) index serialisation and planning.
 */
@Tag(Tags.RequiresFDB)
public class SparseIndexTest extends FDBRecordStoreQueryTestBase {

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void sparseIndexIsUsedWhenItsOrPredicateIsImplied() throws Exception {
        final var recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        final var numValue2 = FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "num_value_2");
        final var range = OrPredicate.or(List.of(
                new ValuePredicate(numValue2, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 42)),
                new ValuePredicate(numValue2, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 10))
        ));
        complexQuerySetup(metaData -> setupIndex(metaData, range));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = planQuery(cascadesPlanner);
        assertMatchesExactly(plan,
                mapPlan(coveringIndexPlan().where(indexPlanOf(indexPlan()
                        .where(RecordQueryPlanMatchers.indexName("SparseIndex"))
                        .and(RecordQueryPlanMatchers.indexScanType(IndexScanType.BY_VALUE))
                        .and(scanComparisons(range("([50],>")))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void sparseIndexIsUsedWhenItsPredicateIsImplied() throws Exception {
        final var compileTimeRange = RangeConstraints.newBuilder();
        compileTimeRange.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 42));
        final var recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        complexQuerySetup(metaData -> setupIndex(metaData, PredicateWithValueAndRanges.sargable(FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "num_value_2"),
                compileTimeRange.build().orElseThrow()).toResidualPredicate()));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = planQuery(cascadesPlanner);
        assertMatchesExactly(plan,
                mapPlan(coveringIndexPlan().where(indexPlanOf(indexPlan()
                        .where(RecordQueryPlanMatchers.indexName("SparseIndex"))
                        .and(RecordQueryPlanMatchers.indexScanType(IndexScanType.BY_VALUE))
                        .and(scanComparisons(range("([50],>")))))));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void sparseIndexIsNotUsedWhenItsPredicateIsNotImplied() throws Exception {
        final var compileTimeRange = RangeConstraints.newBuilder();
        compileTimeRange.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 100));
        final var recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        complexQuerySetup(metaData -> setupIndex(metaData, PredicateWithValueAndRanges.sargable(FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "num_value_2"),
                compileTimeRange.build().orElseThrow()).toResidualPredicate()));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = planQuery(cascadesPlanner);
        assertMatchesExactly(plan, mapPlan(descendantPlans(scanPlan())));
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    public void sparseIndexIsNotPickedWhenDoingFullScan() throws Exception {
        final var compileTimeRange = RangeConstraints.newBuilder();
        compileTimeRange.addComparisonMaybe(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 100));
        final var recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        complexQuerySetup(metaData -> setupIndex(metaData, PredicateWithValueAndRanges.sargable(FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), recordType), "num_value_2"),
                compileTimeRange.build().orElseThrow()).toResidualPredicate()));
        final var cascadesPlanner = (CascadesPlanner)planner;
        final var plan = planQueryWithAllowedIndexes(cascadesPlanner, Optional.of(Set.of("SparseIndex")), false);
        assertMatchesExactly(plan, mapPlan(descendantPlans(scanPlan())));
    }


    /**
     * Appends a filtered (sparse) {@link Index} to the {@link RecordMetaData} object with a specific {@link QueryPredicate}.
     *
     * @param metaData The metadata to add the index to.
     * @param predicate The predicate of the filtered index.
     */
    private static void setupIndex(@Nonnull final RecordMetaDataBuilder metaData, @Nonnull final QueryPredicate predicate) {
        final var normalized = BooleanPredicateNormalizer.getDefaultInstanceForDnf().normalizeAndSimplify(predicate, true).orElse(predicate);
        final var protoIndexBuilder = RecordMetaDataProto.Index.newBuilder()
                .setName("SparseIndex")
                .addRecordType("MySimpleRecord")
                .setType(IndexTypes.VALUE)
                .setRootExpression(field("num_value_2").toKeyExpression())
                .setPredicate(IndexPredicate.fromQueryPredicate(normalized).toProto())
                .build();
        final var index = new Index(protoIndexBuilder);
        metaData.addIndex("MySimpleRecord", index);
    }

    /**
     * Constructs query {@code SELECT num_value_2 FROM MySimpleRecord WHERE num_value_2 > 50} using the provided metadata.
     * @param addPredicate if {@code true} attaches a predicate to the query, otherwise it does not.
     * @return A graph expansion representing the query.
     */
    @Nonnull
    private static Reference constructQueryWithPredicate(boolean addPredicate) {
        final var allRecordTypes = ImmutableSet.of("MySimpleRecord", "MyOtherRecord");
        var qun =
                Quantifier.forEach(Reference.of(
                        new FullUnorderedScanExpression(allRecordTypes,
                                new Type.AnyRecord(false),
                                new AccessHints())));

        qun = Quantifier.forEach(Reference.of(
                new LogicalTypeFilterExpression(ImmutableSet.of("MySimpleRecord"),
                        qun,
                        Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor()))));

        final var num2Value = FieldValue.ofFieldName(qun.getFlowedObjectValue(), "num_value_2");
        final var queryBuilder = GraphExpansion.builder();
        if (addPredicate) {
            queryBuilder.addPredicate(new ValuePredicate(num2Value, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 50)));
        }
        queryBuilder.addQuantifier(qun);
        queryBuilder.addResultColumn(Column.unnamedOf(num2Value));
        final var query = queryBuilder.build().buildSelect();

        qun = Quantifier.forEach(Reference.of(query));
        return Reference.of(LogicalSortExpression.unsorted(qun));
    }

    /**
     * Generates query {@code SELECT num_value_2 FROM MySimpleRecord WHERE num_value_2 > 50} using {@link CascadesPlanner}
     * and returns an optimized physical plan.
     *
     * @param planner The planner.
     * @return optimized query of {@code SELECT num_value_2 FROM MySimpleRecord WHERE num_value_2 > 50}
     */
    @Nonnull
    private static RecordQueryPlan planQuery(@Nonnull final CascadesPlanner planner) {
        return planQueryWithAllowedIndexes(planner, Optional.empty(), true);
    }

    /**
     * Generates query {@code SELECT num_value_2 FROM MySimpleRecord WHERE num_value_2 > 50} using {@link CascadesPlanner}
     * with a list of allowed indexes and returns an optimized physical plan.
     *
     * @param planner The planner.
     * @param allowedIndexes A list of allowed indexes.
     * @param addQueryPredicate if {@code true} attaches a predicate to the query, otherwise it does not.
     * @return optimized query of {@code SELECT num_value_2 FROM MySimpleRecord WHERE num_value_2 > 50}
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @Nonnull
    private static RecordQueryPlan planQueryWithAllowedIndexes(@Nonnull final CascadesPlanner planner,
                                                               @Nonnull final Optional<Collection<String>> allowedIndexes,
                                                               boolean addQueryPredicate) {
        return planner.planGraph(
                () -> constructQueryWithPredicate(addQueryPredicate),
                allowedIndexes,
                IndexQueryabilityFilter.TRUE,
                EvaluationContext.empty()).getPlan();
    }
}
