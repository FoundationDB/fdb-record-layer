/*
 * OutsideValueLikeIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.query.DualPlannerTest;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullTypeScan;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.projectColumn;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.anyValueComparison;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.equalities;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.containsAll;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.flatMapPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.recordTypes;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty.usedTypes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test that we can use the {@link OutsideValueLikeIndexMaintainer} to define
 * indexes that are then matched by the planner(s).
 */
@Tag(Tags.RequiresFDB)
class OutsideValueLikeIndexQueryTest extends FDBRecordStoreQueryTestBase {
    @Nonnull
    private static final String OUTSIDE_INDEX_NAME = "outside_index";

    @Nonnull
    private static final PlannableIndexTypes WITH_OUTSIDE_INDEX_TYPES = new PlannableIndexTypes(
            ImmutableSet.<String>builder()
                    .addAll(PlannableIndexTypes.DEFAULT.getValueTypes())
                    .add(OutsideValueLikeIndexMaintainer.INDEX_TYPE)
                    .build(),
            PlannableIndexTypes.DEFAULT.getRankTypes(),
            PlannableIndexTypes.DEFAULT.getTextTypes(),
            PlannableIndexTypes.DEFAULT.getUnstoredNonPrimaryKeyTypes()
    );

    private void addOutsideNumValue2Index(@Nonnull RecordMetaDataBuilder metaDataBuilder) {
        final Index index = new Index(OUTSIDE_INDEX_NAME, field("num_value_2"), OutsideValueLikeIndexMaintainer.INDEX_TYPE);
        metaDataBuilder.addIndex("MySimpleRecord", index);
    }

    private void addNonCascadesNumValue2Index(@Nonnull RecordMetaDataBuilder metaDataBuilder) {
        final Index index = new Index(OUTSIDE_INDEX_NAME, field("num_value_2"), NonCascadesValueIndexMaintainer.INDEX_TYPE);
        metaDataBuilder.addIndex("MySimpleRecord", index);
    }

    private void openStoreWithOutsideIndex(@Nonnull FDBRecordContext context) {
        openSimpleRecordStore(context, this::addOutsideNumValue2Index);
        setupPlanner(WITH_OUTSIDE_INDEX_TYPES);
    }

    private void openStoreWithNonCascadesValue2Index(@Nonnull FDBRecordContext context) {
        openSimpleRecordStore(context, this::addNonCascadesNumValue2Index);
    }

    @Nonnull
    private List<TestRecords1Proto.MySimpleRecord> saveSimpleData(int count) {
        final List<TestRecords1Proto.MySimpleRecord> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final TestRecords1Proto.MySimpleRecord msg = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1_000L + i)
                    .setNumValue2(i % 10)
                    .setNumValueUnique(i)
                    .setStrValueIndexed(i % 2 == 0 ? "even" : "odd")
                    .setNumValue3Indexed(i % 7)
                    .build();
            recordStore.saveRecord(msg);
            results.add(msg);
        }
        return results;
    }

    @Nonnull
    private List<TestRecords1Proto.MyOtherRecord> saveOtherData(int count) {
        final List<TestRecords1Proto.MyOtherRecord> results = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final TestRecords1Proto.MyOtherRecord msg = TestRecords1Proto.MyOtherRecord.newBuilder()
                    .setRecNo(100_000L + i)
                    .setNumValue2(i % 10)
                    .setNumValue3Indexed(i % 7)
                    .build();
            recordStore.saveRecord(msg);
            results.add(msg);
        }
        return results;
    }

    /**
     * The old planner requires we identify the outside index as a value-like
     * type. If we do that, then it will match the index like a normal
     * value index. The Cascades planner actually doesn't require this (as it
     * uses the index maintainer factory's match candidate method).
     */
    @DualPlannerTest
    void planForSort() {
        try (FDBRecordContext context = openContext()) {
            openStoreWithOutsideIndex(context);

            final List<TestRecords1Proto.MySimpleRecord> simpleRecords = saveSimpleData(50);
            saveOtherData(20); // just so that we have something to avoid during the query

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setSort(field("num_value_2"))
                    .build();

            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, indexPlan()
                    .where(indexName(OUTSIDE_INDEX_NAME))
                    .and(scanComparisons(unbounded())));

            // Validate the results. Make sure results come back in the same order and contain
            // all simple records
            int lastNumValue2 = Integer.MIN_VALUE;
            final List<TestRecords1Proto.MySimpleRecord> queried = new ArrayList<>();
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
                while (cursor.hasNext()) {
                    TestRecords1Proto.MySimpleRecord simpleRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                            .mergeFrom(cursor.next().getRecord())
                            .build();
                    assertThat(simpleRecord.getNumValue2())
                            .as("num_value_2 field should be increasing")
                            .isGreaterThanOrEqualTo(lastNumValue2);
                    lastNumValue2 = simpleRecord.getNumValue2();
                    queried.add(simpleRecord);
                }
            }
            assertThat(queried)
                    .hasSameElementsAs(simpleRecords);
        }
    }

    @DualPlannerTest
    void planForFilter() {
        try (FDBRecordContext context = openContext()) {
            openStoreWithOutsideIndex(context);

            final List<TestRecords1Proto.MySimpleRecord> simpleRecords = saveSimpleData(35);
            final Set<Integer> numValue2s = simpleRecords.stream()
                    .map(TestRecords1Proto.MySimpleRecord::getNumValue2)
                    .collect(Collectors.toSet());
            saveOtherData(20); // just so that we have something to avoid during the query

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("num_value_2").equalsParameter("param"))
                    .setSort(field("num_value_2"))
                    .build();
            setupPlanner(WITH_OUTSIDE_INDEX_TYPES);

            // This should plan as a single scan over the index limited to those values that match
            // the given parameter
            final RecordQueryPlan plan = planQuery(query);
            assertMatchesExactly(plan, indexPlan()
                    .where(indexName(OUTSIDE_INDEX_NAME))
                    .and(scanComparisons(range("[EQUALS $param]"))));

            // Validate the query results by running against each valid parameter
            for (int numValue2 : numValue2s) {
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan, Bindings.newBuilder().set("param", numValue2).build())) {
                    final List<TestRecords1Proto.MySimpleRecord> queried = new ArrayList<>();
                    while (cursor.hasNext()) {
                        TestRecords1Proto.MySimpleRecord simpleRecord = TestRecords1Proto.MySimpleRecord.newBuilder()
                                .mergeFrom(cursor.next().getRecord())
                                .build();
                        assertThat(simpleRecord.getNumValue2())
                                .isEqualTo(numValue2);
                        queried.add(simpleRecord);
                    }
                    assertThat(queried)
                            .hasSameElementsAs(simpleRecords.stream().filter(msg -> msg.getNumValue2() == numValue2).collect(Collectors.toList()));
                }
            }
        }
    }

    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void useAsPartOfJoin() {
        final List<TestRecords1Proto.MySimpleRecord> simpleRecords;
        final List<TestRecords1Proto.MyOtherRecord> otherRecords;
        try (FDBRecordContext context = openContext()) {
            openStoreWithOutsideIndex(context);
            simpleRecords = saveSimpleData(20);
            otherRecords = saveOtherData(20);
            commit(context);
        }

        // Precompute the join results, using num_value_2 as the join criterion
        final Map<Integer, Set<NonnullPair<Long, Long>>> expectedResults = new HashMap<>();
        for (TestRecords1Proto.MySimpleRecord simpleRecord : simpleRecords) {
            for (TestRecords1Proto.MyOtherRecord otherRecord : otherRecords) {
                if (simpleRecord.getNumValue2() == otherRecord.getNumValue2()) {
                    final Set<NonnullPair<Long, Long>> resultList = expectedResults.computeIfAbsent(simpleRecord.getNumValue2(), ignore -> new HashSet<>());
                    resultList.add(NonnullPair.of(simpleRecord.getRecNo(), otherRecord.getRecNo()));
                }
            }
        }

        try (FDBRecordContext context = openContext()) {
            openStoreWithOutsideIndex(context);

            // Plan a query like:
            //   SELECT MySimpleRecord.rec_no AS simple_rec_no, MyOtherRecord.rec_no AS other_rec_no, MySimpleRecord.num_value_2
            //     FROM MySimpleRecord, MyOtherRecord
            //    WHERE MySimpleRecord.num_value_2 = MyOtherRecord.num_value_2
            final RecordQueryPlan plan = planGraph(() -> {
                Quantifier simpleQun = fullTypeScan(recordStore.getRecordMetaData(), "MySimpleRecord");
                Quantifier otherQun = fullTypeScan(recordStore.getRecordMetaData(), "MyOtherRecord");

                final Quantifier select = forEach(GraphExpansion.builder()
                        .addResultColumn(column(simpleQun, "rec_no", "simple_rec_no"))
                        .addResultColumn(column(otherQun, "rec_no", "other_rec_no"))
                        .addResultColumn(projectColumn(simpleQun, "num_value_2"))
                        .addQuantifier(simpleQun)
                        .addQuantifier(otherQun)
                        .addPredicate(fieldPredicate(simpleQun, "num_value_2", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(otherQun, "num_value_2"))))
                        .build().buildSelect());

                return Reference.initialOf(new LogicalSortExpression(RequestedOrdering.preserve(), select));
            });

            // This should produce a plan that scans and grabs every MyOtherRecord, and then it
            // uses the outside value index to look up its corresponding values by num_value_2
            assertMatchesExactly(plan, flatMapPlan(
                    typeFilterPlan(scanPlan().where(scanComparisons(unbounded())))
                            .where(recordTypes(containsAll(ImmutableSet.of("MyOtherRecord")))),
                    indexPlan()
                            .where(indexName(OUTSIDE_INDEX_NAME))
                            .and(scanComparisons(equalities(only(anyValueComparison()))))
            ));

            // Validate the query results
            final Map<Integer, Set<NonnullPair<Long, Long>>> queriedResults = new HashMap<>();
            final TypeRepository typeRepository = TypeRepository.newBuilder().addAllTypes(usedTypes().evaluate(plan)).build();
            try (RecordCursor<QueryResult> cursor = plan.executePlan(recordStore, EvaluationContext.forTypeRepository(typeRepository), null, ExecuteProperties.SERIAL_EXECUTE)) {
                for (RecordCursorResult<QueryResult> result = cursor.getNext(); result.hasNext(); result = cursor.getNext()) {
                    Message msg = result.get().getMessage();
                    Descriptors.Descriptor descriptor = msg.getDescriptorForType();
                    int numValue2 = (int) msg.getField(descriptor.findFieldByName("num_value_2"));
                    long simpleRecNo = (long) msg.getField(descriptor.findFieldByName("simple_rec_no"));
                    long otherRecNo = (long) msg.getField(descriptor.findFieldByName("other_rec_no"));
                    queriedResults.computeIfAbsent(numValue2, ignore -> new HashSet<>())
                            .add(NonnullPair.of(simpleRecNo, otherRecNo));
                }
            }
            assertThat(queriedResults)
                    .isEqualTo(expectedResults);
        }
    }

    @DualPlannerTest
    void fullScanWithIndexWithoutMatchCandidates() {
        try (FDBRecordContext context = openContext()) {
            openStoreWithNonCascadesValue2Index(context);

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("num_value_2").equalsParameter("param"))
                    .build();
            final RecordQueryPlan plan = planQuery(query);
            if (useCascadesPlanner) {
                assertMatchesExactly(plan, predicatesFilterPlan(
                        typeFilterPlan(
                                scanPlan().where(scanComparisons(unbounded()))
                        )
                ));
            } else {
                assertMatchesExactly(plan, filterPlan(
                        typeFilterPlan(
                                scanPlan().where(scanComparisons(unbounded()))
                        )
                ));
            }
        }
    }

    /**
     * Validate that indexes that aren't set up to be matched are not selected when querying.
     * Ideally, we'd be able to assert that this doesn't use the index in the old planner either,
     * but see: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/3648">Issue #3648</a>.
     */
    @DualPlannerTest(planner = DualPlannerTest.Planner.CASCADES)
    void cannotSortWithIndexWithoutMatchCandidates() {
        try (FDBRecordContext context = openContext()) {
            openStoreWithNonCascadesValue2Index(context);

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setSort(field("num_value_2"), true)
                    .build();
            assertThatThrownBy(() -> planQuery(query))
                    .isInstanceOf(RecordCoreException.class)
                    .hasMessageContaining("Cascades planner could not plan query");
        }
    }
}
