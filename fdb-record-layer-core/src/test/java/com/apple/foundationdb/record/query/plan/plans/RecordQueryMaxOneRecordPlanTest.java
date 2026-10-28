/*
 * RecordQueryMaxOneRecordPlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreCardinalityViolationException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.properties.DerivationsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RecordQueryMaxOneRecordPlan}.
 */
@Tag(Tags.RequiresFDB)
class RecordQueryMaxOneRecordPlanTest extends FDBRecordStoreTestBase {
    @Nonnull
    private static final String NUM_VALUE_3_INDEX = "MySimpleRecord$num_value_3_indexed";
    @Nonnull
    private static final String STR_VALUE_INDEX = "MySimpleRecord$str_value_indexed";

    @Nonnull
    private static final CorrelationIdentifier ALIAS = CorrelationIdentifier.of("inner");

    @Nonnull
    private static RecordQueryRangePlan rangePlan(final long exclusiveLimit) {
        return new RecordQueryRangePlan(LiteralValue.ofScalar(exclusiveLimit));
    }

    @Nonnull
    private static RecordQueryScanPlan scanPlan(final boolean reverse) {
        return new RecordQueryScanPlan(ScanComparisons.EMPTY, reverse);
    }

    /**
     * Saves five records with distinct {@code num_value_3_indexed} values, three of which have an
     * {@code str_value_indexed} of {@code "even"}.
     */
    @BeforeEach
    void setUpStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setNumValue3Indexed(i)
                        .setStrValueIndexed(i % 2 == 0 ? "even" : "odd")
                        .build());
            }
            commit(context);
        }
    }

    @Nonnull
    private static RecordQueryPlan indexPlanEquals(@Nonnull final String indexName, @Nonnull final Object value) {
        return new RecordQueryIndexPlan(indexName,
                IndexScanComparisons.byValue(new ScanComparisons(
                        ImmutableList.of(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value)),
                        ImmutableSet.of())),
                false);
    }

    /**
     * Creates a plan whose quantifier has a fixed alias, since equality of plans depends on the aliases of their
     * quantifiers.
     */
    @Nonnull
    private static RecordQueryMaxOneRecordPlan maxOneRecordPlan(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryMaxOneRecordPlan(Quantifier.physical(Reference.plannedOf(child), ALIAS));
    }

    /**
     * Executes {@code plan} to completion and returns the record numbers of the records it returns.
     */
    @Nonnull
    private List<Long> execute(@Nonnull final RecordQueryPlan plan,
                               @Nullable final byte[] continuation,
                               @Nonnull final ExecuteProperties executeProperties) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor =
                         recordStore.executeQuery(plan, continuation, executeProperties)) {
                return context.asyncToSync(FDBStoreTimer.Waits.WAIT_EXECUTE_QUERY,
                        cursor.map(RecordQueryMaxOneRecordPlanTest::recNo).asList());
            }
        }
    }

    private static long recNo(@Nonnull final FDBQueriedRecord<Message> queriedRecord) {
        return TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(queriedRecord.getRecord()).getRecNo();
    }

    /**
     * Tests that the plan returns nothing if its inner returns no records.
     */
    @Test
    void testMaxOneRecord1() throws Exception {
        final RecordQueryPlan plan = maxOneRecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 99));
        assertThat(execute(plan, null, ExecuteProperties.SERIAL_EXECUTE)).isEmpty();
    }

    /**
     * Tests that the plan returns the record of its inner if its inner returns exactly one record.
     */
    @Test
    void testMaxOneRecord2() throws Exception {
        final RecordQueryPlan plan = maxOneRecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2));
        assertThat(execute(plan, null, ExecuteProperties.SERIAL_EXECUTE)).containsExactly(2L);
    }

    /**
     * Tests that the plan raises a {@link RecordCoreCardinalityViolationException} carrying its plan hash if its inner
     * returns more than one record.
     */
    @Test
    void testMaxOneRecord3() {
        final RecordQueryPlan plan = maxOneRecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even"));
        assertThatThrownBy(() -> execute(plan, null, ExecuteProperties.SERIAL_EXECUTE))
                .isInstanceOf(RecordCoreCardinalityViolationException.class)
                .satisfies(e -> assertThat(((RecordCoreException)e).getLogInfo())
                        .containsKey(LogMessageKeys.PLAN_HASH.toString()));
    }

    /**
     * Tests that a returned-row limit of 1 does not hide the second record of the inner from the check.
     */
    @Test
    void testMaxOneRecord4() {
        final ExecuteProperties limitOne = ExecuteProperties.newBuilder().setReturnedRowLimit(1).build();
        assertThatThrownBy(() -> execute(maxOneRecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even")), null, limitOne))
                .isInstanceOf(RecordCoreCardinalityViolationException.class);
    }

    /**
     * Tests that a skip applies to the records of the plan rather than to those of its inner.
     */
    @Test
    void testMaxOneRecord5() throws Exception {
        final ExecuteProperties skipOne = ExecuteProperties.newBuilder().setSkip(1).build();
        assertThat(execute(maxOneRecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2)), null, skipOne)).isEmpty();
        // Skipping the first of several records of the inner must not hide the violation.
        assertThatThrownBy(() -> execute(maxOneRecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even")), null, skipOne))
                .isInstanceOf(RecordCoreCardinalityViolationException.class);
    }

    /**
     * Tests that the plan raises a {@link RecordCoreException} carrying the no-next reason if its inner stops early on
     * a scan limit before producing a second record.
     */
    @Test
    void testMaxOneRecord6() {
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();
        assertThatThrownBy(() -> execute(maxOneRecordPlan(indexPlanEquals(STR_VALUE_INDEX, "even")), null, scanLimitOne))
                .isExactlyInstanceOf(RecordCoreException.class)
                .satisfies(e -> assertThat(((RecordCoreException)e).getLogInfo())
                        .containsEntry(LogMessageKeys.NO_NEXT_REASON.toString(),
                                RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
    }

    /**
     * Tests that resuming the plan from the continuation after its record returns nothing more.
     */
    @Test
    void testMaxOneRecord7() throws Exception {
        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2));
        final byte[] continuation;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor =
                         recordStore.executeQuery(plan, null, ExecuteProperties.SERIAL_EXECUTE)) {
                final RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                assertThat(result.hasNext()).isTrue();
                assertThat(recNo(result.get())).isEqualTo(2L);
                continuation = result.getContinuation().toBytes();
            }
        }
        assertThat(continuation).isNotNull();
        assertThat(execute(plan, continuation, ExecuteProperties.SERIAL_EXECUTE)).isEmpty();
    }

    /**
     * Tests that the plan explains as a {@code MAX 1 RECORD} operator on top of its inner.
     */
    @Test
    void testMaxOneRecord8() {
        assertThat(maxOneRecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2)).toString()).endsWith("| MAX 1 RECORD");
    }

    /**
     * Tests that the plan reports the direction and complexity of its inner, and is not strictly sorted.
     */
    @Test
    void testMaxOneRecord9() {
        final RecordQueryRangePlan range = rangePlan(5L);
        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(range);
        assertThat(plan.isReverse()).isEqualTo(range.isReverse());
        assertThat(plan.getComplexity()).isEqualTo(range.getComplexity());
        assertThat(plan.isStrictlySorted()).isFalse();
        assertThat(plan.strictlySorted(Mockito.mock(FinalMemoizer.class))).isSameAs(plan);
    }

    /**
     * Tests that plans are equal and hash equally if and only if their inners are.
     */
    @Test
    void testMaxOneRecord10() {
        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(scanPlan(false));
        final RecordQueryMaxOneRecordPlan same = maxOneRecordPlan(scanPlan(false));
        final RecordQueryMaxOneRecordPlan other = maxOneRecordPlan(scanPlan(true));
        assertThat(plan).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(plan).isNotEqualTo(other);
        assertThat(plan).isNotEqualTo(scanPlan(false));
    }

    /**
     * Tests that {@code withChild} and {@code translateCorrelations} replace the inner while keeping the alias of the
     * quantifier where applicable.
     */
    @Test
    void testMaxOneRecord11() {
        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(scanPlan(false));
        final Reference newChildRef = Reference.plannedOf(scanPlan(true));

        final RecordQueryPlanWithChild withChild = plan.withChild(newChildRef);
        assertThat(withChild).isInstanceOf(RecordQueryMaxOneRecordPlan.class).isEqualTo(maxOneRecordPlan(scanPlan(true)));
        assertThat(withChild.getQuantifiers().get(0).getAlias()).isEqualTo(plan.getQuantifiers().get(0).getAlias());

        final Quantifier.Physical newQuantifier = Quantifier.physical(newChildRef, ALIAS);
        final RecordQueryMaxOneRecordPlan translated =
                plan.translateCorrelations(TranslationMap.empty(), false, ImmutableList.of(newQuantifier));
        assertThat(translated.getQuantifiers().get(0)).isSameAs(newQuantifier);
        assertThat(translated).isEqualTo(maxOneRecordPlan(scanPlan(true)));
    }

    /**
     * Tests that the planner graph of the plan is a {@code MAX 1 RECORD} node on top of the graph of its inner.
     */
    @Test
    void testMaxOneRecord12() {
        final RecordQueryRangePlan range = rangePlan(5L);
        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(range);
        final PlannerGraph graph = plan.rewritePlannerGraph(ImmutableList.of(range.rewritePlannerGraph(ImmutableList.of())));
        assertThat(graph.getRoot().getDetails()).containsExactly("MAX 1 RECORD");
        assertThat(graph.getNetwork().nodes()).hasSize(2);
    }

    /**
     * Tests that the plan properties pass those of the inner through.
     */
    @Test
    void testMaxOneRecord13() {
        final RecordQueryRangePlan range = rangePlan(5L);
        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(range);

        assertThat(DistinctRecordsProperty.distinctRecords().evaluate(plan))
                .isEqualTo(DistinctRecordsProperty.distinctRecords().evaluate(range))
                .isTrue();
        assertThat(StoredRecordProperty.storedRecord().evaluate(plan))
                .isEqualTo(StoredRecordProperty.storedRecord().evaluate(range))
                .isFalse();
        assertThat(PrimaryKeyProperty.primaryKey().evaluate(plan))
                .isEqualTo(PrimaryKeyProperty.primaryKey().evaluate(range))
                .isEmpty();
        assertThat(OrderingProperty.ordering().evaluate(plan))
                .isEqualTo(OrderingProperty.ordering().evaluate(range));

        final DerivationsProperty.Derivations derivations = DerivationsProperty.derivations().evaluate(plan);
        final DerivationsProperty.Derivations rangeDerivations = DerivationsProperty.derivations().evaluate(range);
        assertThat(derivations.getResultValues()).isEqualTo(rangeDerivations.getResultValues());
        assertThat(derivations.getLocalValues()).isEqualTo(rangeDerivations.getLocalValues());
    }
}
