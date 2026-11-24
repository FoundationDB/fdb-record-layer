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

import com.apple.foundationdb.record.EvaluationContext;
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
import com.apple.foundationdb.record.query.expressions.Query;
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
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * Creates a plan that maps each record of {@code child} to its record number, which, in contrast to the record
     * itself, a record that is held back across a continuation retains in full.
     */
    @Nonnull
    private static RecordQueryPlan recNoPlan(@Nonnull final RecordQueryPlan child) {
        final Quantifier.Physical quantifier = Quantifier.physical(Reference.plannedOf(child), CorrelationIdentifier.of("child"));
        final Type.Record type = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        return new RecordQueryMapPlan(quantifier,
                FieldValue.ofFieldName(QuantifiedObjectValue.of(quantifier.getAlias(), type), "rec_no"));
    }

    /**
     * The outcome of executing a plan until its cursor stops.
     */
    private record Page(@Nonnull List<QueryResult> results,
                        @Nonnull RecordCursor.NoNextReason noNextReason,
                        @Nullable byte[] continuation) {
        @Nonnull
        List<Object> values() {
            return results.stream().map(QueryResult::getDatum).collect(Collectors.toList());
        }
    }

    /**
     * Executes {@code plan} until its cursor stops, and returns the results it returned along with why and where it
     * stopped.
     */
    @Nonnull
    private Page executePage(@Nonnull final RecordQueryPlan plan,
                             @Nullable final byte[] continuation,
                             @Nonnull final ExecuteProperties executeProperties) throws Exception {
        return executePage(plan, EvaluationContext.empty(), continuation, executeProperties);
    }

    @Nonnull
    private Page executePage(@Nonnull final RecordQueryPlan plan,
                             @Nonnull final EvaluationContext evaluationContext,
                             @Nullable final byte[] continuation,
                             @Nonnull final ExecuteProperties executeProperties) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<QueryResult> cursor =
                         plan.executePlan(recordStore, evaluationContext, continuation, executeProperties)) {
                final AtomicReference<RecordCursorResult<QueryResult>> terminatingResult = new AtomicReference<>();
                final List<QueryResult> results = context.asyncToSync(FDBStoreTimer.Waits.WAIT_EXECUTE_QUERY,
                        cursor.asList(terminatingResult));
                return new Page(results, terminatingResult.get().getNoNextReason(),
                        terminatingResult.get().getContinuation().toBytes());
            }
        }
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
     * Tests that the plan holds back the record of its inner across a continuation if its inner stops early on a scan
     * limit, and returns that record once the inner has turned out to be exhausted.
     */
    @Test
    void testMaxOneRecord6() throws Exception {
        final RecordQueryPlan plan = maxOneRecordPlan(recNoPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2)));
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        final Page first = executePage(plan, null, scanLimitOne);
        assertThat(first.values()).isEmpty();
        assertThat(first.noNextReason()).isEqualTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);
        assertThat(first.continuation()).isNotNull();

        final Page second = executePage(plan, first.continuation(), ExecuteProperties.SERIAL_EXECUTE);
        assertThat(second.values()).containsExactly(2L);
        assertThat(second.noNextReason()).isEqualTo(RecordCursor.NoNextReason.SOURCE_EXHAUSTED);
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
     * Tests that the plan properties pass those of the inner through, except for those that describe the stored record
     * and its primary key.
     */
    @Test
    void testMaxOneRecord13() {
        final RecordQueryRangePlan range = rangePlan(5L);
        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(range);

        assertThat(DistinctRecordsProperty.distinctRecords().evaluate(plan))
                .isEqualTo(DistinctRecordsProperty.distinctRecords().evaluate(range))
                .isTrue();
        assertThat(StoredRecordProperty.storedRecord().evaluate(plan)).isFalse();
        assertThat(PrimaryKeyProperty.primaryKey().evaluate(plan)).isEmpty();
        assertThat(OrderingProperty.ordering().evaluate(plan))
                .isEqualTo(OrderingProperty.ordering().evaluate(range));

        final DerivationsProperty.Derivations derivations = DerivationsProperty.derivations().evaluate(plan);
        final DerivationsProperty.Derivations rangeDerivations = DerivationsProperty.derivations().evaluate(range);
        assertThat(derivations.getResultValues()).isEqualTo(rangeDerivations.getResultValues());
        assertThat(derivations.getLocalValues()).isEqualTo(rangeDerivations.getLocalValues());
    }

    /**
     * Tests that a violation is raised after resuming from a held record, even though every page of the inner is
     * limited to a single scanned record.
     */
    @Test
    void testMaxOneRecord14() throws Exception {
        final RecordQueryPlan plan = maxOneRecordPlan(recNoPlan(indexPlanEquals(STR_VALUE_INDEX, "even")));
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        final Page first = executePage(plan, null, scanLimitOne);
        assertThat(first.values()).isEmpty();
        assertThat(first.noNextReason()).isEqualTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);
        assertThat(first.continuation()).isNotNull();

        assertThatThrownBy(() -> executePage(plan, first.continuation(), scanLimitOne))
                .isInstanceOf(RecordCoreCardinalityViolationException.class);
    }

    /**
     * Tests that the plan makes progress when resumed from a scan limit that stopped its inner before the first record,
     * even if every page is limited to a single scanned record, and still returns the one record of its inner.
     */
    @Test
    void testMaxOneRecord15() throws Exception {
        final RecordQueryPlan filtered = new RecordQueryFilterPlan(scanPlan(false), Query.field("num_value_3_indexed").equalsValue(4));
        final RecordQueryPlan plan = maxOneRecordPlan(recNoPlan(filtered));
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        byte[] continuation = null;
        final List<Object> values = new ArrayList<>();
        int page = 0;
        for (; page < 10; page++) {
            final Page result = executePage(plan, continuation, scanLimitOne);
            values.addAll(result.values());
            if (result.noNextReason().isSourceExhausted()) {
                break;
            }
            continuation = result.continuation();
        }
        assertThat(values).containsExactly(4L);
        assertThat(page).isLessThan(10);
    }

    /**
     * Tests that the plan raises a {@link RecordCoreException} if given a continuation that it cannot parse.
     */
    @Test
    void testMaxOneRecord16() {
        final RecordQueryPlan plan = maxOneRecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2));
        assertThatThrownBy(() -> execute(plan, new byte[] {1, 2, 3}, ExecuteProperties.SERIAL_EXECUTE))
                .isExactlyInstanceOf(RecordCoreException.class);
    }

    /**
     * Tests that a held record that is a message survives a continuation, if the type repository of the context knows
     * the type of the record.
     */
    @Test
    void testMaxOneRecord17() throws Exception {
        final Quantifier.Physical quantifier = Quantifier.physical(
                Reference.plannedOf(indexPlanEquals(NUM_VALUE_3_INDEX, 2)), CorrelationIdentifier.of("child"));
        final Type.Record recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        final RecordConstructorValue wrapped = RecordConstructorValue.ofColumns(ImmutableList.of(Column.of(Optional.of("rec_no"),
                FieldValue.ofFieldName(QuantifiedObjectValue.of(quantifier.getAlias(), recordType), "rec_no"))));
        final RecordQueryPlan plan = maxOneRecordPlan(new RecordQueryMapPlan(quantifier, wrapped));
        final EvaluationContext evaluationContext = EvaluationContext.forTypeRepository(
                TypeRepository.newBuilder().addTypeIfNeeded(wrapped.getResultType()).build());
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        final Page fresh = executePage(plan, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE);
        final Page first = executePage(plan, evaluationContext, null, scanLimitOne);
        assertThat(first.results()).isEmpty();
        final Page resumed = executePage(plan, evaluationContext, first.continuation(), ExecuteProperties.SERIAL_EXECUTE);

        assertThat(fresh.results()).hasSize(1);
        assertThat(resumed.results()).hasSize(1);
        assertThat(resumed.results().get(0).getMessage().toString()).isEqualTo(fresh.results().get(0).getMessage().toString());
        assertThat(resumed.results().get(0).getMessage().toString()).contains("2");
    }

    /**
     * Tests the metadata of a record that is held back across a continuation, as opposed to one that is not: the
     * former carries its value only.
     */
    @Test
    void testMaxOneRecord18() throws Exception {
        final RecordQueryPlan plan = maxOneRecordPlan(recNoPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2)));
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        final QueryResult fresh = executePage(plan, null, ExecuteProperties.SERIAL_EXECUTE).results().get(0);
        final Page first = executePage(plan, null, scanLimitOne);
        final QueryResult resumed = executePage(plan, first.continuation(), ExecuteProperties.SERIAL_EXECUTE).results().get(0);

        assertThat(resumed.getDatum()).isEqualTo(fresh.getDatum());
        assertThat((Object)fresh.getQueriedRecord()).isNotNull();
        assertThat((Object)fresh.getPrimaryKey()).isNotNull();
        assertThat((Object)resumed.getQueriedRecord()).isNull();
        assertThat((Object)resumed.getPrimaryKey()).isNull();
    }

    /**
     * Tests that the plan raises a {@link RecordCoreException} carrying the no-next reason as soon as it would have to
     * hold back a record that is a message of a type that the type repository of the context does not know, rather
     * than failing when resuming.
     */
    @Test
    void testMaxOneRecord19() {
        final RecordQueryPlan plan = maxOneRecordPlan(indexPlanEquals(NUM_VALUE_3_INDEX, 2));
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();
        assertThatThrownBy(() -> executePage(plan, null, scanLimitOne))
                .isExactlyInstanceOf(RecordCoreException.class)
                .satisfies(e -> assertThat(((RecordCoreException)e).getLogInfo())
                        .containsEntry(LogMessageKeys.NO_NEXT_REASON.toString(),
                                RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
    }

    /**
     * Tests that the plan reports neither stored records nor a primary key, even if its inner has them, since a record
     * that is held back across a continuation is returned without either.
     */
    @Test
    void testMaxOneRecord20() {
        final RecordQueryScanPlan scan = scanPlan(false);
        assertThat(StoredRecordProperty.storedRecord().evaluate(scan)).isTrue();

        final RecordQueryMaxOneRecordPlan plan = maxOneRecordPlan(scan);
        assertThat(StoredRecordProperty.storedRecord().evaluate(plan)).isFalse();
        assertThat(PrimaryKeyProperty.primaryKey().evaluate(plan)).isEmpty();
    }

    @Nonnull
    private static Stream<Arguments> heldValues() {
        return Stream.of(
                Arguments.of(Named.of("long", LiteralValue.ofScalar(7L))),
                Arguments.of(Named.of("int", LiteralValue.ofScalar(7))),
                Arguments.of(Named.of("double", LiteralValue.ofScalar(1.5d))),
                Arguments.of(Named.of("float", LiteralValue.ofScalar(1.5f))),
                Arguments.of(Named.of("string", LiteralValue.ofScalar("seven"))),
                Arguments.of(Named.of("boolean", LiteralValue.ofScalar(true))),
                Arguments.of(Named.of("bytes", LiteralValue.ofScalar(ByteString.copyFromUtf8("seven")))),
                Arguments.of(Named.of("null", new NullValue(Type.primitiveType(Type.TypeCode.STRING)))));
    }

    /**
     * Tests that a held record that is a scalar value, including {@code null}, is returned unchanged after resuming.
     */
    @ParameterizedTest
    @MethodSource("heldValues")
    void testMaxOneRecord21(@Nonnull final Value value) throws Exception {
        final Quantifier.Physical quantifier = Quantifier.physical(
                Reference.plannedOf(indexPlanEquals(NUM_VALUE_3_INDEX, 2)), CorrelationIdentifier.of("child"));
        final RecordQueryPlan plan = maxOneRecordPlan(new RecordQueryMapPlan(quantifier, value));
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        final Page fresh = executePage(plan, null, ExecuteProperties.SERIAL_EXECUTE);
        final Page first = executePage(plan, null, scanLimitOne);
        assertThat(first.results()).isEmpty();
        final Page resumed = executePage(plan, first.continuation(), ExecuteProperties.SERIAL_EXECUTE);

        assertThat(fresh.values()).hasSize(1);
        assertThat(resumed.values()).isEqualTo(fresh.values());
    }

    /**
     * Tests that the plan raises a {@link RecordCoreException} carrying the no-next reason as soon as it would have to
     * hold back a record that cannot be serialized, such as a list, rather than failing when resuming.
     */
    @Test
    void testMaxOneRecord22() throws Exception {
        final Quantifier.Physical quantifier = Quantifier.physical(
                Reference.plannedOf(indexPlanEquals(NUM_VALUE_3_INDEX, 2)), CorrelationIdentifier.of("child"));
        final Value list = AbstractArrayConstructorValue.LightArrayConstructorValue.of(
                LiteralValue.ofScalar(1L), LiteralValue.ofScalar(2L));
        final RecordQueryPlan plan = maxOneRecordPlan(new RecordQueryMapPlan(quantifier, list));
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        assertThat(executePage(plan, null, ExecuteProperties.SERIAL_EXECUTE).values()).hasSize(1);
        assertThatThrownBy(() -> executePage(plan, null, scanLimitOne))
                .isExactlyInstanceOf(RecordCoreException.class)
                .hasMessageContaining("cannot be serialized")
                .satisfies(e -> assertThat(((RecordCoreException)e).getLogInfo())
                        .containsEntry(LogMessageKeys.NO_NEXT_REASON.toString(),
                                RecordCursor.NoNextReason.SCAN_LIMIT_REACHED));
    }

    /**
     * Tests that a held record that contains a list survives a continuation.
     */
    @Test
    void testMaxOneRecord23() throws Exception {
        final Quantifier.Physical quantifier = Quantifier.physical(
                Reference.plannedOf(indexPlanEquals(NUM_VALUE_3_INDEX, 2)), CorrelationIdentifier.of("child"));
        final RecordConstructorValue wrapped = RecordConstructorValue.ofColumns(ImmutableList.of(Column.of(Optional.of("numbers"),
                AbstractArrayConstructorValue.LightArrayConstructorValue.of(
                        LiteralValue.ofScalar(1L), LiteralValue.ofScalar(2L)))));
        final RecordQueryPlan plan = maxOneRecordPlan(new RecordQueryMapPlan(quantifier, wrapped));
        final EvaluationContext evaluationContext = EvaluationContext.forTypeRepository(
                TypeRepository.newBuilder().addTypeIfNeeded(wrapped.getResultType()).build());
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        final Page fresh = executePage(plan, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE);
        final Page first = executePage(plan, evaluationContext, null, scanLimitOne);
        assertThat(first.results()).isEmpty();
        final Page resumed = executePage(plan, evaluationContext, first.continuation(), ExecuteProperties.SERIAL_EXECUTE);

        assertThat(resumed.results()).hasSize(1);
        assertThat(resumed.results().get(0).getMessage().toString()).isEqualTo(fresh.results().get(0).getMessage().toString());
    }

    /**
     * Tests that a held record that is a message survives a continuation if the type repository of the context is built
     * from the types used by the plan, which is how the relational layer builds it.
     */
    @Test
    void testMaxOneRecord24() throws Exception {
        final Quantifier.Physical quantifier = Quantifier.physical(
                Reference.plannedOf(indexPlanEquals(NUM_VALUE_3_INDEX, 2)), CorrelationIdentifier.of("child"));
        final Type.Record recordType = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
        final RecordConstructorValue wrapped = RecordConstructorValue.ofColumns(ImmutableList.of(Column.of(Optional.of("rec_no"),
                FieldValue.ofFieldName(QuantifiedObjectValue.of(quantifier.getAlias(), recordType), "rec_no"))));
        final RecordQueryPlan plan = maxOneRecordPlan(new RecordQueryMapPlan(quantifier, wrapped));
        final TypeRepository.Builder typeRepositoryBuilder = TypeRepository.newBuilder();
        UsedTypesProperty.usedTypes().evaluate(plan).forEach(typeRepositoryBuilder::addTypeIfNeeded);
        final EvaluationContext evaluationContext = EvaluationContext.forTypeRepository(typeRepositoryBuilder.build());
        final ExecuteProperties scanLimitOne = ExecuteProperties.newBuilder().setScannedRecordsLimit(1).build();

        final Page fresh = executePage(plan, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE);
        final Page first = executePage(plan, evaluationContext, null, scanLimitOne);
        assertThat(first.results()).isEmpty();
        final Page resumed = executePage(plan, evaluationContext, first.continuation(), ExecuteProperties.SERIAL_EXECUTE);

        assertThat(resumed.results()).hasSize(1);
        assertThat(resumed.results().get(0).getMessage().toString()).isEqualTo(fresh.results().get(0).getMessage().toString());
    }
}
