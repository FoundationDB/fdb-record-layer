/*
 * RankIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsMultidimensionalProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@code M} type indexes.
 */
@Tag(Tags.RequiresFDB)
class MultidimensionalIndexTest extends FDBRecordStoreQueryTestBase {
    private static final long epochMean = 1690360647L;  // 2023/07/26

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsMultidimensionalProto.getDescriptor());
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("EventIntervals", DimensionsKeyExpression.of(field("calendar_name"),
                        concat(field("start_epoch"), field("end_epoch")),
                                concat(field("start_epoch"), field("end_epoch"))),
                        IndexTypes.MULTIDIMENSIONAL));
//        metaDataBuilder.addIndex("MyMultidimensionalRecord",
//                new Index("calendarNameStartEpoch",
//                        concat(field("calendar_name"), field("start_epoch")),
//                        IndexTypes.VALUE));
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("calendarNameEndEpochStartEpoch",
                        concat(field("calendar_name"), field("end_epoch"), field("start_epoch")),
                        IndexTypes.VALUE));

        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    public void loadRecords(@Nonnull final RecordMetaDataHook hook, final int seed, final int numSamples) throws Exception {
        final Random random = new Random(seed);
        final long epochStandardDeviation = 3L * 24L * 60L * 60L;
        final long durationCutOff = 30L * 60L; // meetings are at least 30 minutes long
        final long durationStandardDeviation = 60L * 60L;
        final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (long recNo = 0; recNo < numSamples; recNo++) {
                final long startEpoch = (long)(random.nextGaussian() * epochStandardDeviation) + epochMean;
                final long endEpoch = startEpoch + durationCutOff + (long)(Math.abs(random.nextGaussian()) * durationStandardDeviation);
                final long duration = endEpoch - startEpoch;
                Verify.verify(duration > 0L);
//                System.out.println("start: " + format.format(new Date(startEpoch * 1000)) + "; end: " + format.format(new Date(endEpoch * 1000)) +
//                                   "; duration: " + duration / 3600L + "h" + (duration % 3600L) / 60L + "m" + (duration % 3600L) % 60L + "s" +
//                                   "; startEpoch: " + startEpoch + "; endEpoch: " + endEpoch);
                final TestRecordsMultidimensionalProto.MyMultidimensionalRecord record =
                        TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                                .setRecNo(recNo)
                                .setCalendarName("business")
                                .setStartEpoch(startEpoch)
                                .setEndEpoch(endEpoch)
                                .build();
                recordStore.saveRecord(record);
            }
            commit(context);
        }
    }

    public void loadRecordsWithNulls(@Nonnull final RecordMetaDataHook hook, final int seed, final int numSamples) throws Exception {
        final Random random = new Random(seed);
        final long epochStandardDeviation = 3L * 24L * 60L * 60L;
        final long durationCutOff = 30L * 60L; // meetings are at least 30 minutes long
        final long durationStandardDeviation = 60L * 60L;
        final SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (long recNo = 0; recNo < numSamples; recNo++) {
                final Long startEpoch = random.nextFloat() < 0.10f ? null : ((long)(random.nextGaussian() * epochStandardDeviation) + epochMean);
                final Long endEpoch =
                        random.nextFloat() < 0.10f ? null : ((startEpoch == null ? epochMean : startEpoch) + durationCutOff +
                        (long)(Math.abs(random.nextGaussian()) * durationStandardDeviation));
                final Long duration = (startEpoch == null || endEpoch  == null) ? null : (endEpoch - startEpoch);
                Verify.verify(duration == null || duration > 0L);
//                System.out.println("start: " + format.format(new Date(startEpoch * 1000)) + "; end: " + format.format(new Date(endEpoch * 1000)) +
//                                   "; duration: " + duration / 3600L + "h" + (duration % 3600L) / 60L + "m" + (duration % 3600L) % 60L + "s" +
//                                   "; startEpoch: " + startEpoch + "; endEpoch: " + endEpoch);
                final var recordBuilder =
                        TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                                .setRecNo(recNo)
                                .setCalendarName("business");
                if (startEpoch != null) {
                    recordBuilder.setStartEpoch(startEpoch);
                }
                if (endEpoch != null) {
                    recordBuilder.setEndEpoch(endEpoch);
                }
                recordStore.saveRecord(recordBuilder.build());
            }
            commit(context);
        }
    }

    @Test
    void basicRead() throws Exception {
        loadRecords(NO_HOOK, 0, 5);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec);
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord.Builder myrec = TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder();
            myrec.mergeFrom(rec.getRecord());
            assertEquals("achilles", myrec.getCalendarName());
            commit(context);
        }
    }

    @Test
    void basicReadWithNulls() throws Exception {
        loadRecordsWithNulls(NO_HOOK, 0, 5);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec);
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord.Builder myrec = TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder();
            myrec.mergeFrom(rec.getRecord());
            assertEquals("achilles", myrec.getCalendarName());
            commit(context);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 300, 900, 5000})
    void indexRead(int numRecords) throws Exception {
        RecordMetaDataHook additionalStartEndIndex =
                metaDataBuilder -> metaDataBuilder.addIndex("MyMultidimensionalRecord",
                        new Index("calendarNameStartEpoch",
                                concat(field("calendar_name"), field("start_epoch"), field("end_epoch")),
                                IndexTypes.VALUE));
        loadRecords(additionalStartEndIndex, numRecords, numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                null, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalStartEndIndex);
            final RecordCursor<QueryResult> recordCursor =
                    indexPlan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().collect(Collectors.toList());
                    //.forEach(queryResult -> System.out.println(queryResult.getQueriedRecord().getRecord()));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalStartEndIndex);
            final RecordCursor<QueryResult> recordCursor =
                    indexPlan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().collect(Collectors.toList());
            commit(context);
        }

        System.out.println("========================================================");
        
        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan plan = planner.plan(query);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalStartEndIndex);
            final RecordCursor<QueryResult> recordCursor =
                    plan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().collect(Collectors.toList());
                    //.forEach(queryResult -> System.out.println(queryResult.getQueriedRecord().getRecord()));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalStartEndIndex);
            final RecordCursor<QueryResult> recordCursor =
                    plan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().collect(Collectors.toList());
                    //.forEach(queryResult -> System.out.println(queryResult.getQueriedRecord().getRecord()));
            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReadWithIn")
    void indexReadWithIn(int numRecords, int numIns) throws Exception {
        loadRecords(NO_HOOK, numRecords, numRecords);

        final ImmutableList.Builder<MultidimensionalIndexScanBounds.SpatialPredicate> orTermsBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Long> probesBuilder = ImmutableList.builder();
        final Random random = new Random(0);
        for (int i = 0; i < numIns; i++) {
            final long probe;
            if (i == 0) {
                probe = 1690470018L;
            } else {
                probe = (long)Math.abs(random.nextGaussian() * (3L * 60L * 60L)) + epochMean;
            }
            probesBuilder.add(probe);
            final MultidimensionalIndexScanBounds.Hypercube hyperCube =
//                    new MultidimensionalIndexScanBounds.Hypercube(TupleRange.allOf(Tuple.from("business")),
//                            ImmutableList.of(
//                                    TupleRange.betweenInclusive(Tuple.from(probe), null),
//                                    TupleRange.betweenInclusive(null, Tuple.from(probe))));
                            new MultidimensionalIndexScanBounds.Hypercube(
                                    ImmutableList.of(
                                            TupleRange.betweenInclusive(Tuple.from(probe), Tuple.from(probe)),
                                            TupleRange.betweenInclusive(null, null)));
            orTermsBuilder.add(hyperCube);
        }

        final MultidimensionalIndexScanBounds.Or orBounds =
                new MultidimensionalIndexScanBounds.Or(orTermsBuilder.build());

        final MultidimensionalIndexScanBounds.Hypercube greaterThanBounds =
                new MultidimensionalIndexScanBounds.Hypercube(
                        ImmutableList.of(
                                TupleRange.betweenInclusive(null, null),
                                TupleRange.betweenInclusive(Tuple.from(1690476099L), null)));

        final MultidimensionalIndexScanBounds.And andBounds =
                new MultidimensionalIndexScanBounds.And(ImmutableList.of(greaterThanBounds, orBounds));

        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new CompositeScanParameters(
                                new MultidimensionalIndexScanBounds(TupleRange.allOf(Tuple.from("business")), andBounds)),
                        false);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final RecordCursor<QueryResult> recordCursor =
                    indexPlan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().forEach(queryResult -> System.out.println(queryResult.getQueriedRecord().getRecord()));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final RecordCursor<QueryResult> recordCursor =
                    indexPlan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().collect(Collectors.toList());
            commit(context);
        }

        System.out.println("========================================================");

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").in(probesBuilder.build()),
                        Query.field("end_epoch").greaterThan(1690476099L));

        planner.setConfiguration(planner.getConfiguration().asBuilder().setOptimizeForIndexFilters(true).build());

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan plan = planner.plan(query);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final RecordCursor<QueryResult> recordCursor =
                    plan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().forEach(queryResult -> System.out.println(queryResult.getQueriedRecord().getRecord()));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final RecordCursor<QueryResult> recordCursor =
                    plan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            recordCursor.asStream().collect(Collectors.toList());
            //.forEach(queryResult -> System.out.println(queryResult.getQueriedRecord().getRecord()));
            commit(context);
        }
    }

    static Stream<Arguments> argumentsForIndexReadWithIn() {
        return Stream.of(
//                Arguments.of(100, 1)
//                Arguments.of(100, 10),
                Arguments.of(5000, 100)
//                Arguments.of(1000, 100),
//                Arguments.of(1000, 1000),
//                Arguments.of(5000, 10),
//                Arguments.of(5000, 100),
//                Arguments.of(5000, 1000)
        );
    }

    @SuppressWarnings("CheckStyle")
    static class HypercubeScanParameters implements IndexScanParameters {
        @Nonnull
        private final String calendarName;
        @Nullable
        private final Long xMinInclusive;
        @Nullable
        private final Long xMaxInclusive;
        @Nullable
        private final Long yMinInclusive;
        @Nullable
        private final Long yMaxInclusive;

        public HypercubeScanParameters(@Nonnull final String calendarName,
                                       @Nullable final Long xMinInclusive, @Nullable final Long xMaxInclusive,
                                       @Nullable final Long yMinInclusive, @Nullable final Long yMaxInclusive) {
            this.calendarName = calendarName;
            this.xMinInclusive = xMinInclusive;
            this.xMaxInclusive = xMaxInclusive;
            this.yMinInclusive = yMinInclusive;
            this.yMaxInclusive = yMaxInclusive;
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return 11;
        }

        @Nonnull
        @Override
        public IndexScanType getScanType() {
            return IndexScanType.BY_VALUE;
        }

        @Nonnull
        @Override
        public IndexScanBounds bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Index index, @Nonnull final EvaluationContext context) {
            return new MultidimensionalIndexScanBounds(TupleRange.allOf(Tuple.from(calendarName)),
                    new MultidimensionalIndexScanBounds.Hypercube(ImmutableList.of(
                            TupleRange.betweenInclusive(xMinInclusive == null ? null : Tuple.from(xMinInclusive),
                                    xMaxInclusive == null ? null : Tuple.from(xMaxInclusive)),
                            TupleRange.betweenInclusive(yMinInclusive == null ? null : Tuple.from(yMinInclusive),
                                    yMaxInclusive == null ? null : Tuple.from(yMaxInclusive)))));
        }

        @Override
        public boolean isUnique(@Nonnull final Index index) {
            return false;
        }

        @Nonnull
        @Override
        public String getScanDetails() {
            return "multidimensional";
        }

        @Override
        public void getPlannerGraphDetails(@Nonnull final ImmutableList.Builder<String> detailsBuilder, @Nonnull final ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        }

        @Nonnull
        @Override
        public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap) {
            throw new RecordCoreException("not supported");
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        public IndexScanParameters rebase(@Nonnull final AliasMap translationMap) {
            return this;
        }

        @Override
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return false;
        }

        @Override
        public int semanticHashCode() {
            return 0;
        }
    }

    @SuppressWarnings("CheckStyle")
    static class CompositeScanParameters implements IndexScanParameters {
        @Nonnull
        private final MultidimensionalIndexScanBounds scanBounds;

        public CompositeScanParameters(@Nonnull final MultidimensionalIndexScanBounds scanBounds) {
            this.scanBounds = scanBounds;
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return 13;
        }

        @Nonnull
        @Override
        public IndexScanType getScanType() {
            return IndexScanType.BY_VALUE;
        }

        @Nonnull
        @Override
        public IndexScanBounds bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Index index, @Nonnull final EvaluationContext context) {
            return scanBounds;
        }

        @Override
        public boolean isUnique(@Nonnull final Index index) {
            return false;
        }

        @Nonnull
        @Override
        public String getScanDetails() {
            return "multidimensional";
        }

        @Override
        public void getPlannerGraphDetails(@Nonnull final ImmutableList.Builder<String> detailsBuilder, @Nonnull final ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        }

        @Nonnull
        @Override
        public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap) {
            throw new RecordCoreException("not supported");
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return ImmutableSet.of();
        }

        @Nonnull
        @Override
        public IndexScanParameters rebase(@Nonnull final AliasMap translationMap) {
            return this;
        }

        @Override
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            return false;
        }

        @Override
        public int semanticHashCode() {
            return 0;
        }
    }
}
