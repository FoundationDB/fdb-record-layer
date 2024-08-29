/*
 * MultidimensionalIndexTestBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsMultidimensionalProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_NODE;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.unbounded;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.only;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.dimensions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.filterPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexScanParameters;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.multidimensional;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.prefix;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.queryComponents;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.suffix;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedPrimaryKeyDistinctPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.unorderedUnionPlan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for multidimensional type indexes.
 */
@Tag(Tags.RequiresFDB)
public abstract class MultidimensionalIndexTestBase extends FDBRecordStoreQueryTestBase {
    private static final Logger logger = LoggerFactory.getLogger(MultidimensionalIndexTestBase.class);

    private static final IndexQueryabilityFilter noMultidimensionalIndexes = new IndexQueryabilityFilter() {
        @Override
        public boolean isQueryable(@Nonnull final Index index) {
            return !index.getType().equals(IndexTypes.MULTIDIMENSIONAL);
        }

        @Override
        public int queryHash(@Nonnull final QueryHashKind hashKind) {
            return 0;
        }
    };

    private static final long epochMean = 1690360647L;  // 2023/07/26
    private static final long durationCutOff = 30L * 60L; // meetings are at least 30 minutes long
    private static final long expirationCutOff = 30L * 24L * 60L * 60L; // records expire in a month

    private static final SimpleDateFormat timeFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsMultidimensionalProto.getDescriptor());
        metaDataBuilder.getRecordType("MyMultidimensionalRecord").setPrimaryKey(concat(ImmutableList.of(field("info").nest("rec_domain"), field("rec_no"))));
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("calendarNameEndEpochStartEpoch",
                        concat(field("calendar_name"), field("end_epoch"), field("start_epoch")),
                        IndexTypes.VALUE));

        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    @CanIgnoreReturnValue
    RecordMetaDataBuilder addCalendarNameStartEpochIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("calendarNameStartEpoch",
                        concat(field("calendar_name"), field("start_epoch"), field("end_epoch")),
                        IndexTypes.VALUE));
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    RecordMetaDataBuilder addMultidimensionalIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder,
                                                   @Nonnull final String storage,
                                                   final boolean storeHilbertValues,
                                                   final boolean useNodeSlotIndex) {
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("EventIntervals", DimensionsKeyExpression.of(field("calendar_name"),
                        concat(field("start_epoch"), field("end_epoch"))),
                        IndexTypes.MULTIDIMENSIONAL, ImmutableMap.of(IndexOptions.RTREE_STORAGE, storage,
                        IndexOptions.RTREE_STORE_HILBERT_VALUES, Boolean.toString(storeHilbertValues),
                        IndexOptions.RTREE_USE_NODE_SLOT_INDEX, Boolean.toString(useNodeSlotIndex))));
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    RecordMetaDataBuilder addUnprefixedMultidimensionalIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder,
                                                             @Nonnull final String storage,
                                                             final boolean storeHilbertValues,
                                                             final boolean useNodeSlotIndex) {
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("UnprefixedEventIntervals", DimensionsKeyExpression.of(null,
                        concat(field("start_epoch"), field("end_epoch"))),
                        IndexTypes.MULTIDIMENSIONAL, ImmutableMap.of(IndexOptions.RTREE_STORAGE, storage,
                        IndexOptions.RTREE_STORE_HILBERT_VALUES, Boolean.toString(storeHilbertValues),
                        IndexOptions.RTREE_USE_NODE_SLOT_INDEX, Boolean.toString(useNodeSlotIndex))));
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    RecordMetaDataBuilder addUnprefixedSuffixedMultidimensionalIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder,
                                                                     @Nonnull final String storage,
                                                                     final boolean storeHilbertValues,
                                                                     final boolean useNodeSlotIndex) {
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("UnprefixedSuffixedEventIntervals", DimensionsKeyExpression.of(null,
                        concat(field("start_epoch"), field("end_epoch")), field("calendar_name")),
                        IndexTypes.MULTIDIMENSIONAL, ImmutableMap.of(IndexOptions.RTREE_STORAGE, storage,
                        IndexOptions.RTREE_STORE_HILBERT_VALUES, Boolean.toString(storeHilbertValues),
                        IndexOptions.RTREE_USE_NODE_SLOT_INDEX, Boolean.toString(useNodeSlotIndex))));
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    RecordMetaDataBuilder addAdditionalValueMultidimensionalIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder,
                                                                  @Nonnull final String storage,
                                                                  final boolean storeHilbertValues,
                                                                  final boolean useNodeSlotIndex) {
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("EventIntervalsWithAdditionalValue",
                        new KeyWithValueExpression(concat(DimensionsKeyExpression.of(field("info").nest("rec_domain"),
                                concat(field("start_epoch"), field("end_epoch"))), field("calendar_name")), 3),
                        IndexTypes.MULTIDIMENSIONAL,
                        ImmutableMap.of(IndexOptions.RTREE_STORAGE, storage,
                                IndexOptions.RTREE_STORE_HILBERT_VALUES, Boolean.toString(storeHilbertValues),
                                IndexOptions.RTREE_USE_NODE_SLOT_INDEX, Boolean.toString(useNodeSlotIndex))));
        return metaDataBuilder;
    }

    static Function<Integer, Message> getRecordGenerator(@Nonnull Random random, @Nonnull List<String> calendarNames) {
        final long epochStandardDeviation = 3L * 24L * 60L * 60L;
        final long durationCutOff = 30L * 60L; // meetings are at least 30 minutes long
        final long durationStandardDeviation = 60L * 60L;
        final long expirationStandardDeviation = 24L * 60L * 60L;
        return recNo -> {
            final String calendarName = calendarNames.get(random.nextInt(calendarNames.size()));
            final long startEpoch = (long)(random.nextGaussian() * epochStandardDeviation) + epochMean;
            final long endEpoch = startEpoch + durationCutOff + (long)(Math.abs(random.nextGaussian()) * durationStandardDeviation);
            final long duration = endEpoch - startEpoch;
            Verify.verify(duration > 0L);
            final long expirationEpoch = endEpoch + expirationCutOff + (long)(Math.abs(random.nextGaussian()) * expirationStandardDeviation);
            logRecord(calendarName, startEpoch, endEpoch, expirationEpoch);
            return TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                    .setRecNo(recNo)
                    .setCalendarName(calendarName)
                    .setStartEpoch(startEpoch)
                    .setEndEpoch(endEpoch)
                    .setExpirationEpoch(expirationEpoch)
                    .build();
        };
    }

    static Function<Integer, Message> getRecordWithNullGenerator(@Nonnull Random random, @Nonnull List<String> calendarNames) {
        final long epochStandardDeviation = 3L * 24L * 60L * 60L;
        final long durationStandardDeviation = 60L * 60L;
        final long expirationStandardDeviation = 24L * 60L * 60L;
        return recNo -> {
            final String calendarName = calendarNames.get(random.nextInt(calendarNames.size()));
            final Long startEpoch = random.nextFloat() < 0.10f ? null : ((long)(random.nextGaussian() * epochStandardDeviation) + epochMean);
            final Long endEpoch =
                    random.nextFloat() < 0.10f ? null : ((startEpoch == null ? epochMean : startEpoch) + durationCutOff +
                                                                 (long)(Math.abs(random.nextGaussian()) * durationStandardDeviation));
            final Long duration = (startEpoch == null || endEpoch  == null) ? null : (endEpoch - startEpoch);
            Verify.verify(duration == null || duration > 0L);
            final Long expirationEpoch =
                    random.nextFloat() < 0.10f
                    ? null
                    : (endEpoch == null ? epochMean : endEpoch) + expirationCutOff  +
                            (long)(Math.abs(random.nextGaussian()) * expirationStandardDeviation);
            logRecord(calendarName, startEpoch, endEpoch, expirationEpoch);
            final var recordBuilder =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                            .setRecNo(recNo)
                            .setCalendarName(calendarName);
            if (startEpoch != null) {
                recordBuilder.setStartEpoch(startEpoch);
            }
            if (endEpoch != null) {
                recordBuilder.setEndEpoch(endEpoch);
            }
            if (expirationEpoch != null) {
                recordBuilder.setExpirationEpoch(expirationEpoch);
            }
            return recordBuilder.build();
        };
    }

    public void loadRecords(final boolean useAsync, final boolean withNulls, @Nonnull final RecordMetaDataHook hook,
                            final long seed, final List<String> calendarNames, final int numSamples)  {
        final Random random = new Random(seed);
        final var recordGenerator = withNulls ? getRecordWithNullGenerator(random, calendarNames) : getRecordGenerator(random, calendarNames);
        if (useAsync) {
            Assertions.assertDoesNotThrow(() -> batch(hook, numSamples, 500, recNo -> recordStore.saveRecord(recordGenerator.apply(recNo))));
        } else {
            Assertions.assertDoesNotThrow(() -> batchAsync(hook, numSamples, 500, recNo -> recordStore.saveRecordAsync(recordGenerator.apply(recNo))));
        }
    }

    public void deleteRecords(final boolean useAsync, @Nonnull final RecordMetaDataHook hook, final long seed, final int numRecords,
                              final int numDeletes) throws Exception {
        Preconditions.checkArgument(numDeletes <= numRecords);
        final Random random = new Random(seed);
        final List<Integer> recNos = IntStream.range(0, numRecords)
                .boxed()
                .collect(Collectors.toList());
        Collections.shuffle(recNos, random);
        final List<Integer> recNosToBeDeleted = recNos.subList(0, numDeletes);
        if (useAsync) {
            batchAsync(hook, recNosToBeDeleted.size(), 500, recNo -> recordStore.deleteRecordAsync(Tuple.from(recNo)));
        } else {
            batch(hook, recNosToBeDeleted.size(), 500, recNo -> recordStore.deleteRecord(Tuple.from(recNo)));
        }
    }

    private <T> int batch(final RecordMetaDataHook hook, final int numRecords, final int batchSize, Consumer<Integer> recordConsumer) throws Exception {
        int numRecordsCommitted = 0;
        while (numRecordsCommitted < numRecords) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                int recNoInBatch;

                for (recNoInBatch = 0; numRecordsCommitted + recNoInBatch < numRecords && recNoInBatch < batchSize; recNoInBatch++) {
                    recordConsumer.accept(numRecordsCommitted + recNoInBatch);
                }
                commit(context);
                numRecordsCommitted += recNoInBatch;
                logger.info("committed batch, numRecordsCommitted = {}", numRecordsCommitted);
            }
        }
        return numRecordsCommitted;
    }

    private <T> int batchAsync(final RecordMetaDataHook hook, final int numRecords, final int batchSize, Function<Integer, CompletableFuture<T>> recordConsumer) throws Exception {
        int numRecordsCommitted = 0;
        while (numRecordsCommitted < numRecords) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                int recNoInBatch;
                final var futures = new ArrayList<CompletableFuture<T>>();

                for (recNoInBatch = 0; numRecordsCommitted + recNoInBatch < numRecords && recNoInBatch < batchSize; recNoInBatch++) {
                    futures.add(recordConsumer.apply(numRecordsCommitted + recNoInBatch));
                }

                // wait and then commit
                AsyncUtil.whenAll(futures).get();
                commit(context);
                numRecordsCommitted += recNoInBatch;
                logger.info("committed batch, numRecordsCommitted = {}", numRecordsCommitted);
            }
        }
        return numRecordsCommitted;
    }

    private static void logRecord(@Nonnull final String calendarName, final Long startEpoch, final Long endEpoch, final Long expirationEpoch) {
        if (logger.isTraceEnabled()) {
            final Long duration = (startEpoch == null || endEpoch  == null) ? null : (endEpoch - startEpoch);
            Verify.verify(duration == null || duration > 0L);

            final String startAsString = startEpoch == null ? "null" : timeFormat.format(new Date(startEpoch * 1000));
            final String endAsString = endEpoch == null ? "null" : timeFormat.format(new Date(endEpoch * 1000));
            final String durationAsString = duration == null ? "null" : (duration / 3600L + "h" +
                                                                         (duration % 3600L) / 60L + "m" + (duration % 3600L) % 60L + "s");
            logger.trace("calendarName: " + calendarName +
                         "; start: " + startAsString + "; end: " + endAsString +
                         "; duration: " + durationAsString +
                         "; startEpoch: " + (startEpoch == null ? "null" : startEpoch) +
                         "; endEpoch: " + (endEpoch == null ? "null" : endEpoch) +
                         "; expirationEpoch: " + (expirationEpoch == null ? "null" : expirationEpoch));
        }
    }

    public void loadSpecificRecordsWithNullsAndMins(final boolean useAsync, @Nonnull final RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord record1 =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                            .setRecNo(1L)
                            .setCalendarName("business")
                            .setStartEpoch(Long.MIN_VALUE)
                            .setEndEpoch(1L)
                            .setExpirationEpoch(2L)
                            .build();
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord record2 =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                    .setRecNo(2L)
                    .setCalendarName("business")
                    .setStartEpoch(Long.MIN_VALUE)
                    .setEndEpoch(Long.MIN_VALUE)
                    .setExpirationEpoch(3L)
                    .build();
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord record3 =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                    .setRecNo(3L)
                    .setCalendarName("business")
                    .setEndEpoch(1L)
                    .setExpirationEpoch(3L)
                    .build();
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord record4 =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                    .setRecNo(4L)
                    .setCalendarName("business")
                    .setExpirationEpoch(3L)
                    .build();
            if (useAsync) {
                final List<CompletableFuture<FDBStoredRecord<Message>>> futures = new ArrayList<>();
                futures.add(recordStore.saveRecordAsync(record1));
                futures.add(recordStore.saveRecordAsync(record2));
                futures.add(recordStore.saveRecordAsync(record3));
                futures.add(recordStore.saveRecordAsync(record4));
                AsyncUtil.whenAll(futures).get();
            } else {
                recordStore.saveRecord(record1);
                recordStore.saveRecord(record2);
                recordStore.saveRecord(record3);
                recordStore.saveRecord(record4);
            }
            commit(context);
        }
    }

    public void loadSpecificRecordsWithDuplicates(final boolean useAsync, @Nonnull final RecordMetaDataHook hook, int numRecords) throws Exception {
        final Function<Integer, NonnullPair<Message, Message>> getRecords = recNo -> {
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord record1 =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                            .setRecNo(2L * (long)recNo)
                            .setCalendarName("business")
                            .setStartEpoch(0L)
                            .setEndEpoch(1L)
                            .setExpirationEpoch(2L)
                            .build();
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord record2 =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                            .setRecNo(2L * (long)recNo + 1L)
                            .setCalendarName("private")
                            .setStartEpoch(0L)
                            .setEndEpoch(1L)
                            .setExpirationEpoch(2L)
                            .build();
            return NonnullPair.of(record1, record2);
        };
        if (useAsync) {
            batchAsync(hook, numRecords, 500, recNo -> {
                final NonnullPair<Message, Message> records = getRecords.apply(recNo);
                return AsyncUtil.whenAll(ImmutableList.of(
                        recordStore.saveRecordAsync(records.getLeft()),
                        recordStore.saveRecordAsync(records.getRight())));
            });
        } else {
            batch(hook, numRecords, 500, recNo -> {
                final NonnullPair<Message, Message> records = getRecords.apply(recNo);
                recordStore.saveRecord(records.getLeft());
                recordStore.saveRecord(records.getRight());
            });
        }
    }

    void basicReadTest(final boolean useAsync, @Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndex = metaDataBuilder -> addMultidimensionalIndex(metaDataBuilder, storage,
                storeHilbertValues, useNodeSlotIndex);
        loadRecords(useAsync, false, additionalIndex, 0, ImmutableList.of("business"), 500);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalIndex);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(null, 1L));
            assertNotNull(rec);
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord.Builder recordBuilder =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder();
            recordBuilder.mergeFrom(rec.getRecord());
            assertEquals("business", recordBuilder.getCalendarName());
            commit(context);
        }
    }

    void basicReadWithNullsTest(final boolean useAsync, @Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndex = metaDataBuilder -> addMultidimensionalIndex(metaDataBuilder, storage,
                storeHilbertValues, useNodeSlotIndex);
        loadRecords(useAsync, false, additionalIndex, 0, ImmutableList.of("business"), 500);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalIndex);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(null, 1L));
            assertNotNull(rec);
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord.Builder recordBuilder =
                    TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder();
            recordBuilder.mergeFrom(rec.getRecord());
            assertEquals("business", recordBuilder.getCalendarName());
            commit(context);
        }
    }

    void indexReadTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                   final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business"), numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                (Long)null, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);
        Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                indexPlan()
                        .where(indexName("EventIntervals"))
                        .and(indexScanParameters(
                                multidimensional()
                                        .where(prefix(range("[[business],[business]]")))
                                        .and(dimensions(range("([null],[1690378647]]"), range("[[1690364247],>")))
                                        .and(suffix(unbounded())))));
        actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void indexReadWithNullsTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                            final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, true, additionalIndexes, seed, ImmutableList.of("business"), numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                (Long)null, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);
        Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.or(
                                Query.field("start_epoch").isNull(),
                                Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive)),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                unorderedPrimaryKeyDistinctPlan(
                        unorderedUnionPlan(
                                indexPlan()
                                        .where(indexName("EventIntervals"))
                                        .and(indexScanParameters(
                                                multidimensional()
                                                        .where(prefix(range("[[business],[business]]")))
                                                        .and(dimensions(range("([null],[1690378647]]"), range("[[1690364247],>")))
                                                        .and(suffix(unbounded())))),
                                indexPlan().where(indexName("calendarNameStartEpoch"))
                                )));
        actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void indexReadWithNullsAndMinsTest1(final boolean useAsync) throws Exception {
        RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, BY_NODE.toString(), true, false);
                };
        loadSpecificRecordsWithNullsAndMins(useAsync, additionalIndexes);
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                (Long)null, 0L,
                                0L, null),
                        false);
        Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.or(
                                Query.field("start_epoch").isNull(),
                                Query.field("start_epoch").lessThanOrEquals(0L)),
                        Query.field("end_epoch").greaterThanOrEquals(0L));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                unorderedPrimaryKeyDistinctPlan(
                        unorderedUnionPlan(
                                indexPlan()
                                        .where(indexName("EventIntervals"))
                                        .and(indexScanParameters(
                                                multidimensional()
                                                        .where(prefix(range("[[business],[business]]")))
                                                        .and(dimensions(range("([null],[0]]"), range("[[0],>")))
                                                        .and(suffix(unbounded())))),
                                indexPlan().where(indexName("calendarNameStartEpoch"))
                        )));
        actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void indexReadWithNullsAndMinsTest2(final boolean useAsync) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, BY_NODE.toString(), true, false);
                };
        loadSpecificRecordsWithNullsAndMins(useAsync, additionalIndexes);
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                Long.MIN_VALUE, 0L,
                                0L, null),
                        false);
        Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").lessThanOrEquals(0L),
                        Query.field("end_epoch").greaterThanOrEquals(0L));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                indexPlan()
                        .where(indexName("EventIntervals"))
                        .and(indexScanParameters(
                                multidimensional()
                                        .where(prefix(range("[[business],[business]]")))
                                        .and(dimensions(range("([null],[0]]"), range("[[0],>")))
                                        .and(suffix(unbounded())))));
        actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void indexReadIsNullTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                         final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, true, additionalIndexes, seed, ImmutableList.of("business"), numRecords);
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                (Long)null, Long.MIN_VALUE,
                                null, null),
                        false);
        final Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").isNull());

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner does not pick up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan plan2 = planQuery(query);
        // make sure that een now we don't pick the md index
        Assertions.assertTrue(plan2.hasIndexScan("calendarNameStartEpoch"));
    }

    void indexReadWithIn(final boolean useAsync, final long seed, final int numRecords, final int numIns) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, BY_NODE.toString(), true, false);
                };

        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business"), numRecords);

        final ImmutableList.Builder<MultidimensionalIndexScanBounds.SpatialPredicate> orTermsBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Long> probesBuilder = ImmutableList.builder();
        final Random random = new Random(0);
        for (int i = 0; i < numIns; i++) {
            final long probe = (long)Math.abs(random.nextGaussian() * (3L * 60L * 60L)) + epochMean;
            probesBuilder.add(probe);
            final MultidimensionalIndexScanBounds.Hypercube hyperCube =
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
                                new MultidimensionalIndexScanBounds(TupleRange.allOf(Tuple.from("business")), andBounds, TupleRange.ALL)),
                        false);
        final Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").in(probesBuilder.build()),
                        Query.field("end_epoch").greaterThanOrEquals(1690476099L));

        planner.setConfiguration(planner.getConfiguration().asBuilder().setOptimizeForIndexFilters(true).build());

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner does not pick up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan plan2 = planQuery(query);
        // make sure that een now we don't pick the md index
        Assertions.assertTrue(plan2.hasIndexScan("calendarNameStartEpoch"));
    }

    void indexReadsAfterDeletesTest(final boolean useAsync, final long seed, final int numRecords, final int numDeletes,
                                @Nonnull final String storage, final boolean storeHilbertValues,
                                final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, true, additionalIndexes, seed, ImmutableList.of("business"), numRecords);
        deleteRecords(useAsync, additionalIndexes, seed, numRecords, numDeletes);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                (Long)null, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);
        final Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.or(Query.field("start_epoch").isNull(),
                                Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive)),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void indexSkipScanTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                       final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business", "private"), numRecords);

        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business", "private",
                                null, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);
        final Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.or(
                                Query.field("start_epoch").isNull(),
                                Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive)),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner does not pick up the md-index
        // TODO relax the constraint on an equality-bound prefix while matching the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan plan2 = planQuery(query);
        // make sure that een now we don't pick the md index
        Assertions.assertTrue(plan2.hasRecordScan());
    }

    void continuationTest(final boolean useAsync) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, BY_NODE.toString(), true, false);
                };
        loadRecords(useAsync, true, additionalIndexes, 0, ImmutableList.of("business", "private"), 500);

        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 10L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business", "private",
                                Long.MIN_VALUE, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);

        Set<Message> actualResults = getResultsWithContinuations(additionalIndexes, indexPlan, 4);

        final QueryComponent filter =
                Query.and(
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner does not pick up the md-index
        // TODO relax the constraint on an equality-bound prefix while matching the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan plan2 = planQuery(query);
        // make sure that een now we don't pick the md index
        Assertions.assertTrue(plan2.hasRecordScan());
    }

    @SuppressWarnings({"resource", "SameParameterValue"})
    private Set<Message> getResultsWithContinuations(@Nonnull final RecordMetaDataHook additionalIndexes,
                                                     @Nonnull final RecordQueryPlan plan, final int batchSize) throws Exception {
        final Set<Message> results = Sets.newHashSet();
        byte[] continuation = null;
        do {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, additionalIndexes);
                final RecordCursorIterator<QueryResult> recordCursorIterator =
                        plan.executePlan(recordStore,
                                        EvaluationContext.empty(), continuation,
                                        ExecuteProperties.SERIAL_EXECUTE.setReturnedRowLimit(batchSize))
                                .asIterator();
                int numRecordsInBatch = 0;
                while (recordCursorIterator.hasNext()) {
                    // make sure we are not adding duplicates
                    Assertions.assertTrue(results.add(Objects.requireNonNull(recordCursorIterator.next()).getMessage()));
                    numRecordsInBatch ++;
                }
                continuation = recordCursorIterator.getContinuation();
                // Must be the returned row limit or smaller if this is the last batch.
                Assertions.assertTrue((continuation == null && numRecordsInBatch <= 4) || numRecordsInBatch == 4);
                commit(context);
            }
        } while (continuation != null);
        return results;
    }

    void coveringIndexScanWithFetchTest(final boolean useAsync, @Nonnull final String storage, final boolean storeHilbertValues,
                                    final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, true, additionalIndexes, 0, ImmutableList.of("business", "private"), 500);

        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 10L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business", "private",
                                null, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);

        final RecordType myMultidimensionalRecord = recordStore.getRecordMetaData().getRecordType("MyMultidimensionalRecord");
        final Index index = recordStore.getRecordMetaData().getIndex("EventIntervals");
        final AvailableFields availableFields =
                AvailableFields.fromIndex(myMultidimensionalRecord,
                        index,
                        PlannableIndexTypes.DEFAULT,
                        concat(Key.Expressions.field("info").nest("rec_domain"), Key.Expressions.field("rec_no")),
                        indexPlan);
        final IndexKeyValueToPartialRecord.Builder indexKeyToPartialRecord =
                Objects.requireNonNull(availableFields.buildIndexKeyValueToPartialRecord(myMultidimensionalRecord));
        final RecordQueryCoveringIndexPlan coveringIndexPlan =
                new RecordQueryCoveringIndexPlan(indexPlan,
                        "MyMultidimensionalRecord",
                        AvailableFields.NO_FIELDS,
                        indexKeyToPartialRecord.build());
        final Set<Message> actualResults = getResults(additionalIndexes, coveringIndexPlan);
        Assertions.assertEquals(57, actualResults.size());
        actualResults.forEach(record -> {
            final Descriptors.Descriptor descriptorForType = record.getDescriptorForType();
            final Descriptors.FieldDescriptor calendarNameField = descriptorForType.findFieldByName("calendar_name");
            Assertions.assertTrue(record.hasField(calendarNameField));
            Assertions.assertTrue(Sets.newHashSet("business", "private")
                    .contains(Objects.requireNonNull((String)record.getField(calendarNameField))));

            final Descriptors.FieldDescriptor startEpochField = descriptorForType.findFieldByName("start_epoch");
            final Descriptors.FieldDescriptor endEpochField = descriptorForType.findFieldByName("end_epoch");
            final Long startEpoch = record.hasField(startEpochField) ? (Long)record.getField(startEpochField) : null;
            final Long endEpoch = record.hasField(endEpochField) ? (Long)record.getField(endEpochField) : null;
            Assertions.assertTrue(startEpoch == null || startEpoch > 0);
            Assertions.assertTrue(endEpoch == null || endEpoch > 0);
            Assertions.assertTrue(startEpoch == null || endEpoch == null || startEpoch < endEpoch);

            final Descriptors.FieldDescriptor expirationEpochField = descriptorForType.findFieldByName("expiration_epoch");
            Assertions.assertFalse(record.hasField(expirationEpochField));
        });

        final var fetchPlan = new RecordQueryFetchFromPartialRecordPlan(coveringIndexPlan, TranslateValueFunction.UNABLE_TO_TRANSLATE,
                Type.any(), RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY);
        final Set<Message> actualResultsAfterFetch = getResults(additionalIndexes, fetchPlan);

        Assertions.assertEquals(57, actualResultsAfterFetch.size());
        actualResults.forEach(record -> {
            final Descriptors.Descriptor descriptorForType = record.getDescriptorForType();

            final Descriptors.FieldDescriptor endEpochField = descriptorForType.findFieldByName("end_epoch");
            final Long endEpoch = record.hasField(endEpochField) ? (Long)record.getField(endEpochField) : null;
            Assertions.assertTrue(endEpoch == null || endEpoch > 0);
            final Descriptors.FieldDescriptor expirationEpochField = descriptorForType.findFieldByName("expiration_epoch");
            final Long expirationEpoch = record.hasField(expirationEpochField) ? (Long)record.getField(expirationEpochField) : null;
            Assertions.assertTrue(expirationEpoch == null || expirationEpoch > 0);
            Assertions.assertTrue(endEpoch == null || expirationEpoch == null || endEpoch < expirationEpoch);
        });
    }

    void coveringIndexReadTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                           final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business"), numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .setRequiredResults(ImmutableList.of(field("calendar_name"), field("start_epoch"), field("end_epoch")))
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setRequiredResults(ImmutableList.of(field("calendar_name"), field("start_epoch"), field("end_epoch")))
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                coveringIndexPlan()
                        .where(indexPlanOf(indexPlan()
                                .where(indexName("EventIntervals"))
                                .and(indexScanParameters(
                                        multidimensional()
                                                .where(prefix(range("[[business],[business]]")))
                                                .and(dimensions(range("([null],[1690378647]]"), range("[[1690364247],>")))
                                                .and(suffix(unbounded())))))));
        final Set<Message> actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void indexScan3DTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                     final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndex = metaDataBuilder ->
                metaDataBuilder.addIndex("MyMultidimensionalRecord",
                        new Index("EventIntervals3D", DimensionsKeyExpression.of(field("calendar_name"),
                                concat(field("start_epoch"), field("end_epoch"), field("expiration_epoch"))),
                                IndexTypes.MULTIDIMENSIONAL, ImmutableMap.of(IndexOptions.RTREE_STORAGE, storage,
                                IndexOptions.RTREE_STORE_HILBERT_VALUES, Boolean.toString(storeHilbertValues),
                                IndexOptions.RTREE_USE_NODE_SLOT_INDEX, Boolean.toString(useNodeSlotIndex))));

        loadRecords(useAsync, true, additionalIndex, seed, ImmutableList.of("business"), numRecords);

        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals3D",
                        new HypercubeScanParameters("business",
                                Long.MIN_VALUE, intervalEndInclusive,
                                intervalStartInclusive, null,
                                epochMean + expirationCutOff, null),
                        false);
        Set<Message> actualResults = getResults(additionalIndex, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive),
                        Query.field("expiration_epoch").greaterThanOrEquals(epochMean + expirationCutOff));

        planner.setConfiguration(planner.getConfiguration().asBuilder().setOptimizeForIndexFilters(true).build());

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndex, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                indexPlan()
                        .where(indexName("EventIntervals3D"))
                        .and(indexScanParameters(
                                multidimensional()
                                        .where(prefix(range("[[business],[business]]")))
                                        .and(dimensions(range("([null],[1690378647]]"),
                                                range("[[1690364247],>"),
                                                range("[[1692952647],>")
                                        ))
                                        .and(suffix(unbounded())))));
        actualResults = getResults(additionalIndex, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void wrongDimensionTypes(final boolean useAsync) {
        final RecordMetaDataHook additionalIndex = metaDataBuilder ->
                metaDataBuilder.addIndex("MyMultidimensionalRecord",
                        new Index("IndexWithWrongDimensions", DimensionsKeyExpression.of(field("calendar_name"),
                                concat(field("start_epoch"), field("calendar_name"), field("expiration_epoch"))),
                                IndexTypes.MULTIDIMENSIONAL, ImmutableMap.of(IndexOptions.RTREE_STORAGE, BY_NODE.toString(),
                                IndexOptions.RTREE_STORE_HILBERT_VALUES, "true")));

        final var cause = Assertions.assertThrows(AssertionFailedError.class, () ->
                loadRecords(useAsync, true, additionalIndex, 0, ImmutableList.of("business"), 10)).getCause();
        Assertions.assertEquals(KeyExpression.InvalidExpressionException.class, cause.getClass());
    }

    void deleteWhereTest(final boolean useAsync, @Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    metaDataBuilder.addIndex("MyMultidimensionalRecord",
                            new Index("EventIntervals", DimensionsKeyExpression.of(
                                    concat(field("info").nest("rec_domain"), field("calendar_name")),
                                    concat(field("start_epoch"), field("end_epoch"))),
                                    IndexTypes.MULTIDIMENSIONAL,
                                    ImmutableMap.of(IndexOptions.RTREE_STORAGE, storage,
                                            IndexOptions.RTREE_STORE_HILBERT_VALUES, Boolean.toString(storeHilbertValues),
                                            IndexOptions.RTREE_USE_NODE_SLOT_INDEX, Boolean.toString(useNodeSlotIndex))));
                    metaDataBuilder.removeIndex("MyMultidimensionalRecord$calendar_name");
                    metaDataBuilder.removeIndex("calendarNameEndEpochStartEpoch");
                };
        loadRecords(useAsync, true, additionalIndexes, 0, ImmutableList.of("business", "private"), 500);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalIndexes);

            recordStore.deleteRecordsWhere(Query.field("info").matches(Query.field("rec_domain").isNull()));
            commit(context);
        }

        final var bounds = new MultidimensionalIndexScanBounds.Hypercube(ImmutableList.of(
                TupleRange.betweenInclusive(null, null),
                TupleRange.betweenInclusive(null, null)));

        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new CompositeScanParameters(
                                new MultidimensionalIndexScanBounds(TupleRange.allOf(Tuple.from(null, "business")), bounds, TupleRange.ALL)),
                        false);

        final Set<Message> actualResults = getResults(additionalIndexes, indexPlan);
        Assertions.assertTrue(actualResults.isEmpty());
    }

    void unprefixedIndexReadTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                             final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addUnprefixedMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business", "private"), numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("UnprefixedEventIntervals",
                        new HypercubeScanParameters(null,
                                (Long)null, intervalEndInclusive,
                                intervalStartInclusive, null),
                        false);
        Set<Message> actualResults = getResults(additionalIndexes, indexPlan);

        final QueryComponent filter =
                Query.and(
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);
        Assertions.assertEquals(expectedResults, actualResults);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                indexPlan()
                        .where(indexName("UnprefixedEventIntervals"))
                        .and(indexScanParameters(
                                multidimensional()
                                        .where(prefix(unbounded()))
                                        .and(dimensions(range("([null],[1690378647]]"), range("[[1690364247],>")))
                                        .and(suffix(unbounded())))));
        actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void unprefixedSuffixedIndexReadTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                                     final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addUnprefixedSuffixedMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business", "private"), numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;

        final QueryComponent filter =
                Query.and(
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive),
                        Query.field("calendar_name").equalsValue("business"));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                indexPlan()
                        .where(indexName("UnprefixedSuffixedEventIntervals"))
                        .and(indexScanParameters(
                                multidimensional()
                                        .where(prefix(unbounded()))
                                        .and(dimensions(range("([null],[1690378647]]"), range("[[1690364247],>")))
                                        .and(suffix(range("[[business],[business]]"))))));
        final Set<Message> actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void unprefixedSuffixedIndexReadWithResidualsTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                                                  final boolean storeHilbertValues, final Boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addUnprefixedSuffixedMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business", "private"), numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;

        final QueryComponent filter =
                Query.and(
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive),
                        Query.field("calendar_name").greaterThanOrEquals("business"),
                        Query.field("info").matches(Query.field("rec_domain").isNull()));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                fetchFromPartialRecordPlan(
                        filterPlan(
                                coveringIndexPlan()
                                        .where(indexPlanOf(
                                                indexPlan()
                                                        .where(indexName("UnprefixedSuffixedEventIntervals"))
                                                        .and(indexScanParameters(
                                                                multidimensional()
                                                                        .where(prefix(unbounded()))
                                                                        .and(dimensions(range("([null],[1690378647]]"), range("[[1690364247],>")))
                                                                        .and(suffix(range("[[business],>"))))))))
                                .where(queryComponents(only(PrimitiveMatchers.equalsObject(Query.field("info").matches(Query.field("rec_domain").isNull())))))));
        final Set<Message> actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    void indexReadWithAdditionalValueTest(final boolean useAsync, final long seed, final int numRecords, @Nonnull final String storage,
                                      final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addAdditionalValueMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadRecords(useAsync, false, additionalIndexes, seed, ImmutableList.of("business", "private"), numRecords);
        final long intervalStartInclusive = epochMean + 3600L;
        final long intervalEndInclusive = epochMean + 5L * 3600L;

        final QueryComponent filter =
                Query.and(
                        Query.field("info").matches(Query.field("rec_domain").isNull()),
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").lessThanOrEquals(intervalEndInclusive),
                        Query.field("end_epoch").greaterThanOrEquals(intervalStartInclusive));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                fetchFromPartialRecordPlan(
                        filterPlan(
                                coveringIndexPlan()
                                        .where(indexPlanOf(
                                                indexPlan()
                                                        .where(indexName("EventIntervalsWithAdditionalValue"))
                                                        .and(indexScanParameters(
                                                                multidimensional()
                                                                        .where(prefix(range("[[null],[null]]")))
                                                                        .and(dimensions(range("([null],[1690378647]]"), range("[[1690364247],>")))
                                                                        .and(suffix(unbounded())))))))
                                .where(queryComponents(only(PrimitiveMatchers.equalsObject(Query.field("calendar_name").equalsValue("business")))))));
        final Set<Message> actualResults = getResults(additionalIndexes, mdPlan);
        Assertions.assertEquals(expectedResults, actualResults);
    }

    /**
     * This test inserts a lot of records with identical coordinates but different record ids. It is important to ensure
     * that we discard children based on a suffix filter.
     *
     * @param numRecords number of records
     * @param storage kind of storage
     * @param storeHilbertValues indicator whether we should store hilbert values in the index
     * @throws Exception any thrown exception
     */
    void indexReadWithDuplicatesTest(final boolean useAsync, final int numRecords, @Nonnull final String storage,
                                 final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndexes =
                metaDataBuilder -> {
                    addCalendarNameStartEpochIndex(metaDataBuilder);
                    addMultidimensionalIndex(metaDataBuilder, storage, storeHilbertValues, useNodeSlotIndex);
                };
        loadSpecificRecordsWithDuplicates(useAsync, additionalIndexes, numRecords);

        final QueryComponent filter =
                Query.and(
                        Query.field("calendar_name").equalsValue("business"),
                        Query.field("start_epoch").equalsValue(0L),
                        Query.field("end_epoch").equalsValue(1L),
                        Query.field("info").matches(Query.field("rec_domain").isNull()),
                        Query.field("rec_no").equalsValue(0L));

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setIndexQueryabilityFilter(noMultidimensionalIndexes)
                .build();
        final RecordQueryPlan plan = planQuery(query);
        final Set<Message> expectedResults = getResults(additionalIndexes, plan);

        // run an un-hinted query -- make sure the planner picks up the md-index
        query = RecordQuery.newBuilder()
                .setRecordType("MyMultidimensionalRecord")
                .setFilter(filter)
                .setAllowedIndexes(ImmutableSet.of("EventIntervals"))
                .build();
        final RecordQueryPlan mdPlan = planQuery(query);

        assertMatchesExactly(mdPlan,
                indexPlan()
                        .where(indexName("EventIntervals"))
                        .and(indexScanParameters(
                                multidimensional()
                                        .where(prefix(range("[[business],[business]]")))
                                        .and(dimensions(range("[[0],[0]]"), range("[[1],[1]]")))
                                        .and(suffix(range("[[null, 0],[null, 0]]"))))));
        final Set<Message> actualResults = getResults(additionalIndexes, mdPlan, fdbStoreTimer ->
                Assertions.assertTrue(fdbStoreTimer.getCount(FDBStoreTimer.Counts.MULTIDIMENSIONAL_CHILD_NODE_DISCARDS) > 0));
        Assertions.assertEquals(expectedResults, actualResults);
    }

    @Nonnull
    private Set<Message> getResults(@Nonnull final RecordMetaDataHook additionalIndexes,
                                    @Nonnull final RecordQueryPlan queryPlan) throws Exception {
        return getResults(additionalIndexes, queryPlan, fdbStoreTimer -> { });
    }

    @Nonnull
    @SuppressWarnings("resource")
    private Set<Message> getResults(@Nonnull final RecordMetaDataHook additionalIndexes,
                                    @Nonnull final RecordQueryPlan queryPlan,
                                    @Nonnull final Consumer<FDBStoreTimer> timersConsumer) throws Exception {
        final Set<Message> actualResults;
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, additionalIndexes);
            final RecordCursor<QueryResult> recordCursor =
                    queryPlan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE);
            actualResults = recordCursor.asStream()
                    .map(queryResult -> Objects.requireNonNull(queryResult.getQueriedRecord()).getRecord())
                    .collect(Collectors.toSet());
            timersConsumer.accept(context.getTimer());
            commit(context);
        }
        return actualResults;
    }

    @SuppressWarnings("CheckStyle")
    static class HypercubeScanParameters implements IndexScanParameters {
        @Nullable
        private final String minCalendarName;
        @Nullable
        private final String maxCalendarName;
        @Nonnull
        private final Long[] minsInclusive;
        @Nonnull
        private final Long[] maxsInclusive;

        public HypercubeScanParameters(@Nullable final String calendarName,
                                       @Nonnull final Long... minMaxLimits) {
            this(calendarName, calendarName, minMaxLimits);
        }

        public HypercubeScanParameters(@Nullable final String minCalendarName,
                                       @Nullable final String maxCalendarName,
                                       @Nonnull final Long... minMaxLimits) {
            Preconditions.checkArgument(minMaxLimits.length % 2 == 0);
            this.minCalendarName = minCalendarName;
            this.maxCalendarName = maxCalendarName;
            this.minsInclusive = new Long[minMaxLimits.length / 2];
            this.maxsInclusive = new Long[minMaxLimits.length / 2];
            for (int i = 0; i < minMaxLimits.length; i += 2) {
                this.minsInclusive[i / 2] = minMaxLimits[i];
                this.maxsInclusive[i / 2] = minMaxLimits[i + 1];
            }
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            return 11;
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }

        @Nonnull
        @Override
        public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }

        @Nonnull
        @Override
        public IndexScanType getScanType() {
            return IndexScanType.BY_VALUE;
        }

        @Nonnull
        @Override
        public IndexScanBounds bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Index index, @Nonnull final EvaluationContext context) {
            final ImmutableList.Builder<TupleRange> tupleRangesBuilder = ImmutableList.builder();
            for (int i = 0; i < minsInclusive.length; i++) {
                final Long min = minsInclusive[i];
                final Long max = maxsInclusive[i];
                tupleRangesBuilder.add(TupleRange.betweenInclusive(min == null ? null : Tuple.from(min),
                        max == null ? null : Tuple.from(max)));
            }

            return new MultidimensionalIndexScanBounds(
                    TupleRange.betweenInclusive(minCalendarName == null ? null : Tuple.from(minCalendarName),
                            maxCalendarName == null ? null : Tuple.from(maxCalendarName)),
                    new MultidimensionalIndexScanBounds.Hypercube(tupleRangesBuilder.build()), TupleRange.ALL);
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
        public int planHash(@Nonnull final PlanHashMode mode) {
            return 13;
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
        }

        @Nonnull
        @Override
        public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new RecordCoreException("unsupported");
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
