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
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons.DistanceRankValueComparison;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for multidimensional type indexes.
 */
@Tag(Tags.RequiresFDB)
public abstract class VectorIndexTestBase extends FDBRecordStoreQueryTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTestBase.class);

    @CanIgnoreReturnValue
    RecordMetaDataBuilder addVectorIndexes(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex("VectorRecord",
                new Index("UngroupedVectorIndex", new KeyWithValueExpression(field("vector_data"), 0),
                        IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                                IndexOptions.HNSW_NUM_DIMENSIONS, "128")));
        metaDataBuilder.addIndex("VectorRecord",
                new Index("GroupedVectorIndex", new KeyWithValueExpression(concat(field("group_id"), field("vector_data")), 1),
                        IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                                IndexOptions.HNSW_NUM_DIMENSIONS, "128")));
        return metaDataBuilder;
    }

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        metaDataBuilder.getRecordType("VectorRecord").setPrimaryKey(field("rec_no"));
        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    static Function<Long, Message> getRecordGenerator(@Nonnull final Random random) {
        return recNo -> {
            final RealVector vector = randomHalfVector(random, 128);

            return VectorRecord.newBuilder()
                    .setRecNo(recNo)
                    .setVectorData(ByteString.copyFrom(vector.getRawData()))
                    .setGroupId(recNo.intValue() % 2)
                    .build();
        };
    }

    @Nonnull
    static RealVector randomHalfVector(final Random random, final int numDimensions) {
        final Half[] componentData = new Half[numDimensions];
        for (int i = 0; i < componentData.length; i++) {
            componentData[i] = Half.valueOf(random.nextFloat());
        }

        return new HalfRealVector(componentData);
    }

    public void saveRecords(final boolean useAsync, @Nonnull final RecordMetaDataHook hook, @Nonnull final Random random,
                            final int numSamples)  {
        final var recordGenerator = getRecordGenerator(random);
        if (useAsync) {
            Assertions.assertDoesNotThrow(() -> batchAsync(hook, numSamples, 100, recNo -> recordStore.saveRecordAsync(recordGenerator.apply(recNo))));
        } else {
            Assertions.assertDoesNotThrow(() -> batch(hook, numSamples, 100, recNo -> recordStore.saveRecord(recordGenerator.apply(recNo))));
        }
    }

    private long batch(final RecordMetaDataHook hook, final int numRecords, final int batchSize,
                       Consumer<Long> recordConsumer) throws Exception {
        long numRecordsCommitted = 0;
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

    private <T> long batchAsync(final RecordMetaDataHook hook, final int numRecords, final int batchSize,
                                Function<Long, CompletableFuture<T>> recordConsumer) throws Exception {
        long numRecordsCommitted = 0;
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

    private static void logRecord(final long recNo, @Nonnull final ByteString vectorData) {
        if (logger.isInfoEnabled()) {
            logger.info("recNo: {}; vectorData: [{})",
                    recNo, RealVector.fromBytes(vectorData.toByteArray()));
        }
    }

    void basicWriteReadTest() throws Exception {
        final Random random = new Random();
        saveRecords(false, this::addVectorIndexes, random, 1000);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);
            for (long l = 0; l < 1000; l ++) {
                FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(l));

                assertNotNull(rec);
                VectorRecord.Builder recordBuilder =
                        VectorRecord.newBuilder();
                recordBuilder.mergeFrom(rec.getRecord());
                final var record = recordBuilder.build();
                logRecord(record.getRecNo(), record.getVectorData());
            }
            commit(context);
        }
    }

    void basicWriteIndexReadTest() throws Exception {
        final Random random = new Random(0);
        saveRecords(false, this::addVectorIndexes, random, 1000);

        final DistanceRankValueComparison distanceRankComparison =
                new DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 16, 128),
                                randomHalfVector(random, 128)),
                        new LiteralValue<>(10));

        final VectorIndexScanComparisons vectorIndexScanComparisons =
                new VectorIndexScanComparisons(ScanComparisons.EMPTY, distanceRankComparison, ScanComparisons.EMPTY);

        final var baseRecordType =
                Type.Record.fromFieldDescriptorsMap(
                        Type.Record.toFieldDescriptorMap(VectorRecord.getDescriptor().getFields()));

        final var indexPlan = new RecordQueryIndexPlan("UngroupedVectorIndex", field("recNo"),
                vectorIndexScanComparisons, IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY, false, false,
                Optional.empty(), baseRecordType, QueryPlanConstraint.noConstraint());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);

            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(indexPlan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    VectorRecord.Builder myrec = VectorRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    // TODO
                    System.out.println(myrec);
                }
            }

            //commit(context);
        }
    }

    void basicWriteIndexReadGroupedTest() throws Exception {
        final Random random = new Random(0);
        saveRecords(false, this::addVectorIndexes, random, 1000);

        final DistanceRankValueComparison distanceRankComparison =
                new DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 16, 128),
                                randomHalfVector(random, 128)),
                        new LiteralValue<>(10));

        final VectorIndexScanComparisons vectorIndexScanComparisons =
                new VectorIndexScanComparisons(ScanComparisons.EMPTY, distanceRankComparison, ScanComparisons.EMPTY);

        final var baseRecordType =
                Type.Record.fromFieldDescriptorsMap(
                        Type.Record.toFieldDescriptorMap(VectorRecord.getDescriptor().getFields()));

        final var indexPlan = new RecordQueryIndexPlan("GroupedVectorIndex", field("recNo"),
                vectorIndexScanComparisons, IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY, false, false,
                Optional.empty(), baseRecordType, QueryPlanConstraint.noConstraint());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);

            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(indexPlan)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    VectorRecord.Builder myrec = VectorRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    // TODO
                    System.out.println(myrec);
                }
            }

            //commit(context);
        }
    }
}
