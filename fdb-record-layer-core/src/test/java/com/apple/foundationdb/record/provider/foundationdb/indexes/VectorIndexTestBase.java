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
import com.apple.foundationdb.async.hnsw.Metrics;
import com.apple.foundationdb.async.hnsw.Vector;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for multidimensional type indexes.
 */
@Tag(Tags.RequiresFDB)
public abstract class VectorIndexTestBase extends FDBRecordStoreQueryTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTestBase.class);

    private static final SimpleDateFormat timeFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    @CanIgnoreReturnValue
    RecordMetaDataBuilder addVectorIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex("UngroupedVectorRecord",
                new Index("UngroupedVectorIndex", new KeyWithValueExpression(field("vector_data"), 0),
                        IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.HNSW_METRIC, Metrics.EUCLIDEAN_METRIC.toString())));
        return metaDataBuilder;
    }

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        metaDataBuilder.getRecordType("UngroupedVectorRecord").setPrimaryKey(field("rec_no"));
        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    static Function<Long, Message> getRecordGenerator(@Nonnull final Random random) {
        return recNo -> {
            final byte[] vector = new byte[128 * 2];
            random.nextBytes(vector);

            //logRecord(recNo, vector);
            return TestRecordsVectorsProto.UngroupedVectorRecord.newBuilder()
                    .setRecNo(recNo)
                    .setVectorData(ByteString.copyFrom(vector))
                    .build();
        };
    }

    public void saveRecords(final boolean useAsync, @Nonnull final RecordMetaDataHook hook, final long seed,
                            final int numSamples)  {
        final Random random = new Random(seed);
        final var recordGenerator = getRecordGenerator(random);
        if (useAsync) {
            Assertions.assertDoesNotThrow(() -> batchAsync(hook, numSamples, 100, recNo -> recordStore.saveRecordAsync(recordGenerator.apply(recNo))));
        } else {
            Assertions.assertDoesNotThrow(() -> batch(hook, numSamples, 100, recNo -> recordStore.saveRecord(recordGenerator.apply(recNo))));
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

    private <T> long batch(final RecordMetaDataHook hook, final int numRecords, final int batchSize, Consumer<Long> recordConsumer) throws Exception {
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

    private <T> long batchAsync(final RecordMetaDataHook hook, final int numRecords, final int batchSize, Function<Long, CompletableFuture<T>> recordConsumer) throws Exception {
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
                    recNo, Vector.HalfVector.halfVectorFromBytes(vectorData.toByteArray()));
        }
    }

    void basicReadTest() throws Exception {
        saveRecords(false, this::addVectorIndex, 0, 1000);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndex);
            for (long l = 0; l < 1000; l ++) {
                FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(l));

                assertNotNull(rec);
                TestRecordsVectorsProto.UngroupedVectorRecord.Builder recordBuilder =
                        TestRecordsVectorsProto.UngroupedVectorRecord.newBuilder();
                recordBuilder.mergeFrom(rec.getRecord());
                final var record = recordBuilder.build();
                logRecord(record.getRecNo(), record.getVectorData());
            }
            commit(context);
        }
    }

    void basicConcurrentReadTest(final boolean useAsync) throws Exception {
        saveRecords(useAsync, NO_HOOK, 0, 5000);
        for (int i = 0; i < 1; i ++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                for (long l = 0; l < 5000; l += 20) {
                    fetchBatchConcurrently(l, 20);
                }
                commit(context);
            }
        }
    }

    private List<TestRecordsVectorsProto.UngroupedVectorRecord> fetchBatchConcurrently(long startRecNo, int batchSize) throws Exception {
        final ImmutableList.Builder<CompletableFuture<FDBStoredRecord<Message>>> futureBatchBuilder = ImmutableList.builder();
        for (int i = 0; i < batchSize; i ++) {
            final CompletableFuture<FDBStoredRecord<Message>> recordFuture = recordStore.loadRecordAsync(Tuple.from(startRecNo + i));
            futureBatchBuilder.add(recordFuture);
        }
        final var batchFuture = AsyncUtil.getAll(futureBatchBuilder.build());
        final var batch = batchFuture.get();

        return batch.stream()
                .map(rec -> {
                    assertNotNull(rec);
                    TestRecordsVectorsProto.UngroupedVectorRecord.Builder recordBuilder =
                            TestRecordsVectorsProto.UngroupedVectorRecord.newBuilder();
                    recordBuilder.mergeFrom(rec.getRecord());
                    return recordBuilder.build();
                })
                .collect(ImmutableList.toImmutableList());
    }
}
