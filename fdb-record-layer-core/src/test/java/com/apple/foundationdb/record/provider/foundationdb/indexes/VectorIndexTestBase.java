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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.assertj.core.util.Streams;
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

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        metaDataBuilder.getRecordType("NaiveVectorRecord").setPrimaryKey(field("rec_no"));
        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    static Function<Long, Message> getRecordGenerator(@Nonnull final Random random) {
        return recNo -> {
            final Iterable<Double> vector =
                    () -> IntStream.range(0, 4000).mapToDouble(index -> random.nextDouble()).iterator();
            //logRecord(recNo, vector);
            return TestRecordsVectorsProto.NaiveVectorRecord.newBuilder()
                    .setRecNo(recNo)
                    .addAllVectorData(vector)
                    .build();
        };
    }

    public void loadRecords(final boolean useAsync, @Nonnull final RecordMetaDataHook hook, final long seed,
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

    private static void logRecord(final long recNo, @Nonnull final Iterable<Double> vector) {
        if (logger.isInfoEnabled()) {
            logger.info("recNo: {}; vectorData: [{})",
                    recNo, Streams.stream(vector).map(String::valueOf).collect(Collectors.joining(",")));
        }
    }

    void basicReadTest(final boolean useAsync) throws Exception {
        loadRecords(useAsync, NO_HOOK, 0, 5000);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            for (long l = 0; l < 5000; l ++) {
                FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(l));
                //Thread.sleep(10);
                assertNotNull(rec);
                TestRecordsVectorsProto.NaiveVectorRecord.Builder recordBuilder =
                        TestRecordsVectorsProto.NaiveVectorRecord.newBuilder();
                recordBuilder.mergeFrom(rec.getRecord());
                final var record = recordBuilder.build();
                //logRecord(record.getRecNo(), record.getVectorDataList());
            }
            commit(context);
        }
    }

    void basicConcurrentReadTest(final boolean useAsync) throws Exception {
        loadRecords(useAsync, NO_HOOK, 0, 5000);
        for (int i = 0; i < 1; i ++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                for (long l = 0; l < 5000; l += 20) {
                    final var batch = fetchBatchConcurrently(l, 20);
                    //batch.forEach(record -> logRecord(record.getRecNo(), record.getVectorDataList()));
                }
                commit(context);
            }
        }
    }

    private List<TestRecordsVectorsProto.NaiveVectorRecord> fetchBatchConcurrently(long startRecNo, int batchSize) throws Exception {
        final ImmutableList.Builder<CompletableFuture<FDBStoredRecord<Message>>> futureBatchBuilder = ImmutableList.builder();
        for (int i = 0; i < batchSize; i ++) {
            final long task = startRecNo + i;
            //System.out.println("task " + task + " scheduled");
            final CompletableFuture<FDBStoredRecord<Message>> recordFuture = recordStore.loadRecordAsync(Tuple.from(startRecNo + i)).thenApply(r -> {
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
                //System.out.println("task " + task + " done, thread: " + Thread.currentThread());
                return r;
            });
            futureBatchBuilder.add(recordFuture);
        }
        final var batchFuture = AsyncUtil.getAll(futureBatchBuilder.build());
        final var batch = batchFuture.get();

        return batch.stream()
                .map(rec -> {
                    assertNotNull(rec);
                    TestRecordsVectorsProto.NaiveVectorRecord.Builder recordBuilder =
                            TestRecordsVectorsProto.NaiveVectorRecord.newBuilder();
                    recordBuilder.mergeFrom(rec.getRecord());
                    return recordBuilder.build();
                })
                .collect(ImmutableList.toImmutableList());
    }
}
