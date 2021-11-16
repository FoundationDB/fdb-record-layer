/*
 * OnlineIndexerSimpleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer.DEFAULT_PROGRESS_LOG_INTERVAL;
import static com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer.DO_NOT_RE_INCREASE_LIMIT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link OnlineIndexer} which does not need to use {@link OnlineIndexer#buildIndex()} (or similar APIs) to
 * build full indexes. ({@link #testConfigLoader()} does use a such API but it is not necessary.)
 */
public class OnlineIndexerSimpleTest extends OnlineIndexerTest {
    @Test
    public void buildEndpointIdempotency() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        final Supplier<Tuple> getAggregate = () -> {
            Tuple ret;
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).join());
                FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context).uncheckedOpen();
                ret = recordStore2.evaluateAggregateFunction(indexTypes, aggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
                // Do NOT commit the change.
            }
            return ret;
        };

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            final RangeSet rangeSet = new RangeSet(recordStore.indexRangeSubspace(index));

            // Build the endpoints
            TupleRange range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(10L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(0L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(9L).pack(), null).join());
            List<Range> middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(0L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(9L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Make sure running this again doesn't change anything.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(10L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(0L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(9L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(0L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(9L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Remove the first and last records.
            try (FDBRecordContext context = openContext()) {
                recordStore.deleteRecord(Tuple.from(0L));
                recordStore.deleteRecord(Tuple.from(9L));
                context.commit();
            }
            assertEquals(Tuple.from(0L), getAggregate.get());

            // Rerun endpoints with new data.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(1L), range.getLow());
            assertEquals(Tuple.from(8L), range.getHigh());
            assertEquals(Tuple.from(9L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Run it again to show that nothing has happened.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(1L), range.getLow());
            assertEquals(Tuple.from(8L), range.getHigh());
            assertEquals(Tuple.from(9L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Add back the previous first and last records.
            try (FDBRecordContext context = openContext()) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(0L).setNumValue2(1).build());
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(9L).setNumValue2(10).build());
                context.commit();
            }
            assertEquals(Tuple.from(20L), getAggregate.get());

            // Rerun endpoints with new data.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(20L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Run it again to show that nothing has happened.
            range = indexBuilder.buildEndpoints().join();
            assertEquals(Tuple.from(0L), range.getLow());
            assertEquals(Tuple.from(9L), range.getHigh());
            assertEquals(Tuple.from(20L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), null, Tuple.from(1L).pack()).join());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database(), Tuple.from(8L).pack(), null).join());
            middleRanges = rangeSet.missingRanges(fdb.database()).join();
            assertEquals(Collections.singletonList(Tuple.from(1L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.begin)).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(Tuple.from(8L)), middleRanges.stream().map(r -> Tuple.fromBytes(r.end)).collect(Collectors.toList()));

            // Straight up build the whole index.
            indexBuilder.buildIndex(false);
            assertEquals(Tuple.from(55L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database()).join());
        }
    }

    @Test
    public void buildRangeTransactional() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 200).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        final Supplier<Tuple> getAggregate = () -> {
            Tuple ret;
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).join());
                FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context).uncheckedOpen();
                ret = recordStore2.evaluateAggregateFunction(indexTypes, aggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
                // Do NOT commit changes
            }
            return ret;
        };

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            try (FDBRecordContext context = openContext()) {
                recordStore.markIndexWriteOnly(index).join();
                context.commit();
            }

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, null, null).join();
                indexBuilder.buildRange(recordStore, null, null).join();
                context.commit();
            }
            assertEquals(Tuple.from(20100L), getAggregate.get());

            clearIndexData(index);

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, null, Key.Evaluated.scalar(130L)).join();
                context.commit();
            }
            assertEquals(Tuple.from(8515L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(100L), Key.Evaluated.scalar(130L)).join();
                context.commit();
            }
            assertEquals(Tuple.from(8515L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(100L), Key.Evaluated.scalar(150L)).join();
                context.commit();
            }
            assertEquals(Tuple.from(11325L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(100L), null).join();
                context.commit();
            }
            assertEquals(Tuple.from(20100L), getAggregate.get());

            clearIndexData(index);

            try (FDBRecordContext context = openContext()) {
                for (long l = 0L; l < 200L; l += 10) {
                    indexBuilder.buildRange(recordStore, Key.Evaluated.scalar(l), Key.Evaluated.scalar(l + 5L)).join();
                }
                context.commit();
            }
            assertEquals(Tuple.from(9800L), getAggregate.get());

            try (FDBRecordContext context = openContext()) {
                for (long l = 0L; l < 200L; l += 10) {
                    indexBuilder.buildRange(recordStore, null, null).join();
                }
                context.commit();
            }
            assertEquals(Tuple.from(20100L), getAggregate.get());
        }

    }

    @Test
    public void buildRangeWithNull() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 200).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        final Supplier<Tuple> getAggregate = () -> {
            Tuple ret;
            try (FDBRecordContext context = openContext()) {
                assertTrue(recordStore.uncheckedMarkIndexReadable(index.getName()).join());
                FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context).uncheckedOpen();
                ret = recordStore2.evaluateAggregateFunction(indexTypes, aggregateFunction, TupleRange.ALL, IsolationLevel.SERIALIZABLE).join();
                // Do NOT commit changes
            }
            return ret;
        };

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildRange(null, null).join();
            assertEquals(Tuple.from(20100L), getAggregate.get());
        }
    }

    @SuppressWarnings("try")
    @Test
    public void readableAtEnd() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 50).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndex();
        }

        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
        }
    }

    @Test
    public void run() {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(100).setMaxRetries(3).setRecordsPerSecond(10000)
                .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                .setMaxAttempts(2)
                .build()) {

            AtomicInteger attempts = new AtomicInteger();

            // Non-FDB error
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new IllegalStateException("illegal state");
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(IllegalStateException.class));
                assertEquals("illegal state", e.getMessage());
                assertNull(e.getCause());
                assertEquals(1, attempts.get());
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();

            // Retriable error that is not in lessen work codes.
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new RecordCoreRetriableTransactionException("Retriable", new FDBException("commit_unknown_result", 1021));
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreRetriableTransactionException.class));
                assertEquals("Retriable", e.getMessage());
                assertThat(e.getCause(), instanceOf(FDBException.class));
                assertEquals("commit_unknown_result", e.getCause().getMessage());
                assertEquals(FDBError.COMMIT_UNKNOWN_RESULT.code(), ((FDBException)e.getCause()).getCode());
                assertEquals(2, attempts.get());
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();

            // Non-retriable error that is in lessen work codes.
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreException.class));
                assertEquals("Non-retriable", e.getMessage());
                assertNotNull(e.getCause());
                assertThat(e.getCause(), instanceOf(FDBException.class));
                assertEquals("transaction_too_large", e.getCause().getMessage());
                assertEquals(FDBError.TRANSACTION_TOO_LARGE.code(), ((FDBException)e.getCause()).getCode());
                assertEquals(4, attempts.get()); // lessenWorkCodes is maxRetries
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();

            // Retriable error that is in lessen work codes.
            attempts.set(0);
            runAndHandleLessenWorkCodes(indexBuilder, store -> {
                attempts.incrementAndGet();
                throw new RecordCoreRetriableTransactionException("Retriable and lessener", new FDBException("not_committed", 1020));
            }).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreRetriableTransactionException.class));
                assertEquals("Retriable and lessener", e.getMessage());
                assertNotNull(e.getCause());
                assertThat(e.getCause(), instanceOf(FDBException.class));
                assertEquals("not_committed", e.getCause().getMessage());
                assertEquals(FDBError.NOT_COMMITTED.code(), ((FDBException)e.getCause()).getCode());
                assertEquals(8, attempts.get());
                assertEquals("my cool mdc value", ThreadContext.get("mdcKey"));
                return null;
            }).join();
        }
    }

    private <R> CompletableFuture<R> runAndHandleLessenWorkCodes(OnlineIndexer indexBuilder, @Nonnull Function<FDBRecordStore, CompletableFuture<R>> function) {
        return indexBuilder.throttledRunAsync(function, Pair::of, indexBuilder::decreaseLimit, null);
    }

    @Test
    public void lessenLimits() {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(100).setMaxRetries(30).setRecordsPerSecond(10000)
                .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                .setMaxAttempts(3)
                .build()) {

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger limit = new AtomicInteger(100);

            // Non-retriable error that is in lessen work codes.
            attempts.set(0);
            indexBuilder.buildCommitRetryAsync((store, recordsScanned) -> {
                assertEquals(attempts.getAndIncrement(), indexBuilder.getLimit(),
                        limit.getAndUpdate(x -> Math.max(x, (3 * x) / 4)));
                throw new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));
            }, null).handle((val, e) -> {
                assertNotNull(e);
                assertThat(e, instanceOf(RecordCoreException.class));
                assertEquals("Non-retriable", e.getMessage());
                return null;
            }).join();
            assertEquals(31, attempts.get());
        }
    }

    @Test
    void notReincreaseLimit() {
        // Non-retriable error that is in lessen work codes.
        Supplier<RuntimeException> createException =
                () -> new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));

        Queue<Pair<Integer, Supplier<RuntimeException>>> queue = new LinkedList<>();
        // failures until it hits 42
        for (int i = 100; i > 42; i = (3 * i) / 4) {
            queue.add(Pair.of(i, createException));
        }
        // a whole bunch of successes
        for (int i = 0; i < 100; i++) {
            queue.add(Pair.of(42, null));
        }
        reincreaseLimit(queue, index ->
                OnlineIndexer.newBuilder()
                        .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                        .setLimit(100).setMaxRetries(queue.size() + 3).setRecordsPerSecond(10000)
                        .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                        .setMaxAttempts(3)
                        .build());
    }

    @Test
    public void reincreaseLimit() {
        // Non-retriable error that is in lessen work codes.
        Supplier<RuntimeException> createException =
                () -> new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));

        Queue<Pair<Integer, Supplier<RuntimeException>>> queue = new LinkedList<>();
        // failures until it hits 1
        for (int i = 100; i > 1; i = (3 * i) / 4) {
            queue.add(Pair.of(i, createException));
        }
        // queue size = 13
        // success for a while
        for (int i = 0; i < 10; i++) {
            queue.add(Pair.of(1, null));
        }
        // queue size = 23
        // now starts re-increasing
        queue.add(Pair.of(2, null));
        queue.add(Pair.of(3, null));
        queue.add(Pair.of(4, null));
        for (int i = 5; i < 100; i = (i * 4) / 3) {
            queue.add(Pair.of(i, null));
        }
        // queue size = 38
        // does not pass original max
        queue.add(Pair.of(100, null));
        queue.add(Pair.of(100, null));
        queue.add(Pair.of(100, null));
        for (int i = 100; i > 42; i = (3 * i) / 4) {
            queue.add(Pair.of(i, createException));
        }
        // queue size = 44
        // success for a while
        for (int i = 0; i < 10; i++) {
            queue.add(Pair.of(42, null));
        }
        // queue size = 54
        // fail once
        queue.add(Pair.of(56, createException));
        for (int i = 0; i < 10; i++) {
            queue.add(Pair.of(42, null));
        }
        // queue size = 65
        queue.add(Pair.of(56, createException));
        queue.add(Pair.of(42, null));

        reincreaseLimit(queue, index ->
                OnlineIndexer.newBuilder()
                        .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                        .setLimit(100).setMaxRetries(queue.size() + 3).setRecordsPerSecond(10000)
                        .setIncreaseLimitAfter(10)
                        .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                        .setMaxAttempts(3)
                        .setProgressLogIntervalMillis(0)
                        .build());
    }

    private void reincreaseLimit(Queue<Pair<Integer, Supplier<RuntimeException>>> queue,
                                 final Function<Index, OnlineIndexer> buildOnlineIndexer) {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = buildOnlineIndexer.apply(index)) {

            AtomicInteger attempts = new AtomicInteger();
            attempts.set(0);
            AsyncUtil.whileTrue(() ->
                    indexBuilder.buildCommitRetryAsync((store, recordsScanned) -> {
                        Pair<Integer, Supplier<RuntimeException>> behavior = queue.poll();
                        if (behavior == null) {
                            return AsyncUtil.READY_FALSE;
                        } else {
                            int currentAttempt = attempts.getAndIncrement();
                            assertEquals(behavior.getLeft().intValue(), indexBuilder.getLimit(),
                                    "Attempt " + currentAttempt);
                            if (behavior.getRight() != null) {
                                throw behavior.getRight().get();
                            }
                            return AsyncUtil.READY_TRUE;
                        }
                    }, null)).join();
            assertNull(queue.poll());
        }
    }

    @Test
    public void recordsScanned() {
        Supplier<RuntimeException> nonRetriableException =
                () -> new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101));
        Supplier<RuntimeException> retriableException =
                () -> new RecordCoreRetriableTransactionException("Retriable", new FDBException("not_committed", 1020));
        Queue<Pair<Long, Supplier<RuntimeException>>> queue = new LinkedList<>();

        queue.add(Pair.of(0L, retriableException));
        queue.add(Pair.of(0L, nonRetriableException));
        queue.add(Pair.of(0L, null));
        queue.add(Pair.of(1L, null));
        queue.add(Pair.of(2L, null));
        queue.add(Pair.of(3L, null));
        queue.add(Pair.of(4L, retriableException));
        queue.add(Pair.of(4L, retriableException));
        queue.add(Pair.of(4L, nonRetriableException));
        queue.add(Pair.of(4L, nonRetriableException));
        queue.add(Pair.of(4L, null));
        Index index = runAsyncSetup();


        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                .setMaxAttempts(3)
                .setConfigLoader(old ->
                        OnlineIndexer.Config.newBuilder()
                                .setMaxLimit(100)
                                .setMaxRetries(queue.size() + 3)
                                .setRecordsPerSecond(10000)
                                .setIncreaseLimitAfter(10)
                                .setProgressLogIntervalMillis(30)
                                .build()).build()) {

            AtomicInteger attempts = new AtomicInteger();
            attempts.set(0);
            AsyncUtil.whileTrue(() -> indexBuilder.buildCommitRetryAsync(
                    (store, recordsScanned) -> {
                        Pair<Long, Supplier<RuntimeException>> behavior = queue.poll();
                        if (behavior == null) {
                            return AsyncUtil.READY_FALSE;
                        } else {
                            int currentAttempt = attempts.getAndIncrement();
                            assertEquals(1, recordsScanned.incrementAndGet());
                            assertEquals(behavior.getLeft().longValue(), indexBuilder.getTotalRecordsScanned(),
                                    "Attempt " + currentAttempt);
                            if (behavior.getRight() != null) {
                                throw behavior.getRight().get();
                            }
                            return AsyncUtil.READY_TRUE;
                        }
                    },
                    Arrays.asList(LogMessageKeys.CALLING_METHOD, "OnlineIndexerTest.recordsScanned"))
            ).join();
            assertNull(queue.poll());
            assertEquals(5L, indexBuilder.getTotalRecordsScanned());
        }
    }

    @Test
    public void runWithWeakReadSemantics() throws InterruptedException, ExecutionException {
        boolean dbTracksReadVersionOnRead = fdb.isTrackLastSeenVersionOnRead();
        boolean dbTracksReadVersionOnCommit = fdb.isTrackLastSeenVersionOnCommit();
        try {
            fdb.setTrackLastSeenVersion(true);
            Index index = runAsyncSetup();

            FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(0L, Long.MAX_VALUE, true);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                    .setWeakReadSemantics(weakReadSemantics)
                    .build()) {
                long readVersion = runAndHandleLessenWorkCodes(indexBuilder, recordStore -> {
                    assertSame(weakReadSemantics, recordStore.getContext().getWeakReadSemantics());
                    assertTrue(recordStore.getContext().hasReadVersion());
                    return recordStore.getContext().getReadVersionAsync();
                }).get();
                long readVersion2 = runAndHandleLessenWorkCodes(indexBuilder, recordStore -> {
                    assertTrue(recordStore.getContext().hasReadVersion());
                    return recordStore.getContext().getReadVersionAsync();
                }).get();
                assertEquals(readVersion, readVersion2, "weak read semantics did not preserve read version");
            }
        } finally {
            fdb.setTrackLastSeenVersionOnRead(dbTracksReadVersionOnRead);
            fdb.setTrackLastSeenVersionOnRead(dbTracksReadVersionOnCommit);
        }
    }

    @Test
    public void runWithPriorities() throws InterruptedException, ExecutionException {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            runAndHandleLessenWorkCodes(indexBuilder, recordStore -> {
                assertEquals(FDBTransactionPriority.BATCH, recordStore.getContext().getPriority());
                return AsyncUtil.DONE;
            }).get();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setPriority(FDBTransactionPriority.DEFAULT)
                .build()) {
            runAndHandleLessenWorkCodes(indexBuilder, recordStore -> {
                assertEquals(FDBTransactionPriority.DEFAULT, recordStore.getContext().getPriority());
                return AsyncUtil.DONE;
            }).get();
        }
    }

    @Nonnull
    private Index runAsyncSetup() {
        Index index = new Index("newIndex", field("num_value_2"));
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));

        try (FDBRecordContext context = openContext()) {
            // OnlineIndexer.runAsync checks that the index is not readable
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }
        return index;
    }

    @Test
    public void illegalConstructorParams() {
        Index newIndex = new Index("newIndex", field("num_value_2"));
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", newIndex));
        Index indexPrime = metaData.getIndex("newIndex");
        // Absent index
        RecordCoreException e = assertThrows(MetaDataException.class, () -> {
            Index absentIndex = new Index("absent", field("num_value_2"));
            OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(absentIndex).setSubspace(subspace).build();
        });
        assertEquals("Index absent not contained within specified metadata", e.getMessage());
        // Limit
        e = assertThrows(RecordCoreException.class, () ->
                OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setLimit(-1).build()
        );
        assertEquals("Non-positive value -1 given for record limit", e.getMessage());
        e = assertThrows(RecordCoreException.class, () ->
                OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setLimit(0).build()
        );
        assertEquals("Non-positive value 0 given for record limit", e.getMessage());
        // Retries
        e = assertThrows(RecordCoreException.class, () ->
                OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setMaxRetries(-1).build()
        );
        assertEquals("Non-positive value -1 given for maximum retries", e.getMessage());
        e = assertThrows(RecordCoreException.class, () ->
                OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setMaxRetries(0).build()
        );
        assertEquals("Non-positive value 0 given for maximum retries", e.getMessage());
        // Records per second
        e = assertThrows(RecordCoreException.class, () ->
                OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setRecordsPerSecond(-1).build()
        );
        assertEquals("Non-positive value -1 given for records per second value", e.getMessage());
        e = assertThrows(RecordCoreException.class, () ->
                OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setIndex(indexPrime).setSubspace(subspace).setRecordsPerSecond(0).build()
        );
        assertEquals("Non-positive value 0 given for records per second value", e.getMessage());
        // WeakReadSemantics before runner
        e = assertThrows(MetaDataException.class, () ->
                OnlineIndexer.newBuilder().setWeakReadSemantics(new FDBDatabase.WeakReadSemantics(Long.MAX_VALUE, 0L, true))
        );
        assertEquals("weak read semantics can only be set after runner has been set", e.getMessage());
        // priority before runner
        e = assertThrows(MetaDataException.class, () ->
                OnlineIndexer.newBuilder().setPriority(FDBTransactionPriority.DEFAULT)
        );
        assertEquals("transaction priority can only be set after runner has been set", e.getMessage());
    }

    @Test
    public void closeWhileBuilding() throws Exception {
        final Index index = new Index("newIndex", field("num_value_2"));
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
        openSimpleMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 1000; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(record);
            }
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        final CompletableFuture<Void> future;
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setLimit(1).setMaxRetries(Integer.MAX_VALUE).setRecordsPerSecond(Integer.MAX_VALUE)
                .setTimer(timer)
                .build()) {
            future = indexBuilder.buildIndexAsync();
            // Let the builder get some work done.
            int pass = 0;
            while (!future.isDone() && timer.getCount(FDBStoreTimer.Events.COMMIT) < 10 && pass++ < 100) {
                Thread.sleep(100);
            }
            assertThat("Should have done several transactions in a few seconds", pass, lessThan(100));
        }
        int count1 = timer.getCount(FDBStoreTimer.Events.COMMIT);
        assertThrows(FDBDatabaseRunner.RunnerClosed.class, () -> fdb.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, future));
        Thread.sleep(50);
        int count2 = timer.getCount(FDBStoreTimer.Events.COMMIT);
        // Might close just after committing but before recording.
        assertThat("At most one more commits should have occurred", count2, is(oneOf(count1, count1 + 1)));
        Thread.sleep(50);
        int count3 = timer.getCount(FDBStoreTimer.Events.COMMIT);
        assertThat("No more commits should have occurred", count3, is(count2));
    }

    @Test
    public void markReadable() {
        Index index = new Index("newIndex", field("num_value_2"));
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));

        try (FDBRecordContext context = openContext()) {
            // OnlineIndexer.runAsync checks that the index is not readable
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            // No need to build range because there is no record.
            indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.buildEndpoints());

            // Do mark the the index as readable.
            assertTrue(indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadableIfBuilt()));

            // When the index is readable:
            assertFalse(indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadable())); // The status is not modified by markReadable.
            assertTrue(indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadableIfBuilt()));
        }
    }

    @Test
    public void testConfigLoader() throws Exception {
        final Index index = new Index("newIndex", field("num_value_unique"));
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
        openSimpleMetaData(hook);

        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 1000; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValueUnique(i).build();
                recordStore.saveRecord(record);
            }
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        final CompletableFuture<Void> future;
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setConfigLoader(old ->
                        old.toBuilder()
                                .setMaxLimit(old.getMaxLimit() - 1)
                                .setMaxRetries(3)
                                .setRecordsPerSecond(10000)
                                .build())
                .setMaxAttempts(2)
                // The following two options are added to make sure buildIndexAsync begins to use OnlineIndexer.runAsync
                // at the very beginning. Otherwise the assumption "Should have invoked the configuration loader at least
                // once" may not hold true. With the two options, it is running doBuildIndexAsync effectively. Probably
                // it would be better if this test can test the config loader with bare metal OnlineIndexer.runAsync
                // instead.
                .setUseSynchronizedSession(false)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR))
                .build()) {
            int limit = indexBuilder.getLimit();
            future = indexBuilder.buildIndexAsync();
            int pass = 0;
            while (!future.isDone() && timer.getCount(FDBStoreTimer.Events.COMMIT) < 10 && pass++ < 100) {
                Thread.sleep(100);
                assertThat("Should have invoked the configuration loader at least once", indexBuilder.getConfigLoaderInvocationCount(), greaterThan(0));
                assertEquals(indexBuilder.getLimit(), limit - indexBuilder.getConfigLoaderInvocationCount());
                assertEquals(indexBuilder.getConfig().getMaxRetries(), 3);
                assertEquals(indexBuilder.getConfig().getRecordsPerSecond(), 10000);
                assertEquals(indexBuilder.getConfig().getProgressLogIntervalMillis(), DEFAULT_PROGRESS_LOG_INTERVAL);
                assertEquals(indexBuilder.getConfig().getIncreaseLimitAfter(), DO_NOT_RE_INCREASE_LIMIT);
            }
            assertThat("Should have done several transactions in a few seconds", pass, lessThan(100));
        }
    }

    @Test
    public void testOnlineIndexerBuilderWriteLimitBytes() throws Exception {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 200).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("newIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        final FDBStoreTimer timer = new FDBStoreTimer();

        try (FDBRecordContext context = openContext()) {
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            // Build in this transaction.
            try (OnlineIndexer indexer =
                         OnlineIndexer.newBuilder()
                                 .setRecordStore(recordStore)
                                 .setTimer(timer)
                                 .setIndex("newIndex")
                                 .setLimit(100000)
                                 .setMaxWriteLimitBytes(1)
                                 .build()) {
                // this call will "flatten" the staccato iterations to a whole range. Testing compatibility.
                indexer.rebuildIndex(recordStore);
            }
            recordStore.markIndexReadable("newIndex").join();

            assertEquals(200, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals(200, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

            assertEquals(199, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_SIZE));
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT)); // last item

            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexReadable("newIndex"));
            recordStore.clearAndMarkIndexWriteOnly("newIndex").join();
            context.commit();
        }

        timer.reset();
        try (FDBRecordContext context = openContext()) {
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            // Build in this transaction.
            try (OnlineIndexer indexer =
                         OnlineIndexer.newBuilder()
                                 .setRecordStore(recordStore)
                                 .setTimer(timer)
                                 .setIndex("newIndex")
                                 .setLimit(100000)
                                 .setMaxWriteLimitBytes(1)
                                 .build()) {

                Key.Evaluated key = indexer.buildUnbuiltRange(Key.Evaluated.scalar(0L), Key.Evaluated.scalar(25L)).join();
                assertEquals(1, key.getLong(0));
                assertEquals(1, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_SIZE));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
            }
            recordStore.clearAndMarkIndexWriteOnly("newIndex").join();
            context.commit();
        }

        timer.reset();
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTimer(timer)
                .setIndex(index)
                .setLimit(100000)
                .setMaxWriteLimitBytes(1)
                .setRecordsPerSecond(OnlineIndexer.UNLIMITED)
                .build()) {
            indexer.buildIndex();
        }
        assertEquals(200, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(200, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        // this includes two endpoints + one range = total of 3 terminations by count
        // - note that (last, null] endpoint is en empty range
        assertEquals(3, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        // this is the range between the endpoints - 199 items in (first, last] interval
        assertEquals(198, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_SIZE));
    }

    @Test
    public void testMarkReadableClearsBuiltRanges() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 200).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("newIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexer.buildIndex(true);
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            // Verify rangeSet is cleared when index is marked readable
            final RangeSet rangeSet = new RangeSet(recordStore.indexRangeSubspace(index));
            AsyncIterator<Range> ranges = rangeSet.missingRanges(recordStore.ensureContextActive()).iterator();
            final Range range = ranges.next();
            final boolean range1IsEmpty = RangeSet.isFirstKey(range.begin) && RangeSet.isFinalKey(range.end);
            assertTrue(range1IsEmpty);
            context.commit(); // fake commit, happy compiler
        }
    }

    @Test
    public void testTimeLimit() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 200).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("newIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        List<String> indexTypes = Collections.singletonList("MySimpleRecord");
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .setTimeLimitMilliseconds(1)
                .setLimit(20)
                .setConfigLoader(old -> {
                    // Ensure that time limit is exceeded
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        fail("The test was interrupted");
                    }
                    return old;
                })
                .build()) {
            IndexingBase.TimeLimitException e = assertThrows(IndexingBase.TimeLimitException.class, indexer::buildIndex);
            assertTrue(e.getMessage().contains("Time Limit Exceeded"));
        }
    }
}
