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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.OnlineIndexOperationConfig.DEFAULT_PROGRESS_LOG_INTERVAL;
import static com.apple.foundationdb.record.provider.foundationdb.OnlineIndexOperationConfig.DO_NOT_RE_INCREASE_LIMIT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
    private static final Pattern BATCH_GRV_PATTERN = TestHelpers.eventCountPattern(FDBStoreTimer.Events.BATCH_GET_READ_VERSION);
    private static final Pattern SCAN_RECORDS_PATTERN = TestHelpers.eventCountPattern(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
    private static final Pattern BUILD_RANGES_PATTERN = TestHelpers.eventCountPattern(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT);

    @Test
    @SuppressWarnings("removal")
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
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
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

            // Update Aug/22: buildEndpoints uses the IndexingByRecords module, while buildIndex uses the new
            // IndexingMultiTargetByRecords one (which doesn't use endpoints), yet this test - which combines the two -
            // is kept 'as is' for an extra sanity test.
            // Straight up build the whole index.
            indexBuilder.buildIndex(false);
            assertEquals(Tuple.from(55L), getAggregate.get());
            assertEquals(Collections.emptyList(), rangeSet.missingRanges(fdb.database()).join());
        }
    }

    @Test
    @SuppressWarnings("removal")
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
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
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
    @SuppressWarnings("removal")
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
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
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
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
                .build()) {
            indexBuilder.buildIndex();
        }

        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
        }
    }

    @Test
    public void logsEnd() {
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
        final List<String> logs = TestHelpers.assertLogs(IndexingBase.class, "build index online", () -> {
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setIndex(index)
                    .build()) {
                indexBuilder.buildIndex();
            }
            return null;
        });

        MatcherAssert.assertThat(logs, Matchers.hasSize(1));
        final String log = logs.get(0);
        MatcherAssert.assertThat(log, Matchers.allOf(
                Matchers.containsString("records_scanned=\"50\""),
                Matchers.containsString("indexing_method=\"multi target by records\""),
                Matchers.containsString("total_micros"),
                Matchers.containsString("target_index_name"),
                Matchers.containsString("result=\"success\""),
                Matchers.containsString("indexer_id")));
    }

    @Test
    public void lessenLimits() {
        Index index = runAsyncSetup();
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
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


        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
                .setMdcContext(ImmutableMap.of("mdcKey", "my cool mdc value"))
                .setMaxAttempts(3)
                .setConfigLoader(old ->
                        OnlineIndexOperationConfig.newBuilder()
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
            newIndexerBuilder().setIndex(absentIndex).build();
        });
        assertEquals("Index absent not contained within specified metadata", e.getMessage());
        // Limit
        e = assertThrows(RecordCoreException.class, () ->
                newIndexerBuilder().setIndex(indexPrime).setLimit(-1).build()
        );
        assertEquals("Non-positive value -1 given for record limit", e.getMessage());
        e = assertThrows(RecordCoreException.class, () ->
                newIndexerBuilder().setIndex(indexPrime).setLimit(0).build()
        );
        assertEquals("Non-positive value 0 given for record limit", e.getMessage());
        // Retries
        e = assertThrows(RecordCoreException.class, () ->
                newIndexerBuilder().setIndex(indexPrime).setMaxRetries(-1).build()
        );
        assertEquals("Non-positive value -1 given for maximum retries", e.getMessage());
        e = assertThrows(RecordCoreException.class, () ->
                newIndexerBuilder().setIndex(indexPrime).setMaxRetries(0).build()
        );
        assertEquals("Non-positive value 0 given for maximum retries", e.getMessage());
        // Records per second
        e = assertThrows(RecordCoreException.class, () ->
                newIndexerBuilder().setIndex(indexPrime).setRecordsPerSecond(-1).build()
        );
        assertEquals("Non-positive value -1 given for records per second value", e.getMessage());
        e = assertThrows(RecordCoreException.class, () ->
                newIndexerBuilder().setIndex(indexPrime).setRecordsPerSecond(0).build()
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
            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(record);
            }
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        final CompletableFuture<Void> future;
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
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
    @SuppressWarnings("deprecation")
    public void markReadable() {
        Index index = new Index("newIndex", field("num_value_2"));
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index));

        try (FDBRecordContext context = openContext()) {
            // OnlineIndexer.runAsync checks that the index is not readable
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(index)
                .build()) {
            // Build index, but do not mark readable
            indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.buildIndexAsync(false));

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
            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValueUnique(i).build();
                recordStore.saveRecord(record);
            }
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        final CompletableFuture<Void> future;
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
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
    void testConfigLoaderInitialLimit() {
        final Index index = new Index("newIndex", field("num_value_unique"));
        populateData(40);
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));
        final FDBStoreTimer timer = new FDBStoreTimer();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index, timer)
                .setInitialLimit(4)
                .setIncreaseLimitAfter(100) // high enough to keep the initial limit
                .setLimit(10000)
                .build()) {
            indexBuilder.buildIndex();
        }
        // Ensure that 40 records were scanned in 10 separate ranges (of 4 records each - the initial limit)
        assertEquals(10, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Tag(Tags.Slow)
    @Test
    void testOnlineIndexerBuilderWriteLimitBytes() {
        int numRecords = 127;
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("newIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.clearAndMarkIndexWriteOnly(index.getName()).join();
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = openContext()) {
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            // Build in this transaction.
            try (OnlineIndexer indexer =
                         OnlineIndexer.newBuilder()
                                 .setRecordStore(recordStore)
                                 .setTimer(timer)
                                 .setIndex(index.getName())
                                 .setLimit(100000)
                                 .setMaxWriteLimitBytes(1)
                                 .build()) {

                indexer.rebuildIndex(recordStore);
            }
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
            assertEquals(numRecords - 1, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_SIZE));
            assertEquals(1, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));

            recordStore.clearAndMarkIndexWriteOnly(index.getName()).join();
            context.commit();
        }

        timer.reset();
        try (OnlineIndexer indexer = newIndexerBuilder()
                .setTimer(timer)
                .setIndex(index)
                .setLimit(100000)
                .setMaxWriteLimitBytes(1)
                .setRecordsPerSecond(OnlineIndexer.UNLIMITED)
                .build()) {
            indexer.buildIndex();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numRecords - 1, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_SIZE));
        assertEquals(1, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testMarkReadableClearsBuiltRanges() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 128).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());
        Index index = new Index("newIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(index)
                .build()) {
            indexer.buildIndex(true);
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            // Verify rangeSet is cleared when index is marked readable
            assertTrue(recordStore.isIndexReadable(index));

            final RangeSet rangeSet = new RangeSet(recordStore.indexRangeSubspace(index));
            Boolean isEmpty = rangeSet.isEmpty(context.ensureActive()).join();
            assertTrue(isEmpty);
            context.commit(); // fake commit, happy compiler
        }
    }

    @Test
    public void testTimeLimit() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 111).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        Index index = new Index("newIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(index)
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

    @Test
    public void testLogInterval() {
        final int limit = 20;
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 50).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
        ).collect(Collectors.toList());

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }

        Index index = new Index("newIndex", field("num_value_2").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(index)
                .setProgressLogIntervalMillis(10)
                .setLimit(limit)
                .setTimer(null)
                .setConfigLoader(old -> {
                    // Ensure that time limit is exceeded
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        fail("The test was interrupted");
                    }
                    return old;
                })
                .build()) {
            List<String> events = TestHelpers.assertLogs(IndexingBase.class, "Indexer: Built Range",
                    () -> {
                        indexer.buildIndex();
                        return null;
                    });
            events.forEach(logEvent -> {
                TestHelpers.assertDoesNotMatch(BATCH_GRV_PATTERN, logEvent);
                TestHelpers.assertDoesNotMatch(BUILD_RANGES_PATTERN, logEvent);
                TestHelpers.assertDoesNotMatch(SCAN_RECORDS_PATTERN, logEvent);
            });
        }

        // test with zero interval (always log)
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexDisabled(index).join();
            context.commit();
        }
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(index)
                .setTimer(timer)
                .setProgressLogIntervalMillis(0)
                .setLimit(limit)
                .setConfigLoader(old -> {
                    // Ensure that time limit is exceeded
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        fail("The test was interrupted");
                    }
                    return old;
                })
                .build()) {
            List<String> events = TestHelpers.assertLogs(IndexingBase.class, "Indexer: Built Range",
                    () -> {
                        indexer.buildIndex();
                        return null;
                    });
            events.forEach(logEvent -> {
                int batchReadVersions = TestHelpers.extractCount(BATCH_GRV_PATTERN, logEvent);
                int buildRanges = TestHelpers.extractCount(BUILD_RANGES_PATTERN, logEvent);
                assertThat(buildRanges, lessThanOrEqualTo(batchReadVersions));
                assertEquals(1, buildRanges, () -> "expected only 1 build range in \"" + logEvent + "\"");
                int scannedRecords = TestHelpers.extractCount(SCAN_RECORDS_PATTERN, logEvent);
                assertThat("expected only " + limit + " records scanned in \"" + logEvent + "\"", scannedRecords, lessThanOrEqualTo(limit));
            });
        }

        // test with negative interval (never log)
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexDisabled(index).join();
            context.commit();
        }
        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(index)
                .setProgressLogIntervalMillis(-1)
                .setLimit(20)
                .setConfigLoader(old -> {
                    // Ensure that time limit is exceeded
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        fail("The test was interrupted");
                    }
                    return old;
                })
                .build()) {
            TestHelpers.assertDidNotLog(IndexingBase.class, "Indexer: Built Range",
                    () -> {
                        indexer.buildIndex();
                        return null;
                    });
        }
    }

    private void postTransaction(IndexingThrottle.Booker booker, int repeats, long recordScanned, boolean failed) {
        AtomicLong scanned = new AtomicLong(recordScanned);
        final Throwable dummyThrowable = failed ? new Throwable() : null;
        for (int i = 0; i < repeats; i++) {
            booker.handleLimitsPostRunnerTransaction(dummyThrowable, scanned, true, null);
        }
    }

    private void postTransaction(IndexingThrottle.Booker booker, int repeats) {
        postTransaction(booker, repeats, 10, false);
    }

    private void decreaseLimit(IndexingThrottle.Booker booker) {
        FDBException dummyException = new FDBException("Dummy Exception for Booker", 5);
        booker.decreaseLimit(dummyException, Collections.emptyList());
    }

    @Test
    void testIndexingThrottleBooker() {
        final OnlineIndexOperationConfig config = OnlineIndexOperationConfig.newBuilder()
                .setInitialLimit(4)
                .setRecordsPerSecond(100)
                .setIncreaseLimitAfter(5)
                .setMaxLimit(1000)
                .build();
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            final IndexingCommon common = new IndexingCommon(context.newRunner(),
                    recordStore.asBuilder(),
                        Collections.emptyList(),
                    Collections.emptyList(),
                    null,
                    config,
                    false);

            final IndexingThrottle.Booker booker = new IndexingThrottle.Booker(common);
            assertEquals(4, booker.getRecordsLimit());
            postTransaction(booker, 5);
            assertEquals(4, booker.getRecordsLimit());
            postTransaction(booker, 1);
            assertEquals(9, booker.getRecordsLimit());
            postTransaction(booker, 6);
            assertEquals(18, booker.getRecordsLimit());
            postTransaction(booker, 6);
            assertEquals(36, booker.getRecordsLimit());
            postTransaction(booker, 6);
            assertEquals(72, booker.getRecordsLimit());
            postTransaction(booker, 6);
            assertEquals(144, booker.getRecordsLimit());
            postTransaction(booker, 5);
            // do not increase on error
            postTransaction(booker, 1, 100, true);
            assertEquals(144, booker.getRecordsLimit());
            // try decrease - should get last failure scanned (100) * 0.9
            decreaseLimit(booker);
            assertEquals(90, booker.getRecordsLimit());
            long waitTime = booker.waitTimeMilliseconds();
            assertThat("wait time should be smaller than a second", waitTime < 1000);
            // now increase more
            postTransaction(booker, 6);
            assertEquals(180, booker.getRecordsLimit());
            postTransaction(booker, 6);
            assertEquals(240, booker.getRecordsLimit()); // (180 * 4 / 3)
            postTransaction(booker, 6);
            assertEquals(320, booker.getRecordsLimit()); // (240 * 4 / 3)
            postTransaction(booker, 100);
            assertEquals(1000, booker.getRecordsLimit()); // reach max - note that the actual record scanned count might be limited by write size (for example), and be much smaller than the limit
            // try failure
            postTransaction(booker, 1, 500, true);
            assertEquals(1000, booker.getRecordsLimit());
            decreaseLimit(booker);
            assertEquals(450, booker.getRecordsLimit());
            // check wait time
            waitTime = booker.waitTimeMilliseconds();
            assertThat("wait time should be smaller than a second", waitTime < 1000);
            // this wait time should be very close to a second
            postTransaction(booker, 1, 1000, false);
            waitTime = booker.waitTimeMilliseconds();
            assertThat("wait time should be smaller than a second", waitTime < 1000);
            assertThat("wait time should be big (after not doing much)", waitTime > 900);
        }
    }

    void mayRetryAfterHandlingException(@Nonnull IndexingThrottle.Booker booker, @Nullable Throwable ex, int currTries, boolean shouldRetryExpected) {
        final FDBException fdbException = IndexingThrottle.getFDBException(ex);
        final boolean shouldRetry = booker.mayRetryAfterHandlingException(fdbException, Collections.emptyList(), currTries, true);
        assertEquals(shouldRetryExpected, shouldRetry);
    }

    @Test
    void testIndexingThrottleBookerExceptions() {
        final OnlineIndexOperationConfig config = OnlineIndexOperationConfig.newBuilder()
                .setInitialLimit(100)
                .setRecordsPerSecond(100)
                .setIncreaseLimitAfter(5)
                .setMaxRetries(5)
                .setMaxLimit(1000)
                .build();
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            final IndexingCommon common = new IndexingCommon(context.newRunner(),
                    recordStore.asBuilder(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    null,
                    config,
                    false);

            final IndexingThrottle.Booker booker = new IndexingThrottle.Booker(common);
            assertEquals(100, booker.getRecordsLimit());
            mayRetryAfterHandlingException(booker, new IllegalStateException("illegal state"), 1, false);
            assertEquals(100, booker.getRecordsLimit());
            mayRetryAfterHandlingException(booker, new RecordCoreRetriableTransactionException("Retriable", new FDBException("commit_unknown_result", 1021)), 1, false); // retriable in runner, that is
            assertEquals(100, booker.getRecordsLimit());
            mayRetryAfterHandlingException(booker, new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101)), 1, true);
            assertEquals(1, booker.getRecordsLimit()); // last failure count is still zero, down to minimum
            mayRetryAfterHandlingException(booker, new RecordCoreException("Non-retriable", new FDBException("transaction_too_large", 2101)), 6, false); // exceed max retries
            postTransaction(booker, 100);
            assertEquals(1000, booker.getRecordsLimit()); // up to max
            postTransaction(booker, 1, 1000, true); // set last failure count to 1000
            mayRetryAfterHandlingException(booker, new RecordCoreRetriableTransactionException("Retriable and lessener", new FDBException("not_committed", 1020)), 1, true);
            assertEquals(900, booker.getRecordsLimit()); // reduce limit to last failure - 10%
        }
    }

    @Test
    void testIndexingThrottleBookerRepeatingExceptions() {
        final OnlineIndexOperationConfig config = OnlineIndexOperationConfig.newBuilder()
                .setInitialLimit(1000)
                .setRecordsPerSecond(100)
                .setIncreaseLimitAfter(5)
                .setMaxRetries(5)
                .setMaxLimit(10000)
                .build();
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            final IndexingCommon common = new IndexingCommon(context.newRunner(),
                    recordStore.asBuilder(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    null,
                    config,
                    false);

            final IndexingThrottle.Booker booker = new IndexingThrottle.Booker(common);
            postTransaction(booker, 1, 1000, true); // set last failure count to 1000
            assertEquals(1000, booker.getRecordsLimit()); // up to max
            final List<Integer> expectedLimitList = Arrays.asList(900, 720, 504,  252, 126, 63, 31,  3, 1, 1, 1, 1, 1, 1);
            final RecordCoreRetriableTransactionException exception = new RecordCoreRetriableTransactionException("Retriable and lessener", new FDBException("not_committed", 1020));
            for (int expectedLimit: expectedLimitList) {
                mayRetryAfterHandlingException(booker, exception, 1, true);
                assertEquals(expectedLimit, booker.getRecordsLimit()); // limit
                postTransaction(booker, 1, expectedLimit, true); // set last failure count to new limit
            }
        }
    }

    @Test
    public void runWithWeakReadSemantics() {
        boolean dbTracksReadVersionOnRead = fdb.isTrackLastSeenVersionOnRead();
        boolean dbTracksReadVersionOnCommit = fdb.isTrackLastSeenVersionOnCommit();
        AtomicLong readVersionA = new AtomicLong();
        AtomicLong readVersionB = new AtomicLong();
        try {
            fdb.setTrackLastSeenVersion(true);
            Index index = runAsyncSetup();

            FDBDatabase.WeakReadSemantics weakReadSemantics = new FDBDatabase.WeakReadSemantics(0L, Long.MAX_VALUE, true);
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setIndex(index)
                    .setWeakReadSemantics(weakReadSemantics)
                    .build()) {
                indexBuilder.buildCommitRetryAsync((recordStore, recordsScanned) -> {
                    assertSame(weakReadSemantics, recordStore.getContext().getWeakReadSemantics());
                    assertTrue(recordStore.getContext().hasReadVersion());
                    final Long readVersion;
                    try {
                        readVersion = recordStore.getContext().getReadVersionAsync().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    readVersionA.set(readVersion);
                    return null;
                }, null).handle((val, e) -> null).join();
                indexBuilder.buildCommitRetryAsync((recordStore, recordsScanned) -> {
                    assertSame(weakReadSemantics, recordStore.getContext().getWeakReadSemantics());
                    assertTrue(recordStore.getContext().hasReadVersion());
                    final Long readVersion;
                    try {
                        readVersion = recordStore.getContext().getReadVersionAsync().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    readVersionB.set(readVersion);
                    return null;
                }, null).handle((val, e) -> null).join();
                assertEquals(readVersionA.get(), readVersionB.get(), "weak read semantics did not preserve read version");
            }
        } finally {
            fdb.setTrackLastSeenVersionOnRead(dbTracksReadVersionOnRead);
            fdb.setTrackLastSeenVersionOnRead(dbTracksReadVersionOnCommit);
        }
    }

}
