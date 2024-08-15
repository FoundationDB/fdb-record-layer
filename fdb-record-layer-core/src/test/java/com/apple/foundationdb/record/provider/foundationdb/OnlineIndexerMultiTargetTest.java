/*
 * OnlineIndexerIndexFromIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building multi target indexes {@link OnlineIndexer}.
 */
@Tag(Tags.Slow)
class OnlineIndexerMultiTargetTest extends OnlineIndexerTest {

    private void populateOtherData(final long numRecords) {
        List<TestRecords1Proto.MyOtherRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(val + 100000).build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private void buildIndexAndCrashHalfway(int chunkSize, int count, FDBStoreTimer timer, @Nonnull OnlineIndexer.Builder builder) {
        final AtomicLong counter = new AtomicLong(0);
        try (OnlineIndexer indexBuilder = builder
                .setLimit(chunkSize)
                .setTimer(timer)
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > count) {
                        // crash/abort after indexing "count" chunks of "chunkSize" records
                        throw new RecordCoreException("Intentionally crash during test");
                    }
                    return old;
                })
                .build()) {

            assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            // The index should be partially built
        }
        assertEquals(count , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @ParameterizedTest
    @BooleanSource
    void testMultiTargetSimple(boolean reverseScan) {
        // Simply build the index

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setReverseScanOrder(reverseScan))
                .build()) {
            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertReadable(indexes);

        // Here: Add a non-value index
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        // Test building non-idempotent + force
        timer.reset();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .setReverseScanOrder(reverseScan)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);

        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertReadable(indexes);

        // Now use an arbitrary primary index
        timer.reset();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .setReverseScanOrder(reverseScan)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertReadable(indexes);
        assertAllValidated(indexes);
    }

    @ParameterizedTest
    @BooleanSource
    void testMultiTargetContinuation(boolean reverseScan) {
        // Build the index in small chunks

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;
        final int numChunks  = 1 + (numRecords / chunkSize);

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setReverseScanOrder(reverseScan))
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex(true);
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertReadable(indexes);
        assertAllValidated(indexes);
    }

    @ParameterizedTest
    @BooleanSource
    void testMultiTargetWithTimeQuota(boolean reverseScan) {
        // Build the index in small chunks

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setTransactionTimeLimitMilliseconds(1)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setReverseScanOrder(reverseScan))
                .build()) {
            indexBuilder.buildIndex(true);
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_DEPLETION));
        assertReadable(indexes);
        assertAllValidated(indexes);
    }

    @Test
    void testMultiTargetMismatchStateFailure() {
        //Throw when one index has a different status

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 3;

        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // built one index
        try (OnlineIndexer indexer = newIndexerBuilder(indexes.get(1)).build()) {
            indexer.buildIndex(false);
        }

        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer).build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("A target index state doesn't match the primary index state"));
        }

        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

        // Now finish with REBUILD
        timer.reset();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertReadable(indexes);
        assertAllValidated(indexes);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7})
    void testMultiTargetPartlyBuildFailure(int reverseSeed) {
        // Throw when one index has a different type stamp

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 13;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // test all 8 reverse combinations
        boolean reverse1 = 0 != (reverseSeed & 1);
        boolean reverse2 = 0 != (reverseSeed & 2);
        boolean reverse3 = 0 != (reverseSeed & 4);
        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 2, timer, newIndexerBuilder()
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setReverseScanOrder(reverse1))
                .setTargetIndexes(indexes));

        // 2. let one index continue ahead
        Index indexAhead = indexes.get(2);
        timer.reset();
        buildIndexAndCrashHalfway(chunkSize, 2, timer, newIndexerBuilder(indexAhead)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setReverseScanOrder(reverse2)
                        .checkIndexingStampFrequencyMilliseconds(0)
                        .allowTakeoverContinue()));

        // 3. assert mismatch type stamp
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setReverseScanOrder(reverse3)
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
            assertTrue(e instanceof IndexingBase.PartlyBuiltException);
            final IndexBuildProto.IndexBuildIndexingStamp savedStamp = ((IndexingBase.PartlyBuiltException)e).getSavedStamp();
            assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS, savedStamp.getMethod());
        }
    }

    @Test
    void testMultiTargetPartlyBuildChangeTargets() {
        // Throw when the index list changes

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 2, timer, newIndexerBuilder(indexes));

        // 2. Change indexes set
        indexes.remove(2);

        // 3. assert mismatch type stamp
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
            assertTrue(e instanceof IndexingBase.PartlyBuiltException);
            final IndexBuildProto.IndexBuildIndexingStamp savedStamp = ((IndexingBase.PartlyBuiltException)e).getSavedStamp();
            assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS, savedStamp.getMethod());
            assertTrue(savedStamp.getTargetIndexList().containsAll(Arrays.asList("indexA", "indexB", "indexC", "indexD")));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void testMultiTargetContinueAfterCrash(int reverseSeed) {
        // Crash, then continue successfully

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexE", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // test all 4 reverse combinations
        boolean reverse1 = 0 != (reverseSeed & 1);
        boolean reverse2 = 0 != (reverseSeed & 2);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 5, timer, newIndexerBuilder(indexes).setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                .setReverseScanOrder(reverse1)));

        // 2. continue and done
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setReverseScanOrder(reverse2)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertReadable(indexes);
        assertAllValidated(indexes);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void testMultiTargetIndividualContinueAfterCrash(int reverseSeed) {
        // After crash, finish building each index individually

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;

        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // test all 4 reverse combinations
        boolean reverse1 = 0 != (reverseSeed & 1);
        boolean reverse2 = 0 != (reverseSeed & 2);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 3, timer, newIndexerBuilder(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setReverseScanOrder(reverse1)));

        // 2. continue each index to done
        for (Index index: indexes) {
            try (OnlineIndexer indexBuilder = newIndexerBuilder(index)
                    .setLimit(chunkSize)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                            .allowTakeoverContinue()
                            .setReverseScanOrder(reverse2)
                            .build())
                    .build()) {

                indexBuilder.buildIndex(true);
            }
        }

        // 3. Verify all readable and valid
        assertReadable(indexes);
        assertAllValidated(indexes);
    }

    @Test
    void testMultiTargetIndividualContinueByIndexAfterCrash() {
        // After crash, finish building each index individually

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 40;
        final int chunkSize  = 7;

        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 3, timer, newIndexerBuilder(indexes));

        // 2. Finish building "num_value_3_indexed", to be used as a source index
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes.get(0))
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowTakeoverContinue()
                        .build())
                .build()) {
            indexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexReadable(indexes.get(0)));
            context.commit();
        }

        // 3. Build "num_value_2" by index, forbid fallback to by-record
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes.get(1))
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex(indexes.get(0).getName())
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .setLimit(chunkSize)
                .build()) {
            assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            // The index should be partially built
        }

        // 4. Build "num_value_2" by index, allow fallback to by-record
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes.get(1))
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex(indexes.get(0).getName())
                        .allowTakeoverContinue()
                        .build())
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexReadable(indexes.get(1)));
            context.commit();
        }
    }

    @Test
    void testMultiTargetRebuild() {
        // Use inline rebuildIndex
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer).build()) {
                indexBuilder.rebuildIndex(recordStore);
            }
            for (Index index: indexes) {
                recordStore.markIndexReadable(index).join();
            }
            context.commit();
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertAllValidated(indexes);
    }

    @Test
    void testMultiTargetMultiType() {
        // Use different record types

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 42;
        final int numRecordsOther = 24;
        final int chunkSize  = 17;
        final int numChunks  = 1 + ((numRecords + numRecordsOther) / chunkSize);

        populateData(numRecords);
        populateOtherData(numRecordsOther);

        Index indexMyA = new Index("indexMyA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index indexMyB = new Index("indexMyB", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index indexOtherA = new Index("indexOtherA", field("num_value_2"), IndexTypes.VALUE);
        Index indexOtherB = new Index("indexOtherB", field("num_value_2"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", indexMyA);
            metaDataBuilder.addIndex("MySimpleRecord", indexMyB);
            metaDataBuilder.addIndex("MyOtherRecord", indexOtherA);
            metaDataBuilder.addIndex("MyOtherRecord", indexOtherB);
        };
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .addTargetIndex(indexMyA)
                .addTargetIndex(indexOtherA)
                .addTargetIndex(indexMyB)
                .addTargetIndex(indexOtherB)
                .setTimer(timer)
                .setLimit(chunkSize)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords + numRecordsOther, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords + numRecordsOther, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertAllValidated(Arrays.asList(indexMyA, indexMyB, indexOtherA, indexOtherB));
    }

    @Test
    @SuppressWarnings("removal")
    void testSingleTargetContinuation() {
        // Build a single target with the old module, crash halfway, then continue indexing as a single index in the Multi Target module
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize = 17;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        final AtomicLong counter = new AtomicLong(0);
        final String testThrowMsg = "Intentionally crash during test";
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes.get(0), timer)
                .setLimit(chunkSize)
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > 2) {
                        // crash/abort after indexing two chunks of records
                        throw new RecordCoreException(testThrowMsg);
                    }
                    return old;
                })
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndexSingleTarget);
            assertTrue(e.getMessage().contains(testThrowMsg));
        }
        // expecting an extra range, because the old indexer is building endpoints
        assertEquals(3, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        // Here: the index should be partially built by the single target module

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer).build()) {
            // Finish building the single index with the multi target indexer
            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    void testSingleTargetCompletion() {
        // Build few single target ranges with the old module, then complete indexing as a single index with the Multi Target module
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(indexes.get(0)).join();
            context.commit();
        }

        final AtomicLong counter = new AtomicLong();
        final String throwMsg = "Intentionally thrown during test";
        for (long i = 0; (i + 1) * 10 < numRecords; i ++) {
            counter.set(0);
            try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes.get(0), timer)
                    .setLimit(3)
                    .setConfigLoader(old -> {
                        if (counter.incrementAndGet() > 1) {
                            // crash/abort after indexing one chunk of 3 records
                            throw new RecordCoreException(throwMsg);
                        }
                        return old;
                    })
                    .build()) {
                RecordCoreException ex = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
                assertTrue(ex.getMessage().contains(throwMsg));
            }
            assertEquals((i + 1) * 3, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals((i + 1) * 3, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        }
        // Here: the index has multiple built chunks

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer).build()) {
            // Finish building the single index with the multi target indexer
            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    // uncomment to compare
    // @Test
    void benchMarkMultiTarget() {
        // compare single target build to multi target index building
        final FDBStoreTimer singleTimer = new FDBStoreTimer();
        final FDBStoreTimer multiTimer = new FDBStoreTimer();
        final int numRecords = 5555;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        long startSingle = System.currentTimeMillis();
        for (Index index : indexes) {
            openSimpleMetaData(hook);
            try (OnlineIndexer indexBuilder = newIndexerBuilder(index, singleTimer).build()) {
                indexBuilder.buildIndex(true);
            }
        }
        long endSingle = System.currentTimeMillis();
        assertTrue(endSingle > startSingle); // suppress warnings
        disableAll(indexes);

        long startMulti =  System.currentTimeMillis();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, multiTimer).build()) {
            indexBuilder.buildIndex(true);
        }
        long endMulti =  System.currentTimeMillis();
        assertTrue(endMulti > startMulti); // suppress warnings

        System.out.printf("%d indexes, %d records. Single build took %d milliSeconds, MultiIndex took %d%n",
                indexes.size(), numRecords, endSingle - startSingle, endMulti - startMulti);
    }

    @Test
    void testMultiTargetIndexingBlocker() {
        // block indexing, then release
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 2, timer, newIndexerBuilder(indexes));

        // 2. Block indexing
        FDBRecordStoreTestBase.RecordMetaDataHook localHook = allIndexesHook(indexes);
        openSimpleMetaData(localHook);
        String luka = "Blocked by Luka";
        try (OnlineIndexer indexer = newIndexerBuilder(indexes).build()) {
            final Map<String, IndexBuildProto.IndexBuildIndexingStamp> stampMap =
                    indexer.blockIndexBuilds(luka, 10L);
            final List<String> indexNames = indexes.stream().map(Index::getName).collect(Collectors.toList());
            assertTrue(stampMap.keySet().containsAll(indexNames));
            for (String indexName : indexNames) {
                final IndexBuildProto.IndexBuildIndexingStamp stamp = stampMap.get(indexName);
                assertTrue(stamp.getTargetIndexList().containsAll(indexNames));
                assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS, stamp.getMethod());
                assertTrue(stamp.getBlock());
            }
        }

        // 3. query, ensure blocked
        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = newIndexerBuilder(indexes).build()) {
            final Map<String, IndexBuildProto.IndexBuildIndexingStamp> stampMap =
                    indexer.queryIndexingStamps();
            final List<String> indexNames = indexes.stream().map(Index::getName).collect(Collectors.toList());
            assertTrue(stampMap.keySet().containsAll(indexNames));
            for (String indexName : indexNames) {
                final IndexBuildProto.IndexBuildIndexingStamp stamp = stampMap.get(indexName);
                assertTrue(stamp.getTargetIndexList().containsAll(indexNames));
                assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS, stamp.getMethod());
                assertTrue(stamp.getBlock());
                assertEquals(luka, stamp.getBlockID());
                assertTrue(stamp.getBlockExpireEpochMilliSeconds() > System.currentTimeMillis());
                assertTrue(stamp.getBlockExpireEpochMilliSeconds() < 20_000 + System.currentTimeMillis());
            }
        }

        // 4. fail continue without unblock
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setLimit(chunkSize)
                .build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built, and blocked"));
        }

        // 5. continue with unblock
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setAllowUnblock(true)
                        .build())
                .build()) {
            indexBuilder.buildIndex();
        }

        // validate
        assertReadable(indexes);
        assertAllValidated(indexes);
    }

    @Test
    void testMultiTargetIndexingBlockerExpiration() {
        // block indexing with a short ttl, continue after sleep
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 59;
        final int chunkSize  = 13;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 2, timer, newIndexerBuilder(indexes));

        // 2. Block indexing
        FDBRecordStoreTestBase.RecordMetaDataHook localHook = allIndexesHook(indexes);
        openSimpleMetaData(localHook);
        String luka = "Blocked by Luka";
        try (OnlineIndexer indexer = newIndexerBuilder(indexes).build()) {
            indexer.blockIndexBuilds(luka, 2L);
        }

        // 3. fail continue without unblock
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setLimit(chunkSize)
                .build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built, and blocked"));
        }

        // 4. sleep
        snooze(2000);

        // 5. continue normally, the block should been have expired
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes, timer)
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex();
        }

        // validate
        assertAllValidated(indexes);
    }

    void snooze(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testForbidConversionOfActiveMultiTarget() throws InterruptedException {
        // Do not let a conversion of few indexes of an active multi-target session
        final int numRecords = 59;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        Semaphore pauseMutualBuildSemaphore = new Semaphore(1);
        Semaphore startBuildingSemaphore =  new Semaphore(1);
        pauseMutualBuildSemaphore.acquire();
        startBuildingSemaphore.acquire();
        Thread t1 = new Thread(() -> {
            // build index and pause halfway, allowing an active session test
            try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                    .setLeaseLengthMillis(TimeUnit.SECONDS.toMillis(20))
                    .setLimit(4)
                    .setConfigLoader(old -> {
                        try {
                            startBuildingSemaphore.release();
                            pauseMutualBuildSemaphore.acquire(); // pause to try building indexes
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        pauseMutualBuildSemaphore.release();
                        return old;
                    })
                    .build()) {
                indexBuilder.buildIndex();
            }
        });
        t1.start();
        startBuildingSemaphore.acquire();
        startBuildingSemaphore.release();
        // Try one index at a time
        for (Index index : indexes) {
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setIndex(index)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .allowTakeoverContinue(List.of(OnlineIndexer.IndexingPolicy.TakeoverTypes.MULTI_TARGET_TO_SINGLE)))
                    .build()) {
                assertThrows(SynchronizedSessionLockedException.class, indexBuilder::buildIndex);
            }
        }
        // let the other thread finish indexing
        pauseMutualBuildSemaphore.release();
        t1.join();
        // happy indexes assertion
        assertReadable(indexes);
    }
}
