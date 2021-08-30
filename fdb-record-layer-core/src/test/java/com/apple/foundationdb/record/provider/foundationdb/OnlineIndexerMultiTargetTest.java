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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building indexes from other indexes with {@link OnlineIndexer}.
 */
class OnlineIndexerMultiTargetTest extends OnlineIndexerTest {

    private void populateData(final long numRecords) {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private void populateOtherData(final long numRecords) {
        List<TestRecords1Proto.MyOtherRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(val + 100000).build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private FDBRecordStoreTestBase.RecordMetaDataHook allIndexesHook(List<Index> indexes) {
        return metaDataBuilder -> {
            for (Index index: indexes) {
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }
        } ;
    }

    private void disableAll(List<Index> indexes) {
        try (FDBRecordContext context = openContext()) {
            // disable all
            for (Index index : indexes) {
                recordStore.markIndexDisabled(index).join();
            }
            context.commit();
        }
    }

    private void buildIndexAndCrashHalfway(int chunkSize, int count, FDBStoreTimer timer, @Nullable OnlineIndexer.Builder builder) {
        final AtomicLong counter = new AtomicLong(0);
        try (OnlineIndexer indexBuilder = builder
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setLimit(chunkSize)
                .setTimer(timer)
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > count) {
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

    private void validateIndexes(List<Index> indexes) {
        final FDBStoreTimer timer = new FDBStoreTimer();
        for (Index index: indexes) {
            if (index.getType().equals(IndexTypes.VALUE)) {
                try (OnlineIndexScrubber indexScrubber = OnlineIndexScrubber.newBuilder()
                        .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                        .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                                .setLogWarningsLimit(Integer.MAX_VALUE)
                                .setAllowRepair(false)
                                .build())
                        .setTimer(timer)
                        .build()) {
                    indexScrubber.scrubDanglingIndexEntries();
                    indexScrubber.scrubMissingIndexEntries();
                }
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            }
        }
    }

    @Test
    public void testMultiTargetSimple() {
        // Simply build the index

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }

        // Here: Add a non-value index
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        // Test building non-idempotent + force
        timer.reset();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);

        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }

        // Now use an arbitrary primary index
        timer.reset();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);

        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }
        validateIndexes(indexes);
    }

    @Test
    public void testMultiTargetContinuation() {
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

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setLimit(chunkSize)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        validateIndexes(indexes);
    }

    @Test
    public void testMultiTargetMismatchStateFailure() {
        //Throw when one index has a different status

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 3;

        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // built one index
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(indexes.get(1)).setSubspace(subspace)
                .build()) {
            indexer.buildIndex(false);
        }

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("A target index state doesn't match the primary index state"));
        }

        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

        // Now finish with REBUILD
        timer.reset();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        validateIndexes(indexes);
    }

    @Test
    public void testMultiTargetPartlyBuildFailure() {
        // Throw when one index has a different type stamp

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 2, timer, OnlineIndexer.newBuilder()
                .setTargetIndexes(indexes));

        // 2. let one index continue ahead
        timer.reset();
        buildIndexAndCrashHalfway(chunkSize, 2, timer, OnlineIndexer.newBuilder()
                .setIndex(indexes.get(2)));

        // 3. assert mismatch type stamp
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
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

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 2, timer, OnlineIndexer.newBuilder()
                .setTargetIndexes(indexes));

        // 2. Change indexes set
        indexes.remove(2);

        // 3. assert mismatch type stamp
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }
    }

    @Test
    void testMultiTargetContinueAfterCrash() {
        // Crash, then continue successfully

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 5, timer, OnlineIndexer.newBuilder()
                .setTargetIndexes(indexes));

        // 2. continue and done
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setLimit(chunkSize)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }
        validateIndexes(indexes);
    }

    @Test
    public void testMultiTargetIndividualContinueAfterCrash() {
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

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // 1. partly build multi
        buildIndexAndCrashHalfway(chunkSize, 3, timer, OnlineIndexer.newBuilder()
                .setTargetIndexes(indexes));

        // 2. continue each index to done
        for (Index index: indexes) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                    .setIndex(index)
                    .setLimit(chunkSize)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                            .build())
                    .build()) {

                indexBuilder.buildIndex(true);
            }
        }

        // 3. Verify all readable
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }
        validateIndexes(indexes);
    }

    @Test
    public void testMultiTargetRebuild() {
        // Use inline rebuildIndex
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                    .setTargetIndexes(indexes)
                    .setTimer(timer)
                    .build()) {

                indexBuilder.rebuildIndex(recordStore);
            }
            for (Index index: indexes) {
                recordStore.markIndexReadable(index).join();
            }
            context.commit();
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        validateIndexes(indexes);
    }

    @Test
    public void testMultiTargetMultiType() {
        // Use different record types

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 42;
        final int numRecordsOther = 24;
        final int chunkSize  = 17;
        final int numChunks  = 1 + ((numRecords + numRecordsOther) / chunkSize);

        openSimpleMetaData();
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
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
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
        validateIndexes(Arrays.asList(indexMyA, indexMyB, indexOtherA, indexOtherB));
    }

    // uncomment to compare
    // @Test
    public void benchMarkMultiTarget() {
        // compare single target build to multi target index building
        final FDBStoreTimer singleTimer = new FDBStoreTimer();
        final FDBStoreTimer multiTimer = new FDBStoreTimer();
        final int numRecords = 5555;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        long startSingle = System.currentTimeMillis();
        for (Index index : indexes) {
            openSimpleMetaData(hook);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                    .setTimer(singleTimer)
                    .setIndex(index)
                    .build()) {
                indexBuilder.buildIndex(true);
            }
        }
        long endSingle = System.currentTimeMillis();
        assertTrue(endSingle > startSingle); // suppress warnings
        disableAll(indexes);

        long startMulti =  System.currentTimeMillis();
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(multiTimer)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        long endMulti =  System.currentTimeMillis();
        assertTrue(endMulti > startMulti); // suppress warnings

        System.out.printf("%d indexes, %d records. Single build took %d milliSeconds, MultiIndex took %d%n",
                indexes.size(), numRecords, endSingle - startSingle, endMulti - startMulti);
    }
}
