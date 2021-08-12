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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building indexes from other indexes with {@link OnlineIndexer}.
 */
public class OnlineIndexerIndexFromIndexTest extends OnlineIndexerTest {

    private void populateData(final long numRecords) {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private FDBRecordStoreTestBase.RecordMetaDataHook myHook(Index srcIndex, Index tgtIndex) {
        return metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", srcIndex);
            metaDataBuilder.addIndex("MySimpleRecord", tgtIndex);
        } ;
    }

    private void buildSrcIndex(Index srcIndex) {
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(srcIndex).setSubspace(subspace)
                .build()) {
            indexer.buildIndex(true);
        }
        try (FDBRecordContext context = openContext()) {
            recordStore.vacuumReadableIndexesBuildData();
            context.commit();
        }
    }

    private void buildIndexAndCrashHalfway(Index tgtIndex, int chunkSize, int count, FDBStoreTimer timer, @Nullable OnlineIndexer.IndexingPolicy policy) {
        final AtomicLong counter = new AtomicLong(0);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(policy)
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
        final int expected = policy == null ? count + 1 : count; // by-records performs an extra range while building endpoints
        assertEquals(expected , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testIndexFromIndexSimple() {

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setTimer(timer)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexContinuation() {

        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testIndexFromIndexFallback() {
        // Let target index be a non-idempotent index

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 6;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .build())
                .setTimer(timer)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexNoFallback() {
        // Let target index be a non-idempotent index

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 7;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setTimer(timer)
                .build()) {

            assertThrows(IndexingByIndex.ValidationException.class, indexBuilder::buildIndex);
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexNoFallbackNonReadable() {
        // Let srcIndex be a non-readable index

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 3;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setIndex(srcIndex).setSubspace(subspace)
                    .build()) {
                // change srcIndex back to writeOnly
                recordStore.markIndexWriteOnly(srcIndex).join();
                indexBuilder.rebuildIndex(recordStore);
                context.commit();
                assertFalse(recordStore.isIndexReadable(srcIndex));
            }
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setTimer(timer)
                .build()) {

            IndexingByIndex.ValidationException e = assertThrows(IndexingByIndex.ValidationException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("source index is not readable"));
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexNoFallbackNonValueSrc() {
        // Let srcIndex be a non-VALUE index
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 3;

        Index srcIndex = new Index("src_index", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setTimer(timer)
                .build()) {

            IndexingByIndex.ValidationException e = assertThrows(IndexingByIndex.ValidationException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("source index is not a VALUE index"));
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexPersistentContinuation() {
        // start indexing by Index, verify continuation
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 49;
        final int chunkSize  = 12;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 1, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                .setSourceIndex("src_index")
                .forbidRecordScan()
                .build());

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {
            // now continue building from the last successful range
            indexBuilder.buildIndex(true);
        }
        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testIndexFromIndexPersistentPreventBadContinuation() {
        // start indexing by index, verify refusal to continue by records, then continue by index
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 78;
        final int chunkSize  = 17;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 1, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build());

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfDisabled(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                )
                .setTimer(timer)
                .build()) {

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {
            // now continue building from the last successful range
            indexBuilder.buildIndex(true);
        }
        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testIndexFromIndexPersistentContinuePreviousByIndex() {
        // start indexing by Index, verify refusal to continue by records, then allow continuation of the previous by index method - overriding the by records request
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 98;
        final int chunkSize  = 16;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 3, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .setForbidRecordScan(true)
                        .build());

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE))
                .setTimer(timer)
                .build()) {

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setTimer(timer)
                .build()) {

            // now continue building, overriding the requested method with the previous one
            indexBuilder.buildIndex(true);
        }

        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexPersistentContinuePreviousByRecords() {
        // start indexing by records, allow continuation of previous by records method - overriding the by-index request
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 90;
        final int chunkSize  = 17;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 2, timer, null);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .setTimer(timer)
                .build()) {

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            // now continue building, overriding the requested method with the previous one
            indexBuilder.buildIndex(true);
        }

        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexPersistentContinuePreviousByRecordsWithoutTypeStamp() {
        // start indexing by records, earse the type stamp to simulate old code, verify refusal to continue, then allow continuation of previous by records method - overriding the by-index request
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 77;
        final int chunkSize  = 20;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 2, timer, null);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .setTimer(timer)
                .build()) {
            // erase the previous type stamp - of the by-records
            indexBuilder.eraseIndexingTypeStampTestOnly().join();

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            // now continue building, overriding the requested method with the previous one
            indexBuilder.buildIndex(true);
        }

        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @Test
    public void testIndexFromIndexPersistentContinueRebuildWhenTypeStampChange() {
        // start indexing by records, request a rebuild - by index - if the indexing type stamp had changed
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 79;
        final int chunkSize  = 21;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 2, timer, null);

        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .setTimer(timer)
                .build()) {

            indexBuilder.buildIndex(true);
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }


    @Test
    public void testIndexFromIndexRebuildIfWriteOnlyAndForceBuildAndBuildIfDisabled() {
        // test various policy options to ensure coverage (note that the last section will disable and rebuild the source index)
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 87;
        final int chunkSize  = 18;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 3, timer, null);

        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {
            // now continue building from the last successful range
            indexBuilder.buildIndex(true);
        }
        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));

        // now check FORCE_BUILD
        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {
            // now force building (but leave it write_only)
            indexBuilder.buildIndex(false);
        }
        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));

        // now check BUILD_IF_DISABLED when WRITE_ONLY (from previous test)
        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR))
                .setTimer(timer)
                .build()) {

            // now try building if disabled, nothing should be happening
            RecordCoreException e = assertThrows(IndexingBase.ValidationException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("Index state is not as expected"));
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // to test BUILD_IF_DISABLED when disabled - disable the src and build again
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexDisabled(srcIndex).join();
            context.commit();
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(srcIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR))
                .setTimer(timer)
                .build()) {
            indexer.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
    }

    @Test
    public void testIndexFromIndexSrcVersionModifiedWithFallback() {
        // start indexing by index, change src index' last modified version, assert failing to continue, continue with REBUILD_IF.. option
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 81;
        final int chunkSize  = 10;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        // partly build by-index
        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 4, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build());

        // update src index last modified version (and its subspace key)
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setIndex(srcIndex).setSubspace(subspace)
                    .build()) {
                recordStore.markIndexWriteOnly(srcIndex).join();
                srcIndex.setLastModifiedVersion(srcIndex.getLastModifiedVersion() + 1);
                indexBuilder.rebuildIndex(recordStore);
                recordStore.markIndexReadable(srcIndex).join();
                context.commit();
            }
        }

        // try index continuation, expect failure
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setTimer(timer)
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        // try index continuation, but allow rebuild
        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(true);
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testIndexFromIndexOtherSrcIndexWithFallback() {
        // start indexing by src_index, attempt continue with src_index2
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 83;
        final int chunkSize  = 10;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index srcIndex2 = new Index("src_index2", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook =
                metaDataBuilder -> {
                    metaDataBuilder.addIndex("MySimpleRecord", srcIndex);
                    metaDataBuilder.addIndex("MySimpleRecord", srcIndex2);
                    metaDataBuilder.addIndex("MySimpleRecord", tgtIndex);
                } ;

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex2);

        // partly build by-index src_index
        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 7, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build());

        // try index continuation with src_index2, expect failure
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index2")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .setTimer(timer)
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        // try indexing with src_index2, but allow continuation of previous method (src_index)
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index2")
                        .build()) // continue previous if policy changed
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(true);
        }
        // total of records scan - all by src_index
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testIndexFromIndexOtherSrcIndexBecomesUnusable() {
        // start indexing by src_index, attempt continue with src_index2
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 88;
        final int chunkSize  = 15;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index srcIndex2 = new Index("src_index2", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook =
                metaDataBuilder -> {
                    metaDataBuilder.addIndex("MySimpleRecord", srcIndex);
                    metaDataBuilder.addIndex("MySimpleRecord", srcIndex2);
                    metaDataBuilder.addIndex("MySimpleRecord", tgtIndex);
                } ;

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex2);

        // partly build by-index src_index
        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 3, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build());

        // make 'prev' source unreadable
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setIndex(srcIndex).setSubspace(subspace)
                    .build()) {
                // change src_index back to writeOnly
                recordStore.markIndexWriteOnly(srcIndex).join();
                indexBuilder.rebuildIndex(recordStore);
                context.commit();
                assertFalse(recordStore.isIndexReadable(srcIndex));
            }
        }

        // try indexing with src_index2. Since src_index isn't usable, it should rebuild
        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index2")
                        .build()) // rebuild after failing to continue prev
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(true);
        }
        // total of records scan - all by src_index2
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    public void testIndexFromIndexRebuild() {
        // test the inline rebuildIndex function by-index
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildSrcIndex(srcIndex);

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setSourceIndex("src_index")
                            .forbidRecordScan()
                            .build())
                    .setTimer(timer)
                    .build()) {

                indexBuilder.rebuildIndex(recordStore);
                context.commit();
            }
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }
}
