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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.test.BooleanSource;
import com.google.common.collect.Comparators;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for building indexes from other indexes with {@link OnlineIndexer}.
 */
class OnlineIndexerIndexFromIndexTest extends OnlineIndexerTest {

    private void populateData(final long numRecords, final long numOtherRecords) {
        openSimpleMetaData();
        List<TestRecords1Proto.MySimpleRecord> simpleRecords = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .addAllRepeater(IntStream.range(0, (int)Math.max(numRecords, 100)).boxed().collect(Collectors.toList()))
                        .build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MyOtherRecord> otherRecords = LongStream.range(0, numOtherRecords).mapToObj(val ->
                TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(numRecords + val).setNumValue2((int) val).build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            simpleRecords.forEach(recordStore::saveRecord);
            otherRecords.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private FDBRecordStoreTestBase.RecordMetaDataHook myHook(Index srcIndex, Index tgtIndex) {
        return allIndexesHook(List.of(srcIndex, tgtIndex));
    }

    private void buildIndexAndCrashHalfway(Index tgtIndex, int chunkSize, int count, FDBStoreTimer timer, @Nullable OnlineIndexer.IndexingPolicy policy) {
        final AtomicLong counter = new AtomicLong(0);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(policy)
                .setLimit(chunkSize)
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
        assertEquals(count, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
    }

    @Test
    void testIndexFromIndexSimple() {

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @ParameterizedTest
    @BooleanSource
    void testIndexFromIndexContinuation(boolean reverseScan) {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 107;
        final int chunkSize  = 17;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setReverseScanOrder(reverseScan)
                        .build())
                .setLimit(chunkSize)
                .setTimer(timer)
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        scrubAndValidate(List.of(tgtIndex));
    }

    @ParameterizedTest
    @BooleanSource
    void testNonIdempotentIndexFromIndex(boolean reverseScan) {
        this.formatVersion = Comparators.min(FormatVersion.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE, this.formatVersion);
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 8;
        final long otherRecords = 4;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords, otherRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .setReverseScanOrder(reverseScan)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testCanBuildNonIdempotentIndexFromIndexOnNewStoreWithOldFormatVersionInIndexer() {
        this.formatVersion = Comparators.min(FormatVersion.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE, this.formatVersion);
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 8;
        final long otherRecords = 4;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords, otherRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        // Set a format version on the store that does not allow index-from-index builds
        // Because the store already has this format version, though it should be allowed
        this.formatVersion = FormatVersionTestUtils.previous(FormatVersion.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        this.formatVersion = Comparators.min(FormatVersion.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE, this.formatVersion);
        scrubAndValidate(List.of(tgtIndex));
    }

    @ParameterizedTest
    @BooleanSource
    void testNonIdempotentIndexFromIndexOldFormatFallback(boolean reverseScan) {
        // Attempt to build a non-idempotent index at an older format version. This should fall back to a full record scan
        this.formatVersion = FormatVersionTestUtils.previous(FormatVersion.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE);
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 6;
        final long otherRecords = 5;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords, otherRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .setReverseScanOrder(reverseScan)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords + otherRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testNonIdempotentIndexFromIndexOldFormatNoFallback() {
        // Attempt to build a non-idempotent index at old format version where this is not supported. This should
        // error as falling back to a record scan is not enabled
        this.formatVersion = FormatVersionTestUtils.previous(FormatVersion.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE);
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 7;
        final long otherRecords = 8;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords, otherRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            assertThrows(IndexingByIndex.ValidationException.class, indexBuilder::buildIndex);
        }
    }

    @Test
    void testIndexFromIndexNoFallbackNonReadable() {
        // Let srcIndex be a non-readable index
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 3;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = newIndexerBuilder(srcIndex)
                    .build()) {
                // change srcIndex back to writeOnly
                recordStore.markIndexWriteOnly(srcIndex).join();
                indexBuilder.rebuildIndex(recordStore);
                context.commit();
                assertFalse(recordStore.isIndexReadable(srcIndex));
            }
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            IndexingByIndex.ValidationException e = assertThrows(IndexingByIndex.ValidationException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("source index is not scannable"));
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @SuppressWarnings("try")
    @Test
    void testIndexFromIndexNoFallbackNonValueSrc() {
        // Let srcIndex be a non-VALUE index
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 3;

        Index srcIndex = new Index("src_index", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            IndexingByIndex.ValidationException e = assertThrows(IndexingByIndex.ValidationException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("source index is not a VALUE index"));

            try (FDBRecordContext context = openContext()) {
                e = assertThrows(IndexingBase.ValidationException.class, () -> indexBuilder.rebuildIndex(recordStore));
                assertTrue(e.getMessage().contains("source index is not a VALUE index"));
                context.commit();
            }
        }

        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @SuppressWarnings("try")
    @Test
    void testIndexFromIndexWithDuplicates() {
        this.formatVersion = Comparators.min(FormatVersion.CHECK_INDEX_BUILD_TYPE_DURING_UPDATE, this.formatVersion);
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 5;

        Index srcIndex = new Index("src_index", field("repeater", KeyExpression.FanType.FanOut));
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        openContext();
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            IndexingByIndex.ValidationException e = assertThrows(IndexingByIndex.ValidationException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("source index creates duplicates"));

            try (FDBRecordContext context = openContext()) {
                e = assertThrows(IndexingBase.ValidationException.class, () -> indexBuilder.rebuildIndex(recordStore));
                assertTrue(e.getMessage().contains("source index creates duplicates"));
                context.commit();
            }
        }

        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void testIndexFromIndexPersistentContinuation(int reverseSeed) {
        // start indexing by Index, verify continuation
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 49;
        final int chunkSize  = 12;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        // test all 4 reverse combinations
        boolean reverse1 = 0 != (reverseSeed & 1);
        boolean reverse2 = 0 != (reverseSeed & 2);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 1, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setReverseScanOrder(reverse1)
                        .build());

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setReverseScanOrder(reverse2)
                        .build())
                .setLimit(chunkSize)
                .build()) {
            // now continue building from the last successful range
            indexBuilder.buildIndex(true);
        }
        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexPersistentPreventBadContinuation() {
        // start indexing by index, verify refusal to continue by records, then continue by index
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 78;
        final int chunkSize  = 17;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 1, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build());

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfDisabled(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                )
                .build()) {

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setLimit(chunkSize)
                .build()) {
            // now continue building from the last successful range
            indexBuilder.buildIndex(true);
        }
        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexPersistentContinuePreviousByIndex() {
        // start indexing by Index, verify refusal to continue by records, then allow continuation of the previous by index method - overriding the by records request
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 98;
        final int chunkSize  = 16;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 3, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .setForbidRecordScan(true)
                        .build());

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE))
                .build()) {

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer).build()) {
            // now continue building, overriding the requested method with the previous one
            indexBuilder.buildIndex(true);
        }

        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexPersistentContinuePreviousByRecords() {
        // start indexing by records, allow continuation of previous by records method - overriding the by-index request
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 90;
        final int chunkSize  = 17;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 2, timer, null);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
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
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexPersistentContinuePreviousByRecordsWithoutTypeStamp() {
        // start indexing by records, earse the type stamp to simulate old code, verify refusal to continue, then allow continuation of previous by records method - overriding the by-index request
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 77;
        final int chunkSize  = 20;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 2, timer, null);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {
            // erase the previous type stamp - of the by-records
            indexBuilder.eraseIndexingTypeStampTestOnly().join();

            // now try building by records, a failure is expected
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
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
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexPersistentContinueRebuildWhenTypeStampChange() {
        // start indexing by records, request a rebuild - by index - if the indexing type stamp had changed
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 79;
        final int chunkSize  = 21;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 2, timer, null);

        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexRebuildIfWriteOnlyAndForceBuildAndBuildIfDisabled() {
        // test various policy options to ensure coverage (note that the last section will disable and rebuild the source index)
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 87;
        final int chunkSize  = 18;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 3, timer, null);

        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .setLimit(chunkSize)
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
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .setLimit(chunkSize)
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
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR))
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
        try (OnlineIndexer indexer = newIndexerBuilder(srcIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR))
                .build()) {
            indexer.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
    }

    @Test
    void testIndexFromIndexSrcVersionModifiedWithFallback() {
        // start indexing by index, change src index' last modified version, assert failing to continue, continue with REBUILD_IF.. option
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 81;
        final int chunkSize  = 10;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

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
            try (OnlineIndexer indexBuilder = newIndexerBuilder(srcIndex).build()) {
                recordStore.markIndexWriteOnly(srcIndex).join();
                srcIndex.setLastModifiedVersion(srcIndex.getLastModifiedVersion() + 1);
                indexBuilder.rebuildIndex(recordStore);
                recordStore.markIndexReadable(srcIndex).join();
                context.commit();
            }
        }

        // try index continuation, expect failure
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        // try index continuation, but allow rebuild
        openSimpleMetaData(hook);
        timer.reset();
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                        .build())
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex(true);
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexOtherSrcIndexWithFallback() {
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

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex2);

        // partly build by-index src_index
        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 7, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build());

        // try index continuation with src_index2, expect failure
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index2")
                        .forbidRecordScan()
                        .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                        .build())
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        // try indexing with src_index2, but allow continuation of previous method (src_index)
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index2")
                        .build()) // continue previous if policy changed
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex(true);
        }
        // total of records scan - all by src_index
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexOtherSrcIndexBecomesUnusable() {
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

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex2);

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
            try (OnlineIndexer indexBuilder = newIndexerBuilder(srcIndex).build()) {
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
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index2")
                        .build()) // rebuild after failing to continue prev
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex(true);
        }
        // total of records scan - all by src_index2
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexRebuild() {
        // test the inline rebuildIndex function by-index
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setSourceIndex("src_index")
                            .forbidRecordScan()
                            .build())
                    .build()) {
                indexBuilder.rebuildIndex(recordStore);
            }
            recordStore.markIndexReadable(tgtIndex).join();
            context.commit();
        }

        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertReadable(tgtIndex);
        scrubAndValidate(List.of(tgtIndex));
    }

    @Test
    void testIndexFromIndexRebuildReverseScanException() {
        // assert failure for reverse scan
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 80;

        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(srcIndex, tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setSourceIndex("src_index")
                            .forbidRecordScan()
                            .setReverseScanOrder(true)
                            .build())
                    .build()) {
                assertThrows(RecordCoreException.class, () -> indexBuilder.rebuildIndex(recordStore));
            }
            context.commit();
        }
    }

    @Test
    void testIndexFromIndexBlock() {
        // start indexing by Index, verify continuation
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 49;
        final int chunkSize  = 12;
        final int numChunks  = 1 + (numRecords / chunkSize);

        populateData(numRecords);

        Index sourceIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        openSimpleMetaData(metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", sourceIndex));
        buildIndexClean(sourceIndex);

        // Partly build index
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(sourceIndex, tgtIndex);
        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtIndex, chunkSize, 1, timer,
                OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build());

        openSimpleMetaData(hook);
        String luka = "Blocked by Luka: Feb 2023";
        try (OnlineIndexer indexer = newIndexerBuilder(tgtIndex).build()) {
            final Map<String, IndexBuildProto.IndexBuildIndexingStamp> stampMap =
                    indexer.blockIndexBuilds(luka, 10L);
            String indexName = tgtIndex.getName();
            final IndexBuildProto.IndexBuildIndexingStamp stamp = stampMap.get(indexName);
            assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_INDEX, stamp.getMethod());
            assertTrue(stamp.getBlock());
        }

        // query, ensure blocked
        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = newIndexerBuilder(tgtIndex).build()) {
            final Map<String, IndexBuildProto.IndexBuildIndexingStamp> stampMap =
                    indexer.queryIndexingStamps();
            String indexName = tgtIndex.getName();
            final IndexBuildProto.IndexBuildIndexingStamp stamp = stampMap.get(indexName);
            assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_INDEX, stamp.getMethod());
            assertTrue(stamp.getBlock());
            assertEquals(luka, stamp.getBlockID());
            assertTrue(stamp.getBlockExpireEpochMilliSeconds() > System.currentTimeMillis());
            assertTrue(stamp.getBlockExpireEpochMilliSeconds() < 20_000 + System.currentTimeMillis());
        }

        // ensure blocked
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setLimit(chunkSize)
                .build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built, and blocked"));
        }
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexWriteOnly(tgtIndex));
            context.commit();
        }

        // Successfully build
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .setAllowUnblock(true)
                        .build())
                .setLimit(chunkSize)
                .build()) {
            // now continue building from the last successful range
            indexBuilder.buildIndex();
        }

        // counters should demonstrate a continuation to completion
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(numChunks , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertReadable(tgtIndex);
        scrubAndValidate(List.of(tgtIndex));
    }
}
