/*
 * OnlineIndexerIndexFromIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for scrubbing readable indexes with {@link OnlineIndexer}.
 */
@SuppressWarnings("removal")
class OnlineIndexScrubberTest extends OnlineIndexerTest {

    private FDBRecordStoreTestBase.RecordMetaDataHook myHook(Index index) {
        return allIndexesHook(List.of(index));
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleMissing(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 50;
        long res;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, res);

        // manually delete a few index entries
        openSimpleMetaData(hook);
        int missingCount = 0;
        try (FDBRecordContext context = openContext()) {
            List<IndexEntry> indexEntries = recordStore.scanIndex(tgtIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final IndexEntry indexEntry = indexEntries.get(i);
                final Tuple valueKey = indexEntry.getKey();
                final byte[] keyBytes = recordStore.indexSubspace(tgtIndex).pack(valueKey);
                recordStore.getContext().ensureActive().clear(keyBytes);
                missingCount ++;
            }
            context.commit();
        }

        // verify the missing entries are found and fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(missingCount, res);

        // now verify it's fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, res);
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleDangling(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        long numRecords = 51;
        long res;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, res);

        // manually delete a few records w/o updating the indexes
        openSimpleMetaData(hook);
        int danglingCount = 0;
        try (FDBRecordContext context = openContext(false)) {
            List<FDBIndexedRecord<Message>> indexRecordEntries = recordStore.scanIndexRecords(tgtIndex.getName(), IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final FDBIndexedRecord<Message> indexRecord = indexRecordEntries.get(i);
                final FDBStoredRecord<Message> rec = indexRecord.getStoredRecord();
                final Subspace subspace = recordStore.recordsSubspace().subspace(rec.getPrimaryKey());
                recordStore.getContext().ensureActive().clear(subspace.range());
                danglingCount ++;
            }
            context.commit();
        }

        // verify the missing entries are found (report only, no repair)
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(false))
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(danglingCount, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(danglingCount, res);

        // verify the missing entries are found and fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(danglingCount, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(danglingCount, res);

        numRecords -= danglingCount; // if the dangling indexes were removed, this should be reflected later

        // now verify it's fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, res);
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberLimits(boolean legacy) {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 52;
        final int chunkSize = 7;
        final int numChunks = 1 + (numRecords / chunkSize);
        long resDangling;
        long resMissing;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        // Scrub both dangling & missing. Scan counts in this test should be doubles.
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                // user default ScrubbingPolicy
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(false)
                        .setEntriesScanLimit(1000000)
                        .useLegacyScrubber(legacy)
                        .build())
                .setLimit(chunkSize)
                .build()) {
            resDangling = indexScrubber.scrubDanglingIndexEntries();
            resMissing = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numChunks * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, resDangling);
        assertEquals(0, resMissing);


        // Scrub dangling with a quota
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(1))
                .setLimit(chunkSize)
                .build()) {
            resDangling = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(1, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertEquals(chunkSize, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, resDangling);

        // Scrub both with a quota
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(chunkSize * 3))
                .setLimit(chunkSize)
                .build()) {
            resDangling = indexScrubber.scrubDanglingIndexEntries();
            resMissing = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(3 * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertEquals(chunkSize * 3 * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, resDangling);
        assertEquals(0, resMissing);
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberLimitsAlternatingLegacy(boolean legacyInit) {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 50;
        final int chunkSize = 10;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);
        ScrubbersMissingRanges lastRanges = getScrubbersMissingRange(tgtIndex);

        boolean legacy = !legacyInit;
        for (int i = 0; i < 5; i++) {
            // Scrub both dangling & missing. Scan counts in this test should be doubles.
            legacy = !legacy;
            long resDangling;
            long resMissing;
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                    // user default ScrubbingPolicy
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setAllowRepair(false)
                            .setEntriesScanLimit(chunkSize)
                            .useLegacyScrubber(legacy)
                            .build())
                    .setLimit(chunkSize)
                    .build()) {
                resDangling = indexScrubber.scrubDanglingIndexEntries();
                resMissing = indexScrubber.scrubMissingIndexEntries();
            }
            assertEquals(0, resDangling);
            assertEquals(0, resMissing);

            final ScrubbersMissingRanges ranges = getScrubbersMissingRange(tgtIndex);
            assertNotEquals(lastRanges.indexes, ranges.indexes);
            assertNotEquals(lastRanges.records, ranges.records);
            lastRanges = ranges;
        }
        assertEquals(numRecords * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertTrue(lastRanges.indexes == null || RangeSet.isFirstKey(lastRanges.indexes.begin));
        assertTrue(lastRanges.records == null || RangeSet.isFirstKey(lastRanges.records.begin));
    }

    private static class ScrubbersMissingRanges {
        final Range indexes;
        final Range records;

        public ScrubbersMissingRanges(final Range indexes, final Range records) {
            this.indexes = indexes;
            this.records = records;
        }
    }

    private ScrubbersMissingRanges getScrubbersMissingRange(Index index) {
        return getScrubbersMissingRange(index, 0);
    }

    private ScrubbersMissingRanges getScrubbersMissingRange(Index index, int rangeId) {
        try (FDBRecordContext context = openContext()) {
            Range indexes = IndexingRangeSet.forScrubbingIndex(recordStore, index, rangeId).firstMissingRangeAsync().thenApply(Function.identity()).join();
            Range records = IndexingRangeSet.forScrubbingRecords(recordStore, index, rangeId).firstMissingRangeAsync().thenApply(Function.identity()).join();
            context.commit();
            return new ScrubbersMissingRanges(indexes, records);
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberInvalidIndexState(boolean legacy) {
        final int numRecords = 20;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        // refuse to scrub a non-readable index
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .useLegacyScrubber(legacy))
                    .build()) {
                recordStore.markIndexWriteOnly(tgtIndex).join();
                context.commit();
                assertThrows(IndexingBase.ValidationException.class, indexScrubber::scrubDanglingIndexEntries);
                assertThrows(IndexingBase.ValidationException.class, indexScrubber::scrubMissingIndexEntries);
            }
        }

        // refuse to scrub a non-value index
        Index nonValueIndex = new Index("count_index", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        FDBRecordStoreTestBase.RecordMetaDataHook hook2 = myHook(nonValueIndex);

        openSimpleMetaData(hook2);
        buildIndexClean(nonValueIndex);
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(nonValueIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy))
                .build()) {
            if (legacy) {
                assertThrows(IndexingBase.ValidationException.class, indexScrubber::scrubDanglingIndexEntries);
                assertThrows(IndexingBase.ValidationException.class, indexScrubber::scrubMissingIndexEntries);
            } else {
                assertThrows(UnsupportedOperationException.class, indexScrubber::scrubDanglingIndexEntries);
                assertThrows(UnsupportedOperationException.class, indexScrubber::scrubMissingIndexEntries);
            }
        }
    }

    @Test
    void testScrubberNonValueIndex() {
        final FDBStoreTimer timer = new FDBStoreTimer();
        long numRecords = 9;

        final Index tgtIndex = new Index("myVersionIndex", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .useLegacyScrubber(true) // only supported within legacy scrubbing code
                        .ignoreIndexTypeCheck() // required to allow non-value index scrubbing
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubUnnestedIndex(boolean legacy) {
        final Index targetIndex = new Index("unnestedIndex", concat(field("entry").nest("key"), field("parent").nest("other_id"), field("entry").nest("int_value")));
        final OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler recordHandler = OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler.instance();
        FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.baseHook(true, null)
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OnlineIndexerBuildUnnestedIndexTest.UNNESTED, targetIndex));
        openMetaData(recordHandler.getFileDescriptor(), hook);

        final long numRecords = 20;
        populateNestedMapData(numRecords);

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            if (legacy) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();

                // Scanned 5 * numRecords. This is from:
                //  1. When looking for missing entries, we scan every record in the database. There are numRecords of type
                //     OuterRecord and numRecords of type OtherRecord (so that's 2 * numRecords)
                //  2. When scanning dangling entries, we look up one for every entry in the index. There are 3 map entries
                //     for each of type OuterRecord, so that's 3 * numRecords.
                assertEquals(numRecords * 5, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            } else {
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubDanglingIndexEntries());
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubMissingIndexEntries());
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testDetectDanglingUnnestedIndex(boolean legacy) {
        final Index targetIndex = new Index("unnestedIndex", concat(field("entry").nest("key"), field("parent").nest("other_id"), field("entry").nest("int_value")));
        final OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler recordHandler = OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler.instance();
        FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.baseHook(true, null)
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OnlineIndexerBuildUnnestedIndexTest.UNNESTED, targetIndex));
        openMetaData(recordHandler.getFileDescriptor(), hook);

        final long numRecords = 30;
        final List<Message> data = populateNestedMapData(numRecords);
        int doDelete = 0;
        try (FDBRecordContext context = openContext()) {
            for (Message datum : data) {
                if (!(datum instanceof TestRecordsNestedMapProto.OuterRecord)) {
                    continue;
                }
                // Clear out the record data for 1/3 records
                if (doDelete == 0) {
                    Tuple primaryKey = recordHandler.getPrimaryKey(datum);
                    context.ensureActive().clear(recordStore.recordsSubspace().subspace(primaryKey).range());
                }
                doDelete = (doDelete + 1) % 3;
            }
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            if (legacy) {
                assertThrows(RecordDoesNotExistException.class, indexScrubber::scrubDanglingIndexEntries);
            } else {
                // numrecord/3 is the top level records deleted, each with 3 sub records
                assertEquals(numRecords, indexScrubber.scrubDanglingIndexEntries());
                indexScrubber.scrubMissingIndexEntries();
                assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testDetectMissingUnnestedIndex(boolean legacy) {
        final Index targetIndex = new Index("unnestedIndex", concat(field("entry").nest("key"), field("parent").nest("other_id"), field("entry").nest("int_value")));
        final OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler recordHandler = OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler.instance();
        FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.baseHook(true, null)
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OnlineIndexerBuildUnnestedIndexTest.UNNESTED, targetIndex));
        openMetaData(recordHandler.getFileDescriptor(), hook);

        final long numRecords = 30;
        final List<Message> data = populateNestedMapData(numRecords);
        int doDelete = 0;
        int deleteCount = 0;
        try (FDBRecordContext context = openContext()) {
            for (Message datum : data) {
                if (!(datum instanceof TestRecordsNestedMapProto.OuterRecord)) {
                    continue;
                }
                if (doDelete == 0) {
                    // Clear out one of the entries for this record
                    TestRecordsNestedMapProto.OuterRecord outerRecord = (TestRecordsNestedMapProto.OuterRecord)datum;
                    if (!outerRecord.getMap().getEntryList().isEmpty()) {
                        int entryToDeleteIndex = deleteCount % outerRecord.getMap().getEntryCount();
                        TestRecordsNestedMapProto.MapRecord.Entry entryToDelete = outerRecord.getMap().getEntry(entryToDeleteIndex);
                        final Tuple indexKey = Tuple.from(entryToDelete.getKey(), outerRecord.getOtherId(), entryToDelete.getIntValue(),
                                metaData.getSyntheticRecordType(OnlineIndexerBuildUnnestedIndexTest.UNNESTED).getRecordTypeKey(), Tuple.from(outerRecord.getRecId()), Tuple.from(entryToDeleteIndex));
                        context.ensureActive().clear(recordStore.indexSubspace(targetIndex).pack(indexKey));
                        deleteCount++;
                    }
                }
                doDelete = (doDelete + 1) % 3;
            }
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        // numRecords/3 records have been deleted. Each one created 3 entries, so the total number of
        // dangling entries should equal numRecords
        assertEquals(numRecords / 3, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

        if (legacy) {
            timer.reset();
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setLogWarningsLimit(Integer.MAX_VALUE)
                            .setAllowRepair(true)
                            .useLegacyScrubber(legacy)
                            .build())
                    .build()) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();
            }

            assertEquals(numRecords / 3, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            assertEquals(numRecords / 3, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

            timer.reset();
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setLogWarningsLimit(Integer.MAX_VALUE)
                            .setAllowRepair(true)
                            .useLegacyScrubber(legacy)
                            .build())
                    .build()) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();
            }

            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        } else {
            // Repair is not supported for synthetic records
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setLogWarningsLimit(Integer.MAX_VALUE)
                            .setAllowRepair(true)
                            .useLegacyScrubber(legacy)
                            .build())
                    .build()) {
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubDanglingIndexEntries());
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubMissingIndexEntries());
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubJoinedIndex(boolean legacy) {
        final Index targetIndex = new Index("joinedIndex", concat(field("simple").nest("num_value"), field("other").nest("num_value_3"), field("simple").nest("num_value_2")));
        final OnlineIndexerBuildJoinedIndexTest.OnlineIndexerJoinedRecordHandler recordHandler = OnlineIndexerBuildJoinedIndexTest.OnlineIndexerJoinedRecordHandler.instance();
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.baseHook(true, null)
                .andThen(recordHandler.addIndexHook(targetIndex));
        openMetaData(recordHandler.getFileDescriptor(), hook);

        final long numRecords = 30;
        populateJoinedData(numRecords);

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            if (legacy) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();
                // Scanned 3 * numRecords. This is from:
                //  1. When looking for missing entries, we scan every record in the database. There are numRecords of type
                //     MySimpleRecord and numRecords of type MyOtherRecord (so that's 2 * numRecords)
                //  2. When scanning dangling entries, we look up one for every entry in the index. There are is one entry
                //     for each pair of simple and other records, so that's another numRecords scanned
                assertEquals(numRecords * 3, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            } else {
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubDanglingIndexEntries());
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubMissingIndexEntries());
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testDetectDanglingFromJoinedIndex(boolean legacy) {
        final Index targetIndex = new Index("joinedIndex", concat(field("simple").nest("num_value"), field("other").nest("num_value_3"), field("simple").nest("num_value_2")));
        final OnlineIndexerBuildJoinedIndexTest.OnlineIndexerJoinedRecordHandler recordHandler = OnlineIndexerBuildJoinedIndexTest.OnlineIndexerJoinedRecordHandler.instance();
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.baseHook(true, null)
                .andThen(recordHandler.addIndexHook(targetIndex));
        openMetaData(recordHandler.getFileDescriptor(), hook);

        final long numRecords = 100;
        final List<Message> data = populateJoinedData(numRecords);
        try (FDBRecordContext context = openContext()) {
            int doDelete = 0;
            for (Message datum : data) {
                if (!(datum instanceof TestRecordsJoinIndexProto.MySimpleRecord)) {
                    continue;
                }
                if (doDelete == 0) {
                    TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = (TestRecordsJoinIndexProto.MySimpleRecord)datum;
                    TestRecordsJoinIndexProto.MyOtherRecord otherRecord = recordStore.loadRecordAsync(Tuple.from(simpleRecord.getOtherRecNo()))
                            .thenApply(storedRec -> TestRecordsJoinIndexProto.MyOtherRecord.newBuilder().mergeFrom(storedRec.getRecord()).build())
                            .join();

                    // Clear out both records that are a part of this join pair
                    context.ensureActive().clear(recordStore.recordsSubspace().subspace(recordHandler.getPrimaryKey(simpleRecord)).range());
                    context.ensureActive().clear(recordStore.recordsSubspace().subspace(recordHandler.getPrimaryKey(otherRecord)).range());
                }
                doDelete = (doDelete + 1) % 4;
            }
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            if (legacy) {
                assertThrows(RecordDoesNotExistException.class, indexScrubber::scrubDanglingIndexEntries);
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
            } else {
                assertEquals(numRecords / 4, indexScrubber.scrubDanglingIndexEntries());
                assertEquals(numRecords / 4, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
            }

            indexScrubber.scrubMissingIndexEntries();
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testDetectMissingFromJoinedIndex(boolean legacy) {
        final Index targetIndex = new Index("joinedIndex", concat(field("simple").nest("num_value"), field("other").nest("num_value_3"), field("simple").nest("num_value_2")));
        final OnlineIndexerBuildJoinedIndexTest.OnlineIndexerJoinedRecordHandler recordHandler = OnlineIndexerBuildJoinedIndexTest.OnlineIndexerJoinedRecordHandler.instance();
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.baseHook(true, null)
                .andThen(recordHandler.addIndexHook(targetIndex));
        openMetaData(recordHandler.getFileDescriptor(), hook);

        final long numRecords = 100;
        final List<Message> data = populateJoinedData(numRecords);
        try (FDBRecordContext context = openContext()) {
            int doDelete = 0;
            for (Message datum : data) {
                if (!(datum instanceof TestRecordsJoinIndexProto.MySimpleRecord)) {
                    continue;
                }
                if (doDelete == 0) {
                    TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = (TestRecordsJoinIndexProto.MySimpleRecord)datum;
                    TestRecordsJoinIndexProto.MyOtherRecord otherRecord = recordStore.loadRecordAsync(Tuple.from(simpleRecord.getOtherRecNo()))
                            .thenApply(storedRec -> TestRecordsJoinIndexProto.MyOtherRecord.newBuilder().mergeFrom(storedRec.getRecord()).build())
                            .join();
                    // Clear out the index key for this join pair
                    final Tuple indexKey = Tuple.from(simpleRecord.getNumValue(), otherRecord.getNumValue3(), simpleRecord.getNumValue2(),
                            metaData.getSyntheticRecordType("SimpleOtherJoin").getRecordTypeKey(), recordHandler.getPrimaryKey(simpleRecord), recordHandler.getPrimaryKey(otherRecord));
                    context.ensureActive().clear(recordStore.indexSubspace(targetIndex).pack(indexKey));
                }
                doDelete = (doDelete + 1) % 4;
            }
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        assertEquals(numRecords / 4, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

        if (legacy) {
            timer.reset();
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setLogWarningsLimit(Integer.MAX_VALUE)
                            .setAllowRepair(true)
                            .useLegacyScrubber(legacy)
                            .build())
                    .build()) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();
            }

            assertEquals(numRecords / 4, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            assertEquals(numRecords / 4, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

            timer.reset();
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setLogWarningsLimit(Integer.MAX_VALUE)
                            .setAllowRepair(true)
                            .useLegacyScrubber(legacy)
                            .build())
                    .build()) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();
            }

            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        } else {
            // Repair is not supported for synthetic records
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setLogWarningsLimit(Integer.MAX_VALUE)
                            .setAllowRepair(true)
                            .useLegacyScrubber(legacy)
                            .build())
                    .build()) {
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubDanglingIndexEntries());
                assertThrows(UnsupportedOperationException.class, () -> indexScrubber.scrubMissingIndexEntries());
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleMissingMixedRanges(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 50;
        long res;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .setRangeId(10)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, res);

        // manually delete a few index entries
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            List<IndexEntry> indexEntries = recordStore.scanIndex(tgtIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final IndexEntry indexEntry = indexEntries.get(i);
                final Tuple valueKey = indexEntry.getKey();
                final byte[] keyBytes = recordStore.indexSubspace(tgtIndex).pack(valueKey);
                recordStore.getContext().ensureActive().clear(keyBytes);
            }
            context.commit();
        }

        // Run/fix few of the records on range-id 0, make sure it is a partial scan
        timer.reset();
        ScrubbersMissingRanges lastRanges = getScrubbersMissingRange(tgtIndex);
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .build())
                .setLimit(10)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex);
        assertFalse(Arrays.equals(midRange.records.begin, lastRanges.records.begin));

        // Run/Detect all range-id 2. Make sure it scans the all range
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setRangeId(2)
                        .setAllowRepair(false)
                        .build())
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Make sure range-id 0 was not affected
        final ScrubbersMissingRanges range = getScrubbersMissingRange(tgtIndex);
        assertArrayEquals(range.records.begin, midRange.records.begin);

        // finish fixing in range-id 0
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(true)
                        .build())
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }

        // now verify it's fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, res);
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleMissingMixedRangesInterleaved(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 50;
        long res;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .setRangeId(10)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, res);

        // manually delete a few index entries
        openSimpleMetaData(hook);
        int missingCount = 0;
        try (FDBRecordContext context = openContext()) {
            List<IndexEntry> indexEntries = recordStore.scanIndex(tgtIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final IndexEntry indexEntry = indexEntries.get(i);
                final Tuple valueKey = indexEntry.getKey();
                final byte[] keyBytes = recordStore.indexSubspace(tgtIndex).pack(valueKey);
                recordStore.getContext().ensureActive().clear(keyBytes);
                missingCount ++;
            }
            context.commit();
        }

        // Run/fix few of the records on range-id 0, make sure it is a partial scan, preserve timer
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .build())
                .setLimit(10)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Run/Detect few records in range-id 1, no timer
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setRangeId(1)
                        .setAllowRepair(false)
                        .setEntriesScanLimit(21)
                        .build())
                .setLimit(20)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex);
        assertFalse(midRange.records.begin == null || RangeSet.isFinalKey(midRange.records.begin));

        // finish fixing in range-id 0, with preserved timer. Check timer's values
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(true)
                        .build())
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));


        // now verify it's fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, res);
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleMissingMixedRangesWithReset(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 32;
        long res;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);
        populateData(numRecords);
        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        // manually delete a few index entries
        openSimpleMetaData(hook);
        int missingCount = 0;
        try (FDBRecordContext context = openContext()) {
            List<IndexEntry> indexEntries = recordStore.scanIndex(tgtIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final IndexEntry indexEntry = indexEntries.get(i);
                final Tuple valueKey = indexEntry.getKey();
                final byte[] keyBytes = recordStore.indexSubspace(tgtIndex).pack(valueKey);
                recordStore.getContext().ensureActive().clear(keyBytes);
                missingCount ++;
            }
            context.commit();
        }

        // Run/Detect few of the records on range-id 2, make sure it is a partial scan
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(20)
                        .setAllowRepair(false)
                        .setRangeId(2)
                        .build())
                .setLimit(7)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertTrue(numRecords > timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final byte[] begin = getScrubbersMissingRange(tgtIndex).records.begin;
        assertFalse(begin == null || RangeSet.isFinalKey(begin));

        // Run/Detect all range-id 2, with a reset, assert full scan
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(false)
                        .setRangeId(2)
                        .setRangeReset(true)
                        .build())
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleDanglingMixedRanges(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        long numRecords = 53;
        long res;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);
        populateData(numRecords);
        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .setRangeId(10)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, res);

        // manually delete a few records w/o updating the indexes
        openSimpleMetaData(hook);
        int danglingCount = 0;
        try (FDBRecordContext context = openContext(false)) {
            List<FDBIndexedRecord<Message>> indexRecordEntries = recordStore.scanIndexRecords(tgtIndex.getName(), IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final FDBIndexedRecord<Message> indexRecord = indexRecordEntries.get(i);
                final FDBStoredRecord<Message> rec = indexRecord.getStoredRecord();
                final Subspace subspace = recordStore.recordsSubspace().subspace(rec.getPrimaryKey());
                recordStore.getContext().ensureActive().clear(subspace.range());
                danglingCount ++;
            }
            context.commit();
        }
        numRecords -= danglingCount; // if the dangling indexes were removed, this should be reflected later


        // Run/fix few of the missing entries on range-id 0, make sure it is a partial scan
        timer.reset();
        ScrubbersMissingRanges lastRanges = getScrubbersMissingRange(tgtIndex);
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .build())
                .setLimit(10)
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex);
        assertFalse(Arrays.equals(midRange.indexes.begin, lastRanges.indexes.begin));

        // Run/Detect all range-id 2. Make sure it scans the all range
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setRangeId(2)
                        .setAllowRepair(false)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Make sure range-id 0 was not affected
        final ScrubbersMissingRanges range = getScrubbersMissingRange(tgtIndex);
        assertArrayEquals(range.indexes.begin, midRange.indexes.begin);

        // finish fixing in range-id 0
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(true)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }

        // now verify it's fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, res);
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleDangingMixedRangesInterleaved(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        long numRecords = 55;
        long res;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);
        populateData(numRecords);
        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .setRangeId(10)
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, res);

        // manually delete a few records w/o updating the indexes
        openSimpleMetaData(hook);
        int danglingCount = 0;
        try (FDBRecordContext context = openContext(false)) {
            List<FDBIndexedRecord<Message>> indexRecordEntries = recordStore.scanIndexRecords(tgtIndex.getName(), IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final FDBIndexedRecord<Message> indexRecord = indexRecordEntries.get(i);
                final FDBStoredRecord<Message> rec = indexRecord.getStoredRecord();
                final Subspace subspace = recordStore.recordsSubspace().subspace(rec.getPrimaryKey());
                recordStore.getContext().ensureActive().clear(subspace.range());
                danglingCount ++;
            }
            context.commit();
        }

        // Run/fix few of the records on range-id 0, make sure it is a partial scan, preserve timer
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .build())
                .setLimit(10)
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Run/Detect few records in range-id 1, no timer
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setRangeId(1)
                        .setAllowRepair(false)
                        .setEntriesScanLimit(21)
                        .build())
                .setLimit(20)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex);
        assertFalse(midRange.indexes.begin == null || RangeSet.isFinalKey(midRange.indexes.begin));

        // finish fixing in range-id 0, with preserved timer. Check timer's values
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(true)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(danglingCount, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

        numRecords -= danglingCount; // if the dangling indexes were removed, this should be reflected later

        // now verify it's fixed
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, res);
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberSimpleDanglingMixedRangesWithReset(boolean legacy) throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        long numRecords = 32;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);
        populateData(numRecords);
        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        // manually delete a few records w/o updating the indexes
        openSimpleMetaData(hook);
        int danglingCount = 0;
        try (FDBRecordContext context = openContext(false)) {
            List<FDBIndexedRecord<Message>> indexRecordEntries = recordStore.scanIndexRecords(tgtIndex.getName(), IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            for (int i = 3; i < numRecords; i *= 2) {
                final FDBIndexedRecord<Message> indexRecord = indexRecordEntries.get(i);
                final FDBStoredRecord<Message> rec = indexRecord.getStoredRecord();
                final Subspace subspace = recordStore.recordsSubspace().subspace(rec.getPrimaryKey());
                recordStore.getContext().ensureActive().clear(subspace.range());
                danglingCount ++;
            }
            context.commit();
        }

        // Run/Detect few of the index entries on range-id 2, make sure it is a partial scan
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setEntriesScanLimit(20)
                        .setAllowRepair(false)
                        .setRangeId(2)
                        .build())
                .setLimit(7)
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertTrue(numRecords > timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final byte[] begin = getScrubbersMissingRange(tgtIndex).indexes.begin;
        assertFalse(begin == null || RangeSet.isFinalKey(begin));

        // Run/Detect all range-id 2, with a reset, assert full scan
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(false)
                        .setRangeId(2)
                        .setRangeReset(true)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
    }

    @ParameterizedTest
    @BooleanSource
    void testScrubberEraseIndexScrubbingData(boolean legacy) {
        final FDBStoreTimer timer = new FDBStoreTimer();
        long numRecords = 52;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);
        populateData(numRecords);
        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        final List<Integer> rangeIds = List.of(0, 1, 10);

        // Iterate some records and some indexes in each range-id, make sure that they are partly scanned
        for (int rangeId: rangeIds) {
            timer.reset();
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .useLegacyScrubber(legacy)
                            .setEntriesScanLimit(20)
                            .setRangeId(rangeId)
                            .build())
                    .setLimit(7)
                    .build()) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();
            }
            ScrubbersMissingRanges ranges = getScrubbersMissingRange(tgtIndex, rangeId);
            assertFalse(ranges.indexes.begin == null || RangeSet.isFinalKey(ranges.indexes.begin));
            assertFalse(ranges.records.begin == null || RangeSet.isFinalKey(ranges.records.begin));
        }

        // Erase all index scrubbing data
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                    .build()) {
                indexScrubber.eraseAllIndexingScrubbingData(context, recordStore);
            }
            context.commit();
        }

        // Assert full iterations after erasing the data in each range-id
        for (int rangeId: rangeIds) {
            assertFullIteration(tgtIndex, rangeId, numRecords, legacy, true);
            assertFullIteration(tgtIndex, rangeId, numRecords, legacy, false);
        }

        // Assert full iteration in a random range-id, just to make a point
        assertFullIteration(tgtIndex, 77, numRecords, legacy, true);
        assertFullIteration(tgtIndex, 77, numRecords, legacy, false);
    }

    private void assertFullIteration(Index index, int rangeId, long numRecords, boolean legacy, boolean missing) {
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(index, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .useLegacyScrubber(legacy)
                        .setAllowRepair(false)
                        .setRangeId(rangeId)
                        .build())
                .build()) {
            if (missing) {
                indexScrubber.scrubMissingIndexEntries();
            } else {
                indexScrubber.scrubDanglingIndexEntries();
            }
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

    }
}
