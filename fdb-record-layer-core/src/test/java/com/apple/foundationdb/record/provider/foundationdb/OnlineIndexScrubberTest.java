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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for scrubbing readable indexes with {@link OnlineIndexer}.
 */
class OnlineIndexScrubberTest extends OnlineIndexerTest {

    private FDBRecordStoreTestBase.RecordMetaDataHook myHook(Index index) {
        return allIndexesHook(List.of(index));
    }

    @Test
    void testScrubberSimpleMissing() throws ExecutionException, InterruptedException {
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
                        .build())
                .build()) {
            res = indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, res);
    }

    @Test
    void testScrubberSimpleDangling() throws ExecutionException, InterruptedException {
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
                        .build())
                .build()) {
            res = indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, res);
    }

    @Test
    void testScrubberLimits() {
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

    @Test
    void testScrubberInvalidIndexState() {
        final int numRecords = 20;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);

        // refuse to scrub a non-readable index
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex).build()) {
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
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(nonValueIndex).build()) {
            assertThrows(IndexingBase.ValidationException.class, indexScrubber::scrubDanglingIndexEntries);
            assertThrows(IndexingBase.ValidationException.class, indexScrubber::scrubMissingIndexEntries);
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

    @Test
    void testScrubUnnestedIndex() {
        final Index targetIndex = new Index("unnestedIndex", concat(field("entry").nest("key"), field("parent").nest("other_id"), field("entry").nest("int_value")));
        final OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler recordHandler = OnlineIndexerBuildUnnestedIndexTest.OnlineIndexerTestUnnestedRecordHandler.instance();
        FDBRecordStoreTestBase.RecordMetaDataHook hook = recordHandler.baseHook(true, null)
                .andThen(metaDataBuilder -> metaDataBuilder.addIndex(OnlineIndexerBuildUnnestedIndexTest.UNNESTED, targetIndex));
        openMetaData(recordHandler.getFileDescriptor(), hook);

        final int numRecords = 20;
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < numRecords; i++) {
                recordStore.saveRecord(TestRecordsNestedMapProto.OuterRecord.newBuilder()
                        .setRecId(2 * i)
                        .setOtherId(i % 2)
                        .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                                .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                        .setKey("a")
                                        .setIntValue(i))
                                .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                        .setKey("b")
                                        .setIntValue(i))
                                .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                        .setKey("c")
                                        .setIntValue(i))
                        )
                        .build()
                );
                recordStore.saveRecord(TestRecordsNestedMapProto.OtherRecord.newBuilder()
                        .setRecId(2 * i + 1)
                        .setOtherId(i % 2)
                        .build()
                );
            }

            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        // Scanned 5 * numRecords. There are numRecords :
        //  1. When looking for missing entries, we scan every record in the database. There are numRecords of type
        //     OuterRecord and numRecords of type OtherRecord (so that's 2 * numRecords)
        //  2. When scanning dangling entries, we look up one for every entry in the index. There are 3 map entries
        //     for each of type OuterRecord, so that's 3 * numRecords.
        assertEquals(numRecords * 5, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
    }
}
