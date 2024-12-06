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
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
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
import com.apple.test.BooleanSource;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
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
                            .ignoreIndexTypeCheck(legacy)
                            .build())
                    .setLimit(chunkSize)
                    .build()) {
                resDangling = indexScrubber.scrubDanglingIndexEntries();
                resMissing = indexScrubber.scrubMissingIndexEntries();
            }
            assertEquals(0, resDangling);
            assertEquals(0, resMissing);
        }
        assertEquals(numRecords * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
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
                            .ignoreIndexTypeCheck(legacy))
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
                        .ignoreIndexTypeCheck(legacy))
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
                        .ignoreIndexTypeCheck(legacy)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        // Scanned 5 * numRecords. This is from:
        //  1. When looking for missing entries, we scan every record in the database. There are numRecords of type
        //     OuterRecord and numRecords of type OtherRecord (so that's 2 * numRecords)
        //  2. When scanning dangling entries, we look up one for every entry in the index. There are 3 map entries
        //     for each of type OuterRecord, so that's 3 * numRecords.
        assertEquals(numRecords * 5, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
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
                        .ignoreIndexTypeCheck(legacy)
                        .build())
                .build()) {
            assertThrows(RecordDoesNotExistException.class, indexScrubber::scrubDanglingIndexEntries);
            indexScrubber.scrubMissingIndexEntries();
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
                        .ignoreIndexTypeCheck(legacy)
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

        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(true)
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
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
                        .ignoreIndexTypeCheck(legacy)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        // Scanned 3 * numRecords. This is from:
        //  1. When looking for missing entries, we scan every record in the database. There are numRecords of type
        //     MySimpleRecord and numRecords of type MyOtherRecord (so that's 2 * numRecords)
        //  2. When scanning dangling entries, we look up one for every entry in the index. There are is one entry
        //     for each pair of simple and other records, so that's another numRecords scanned
        assertEquals(numRecords * 3, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
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
                        .ignoreIndexTypeCheck(legacy)
                        .build())
                .build()) {
            assertThrows(RecordDoesNotExistException.class, indexScrubber::scrubDanglingIndexEntries);
            indexScrubber.scrubMissingIndexEntries();
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
                        .ignoreIndexTypeCheck(legacy)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        assertEquals(numRecords / 4, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(true)
                        .ignoreIndexTypeCheck(legacy)
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
                        .ignoreIndexTypeCheck(legacy)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
    }
}
