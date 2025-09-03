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
import com.apple.foundationdb.Transaction;
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
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for scrubbing readable indexes with {@link OnlineIndexer}.
 */
class OnlineIndexScrubberTest extends OnlineIndexerTest {

    private FDBRecordStoreTestBase.RecordMetaDataHook myHook(Index index) {
        return allIndexesHook(List.of(index));
    }

    @Test
    void testScrubberSimpleMissing() throws ExecutionException, InterruptedException {
        final long numRecords = 50;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, true);
        assertFullIterationNoFix(tgtIndex, 0, numRecords, true, false);

        // manually delete a few index entries
        final int missingCount = causeMissingIndexEntries(tgtIndex, numRecords);

        // verify the missing entries are found and fixed
        final FDBStoreTimer timer = new FDBStoreTimer();
        long res;
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
        assertFullIterationNoFix(tgtIndex, 0, numRecords, true, false);
    }

    @Test
    void testScrubberSimpleDangling() throws ExecutionException, InterruptedException {
        long numRecords = 51;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, false);
        assertFullIterationNoFix(tgtIndex, 0, numRecords, false, false);

        // manually delete a few records w/o updating the indexes
        final int danglingCount = causeDanglingIndexEntries(tgtIndex, numRecords);

        // verify the missing entries are found (report only, no repair)
        final FDBStoreTimer timer = new FDBStoreTimer();
        long res;
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
        assertFullIterationNoFix(tgtIndex, 0, numRecords, false, false);
    }

    @Test
    void testScrubberLimits() {
        final int numRecords = 52;
        final int chunkSize = 7;
        final int numChunks = 1 + (numRecords / chunkSize);
        long resDangling;
        long resMissing;

        Index tgtIndex = createValueIndexAndPopulateData(numRecords, true);

        // Scrub both dangling & missing. Scan counts in this test should be doubles.
        final FDBStoreTimer timer = new FDBStoreTimer();
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

    @Test
    void testScrubberInvalidIndexState() {
        final int numRecords = 20;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, true);

        // refuse to scrub a non-readable index
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
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
                .build()) {
            assertThrows(UnsupportedOperationException.class, indexScrubber::scrubDanglingIndexEntries);
            assertThrows(UnsupportedOperationException.class, indexScrubber::scrubMissingIndexEntries);
        }
    }

    @Test
    void testScrubberVersionIndexType() {
        // Update - this used to test legacy mode with ignoreTypeCheck. Since the code had changed, these functions are not supported
        // anymore and it is up to the index maintainer to decide if scrubbing is supported. Version index maintainer, like Value index maintainer,
        // should now support scrubbing.
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

        final long numRecords = 20;
        populateNestedMapData(numRecords);

        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .build())
                .build()) {
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
        }
    }

    @Test
    void testDetectDanglingUnnestedIndex() {
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
                        .build())
                .build()) {
            // numrecord/3 is the top level records deleted, each with 3 sub records
            assertEquals(numRecords, indexScrubber.scrubDanglingIndexEntries());
            indexScrubber.scrubMissingIndexEntries();
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        }
    }

    @Test
    void testDetectMissingUnnestedIndex() {
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

        // Repair is not supported for synthetic records
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(true)
                        .build())
                .build()) {
            assertThrows(UnsupportedOperationException.class, indexScrubber::scrubDanglingIndexEntries);
            assertThrows(UnsupportedOperationException.class, indexScrubber::scrubMissingIndexEntries);
        }
    }

    @Test
    void testScrubJoinedIndex() {
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
                        .setAllowRepair(false)
                        .build())
                .build()) {
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
        }
    }

    @Test
    void testDetectDanglingFromJoinedIndex() {
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
                        .build())
                .build()) {
            assertEquals(numRecords / 4, indexScrubber.scrubDanglingIndexEntries());
            assertEquals(numRecords / 4, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

            indexScrubber.scrubMissingIndexEntries();
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        }
    }

    @Test
    void testDetectMissingFromJoinedIndex() {
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
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
            indexScrubber.scrubMissingIndexEntries();
        }

        assertEquals(numRecords / 4, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));

        // Repair is not supported for synthetic records
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(targetIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setLogWarningsLimit(Integer.MAX_VALUE)
                        .setAllowRepair(true)
                        .build())
                .build()) {
            assertThrows(UnsupportedOperationException.class, indexScrubber::scrubDanglingIndexEntries);
            assertThrows(UnsupportedOperationException.class, indexScrubber::scrubMissingIndexEntries);
        }
    }

    private static Stream<Arguments> twoRangeIds() {
        return Stream.of(
                Arguments.of(0, 2),
                Arguments.of(2, 0),
                Arguments.of(2, 3),
                Arguments.of(0, 100),
                Arguments.of(100, 0),
                Arguments.of(100, 101)
        );
    }

    @ParameterizedTest
    @MethodSource("twoRangeIds")
    void testScrubberMissingMixedRanges(int rangeId, int anotherRangeId) throws ExecutionException, InterruptedException {
        // Start fixing one range, then fully scan another, then finish fixing within original the range. Verify all fixed.
        final long numRecords = 47;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, true);

        assertFullIterationNoFix(tgtIndex, 10, numRecords, true, false);
        causeMissingIndexEntries(tgtIndex, numRecords);

        // Run/fix few of the records, make sure it is a partial scan
        final FDBStoreTimer timer = new FDBStoreTimer();
        ScrubbersMissingRanges lastRanges = getScrubbersMissingRange(tgtIndex);
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(10)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex, rangeId);
        assertFalse(Arrays.equals(midRange.records.begin, lastRanges.records.begin));

        // Run/Detect all in another range id. Make sure it scans the whole range
        assertFullIterationNoFix(tgtIndex, anotherRangeId, numRecords, true, true);

        // Make sure previous range was not affected
        final ScrubbersMissingRanges range = getScrubbersMissingRange(tgtIndex, rangeId);
        assertArrayEquals(range.records.begin, midRange.records.begin);

        // finish fixing
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }

        // now verify it's fixed (arbitrarily using range-id 10)
        assertFullIterationNoFix(tgtIndex, 10, numRecords, true, false);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100})
    void testScrubberMixedTypesSameRangeIdInterleaved(int rangeId) {
        // Start fixing one range, then make a partial scan on another, then finish fixing within original the range. Verify all fixed.
        final long numRecords = 51;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, false);

        assertFullIterationNoFix(tgtIndex, 10, numRecords, true, false);
        assertFullIterationNoFix(tgtIndex, 10, numRecords, false, false);

        // Partly scan missing
        final FDBStoreTimer timerMissing = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timerMissing)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(10)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertTrue(0 < timerMissing.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final ScrubbersMissingRanges midMissingRange = getScrubbersMissingRange(tgtIndex, rangeId);
        assertFalse(isFullScrubRange(midMissingRange.records));

        // Partly scan dangling
        final FDBStoreTimer timerDangling = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timerDangling)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(false)
                        .setEntriesScanLimit(11)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(5)
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        // Verify that this other range is partial, and that the records range hadn't changed
        assertTrue(0 < timerDangling.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final ScrubbersMissingRanges midDanglingRange = getScrubbersMissingRange(tgtIndex, rangeId);
        assertFalse(isFullScrubRange(midDanglingRange.indexes));
        assertArrayEquals(midMissingRange.records.begin, midDanglingRange.records.begin);

        // Continue scan missing
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timerMissing)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        // Verify full scan, and that the dangling range hadn't changed
        assertEquals(numRecords, timerMissing.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertArrayEquals(midDanglingRange.indexes.begin, getScrubbersMissingRange(tgtIndex, rangeId).indexes.begin);

        // Continue scan dangling
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timerDangling)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        // Verify full scan
        assertEquals(numRecords, timerDangling.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
    }

    @ParameterizedTest
    @MethodSource("twoRangeIds")
    void testScrubberMissingMixedRangesInterleaved(int rangeId, int anotherRangeId) throws ExecutionException, InterruptedException {
        // Start fixing one range, then make a partial scan on another, then finish fixing within original the range. Verify all fixed.
        final long numRecords = 51;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, false);

        assertFullIterationNoFix(tgtIndex, 10, numRecords, true, false);
        final int missingCount = causeMissingIndexEntries(tgtIndex, numRecords);

        // Run/fix few of the records, make sure it is a partial scan, preserve timer
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(10)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Run/Detect few records in another range-id, no timer
        long fixedCount;
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(false)
                        .setEntriesScanLimit(21)
                        .setScrubbingRangeId(anotherRangeId)
                        .build())
                .setLimit(20)
                .build()) {
            fixedCount = indexScrubber.scrubMissingIndexEntries();
        }
        // Verify that this other range is partial
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex, anotherRangeId);
        assertFalse(isFullScrubRange(midRange.records));

        // finish fixing in original range-id with preserved timer. Check timer's values
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .build()) {
            fixedCount += indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
        assertEquals(missingCount, fixedCount);


        // now verify it's fixed (arbitrarily using range-id 10)
        assertFullIterationNoFix(tgtIndex, 10, numRecords, true, false);
    }

    @ParameterizedTest
    @MethodSource("twoRangeIds")
    void testScrubberMissingMixedRangesWithReset(int rangeId, int anotherRangeId) {
        // Make a partial scan, then assert that "reset" implies a full scan
        final long numRecords = 32;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, true);

        // Run/Detect few of the records, make sure it is a partial scan
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(20)
                        .setAllowRepair(false)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(7)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        final int scanCount = timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
        assertTrue(0 < scanCount);
        assertTrue(numRecords > scanCount);

        // Partly scan another range id
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(10)
                        .setAllowRepair(false)
                        .setScrubbingRangeId(anotherRangeId)
                        .build())
                .setLimit(7)
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        final Range otherRange = getScrubbersMissingRange(tgtIndex, anotherRangeId).records;
        assertFalse(isFullScrubRange(otherRange));

        // Run/Detect all range-id 2, with a reset, assert full scan
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(false)
                        .setScrubbingRangeId(rangeId)
                        .setScrubbingRangeReset(true)
                        .build())
                .build()) {
            indexScrubber.scrubMissingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Make sure that other range id was not affected
        assertArrayEquals(otherRange.begin, getScrubbersMissingRange(tgtIndex, anotherRangeId).records.begin);
    }

    @ParameterizedTest
    @MethodSource("twoRangeIds")
    void testScrubberDanglingMixedRanges(int rangeId, int anotherRangeId) throws ExecutionException, InterruptedException {
        // Start fixing one range, then fully scan another, then finish fixing within original the range. Verify all fixed.
        long numRecords = 53;

        Index tgtIndex = createValueIndexAndPopulateData(numRecords, false);
        assertFullIterationNoFix(tgtIndex, 10, numRecords, false, false);

        final int danglingCount = causeDanglingIndexEntries(tgtIndex, numRecords);

        // Run/fix few of the missing entries, make sure it is a partial scan
        final FDBStoreTimer timer = new FDBStoreTimer();
        final ScrubbersMissingRanges lastRanges = getScrubbersMissingRange(tgtIndex);
        long fixedCount;
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(10)
                .build()) {
            fixedCount = indexScrubber.scrubDanglingIndexEntries();
        }
        assertTrue(0 < fixedCount && fixedCount < danglingCount);
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex, rangeId);
        assertFalse(Arrays.equals(midRange.indexes.begin, lastRanges.indexes.begin));

        // Run/Detect all in another range id. Make sure it scans the whole range
        assertFullIterationNoFix(tgtIndex, anotherRangeId, numRecords - fixedCount, false, true);

        // Make sure previous range-id was not affected
        final ScrubbersMissingRanges range = getScrubbersMissingRange(tgtIndex, rangeId);
        assertArrayEquals(range.indexes.begin, midRange.indexes.begin);

        // finish fixing
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .build()) {
            fixedCount += indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(danglingCount, fixedCount);

        // now verify it's fixed
        assertFullIterationNoFix(tgtIndex, 10, numRecords - fixedCount, false, false);
    }

    @ParameterizedTest
    @MethodSource("twoRangeIds")
    void testScrubberDangingMixedRangesInterleaved(int rangeId, int anotherRangeId) throws ExecutionException, InterruptedException {
        // Start fixing one range, then make a partial scan on another, then finish fixing within original the range. Verify all fixed.
        long numRecords = 55;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, true);

        assertFullIterationNoFix(tgtIndex, 10, numRecords, false, false);
        final int danglingCount = causeDanglingIndexEntries(tgtIndex, numRecords);

        // Run/fix few of the records, make sure it is a partial scan, preserve timer
        final FDBStoreTimer timer = new FDBStoreTimer();
        long fixedCount;
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(10)
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(10)
                .build()) {
            fixedCount = indexScrubber.scrubDanglingIndexEntries();
        }
        assertTrue(0 < timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Run/Detect few records with another range-id, no timer
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setScrubbingRangeId(anotherRangeId)
                        .setAllowRepair(false)
                        .setEntriesScanLimit(21)
                        .build())
                .setLimit(20)
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        // Verify that the other range is partial
        final ScrubbersMissingRanges midRange = getScrubbersMissingRange(tgtIndex, anotherRangeId);
        assertFalse(isFullScrubRange(midRange.indexes));

        // finish fixing previous range-id, with preserved timer. Check timer's values
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(true)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .build()) {
            fixedCount += indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(danglingCount, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
        assertEquals(danglingCount, fixedCount);

        numRecords -= danglingCount; // if the dangling indexes were removed, this should be reflected later

        // now verify it's fixed
        assertFullIterationNoFix(tgtIndex, 10, numRecords, false, false);
    }

    @ParameterizedTest
    @MethodSource("twoRangeIds")
    void testScrubberDanglingMixedRangesWithReset(int rangeId, int anotherRangeId) {
        // Make a partial scan, then assert that "reset" implies a full scan
        long numRecords = 32;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, false);

        // Partly scan
        final FDBStoreTimer timer = new FDBStoreTimer();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(10)
                        .setAllowRepair(false)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .setLimit(7)
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        final Range otherRange = getScrubbersMissingRange(tgtIndex, rangeId).indexes;
        assertFalse(isFullScrubRange(otherRange));

        // Run/Detect few of the index entries on another range-id, make sure it is a partial scan
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setEntriesScanLimit(20)
                        .setAllowRepair(false)
                        .setScrubbingRangeId(anotherRangeId)
                        .build())
                .setLimit(7)
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        final int scanCount = timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
        assertTrue(0 < scanCount);
        assertTrue(numRecords > scanCount);

        // Run/Detect all other range-id, with a reset, assert full scan
        timer.reset();
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(false)
                        .setScrubbingRangeId(anotherRangeId)
                        .setScrubbingRangeReset(true)
                        .build())
                .build()) {
            indexScrubber.scrubDanglingIndexEntries();
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));

        // Make sure that the original range-id was not affected
        assertArrayEquals(otherRange.begin, getScrubbersMissingRange(tgtIndex, rangeId).indexes.begin);
    }

    @Test
    void testScrubberEraseIndexScrubbingData() {
        // Create a few partial scans, erase all, the verify full scans
        long numRecords = 52;
        Index tgtIndex = createValueIndexAndPopulateData(numRecords, true);

        final List<Integer> rangeIds = List.of(0, 1, 10);

        // Iterate some records and some indexes in each range-id, make sure that they are partly scanned
        for (int rangeId: rangeIds) {
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setEntriesScanLimit(20)
                            .setScrubbingRangeId(rangeId)
                            .build())
                    .setLimit(7)
                    .build()) {
                indexScrubber.scrubDanglingIndexEntries();
                indexScrubber.scrubMissingIndexEntries();
            }
            ScrubbersMissingRanges ranges = getScrubbersMissingRange(tgtIndex, rangeId);
            assertFalse(isFullScrubRange(ranges.indexes));
            assertFalse(isFullScrubRange(ranges.records));
        }

        // Erase all index scrubbing data
        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(tgtIndex)
                    .build()) {
                indexScrubber.eraseAllIndexingScrubbingData(context, recordStore);
            }
            context.commit();
        }

        // Assert full iterations after erasing the data in each range-id
        for (int rangeId: rangeIds) {
            assertFullIterationNoFix(tgtIndex, rangeId, numRecords, true, false);
            assertFullIterationNoFix(tgtIndex, rangeId, numRecords, false, false);
        }

        // Assert full iteration in a random range-id, just to make a point
        assertFullIterationNoFix(tgtIndex, 77, numRecords, true, false);
        assertFullIterationNoFix(tgtIndex, 77, numRecords, false, false);
    }

    private void assertFullIterationNoFix(Index index, int rangeId, long numRecords, boolean missing, boolean expectIssues) {
        FDBStoreTimer timer = new FDBStoreTimer();
        long res;
        try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(index, timer)
                .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                        .setAllowRepair(false)
                        .setScrubbingRangeId(rangeId)
                        .build())
                .build()) {
            if (missing) {
                res = indexScrubber.scrubMissingIndexEntries();
            } else {
                res = indexScrubber.scrubDanglingIndexEntries();
            }
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        if (!expectIssues) {
            // ONLINE_INDEX_BUILDER_RECORDS_INDEXED is always 0 during dangling scrubbing. In this case, we also do not expect issues during missing scrubbing.
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
            assertEquals(0, res);
        }
    }

    private int causeMissingIndexEntries(Index index, long numRecords) throws ExecutionException, InterruptedException {
        // manually delete a few index entries
        int missingCount = 0;
        try (FDBRecordContext context = openContext()) {
            List<IndexEntry> indexEntries = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            final Transaction transaction = recordStore.getContext().ensureActive();
            for (int i = 3; i < numRecords; i *= 2) {
                final IndexEntry indexEntry = indexEntries.get(i);
                final Tuple valueKey = indexEntry.getKey();
                final byte[] keyBytes = recordStore.indexSubspace(index).pack(valueKey);
                transaction.clear(keyBytes);
                missingCount ++;
            }
            context.commit();
        }
        assertTrue(0 < missingCount);
        return missingCount;
    }

    private int causeDanglingIndexEntries(Index index, long numRecords) throws ExecutionException, InterruptedException {
        int danglingCount = 0;
        try (FDBRecordContext context = openContext(false)) {
            List<FDBIndexedRecord<Message>> indexRecordEntries = recordStore.scanIndexRecords(index.getName(), IndexScanType.BY_VALUE, TupleRange.ALL, null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            final Transaction transaction = recordStore.getContext().ensureActive();
            for (int i = 3; i < numRecords; i *= 2) {
                final FDBIndexedRecord<Message> indexRecord = indexRecordEntries.get(i);
                final FDBStoredRecord<Message> rec = indexRecord.getStoredRecord();
                final Subspace subspace = recordStore.recordsSubspace().subspace(rec.getPrimaryKey());
                transaction.clear(subspace.range());
                danglingCount ++;
            }
            context.commit();
        }
        assertTrue(0 < danglingCount);
        return danglingCount;
    }

    private Index createValueIndexAndPopulateData(long numRecords, boolean unique) {
        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY,
                IndexTypes.VALUE, unique ? IndexOptions.UNIQUE_OPTIONS : IndexOptions.EMPTY_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);
        populateData(numRecords);
        openSimpleMetaData(hook);
        buildIndexClean(tgtIndex);
        return tgtIndex;
    }

    private boolean isFullScrubRange(Range range) {
        // Note: in this context (index scrubbing), the "end" key is fixed..
        return range == null || range.begin == null || RangeSet.isFirstKey(range.begin) || RangeSet.isFinalKey(range.begin);
    }
}
