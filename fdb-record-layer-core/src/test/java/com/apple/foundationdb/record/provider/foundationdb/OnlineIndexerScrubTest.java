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
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for scrubbing readable indexes with {@link OnlineIndexer}.
 */
public class OnlineIndexerScrubTest extends OnlineIndexerTest {
    private void populateData(final long numRecords) {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private FDBRecordStoreTestBase.RecordMetaDataHook myHook(Index index) {
        return metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);
    }

    private void buildIndex(Index srcIndex) {
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(srcIndex).setSubspace(subspace)
                .build()) {
            indexer.buildIndex(true);
        }
    }

    @Test
    void testScrubberSimpleMissing() throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 100;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndex(tgtIndex);

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setScrubDangling(false)
                        .setReportMissingLimit(Integer.MAX_VALUE)
                        .build())
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_MISSING));

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
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setScrubDangling(false)
                        .setReportMissingLimit(Integer.MAX_VALUE)
                        .build())
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(missingCount, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_MISSING));

        // now verify it's fixed
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setScrubDangling(false)
                        .setReportMissingLimit(Integer.MAX_VALUE)
                        .build())
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_MISSING));
    }


    @Test
    void testScrubberSimpleDangling() throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        long numRecords = 100;

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndex(tgtIndex);

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setScrubMissing(false)
                        .setScrubDangling(true)
                        .setReportDanglingLimit(Integer.MAX_VALUE)
                        .build())
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_DANGLING));

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
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setScrubDangling(true)
                        .setScrubMissing(false)
                        .setReportDanglingLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .build())
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(danglingCount, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_DANGLING));

        // verify the missing entries are found and fixed
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setScrubDangling(true)
                        .setScrubMissing(false)
                        .setReportDanglingLimit(Integer.MAX_VALUE)
                        .build())
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(danglingCount, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_DANGLING));
        numRecords -= danglingCount; // if the dangling indexes were removed, this should be reflected later

        // now verify it's fixed
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setScrubMissing(false)
                        .setReportDanglingLimit(Integer.MAX_VALUE)
                        .build())
                .setTimer(timer)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_DANGLING));
    }

    @Test
    void testScrubberLimits() throws ExecutionException, InterruptedException {
        final FDBStoreTimer timer = new FDBStoreTimer();
        final int numRecords = 1328;
        final int chunkSize  = 42;
        final int numChunks  = 1 + (numRecords / chunkSize);

        Index tgtIndex = new Index("tgt_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = myHook(tgtIndex);

        openSimpleMetaData();
        populateData(numRecords);

        openSimpleMetaData(hook);
        buildIndex(tgtIndex);

        // Scrub both dangling & missing. Scan counts in this test should be doubles.
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setReportMissingLimit(Integer.MAX_VALUE)
                        .setAllowRepair(false)
                        .setQuota(1000000)
                        .build())
                .setTimer(timer)
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(numRecords * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numChunks * 2 , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_MISSING));

        // Scrub dangling with a quota
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setReportMissingLimit(Integer.MAX_VALUE)
                        .setScrubMissing(false)
                        .setQuota(1)
                        .build())
                .setTimer(timer)
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(1 , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertEquals(chunkSize, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_MISSING));


        // Scrub both with a quota
        timer.reset();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(tgtIndex).setSubspace(subspace)
                .setScrubbingPolicy(OnlineIndexer.ScrubbingPolicy.newBuilder()
                        .setQuota(chunkSize * 3)
                        .build())
                .setTimer(timer)
                .setLimit(chunkSize)
                .build()) {
            indexBuilder.buildIndex(false);
        }
        assertEquals(3 * 2 , timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT));
        assertEquals(chunkSize * 3 * 2, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_SCRUBBER_INDEXES_MISSING));
    }
}
