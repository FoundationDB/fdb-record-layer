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

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import org.junit.jupiter.api.Test;

import java.util.List;
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
    }

    @Test
    public void testIndexFromIndexSimple() {

        final FDBStoreTimer timer = new FDBStoreTimer();
        final long numRecords = 1000;

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
                .setIndexFromIndex(OnlineIndexer.IndexFromIndexPolicy.newBuilder()
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
        final int numRecords = 1327;
        final int chunkSize  = 42;
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
                .setIndexFromIndex(OnlineIndexer.IndexFromIndexPolicy.newBuilder()
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
        // Let tgtIndex be a non-idempotent index

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
                .setIndexFromIndex(OnlineIndexer.IndexFromIndexPolicy.newBuilder()
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
        // Let tgtIndex be a non-idempotent index

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
                .setIndexFromIndex(OnlineIndexer.IndexFromIndexPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setTimer(timer)
                .build()) {

            OnlineIndexerException e = assertThrows(OnlineIndexerException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("IndexFromIndex:")); // could be either non-idempotent or non-IndexTypes.VALUE warning, depends on implementation
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
                .setIndexFromIndex(OnlineIndexer.IndexFromIndexPolicy.newBuilder()
                        .setSourceIndex("src_index")
                        .forbidRecordScan()
                        .build())
                .setTimer(timer)
                .build()) {

            OnlineIndexerException e = assertThrows(OnlineIndexerException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("IndexFromIndex: src is not readable"));
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
    }
}
