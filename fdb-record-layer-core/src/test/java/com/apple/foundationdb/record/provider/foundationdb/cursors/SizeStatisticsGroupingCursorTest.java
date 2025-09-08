/*
 * SizeStatisticsGroupedCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.StringUtils;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test the {@link SizeStatisticsGroupingCursor} through the use of real records and store.
 */
@Tag(Tags.RequiresFDB)
public class SizeStatisticsGroupingCursorTest extends FDBRecordStoreTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SizeStatisticsGroupingCursorTest.class);

    @Test
    public void calcSize() throws Exception {
        populateStore(1_000, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long exactStoreSize = getTotalSize(getStoreSizeResult(recordStore));
            long exactRecordsSize = getTotalSize(getRecordsSizeResult(recordStore));
            long exactIndexSize = getTotalSize(getIndexSizeResult(recordStore, "MySimpleRecord$num_value_3_indexed"));
            long exactSubspaceSize = getTotalSize(getSubspaceSizeResult(recordStore, recordStore.getSubspace()));

            Assertions.assertThat(exactStoreSize).isGreaterThan(0);
            Assertions.assertThat(exactRecordsSize).isGreaterThan(0);
            Assertions.assertThat(exactIndexSize).isGreaterThan(0);
            Assertions.assertThat(exactStoreSize).isEqualTo(exactSubspaceSize);

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    public void calcSizeWithContinuation() throws Exception {
        populateStore(1_000, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder().setScannedBytesLimit(10000).build());
            // Scan a little of the data
            RecordCursorResult<SizeStatisticsGroupedResults> continuationResult = getStoreSizeResult(recordStore, scanProperties, null, 0);
            Assertions.assertThat(continuationResult.hasNext()).isFalse(); // should return early due to BYTE LIMIT, no value returned
            Assertions.assertThat(continuationResult.getNoNextReason()).isEqualTo(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED);
            Assertions.assertThat(continuationResult.getContinuation()).isNotNull();
            // scan the rest of the data
            continuationResult = getStoreSizeResult(recordStore, ScanProperties.FORWARD_SCAN, continuationResult.getContinuation().toBytes(), 0);
            Assertions.assertThat(continuationResult.hasNext()).isTrue();
            Assertions.assertThat(continuationResult.getContinuation().isEnd()).isFalse();
            // scan again, to get the continuation to the end
            RecordCursorResult<SizeStatisticsGroupedResults> finalResult = getStoreSizeResult(recordStore, ScanProperties.FORWARD_SCAN, continuationResult.getContinuation().toBytes(), 0);
            Assertions.assertThat(finalResult.hasNext()).isFalse();
            Assertions.assertThat(finalResult.getContinuation().isEnd()).isTrue();

            final RecordCursorResult<SizeStatisticsGroupedResults> controlResult = getStoreSizeResult(recordStore);

            Assertions.assertThat(continuationResult.get().getStats()).usingRecursiveComparison().isEqualTo(controlResult.get().getStats());

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    public void calcSizeWithAggregationDepthOne() throws Exception {
        final int recordCount = 1_000;
        populateStore(recordCount, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Map<Tuple, SizeStatisticsResults> result = getStoreSizeAllResults(recordStore, 1);

            Assertions.assertThat(result.size()).isEqualTo(3); // store header, records, indexes
            Assertions.assertThat(result.get(Tuple.from(0))).matches(stats -> stats.getKeyCount() == 1); // header
            Assertions.assertThat(result.get(Tuple.from(1))).matches(stats -> stats.getKeyCount() == recordCount * 2); // records and versions
            Assertions.assertThat(result.get(Tuple.from(2))).matches(stats -> stats.getKeyCount() > 0); // indexes

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    public void calcSizeWithAggregationDepthTwo() throws Exception {
        final int recordCount = 1_000;
        populateStore(recordCount, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Map<Tuple, SizeStatisticsResults> result = getStoreSizeAllResults(recordStore, 2);

            Assertions.assertThat(result.size()).isEqualTo(1006);

            SizeStatisticsResults headerStats = result.get(Tuple.from(0));
            Assertions.assertThat(headerStats.getKeyCount()).isEqualTo(1); // header

            List<Map.Entry<Tuple, SizeStatisticsResults>> recordStats = result.entrySet().stream()
                    .filter(entry -> entry.getKey().getLong(0) == 1).collect(Collectors.toList()); // all entries for records have "1" as the first element of the tuple
            Assertions.assertThat(recordStats).hasSize(recordCount);
            Assertions.assertThat(recordStats).allMatch(entry -> entry.getKey().size() == 2); // grouping key has size 2
            Assertions.assertThat(recordStats).allMatch(entry -> entry.getValue().getKeyCount() == 2); // record data and version

            List<Map.Entry<Tuple, SizeStatisticsResults>> indexStats = result.entrySet().stream()
                    .filter(entry -> entry.getKey().getLong(0) == 2).collect(Collectors.toList()); // all entries for index have "2" as the first element of the tuple
            Assertions.assertThat(indexStats).hasSize(5); // 5 indexes on the store
            Assertions.assertThat(indexStats).allMatch(entry -> entry.getKey().size() == 2); // grouping key has size 2
            Assertions.assertThat(indexStats).allMatch(entry ->
                    !(entry.getKey().getString(1).contains("value")) || (entry.getValue().getKeyCount() == recordCount)); // value indexes contain all records

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    public void calcSizeWithAggregationDepthUnlimited() throws Exception {
        final int recordCount = 1_000;
        populateStore(recordCount, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final Map<Tuple, SizeStatisticsResults> result = getStoreSizeAllResults(recordStore, 1000);

            Assertions.assertThat(result.size()).isEqualTo(5003);

            SizeStatisticsResults headerStats = result.get(Tuple.from(0));
            Assertions.assertThat(headerStats.getKeyCount()).isEqualTo(1); // header

            List<Map.Entry<Tuple, SizeStatisticsResults>> recordStats = result.entrySet().stream()
                    .filter(entry -> entry.getKey().getLong(0) == 1).collect(Collectors.toList()); // all entries for records have "1" as the first element of the tuple
            Assertions.assertThat(recordStats).hasSize(recordCount * 2); // record data and version
            Assertions.assertThat(recordStats).allMatch(entry -> entry.getKey().size() == 3); // grouping key has size 3
            Assertions.assertThat(recordStats).allMatch(entry -> (entry.getKey().getLong(2) == 0) || (entry.getKey().getLong(2) == -1)); // split suffix is either 0 or -1
            Assertions.assertThat(recordStats).allMatch(entry -> entry.getValue().getKeyCount() == 1); // single entry per stat

            List<Map.Entry<Tuple, SizeStatisticsResults>> indexStats = result.entrySet().stream()
                    .filter(entry -> entry.getKey().getLong(0) == 2).collect(Collectors.toList()); // all entries for index have "2" as the first element of the tuple
            Assertions.assertThat(indexStats).hasSizeGreaterThan(recordCount);

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    public void emptyStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long exactStoreSize = getTotalSize(getStoreSizeResult(recordStore));
            long exactRecordsSize = getTotalSize(getRecordsSizeResult(recordStore));
            long exactIndexsSize = getTotalSize(getIndexSizeResult(recordStore, "MySimpleRecord$num_value_3_indexed"));

            Assertions.assertThat(exactStoreSize).isGreaterThan(0); // header
            Assertions.assertThat(exactRecordsSize).isZero();
            Assertions.assertThat(exactIndexsSize).isZero();

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    public void calcSizeRecordsOnly() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long totalRecordSize = getTotalSize(getRecordsSizeResult(recordStore));
            Assertions.assertThat(totalRecordSize).isGreaterThan(0);

            final Map<Tuple, SizeStatisticsResults> result = getRecordsSizeAllResults(recordStore, 0);
            Assertions.assertThat(result.size()).isEqualTo(1);
            Assertions.assertThat(result.get(Tuple.from()))
                    .matches(stats -> stats.getKeyCount() == recordCount * 2) // records and versions
                    .matches(stats -> stats.getTotalSize() == totalRecordSize);

            commit(context);
        }
    }

    @Test
    public void calcSizeSpecificIndex() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long totalIndexSize = getTotalSize(getIndexSizeResult(recordStore, "MySimpleRecord$num_value_3_indexed"));
            Assertions.assertThat(totalIndexSize).isGreaterThan(0);

            final Map<Tuple, SizeStatisticsResults> result = getIndexSizeAllResults(recordStore, "MySimpleRecord$num_value_3_indexed", 0);
            Assertions.assertThat(result.size()).isEqualTo(1); // only the specific index
            Assertions.assertThat(result.get(Tuple.from()))
                    .matches(stats -> stats.getKeyCount() == recordCount) // index entries
                    .matches(stats -> stats.getTotalSize() == totalIndexSize);

            commit(context);
        }
    }

    @Test
    public void calcSizeWithReverseScan() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ScanProperties reverseScanProperties = ScanProperties.FORWARD_SCAN.setReverse(true);
            final long reverse = getTotalSize(getStoreSizeResult(recordStore, reverseScanProperties, null, 0));
            final long forward = getTotalSize(getStoreSizeResult(recordStore, ScanProperties.FORWARD_SCAN, null, 0));

            Assertions.assertThat(reverse).isEqualTo(forward);
            commit(context);
        }
    }

    @Test
    public void invalidContinuation() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            byte[] invalidContinuation = new byte[] {1, 2, 3, 4, 5}; // Invalid protobuf data

            Assertions.assertThatThrownBy(
                            () -> SizeStatisticsGroupingCursor.ofStore(recordStore, context, ScanProperties.FORWARD_SCAN, invalidContinuation, 0))
                    .isInstanceOf(RecordCoreException.class)
                    .hasMessageContaining("Error parsing SizeStatisticsGroupingContinuation continuation");

            commit(context);
        }
    }

    @Test
    public void cursorCloseBehavior() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsGroupingCursor cursor = SizeStatisticsGroupingCursor.ofStore(recordStore, context, ScanProperties.FORWARD_SCAN, null, 0);

            Assertions.assertThat(cursor.isClosed()).isFalse();
            cursor.close();
            Assertions.assertThat(cursor.isClosed()).isTrue();

            commit(context);
        }
    }

    private Map<Tuple, SizeStatisticsResults> getStoreSizeAllResults(@Nonnull FDBRecordStore store, int aggregationDepth) {
        byte[] continuation = null;
        boolean done = false;
        Map<Tuple, SizeStatisticsResults> result = new HashMap<>();
        while (!done) {
            final RecordCursorResult<SizeStatisticsGroupedResults> storeSizeResult = getStoreSizeResult(store, ScanProperties.FORWARD_SCAN, continuation, aggregationDepth);
            if (storeSizeResult.hasNext()) {
                continuation = storeSizeResult.getContinuation().toBytes();
                Assertions.assertThat(result).doesNotContainKey(storeSizeResult.get().getAggregationKey());
                result.put(storeSizeResult.get().getAggregationKey(), storeSizeResult.get().getStats());
            }
            if (storeSizeResult.getContinuation().isEnd()) {
                done = true;
            } else {
                continuation = storeSizeResult.getContinuation().toBytes();
            }
        }
        return result;
    }

    private Map<Tuple, SizeStatisticsResults> getRecordsSizeAllResults(@Nonnull FDBRecordStore store, int aggregationDepth) {
        byte[] continuation = null;
        boolean done = false;
        Map<Tuple, SizeStatisticsResults> result = new HashMap<>();
        while (!done) {
            final RecordCursorResult<SizeStatisticsGroupedResults> recordsSizeResult = getRecordsSizeResult(store, ScanProperties.FORWARD_SCAN, continuation, aggregationDepth);
            if (recordsSizeResult.hasNext()) {
                continuation = recordsSizeResult.getContinuation().toBytes();
                Assertions.assertThat(result).doesNotContainKey(recordsSizeResult.get().getAggregationKey());
                result.put(recordsSizeResult.get().getAggregationKey(), recordsSizeResult.get().getStats());
            }
            if (recordsSizeResult.getContinuation().isEnd()) {
                done = true;
            } else {
                continuation = recordsSizeResult.getContinuation().toBytes();
            }
        }
        return result;
    }

    private Map<Tuple, SizeStatisticsResults> getIndexSizeAllResults(@Nonnull FDBRecordStore store, String indexName, int aggregationDepth) {
        byte[] continuation = null;
        boolean done = false;
        Map<Tuple, SizeStatisticsResults> result = new HashMap<>();
        while (!done) {
            final RecordCursorResult<SizeStatisticsGroupedResults> indexSizeResult = getIndexSizeResult(store, indexName, ScanProperties.FORWARD_SCAN, continuation, aggregationDepth);
            if (indexSizeResult.hasNext()) {
                continuation = indexSizeResult.getContinuation().toBytes();
                Assertions.assertThat(result).doesNotContainKey(indexSizeResult.get().getAggregationKey());
                result.put(indexSizeResult.get().getAggregationKey(), indexSizeResult.get().getStats());
            }
            if (indexSizeResult.getContinuation().isEnd()) {
                done = true;
            } else {
                continuation = indexSizeResult.getContinuation().toBytes();
            }
        }
        return result;
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getStoreSizeResult(@Nonnull FDBRecordStore store) {
        return getStoreSizeResult(store, ScanProperties.FORWARD_SCAN, null, 0);
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getStoreSizeResult(@Nonnull FDBRecordStore store, final ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        try (SizeStatisticsGroupingCursor statsCursor = SizeStatisticsGroupingCursor.ofStore(store, store.getContext(), scanProperties, continuation, aggregationDepth)) {
            return statsCursor.getNext();
        }
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getRecordsSizeResult(@Nonnull FDBRecordStore store) {
        return getRecordsSizeResult(store, ScanProperties.FORWARD_SCAN, null, 0);
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getRecordsSizeResult(@Nonnull FDBRecordStore store, final ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        try (SizeStatisticsGroupingCursor statsCursor = SizeStatisticsGroupingCursor.ofRecords(store, store.getContext(), scanProperties, continuation, aggregationDepth)) {
            return statsCursor.getNext();
        }
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getIndexSizeResult(@Nonnull FDBRecordStore store, String indexName) {
        return getIndexSizeResult(store, indexName, ScanProperties.FORWARD_SCAN, null, 0);
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getIndexSizeResult(@Nonnull FDBRecordStore store, String indexName, final ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        try (SizeStatisticsGroupingCursor statsCursor = SizeStatisticsGroupingCursor.ofIndex(store, indexName, store.getContext(), scanProperties, continuation, aggregationDepth)) {
            return statsCursor.getNext();
        }
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getSubspaceSizeResult(@Nonnull FDBRecordStore store, Subspace subspace) {
        return getSubspaceSizeResult(store, subspace, ScanProperties.FORWARD_SCAN, null, 0);
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getSubspaceSizeResult(@Nonnull FDBRecordStore store, Subspace subspace, final ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        try (SizeStatisticsGroupingCursor statsCursor = SizeStatisticsGroupingCursor.ofSubspace(subspace, store.getContext(), scanProperties, continuation, aggregationDepth)) {
            return statsCursor.getNext();
        }
    }

    private static long getTotalSize(@Nonnull RecordCursorResult<SizeStatisticsGroupedResults> result) {
        return getTotalSize(result, true);
    }

    private static long getTotalSize(@Nonnull RecordCursorResult<SizeStatisticsGroupedResults> result, boolean hasResult) {
        Assertions.assertThat(result.hasNext()).isEqualTo(hasResult);
        if (result.hasNext()) {
            return result.get().getStats().getTotalSize();
        } else {
            return -1;
        }
    }

    private void populateStore(int recordCount, long byteCount) throws Exception {
        int bytesPerRecord = (int)(byteCount / recordCount);
        final String data = StringUtils.repeat('x', bytesPerRecord);

        int currentRecord = 0;
        while (currentRecord < recordCount) {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);

                long transactionSize;
                do {
                    final Message record = TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setRecNo(currentRecord)
                            .setStrValueIndexed(data)
                            .build();
                    recordStore.saveRecord(record);
                    transactionSize = context.asyncToSync(FDBStoreTimer.Waits.WAIT_APPROXIMATE_TRANSACTION_SIZE,
                            context.getApproximateTransactionSize());
                    currentRecord++;
                } while (currentRecord < recordCount && transactionSize < 1_000_000);

                commit(context);
            }
        }
    }
}
