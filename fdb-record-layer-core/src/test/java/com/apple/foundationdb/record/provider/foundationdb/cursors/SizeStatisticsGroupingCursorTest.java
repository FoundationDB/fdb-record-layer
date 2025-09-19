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
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.StringUtils;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the {@link SizeStatisticsGroupingCursor} through the use of real records and store.
 */
@Tag(Tags.RequiresFDB)
public class SizeStatisticsGroupingCursorTest extends FDBRecordStoreTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SizeStatisticsGroupingCursorTest.class);

    @Test
    void calcSize() throws Exception {
        populateStore(1_000, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long exactStoreSize = getStoreTotalSize(recordStore);
            long exactRecordsSize = getRecordsTotalSize(recordStore);
            long exactIndexSize = getIndexTotalSize(recordStore, "MySimpleRecord$num_value_3_indexed");
            long exactSubspaceSize = getSubspaceTotalSize(recordStore, recordStore.getSubspace());

            assertThat(exactStoreSize).isGreaterThan(0);
            assertThat(exactRecordsSize).isGreaterThan(0);
            assertThat(exactIndexSize).isGreaterThan(0);
            assertThat(exactStoreSize).isEqualTo(exactSubspaceSize);

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    void calcSizeWithContinuation() throws Exception {
        populateStore(1_000, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder().setScannedBytesLimit(10_000).build());
            // Scan a little of the data
            AtomicReference<RecordCursorResult<SizeStatisticsGroupedResults>> resultRef = new AtomicReference<>();
            List<SizeStatisticsGroupedResults> allResults = getStoreCursor(recordStore, scanProperties, null, 0).asList(resultRef).join();
            assertThat(allResults).isEmpty();
            RecordCursorResult<SizeStatisticsGroupedResults> continuationResult = resultRef.get(); // the last result from the cursor
            assertThat(continuationResult.hasNext()).isFalse(); // should return early due to BYTE LIMIT, no value returned
            assertThat(continuationResult.getNoNextReason()).isEqualTo(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED);
            assertThat(continuationResult.getContinuation()).isNotNull();
            // scan the rest of the data
            allResults = getStoreCursor(recordStore, ScanProperties.FORWARD_SCAN, continuationResult.getContinuation().toBytes(), 0).asList(resultRef).join();
            continuationResult = resultRef.get();
            assertThat(allResults).hasSize(1);
            assertThat(continuationResult.hasNext()).isFalse();
            assertThat(continuationResult.getContinuation().isEnd()).isTrue();

            final SizeStatisticsGroupedResults controlResult = getStoreTotalResult(recordStore);

            assertThat(allResults.get(0).getStats()).usingRecursiveComparison().isEqualTo(controlResult.getStats());

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    void calcSizeWithManyContinuations() throws Exception {
        populateStore(100, 100_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder().setScannedBytesLimit(10_000).build());
            List<SizeStatisticsGroupedResults> allResults = new ArrayList<>();
            boolean done = false;
            byte[] continuation = null;
            AtomicReference<RecordCursorResult<SizeStatisticsGroupedResults>> resultRef = new AtomicReference<>();
            while (!done) {
                List<SizeStatisticsGroupedResults> results = getStoreCursor(recordStore, scanProperties, continuation, 10).asList(resultRef).join();
                allResults.addAll(results);
                if (resultRef.get().getContinuation().isEnd()) {
                    done = true;
                } else {
                    continuation = resultRef.get().getContinuation().toBytes();
                }
            }

            final List<SizeStatisticsGroupedResults> controlResult = getAllResults(getStoreCursor(recordStore, ScanProperties.FORWARD_SCAN, null, 10));

            assertThat(allResults).usingRecursiveComparison().isEqualTo(controlResult);

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    void calcSizeWithAggregationDepthOne() throws Exception {
        final int recordCount = 1_000;
        populateStore(recordCount, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            List<SizeStatisticsGroupedResults> allResults = getStoreCursor(recordStore, ScanProperties.FORWARD_SCAN, null, 1).asList().join();

            assertThat(allResults).hasSize(3); // store header, records, indexes
            assertResultKeyCountEquals(allResults.get(0), 0, 1); // header
            assertResultKeyCountEquals(allResults.get(1), 1, recordCount * 2); // records and versions
            assertResultKeyCountLargerThan(allResults.get(2), 2, 0); // indexes

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    void calcSizeWithAggregationDepthTwo() throws Exception {
        final int recordCount = 1_000;
        populateStore(recordCount, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            List<SizeStatisticsGroupedResults> allResults = getStoreCursor(recordStore, ScanProperties.FORWARD_SCAN, null, 2).asList().join();

            assertThat(allResults).hasSize(1006); // Fragile

            assertThat(allResults.get(0))
                    .matches(result -> result.getAggregationKey().equals(Tuple.from(0)))
                    .matches(result -> assertResultKeyCountEquals(result, 0, 1)); // header

            List<SizeStatisticsGroupedResults> recordStats = allResults.stream()
                    .filter(result -> result.getAggregationKey().getLong(0) == 1).collect(Collectors.toList()); // all entries for records have "1" as the first element of the tuple
            assertThat(recordStats).hasSize(recordCount);
            assertThat(recordStats)
                    .allMatch(result -> result.getAggregationKey().size() == 2) // all key tuples are of size 2
                    .allMatch(result -> assertResultKeyCountEquals(result, 1, 2)); // each record has 2 entries (one for version)

            List<SizeStatisticsGroupedResults> indexStats = allResults.stream()
                    .filter(result -> result.getAggregationKey().getLong(0) == 2).collect(Collectors.toList()); // all entries for index have "2" as the first element of the tuple
            assertThat(indexStats).hasSize(5); // 5 indexes on the store
            assertThat(indexStats)
                    .allMatch(result -> result.getAggregationKey().size() == 2) // all key tuples are of size 2
                    .allMatch(result ->
                            !(result.getAggregationKey().getString(1).contains("value")) ||
                                    (result.getStats().getKeyCount() == recordCount)); // value indexes contain all records

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    void calcSizeWithAggregationDepthUnlimited() throws Exception {
        final int recordCount = 1_000;
        populateStore(recordCount, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            List<SizeStatisticsGroupedResults> allResults = getStoreCursor(recordStore, ScanProperties.FORWARD_SCAN, null, 1000).asList().join();

            assertThat(allResults.size()).isEqualTo(5003); // Fragile

            assertThat(allResults.get(0))
                    .matches(result -> result.getAggregationKey().equals(Tuple.from(0)))
                    .matches(result -> assertResultKeyCountEquals(allResults.get(0), 0, 1)); // header

            List<SizeStatisticsGroupedResults> recordStats = allResults.stream()
                    .filter(result -> result.getAggregationKey().getLong(0) == 1).collect(Collectors.toList()); // all entries for records have "1" as the first element of the tuple
            assertThat(recordStats).hasSize(recordCount * 2); // record data and version
            assertThat(recordStats)
                    .allMatch(result -> result.getAggregationKey().size() == 3) // all key tuples are of size 3
                    .allMatch(result -> (result.getAggregationKey().getLong(2) == 0) || (result.getAggregationKey().getLong(2) == -1)) // split suffix is either 0 or -1)
                    .allMatch(result -> assertResultKeyCountEquals(result, 1, 1)); // single entry per stat

            List<SizeStatisticsGroupedResults> indexStats = allResults.stream()
                    .filter(result -> result.getAggregationKey().getLong(0) == 2).collect(Collectors.toList()); // all entries for index have "2" as the first element of the tuple
            assertThat(indexStats).hasSizeGreaterThan(recordCount);

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    void emptyStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long exactStoreSize = getStoreTotalSize(recordStore);
            long exactRecordsSize = getRecordsTotalSize(recordStore);
            long exactIndexSize = getIndexTotalSize(recordStore, "MySimpleRecord$num_value_3_indexed");

            assertThat(exactStoreSize).isGreaterThan(0); // header
            assertThat(exactRecordsSize).isZero();
            assertThat(exactIndexSize).isZero();

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Test
    void calcSizeRecordsOnly() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long totalRecordSize = getRecordsTotalSize(recordStore);
            assertThat(totalRecordSize).isGreaterThan(0);

            // calculate the size again, this time using a large aggregation depth
            final List<SizeStatisticsGroupedResults> results = getAllResults(getRecordsCursor(recordStore, ScanProperties.FORWARD_SCAN, null, 1000));
            assertThat(results.size()).isEqualTo(recordCount * 2);
            assertThat(totalRecordSize).isEqualTo(getTotalSize(results));

            commit(context);
        }
    }

    @Test
    void calcSizeSpecificIndex() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long totalIndexSize = getIndexTotalSize(recordStore, "MySimpleRecord$num_value_3_indexed");
            assertThat(totalIndexSize).isGreaterThan(0);

            // calculate the size again, this time using a large aggregation depth
            final List<SizeStatisticsGroupedResults> results = getAllResults(getIndexCursor(recordStore, "MySimpleRecord$num_value_3_indexed", ScanProperties.FORWARD_SCAN, null, 1000));
            assertThat(results.size()).isEqualTo(recordCount); // all records are indexed
            assertThat(totalIndexSize).isEqualTo(getTotalSize(results));

            commit(context);
        }
    }

    @Test
    void calcSizeWithReverseScan() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ScanProperties reverseScanProperties = ScanProperties.FORWARD_SCAN.setReverse(true);
            final long reverse = getTotalSize(getAllResults(getStoreCursor(recordStore, reverseScanProperties, null, 0)));
            final long forward = getTotalSize(getAllResults(getStoreCursor(recordStore, ScanProperties.FORWARD_SCAN, null, 0)));

            assertThat(reverse).isEqualTo(forward);
            commit(context);
        }
    }

    @Test
    void invalidContinuation() throws Exception {
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
    @Disabled("Issue #3615")
    void cursorCloseBeforeOnNext() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsGroupingCursor cursor = SizeStatisticsGroupingCursor.ofStore(recordStore, context, ScanProperties.FORWARD_SCAN, null, 100);

            assertThat(cursor.isClosed()).isFalse();
            cursor.close();
            assertThat(cursor.isClosed()).isTrue();
            Assertions.assertThatThrownBy(() -> cursor.onNext().join()).hasCauseInstanceOf(CancellationException.class);

            commit(context);
        }
    }

    @Test
    void cursorCloseBehavior() throws Exception {
        final int recordCount = 100;
        populateStore(recordCount, 50_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsGroupingCursor cursor = SizeStatisticsGroupingCursor.ofStore(recordStore, context, ScanProperties.FORWARD_SCAN, null, 100);

            cursor.onNext().join();
            cursor.close();
            Assertions.assertThatThrownBy(() -> cursor.onNext().join()).hasCauseInstanceOf(CancellationException.class);

            commit(context);
        }
    }

    @Test
    void getExecutorTest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsGroupingCursor cursor = SizeStatisticsGroupingCursor.ofStore(recordStore, context, ScanProperties.FORWARD_SCAN, null, 0);
            assertThat(cursor.getExecutor()).isEqualTo(context.getExecutor());
            commit(context);
        }
    }

    @ParameterizedTest
    @BooleanSource
    void acceptTest(boolean response) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsGroupingCursor cursor = SizeStatisticsGroupingCursor.ofStore(recordStore, context, ScanProperties.FORWARD_SCAN, null, 0);

            TestVisitor visitor = new TestVisitor(response);
            boolean result = cursor.accept(visitor);

            assertThat(visitor.visitEnterCalled).isTrue();
            assertThat(visitor.visitLeaveCalled).isTrue();
            assertThat(result).isEqualTo(response);

            commit(context);
        }
    }

    private long getStoreTotalSize(@Nonnull FDBRecordStore store) {
        return getStoreTotalResult(store).getStats().getTotalSize();
    }

    private SizeStatisticsGroupedResults getStoreTotalResult(@Nonnull FDBRecordStore store) {
        final List<SizeStatisticsGroupedResults> allResults = getAllResults(getStoreCursor(store, ScanProperties.FORWARD_SCAN, null, 0));
        assertThat(allResults).hasSize(1);
        return allResults.get(0);
    }

    private long getRecordsTotalSize(@Nonnull FDBRecordStore store) {
        return getTotalSize(getAllResults(getRecordsCursor(store, ScanProperties.FORWARD_SCAN, null, 0)));
    }

    private long getIndexTotalSize(@Nonnull FDBRecordStore store, @Nonnull String indexName) {
        return getTotalSize(getAllResults(getIndexCursor(store, indexName, ScanProperties.FORWARD_SCAN, null, 0)));
    }

    private long getSubspaceTotalSize(@Nonnull FDBRecordStore store, @Nonnull Subspace subspace) {
        return getTotalSize(getAllResults(getSubspaceCursor(store, subspace, ScanProperties.FORWARD_SCAN, null, 0)));
    }

    private SizeStatisticsGroupingCursor getStoreCursor(@Nonnull FDBRecordStore store, ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        return SizeStatisticsGroupingCursor.ofStore(store, store.getContext(), scanProperties, continuation, aggregationDepth);
    }

    private SizeStatisticsGroupingCursor getRecordsCursor(@Nonnull FDBRecordStore store, ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        return SizeStatisticsGroupingCursor.ofRecords(store, store.getContext(), scanProperties, continuation, aggregationDepth);
    }

    private SizeStatisticsGroupingCursor getIndexCursor(@Nonnull FDBRecordStore store, String indexName, ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        return SizeStatisticsGroupingCursor.ofIndex(store, indexName, store.getContext(), scanProperties, continuation, aggregationDepth);
    }

    private SizeStatisticsGroupingCursor getSubspaceCursor(@Nonnull FDBRecordStore store, Subspace subspace, ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        return SizeStatisticsGroupingCursor.ofSubspace(subspace, store.getContext(), scanProperties, continuation, aggregationDepth);
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getStoreSizeResult(@Nonnull FDBRecordStore store) {
        return getStoreSizeResult(store, ScanProperties.FORWARD_SCAN, null, 0);
    }

    private List<SizeStatisticsGroupedResults> getAllResults(@Nonnull SizeStatisticsGroupingCursor cursor) {
        return cursor.asList().join();
    }

    private long getTotalSize(List<SizeStatisticsGroupedResults> results) {
        return results.stream().mapToLong(result -> result.getStats().getTotalSize()).sum();
    }

    private boolean assertResultKeyCountEquals(SizeStatisticsGroupedResults result, long tupleFirstElement, int count) {
        assertThat(result)
                .matches(res -> res.getAggregationKey().getLong(0) == tupleFirstElement)
                .matches(res -> result.getStats().getKeyCount() == count); // header
        return true;
    }

    private boolean assertResultKeyCountLargerThan(SizeStatisticsGroupedResults result, long tupleFirstElement, int count) {
        assertThat(result)
                .matches(res -> res.getAggregationKey().getLong(0) == tupleFirstElement)
                .matches(res -> result.getStats().getKeyCount() > count); // header
        return true;
    }

    private RecordCursorResult<SizeStatisticsGroupedResults> getStoreSizeResult(@Nonnull FDBRecordStore store, final ScanProperties scanProperties, byte[] continuation, int aggregationDepth) {
        try (SizeStatisticsGroupingCursor statsCursor = SizeStatisticsGroupingCursor.ofStore(store, store.getContext(), scanProperties, continuation, aggregationDepth)) {
            return statsCursor.getNext();
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

    private static class TestVisitor implements RecordCursorVisitor {
        boolean visitEnterCalled = false;
        boolean visitLeaveCalled = false;
        boolean visitLeaveResult;

        public TestVisitor(final boolean visitLeaveResult) {
            this.visitLeaveResult = visitLeaveResult;
        }

        @Override
        public boolean visitEnter(RecordCursor<?> cursor) {
            visitEnterCalled = true;
            return true;
        }

        @Override
        public boolean visitLeave(RecordCursor<?> cursor) {
            visitLeaveCalled = true;
            return visitLeaveResult;
        }
    }
}
