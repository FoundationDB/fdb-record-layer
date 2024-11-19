/*
 * FDBRecordStoreEstimatedSizeTest.java
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

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.cursors.SizeStatisticsCollectorCursor;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.StringUtils;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the methods on an {@link FDBRecordStore} around estimating the size of a store.
 *
 * <p>
 * Note that as the guarantees of the "estimate size" APIs are relatively weak, these tests mostly assert that the
 * estimate happened without error, though they can occasionally make more sophisticated assertions like "this estimate
 * should be less than this other estimate".
 * </p>
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreEstimateSizeTest extends FDBRecordStoreTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordStoreEstimateSizeTest.class);

    @Test
    public void estimatedSize() throws Exception {
        populateStore(1_000, 1_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long estimatedStoreSize = estimateStoreSize(recordStore);
            long exactStoreSize = getExactStoreSize(recordStore);
            LOGGER.info(KeyValueLogMessage.of("calculated estimated store size",
                    "estimated", estimatedStoreSize,
                    "exact", exactStoreSize));

            long estimatedRecordsSize = estimateRecordsSize(recordStore);
            long exactRecordsSize = getExactRecordsSize(recordStore);
            LOGGER.info(KeyValueLogMessage.of("calculated estimated records size",
                    "estimated", estimatedRecordsSize,
                    "exact", exactRecordsSize));

            assertThat("estimated record size should be less than estimated store size",
                    estimatedRecordsSize, lessThanOrEqualTo(estimatedStoreSize));

            commit(context); // commit as that logs the store timer stats
        }
    }

    @Disabled("too expensive but useful for smoke-checking the performance on large stores")
    @Test
    public void estimateLargeStore() throws Exception {
        populateStore(200_000, 1_000_000_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long estimatedStoreSize = estimateStoreSize(recordStore);
            long estimatedRecordsSize = estimateRecordsSize(recordStore);
            LOGGER.info(KeyValueLogMessage.of("calculated estimated record size on 1 GB store",
                    "estimated_store_size", estimatedStoreSize,
                    "estimated_records_size", estimatedRecordsSize));
            assertThat(estimatedRecordsSize, lessThanOrEqualTo(estimatedStoreSize));

            commit(context);
        }
    }

    @Test
    public void estimateEmptyStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long estimatedStoreSize = estimateStoreSize(recordStore);
            long estimatedRecordsSize = estimateRecordsSize(recordStore);
            LOGGER.info(KeyValueLogMessage.of("estimated size of empty store",
                    "estimated_store_size", estimatedStoreSize,
                    "estimated_record_size", estimatedRecordsSize));
            assertThat(estimatedRecordsSize, lessThanOrEqualTo(estimatedStoreSize));

            commit(context);
        }
    }

    @Test
    public void estimateSingleRecord() throws Exception {
        populateStore(1, 5_000L);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long estimatedStoreSize = estimateStoreSize(recordStore);
            long estimatedRecordsSize = estimateRecordsSize(recordStore);
            LOGGER.info(KeyValueLogMessage.of("estimated size of store with a single record",
                    "estimated_store_size", estimatedStoreSize,
                    "estimated_record_size", estimatedRecordsSize));
            assertThat(estimatedRecordsSize, lessThanOrEqualTo(estimatedStoreSize));

            commit(context);
        }
    }

    @Test
    public void estimateInTwoRanges() throws Exception {
        final int recordCount = 500;
        populateStore(recordCount, recordCount * 5_000);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            long estimatedRecordSize = estimateRecordsSize(recordStore);
            final Tuple halfWayTuple = Tuple.from(recordCount / 2);
            final TupleRange firstHalf = new TupleRange(null, halfWayTuple, EndpointType.TREE_START, EndpointType.RANGE_EXCLUSIVE);
            long estimatedFirstHalf = estimateRecordsSize(recordStore, firstHalf);
            final TupleRange secondHalf = new TupleRange(halfWayTuple, null, EndpointType.RANGE_INCLUSIVE, EndpointType.TREE_END);
            long estimatedSecondHalf = estimateRecordsSize(recordStore, secondHalf);
            LOGGER.info(KeyValueLogMessage.of("estimated both halves of records",
                    "estimated_records_size", estimatedRecordSize,
                    "estimated_first_half_size", estimatedFirstHalf,
                    "estimated_second_half_size", estimatedSecondHalf));

            assertEquals(estimatedFirstHalf + estimatedSecondHalf, estimatedRecordSize,
                    "expected first half size (" + estimatedFirstHalf + ") and second half size (" + estimatedSecondHalf + ") to match full size");

            commit(context);
        }
    }

    private static long estimateStoreSize(@Nonnull FDBRecordStore store) {
        return store.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ESTIMATE_SIZE, store.estimateStoreSizeAsync());
    }

    private static long estimateRecordsSize(@Nonnull FDBRecordStore store) {
        return store.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ESTIMATE_SIZE, store.estimateRecordsSizeAsync());
    }

    private static long estimateRecordsSize(@Nonnull FDBRecordStore store, @Nonnull TupleRange range) {
        return store.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ESTIMATE_SIZE, store.estimateRecordsSizeAsync(range));
    }

    private static long getExactStoreSize(@Nonnull FDBRecordStore store) {
        SizeStatisticsCollectorCursor statsCursor = SizeStatisticsCollectorCursor.ofStore(store, store.getContext(), ScanProperties.FORWARD_SCAN, null);
        return getTotalSize(statsCursor);
    }

    private static long getExactRecordsSize(@Nonnull FDBRecordStore store) {
        SizeStatisticsCollectorCursor statsCursor = SizeStatisticsCollectorCursor.ofRecords(store, store.getContext(), ScanProperties.FORWARD_SCAN, null);
        return getTotalSize(statsCursor);
    }

    private static long getTotalSize(@Nonnull SizeStatisticsCollectorCursor statsCursor) {
        final RecordCursorResult<SizeStatisticsCollectorCursor.SizeStatisticsResults> result = statsCursor.getNext();
        assertTrue(result.hasNext());
        return result.get().getTotalSize();
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
