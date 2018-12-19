/*
 * SizeStatisticsCollectorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of the {@link SizeStatisticsCollector}.
 */
@Tag(Tags.RequiresFDB)
public class SizeStatisticsCollectorTest extends FDBRecordStoreTestBase {

    @Test
    public void empty() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsCollector statisticsCollector = SizeStatisticsCollector.ofStore(recordStore);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE), is(true));
            assertEquals(0L, statisticsCollector.getKeyCount());
            assertEquals(0L, statisticsCollector.getKeySize());
            assertEquals(0L, statisticsCollector.getValueSize());
            commit(context);
        }
    }

    @Test
    public void records100() throws Exception {
        final int recordCount = 100;
        final int keyBytes;
        final int valueBytes;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int i = 0; i < recordCount; i++) {
                MySimpleRecord simpleRecord = MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setStrValueIndexed(i % 2 == 0 ? "even" : "odd")
                        .build();
                recordStore.saveRecord(simpleRecord);
            }
            keyBytes = recordStore.getTimer().getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY_BYTES);
            valueBytes = recordStore.getTimer().getCount(FDBStoreTimer.Counts.SAVE_RECORD_VALUE_BYTES);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsCollector statisticsCollector = SizeStatisticsCollector.ofRecords(recordStore);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE), is(true));
            assertEquals(recordCount * 2, statisticsCollector.getKeyCount());
            assertEquals(keyBytes, statisticsCollector.getKeySize());
            assertEquals(valueBytes, statisticsCollector.getValueSize());
            assertEquals(keyBytes + valueBytes, statisticsCollector.getTotalSize());
            assertEquals(keyBytes * 0.5 / recordCount, statisticsCollector.getAverageKeySize());
            assertEquals(valueBytes * 0.5 / recordCount, statisticsCollector.getAverageValueSize());

            // Batches of 10
            SizeStatisticsCollector batchedCollector = SizeStatisticsCollector.ofRecords(recordStore);
            ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setReturnedRowLimit(10).build();
            boolean done = false;
            int iterations = 0;
            while (!done) {
                done = batchedCollector.collect(context, executeProperties);
                iterations += 1;
            }
            assertThat(iterations, anyOf(equalTo(recordCount * 2 / 10), equalTo(recordCount * 2 / 10 + 1)));
            assertEquals(statisticsCollector.getKeyCount(), batchedCollector.getKeyCount());
            assertEquals(statisticsCollector.getKeySize(), batchedCollector.getKeySize());
            assertEquals(statisticsCollector.getValueSize(), batchedCollector.getValueSize());

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            SizeStatisticsCollector indexCollector = SizeStatisticsCollector.ofIndex(recordStore, "MySimpleRecord$str_value_indexed");

            // Batches of 10
            ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setReturnedRowLimit(10).build();
            boolean done = false;
            int iterations = 0;
            while (!done) {
                done = indexCollector.collect(context, executeProperties);
                iterations += 1;
            }
            assertThat(iterations, anyOf(equalTo(recordCount / 10), equalTo(recordCount / 10 + 1)));
            assertEquals(recordCount, indexCollector.getKeyCount());
            assertEquals(0, indexCollector.getValueSize());

            commit(context);
        }
    }

    @Test
    public void indexSize() throws Exception {
        final int recordCount = 100;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            final Subspace indexSubspace = recordStore.indexSubspace(recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"));
            final int indexSubspaceSize = indexSubspace.pack().length;
            long[] sizeBuckets = new long[Integer.SIZE];
            List<Integer> keySizes = new ArrayList<>(recordCount);
            int keySize = 0;
            for (int i = 0; i < recordCount; i++) {
                MySimpleRecord simpleRecord = MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setStrValueIndexed(Strings.repeat("x", i))
                        .build();
                recordStore.saveRecord(simpleRecord);
                // Size contributions from:
                //                 index prefix      + index key (+ overhead) + primary key
                int indexKeySize = indexSubspaceSize + i + 2                  + Tuple.from(i).pack().length;
                keySize += indexKeySize;
                int msb = Integer.SIZE - Integer.numberOfLeadingZeros(indexKeySize) - 1;
                sizeBuckets[msb] += 1;
                keySizes.add(indexKeySize);
            }

            SizeStatisticsCollector indexCollector = SizeStatisticsCollector.ofIndex(recordStore, "MySimpleRecord$str_value_indexed");
            assertThat(indexCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE), is(true));

            assertEquals(keySize, indexCollector.getKeySize());
            assertEquals(0, indexCollector.getValueSize());
            assertEquals(keySize, indexCollector.getTotalSize());
            assertEquals(keySize / (1.0 * recordCount), indexCollector.getAverage());
            assertArrayEquals(sizeBuckets, indexCollector.getSizeBuckets());

            for (double proportion : Arrays.asList(0.2, 0.5, 0.75, 0.90, 0.95)) {
                int realValue = keySizes.get((int)(keySizes.size() * proportion));
                int lowerBound = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(realValue) - 1);
                int upperBound = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(realValue));
                assertThat(indexCollector.getProportion(proportion),
                        allOf(lessThanOrEqualTo((double)upperBound), greaterThanOrEqualTo((double)lowerBound)));
            }
            assertEquals(indexCollector.getProportion(0.5), indexCollector.getMedian());
            assertEquals(indexCollector.getProportion(0.90), indexCollector.getP90());
            assertEquals(indexCollector.getProportion(0.95), indexCollector.getP95());

            commit(context);
        }
    }

    /**
     * Verify that if the collector encounters a retriable error that it gracefully handles it by continuing to work.
     * A better version of this test would throw the error *during* the read, but that's hard, so this just sets the
     * read version to something in the past and then attempts to do a read. This should do nothing, which is then
     * verified.
     *
     * @throws Exception if creating store or committing a context fails
     */
    @Test
    public void tryStaleRead() throws Exception {
        final int recordCount = 100;
        final long commitVersion;
        final SizeStatisticsCollector statisticsCollector;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int i = 0; i < recordCount; i++) {
                MySimpleRecord simpleRecord = MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setStrValueIndexed(i % 2 == 0 ? "even" : "odd")
                        .build();
                recordStore.saveRecord(simpleRecord);
            }
            statisticsCollector = SizeStatisticsCollector.ofRecords(recordStore);
            commit(context);
            commitVersion = context.getCommittedVersion();
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()), is(false));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            // Somewhat leaky, but should get transaction_too_old.
            // One can set knob_max_read_transaction_life_versions up to max_versions_in_flight, which is 100000000.
            context.ensureActive().setReadVersion(commitVersion - 101_000_000);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE), is(false));
            assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class, context::commit);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.newBuilder().setReturnedRowLimit(2 * recordCount - 10).build()), is(false));
            assertThat(statisticsCollector.collect(context, ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()), is(true));

            SizeStatisticsCollector oneShotCollector = SizeStatisticsCollector.ofRecords(recordStore);
            oneShotCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE);
            assertEquals(oneShotCollector.getKeyCount(), statisticsCollector.getKeyCount());
            assertEquals(oneShotCollector.getKeySize(), statisticsCollector.getKeySize());
            assertEquals(oneShotCollector.getValueSize(), statisticsCollector.getValueSize());
            assertArrayEquals(oneShotCollector.getSizeBuckets(), statisticsCollector.getSizeBuckets());

            commit(context);
        }
    }
}
