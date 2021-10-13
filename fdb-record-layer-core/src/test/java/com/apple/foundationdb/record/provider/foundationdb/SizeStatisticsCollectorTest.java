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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.cursors.SizeStatisticsCollectorCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.Float.NaN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
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
            uncheckedOpenSimpleRecordStore(context);
            SizeStatisticsCollector statisticsCollector = new SizeStatisticsCollector(recordStore);
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
            SizeStatisticsCollector statisticsCollector = new SizeStatisticsCollector(recordStore);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE), is(true));
            assertEquals(recordCount * 2, statisticsCollector.getKeyCount());
            assertEquals(keyBytes, statisticsCollector.getKeySize());
            assertEquals(valueBytes, statisticsCollector.getValueSize());
            assertEquals(keyBytes + valueBytes, statisticsCollector.getTotalSize());
            assertEquals(keyBytes * 0.5 / recordCount, statisticsCollector.getAverageKeySize());
            assertEquals(valueBytes * 0.5 / recordCount, statisticsCollector.getAverageValueSize());

            // Batches of 10
            SizeStatisticsCollector batchedCollector = new SizeStatisticsCollector(recordStore);
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
            SizeStatisticsCollector indexCollector = new SizeStatisticsCollector(recordStore, "MySimpleRecord$str_value_indexed");

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
                int indexKeySize = indexSubspaceSize + i + 2 + Tuple.from(i).pack().length;
                keySize += indexKeySize;
                int msb = Integer.SIZE - Integer.numberOfLeadingZeros(indexKeySize) - 1;
                sizeBuckets[msb] += 1;
                keySizes.add(indexKeySize);
            }

            SizeStatisticsCollector indexCollector = new SizeStatisticsCollector(recordStore, "MySimpleRecord$str_value_indexed");
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
            statisticsCollector = new SizeStatisticsCollector(recordStore);
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
            long staleReadVersion = commitVersion - 101_000_000;
            Assumptions.assumeTrue(staleReadVersion > 0, "read versions must always be positive");
            context.setReadVersion(staleReadVersion);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE), is(false));
            assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class, context::commit);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(statisticsCollector.collect(context, ExecuteProperties.newBuilder().setReturnedRowLimit(2 * recordCount - 10).build()), is(false));
            assertThat(statisticsCollector.collect(context, ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()), is(true));

            SizeStatisticsCollector oneShotCollector = new SizeStatisticsCollector(recordStore);
            oneShotCollector.collect(context, ExecuteProperties.SERIAL_EXECUTE);
            assertEquals(oneShotCollector.getKeyCount(), statisticsCollector.getKeyCount());
            assertEquals(oneShotCollector.getKeySize(), statisticsCollector.getKeySize());
            assertEquals(oneShotCollector.getValueSize(), statisticsCollector.getValueSize());
            assertArrayEquals(oneShotCollector.getSizeBuckets(), statisticsCollector.getSizeBuckets());

            commit(context);
        }
    }


    /**
     * A class that collects statistics on the keys and values within a record store or within
     * an index using {@link SizeStatisticsCollectorCursor}. It tracks a continuation internally,
     * so calling {@link #collect(FDBRecordContext, ExecuteProperties)} successively should
     * result in the collector making progress.
     */
    private class SizeStatisticsCollector {
        @Nonnull
        private SubspaceProvider subspaceProvider;
        @Nonnull
        private Optional<SizeStatisticsCollectorCursor.SizeStatisticsResults> sizeStatsResults;
        @Nullable
        private RecordCursorContinuation continuation;

        private SizeStatisticsCollector(@Nonnull SubspaceProvider subspaceProvider) {
            this.subspaceProvider = subspaceProvider;
            this.continuation = RecordCursorStartContinuation.START;
            this.sizeStatsResults = Optional.empty();
        }

        /**
         * Create a statistics collector of all keys used by a given {@link FDBRecordStore}.
         * This includes records, indexes, and other meta-data.
         *
         * @param store the store from which to collect statistics on key and value sizes
         *
         * @return a statistics collector of that store
         */
        @Nonnull
        private SizeStatisticsCollector(@Nonnull FDBRecordStore store) {
            this(new SubspaceProviderBySubspace(store.recordsSubspace()));
        }

        /**
         * Create a statistics collector of all keys used by index within a given {@link FDBRecordStore}.
         * This includes only the key-value pairs within the index's primary subspace.
         *
         * @param store a store with the given index
         * @param indexName the name of the index to collect statistics on key and value sizes
         *
         * @return a statistics collector of the given index
         */
        @Nonnull
        private SizeStatisticsCollector(@Nonnull FDBRecordStore store, @Nonnull String indexName) {
            this(store, store.getRecordMetaData().getIndex(indexName));
        }

        /**
         * Create a statistics collector of all keys used by index within a given {@link FDBRecordStore}.
         * This includes only the key-value pairs within the index's primary subspace.
         *
         * @param store a store with the given index
         * @param index the index to collect statistics on key and value sizes
         *
         * @return a statistics collector of the given index
         */
        @Nonnull
        private SizeStatisticsCollector(@Nonnull FDBRecordStore store, @Nonnull Index index) {
            this(new SubspaceProviderBySubspace(store.indexSubspace(index)));
        }

        /**
         * Create a statistics collector of all keys used by index within a given {@link Subspace}.
         *
         * @param subspace the subspace to collect statistics on key and value sizes
         *
         * @return a statistics collector of the given subspace
         */
        @Nonnull
        private SizeStatisticsCollector(@Nonnull Subspace subspace) {
            this(new SubspaceProviderBySubspace(subspace));
        }

        /**
         * Collect statistics about the key and value sizes.
         * This will pick up from where this object previously left off so that no key should be included
         * in the collected statistics twice. Typically, the user should specify some limit through the
         * <code>executeProperties</code> parameter. These properties will then be applied to a scan
         * of the database, and the key and value sizes for each key will be recorded. If this collector
         * is done collecting statistics (i.e., if there are no more keys in the range of keys that
         * it was tasked to collect statistics on), then this method will return a future that completes
         * to <code>true</code>. Otherwise, this function will return a future that completes to <code>false</code>.
         *
         * @param context the transaction context in which to collect statistics
         * @param executeProperties limits on execution
         *
         * @return a future that completes to <code>true</code> if this object is done collecting statistics or
         * <code>false</code> otherwise
         */
        @Nonnull
        private CompletableFuture<Boolean> collectAsync(@Nonnull FDBRecordContext context, @Nonnull ExecuteProperties executeProperties) {
            if (continuation.isEnd()) {
                return AsyncUtil.READY_TRUE;
            }
            return subspaceProvider.getSubspaceAsync(context).thenCompose(subspace -> {
                final ScanProperties scanProperties = new ScanProperties(executeProperties)
                        .setStreamingMode(CursorStreamingMode.WANT_ALL);
                final SizeStatisticsCollectorCursor statsCursor = SizeStatisticsCollectorCursor.ofSubspace(subspace, context, scanProperties, continuation.toBytes());

                return statsCursor.forEachResult(nextResult -> {
                    sizeStatsResults = Optional.of(nextResult.get()); //wholesale replacement of initialized version with fully aggregated results
                    continuation = nextResult.getContinuation();
                }).handle((result, err) -> {
                    if (err == null) {
                        continuation = result.getContinuation();
                        return continuation.isEnd();
                    } else {
                        if (FDBExceptions.isRetriable(err)) {
                            return false;
                        } else {
                            throw context.getDatabase().mapAsyncToSyncException(err);
                        }
                    }
                }).whenComplete((ignore, err) -> statsCursor.close());
            });
        }

        /**
         * Collect statistics about the key and value sizes.
         * This is a blocking variant of {@link #collectAsync(FDBRecordContext, ExecuteProperties)}.
         *
         * @param context the transaction context in which to collect statistics
         * @param executeProperties limits on execution
         *
         * @return <code>true</code> if this object is done collecting statistics or <code>false</code> otherwise
         */
        private boolean collect(@Nonnull FDBRecordContext context, @Nonnull ExecuteProperties executeProperties) {
            return context.asyncToSync(FDBStoreTimer.Waits.WAIT_COLLECT_STATISTICS, collectAsync(context, executeProperties));
        }

        /**
         * Get the number of keys in the requested key range.
         *
         * @return the number of keys
         */
        private long getKeyCount() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getKeyCount()).orElse(0L);
        }

        /**
         * Get the total size (in bytes) of all keys in the requested key range.
         *
         * @return the size (in bytes) of the requested keys
         */
        private long getKeySize() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getKeySize()).orElse(0L);
        }

        /**
         * Get the total size (in bytes) of all values in the requested key range.
         *
         * @return the size (in bytes) of the requested values
         */
        private long getValueSize() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getValueSize()).orElse(0L);
        }

        /**
         * Get the total size (in bytes) of all keys and values in the requested key range.
         *
         * @return the size (in bytes) of the requested keys and values
         */
        private long getTotalSize() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getTotalSize()).orElse(0L);
        }

        /**
         * Get the size (in bytes) of the largest key in the requested key range.
         *
         * @return the size (in bytes) of the largest key
         */
        private long getMaxKeySize() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getMaxKeySize()).orElse(0L);
        }

        /**
         * Get the size (in bytes) of the largest value in the requested key range.
         *
         * @return the size (in bytes) of the largest value
         */
        private long getMaxValueSize() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getMaxValueSize()).orElse(0L);
        }

        /**
         * Get the mean size (in bytes) of keys in the requested key range.
         *
         * @return the mean size (in bytes) of all keys
         */
        private double getAverageKeySize() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getAverageKeySize()).orElse((double)NaN);
        }

        /**
         * Get the mean size (in bytes) of values in the requested key range.
         *
         * @return the mean size (in bytes) of all values
         */
        private double getAverageValueSize() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getAverageValueSize()).orElse((double)NaN);
        }

        /**
         * Get the mean size (in bytes) of combined key-value pairs in the requested key range.
         *
         * @return the mean size (in bytes) of all key-value pairs
         */
        private double getAverage() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getAverage()).orElse((double)NaN);
        }

        /**
         * Get an array of buckets used to get an estimate of size distribution.
         * Each bucket <i>i</i> contains the number of key-value pairs whose combined size is
         * between greater than or equal to 2<sup><i>i</i></sup> and less than 2<sup><i>i</i> + 1</sup>.
         * In other words, bucket <i>i</i> contains the number of key-value pairs where the
         * combined size's most significant bit was bit <i>i</i> (numbering from the least
         * significant bit and indexing from zero).
         *
         * @return an array with a distribution of the sizes of key-value pairs
         */
        @Nonnull
        private long[] getSizeBuckets() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getSizeBuckets()).orElse(new long[Integer.SIZE]);
        }

        /**
         * Get an estimate for the size for which the provided proportion of key-value pairs have a combined
         * size that is less than that size. For example, if 0.8 is passed as the proportion, then
         * this gives an estimate for the 80th percentile value. This value is inexact as it must be
         * interpolated from the recorded size distribution.
         *
         * @param proportion the proportion of key-value pairs that should have a size less than the returned size
         *
         * @return an estimate for the size that is consistent with the given proportion
         */
        private double getProportion(double proportion) {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getProportion(proportion)).orElse(1.0);
        }

        /**
         * Get an estimate for the size of the median key-value pair.
         *
         * @return an estimate for the median key-value pair
         */
        private double getMedian() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getMedian()).orElse(1.0);
        }

        /**
         * Get an estimate for the size of the 90th percentile key-value pair.
         *
         * @return an estimate for the size of the 90th percentile key-value pair
         */
        private double getP90() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getP90()).orElse(1.0);
        }

        /**
         * Get an estimate for the size of the 95th percentile key-value pair.
         *
         * @return an estimate for the size of the 95th percentile key-value pair
         */
        private double getP95() {
            return sizeStatsResults.map(sizeStatsResults -> sizeStatsResults.getP95()).orElse(1.0);
        }
    }

}
