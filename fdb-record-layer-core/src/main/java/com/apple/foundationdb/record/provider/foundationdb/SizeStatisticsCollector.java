/*
 * SizeStatisticsCollector.java
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

import com.apple.foundationdb.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * A class that collects statistics on the keys and values within a record store or within
 * an index. This must perform a full scan of the subspace backing whatever it is collecting
 * statistics on. It should therefore be run relatively sparingly, and it should not be
 * expected to finish within a single transaction. It tracks a continuation internally,
 * so calling {@link #collect(FDBRecordContext, ExecuteProperties)} successively should
 * result in the collector making progress. Note that this class makes no attempt at keeping
 * its results transactional. As a result, if this is run on an actively mutating data set,
 * there are no guarantees that the values it returns were ever actually true for any
 * version in the transaction history. However, as long as the data set is not too volatile,
 * it should produce an approximate answer for the statistics it gives.
 */
@API(API.Status.EXPERIMENTAL)
public class SizeStatisticsCollector {
    @Nonnull
    private final Subspace subspace;
    private long keyCount;
    private long keySize;
    private long maxKeySize;
    private long valueSize;
    private long maxValueSize;
    @Nonnull
    private long[] sizeBuckets;
    @Nullable
    private byte[] continuation;
    private boolean done;

    private SizeStatisticsCollector(@Nonnull Subspace subspace) {
        this.subspace = subspace;
        this.keyCount = 0;
        this.keySize = 0;
        this.maxKeySize = 0;
        this.valueSize = 0;
        this.maxValueSize = 0;
        this.sizeBuckets = new long[Integer.SIZE];
        this.continuation = null;
        this.done = false;
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
     * @return a future that completes to <code>true</code> if this object is done collecting statistics or <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> collectAsync(@Nonnull FDBRecordContext context, @Nonnull ExecuteProperties executeProperties) {
        if (done) {
            return AsyncUtil.READY_TRUE;
        }
        final ScanProperties scanProperties = new ScanProperties(executeProperties)
                .setStreamingMode(CursorStreamingMode.WANT_ALL);
        final KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                .setContext(context)
                .setContinuation(continuation)
                .setScanProperties(scanProperties)
                .build();
        return kvCursor.forEach(kv -> {
            keyCount += 1;
            keySize += kv.getKey().length;
            maxKeySize = Math.max(maxKeySize, kv.getKey().length);
            valueSize += kv.getValue().length;
            maxValueSize = Math.max(maxValueSize, kv.getValue().length);
            int totalSize = kv.getKey().length + kv.getValue().length;
            if (totalSize > 0) {
                sizeBuckets[Integer.SIZE - Integer.numberOfLeadingZeros(totalSize) - 1] += 1;
            }
            continuation = kvCursor.getContinuation();
        }).handle((vignore, err) -> {
            if (err == null) {
                boolean exhausted = kvCursor.getNoNextReason().isSourceExhausted();
                if (!exhausted) {
                    continuation = kvCursor.getContinuation();
                } else {
                    done = true;
                }
                return exhausted;
            } else {
                if (FDBExceptions.isRetriable(err)) {
                    return false;
                } else {
                    throw context.getDatabase().mapAsyncToSyncException(err);
                }
            }
        }).whenComplete((vignore, err) -> kvCursor.close());
    }

    /**
     * Collect statistics about the key and value sizes.
     * This is a blocking variant of {@link #collectAsync(FDBRecordContext, ExecuteProperties)}.
     *
     * @param context the transaction context in which to collect statistics
     * @param executeProperties limits on execution
     * @return <code>true</code> if this object is done collecting statistics or <code>false</code> otherwise
     */
    public boolean collect(@Nonnull FDBRecordContext context, @Nonnull ExecuteProperties executeProperties) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_COLLECT_STATISTICS, collectAsync(context, executeProperties));
    }

    /**
     * Get the number of keys in the requested key range.
     * @return the number of keys
     */
    public long getKeyCount() {
        return keyCount;
    }

    /**
     * Get the total size (in bytes) of all keys in the requested key range.
     * @return the size (in bytes) of the requested keys
     */
    public long getKeySize() {
        return keySize;
    }

    /**
     * Get the total size (in bytes) of all values in the requested key range.
     * @return the size (in bytes) of the requested values
     */
    public long getValueSize() {
        return valueSize;
    }

    /**
     * Get the total size (in bytes) of all keys and values in the requested key range.
     * @return the size (in bytes) of the requested keys and values
     */
    public long getTotalSize() {
        return keySize + valueSize;
    }

    /**
     * Get the size (in bytes) of the largest key in the requested key range.
     * @return the size (in bytes) of the largest key
     */
    public long getMaxKeySize() {
        return maxKeySize;
    }

    /**
     * Get the size (in bytes) of the largest value in the requested key range.
     * @return the size (in bytes) of the largest value
     */
    public long getMaxValueSize() {
        return maxValueSize;
    }

    /**
     * Get the mean size (in bytes) of keys in the requested key range.
     * @return the mean size (in bytes) of all keys
     */
    public double getAverageKeySize() {
        return keySize * 1.0 / keyCount;
    }

    /**
     * Get the mean size (in bytes) of values in the requested key range.
     * @return the mean size (in bytes) of all values
     */
    public double getAverageValueSize() {
        return valueSize * 1.0 / keyCount;
    }

    /**
     * Get the mean size (in bytes) of combined key-value pairs in the requested key range.
     * @return the mean size (in bytes) of all key-value pairs
     */
    public double getAverage() {
        return getTotalSize() * 1.0 / keyCount;
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
    public long[] getSizeBuckets() {
        // Defensively copy this array. It is only 8 * 32 = 256 bytes anyway and this
        // is not performance critical.
        return Arrays.copyOf(sizeBuckets, sizeBuckets.length);
    }

    /**
     * Get an estimate for the size for which the provided proportion of key-value pairs have a combined
     * size that is less than that size. For example, if 0.8 is passed as the proportion, then
     * this gives an estimate for the 80th percentile value. This value is inexact as it must be
     * interpolated from the recorded size distribution.
     *
     * @param proportion the proportion of key-value pairs that should have a size less than the returned size
     * @return an estimate for the size that is consistent with the given proportion
     */
    public double getProportion(double proportion) {
        if (proportion < 0 || proportion >= 1) {
            throw new RecordCoreArgumentException("proportion " + proportion + " outside legal range");
        }
        long target = (long)(keyCount * proportion);
        long soFar = 0L;
        int i = 0;
        while (soFar < target) {
            soFar += sizeBuckets[i];
            i++;
        }
        if (i == 0) {
            return 1;
        }
        // Linearly extrapolate to find a value between bucket limits
        long before = 1L << (i - 1);
        long after = 1L << i;
        long inBucket = sizeBuckets[i - 1];
        return before + (target - soFar + inBucket) * (after - before) * 1.0 / inBucket;
    }

    /**
     * Get an estimate for the size of the median key-value pair.
     *
     * @return an estimate for the median key-value pair
     */
    public double getMedian() {
        return getProportion(0.5);
    }

    /**
     * Get an estimate for the size of the 90th percentile key-value pair.
     *
     * @return an estimate for the size of the 90th percentile key-value pair
     */
    public double getP90() {
        return getProportion(0.90);
    }

    /**
     * Get an estimate for the size of the 95th percentile key-value pair.
     *
     * @return an estimate for the size of the 95th percentile key-value pair
     */
    public double getP95() {
        return getProportion(0.95);
    }

    // Static initializers

    /**
     * Create a statistics collector of all keys used by a given {@link FDBRecordStore}.
     * This includes records, indexes, and other meta-data.
     *
     * @param store the store from which to collect statistics on key and value sizes
     * @return a statistics collector of that store
     */
    @Nonnull
    public static SizeStatisticsCollector ofStore(@Nonnull FDBRecordStore store) {
        return new SizeStatisticsCollector(store.getSubspace());
    }

    /**
     * Create a statistics collector of all records stored within a given {@link FDBRecordStore}.
     * This only looks at the records stored by that store, not any indexes.
     *
     * @param store the store from which to collect statistics on key and value sizes
     * @return a statistics collector of the records of that store
     */
    @Nonnull
    public static SizeStatisticsCollector ofRecords(@Nonnull FDBRecordStore store) {
        return new SizeStatisticsCollector(store.recordsSubspace());
    }

    /**
     * Create a statistics collector of all keys used by index within a given {@link FDBRecordStore}.
     * This includes only the key-value pairs within the index's primary subspace.
     *
     * @param store a store with the given index
     * @param indexName the name of the index to collect statistics on key and value sizes
     * @return a statistics collector of the given index
     */
    @Nonnull
    public static SizeStatisticsCollector ofIndex(@Nonnull FDBRecordStore store, @Nonnull String indexName) {
        final RecordMetaData metaData = store.getRecordMetaData();
        return ofIndex(store, metaData.getIndex(indexName));
    }

    /**
     * Create a statistics collector of all keys used by index within a given {@link FDBRecordStore}.
     * This includes only the key-value pairs within the index's primary subspace.
     *
     * @param store a store with the given index
     * @param index the index to collect statistics on key and value sizes
     * @return a statistics collector of the given index
     */
    @Nonnull
    public static SizeStatisticsCollector ofIndex(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        return new SizeStatisticsCollector(store.indexSubspace(index));
    }

    /**
     * Create a statistics collector of all keys used by index within a given {@link Subspace}.
     *
     * @param subspace the subspace to collect statistics on key and value sizes
     * @return a statistics collector of the given subspace
     */
    @Nonnull
    public static SizeStatisticsCollector ofSubspace(@Nonnull Subspace subspace) {
        return new SizeStatisticsCollector(subspace);
    }

    /**
     * Create a statistics collector of all keys used by index within a given {@link SubspaceProvider}'s subspace.
     * If the implementation of {@link SubspaceProvider#getSubspace() getSubspace()} is blocking for the
     * given <code>SubspaceProvide</code>, then this method will also be blocking.
     *
     * @param subspaceProvider the provider of the subspace to collect statistics on key and value sizes
     * @return a statistics collector of the given subspace
     */
    @Nonnull
    public static SizeStatisticsCollector ofSubspaceProvider(@Nonnull SubspaceProvider subspaceProvider) {
        return new SizeStatisticsCollector(subspaceProvider.getSubspace());
    }

    /**
     * Create a statistics collector of all keys used by index within a given {@link SubspaceProvider}'s subspace.
     * This method is non-blocking, and it returns a future that will contain the statistics collector when
     * ready.
     *
     * @param subspaceProvider the provider of the subspace to collect statistics on key and value sizes
     * @return a future containing the statistics collector of the given subspace
     */
    @Nonnull
    public static CompletableFuture<SizeStatisticsCollector> ofSubspaceProviderAsync(@Nonnull SubspaceProvider subspaceProvider) {
        return subspaceProvider.getSubspaceAsync().thenApply(SizeStatisticsCollector::new);
    }
}
