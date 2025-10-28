/*
 * SizeStatisticsCollectorCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProviderBySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A cursor that collects various distribution statistics on the keys and values within a record store or within
 * an index. The cursor performs a full scan of the subspace backing whatever it is collecting statistics on. It
 * should therefore be run relatively sparingly, and it should not be expected to finish within a single transaction.
 * Note that this class makes no attempt at keeping its results transactional. As a result, if this is run on an
 * actively mutating data set, there are no guarantees that the values it returns were ever actually true for any
 * version in the
 * transaction history. However, as long as the data set is not too volatile, it should produce an approximate answer
 * for the statistics it yields.
 * <p>
 * Note that the cursor emits only a single cursor result containing a value. That value represents full statistics
 * for all keys and values in the specified subspace. Any cursor results emitted prior to that are a manifestation of
 * hitting an execution limit while scanning the subspace. These results provide a continuation that can be used
 * to resume aggregating the remaining keys and values of the subspace.
 * Deprecated, use the {@link SizeStatisticsGroupingCursor} instead.
 */
@API(API.Status.DEPRECATED)
public class SizeStatisticsCollectorCursor implements RecordCursor<SizeStatisticsCollectorCursor.SizeStatisticsResults> {
    @Nonnull
    private final SubspaceProvider subspaceProvider;
    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final ScanProperties scanProperties;
    @Nullable
    private RecordCursorResult<SizeStatisticsResults> nextStatsResult;
    private boolean finalResultsEmitted;
    @Nullable
    private byte[] kvCursorContinuation;
    @Nonnull
    private SizeStatisticsResults sizeStatisticsResults;  //the final output of the cursor
    private boolean closed;

    private SizeStatisticsCollectorCursor(@Nonnull SubspaceProvider subspaceProvider, @Nonnull FDBRecordContext context,
                                          @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation) {
        this.subspaceProvider = subspaceProvider;
        this.sizeStatisticsResults = new SizeStatisticsResults();
        this.context = context;
        this.scanProperties = scanProperties;
        this.finalResultsEmitted = false;
        this.kvCursorContinuation = null;
        this.nextStatsResult = null;
        this.closed = false;

        if (continuation != null) {
            try {
                // if this is a continuation update stats with partial values and get the underlying cursor's continuation
                RecordCursorProto.SizeStatisticsContinuation statsContinuation = RecordCursorProto.SizeStatisticsContinuation.parseFrom(continuation);
                if (statsContinuation.hasPartialResults()) {
                    this.sizeStatisticsResults.fromProto(statsContinuation.getPartialResults());
                    kvCursorContinuation = statsContinuation.getContinuation().toByteArray(); //underlying KV cursor continues here
                } else {
                    // final results were returned on a previous iteration and the caller inexplicably decided to reset the cursor before calling onNext() once more
                    // so this is tantamount to reset the cursor after we have already sent the last result (in our case the last and only result)
                    this.finalResultsEmitted = true;
                }
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("Error parsing SizeStatisticsCollectorCursor continuation", ex)
                        .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuation));
            }
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<SizeStatisticsResults>> onNext() {
        //Each time this method is called it instantiates the underlying KV cursor, aggregates as much of its input as it can provide, and closes it
        //Either the fully aggregated result is returned, or no value and a continuation comprised of partially aggregated results is returned

        //the final aggregated results were emitted on the last call so we are done aggregating the underlying cursor
        if (finalResultsEmitted) {
            return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
        }

        //if this cursor instance has previously hit a limit then it needs to be reconstituted with a continuation to advance aggregation
        if (nextStatsResult != null && !nextStatsResult.hasNext()) {
            return CompletableFuture.completedFuture(nextStatsResult);
        }

        return subspaceProvider.getSubspaceAsync(context).thenCompose(subspace -> {
            KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(subspace).setContext(context).setContinuation(kvCursorContinuation).setScanProperties(scanProperties).build();
            return kvCursor.forEachResult(nextKv -> {
                sizeStatisticsResults.updateStatistics(nextKv.get());
            }).thenApply(resultKv -> {
                //resultKV.hasNext() is false and so determine whether this is because of in-band reason or a continuable out-of-band reason
                if (resultKv.getNoNextReason() == NoNextReason.SOURCE_EXHAUSTED) {
                    finalResultsEmitted = true; //the continuation computation is using this so set it here
                    nextStatsResult = RecordCursorResult.withNextValue(sizeStatisticsResults, new SizeStatisticsCollectorCursorContinuation(resultKv, sizeStatisticsResults, finalResultsEmitted));
                } else {
                    //the underlying cursor did not produce a row but there are more, return a continuation and propagate the underlying no next reason
                    nextStatsResult = RecordCursorResult.withoutNextValue(new SizeStatisticsCollectorCursorContinuation(resultKv, sizeStatisticsResults, finalResultsEmitted), resultKv.getNoNextReason());
                    kvCursorContinuation = resultKv.getContinuation().toBytes(); //not strictly needed because we prevent advancing cursor if nextStatsResult is not null
                }
                return nextStatsResult;
            }).whenComplete((ignore, err) -> kvCursor.close()); //no need to keep it open as it must be reconstituted with a continuation later
        });
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return context.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    //form a continuation that allows us to restart statistics aggregation from where we left off
    // Note that this continuation SHOULD NOT be used to represent an end continuation
    private static class SizeStatisticsCollectorCursorContinuation implements RecordCursorContinuation {
        @Nonnull
        private final RecordCursorResult<KeyValue> currentKvResult;
        @Nonnull
        private final Function<ByteString, RecordCursorProto.SizeStatisticsContinuation> continuationFunction;
        @Nonnull
        private final SizeStatisticsResults sizeStatisticsResults;
        @Nullable
        private byte[] cachedBytes;
        @Nullable
        private ByteString cachedByteString;
        private boolean finalResultsEmitted;

        private SizeStatisticsCollectorCursorContinuation(RecordCursorResult<KeyValue> currentKvResult, SizeStatisticsResults sizeStatisticsResults, boolean finalResultsEmitted) {
            this.cachedBytes = null;
            this.currentKvResult = currentKvResult;
            this.sizeStatisticsResults = sizeStatisticsResults.copy(); //cache an immutable snapshot of the partial aggregate state
            this.finalResultsEmitted = finalResultsEmitted;

            //defer forming bytes until requested
            continuationFunction = b -> {
                if (this.finalResultsEmitted == false) {
                    return RecordCursorProto.SizeStatisticsContinuation.newBuilder().setPartialResults(this.sizeStatisticsResults.toProto()).setContinuation(b).build();
                } else {
                    //nothing more to aggregate
                    return RecordCursorProto.SizeStatisticsContinuation.getDefaultInstance();
                }
            };
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            // form bytes exactly once
            if (this.cachedBytes == null) {
                this.cachedBytes = toByteString().toByteArray();
            }
            return cachedBytes;
        }

        @Override
        @Nonnull
        public ByteString toByteString() {
            if (this.cachedByteString == null) {
                this.cachedByteString = continuationFunction.apply(this.currentKvResult.getContinuation().toByteString()).toByteString();
            }
            return cachedByteString;
        }

        @Override
        public boolean isEnd() {
            // instances of this class are NOT meant to represent end continuations
            return false;
        }
    }

    // Static initializers

    /**
     * Create a statistics collector cursor of all keys used by a given {@link FDBRecordStore}.
     * This includes records, indexes, and other meta-data.
     *
     * @param store the store from which to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a cursor for collecting statistics of that store
     */
    @Nonnull
    public static SizeStatisticsCollectorCursor ofStore(@Nonnull FDBRecordStore store, @Nonnull FDBRecordContext context, @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation) {
        return new SizeStatisticsCollectorCursor(store.getSubspaceProvider(), context, scanProperties, continuation);
    }

    /**
     * Create a statistics collector cursor of all records stored within a given {@link FDBRecordStore}.
     * This only looks at the records stored by that store, not any indexes.
     *
     * @param store the store from which to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the records of that store
     */
    @Nonnull
    public static SizeStatisticsCollectorCursor ofRecords(@Nonnull FDBRecordStore store, @Nonnull FDBRecordContext context,
                                                          @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation) {
        return new SizeStatisticsCollectorCursor(new SubspaceProviderBySubspace(store.recordsSubspace()), context, scanProperties, continuation);
    }

    /**
     * Create a statistics collector cursor of all keys used by index within a given {@link FDBRecordStore}.
     * This includes only the key-value pairs within the index's primary subspace.
     *
     * @param store a store with the given index
     * @param indexName the name of the index to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the given index
     */
    @Nonnull
    public static SizeStatisticsCollectorCursor ofIndex(@Nonnull FDBRecordStore store, @Nonnull String indexName, @Nonnull FDBRecordContext context,
                                                        @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation) {
        final RecordMetaData metaData = store.getRecordMetaData();
        return ofIndex(store, metaData.getIndex(indexName), context, scanProperties, continuation);
    }

    /**
     * Create a statistics collector cursor of all keys used by index within a given {@link FDBRecordStore}.
     * This includes only the key-value pairs within the index's primary subspace.
     *
     * @param store a store with the given index
     * @param index the index to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the given index
     */
    @Nonnull
    public static SizeStatisticsCollectorCursor ofIndex(@Nonnull FDBRecordStore store, @Nonnull Index index, @Nonnull FDBRecordContext context,
                                                        @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation) {
        return new SizeStatisticsCollectorCursor(new SubspaceProviderBySubspace(store.indexSubspace(index)), context, scanProperties, continuation);
    }

    /**
     * Create a statistics collector cursor of all keys used by index within a given {@link Subspace}.
     *
     * @param subspace the subspace to collect statistics on key and value sizes
     * @param context the transaction context under which stats collection occurs
     * @param scanProperties scan direction, limits, etc. under which underlying record store scan is performed
     * @param continuation the starting cursor location to start stats collection
     *
     * @return a statistics collector of the given subspace
     */
    @Nonnull
    public static SizeStatisticsCollectorCursor ofSubspace(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context,
                                                           @Nonnull ScanProperties scanProperties, @Nullable byte[] continuation) {
        return new SizeStatisticsCollectorCursor(new SubspaceProviderBySubspace(subspace), context, scanProperties, continuation);
    }

    /**
     * Encapsulates the distribution statistics returned by a SizeStatisticsCollectorCursor.
     * This is deprecated. Use the {@link com.apple.foundationdb.record.provider.foundationdb.cursors.SizeStatisticsResults} instead
     */
    @API(API.Status.DEPRECATED)
    public static class SizeStatisticsResults {
        private long keyCount;
        private long keySize;
        private long maxKeySize;
        private long valueSize;
        private long maxValueSize;
        private long[] sizeBuckets;

        private SizeStatisticsResults() {
            this.keyCount = 0;
            this.keySize = 0;
            this.maxKeySize = 0;
            this.valueSize = 0;
            this.maxValueSize = 0;
            this.sizeBuckets = new long[Integer.SIZE];
        }

        /**
         * Update partial kv stats.
         *
         * @param kv latest key and value for partial stats update
         */
        private void updateStatistics(@Nonnull KeyValue kv) {
            this.keyCount += 1;
            this.keySize += kv.getKey().length;
            this.maxKeySize = Math.max(this.maxKeySize, kv.getKey().length);
            this.valueSize += kv.getValue().length;
            this.maxValueSize = Math.max(this.maxValueSize, kv.getValue().length);
            int totalSize = kv.getKey().length + kv.getValue().length;
            if (totalSize > 0) {
                this.sizeBuckets[Integer.SIZE - Integer.numberOfLeadingZeros(totalSize) - 1] += 1;
            }
        }

        /**
         * Sets partial kv stats from a protobuf serialization of partial stats results.
         *
         * @param partialResults partial kv stats from a protobuf serialization of partial stats results.
         */
        private void fromProto(RecordCursorProto.SizeStatisticsPartialResults partialResults) {
            this.keyCount = partialResults.getKeyCount();
            this.keySize = partialResults.getKeySize();
            this.maxKeySize = partialResults.getMaxKeySize();
            this.valueSize = partialResults.getValueSize();
            this.maxValueSize = partialResults.getMaxValueSize();
            List<Long> bucketList = partialResults.getSizeBucketsList();
            Arrays.setAll(sizeBuckets, i -> bucketList.get(i));
        }

        /**
         * Gets protobuf representation of partial stats.
         *
         * @return protobuf representation of partial stats.
         */
        private RecordCursorProto.SizeStatisticsPartialResults toProto() {
            RecordCursorProto.SizeStatisticsPartialResults.Builder pr;
            pr = RecordCursorProto.SizeStatisticsPartialResults.newBuilder().setKeyCount(this.getKeyCount()).setKeySize(this.getKeySize()).setMaxKeySize(this.getMaxKeySize()).setValueSize(this.getValueSize()).setMaxValueSize(this.getMaxValueSize());
            List<Long> bucketList = Arrays.stream(this.getSizeBuckets()).boxed().collect(Collectors.toList());
            pr.addAllSizeBuckets(bucketList);
            return pr.build();
        }

        /**
         * Return a copy of this instance.
         *
         * @return a copy of this instance
         */
        private SizeStatisticsResults copy() {
            SizeStatisticsResults target = new SizeStatisticsResults();
            target.setKeyCount(this.getKeyCount());
            target.setKeySize(this.getKeySize());
            target.setMaxKeySize(this.getMaxKeySize());
            target.setValueSize(this.getValueSize());
            target.setMaxValueSize(this.getMaxValueSize());
            target.setSizeBuckets(this.getSizeBuckets());
            return target;
        }

        /**
         * Set the total number of keys in the requested key range.
         *
         * @param keyCount total number of keys in the requested key range.
         */
        private void setKeyCount(long keyCount) {
            this.keyCount = keyCount;
        }

        /**
         * Set the total size (in bytes) of all keys in the requested key range.
         *
         * @param keySize total size (in bytes) of all keys in the requested key range.
         */
        private void setKeySize(long keySize) {
            this.keySize = keySize;
        }

        /**
         * Set the total size (in bytes) of all values in the requested key range.
         *
         * @param valueSize the total size (in bytes) of all values in the requested key range.
         */
        private void setValueSize(long valueSize) {
            this.valueSize = valueSize;
        }

        /**
         * Set the size (in bytes) of the largest key in the requested key range.
         *
         * @param maxKeySize the size (in bytes) of the largest key in the requested key range.
         */
        private void setMaxKeySize(long maxKeySize) {
            this.maxKeySize = maxKeySize;
        }

        /**
         * Set the size (in bytes) of the largest value in the requested key range.
         *
         * @param maxValueSize the size (in bytes) of the largest key in the requested key range.
         */
        private void setMaxValueSize(long maxValueSize) {
            this.maxValueSize = maxValueSize;
        }

        /**
         * Set an array of buckets used to get an estimate of size distribution.
         * Each bucket <i>i</i> contains the number of key-value pairs whose combined size is
         * between greater than or equal to 2<sup><i>i</i></sup> and less than 2<sup><i>i</i> + 1</sup>.
         * In other words, bucket <i>i</i> contains the number of key-value pairs where the
         * combined size's most significant bit was bit <i>i</i> (numbering from the least
         * significant bit and indexing from zero).
         *
         * @param sizeBuckets an array with a distribution of the sizes of key-value pairs
         */
        private void setSizeBuckets(long[] sizeBuckets) {
            // Defensively copy this array. It is only 8 * 32 = 256 bytes anyway and this
            // is not performance critical.
            this.sizeBuckets = Arrays.copyOf(sizeBuckets, sizeBuckets.length);
        }

        /**
         * Get the total number of keys in the requested key range.
         *
         * @return keyCount the total number of keys in the requested key range.
         */
        public long getKeyCount() {
            return keyCount;
        }

        /**
         * Get the total size (in bytes) of all keys in the requested key range.
         *
         * @return the size (in bytes) of the requested keys
         */
        public long getKeySize() {
            return keySize;
        }

        /**
         * Get the size (in bytes) of the largest key in the requested key range.
         *
         * @return the size (in bytes) of the largest key
         */
        public long getMaxKeySize() {
            return maxKeySize;
        }

        /**
         * Get the total size (in bytes) of all values in the requested key range.
         *
         * @return the size (in bytes) of the requested values
         */
        public long getValueSize() {
            return valueSize;
        }

        /**
         * Get the size (in bytes) of the largest value in the requested key range.
         *
         * @return the size (in bytes) of the largest value
         */
        public long getMaxValueSize() {
            return maxValueSize;
        }

        /**
         * Get the total size (in bytes) of all keys and values in the requested key range.
         *
         * @return the size (in bytes) of the requested keys and values
         */
        public long getTotalSize() {
            return keySize + valueSize;
        }

        /**
         * Get the mean size (in bytes) of keys in the requested key range.
         *
         * @return the mean size (in bytes) of all keys
         */
        public double getAverageKeySize() {
            return keySize * 1.0 / keyCount;
        }

        /**
         * Get the mean size (in bytes) of values in the requested key range.
         *
         * @return the mean size (in bytes) of all values
         */
        public double getAverageValueSize() {
            return valueSize * 1.0 / keyCount;
        }

        /**
         * Get the mean size (in bytes) of combined key-value pairs in the requested key range.
         *
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
         *
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

    }

}
