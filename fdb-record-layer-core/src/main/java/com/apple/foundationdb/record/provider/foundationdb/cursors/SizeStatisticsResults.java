/*
 * SizeStatisticsResults.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursorProto;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Encapsulates the distribution statistics returned by a SizeStatisticsCollectorCursor.
 */
@API(API.Status.EXPERIMENTAL)
public class SizeStatisticsResults {
    private long keyCount;
    private long keySize;
    private long maxKeySize;
    private long valueSize;
    private long maxValueSize;
    private long[] sizeBuckets;

    SizeStatisticsResults() {
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
    public void updateStatistics(@Nonnull KeyValue kv) {
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
    public static SizeStatisticsResults fromProto(RecordCursorProto.SizeStatisticsPartialResults partialResults) {
        SizeStatisticsResults result = new SizeStatisticsResults();
        result.keyCount = partialResults.getKeyCount();
        result.keySize = partialResults.getKeySize();
        result.maxKeySize = partialResults.getMaxKeySize();
        result.valueSize = partialResults.getValueSize();
        result.maxValueSize = partialResults.getMaxValueSize();
        List<Long> bucketList = partialResults.getSizeBucketsList();
        Arrays.setAll(result.sizeBuckets, i -> bucketList.get(i));

        return result;
    }

    /**
     * Gets protobuf representation of partial stats.
     *
     * @return protobuf representation of partial stats.
     */
    public RecordCursorProto.SizeStatisticsPartialResults toProto() {
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
    public SizeStatisticsResults copy() {
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
