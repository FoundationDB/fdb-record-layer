/*
 * MockedLocalityUtil.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * A mocked implementation of the {@link FDBLocalityProvider} interface.
 */
public class MockedLocalityUtil implements FDBLocalityProvider {

    private static class KeyRange {
        @Nonnull
        private byte[] key;
        @Nonnull
        private Tuple tuple;
        private int rangeIndex;

        public KeyRange(@Nonnull byte[] key, @Nonnull Tuple tuple, int rangeIndex) {
            this.key = key;
            this.tuple = tuple;
            this.rangeIndex = rangeIndex;
        }
    }

    private static final MockedLocalityUtil INSTANCE = new MockedLocalityUtil();
    @Nonnull
    private List<KeyRange> keyRanges;
    @Nonnull
    private List<Integer> ranges;

    private MockedLocalityUtil() {
    }

    /**
     * Get the single instance of this utility class.
     * @return the only instance of the class
     */
    @Nonnull
    public static MockedLocalityUtil instance() {
        return INSTANCE;
    }

    /**
     * Get the key of the last range.
     * @return the key of the last range
     */
    public static byte[] getLastRange() {
        return INSTANCE.keyRanges.get(MockedLocalityUtil.instance().ranges.get(MockedLocalityUtil.instance().ranges.size() - 1)).key;
    }

    /**
     * Initialize the {@code MockedLocalityUtil}'s singleton.
     *
     * <p>
     * This is method randomly partitions the sorted list of {@code keys} among {@code rangeCount} servers as if the keys
     * were stored contiguously on those servers. The outcome of this partition is later used by {@link #getBoundaryKeys(Transaction, byte[], byte[])}.
     * Note that {@code getBoundaryKeys} must be called after this method is called, otherwise it will assume no keys exists.
     * </p>
     *
     * @param keys a sorted list of keys
     * @param rangeCount the number of ranges to return.
     */
    @Nonnull
    public static void init(@Nonnull List<byte[]> keys, int rangeCount) {
        if (keys.size() < rangeCount) {
            throw new IllegalArgumentException("rangeCount must be less than (or equal) the size of keys");
        }
        INSTANCE.ranges = new ArrayList<>(rangeCount);
        INSTANCE.keyRanges = new ArrayList<>(keys.size());

        // Build the boundaries randomly
        Random randomGenerator = new Random();
        int lastBoundaryIndex = 0;
        for (int i = 0; i < rangeCount; i++) {
            if (i > 0) {
                lastBoundaryIndex += randomGenerator.nextInt((keys.size() - INSTANCE.ranges.get(i - 1)) / (rangeCount - i));
            }
            INSTANCE.ranges.add(lastBoundaryIndex);
        }

        // Create a map between keys and their ranges.
        int rangeIndex = 0;
        for (int i = 0; INSTANCE.ranges.size() > 0 && i < keys.size(); i++) {
            while (rangeIndex < INSTANCE.ranges.size() && INSTANCE.ranges.get(rangeIndex) < i) {
                rangeIndex++;
            }
            INSTANCE.keyRanges.add(new KeyRange(keys.get(i), Tuple.fromBytes(keys.get(i)),
                    rangeIndex == INSTANCE.ranges.size() ? INSTANCE.ranges.get(rangeIndex - 1) /* it's the last range */ : INSTANCE.ranges.get(rangeIndex)));
        }
    }

    /**
     * Return a {@code CloseableAsyncIterator} of keys {@code k} such that {@code begin <= k < end} and {@code k}
     * represents a location at the start of a contiguous range stored on a single server. Note that this class
     * fakes the actual locations, and the returned keys are derived from the random boundaries created in {@link #init(List, int)}.
     *
     * @param tr the transaction on which to base the query
     * @param begin the inclusive start of the range
     * @param end the exclusive end of the range
     *
     * @return a sequence of keys denoting the start of the ranges
     */
    @Nonnull
    public CloseableAsyncIterator<byte[]> getBoundaryKeys(@Nonnull Transaction tr, @Nonnull byte[] begin, @Nonnull byte[] end) {
        // This mocked locality API does not need tr.
        return new MockedBoundaryIterator(keyRanges, ranges, begin, end);
    }

    static class MockedBoundaryIterator implements CloseableAsyncIterator<byte[]> {
        private List<byte[]> ranges;
        int lastBeginIndex;

        MockedBoundaryIterator(@Nonnull List<KeyRange> keyRanges, @Nonnull List<Integer> ranges, @Nonnull byte[] begin, @Nonnull byte[] end) {
            this.ranges = new ArrayList<>();
            Tuple beginTuple = Tuple.fromBytes(begin);
            Tuple endTuple = Tuple.fromBytes(end);
            if (beginTuple.compareTo(endTuple) > 0) {
                return;
            }
            int rangeIndex = -1;
            for (KeyRange keyRange : keyRanges) {
                if (beginTuple.compareTo(keyRange.tuple) <= 0) {
                    rangeIndex = keyRange.rangeIndex;
                    break;
                }
            }
            while (rangeIndex >= 0 && rangeIndex < ranges.size() && keyRanges.get(ranges.get(rangeIndex)).tuple.compareTo(endTuple) < 0) {
                this.ranges.add(keyRanges.get(ranges.get(rangeIndex++)).key);
            }
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return new CompletableFuture<>().completedFuture(lastBeginIndex < ranges.size());
        }

        @Override
        public boolean hasNext() {
            return lastBeginIndex < ranges.size();
        }

        @Override
        public byte[] next() {
            return ranges.get(lastBeginIndex++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Boundary keys are read-only");
        }

        @Override
        public void close() {
            // does nothing here.
        }
    }
}
