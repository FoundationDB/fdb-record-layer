/*
 * RTreeIndexHelper.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.RTree;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Helper functions for index maintainers that use a {@link RTree}.
 */
@API(API.Status.EXPERIMENTAL)
public class RTreeIndexHelper {
    private static final long nullReplacement = Long.MIN_VALUE;

    /**
     * Parse standard options into {@link RTree.Config}.
     * @param index the index definition to get options from
     * @return parsed config options
     */
    public static RTree.Config getConfig(@Nonnull final Index index) {
        final RTree.ConfigBuilder builder = RTree.newConfigBuilder();
        final String rtreeMinMOption = index.getOption(IndexOptions.RTREE_MIN_M);
        if (rtreeMinMOption != null) {
            builder.setMinM(Integer.parseInt(rtreeMinMOption));
        }
        final String rtreeMaxMOption = index.getOption(IndexOptions.RTREE_MAX_M);
        if (rtreeMaxMOption != null) {
            builder.setMaxM(Integer.parseInt(rtreeMaxMOption));
        }
        final String rtreeSplitS = index.getOption(IndexOptions.RTREE_SPLIT_S);
        if (rtreeSplitS != null) {
            builder.setSplitS(Integer.parseInt(rtreeSplitS));
        }
        return builder.build();
    }

    /**
     * Instrumentation events specific to rank index maintenance.
     */
    public enum Events implements StoreTimer.DetailEvent {
        RANKED_SET_SCORE_FOR_RANK("ranked set score for rank"),
        RANKED_SET_RANK_FOR_SCORE("ranked set rank for score"),
        RANKED_SET_UPDATE("ranked set update");

        private final String title;
        private final String logKey;

        Events(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.DetailEvent.super.logKey();
        }

        Events(String title) {
            this(title, null);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }
    }

    private RTreeIndexHelper() {
    }

    static BigInteger hilbertValue(@Nonnull final RTree.Point point) {
        int numBits = 64;
        final long[] shiftedCoordinates = new long[point.getNumDimensions()];
        for (int i = 0; i < point.getNumDimensions(); i++) {
            Long coordinateAsLong = (Long)point.getCoordinateAsNumber(i);
            coordinateAsLong = coordinateAsLong == null ? nullReplacement : coordinateAsLong;
            shiftedCoordinates[i] = shiftCoordinate(coordinateAsLong);
        }
        return toIndex(numBits, transposedIndex(numBits, shiftedCoordinates));
    }

    private static long shiftCoordinate(long coordinate) {
        return coordinate < 0
               ? (Long.MAX_VALUE - (-coordinate - 1L))
               : (coordinate | (1L << 63));
    }

    private static BigInteger toIndex(int numBits, long... transposedIndexes) {
        int length = numBits * transposedIndexes.length;
        byte[] b = new byte[length / 8 + 1];
        int bIndex = length - 1;
        long mask = 1L << (numBits - 1);
        for (int i = 0; i < numBits; i++) {
            for (final long transposedIndex : transposedIndexes) {
                if ((transposedIndex & mask) != 0) {
                    b[b.length - 1 - bIndex / 8] |= 1 << (bIndex % 8);
                }
                bIndex--;
            }
            mask >>>= 1;
        }
        // b is expected to be BigEndian
        return new BigInteger(1, b);
    }

    private static long[] transposedIndex(int numBits, long... unsignedPoints) {
        final long M = 1L << (numBits - 1);
        final int n = unsignedPoints.length; // n: Number of dimensions
        final long[] x = Arrays.copyOf(unsignedPoints, n);
        long p;
        long q;
        long t;
        int i;
        // Inverse undo
        for (q = M; q != 1; q >>>= 1) {
            p = q - 1;
            for (i = 0; i < n; i++) {
                if ((x[i] & q) != 0) {
                    x[0] ^= p; // invert
                } else {
                    t = (x[0] ^ x[i]) & p;
                    x[0] ^= t;
                    x[i] ^= t;
                }
            }
        } // exchange
        // Gray encode
        for (i = 1; i < n; i++) {
            x[i] ^= x[i - 1];
        }
        t = 0;
        for (q = M; q != 1; q >>>= 1) {
            if ((x[n - 1] & q) != 0) {
                t ^= q - 1;
            }
        }
        for (i = 0; i < n; i++) {
            x[i] ^= t;
        }

        return x;
    }
}
