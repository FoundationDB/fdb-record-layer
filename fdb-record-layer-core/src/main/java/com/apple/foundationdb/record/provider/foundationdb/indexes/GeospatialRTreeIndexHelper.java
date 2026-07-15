/*
 * GeospatialRTreeIndexHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;

/**
 * Helper functions for {@link GeospatialRTreeIndexMaintainer}.
 * @see GeospatialRTreeIndexMaintainer for the index layout
 */
@API(API.Status.EXPERIMENTAL)
public class GeospatialRTreeIndexHelper {
    /**
     * Number of coordinate columns (latitude, longitude) that follow the grouping columns.
     */
    public static final int COORDINATE_DIMENSIONS = 2;

    private static final int DEFAULT_PRECISION_DIGITS = 7;
    // 180 * 10^16 stays within a signed long; beyond this the fixed-point encoding of a longitude would overflow.
    private static final int MAX_PRECISION_DIGITS = 15;

    private GeospatialRTreeIndexHelper() {
    }

    /**
     * Parse the R-tree structural options shared with the multidimensional index.
     * @param index the index definition
     * @return the parsed config
     * @see MultiDimensionalIndexHelper#getConfig(Index)
     */
    @Nonnull
    public static RTree.Config getConfig(@Nonnull final Index index) {
        return MultiDimensionalIndexHelper.getConfig(index);
    }

    /**
     * Fixed-point scale factor ({@code 10^digits}) applied to a {@code double} degree value before it is stored as a
     * {@code long} in the R-tree.
     * @param index the index definition
     * @return the scale factor
     * @see IndexOptions#GEOSPATIAL_RTREE_PRECISION_DIGITS
     */
    public static long getScale(@Nonnull final Index index) {
        final String option = index.getOption(IndexOptions.GEOSPATIAL_RTREE_PRECISION_DIGITS);
        final int digits = option == null ? DEFAULT_PRECISION_DIGITS : Integer.parseInt(option);
        if (digits < 0 || digits > MAX_PRECISION_DIGITS) {
            throw new RecordCoreArgumentException("geospatial precision digits out of range")
                    .addLogInfo(LogMessageKeys.INDEX_NAME, index.getName())
                    .addLogInfo(LogMessageKeys.VALUE, digits);
        }
        long scale = 1L;
        for (int i = 0; i < digits; i++) {
            scale *= 10L;
        }
        return scale;
    }

    /**
     * Number of leading grouping columns, each distinct value of which is backed by its own R-tree. Coordinate columns
     * follow the grouping columns.
     * @param root the index root key expression
     * @return the grouping (prefix) size, or {@code 0} when the root is not grouped
     */
    public static int getGroupingCount(@Nonnull final KeyExpression root) {
        return root instanceof GroupingKeyExpression ? ((GroupingKeyExpression)root).getGroupingCount() : 0;
    }
}
