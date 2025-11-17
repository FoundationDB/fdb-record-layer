/*
 * MultidimensionalIndexScanBounds.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.query.expressions.Comparisons;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class VectorIndexScanBounds implements IndexScanBounds {
    @Nonnull
    private final TupleRange prefixRange;

    @Nonnull
    private final Comparisons.Type comparisonType;
    @Nullable
    private final RealVector queryVector;
    private final int limit;
    @Nonnull final VectorIndexScanOptions vectorIndexScanOptions;

    public VectorIndexScanBounds(@Nonnull final TupleRange prefixRange,
                                 @Nonnull final Comparisons.Type comparisonType,
                                 @Nullable final RealVector queryVector,
                                 final int limit,
                                 @Nonnull final VectorIndexScanOptions vectorIndexScanOptions) {
        this.prefixRange = prefixRange;
        this.comparisonType = comparisonType;
        this.queryVector = queryVector;
        this.limit = limit;
        this.vectorIndexScanOptions = vectorIndexScanOptions;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_VALUE;
    }

    @Nonnull
    public TupleRange getPrefixRange() {
        return prefixRange;
    }

    @Nonnull
    public Comparisons.Type getComparisonType() {
        return comparisonType;
    }

    @Nullable
    public RealVector getQueryVector() {
        return queryVector;
    }

    public int getLimit() {
        return limit;
    }

    @Nonnull
    public VectorIndexScanOptions getVectorIndexScanOptions() {
        return vectorIndexScanOptions;
    }

    public int getAdjustedLimit() {
        switch (getComparisonType()) {
            case DISTANCE_RANK_LESS_THAN:
                return limit - 1;
            case DISTANCE_RANK_LESS_THAN_OR_EQUAL:
                return limit;
            default:
                throw new RecordCoreException("unsupported comparison");
        }
    }

    public boolean isWithinLimit(int rank) {
        switch (getComparisonType()) {
            case DISTANCE_RANK_EQUALS:
                return rank == limit;
            case DISTANCE_RANK_LESS_THAN:
                return rank < limit;
            case DISTANCE_RANK_LESS_THAN_OR_EQUAL:
                return rank <= limit;
            default:
                throw new RecordCoreException("unsupported comparison");
        }
    }
}
