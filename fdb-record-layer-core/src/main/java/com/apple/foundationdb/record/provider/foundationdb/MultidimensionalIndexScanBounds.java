/*
 * IndexScanRange.java
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.TupleRange;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.IndexScanType.BY_VALUE;

/**
 * {@link TupleRange} for an index scan.
 */
@API(API.Status.UNSTABLE)
public class MultidimensionalIndexScanBounds implements IndexScanBounds {
    @Nonnull
    private final TupleRange prefixRange;

    @Nonnull
    private final List<TupleRange> dimensionRanges;

    public MultidimensionalIndexScanBounds(@Nonnull final TupleRange prefixRange, @Nonnull final List<TupleRange> dimensionRanges) {
        this.prefixRange = prefixRange;
        this.dimensionRanges = ImmutableList.copyOf(dimensionRanges);
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_VALUE_OVER_SCAN;
    }

    @Nonnull
    public TupleRange getPrefixRange() {
        return prefixRange;
    }

    @Nonnull
    public List<TupleRange> getDimensionRanges() {
        return dimensionRanges;
    }

    @Override
    public String toString() {
        return "MD:" + BY_VALUE + ":" + prefixRange;
    }
}
