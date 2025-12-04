/*
 * AggregatedVector.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A record-like class wrapping a {@link RealVector} and a count. This data structure is used to keep a running sum
 * of many vectors in order to compute their centroid at a later time.
 */
class AggregatedVector {
    private final int partialCount;
    @Nonnull
    private final Transformed<RealVector> partialVector;

    public AggregatedVector(final int partialCount, @Nonnull final Transformed<RealVector> partialVector) {
        this.partialCount = partialCount;
        this.partialVector = partialVector;
    }

    public int getPartialCount() {
        return partialCount;
    }

    @Nonnull
    public Transformed<RealVector> getPartialVector() {
        return partialVector;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof AggregatedVector)) {
            return false;
        }
        final AggregatedVector that = (AggregatedVector)o;
        return partialCount == that.partialCount && Objects.equals(partialVector, that.partialVector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partialCount, partialVector);
    }

    @Override
    public String toString() {
        return "AggregatedVector[" + partialCount + ", " + partialVector + "]";
    }
}
