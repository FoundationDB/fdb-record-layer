/*
 * AggregatedVector.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;

import javax.annotation.Nonnull;

/**
 * A record-like class wrapping a {@link RealVector} and a count. This data structure is used to keep a running sum
 * of many vectors in order to compute their centroid at a later time.
 * @param partialCount count aggregate to indicate how many vectors have been aggregated to form {@code partialVector}
 * @param partialVector the vector aggregate (mostly the sum of all individual vectors)
 */
public record AggregatedVector(int partialCount, @Nonnull Transformed<RealVector> partialVector) {
    @Nonnull
    @Override
    public String toString() {
        return "AggregatedVector[" + partialCount + ", " + partialVector + "]";
    }
}
