/*
 * ResultEntry.java
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

import com.apple.foundationdb.async.hnsw.Config;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Record class that wraps the results of a kNN-search.
 *
 * @param primaryKey Primary key of the item in the HNSW.
 * @param vector The vector that is stored with the item in the structure. This vector is expressed in the client's
 *        coordinate system and should be of class {@link HalfRealVector}, {@link FloatRealVector},
 *        or {@link DoubleRealVector}. This member is nullable. It is set to {@code null}, if the caller to
 *        {@code kNearestNeighborsSearch(.)} requested to not return vectors. The vector, if set, may or may not be
 *        exactly equal to the vector that was originally inserted into the vector structure. Depending on
 *        quantization settings (see {@link Config}), the vector that is returned may only be an approximation of the
 *        original vector.
 * @param additionalValues additional values that are stored together with the primary key and the vector for faster
 *        retrieval.
 * @param distance The distance of item's vector to the query vector.
 * @param rankOrRowNumber The row number of the item. TODO support rank.
 */
public record ResultEntry(@Nonnull Tuple primaryKey, @Nullable RealVector vector, @Nullable Tuple additionalValues,
                          double distance, int rankOrRowNumber) {
    public ResultEntry {
        if (vector != null) {
            Preconditions.checkArgument(vector instanceof DoubleRealVector ||
                            vector instanceof FloatRealVector ||
                            vector instanceof HalfRealVector,
                    "vector has to be a data vector but it is " +
                            vector.getClass().getSimpleName());
        }
    }

    @Nonnull
    @Override
    public String toString() {
        return "[" +
                "primaryKey=" + primaryKey +
                ", vector=" + vector +
                ", additionalValues=" + additionalValues +
                ", distance=" + distance +
                ", rankOrRowNumber=" + rankOrRowNumber + "]";
    }
}
