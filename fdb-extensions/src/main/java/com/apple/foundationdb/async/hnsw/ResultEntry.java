/*
 * ResultEntry.java
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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Record-like class to wrap the results of a kNN-search.
 */
public class ResultEntry {
    /**
     * Primary key of the item in the HNSW.
     */
    @Nonnull
    private final Tuple primaryKey;

    /**
     * The vector that is stored with the item in the structure. This vector is expressed in the client's coordinate
     * system and should be of class {@link com.apple.foundationdb.linear.HalfRealVector},
     * {@link com.apple.foundationdb.linear.FloatRealVector}, or {@link com.apple.foundationdb.linear.DoubleRealVector}.
     * This member is nullable. It is set to {@code null}, if the caller to
     * {@link HNSW#kNearestNeighborsSearch(ReadTransaction, int, int, boolean, RealVector)} requested to not return
     * vectors.
     * <p>
     * The vector, if set, may or may not be exactly equal to the vector that was originally inserted in the HNSW.
     * Depending on quantization settings (see {@link Config}, the vector that
     * is returned may only be an approximation of the original vector.
     */
    @Nullable
    private final RealVector vector;

    /**
     * The distance of item's vector to the query vector.
     */
    private final double distance;

    /**
     * The row number of the item. TODO support rank.
     */
    private final int rankOrRowNumber;

    public ResultEntry(@Nonnull final Tuple primaryKey, @Nullable final RealVector vector, final double distance,
                       final int rankOrRowNumber) {
        this.primaryKey = primaryKey;
        this.vector = vector;
        this.distance = distance;
        this.rankOrRowNumber = rankOrRowNumber;
    }

    @Nonnull
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    @Nullable
    public RealVector getVector() {
        return vector;
    }

    public double getDistance() {
        return distance;
    }

    public int getRankOrRowNumber() {
        return rankOrRowNumber;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof ResultEntry)) {
            return false;
        }
        final ResultEntry that = (ResultEntry)o;
        return Double.compare(distance, that.distance) == 0 &&
                rankOrRowNumber == that.rankOrRowNumber &&
                Objects.equals(primaryKey, that.primaryKey) &&
                Objects.equals(vector, that.vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKey, vector, distance, rankOrRowNumber);
    }

    @Override
    public String toString() {
        return "[" +
                "primaryKey=" + primaryKey +
                ", vector=" + vector +
                ", distance=" + distance +
                ", rankOrRowNumber=" + rankOrRowNumber + "]";
    }
}
