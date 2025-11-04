/*
 * AccessInfo.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Class to capture the current state of this HNSW that cannot be expressed as metadata but that also is not the actual
 * data that is inserted, organized and retrieved. For instance, an HNSW needs to keep track of the entry point that
 * resides in the highest layer(currently). Another example is any information that pertains to coordinate system
 * transformations that have to be carried out prior/posterior to inserting/retrieving an item into/from the HNSW.
 */
class AccessInfo {
    /**
     * The current entry point. All searches start here.
     */
    @Nonnull
    private final EntryNodeReference entryNodeReference;

    /**
     * A seed that can be used to reconstruct a random rotator {@link com.apple.foundationdb.linear.FhtKacRotator} used
     * in ({@link StorageTransform}.
     */
    private final long rotatorSeed;

    /**
     * The negated centroid that is usually derived as an average over some vectors seen so far. It is used to create
     * the {@link StorageTransform}. The centroid is stored in its negated form (i.e. {@code centroid * (-1)}) as the
     * {@link com.apple.foundationdb.linear.AffineOperator} adds its translation vector but the centroid needs to be
     * subtracted.
     */
    @Nullable
    private final RealVector negatedCentroid;

    public AccessInfo(@Nonnull final EntryNodeReference entryNodeReference, final long rotatorSeed,
                      @Nullable final RealVector negatedCentroid) {
        this.entryNodeReference = entryNodeReference;
        this.rotatorSeed = rotatorSeed;
        this.negatedCentroid = negatedCentroid;
    }

    @Nonnull
    public EntryNodeReference getEntryNodeReference() {
        return entryNodeReference;
    }

    public boolean canUseRaBitQ() {
        return getNegatedCentroid() != null;
    }

    public long getRotatorSeed() {
        return rotatorSeed;
    }

    @Nullable
    public RealVector getNegatedCentroid() {
        return negatedCentroid;
    }

    @Nonnull
    public AccessInfo withNewEntryNodeReference(@Nonnull final EntryNodeReference entryNodeReference) {
        return new AccessInfo(entryNodeReference, getRotatorSeed(), getNegatedCentroid());
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof AccessInfo)) {
            return false;
        }
        final AccessInfo that = (AccessInfo)o;
        return rotatorSeed == that.rotatorSeed &&
                Objects.equals(entryNodeReference, that.entryNodeReference) &&
                Objects.equals(negatedCentroid, that.negatedCentroid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entryNodeReference, rotatorSeed, negatedCentroid);
    }

    @Nonnull
    @Override
    public String toString() {
        return "AccessInfo[" +
                "entryNodeReference=" + entryNodeReference +
                ", rotatorSeed=" + rotatorSeed +
                ", centroid=" + negatedCentroid + "]";
    }
}
