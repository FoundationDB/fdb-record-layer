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
import com.google.common.base.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
     * A centroid that is usually derived as an average over some vectors seen so far. It is used to create the
     * {@link StorageTransform}.
     */
    @Nullable
    private final RealVector centroid;

    public AccessInfo(@Nonnull final EntryNodeReference entryNodeReference, final long rotatorSeed,
                      @Nullable final RealVector centroid) {
        this.entryNodeReference = entryNodeReference;
        this.rotatorSeed = rotatorSeed;
        this.centroid = centroid;
    }

    @Nonnull
    public EntryNodeReference getEntryNodeReference() {
        return entryNodeReference;
    }

    public boolean canUseRaBitQ() {
        return getCentroid() != null;
    }

    public long getRotatorSeed() {
        return rotatorSeed;
    }

    @Nullable
    public RealVector getCentroid() {
        return centroid;
    }

    @Nonnull
    public AccessInfo withNewEntryNodeReference(@Nonnull final EntryNodeReference entryNodeReference) {
        return new AccessInfo(entryNodeReference, getRotatorSeed(), getCentroid());
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof AccessInfo)) {
            return false;
        }
        final AccessInfo that = (AccessInfo)o;
        return rotatorSeed == that.rotatorSeed &&
                Objects.equal(entryNodeReference, that.entryNodeReference) &&
                Objects.equal(centroid, that.centroid);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(entryNodeReference, rotatorSeed, centroid);
    }

    @Nonnull
    @Override
    public String toString() {
        return "AccessInfo[" +
                "entryNodeReference=" + entryNodeReference +
                ", rotatorSeed=" + rotatorSeed +
                ", centroid=" + centroid + "]";
    }
}
