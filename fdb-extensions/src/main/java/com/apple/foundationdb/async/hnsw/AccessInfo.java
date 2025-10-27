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

class AccessInfo {
    @Nonnull
    private final EntryNodeReference entryNodeReference;

    private final long rotatorSeed;

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
    public final boolean equals(final Object o) {
        if (!(o instanceof AccessInfo)) {
            return false;
        }

        final AccessInfo that = (AccessInfo)o;
        return rotatorSeed == that.rotatorSeed &&
                entryNodeReference.equals(that.entryNodeReference) &&
                Objects.equals(centroid, that.centroid);
    }

    @Override
    public int hashCode() {
        int result = entryNodeReference.hashCode();
        result = 31 * result + Long.hashCode(rotatorSeed);
        result = 31 * result + Objects.hashCode(centroid);
        return result;
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
