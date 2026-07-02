/*
 * PrimaryCopy.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * A {@link VectorReference} for a vector this cluster owns. May be {@code isUnderreplicated} (its replicas have not yet
 * been propagated to neighbors) and/or {@code isCollapsed}. A primary carries no replication priority.
 *
 * @param id the vector's identity
 * @param vector the vector data in the transformed coordinate space
 * @param isUnderreplicated whether this primary's replicas have not yet been propagated
 * @param isCollapsed whether this reference represents a collapsed set of identical vectors
 */
record PrimaryCopy(@Nonnull VectorId id, @Nonnull Transformed<RealVector> vector,
                   boolean isUnderreplicated, boolean isCollapsed) implements VectorReference {
    @Nonnull
    @Override
    public VectorReference withVectorId(@Nonnull final VectorId newVectorId) {
        return id.equals(newVectorId) ? this : new PrimaryCopy(newVectorId, vector, isUnderreplicated, isCollapsed);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public VectorReference withVector(@Nonnull final Transformed<RealVector> newVector) {
        return vector == newVector ? this : new PrimaryCopy(id, newVector, isUnderreplicated, isCollapsed);
    }

    @Nonnull
    @Override
    public VectorReference toPrimaryCopy() {
        return isUnderreplicated ? new PrimaryCopy(id, vector, false, isCollapsed) : this;
    }

    @Nonnull
    @Override
    public VectorReference toPrimaryUnderreplicatedCopy() {
        return isUnderreplicated ? this : new PrimaryCopy(id, vector, true, isCollapsed);
    }

    @Nonnull
    @Override
    public VectorReference toReplicatedCopy(final double newReplicationScore) {
        return new ReplicatedCopy(id, vector, newReplicationScore, isCollapsed);
    }

    @Nonnull
    @Override
    public VectorReference toCollapsed(@Nonnull final UUID signature, @Nonnull final UUID vectorUuid) {
        return new PrimaryCopy(new VectorId(Tuple.from(signature), vectorUuid), vector, isUnderreplicated, true);
    }

    @Nonnull
    @Override
    public String toString() {
        return "VR[" + id + ", PRIMARY, isUnderreplicated=" + isUnderreplicated
                + ", isCollapsed=" + isCollapsed + "]";
    }
}
