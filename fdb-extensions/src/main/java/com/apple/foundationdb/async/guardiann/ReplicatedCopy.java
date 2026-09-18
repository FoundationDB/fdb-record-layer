/*
 * ReplicatedCopy.java
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
 * A {@link VectorReference} that is a recall copy of a vector owned by a neighboring cluster, carrying a
 * {@code replicationPriority} score (higher means more important to keep). May be {@code isCollapsed}. A replica is
 * never underreplicated — underreplication is a property only a {@link PrimaryCopy} can have.
 *
 * @param id the vector's identity
 * @param vector the vector data in the transformed coordinate space
 * @param replicationPriority the replication-priority score
 * @param isCollapsed whether this reference represents a collapsed set of identical vectors
 */
record ReplicatedCopy(@Nonnull VectorId id, @Nonnull Transformed<RealVector> vector,
                      double replicationPriority, boolean isCollapsed) implements VectorReference {
    @Nonnull
    @Override
    public VectorReference withVectorId(@Nonnull final VectorId newVectorId) {
        return id.equals(newVectorId) ? this : new ReplicatedCopy(newVectorId, vector, replicationPriority, isCollapsed);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public VectorReference withVector(@Nonnull final Transformed<RealVector> newVector) {
        return vector == newVector ? this : new ReplicatedCopy(id, newVector, replicationPriority, isCollapsed);
    }

    @Nonnull
    @Override
    public VectorReference toPrimaryCopy() {
        return new PrimaryCopy(id, vector, false, isCollapsed);
    }

    @Nonnull
    @Override
    public VectorReference toPrimaryUnderreplicatedCopy() {
        return new PrimaryCopy(id, vector, true, isCollapsed);
    }

    @Nonnull
    @Override
    public VectorReference toReplicatedCopy(final double newReplicationScore) {
        return replicationPriority == newReplicationScore
               ? this : new ReplicatedCopy(id, vector, newReplicationScore, isCollapsed);
    }

    @Nonnull
    @Override
    public VectorReference toCollapsed(@Nonnull final UUID signature, @Nonnull final UUID vectorUuid) {
        return new ReplicatedCopy(new VectorId(Tuple.from(signature), vectorUuid), vector, replicationPriority, true);
    }

    @Nonnull
    @Override
    public String toString() {
        return "VR[" + id + ", REPLICATED, replicationPriority=" + replicationPriority
                + ", isCollapsed=" + isCollapsed + "]";
    }
}
