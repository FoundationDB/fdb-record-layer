/*
 * VectorReference.java
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
import com.apple.foundationdb.util.Lens;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.UUID;

/**
 * A reference to a vector stored within a cluster. Each cluster contains vector references for both
 * its primary vectors (owned by this cluster) and replicated vectors (copies from neighboring clusters
 * kept for search recall). A vector reference carries the vector data in the transformed coordinate
 * space along with metadata describing its role in the cluster.
 *
 * <p>
 * A vector reference can be in one of several states:
 * <ul>
 *   <li><b>Primary copy</b> — this cluster is the authoritative owner of the vector. The running
 *       standard deviation in the cluster metadata accounts for this vector's distance to the centroid.</li>
 *   <li><b>Primary underreplicated copy</b> — a primary copy whose replication to neighboring clusters
 *       has not yet been completed. A reassign task will eventually propagate replicas.</li>
 *   <li><b>Replicated copy</b> — a copy stored in a neighboring cluster for improved search recall.
 *       Carries a {@code replicationPriority} score indicating how important this replica is.</li>
 *   <li><b>Collapsed</b> — a representative for many identical vectors sharing the same content
 *       signature. The individual vector IDs are stored separately in the collapsed vector IDs
 *       subspace.</li>
 * </ul>
 *
 * @param id the vector's identity (primary key + UUID)
 * @param isPrimaryCopy whether this is the authoritative primary copy in this cluster
 * @param isUnderreplicated whether this primary copy's replicas have not yet been propagated
 * @param isCollapsed whether this reference represents a collapsed set of identical vectors
 * @param vector the vector data in the transformed coordinate space
 * @param replicationPriority the replication priority score (only meaningful for replicated copies;
 *        {@code -1.0} for primary copies)
 */
record VectorReference(@Nonnull VectorId id, boolean isPrimaryCopy, boolean isUnderreplicated, boolean isCollapsed,
                       @Nonnull Transformed<RealVector> vector, double replicationPriority) {
    private static final Lens<VectorReference, RealVector> VECTOR_LENS =
            new VectorReferenceVectorLens().compose(Transformed.underlyingLens());

    VectorReference {
        Preconditions.checkArgument(isPrimaryCopy || !isUnderreplicated);
    }

    /**
     * Returns a copy of this reference with a different vector ID, or {@code this} if the ID is unchanged.
     *
     * @param newVectorId the new vector ID
     * @return the updated vector reference
     */
    @Nonnull
    public VectorReference withVectorId(final VectorId newVectorId) {
        if (id() == newVectorId) {
            return this;
        }
        return new VectorReference(newVectorId, isPrimaryCopy(), isUnderreplicated(), isCollapsed(),
                vector(), replicationPriority());
    }

    /**
     * Returns a copy toggling between primary/replicated and underreplicated states. Resets the
     * replication priority to {@code -1.0} when becoming a primary copy.
     *
     * @param isPrimaryCopy whether the result should be a primary copy
     * @param isUnderreplicated whether the result should be marked as underreplicated
     * @return the updated vector reference, or {@code this} if unchanged
     */
    @Nonnull
    public VectorReference withPrimaryCopy(final boolean isPrimaryCopy, final boolean isUnderreplicated) {
        if (isPrimaryCopy() == isPrimaryCopy && isUnderreplicated() == isUnderreplicated) {
            return this;
        }
        return new VectorReference(id(), isPrimaryCopy, isUnderreplicated, isCollapsed(),
                vector(), isPrimaryCopy ? -1.0d : replicationPriority());
    }

    /**
     * Returns a copy with the vector data replaced, or {@code this} if the vector is unchanged.
     *
     * @param newVector the new vector data in the transformed coordinate space
     * @return the updated vector reference
     */
    @Nonnull
    public VectorReference withVector(final Transformed<RealVector> newVector) {
        if (vector() == newVector) {
            return this;
        }
        return new VectorReference(id(), isPrimaryCopy(), isUnderreplicated(), isCollapsed(), newVector,
                replicationPriority());
    }

    /** Converts this reference to a non-underreplicated primary copy. */
    @Nonnull
    public VectorReference toPrimaryCopy() {
        return withPrimaryCopy(true, false);
    }

    /**
     * Converts this reference to a replicated copy with the given replication priority.
     *
     * @param newReplicationScore the replication priority score for the replica
     * @return the replicated vector reference
     */
    @Nonnull
    public VectorReference toReplicatedCopy(final double newReplicationScore) {
        return new VectorReference(id(), false, false, isCollapsed(), vector(),
                newReplicationScore);
    }

    /** Converts this reference to an underreplicated primary copy. */
    @Nonnull
    public VectorReference toPrimaryUnderreplicatedCopy() {
        return withPrimaryCopy(true, true);
    }

    /**
     * Creates a collapsed version of this reference. The collapsed reference uses the signature as its
     * primary key and is marked as collapsed. The individual vector IDs behind this collapsed reference
     * are stored separately in the collapsed vector IDs subspace.
     *
     * @param signature the content signature (SHA-256 hash truncated to UUID) shared by all identical vectors
     * @param vectorUuid the UUID assigned to the collapsed representative
     * @return the collapsed vector reference
     */
    @Nonnull
    public VectorReference toCollapsed(@Nonnull final UUID signature, @Nonnull final UUID vectorUuid) {
        return new VectorReference(new VectorId(Tuple.from(signature), vectorUuid), isPrimaryCopy(),
                isUnderreplicated(), true, vector(), replicationPriority());
    }

    @Nonnull
    @Override
    public String toString() {
        return "VR[" + id() +
                ", isPrimaryCopy=" + isPrimaryCopy() +
                ", isUnderreplicated=" + isUnderreplicated() +
                ", isCollapsed=" + isCollapsed() +
                ", replicationPriority=" + replicationPriority() + "]";
    }

    /**
     * Returns a lens that extracts the underlying {@link RealVector} from a vector reference,
     * composing the vector reference → transformed vector → raw vector lenses.
     *
     * @return the composed lens
     */
    @Nonnull
    public static Lens<VectorReference, RealVector> vectorLens() {
        return VECTOR_LENS;
    }

    /**
     * Lens to access the underlying vector of a transformed vector in logic that can be called for containers of
     * both vectors and transformed vectors.
     */
    static class VectorReferenceVectorLens implements Lens<VectorReference, Transformed<RealVector>> {
        @Nullable
        @Override
        public Transformed<RealVector> get(@Nonnull final VectorReference vectorReference) {
            return vectorReference.vector();
        }

        @Nonnull
        @Override
        public VectorReference set(@Nullable final VectorReference vectorReference,
                                   @Nullable final Transformed<RealVector> transformed) {
            Objects.requireNonNull(vectorReference);
            return vectorReference.withVector(transformed);
        }
    }
}
