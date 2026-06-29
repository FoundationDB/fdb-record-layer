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
import com.apple.foundationdb.util.Lens;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A reference to a vector stored within a cluster: the vector data in the transformed coordinate space together with
 * its role in that cluster. Each cluster holds references both for the vectors it owns (primary copies) and for copies
 * of neighboring clusters' vectors kept to improve search recall (replicated copies).
 *
 * <p>
 * This is a sealed sum type with one variant per role, so the dependencies between a reference's role and its
 * other state hold by construction:
 * <ul>
 *   <li>{@link PrimaryCopy} — this cluster is the authoritative owner of the vector. A primary may additionally be
 *       <em>underreplicated</em> ({@link PrimaryCopy#isUnderreplicated()}): its replicas have not yet been propagated
 *       to neighbors, and a reassign task will eventually do so. Underreplication is thus a property that only a
 *       primary can carry — an "underreplicated replica" is unrepresentable.</li>
 *   <li>{@link ReplicatedCopy} — a copy kept in a neighboring cluster for recall, carrying a
 *       {@link ReplicatedCopy#replicationPriority()} score. Only a replica has a priority; a primary has no
 *       priority component.</li>
 * </ul>
 *
 * <p>
 * {@link #isCollapsed() Collapsed} is deliberately <em>not</em> a variant. A collapsed reference is a representative
 * standing in for many identical vectors that share one content signature, and a collapse can sit on top of either a
 * primary or a replica (preserving its role and, for a replica, its priority). Because it is orthogonal to the role it
 * stays a boolean carried by both variants rather than multiplying the hierarchy; folding it into the sum type would
 * cross-product the variants for no safety gain (there is no illegal collapsed combination). The individual member ids
 * behind a collapsed reference live separately in the collapsed-vector-ids subspace.
 */
sealed interface VectorReference permits PrimaryCopy, ReplicatedCopy {
    /**
     * The vector's identity (primary key plus UUID). For a collapsed reference the primary key is the content
     * signature rather than a source-record key.
     *
     * @return the vector id
     */
    @Nonnull
    VectorId id();

    /**
     * The vector data in the transformed (stored) coordinate space.
     *
     * @return the transformed vector
     */
    @Nonnull
    Transformed<RealVector> vector();

    /**
     * Whether this reference is a collapsed representative for a set of identical vectors. Orthogonal to the role:
     * both a {@link PrimaryCopy} and a {@link ReplicatedCopy} may be collapsed.
     *
     * @return {@code true} if this reference represents a collapsed set
     */
    boolean isCollapsed();

    /**
     * Whether this cluster is the authoritative owner of the vector (i.e. this is a {@link PrimaryCopy}). Convenience
     * accessor so role-branching call sites need not pattern-match on the variant.
     *
     * @return {@code true} for a primary copy, {@code false} for a replica
     */
    default boolean isPrimaryCopy() {
        return this instanceof PrimaryCopy;
    }

    /**
     * Whether this is a primary copy whose replicas have not yet been propagated. Always {@code false} for a replica
     * (only a {@link PrimaryCopy} can be underreplicated).
     *
     * @return {@code true} if this is an underreplicated primary copy
     */
    default boolean isUnderreplicated() {
        return false;
    }

    /**
     * The replication-priority score of this replica (higher means more important to keep). Returns {@code -1.0} for a
     * primary copy, which carries no priority.
     *
     * @return the replication priority, or {@code -1.0} for a primary copy
     */
    default double replicationPriority() {
        return -1.0d;
    }

    /**
     * Returns a copy of this reference with a different vector id, preserving role and all other state.
     *
     * @param newVectorId the new vector id
     * @return the updated reference (or {@code this} if the id is unchanged)
     */
    @Nonnull
    VectorReference withVectorId(@Nonnull VectorId newVectorId);

    /**
     * Returns a copy of this reference with the vector data replaced, preserving role and all other state.
     *
     * @param newVector the new vector data in the transformed coordinate space
     * @return the updated reference (or {@code this} if the vector is unchanged)
     */
    @Nonnull
    VectorReference withVector(@Nonnull Transformed<RealVector> newVector);

    /**
     * Returns this reference as a non-underreplicated primary copy. A replica is promoted to a primary (dropping its
     * replication priority); an already-non-underreplicated primary is returned unchanged.
     *
     * @return the primary-copy reference
     */
    @Nonnull
    VectorReference toPrimaryCopy();

    /**
     * Returns this reference as an underreplicated primary copy. A replica is promoted to a primary (dropping its
     * replication priority).
     *
     * @return the underreplicated primary-copy reference
     */
    @Nonnull
    VectorReference toPrimaryUnderreplicatedCopy();

    /**
     * Returns this reference as a replicated copy with the given replication priority. A primary is demoted to a
     * replica.
     *
     * @param newReplicationScore the replication priority for the replica
     * @return the replicated-copy reference
     */
    @Nonnull
    VectorReference toReplicatedCopy(double newReplicationScore);

    /**
     * Returns a collapsed version of this reference: the signature becomes its primary key, it is marked collapsed,
     * and its role (and a replica's priority) is preserved. The individual vector ids behind it are recorded
     * separately in the collapsed-vector-ids subspace.
     *
     * @param signature the content signature shared by all identical vectors
     * @param vectorUuid the UUID assigned to the collapsed representative
     * @return the collapsed reference
     */
    @Nonnull
    VectorReference toCollapsed(@Nonnull UUID signature, @Nonnull UUID vectorUuid);

    /**
     * Creates a primary-copy reference (this cluster owns the vector).
     *
     * @param id the vector's identity
     * @param vector the vector data in the transformed coordinate space
     * @param isUnderreplicated whether this primary's replicas have not yet been propagated
     * @param isCollapsed whether this reference represents a collapsed set of identical vectors
     * @return the primary-copy reference
     */
    @Nonnull
    static VectorReference primaryCopy(@Nonnull final VectorId id, @Nonnull final Transformed<RealVector> vector,
                                       final boolean isUnderreplicated, final boolean isCollapsed) {
        return new PrimaryCopy(id, vector, isUnderreplicated, isCollapsed);
    }

    /**
     * Creates a replicated-copy reference (a recall copy of a vector owned by a neighboring cluster).
     *
     * @param id the vector's identity
     * @param vector the vector data in the transformed coordinate space
     * @param replicationPriority the replication-priority score for the replica
     * @param isCollapsed whether this reference represents a collapsed set of identical vectors
     * @return the replicated-copy reference
     */
    @Nonnull
    static VectorReference replicatedCopy(@Nonnull final VectorId id, @Nonnull final Transformed<RealVector> vector,
                                          final double replicationPriority, final boolean isCollapsed) {
        return new ReplicatedCopy(id, vector, replicationPriority, isCollapsed);
    }

    /**
     * Returns a lens that extracts the underlying {@link RealVector} from a vector reference, composing the vector
     * reference to transformed vector to raw vector lenses.
     *
     * @return the composed lens
     */
    @Nonnull
    static Lens<VectorReference, RealVector> vectorLens() {
        return VectorReferenceVectorLens.VECTOR_LENS;
    }

    /**
     * The persisted role discriminant of a vector reference. A single role code cannot represent an illegal
     * combination (such as an underreplicated replica), and a replication priority is persisted only for
     * {@link #REPLICATED}. Mirrors {@link AbstractDeferredTask.Kind}: each constant carries a stable
     * {@link #getCode()} used in the serialized form.
     */
    enum Role {
        PRIMARY(0),
        UNDERREPLICATED_PRIMARY(1),
        REPLICATED(2);

        private static final Map<Integer, Role> BY_CODE =
                Arrays.stream(values()).collect(ImmutableMap.toImmutableMap(role -> role.code, role -> role));

        private final int code;

        Role(final int code) {
            this.code = code;
        }

        /**
         * Returns the stable code persisted for this role.
         *
         * @return the role's serialized code
         */
        public int getCode() {
            return code;
        }

        /**
         * Returns the role for a persisted code.
         *
         * @param code a persisted role code
         * @return the matching role
         * @throws NullPointerException if the code does not correspond to any role
         */
        @Nonnull
        public static Role ofCode(final int code) {
            return Objects.requireNonNull(BY_CODE.get(code),
                    () -> "unknown vector reference role code: " + code);
        }
    }
}
