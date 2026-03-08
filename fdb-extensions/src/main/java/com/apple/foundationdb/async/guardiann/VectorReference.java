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
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

class VectorReference {
    private static final Lens<VectorReference, RealVector> VECTOR_LENS =
            new VectorReferenceVectorLens().compose(Transformed.underlyingLens());

    @Nonnull
    private final VectorId id;
    private final boolean isPrimaryCopy;
    private final boolean isUnderreplicated;
    @Nonnull
    private final Transformed<RealVector> vector;
    private final double replicationScore;

    public VectorReference(@Nonnull final VectorId id, final boolean isPrimaryCopy, final boolean isUnderreplicated,
                           @Nonnull final Transformed<RealVector> vector, final double replicationScore) {
        Preconditions.checkArgument(isPrimaryCopy || !isUnderreplicated);
        this.id = id;
        this.isPrimaryCopy = isPrimaryCopy;
        this.isUnderreplicated = isUnderreplicated;
        this.vector = vector;
        this.replicationScore = replicationScore;
    }

    @Nonnull
    public VectorId getId() {
        return id;
    }

    public boolean isPrimaryCopy() {
        return isPrimaryCopy;
    }

    public boolean isUnderreplicated() {
        return isUnderreplicated;
    }

    @Nonnull
    public Transformed<RealVector> getVector() {
        return vector;
    }

    public double getReplicationScore() {
        return replicationScore;
    }

    @Nonnull
    public VectorReference withVectorId(final VectorId newVectorId) {
        if (getId() == newVectorId) {
            return this;
        }
        return new VectorReference(newVectorId, isPrimaryCopy(), isUnderreplicated(),
                getVector(), getReplicationScore());
    }

    @Nonnull
    public VectorReference withPrimaryCopy(final boolean isPrimaryCopy, final boolean isUnderreplicated) {
        if (isPrimaryCopy() == isPrimaryCopy && isUnderreplicated() == isUnderreplicated) {
            return this;
        }
        return new VectorReference(getId(), isPrimaryCopy, isUnderreplicated,
                getVector(), isPrimaryCopy ? -1.0d : getReplicationScore());
    }

    @Nonnull
    public VectorReference withVector(final Transformed<RealVector> newVector) {
        if (getVector() == newVector) {
            return this;
        }
        return new VectorReference(getId(), isPrimaryCopy(), isUnderreplicated(), newVector, getReplicationScore());
    }

    @Nonnull
    public VectorReference toPrimaryCopy() {
        return withPrimaryCopy(true, false);
    }

    @Nonnull
    public VectorReference toReplicatedCopy(final double newReplicationScore) {
        return new VectorReference(getId(), false, false, getVector(), newReplicationScore);
    }

    @Nonnull
    public VectorReference toPrimaryUnderreplicatedCopy() {
        return withPrimaryCopy(true, true);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final VectorReference that = (VectorReference)o;
        return Objects.equals(getId(), that.getId()) &&
                isPrimaryCopy() == that.isPrimaryCopy() &&
                isUnderreplicated() == that.isUnderreplicated() &&
                Objects.equals(getVector(), that.getVector()) &&
                getReplicationScore() == that.getReplicationScore();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), isPrimaryCopy(), isUnderreplicated(), getVector(), getReplicationScore());
    }

    @Override
    public String toString() {
        return "VR[" + getId() +
                ", isPrimaryCopy=" + isPrimaryCopy() +
                ", isUnderreplicated=" + isUnderreplicated() +
                ", replicationScore=" + getReplicationScore() + "]";
    }

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
            return vectorReference.getVector();
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
