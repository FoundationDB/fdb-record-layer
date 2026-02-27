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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

class VectorReference {
    private static final Lens<VectorReference, RealVector> VECTOR_LENS =
            new VectorReferenceVectorLens().compose(Transformed.underlyingLens());

    @Nonnull
    private final VectorId id;
    private final boolean isPrimaryCopy;
    @Nonnull
    private final Transformed<RealVector> vector;

    public VectorReference(@Nonnull final VectorId id, boolean isPrimaryCopy,
                           @Nonnull final Transformed<RealVector> vector) {
        this.id = id;
        this.isPrimaryCopy = isPrimaryCopy;
        this.vector = vector;
    }

    @Nonnull
    public VectorId getId() {
        return id;
    }

    public boolean isPrimaryCopy() {
        return isPrimaryCopy;
    }

    @Nonnull
    public Transformed<RealVector> getVector() {
        return vector;
    }

    @Nonnull
    public VectorReference toPrimaryCopy() {
        return withPrimaryCopy(true);
    }

    @Nonnull
    public VectorReference toReplicatedCopy() {
        return withPrimaryCopy(false);
    }

    @Nonnull
    public VectorReference withPrimaryCopy(final boolean isPrimaryCopy) {
        if (isPrimaryCopy() == isPrimaryCopy) {
            return this;
        }
        return new VectorReference(getId(), isPrimaryCopy, getVector());
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final VectorReference that = (VectorReference)o;
        return Objects.equals(getId(), that.getId()) && Objects.equals(getVector(), that.getVector());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getVector());
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
            return new VectorReference(vectorReference.getId(), vectorReference.isPrimaryCopy(),
                    Objects.requireNonNull(transformed));
        }
    }
}
