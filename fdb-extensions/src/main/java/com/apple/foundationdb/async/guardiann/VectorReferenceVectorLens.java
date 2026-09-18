/*
 * VectorReferenceVectorLens.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.util.Lens;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Lens to access the underlying vector of a {@link VectorReference} in logic that can be called for containers of both
 * vectors and transformed vectors. Backs {@link VectorReference#vectorLens()}.
 */
class VectorReferenceVectorLens implements Lens<VectorReference, Transformed<RealVector>> {
    static final Lens<VectorReference, RealVector> VECTOR_LENS =
            new VectorReferenceVectorLens().compose(Transformed.underlyingLens());

    @Nullable
    @Override
    public Transformed<RealVector> get(@Nonnull final VectorReference vectorReference) {
        return vectorReference.vector();
    }

    /**
     * Replaces the vector on {@code vectorReference}, preserving its role, id, and flags. Both arguments must be
     * non-null: a lens can only rewrite the vector of an existing reference, not synthesize one from a vector alone
     * (the role and id have no source outside the reference). Delegates to
     * {@link VectorReference#withVector(Transformed)}, which returns the same reference when {@code transformed} is the
     * identical instance already held.
     *
     * @param vectorReference the reference to rewrite; must not be null
     * @param transformed the replacement vector; must not be null
     *
     * @return the reference carrying {@code transformed}
     */
    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public VectorReference set(@Nullable final VectorReference vectorReference,
                               @Nullable final Transformed<RealVector> transformed) {
        Objects.requireNonNull(vectorReference);
        return vectorReference.withVector(Objects.requireNonNull(transformed));
    }
}
