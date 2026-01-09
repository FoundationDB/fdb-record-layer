/*
 * StorageTransform.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.LinearOperator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.VectorOperator;
import com.apple.foundationdb.rabitq.EncodedRealVector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A special affine operator that uses a random rotator seeded by the current {@link AccessInfo} and a given
 * (pre-rotated) centroid. This operator is used inside the HNSW to transform back and forth between the coordinate
 * system of the client and the coordinate system that is currently employed in the HNSW.
 */
@SpotBugsSuppressWarnings(value = "SING_SINGLETON_HAS_NONPRIVATE_CONSTRUCTOR", justification = "Singleton designation is a false positive")
class StorageTransform implements VectorOperator {
    private static final StorageTransform IDENTITY_STORAGE_TRANSFORM =
            new StorageTransform(null, null, false);

    @Nonnull
    private final AffineOperator affineOperator;
    private final boolean normalizeVectors;

    public StorageTransform(@Nullable final LinearOperator linearOperator,
                            @Nullable final RealVector translationVector,
                            final boolean normalizeVectors) {
        this.affineOperator = new AffineOperator(linearOperator, translationVector);
        this.normalizeVectors = normalizeVectors;
    }

    @Override
    public int getNumDimensions() {
        return affineOperator.getNumDimensions();
    }

    @Nonnull
    @Override
    public RealVector apply(@Nonnull final RealVector vector) {
        //
        // Only transform the vector if it is needed. We make the decision based on whether the vector is encoded or
        // not. When we switch on encoding, we apply the new coordinate system from that point onwards meaning that all
        // vectors inserted before use the client coordinate system. Therefore, we must transform all regular vectors
        // and ignore all encoded vectors.
        //
        // TODO This could be done better in the future by keeping something like a generation id with the vector
        //      so we would know in what coordinate system the vector is.
        if (vector instanceof EncodedRealVector) {
            return vector;
        }
        return affineOperator.apply(normalizeVectors ? vector.normalize() : vector);
    }

    @Nonnull
    @Override
    public RealVector invertedApply(@Nonnull final RealVector vector) {
        return affineOperator.invertedApply(vector);
    }

    @Nonnull
    public static StorageTransform identity() {
        return IDENTITY_STORAGE_TRANSFORM;
    }
}
