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

import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.rabitq.EncodedRealVector;

import javax.annotation.Nonnull;

/**
 * A special affine operator that uses a random rotator seeded by the current {@link AccessInfo} and a given
 * (pre-rotated) centroid. This operator is used inside the HNSW to transform back and forth between the coordinate
 * system of the client and the coordinate system that is currently employed in the HNSW.
 */
class StorageTransform extends AffineOperator {
    public StorageTransform(final long seed, final int numDimensions, @Nonnull final RealVector translationVector) {
        super(new FhtKacRotator(seed, numDimensions, 10), translationVector);
    }

    @Nonnull
    @Override
    public RealVector apply(@Nonnull final RealVector vector) {
        if (!(vector instanceof EncodedRealVector)) {
            return vector;
        }
        return super.apply(vector);
    }

    @Nonnull
    @Override
    public RealVector invertedApply(@Nonnull final RealVector vector) {
        if (vector instanceof EncodedRealVector) {
            return vector;
        }
        return super.invertedApply(vector);
    }
}
