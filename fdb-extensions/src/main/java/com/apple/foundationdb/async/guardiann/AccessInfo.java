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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.FhtKacRotator;
import com.apple.foundationdb.linear.RealVector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A small immutable bundle of the coordinate-system-transformation parameters a GuardiANN structure needs to map
 * vectors between the client's coordinate space and the stored one. This is the extra state needed to build the
 * {@link StorageTransform} that is applied before insertion/before deletion and after retrieval (if necessary). It is
 * neither schema/metadata nor the stored vector data itself.
 *
 * <p>
 * The transform exists to make RaBitQ quantization more effective; when RaBitQ is not in use it is the identity and
 * these parameters carry no rotation and no centroid. RaBitQ's distance estimates are tightest when a vector's
 * information is spread evenly across its dimensions and when its magnitude is small, and the two operations below
 * attempt exactly that:
 * <ul>
 *   <li><b>Rotation</b> — a pseudo-random orthogonal rotation ({@link FhtKacRotator}, reconstructed from
 *       {@code rotatorSeed}). Because it is orthogonal it preserves distances, so it never changes nearest-neighbor
 *       relationships; it only decorrelates the coordinates and spreads each vector's information across all
 *       dimensions, which is what gives RaBitQ's per-dimension encoding tighter error bounds. Rotation
 *       is distance-preserving under any metric.</li>
 *   <li><b>Translation</b> — subtracting the data centroid (mean-centering), which shrinks each vector's magnitude and
 *       so tightens RaBitQ's estimates. Unlike rotation, translation is only valid for a metric that is preserved
 *       under it: Euclidean distance is (shifting both points by the same vector leaves the distance unchanged), but
 *       the cosine metric is not (a shift changes the vectors' directions, hence their angles). So under the cosine
 *       metric no centroid is applied and vectors are left un-translated. Note that we mostly refer to a
 *       <em>negated</em> centroid in this class which is necessary as the affine operator we us under the cover
 *       defines translation to be the addition of vectors, however, the global centroid needs to be subtracted.</li>
 * </ul>
 *
 * @param rotatorSeed a seed used to reconstruct the random rotator {@link FhtKacRotator} used by the
 *        {@link StorageTransform}
 * @param negatedCentroid the centroid to translate vectors around, stored in negated form ({@code centroid * -1}) so
 *        it can be passed directly to the {@link AffineOperator}, which adds its translation vector. Equivalent to
 *        subtracting the original centroid. {@code null} means "do not translate" (a zero vector would work but would
 *        incur a full-length addition on every transform for no effect); a {@code null} centroid is also what makes
 *        RaBitQ unavailable (see {@link #canUseRaBitQ()}).
 */
record AccessInfo(long rotatorSeed, @Nullable RealVector negatedCentroid) {
    /**
     * Returns the negated centroid and enforces non-nullness. Callers should always probe {@link #canUseRaBitQ()}
     * before accessing this field.
     * @return a vector which is the negated centroid used for translation
     */
    @Nonnull
    public RealVector negatedCentroid() {
        return Objects.requireNonNull(negatedCentroid);
    }

    /**
     * Indicates whether RaBitQ quantization can be used for this access context, which requires a centroid to build
     * the {@link StorageTransform}.
     *
     * @return {@code true} if a centroid is available and RaBitQ can be used
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean canUseRaBitQ() {
        return negatedCentroid != null;
    }

    @Nonnull
    @Override
    public String toString() {
        return "AccessInfo[" +
                "rotatorSeed=" + rotatorSeed +
                ", negatedCentroid=" + negatedCentroid + "]";
    }
}
