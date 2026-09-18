/*
 * VectorReferenceAndDistance.java
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

import javax.annotation.Nonnull;

/**
 * Pairs a {@link VectorReference} with its computed distance to a query vector. Used during search
 * to rank candidate vectors and collect the top-K nearest results.
 *
 * @param vectorReference the vector reference from a cluster
 * @param distance the computed distance between this vector and the query vector
 */
record VectorReferenceAndDistance(@Nonnull VectorReference vectorReference, double distance) {

    /**
     * Returns a copy of this pair with a different vector reference, preserving the same distance.
     * Used when expanding collapsed references — all expanded entries share the same distance since
     * the underlying vector data is identical.
     *
     * @param vectorReference the replacement vector reference
     * @return a new pair with the given reference and the original distance
     */
    @Nonnull
    public VectorReferenceAndDistance withVectorReference(@Nonnull final VectorReference vectorReference) {
        return new VectorReferenceAndDistance(vectorReference, distance());
    }
}
