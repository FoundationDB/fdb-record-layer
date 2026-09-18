/*
 * VectorRecord.java
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

import javax.annotation.Nonnull;

/**
 * A fully enriched search result: a vector's {@link VectorMetadata} together with its stored vector data and the
 * distance computed to the query vector. This is the terminal result type produced once a search candidate has
 * survived the staleness check and had its metadata resolved.
 *
 * <p>
 * It is the post-enrichment counterpart to {@link VectorReferenceAndDistance}: the latter pairs a not-yet-enriched
 * {@link VectorReference} with a distance while candidates are still being ranked, whereas a {@code VectorRecord}
 * carries the resolved {@link VectorMetadata} (including any covering values) needed to produce a client-facing
 * result.
 *
 * @param vectorMetadata the resolved metadata (identity plus any covering values) for this vector
 * @param vector the vector data in the transformed (stored) coordinate space; reconstructed into the client's
 *        coordinate space during result post-processing
 * @param distance the computed distance between this vector and the query vector
 */
record VectorRecord(@Nonnull VectorMetadata vectorMetadata, @Nonnull Transformed<RealVector> vector, double distance) {
}
