/*
 * ClusterCandidateForUpdate.java
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
 * A candidate cluster for an insert: the same triple as {@link ClusterMetadataWithDistance}, except that the metadata
 * arrives as a {@link ClusterMetadataForUpdate} and therefore carries permission to record the insert's change as an
 * appended {@link ClusterMetadataDelta}.
 * <p>
 * This exists as its own type rather than as an extra field on {@link ClusterMetadataWithDistance} because append
 * permission is meaningful only where a write follows a read in the same transaction. Search and the deferred tasks
 * pass cluster metadata around constantly and have no use for it; giving them a component they must supply — and a
 * nullable one at that — would push a storage concern into paths that have nothing to do with storage.
 *
 * @param forUpdate the cluster's metadata together with its append permission
 * @param centroid the transformed centroid of the cluster
 * @param distance the distance from the vector being inserted to {@code centroid}
 */
record ClusterCandidateForUpdate(@Nonnull ClusterMetadataForUpdate forUpdate,
                                 @Nonnull Transformed<RealVector> centroid,
                                 double distance) {
    /**
     * Returns the cluster's metadata. A delegating accessor rather than a component of its own, so the metadata
     * cannot drift from the permission that was granted for it.
     *
     * @return the metadata
     */
    @Nonnull
    ClusterMetadata clusterMetadata() {
        return forUpdate().clusterMetadata();
    }
}
