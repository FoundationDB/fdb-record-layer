/*
 * ClusterMetadataForUpdate.java
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
 * A cluster's {@link ClusterMetadata} together with permission to record the next change to it as an appended
 * {@link ClusterMetadataDelta} rather than a rewrite of the whole value.
 * <p>
 * Deliberately a <em>decision</em> and not the raw storage facts it is derived from. The pending-delta count and the
 * stored value's length are physical details of one key; they are consumed where they are known — inside
 * {@link Primitives#fetchClusterMetadataForUpdate} — and only the verdict travels to the writer. That keeps byte-level
 * concerns out of the records that carry cluster metadata around the insert and search paths.
 * <p>
 * {@link #wholeValueOnly} exists so a caller that holds metadata without having read it in this transaction can say
 * so truthfully instead of fabricating facts. Appending is a privilege that has to be granted by a read.
 *
 * @param clusterMetadata the metadata, with every appended delta already folded in
 * @param mayAppendDelta whether one more delta may be appended to the stored value: {@code false} means the delta log
 *        is at its configured length, or the value is close enough to the size ceiling that another append could be
 *        silently dropped, so the change must be folded in and the value rewritten
 */
record ClusterMetadataForUpdate(@Nonnull ClusterMetadata clusterMetadata, boolean mayAppendDelta) {
    /**
     * Wraps metadata that carries no append permission, because whoever holds it did not learn the state of the
     * stored value — a deferred task working from metadata it built itself, for instance. Changes recorded against it
     * are always written as a whole value.
     *
     * @param clusterMetadata the metadata
     * @return a view that forbids appending
     */
    @Nonnull
    static ClusterMetadataForUpdate wholeValueOnly(@Nonnull final ClusterMetadata clusterMetadata) {
        return new ClusterMetadataForUpdate(clusterMetadata, false);
    }
}
