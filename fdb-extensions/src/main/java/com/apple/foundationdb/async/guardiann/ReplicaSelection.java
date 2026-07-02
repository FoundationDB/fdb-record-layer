/*
 * ReplicaSelection.java
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

import com.google.common.collect.ImmutableListMultimap;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * The per-primary output of replication selection: the replicas to place keyed by destination cluster id, together
 * with this primary's contribution to the replication trace counters. Pure data — the caller decides how to route the
 * placements into its assignment builders (e.g. directly, or via a bounded {@link TopK} for newly-minted clusters).
 *
 * @param replicasByCluster the replicas to place, keyed by destination cluster id
 * @param stats this primary's contribution to the replication trace counters
 */
record ReplicaSelection(@Nonnull ImmutableListMultimap<UUID, VectorReference> replicasByCluster,
                        @Nonnull ReplicationStats stats) {
}
