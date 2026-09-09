/*
 * VectorIndexSecondarySubspaceKeys.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

/**
 * Compact numeric prefixes that segregate the kinds of data the vector index keeps in its <em>secondary</em>
 * subspace: the outstanding-work task counters ({@link VectorIndexTaskCounts}) and the per-partition merge lease plus
 * its delete-guard ({@link VectorIndexMergeLock}). Each kind decorates the index's secondary subspace with its prefix
 * (e.g. {@code indexSecondarySubspace.subspace(Tuple.from(TASK_COUNTS))}).
 * <p>
 * They are gathered here — sequential and in one place — for two reasons. First, a small integer packs into ~1 byte on
 * every key, whereas a descriptive word (the prefixes used to be {@code "taskCount"}, {@code "mergeLock"}, …) repeats
 * ~10–20 bytes into every key. Second, keeping them together makes a clash between two kinds, or a gap, obvious on
 * review rather than hidden across files. This mirrors {@code guardiann}'s {@code StorageAdapter}.
 * <p>
 * These values are part of the on-disk key layout. They live in the secondary subspace only; the vector structure's
 * own keys (guardiann/HNSW) live in the primary/partition subspace, a separate key tree, so this numbering is
 * independent of theirs.
 */
final class VectorIndexSecondarySubspaceKeys {
    /** Per-partition outstanding-task counters (was {@code "taskCount"}). */
    static final long TASK_COUNTS = 0x00L;
    /** Per-partition merge lease (was {@code "mergeLock"}). */
    static final long MERGE_LOCK = 0x01L;
    /** Index-wide merge delete-guard conflict key (was {@code "mergeLockDeleteGuard"}). */
    static final long MERGE_LOCK_DELETE_GUARD = 0x02L;

    private VectorIndexSecondarySubspaceKeys() {
    }
}
