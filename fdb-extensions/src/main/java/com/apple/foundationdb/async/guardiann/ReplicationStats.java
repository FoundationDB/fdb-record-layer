/*
 * ReplicationStats.java
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
 * The running trace counters accumulated across the per-primary replication passes (used only for logging): the
 * {@link RunningStats} of replication-priority scores, the number of replicas written, and the number of candidates
 * skipped due to occlusion. Folded across primaries via {@link #combine} so the per-primary replication selection can
 * be a pure function.
 *
 * @param replicationPriorityStandardDeviation running statistics over the replication-priority scores
 * @param numReplicated the number of replicas written
 * @param numOccluded the number of candidates skipped because they were occluded
 */
record ReplicationStats(@Nonnull RunningStats replicationPriorityStandardDeviation,
                        int numReplicated, int numOccluded) {
    /** Returns the empty accumulator (no replicas, no occlusions, empty priority stats). */
    @Nonnull
    static ReplicationStats identity() {
        return new ReplicationStats(RunningStats.identity(), 0, 0);
    }

    /**
     * Combines two accumulators: the priority statistics are merged via {@link RunningStats#combine} and the
     * replica/occlusion counts are summed.
     *
     * @param other the accumulator to combine with
     * @return a new accumulator representing the combined counters
     */
    @Nonnull
    ReplicationStats combine(@Nonnull final ReplicationStats other) {
        return new ReplicationStats(
                replicationPriorityStandardDeviation.combine(other.replicationPriorityStandardDeviation()),
                numReplicated + other.numReplicated(),
                numOccluded + other.numOccluded());
    }
}
