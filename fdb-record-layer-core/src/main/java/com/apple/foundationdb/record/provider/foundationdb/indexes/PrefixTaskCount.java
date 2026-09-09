/*
 * PrefixTaskCount.java
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

import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * One entry of a {@link VectorIndexTaskCounts} snapshot: a partition {@code prefix} paired with the number of deferred
 * maintenance tasks currently outstanding for it. A merge streams these (only the positive ones) to decide how much
 * work each partition has and to bound how many tasks it drains there.
 *
 * @param prefix the partition prefix (empty for an unpartitioned index)
 * @param count the number of outstanding tasks recorded for {@code prefix} (always positive when produced by
 *        {@link VectorIndexTaskCounts#prefixesWithOutstandingWork})
 */
record PrefixTaskCount(@Nonnull Tuple prefix, long count) {
}
