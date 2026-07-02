/*
 * PendingWriteQueueIndexingFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.queue.PendingWritesQueue;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Factory for the {@link PendingWritesQueue} used by the online indexer to defer index updates while an index is
 * being built in the {@link com.apple.foundationdb.record.IndexState#WRITE_ONLY_WITH_QUEUE} state. This keeps the
 * indexing-specific queue wiring (subspaces, payload type, capacity) in one place so producers (the index maintainer)
 * and consumers (the drainer) always agree on it.
 */
@ParametersAreNonnullByDefault
public final class PendingWriteQueueIndexingFactory {
    // TODO: configurable maxQueueSize
    private static final int MAX_QUEUE_SIZE = 100_000;

    private PendingWriteQueueIndexingFactory() {
    }

    @Nonnull
    public static PendingWritesQueue<IndexBuildProto.PendingWritesQueueEntry> getIndexingQueue(final FDBRecordStore store, final Index index) {
        return new PendingWritesQueue<>(
                IndexingSubspaces.indexWritePendingQueueSubspace(store, index),
                IndexingSubspaces.indexWritePendingQueueSizeSubspace(store, index),
                MAX_QUEUE_SIZE,
                IndexBuildProto.PendingWritesQueueEntry.class
        );
    }
}
