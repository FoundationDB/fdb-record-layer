/*
 * MaintenanceControlRegister.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;

import javax.annotation.Nonnull;

/**
 * A {@link TaskEventRegister} that flags its index as needing a background merge — through the record store's
 * {@link IndexDeferredMaintenanceControl#setMergeRequiredIndexes(Index)} — the first time a deferred maintenance task
 * is enqueued during a write. A caller's commit hook then reads {@link IndexDeferredMaintenanceControl#getMergeRequiredIndexes()}
 * and schedules the merge (this is how Lucene indexes already signal). The maintainer constructs it (it holds the store
 * and index), so the vector engine that fires the callback stays decoupled from the record store. Executing a queued
 * task pays down merge work rather than creating it, so {@link #onTaskExecuted} is a no-op.
 */
final class MaintenanceControlRegister implements TaskEventRegister {
    @Nonnull
    private final IndexDeferredMaintenanceControl mergeControl;
    @Nonnull
    private final Index index;
    // A single write may enqueue tasks from several executor threads; volatile makes the flip promptly visible to
    // them. The check-then-set is not atomic, so a rare race may signal twice — harmless, as setMergeRequiredIndexes
    // is synchronized and idempotent.
    private volatile boolean signaled;

    MaintenanceControlRegister(@Nonnull final IndexDeferredMaintenanceControl mergeControl, @Nonnull final Index index) {
        this.mergeControl = mergeControl;
        this.index = index;
    }

    @Override
    public void onTaskEnqueued(@Nonnull final Transaction transaction) {
        if (!signaled) {
            signaled = true;
            mergeControl.setMergeRequiredIndexes(index);
        }
    }

    @Override
    public void onTaskExecuted(@Nonnull final Transaction transaction) {
        // No-op: executing a queued task drains merge work, it does not create it.
    }
}
