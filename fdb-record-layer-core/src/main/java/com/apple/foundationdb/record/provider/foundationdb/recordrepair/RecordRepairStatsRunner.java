/*
 * RecordRepairStatsRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.runners.throttled.ThrottledRetryingIterator;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A record repair runner that returns an aggregate of all the issues found.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordRepairStatsRunner extends RecordRepair {
    @Nonnull
    private final RepairStatsResults statsResult;

    RecordRepairStatsRunner(@Nonnull final Builder config) {
        // stats runner never commits a transaction
        super(config, false);
        statsResult = new RepairStatsResults();
    }

    /**
     * Run the validation operation.
     * @return a future that completes once the iteration completes.
     */
    public CompletableFuture<RepairStatsResults> run() {
        return iterateAll().handle((ignoreVoid, ex) -> {
            if (ex != null) {
                statsResult.setExceptionCaught(ex);
            }
            return statsResult;
        });
    }

    @Nonnull
    @Override
    protected CompletableFuture<Void> handleOneItem(@Nonnull FDBRecordStore store, @Nonnull RecordCursorResult<Tuple> lastResult, @Nonnull ThrottledRetryingIterator.QuotaManager quotaManager) {
        return validateInternal(lastResult, store, false).thenAccept(result -> {
            statsResult.increment(result.getErrorCode());
        });
    }
}
