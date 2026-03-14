/*
 * RecordRepairValidateRunner.java
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A repair runner that can run validation and repairs on a store's records.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordRepairValidateRunner extends RecordRepair {
    private final boolean allowRepair;
    private final int maxResultsReturned;

    @Nonnull
    private final List<RecordRepairResult> invalidResults;
    @Nonnull
    private final AtomicInteger validResultCount;
    @Nonnull
    private final AtomicBoolean earlyReturn;

    RecordRepairValidateRunner(@Nonnull final Builder config, boolean allowRepair) {
        super(config, allowRepair);
        this.allowRepair = allowRepair;
        this.maxResultsReturned = config.getMaxResultsReturned();

        invalidResults = new ArrayList<>();
        validResultCount = new AtomicInteger(0);
        earlyReturn = new AtomicBoolean(false);
    }

    /**
     * Run the validation operation.
     * @return a future that completes once the iteration completes.
     */
    public CompletableFuture<RepairValidationResults> run() {
        return iterateAll().handle((ignoreVoid, ex) -> {
            if (ex != null) {
                return new RepairValidationResults(false, ex, invalidResults, validResultCount.get());
            }
            return new RepairValidationResults(!earlyReturn.get(), null, invalidResults, validResultCount.get());
        });
    }

    @Nonnull
    @Override
    protected CompletableFuture<Void> handleOneItem(@Nonnull FDBRecordStore store, @Nonnull RecordCursorResult<Tuple> primaryKey, @Nonnull ThrottledRetryingIterator.QuotaManager quotaManager) {
        return validateInternal(primaryKey, store, allowRepair).thenAccept(result -> {
            if (result.isValid()) {
                validResultCount.incrementAndGet();
            } else {
                invalidResults.add(result);
                if ((maxResultsReturned > 0) && (invalidResults.size() >= maxResultsReturned)) {
                    quotaManager.markExhausted();
                    earlyReturn.set(true);
                }
                // Mark record as deleted
                if (result.isRepaired() && RecordRepairResult.REPAIR_RECORD_DELETED.equals(result.getRepairCode())) {
                    quotaManager.deleteCountAdd(1);
                }
            }
        });
    }
}
