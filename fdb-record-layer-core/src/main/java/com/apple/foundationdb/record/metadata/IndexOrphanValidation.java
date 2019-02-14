/*
 * IndexOrphanValidation.java
 *
 * This source file is part of the FoundationDB open source project
 *  
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Validate entries in the index. It scans the index and checks if the record associated with each index entry exists.
 */
@API(API.Status.EXPERIMENTAL)
public class IndexOrphanValidation {

    private IndexOrphanValidation() {}

    public static CompletableFuture<List<IndexEntry>> validate(@Nonnull FDBDatabaseRunner runner, @Nonnull IndexMaintainer indexMaintainer) {
        return validate(runner, indexMaintainer, null);
    }

    @VisibleForTesting
    public static CompletableFuture<List<IndexEntry>> validate(@Nonnull FDBDatabaseRunner runner,
                                                               @Nonnull IndexMaintainer indexMaintainer,
                                                               @Nullable Integer scannedRecordsLimit) {
        List<IndexEntry> results = new ArrayList<>();
        return validateInternal(runner, indexMaintainer, null, scannedRecordsLimit, results)
                .thenApply(ignore -> results);
    }

    private static CompletableFuture<byte[]> validateInternal(@Nonnull FDBDatabaseRunner runner,
                                                              @Nonnull IndexMaintainer indexMaintainer,
                                                              @Nullable byte[] previousContinuation,
                                                              @Nullable Integer scannedRecordsLimit,
                                                              @Nonnull List<IndexEntry> results) {
        final FDBRecordContext context = runner.openContext();

        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT);
        if (scannedRecordsLimit != null) {
            executeProperties.setScannedRecordsLimit(scannedRecordsLimit);
        }
        final ScanProperties scanProperties = new ScanProperties(executeProperties.build());

        final RecordCursor<IndexEntry> cursor = indexMaintainer.validateOrphanEntries(IndexScanType.BY_VALUE, TupleRange.ALL, previousContinuation, scanProperties);

        CompletableFuture<byte[]> nextContinuation = AsyncUtil.whileTrue(() ->
                cursor.onHasNext().thenApply(hasNext -> {
                    if (hasNext) {
                        results.add(cursor.next());
                    }
                    return hasNext;
                }), context.getExecutor()
        ).thenApply(ignore -> cursor.getContinuation());

        return nextContinuation.thenCompose(continuation -> {
            context.close();
            if (continuation != null) {
                return validateInternal(runner, indexMaintainer, continuation, scannedRecordsLimit, results);
            }
            return CompletableFuture.completedFuture(null);
        });
    }
}
