/*
 * IndexingMerger.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Implement an index merge.
 * This can be either used for an explicit index merge call or during online indexing. In the second case,
 * keeping throttle values between calls has an overall impact on performance.
 */
@ParametersAreNonnullByDefault
public class IndexingMerger {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingMerger.class);
    private final Index index;
    private long mergesLimit = 0;
    private int mergeSuccesses = 0;
    private final IndexingCommon common;

    // These error codes represent a list of errors that can occur if there is too much work to be done
    // in a single transaction.
    private static final Set<Integer> lessenWorkCodes = new HashSet<>(Arrays.asList(
            FDBError.TIMED_OUT.code(),
            FDBError.TRANSACTION_TOO_OLD.code(),
            FDBError.NOT_COMMITTED.code(),
            FDBError.TRANSACTION_TIMED_OUT.code(),
            FDBError.COMMIT_READ_INCOMPLETE.code(),
            FDBError.TRANSACTION_TOO_LARGE.code()));

    public IndexingMerger(final Index index,  IndexingCommon common) {
        this.index = index;
        this.common = common;
    }

    protected CompletableFuture<FDBRecordStore> openRecordStore(@Nonnull FDBRecordContext context) {
        return common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync();
    }

    @SuppressWarnings("squid:S3776") // cognitive complexity is high, candidate for refactoring
    CompletableFuture<Void> mergeIndex() {
        final AtomicInteger iterationLimit = new AtomicInteger(1000);
        AtomicReference<IndexDeferredMaintenancePolicy> policyRef = new AtomicReference<>();
        return AsyncUtil.whileTrue(() ->
                common.getRunner().runAsync(context -> openRecordStore(context)
                                .thenCompose(store -> {
                                    final IndexDeferredMaintenancePolicy policy = store.getIndexDeferredMaintenancePolicy();
                                    policyRef.set(policy);
                                    policy.setMergesLimit(mergesLimit);
                                    return store.getIndexMaintainer(index).mergeIndex();
                                }).thenApply(ignore -> false),
                        Pair::of,
                        common.indexLogMessageKeyValues()
                ).handle((ignore, e) -> {
                    final IndexDeferredMaintenancePolicy policy = policyRef.get();
                    if (e == null) {
                        if (mergesLimit > 0 && mergeSuccesses > 3) {
                            mergeSuccesses = 0;
                            mergesLimit = (mergesLimit * 5) / 4; // increase 25%, case there was an isolated issue
                        }
                        mergeSuccesses++;
                        // Here: no error, stop the iteration unless has more
                        tellLogger("Success", policy);
                        final boolean hasMore = policy.getMergesFound() > policy.getMergesTried();
                        return hasMore ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
                    }
                    // Here: got exception.
                    if (0 > iterationLimit.decrementAndGet() || policy.getMergesTried() < 2) {
                        tellLogger("Gave up merge dilution", policy);
                    } else {
                        final FDBException ex = IndexingBase.findException(e, FDBException.class);
                        if (ex != null && !lessenWorkCodes.contains(ex.getCode())) {
                            // Here: this exception might be resolved by reducing the load
                            mergesLimit = policy.getMergesTried() / 2;
                            tellLogger("Merges diluted", policy);
                            return AsyncUtil.READY_TRUE; // and retry
                        }
                    }
                    // Here: this exception will not be recovered by dilution. Throw it.
                    throw common.getRunner().getDatabase().mapAsyncToSyncException(e);
                }).thenCompose(Function.identity()
                ), common.getRunner().getExecutor());
    }

    void tellLogger(String msg, final IndexDeferredMaintenancePolicy policy) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("IndexMerge: " + msg,
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_MERGES_LIMIT, policy.getMergesLimit(),
                    LogMessageKeys.INDEX_MERGES_FOUND, policy.getMergesFound(),
                    LogMessageKeys.INDEX_MERGES_TRIED, policy.getMergesTried()
                    ));
        }
    }

}
