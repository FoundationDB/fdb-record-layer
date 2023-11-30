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
import java.util.List;
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
    private long mergesLimit;
    private int mergeSuccesses = 0;
    private final IndexingCommon common;

    public IndexingMerger(final Index index,  IndexingCommon common, long initialMergesCountLimit) {
        this.index = index;
        this.common = common;
        this.mergesLimit = initialMergesCountLimit;
    }

    private CompletableFuture<FDBRecordStore> openRecordStore(@Nonnull FDBRecordContext context) {
        return common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync();
    }

    @SuppressWarnings("squid:S3776") // cognitive complexity is high, candidate for refactoring
    CompletableFuture<Void> mergeIndex() {
        final AtomicInteger failureCountLimit = new AtomicInteger(1000);
        AtomicReference<IndexDeferredMaintenanceControl> mergeControlRef = new AtomicReference<>();
        AtomicReference<Runnable> recordTime = new AtomicReference<>();
        return AsyncUtil.whileTrue(() ->
                common.getRunner().runAsync(context -> openRecordStore(context)
                                .thenCompose(store -> {
                                    long startTime = System.nanoTime();
                                    recordTime.set(() -> context.record(FDBStoreTimer.Events.MERGE_INDEX, System.nanoTime() - startTime));
                                    final IndexDeferredMaintenanceControl mergeControl = store.getIndexDeferredMaintenanceControl();
                                    mergeControlRef.set(mergeControl);
                                    mergeControl.setMergesLimit(mergesLimit);
                                    return store.getIndexMaintainer(index).mergeIndex();
                                }).thenApply(ignore -> false),
                        Pair::of,
                        common.indexLogMessageKeyValues()
                ).handle((ignore, e) -> {
                    recordTime.get().run();
                    final IndexDeferredMaintenanceControl mergeControl = mergeControlRef.get();
                    if (e == null) {
                        if (mergesLimit > 0 && mergeSuccesses > 2) {
                            mergeSuccesses = 0;
                            mergesLimit = (mergesLimit * 5) / 4; // increase 25%, case there was an isolated issue
                        }
                        mergeSuccesses++;
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(KeyValueLogMessage.build("IndexMerge: Success")
                                    .addKeysAndValues(mergerKeysAndValues(mergeControl))
                                    .toString());
                        }
                        // Here: no error, stop the iteration unless has more
                        final boolean hasMore = mergeControl.getMergesFound() > mergeControl.getMergesTried();
                        return hasMore ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
                    }
                    // Here: got exception.
                    if (0 > failureCountLimit.decrementAndGet() || mergeControl.getMergesTried() < 2) {
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn(KeyValueLogMessage.build("IndexMerge: Gave up merge dilution")
                                            .addKeysAndValues(mergerKeysAndValues(mergeControl))
                                            .toString(), e);
                        }
                    } else {
                        final FDBException ex = IndexingBase.findException(e, FDBException.class);
                        if (IndexingBase.shouldLessenWork(ex)) {
                            // Here: this exception might be resolved by reducing the load
                            mergesLimit = mergeControl.getMergesTried() / 2;
                            if (LOGGER.isInfoEnabled()) {
                                // TODO: demote this info message to a trace or debug after this code is tested a bit
                                LOGGER.info(KeyValueLogMessage.build("IndexMerge: Merges diluted")
                                        .addKeysAndValues(mergerKeysAndValues(mergeControl))
                                        .toString(), e);
                            }
                            return AsyncUtil.READY_TRUE; // and retry
                        }
                    }
                    // Here: this exception will not be recovered by dilution. Throw it.
                    throw common.getRunner().getDatabase().mapAsyncToSyncException(e);
                }).thenCompose(Function.identity()
                ), common.getRunner().getExecutor());
    }

    List<Object> mergerKeysAndValues(final IndexDeferredMaintenanceControl mergeControl) {
        return List.of(
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_MERGES_LIMIT, mergeControl.getMergesLimit(),
                    LogMessageKeys.INDEX_MERGES_FOUND, mergeControl.getMergesFound(),
                    LogMessageKeys.INDEX_MERGES_TRIED, mergeControl.getMergesTried()
        );
    }

}
