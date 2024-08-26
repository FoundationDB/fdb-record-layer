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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    private long timeQuotaMillis = 0;
    private final IndexingCommon common;
    private SubspaceProvider subspaceProvider = null;
    private int repartitionDocumentCount = 0;
    private int repartitionSecondChances = 0;

    public IndexingMerger(final Index index,  IndexingCommon common, long initialMergesCountLimit) {
        this.index = index;
        this.common = common;
        this.mergesLimit = initialMergesCountLimit;
    }

    private CompletableFuture<FDBRecordStore> openRecordStore(@Nonnull FDBRecordContext context) {
        return common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync();
    }

    @SuppressWarnings("squid:S3776") // cognitive complexity is high, candidate for refactoring
    CompletableFuture<Void> mergeIndex(@Nullable SubspaceProvider subspaceProvider) {
        this.subspaceProvider = subspaceProvider; // for logs only
        final AtomicInteger failureCountLimit = new AtomicInteger(1000);
        AtomicReference<IndexDeferredMaintenanceControl> mergeControlRef = new AtomicReference<>();
        final FDBStoreTimer timer = common.getRunner().getTimer();
        AtomicLong mergeStartTime = new AtomicLong();
        return AsyncUtil.whileTrue(() ->
                // Merge operation may take a long time, hence the runner's context must be a read-only. Ensure that it
                // isn't a synchronized one, which may attempt a heartbeat write
                // Note: this runAsync will retry according to the runner's "maxAttempts" setting
                common.getNonSynchronizedRunner().runAsync(context -> openRecordStore(context)
                                .thenCompose(store -> {
                                    mergeStartTime.set(System.nanoTime());
                                    final IndexDeferredMaintenanceControl mergeControl = store.getIndexDeferredMaintenanceControl();
                                    mergeControlRef.set(mergeControl);
                                    mergeControl.setMergesLimit(mergesLimit);
                                    mergeControl.setTimeQuotaMillis(timeQuotaMillis);
                                    mergeControl.setRepartitionDocumentCount(repartitionDocumentCount);
                                    mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.NONE);
                                    mergeControl.setRepartitionCapped(false);
                                    return store.getIndexMaintainer(index).mergeIndex();
                                }).thenApply(ignore -> false),
                        Pair::of,
                        common.indexLogMessageKeyValues()
                ).handle((ignore, e) -> {
                    if (timer != null && mergeStartTime.get() > 0) {
                        timer.recordSinceNanoTime(FDBStoreTimer.Events.MERGE_INDEX, mergeStartTime.get());
                    }
                    mergeStartTime.set(0);
                    final IndexDeferredMaintenanceControl mergeControl = mergeControlRef.get();
                    // Note: this mergeControl will not be re-used and should not be modified.
                    if (e == null) {
                        // Here: no errors
                        return handleSuccess(mergeControl);
                    }
                    if (mergeControl == null) {
                        // Here: very early exception that has nothing to do with the merge
                        throw common.getRunner().getDatabase().mapAsyncToSyncException(e);
                    }
                    if (0 > failureCountLimit.decrementAndGet()) {
                        // Here: too many retries, unconditionally give up
                        giveUpMerging(mergeControl, e);
                    }
                    return handleFailure(mergeControl, e);
                }).thenCompose(Function.identity()
                ), common.getRunner().getExecutor());
    }

    private CompletableFuture<Boolean> handleSuccess(final IndexDeferredMaintenanceControl mergeControl) {
        if (mergesLimit > 0 && mergeSuccesses > 2) {
            mergeSuccesses = 0;
            mergesLimit = (mergesLimit * 5) / 4; // increase 25%, case there was an isolated issue
        }
        mergeSuccesses++;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(mergerLogMessage("IndexMerge: Success", mergeControl));
        }
        // after a successful merge, reset the time quota and document count. They are unlikely to be applicable for the next merge.
        timeQuotaMillis = 0;
        if (repartitionDocumentCount > 0) {
            repartitionDocumentCount = 0;
        }
        // Here: no errors, stop the iteration unless has more
        final boolean hasMore =
                shouldGiveRepartitionSecondChance(mergeControl) ||
                        mergeControl.getMergesFound() > mergeControl.getMergesTried();
        return  hasMore ? AsyncUtil.READY_TRUE : AsyncUtil.READY_FALSE;
    }

    private CompletableFuture<Boolean> handleFailure(final IndexDeferredMaintenanceControl mergeControl, Throwable e) {
        // Here: mergeIndex was iterating over grouping keys and partitions, this mergeControl object
        // reflects the state of the last merge attempt that had failed. The values are adjusted according to it entity
        // that had failed, assuming that a retry will reach the same obstacle.
        // Note: when passing that said obstacle, the same restrictions will be applied on the following groups/partitions
        // merges. Not perfect, but as long as it's rare the impact should be minimal.

        mergeControl.mergeHadFailed(); // report to adjust stats
        final FDBException ex = IndexingBase.findException(e, FDBException.class);
        final IndexDeferredMaintenanceControl.LastStep lastStep = mergeControl.getLastStep();
        if (!IndexingBase.shouldLessenWork(ex)) {
            giveUpMerging(mergeControl, e);
        }
        switch (lastStep) {
            case REPARTITION:
                // Here: this exception might be resolved by reducing the number of documents to move during repartitioning
                handleRepartitioningFailure(mergeControl, e);
                break;

            case MERGE:
                // Here: this exception might be resolved by reducing the number of merges or forcing shorter intervals between auto-commits
                if (mergeControl.getMergesTried() < 2) {
                    handleSingleMergeFailure(mergeControl, e);
                } else {
                    handleMultiMergeFailure(mergeControl, e);
                }
                break;

            default:
                giveUpMerging(mergeControl, e);
                break; // "A switch statement does not contain a break", croaks a grumpy pmdMain.
        }
        return AsyncUtil.READY_TRUE; // and retry
    }

    private void handleRepartitioningFailure(final IndexDeferredMaintenanceControl mergeControl, Throwable e) {
        repartitionDocumentCount = mergeControl.getRepartitionDocumentCount();
        if (repartitionDocumentCount == -1) {
            // Here: failed, despite being set to skip. No recovery.
            giveUpMerging(mergeControl, e);
        }
        repartitionDocumentCount /= 2;
        if (repartitionDocumentCount == 0) {
            // Here: retry merge, but without repartitioning
            repartitionDocumentCount = -1;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(mergerLogMessage("IndexMerge: Repartitioning failure", mergeControl), e);
        }
    }

    private boolean shouldGiveRepartitionSecondChance(IndexDeferredMaintenanceControl mergeControl) {
        if (repartitionDocumentCount == -1 && repartitionSecondChances == 0) {
            repartitionSecondChances++;
            repartitionDocumentCount = 0;
            return true;
        }
        if (mergeControl.repartitionCapped()) {
            mergeControl.setRepartitionCapped(false);
            repartitionDocumentCount = 0;
            // we didn't fail, so we don't need to increment the repartitionSecondChances
            return true;
        }
        repartitionSecondChances = 0;
        return false;
    }


    private void handleMultiMergeFailure(final IndexDeferredMaintenanceControl mergeControl, Throwable e) {
        // Here: reduce the number of OneMerge items attempted
        mergesLimit = mergeControl.getMergesTried() / 2;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(mergerLogMessage("IndexMerge: Merges diluted", mergeControl), e);
        }
    }

    private void handleSingleMergeFailure(final IndexDeferredMaintenanceControl mergeControl, Throwable e) {
        // Here: make agility context auto-commit more rapidly
        // Note: this will only change the time quota. Size quota seems to be a non-issue.
        timeQuotaMillis = mergeControl.getTimeQuotaMillis();
        // log 4000 base 2 =~ 11.96 So it'll take about 12 retries from 4 seconds to the minimum.
        if (timeQuotaMillis <= 2) {
            giveUpMerging(mergeControl, e);
        }
        timeQuotaMillis /= 2;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(mergerLogMessage("IndexMerge: Decrease time quota", mergeControl), e);
        }
    }

    private void giveUpMerging(final IndexDeferredMaintenanceControl mergeControl, Throwable e) {
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn(mergerLogMessage("IndexMerge: Gave up merge", mergeControl), e);
        }
        throw common.getRunner().getDatabase().mapAsyncToSyncException(e);
    }

    List<Object> mergerKeysAndValues(final IndexDeferredMaintenanceControl mergeControl) {
        return List.of(
                LogMessageKeys.INDEX_NAME, index.getName(),
                LogMessageKeys.INDEX_MERGES_LAST_LIMIT, mergeControl.getMergesLimit(),
                LogMessageKeys.INDEX_MERGES_LAST_FOUND, mergeControl.getMergesFound(),
                LogMessageKeys.INDEX_MERGES_LAST_TRIED, mergeControl.getMergesTried(),
                LogMessageKeys.INDEX_MERGES_NUM_COMPLETED, mergeControl.getTotalMerges(),
                LogMessageKeys.INDEX_MERGES_CONTEXT_TIME_QUOTA, mergeControl.getTimeQuotaMillis(),
                LogMessageKeys.INDEX_REPARTITION_DOCUMENT_COUNT, mergeControl.getRepartitionDocumentCount(),
                LogMessageKeys.INDEX_DEFERRED_ACTION_STEP, mergeControl.getLastStep()
        );
    }

    String mergerLogMessage(String ttl, final IndexDeferredMaintenanceControl mergeControl) {
        final KeyValueLogMessage msg = KeyValueLogMessage.build(ttl);
        msg.addKeysAndValues(mergerKeysAndValues(mergeControl));
        if (subspaceProvider != null) {
            msg.addKeyAndValue(subspaceProvider.logKey(), subspaceProvider);
        }
        return msg.toString();
    }

}
