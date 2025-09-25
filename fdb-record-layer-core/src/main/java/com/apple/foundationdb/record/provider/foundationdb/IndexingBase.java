/*
 * IndexingBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.StoreTimerSnapshot;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingHeartbeat;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A base class for different types of online indexing process.
 */
@API(API.Status.INTERNAL)
public abstract class IndexingBase {

    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingBase.class);
    @Nonnull
    protected final IndexingCommon common; // to be used by extenders
    @Nonnull
    protected final OnlineIndexer.IndexingPolicy policy;
    @Nonnull
    private final IndexingThrottle throttle;
    private final boolean isScrubber;

    private long timeOfLastProgressLogMillis = 0;
    private StoreTimerSnapshot lastProgressSnapshot = null;
    private boolean forceStampOverwrite = false;
    private final long startingTimeMillis;
    private Map<String, IndexingMerger> indexingMergerMap = null;
    @Nullable
    private IndexingHeartbeat heartbeat = null; // this will stay null for index scrubbing

    IndexingBase(@Nonnull IndexingCommon common,
                 @Nonnull OnlineIndexer.IndexingPolicy policy) {
        this(common, policy, false);
    }


    IndexingBase(@Nonnull IndexingCommon common,
                 @Nonnull OnlineIndexer.IndexingPolicy policy,
                 boolean isScrubber) {
        this.common = common;
        this.policy = policy;
        this.isScrubber = isScrubber;
        this.throttle = new IndexingThrottle(common, isScrubber);
        this.startingTimeMillis = System.currentTimeMillis();
    }

    // helper functions
    protected FDBDatabaseRunner getRunner() {
        return common.getRunner();
    }

    @SuppressWarnings("squid:S1452")
    protected CompletableFuture<FDBRecordStore> openRecordStore(@Nonnull FDBRecordContext context) {
        return common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync();
    }

    // Turn a (possibly null) tuple into a (possibly null) byte array.
    @Nullable
    protected static byte[] packOrNull(@Nullable Tuple tuple) {
        return (tuple == null) ? null : tuple.pack();
    }

    @Nonnull
    protected CompletableFuture<FDBStoredRecord<Message>> recordIfInIndexedTypes(FDBStoredRecord<Message> rec) {
        return CompletableFuture.completedFuture( rec != null && common.getAllRecordTypes().contains(rec.getRecordType()) ? rec : null);
    }

    // buildIndexAsync - the main indexing function. Builds and commits indexes asynchronously; throttling to avoid overloading the system.
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Void> buildIndexAsync(boolean markReadable) {
        KeyValueLogMessage message = KeyValueLogMessage.build("build index online",
                LogMessageKeys.SHOULD_MARK_READABLE, markReadable,
                LogMessageKeys.INDEXER_ID, common.getIndexerId());
        long startNanos = System.nanoTime();
        FDBDatabaseRunner runner = getRunner();
        final FDBStoreTimer timer = runner.getTimer();
        if (timer != null) {
            lastProgressSnapshot = StoreTimerSnapshot.from(timer);
        }
        return MoreAsyncUtil.composeWhenComplete(
                handleStateAndDoBuildIndexAsync(markReadable, message),
                (result, ex) -> {
                    // proper log
                    message.addKeysAndValues(indexingLogMessageKeyValues()) // add these here to pick up state accumulated during build
                            .addKeysAndValues(common.indexLogMessageKeyValues())
                            .addKeyAndValue(LogMessageKeys.TOTAL_MICROS, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos));
                    if (LOGGER.isWarnEnabled() && (ex != null)) {
                        message.addKeyAndValue(LogMessageKeys.RESULT, "failure");
                        message.addKeysAndValues(throttle.logMessageKeyValues()); // this "last attempt" snapshot information can help debugging
                        LOGGER.warn(message.toString(), ex);
                    } else if (LOGGER.isInfoEnabled()) {
                        message.addKeyAndValue(LogMessageKeys.RESULT, "success");
                        LOGGER.info(message.toString());
                    }
                    // Here: if the heartbeats were not fully cleared while marking the index as readable, they will be cleared in
                    // this dedicated transaction. Clearing the heartbeats at the end of the indexing session is a "best effort"
                    // operation, hence exceptions are ignored.
                    return clearHeartbeats()
                            .handle((ignoreRet, ignoreEx) -> null);
                },
                getRunner().getDatabase()::mapAsyncToSyncException
        );
    }

    abstract List<Object> indexingLogMessageKeyValues();

    @Nonnull
    private CompletableFuture<Void> handleStateAndDoBuildIndexAsync(boolean markReadable, KeyValueLogMessage message) {
        /*
         * Multi target:
         * The primary and follower indexes must the same state.
         * If the primary is disabled, a follower is write_only/readable, and the policy for write_only/readable is
         *   rebuild - the follower's state will be cleared.
         * If the primary is cleared, all the followers are cleared (must match stamps/ranges).
         * If the primary is write_only, and the followers' stamps/ranges do not match, it would be caught during indexing.
         */
        final List<Index> targetIndexes = common.getTargetIndexes();
        final Index primaryIndex = targetIndexes.get(0);
        return getRunner().runAsync(context -> openRecordStore(context).thenCompose(store -> {
            IndexState indexState = store.getIndexState(primaryIndex);
            if (isScrubber) {
                validateOrThrowEx(indexState.isScannable(), "Scrubber was called for a non-readable index. Index:" + primaryIndex.getName() + " State: " + indexState);
                return setScrubberTypeOrThrow(store).thenApply(ignore -> true);
            }
            OnlineIndexer.IndexingPolicy.DesiredAction desiredAction = policy.getStateDesiredAction(indexState);
            if (desiredAction == OnlineIndexer.IndexingPolicy.DesiredAction.ERROR) {
                throw new ValidationException("Index state is not as expected",
                        LogMessageKeys.INDEX_NAME, primaryIndex.getName(),
                        LogMessageKeys.INDEX_VERSION, primaryIndex.getLastModifiedVersion(),
                        LogMessageKeys.INDEX_STATE, indexState
                        );
            }
            if (desiredAction == OnlineIndexer.IndexingPolicy.DesiredAction.MARK_READABLE) {
                return markIndexReadable(markReadable)
                        .thenCompose(ignore -> AsyncUtil.READY_FALSE);
            }
            boolean shouldClear = desiredAction == OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD;
            boolean shouldBuild = shouldClear || indexState != IndexState.READABLE;
            message.addKeyAndValue(LogMessageKeys.INITIAL_INDEX_STATE, indexState);
            message.addKeyAndValue(LogMessageKeys.INDEXING_POLICY_DESIRED_ACTION, desiredAction);
            message.addKeyAndValue(LogMessageKeys.SHOULD_BUILD_INDEX, shouldBuild);
            message.addKeyAndValue(LogMessageKeys.SHOULD_CLEAR_EXISTING_DATA, shouldClear);

            if (!shouldBuild) {
                return AsyncUtil.READY_FALSE; // do not index
            }

            List<Index> indexesToClear = new ArrayList<>(targetIndexes.size());
            if (shouldClear) {
                indexesToClear.add(primaryIndex);
                enforceStampOverwrite(); // The code can work without this line, but it'll save probing the missing ranges
            }

            boolean continuedBuild = !shouldClear && indexState == IndexState.WRITE_ONLY;
            for (Index targetIndex : targetIndexes.subList(1, targetIndexes.size())) {
                // Must follow the primary index' status
                IndexState state = store.getIndexState(targetIndex);
                if (state != indexState) {
                    if (policy.getStateDesiredAction(state) != OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD ||
                            continuedBuild) {
                        throw new ValidationException("A target index state doesn't match the primary index state",
                                LogMessageKeys.INDEX_NAME, primaryIndex.getName(),
                                LogMessageKeys.INDEX_STATE, indexState,
                                LogMessageKeys.TARGET_INDEX_NAME, targetIndex.getName(),
                                LogMessageKeys.TARGET_INDEX_STATE, state);
                    }
                    // just clear this one, the primary is disabled
                    indexesToClear.add(targetIndex);
                } else if (shouldClear) {
                    indexesToClear.add(targetIndex);
                }
            }

            return AsyncUtil.whenAll(indexesToClear.stream().map(store::clearAndMarkIndexWriteOnly).collect(Collectors.toList()))
                    .thenCompose(vignore -> markIndexesWriteOnly(continuedBuild, store))
                    .thenCompose(vignore -> setIndexingTypeOrThrow(store, continuedBuild))
                    .thenApply(ignore -> true);
        }), common.indexLogMessageKeyValues("IndexingBase::handleIndexingState")
        ).thenCompose(doIndex ->
                doIndex ?
                buildIndexInternalAsync().thenApply(ignore -> markReadable) :
                AsyncUtil.READY_FALSE
        ).thenCompose(this::markIndexReadable
        ).thenApply(ignore -> null);
    }

    private CompletableFuture<Void> markIndexesWriteOnly(boolean continueBuild, FDBRecordStore store) {
        if (continueBuild) {
            return AsyncUtil.DONE;
        }
        return forEachTargetIndex(store::markIndexWriteOnly);
    }

    @Nonnull
    public CompletableFuture<Boolean> markReadableIfBuilt() {
        AtomicBoolean allReadable = new AtomicBoolean(true);
        return getRunner().runAsync(context -> openRecordStore(context).thenCompose(store ->
            forEachTargetIndex(index -> {
                if (store.isIndexReadable(index)) {
                    return AsyncUtil.DONE;
                }
                final IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(store, index);
                return rangeSet.firstMissingRangeAsync()
                        .thenCompose(range -> {
                            if (range != null) {
                                allReadable.set(false);
                                return AsyncUtil.DONE;
                            }
                            // Index is built because there is no missing range.
                            return store.markIndexReadable(index)
                                    // markIndexReadable will return false if the index was already readable
                                    .thenApply(vignore2 -> null);
                        });
            })
        ).thenApply(ignore -> allReadable.get()), common.indexLogMessageKeyValues("IndexingBase::markReadableIfBuilt"));
    }

    @Nonnull
    public CompletableFuture<Boolean> markIndexReadable(boolean markReadablePlease) {
        if (!markReadablePlease) {
            return AsyncUtil.READY_FALSE; // they didn't say please..
        }

        AtomicReference<RuntimeException> runtimeExceptionAtomicReference = new AtomicReference<>();
        AtomicBoolean anythingChanged = new AtomicBoolean(false);

        // Mark each index readable in its own (retriable, parallel) transaction. If one target fails to become
        // readable, it should not affect the others.
        return forEachTargetIndex(index ->
                markIndexReadableForIndex(index, anythingChanged, runtimeExceptionAtomicReference)
        ).thenApply(ignore -> {
            RuntimeException ex = runtimeExceptionAtomicReference.get();
            if (ex != null) {
                throw ex;
            }
            heartbeat = null; // Here: heartbeats had been successfully cleared. No need to clear again
            return anythingChanged.get();
        });
    }

    private CompletableFuture<Boolean> markIndexReadableForIndex(Index index, AtomicBoolean anythingChanged,
                                                                 AtomicReference<RuntimeException> runtimeExceptionAtomicReference) {
        // An extension function to reduce markIndexReadable's complexity
        return getRunner().runAsync(context ->
                common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync()
                        .thenCompose(store -> {
                            clearHeartbeatForIndex(store, index);
                            return policy.shouldAllowUniquePendingState(store) ?
                                   store.markIndexReadableOrUniquePending(index) :
                                   store.markIndexReadable(index);
                        })
        ).handle((changed, ex) -> {
            if (ex == null) {
                if (Boolean.TRUE.equals(changed)) {
                    anythingChanged.set(true);
                }
                return changed; // ignored
            }
            // Note: in case of multiple violations, an arbitrary one is thrown.
            runtimeExceptionAtomicReference.set((RuntimeException)ex);
            return false;
        });
    }

    public void enforceStampOverwrite() {
        forceStampOverwrite = true; // must overwrite a previous indexing method's stamp
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Void> setIndexingTypeOrThrow(FDBRecordStore store, boolean continuedBuild) {
        // continuedBuild is set if this session isn't a continuation of a previous indexing
        IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp = getIndexingTypeStamp(store);
        final IndexBuildProto.IndexBuildIndexingStamp.Method method = indexingTypeStamp.getMethod();
        boolean allowMutual =
                method == IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS ||
                method == IndexBuildProto.IndexBuildIndexingStamp.Method.SCRUB_REPAIR;
        heartbeat = new IndexingHeartbeat(common.getIndexerId(), indexingTypeStamp.getMethod().toString(), common.config.getLeaseLengthMillis(), allowMutual);

        return forEachTargetIndex(index -> setIndexingTypeOrThrow(store, continuedBuild, index, indexingTypeStamp)
                .thenCompose(ignore -> updateHeartbeat(store, index)));
    }

    @Nonnull
    private CompletableFuture<Void> setIndexingTypeOrThrow(FDBRecordStore store, boolean continuedBuild, Index index, IndexBuildProto.IndexBuildIndexingStamp newStamp) {
        if (forceStampOverwrite && !continuedBuild) {
            // Fresh session + overwrite = no questions asked
            store.saveIndexingTypeStamp(index, newStamp);
            return AsyncUtil.DONE;
        }
        return store.loadIndexingTypeStampAsync(index)
                .thenCompose(savedStamp -> {
                    if (savedStamp == null) {
                        if (continuedBuild && newStamp.getMethod() !=
                                              IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS) {
                            // backward compatibility - maybe continuing an old BY_RECORD session
                            return isWriteOnlyButNoRecordScanned(store, index)
                                    .thenCompose(noRecordScanned -> throwAsByRecordsUnlessNoRecordWasScanned(noRecordScanned, store, index, newStamp));
                        }
                        // Here: either not a continuedBuild (new session), or a BY_RECORD session (allowed to overwrite the null stamp)
                        store.saveIndexingTypeStamp(index, newStamp);
                        return AsyncUtil.DONE;
                    }
                    // Here: has non-null type stamp
                    if (newStamp.equals(savedStamp)) {
                        // A matching stamp is already there - One less thing to worry about
                        return AsyncUtil.DONE;
                    }
                    if (isTypeStampBlocked(savedStamp) && !policy.shouldAllowUnblock(savedStamp.getBlockID())) {
                        // Indexing is blocked
                        throw newPartlyBuiltException(savedStamp, newStamp, index);
                    }
                    if (areSimilar(newStamp, savedStamp)) {
                        // Similar stamps, replace it
                        store.saveIndexingTypeStamp(index, newStamp);
                        return AsyncUtil.DONE;
                    }
                    // Here: check if type conversion is allowed
                    if (continuedBuild && shouldAllowTypeConversionContinue(newStamp, savedStamp)) {
                        // Special case: partly built by another indexing method, but may be continued with the current one
                        store.saveIndexingTypeStamp(index, newStamp);
                        return AsyncUtil.DONE;
                    }
                    // Here: conversion is not allowed, yet there might be a case of a WRITE_ONLY index that hadn't scanned any records.
                    if (forceStampOverwrite) {  // and a continued Build
                        // check if partly built
                        return isWriteOnlyButNoRecordScanned(store, index)
                                .thenCompose(noRecordScanned ->
                                throwUnlessNoRecordWasScanned(noRecordScanned, store, index, newStamp, savedStamp));
                    }
                    // fall down to exception
                    throw newPartlyBuiltException(savedStamp, newStamp, index);
                });
    }

    private boolean shouldAllowTypeConversionContinue(IndexBuildProto.IndexBuildIndexingStamp newStamp, IndexBuildProto.IndexBuildIndexingStamp savedStamp) {
        return policy.shouldAllowTypeConversionContinue(newStamp, savedStamp);
    }

    private static boolean areSimilar(IndexBuildProto.IndexBuildIndexingStamp newStamp, IndexBuildProto.IndexBuildIndexingStamp savedStamp) {
        return newStamp.equals(savedStamp) // The common case, or so we hope
               || blocklessStampOf(newStamp).equals(blocklessStampOf(savedStamp));
    }

    private static IndexBuildProto.IndexBuildIndexingStamp blocklessStampOf(IndexBuildProto.IndexBuildIndexingStamp stamp) {
        return stamp.toBuilder()
                .setBlock(false)
                .setBlockID("")
                .setBlockExpireEpochMilliSeconds(0)
                .build();
    }

    @Nonnull
    private CompletableFuture<Void> throwAsByRecordsUnlessNoRecordWasScanned(boolean noRecordScanned,
                                                                             FDBRecordStore store,
                                                                             Index index,
                                                                             IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp) {
        // A complicated way to reduce complexity.
        if (noRecordScanned) {
            // an empty type stamp, and nothing was indexed - it is safe to write stamp
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.build("no scanned ranges - continue indexing")
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .toString());
            }
            store.saveIndexingTypeStamp(index, indexingTypeStamp);
            return AsyncUtil.DONE;
        }
        // Here: there is no type stamp, but indexing is ongoing. For backward compatibility reasons, we'll consider it a BY_RECORDS stamp
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.build("continuation with null type stamp, assuming previous by-records scan")
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .toString());
        }
        final IndexBuildProto.IndexBuildIndexingStamp fakeSavedStamp = IndexingMultiTargetByRecords.compileSingleTargetLegacyIndexingTypeStamp();
        throw newPartlyBuiltException(fakeSavedStamp, indexingTypeStamp, index);
    }

    @Nonnull
    private CompletableFuture<Void> throwUnlessNoRecordWasScanned(boolean noRecordScanned,
                                                                  FDBRecordStore store,
                                                                  Index index,
                                                                  IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp,
                                                                  IndexBuildProto.IndexBuildIndexingStamp savedStamp) {
        // Ditto (a complicated way to reduce complexity)
        if (noRecordScanned) {
            // we can safely overwrite the previous type stamp
            store.saveIndexingTypeStamp(index, indexingTypeStamp);
            return AsyncUtil.DONE;
        }
        // A force overwrite cannot be allowed when partly built
        throw newPartlyBuiltException(savedStamp, indexingTypeStamp, index);
    }

    @Nonnull
    protected CompletableFuture<Void> setScrubberTypeOrThrow(FDBRecordStore store) {
        // This path should never be reached
        throw new ValidationException("Called setScrubberTypeOrThrow in a non-scrubbing path",
                "isScrubber", isScrubber);
    }

    @Nonnull
    abstract IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(FDBRecordStore store);

    abstract CompletableFuture<Void> buildIndexInternalAsync();

    private CompletableFuture<Boolean> isWriteOnlyButNoRecordScanned(FDBRecordStore store, Index index) {
        IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(store, index);
        return rangeSet.firstMissingRangeAsync().thenCompose(range -> {
            if (range == null) {
                return AsyncUtil.READY_FALSE; // fully built - no missing ranges
            }
            return CompletableFuture.completedFuture(RangeSet.isFirstKey(range.begin) && RangeSet.isFinalKey(range.end));
        });
    }

    private RecordCoreException newPartlyBuiltException(IndexBuildProto.IndexBuildIndexingStamp savedStamp,
                                                        IndexBuildProto.IndexBuildIndexingStamp expectedStamp,
                                                        Index index) {
        return new PartlyBuiltException(savedStamp, expectedStamp, index, common.getIndexerId(),
                savedStamp.getBlock() ?
                "This index was partly built, and blocked" :
                "This index was partly built by another method");
    }

    // Helpers for implementing modules. Some of them are public to support unit-testing.
    protected CompletableFuture<Boolean> doneOrThrottleDelayAndMaybeLogProgress(boolean done, List<Object> additionalLogMessageKeyValues) {
        if (done) {
            return AsyncUtil.READY_FALSE;
        }
        long toWait = throttle.waitTimeMilliseconds();

        if (LOGGER.isInfoEnabled() && shouldLogBuildProgress()) {
            FDBStoreTimer timer = getRunner().getTimer();
            StoreTimer metricsDiff = null;
            if (timer != null) {
                metricsDiff = lastProgressSnapshot == null ? timer : StoreTimer.getDifference(timer, lastProgressSnapshot);
                lastProgressSnapshot = StoreTimerSnapshot.from(timer);
            }
            LOGGER.info(KeyValueLogMessage.build("Indexer: Built Range",
                            LogMessageKeys.DELAY, toWait)
                    .addKeysAndValues(additionalLogMessageKeyValues != null ? additionalLogMessageKeyValues : Collections.emptyList())
                    .addKeysAndValues(indexingLogMessageKeyValues())
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .addKeysAndValues(throttle.logMessageKeyValues())
                    .addKeysAndValues(metricsDiff == null ? Collections.emptyMap() : metricsDiff.getKeysAndValues())
                    .toString());
        }

        validateTimeLimit(toWait);

        CompletableFuture<Boolean> delay = MoreAsyncUtil.delayedFuture(toWait, TimeUnit.MILLISECONDS, getRunner().getScheduledExecutor()).thenApply(vignore3 -> true);
        if (getRunner().getTimer() != null) {
            delay = getRunner().getTimer().instrument(FDBStoreTimer.Events.INDEXER_DELAY, delay, getRunner().getExecutor());
        }
        return delay;
    }

    private void validateTimeLimit(long toWait) {
        final long timeLimitMilliseconds = common.config.getTimeLimitMilliseconds();
        if (timeLimitMilliseconds == OnlineIndexOperationConfig.UNLIMITED_TIME) {
            return;
        }
        final long now = System.currentTimeMillis() + toWait; // adding the time we are about to wait
        if (startingTimeMillis + timeLimitMilliseconds >= now) {
            return;
        }
        throw new TimeLimitException("Time Limit Exceeded",
                LogMessageKeys.TIME_LIMIT_MILLIS, timeLimitMilliseconds,
                LogMessageKeys.TIME_STARTED_MILLIS, startingTimeMillis,
                LogMessageKeys.TIME_ENDED_MILLIS, now,
                LogMessageKeys.TIME_TO_WAIT_MILLIS, toWait);
    }

    private boolean shouldLogBuildProgress() {
        long interval = common.config.getProgressLogIntervalMillis();
        long now = System.currentTimeMillis();
        if (interval < 0 || (interval != 0 && interval > (now - timeOfLastProgressLogMillis))) {
            return false;
        }
        timeOfLastProgressLogMillis = now;
        return true;
    }

    public int getLimit() {
        // made public to support tests
        return throttle.getLimit();
    }

    public <R> CompletableFuture<R> buildCommitRetryAsync(@Nonnull BiFunction<FDBRecordStore, AtomicLong, CompletableFuture<R>> buildFunction,
                                                          @Nullable List<Object> additionalLogMessageKeyValues) {
        return buildCommitRetryAsync(buildFunction, additionalLogMessageKeyValues, false);
    }

    public <R> CompletableFuture<R> buildCommitRetryAsync(@Nonnull BiFunction<FDBRecordStore, AtomicLong, CompletableFuture<R>> buildFunction,
                                                          @Nullable List<Object> additionalLogMessageKeyValues, final boolean duringRangesIteration) {
        return throttle.buildCommitRetryAsync(buildFunction, null, additionalLogMessageKeyValues, duringRangesIteration);
    }

    protected void timerIncrement(StoreTimer.Count event) {
        // helper function to reduce complexity
        final FDBStoreTimer timer = getRunner().getTimer();
        if (timer != null) {
            timer.increment(event);
        }
    }

    @Nonnull
    private <T> CompletableFuture<Void> forEachTargetIndex(Function<Index, CompletableFuture<T>> function) {
        // helper to operate on all target indexes (indexes only!)
        List<Index> targetIndexes = common.getTargetIndexes();
        return AsyncUtil.whenAll(targetIndexes.stream().map(function).collect(Collectors.toList()));
    }

    @Nonnull
    private <T> CompletableFuture<Void> forEachTargetIndexContext(Function<IndexingCommon.IndexContext, CompletableFuture<T>> function) {
        // helper to operate on all target indexers (indexers - for index maintainers)
        List<IndexingCommon.IndexContext> indexContexts = common.getTargetIndexContexts();
        return AsyncUtil.whenAll(indexContexts.stream().map(function).collect(Collectors.toList()));
    }
    /**
     * iterate cursor's items and index them.
     *
     * @param store the record store.
     * @param cursor iteration items.
     * @param getRecordToIndex function to convert cursor's item to a record that should be indexed (or null, if inapplicable)
     * @param nextResultCont when return, if hasMore is true, holds the last cursored result - unprocessed - as a
     * continuation item.
     * @param hasMore when return, true if the cursor's source is not exhausted (not more items in range).
     * @param recordsScanned when return, number of scanned records.
     * @param isIdempotent are all the built indexes idempotent
     * @param <T> cursor result's type.
     *
     * @return hasMore, nextResultCont, and recordsScanned.
     */

    @SuppressWarnings("PMD.CloseResource")
    protected  <T> CompletableFuture<Void> iterateRangeOnly(@Nonnull FDBRecordStore store,
                                                            @Nonnull RecordCursor<T> cursor,
                                                            @Nonnull BiFunction<FDBRecordStore, RecordCursorResult<T>, CompletableFuture<FDBStoredRecord<Message>>> getRecordToIndex,
                                                            @Nonnull AtomicReference<RecordCursorResult<T>> nextResultCont,
                                                            @Nonnull AtomicBoolean hasMore,
                                                            @Nullable AtomicLong recordsScanned,
                                                            final boolean isIdempotent) {

        // Need to do this each transaction because other index enabled state might have changed. Could cache based on that.
        // Copying the state also guards against changes made by other online building from check version.
        AtomicLong recordsScannedCounter = new AtomicLong();
        final AtomicReference<RecordCursorResult<T>> nextResult = new AtomicReference<>(null);
        deferAutoMergeDuringCommit(store);

        return validateTypeStamp(store)
                .thenCompose(ignore ->
                        AsyncUtil.whileTrue(() -> cursor.onNext()
                                .thenCompose(result ->
                                        policy.isReverseScanOrder() ?
                                        handleCursorResultReverse(store, result,
                                                getRecordToIndex, nextResultCont,
                                                recordsScannedCounter, hasMore, isIdempotent)
                                        :
                                        handleCursorResult(store, result,
                                                getRecordToIndex, nextResult, nextResultCont,
                                                recordsScannedCounter, hasMore, isIdempotent)
                                ), cursor.getExecutor()))
                .thenApply(vignore -> {
                    long recordsScannedInTransaction = recordsScannedCounter.get();
                    if (recordsScanned != null) {
                        recordsScanned.addAndGet(recordsScannedInTransaction);
                    }
                    if (common.isTrackProgress()) {
                        for (Index index: common.getTargetIndexes()) {
                            final Subspace scannedRecordsSubspace = IndexingSubspaces.indexBuildScannedRecordsSubspace(store, index);
                            store.context.ensureActive().mutate(MutationType.ADD, scannedRecordsSubspace.getKey(),
                                    FDBRecordStore.encodeRecordCount(recordsScannedInTransaction));
                        }
                    }
                    return null;
                });
    }

    @SuppressWarnings("squid:S00107") // too many parameters
    private <T> CompletableFuture<Boolean> handleCursorResult(@Nonnull FDBRecordStore store,
                                                              @Nonnull RecordCursorResult<T> cursorResult,
                                                              @Nonnull BiFunction<FDBRecordStore, RecordCursorResult<T>, CompletableFuture<FDBStoredRecord<Message>>> getRecordToIndex,
                                                              @Nonnull AtomicReference<RecordCursorResult<T>> nextResult,
                                                              @Nonnull AtomicReference<RecordCursorResult<T>> nextResultCont,
                                                              @Nonnull AtomicLong recordsScannedCounter,
                                                              @Nonnull AtomicBoolean hasMore,
                                                              final boolean isIdempotent) {
        RecordCursorResult<T> currResult;
        final boolean isExhausted;
        if (cursorResult.hasNext()) {
            // has next, process one previous item (if exists)
            currResult = nextResult.get();
            nextResult.set(cursorResult);
            if (currResult == null) {
                // that was the first item, nothing to process
                return AsyncUtil.READY_TRUE;
            }
            isExhausted = false;
        } else {
            // end of the cursor list
            timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT);
            if (!cursorResult.getNoNextReason().isSourceExhausted()) {
                nextResultCont.set(nextResult.get());
                hasMore.set(true);
                return AsyncUtil.READY_FALSE;
            }
            // source is exhausted, fall down to handle the last item and return with hasMore=false
            currResult = nextResult.get();
            if (currResult == null) {
                // there was no data
                hasMore.set(false);
                return AsyncUtil.READY_FALSE;
            }
            // here, process the last item and return
            nextResult.set(null);
            isExhausted = true;
        }

        // here: currResult must have value
        timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
        recordsScannedCounter.incrementAndGet();

        return getRecordToIndex.apply(store, currResult)
                .thenCompose(rec -> {
                    if (null == rec) {
                        if (isExhausted) {
                            hasMore.set(false);
                            return AsyncUtil.READY_FALSE;
                        }
                        return AsyncUtil.READY_TRUE;
                    }
                    // This record should be indexed. Add it to the transaction.
                    if (isIdempotent) {
                        store.addRecordReadConflict(rec.getPrimaryKey());
                    }
                    timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED);

                    final CompletableFuture<Void> updateMaintainer = updateMaintainerBuilder(store, rec);
                    if (isExhausted) {
                        // we've just processed the last item
                        timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_DEPLETION);
                        hasMore.set(false);
                        return updateMaintainer.thenApply(vignore -> false);
                    }
                    return updateMaintainer.thenCompose(vignore ->
                            hadTransactionReachedLimits(store)
                                    .thenApply(shouldCommit -> {
                                        if (shouldCommit) {
                                            nextResultCont.set(nextResult.get());
                                            hasMore.set(true);
                                            return false;
                                        }
                                        return true;
                                    })
                    );
                });
    }

    @SuppressWarnings("squid:S00107") // too many parameters
    private <T> CompletableFuture<Boolean> handleCursorResultReverse(@Nonnull FDBRecordStore store,
                                                                     @Nonnull RecordCursorResult<T> cursorResult,
                                                                     @Nonnull BiFunction<FDBRecordStore, RecordCursorResult<T>, CompletableFuture<FDBStoredRecord<Message>>> getRecordToIndex,
                                                                     @Nonnull AtomicReference<RecordCursorResult<T>> nextResultCont,
                                                                     @Nonnull AtomicLong recordsScannedCounter,
                                                                     @Nonnull AtomicBoolean hasMore,
                                                                     final boolean isIdempotent) {
        // When setting the rangeSet the first item is inclusive, the last one is exclusive. Hence, if scanning in reverse order (which is rare),
        // the 'lastResultCont' item should also be processed
        if (!cursorResult.hasNext()) {
            timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT);
            if (cursorResult.getNoNextReason().isSourceExhausted()) {
                timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_DEPLETION);
                hasMore.set(false);
            } else {
                hasMore.set(true);
            }
            return AsyncUtil.READY_FALSE; // all done
        }

        // here: rangeCursor must have value
        timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
        recordsScannedCounter.incrementAndGet();
        nextResultCont.set(cursorResult);

        return getRecordToIndex.apply(store, cursorResult)
                .thenCompose(rec -> {
                    if (null == rec) {
                        return AsyncUtil.READY_TRUE; // next
                    }
                    // This record should be indexed. Add it to the transaction.
                    if (isIdempotent) {
                        store.addRecordReadConflict(rec.getPrimaryKey());
                    }
                    timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED);

                    final CompletableFuture<Void> updateMaintainer = updateMaintainerBuilder(store, rec);
                    return updateMaintainer.thenCompose(vignore ->
                            hadTransactionReachedLimits(store)
                                    .thenApply(shouldCommit -> {
                                        if (shouldCommit) {
                                            hasMore.set(true);
                                            return false;
                                        }
                                        return true;
                                    })
                    );
                });
    }

    private CompletableFuture<Boolean> hadTransactionReachedLimits(FDBRecordStore store) {
        final long transactionTimeLimitMilliseconds = common.config.getTransactionTimeLimitMilliseconds();
        if (transactionTimeLimitMilliseconds > 0 &&
                transactionTimeLimitMilliseconds < store.getContext().getTransactionAge()) {
            // return true, since exceeded transaction time limit.
            // Note that limiting transaction's time via cursor's ExecuteProperties::timeLimit could have caused it to provide
            // a single record, which would not have been indexed but used for continuation (hence an infinite loop).
            timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_TIME);
            return AsyncUtil.READY_TRUE;
        }
        final long maxWriteLimit = common.config.getMaxWriteLimitBytes();
        if (maxWriteLimit > 0) {
            // return a transaction write size limit check
            return store.getContext().getApproximateTransactionSize()
                    .thenApply(size -> {
                        if (size > maxWriteLimit) {
                            timerIncrement(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_SIZE);
                            return true;
                        }
                        return false;
                    });
        }
        return AsyncUtil.READY_FALSE;
    }

    private CompletableFuture<Void> validateTypeStamp(@Nonnull FDBRecordStore store) {
        // check other heartbeats (if exclusive) & typestamp
        if (isScrubber) {
            // Scrubber's type-stamp is never commited. It is protected by expecting a READABLE index state.
            return AsyncUtil.DONE;
        }
        final IndexBuildProto.IndexBuildIndexingStamp expectedTypeStamp = getIndexingTypeStamp(store);
        return forEachTargetIndex(index -> CompletableFuture.allOf(
                updateHeartbeat(store, index),
                store.loadIndexingTypeStampAsync(index)
                        .thenAccept(typeStamp -> validateTypeStamp(typeStamp, expectedTypeStamp, index))
        ));
    }

    private CompletableFuture<Void> updateHeartbeat(FDBRecordStore store, Index index) {
        return heartbeat == null ?
               AsyncUtil.DONE :
               heartbeat.checkAndUpdateHeartbeat(store, index);
    }

    private CompletableFuture<Void> clearHeartbeats() {
        if (heartbeat == null) {
            // Here: either silent heartbeats or heartbeats had been cleared during markReadable phase
            return AsyncUtil.DONE;
        }
        // Here: for each index we clear (only) the heartbeat generated by this indexer. This is a quick operation that can be done in a single transaction.
        return getRunner().runAsync(context ->
                common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync()
                        .thenApply(store -> {
                            clearHeartbeats(store);
                            return null;
                        }));
    }

    private void clearHeartbeats(FDBRecordStore store) {
        if (heartbeat != null) {
            for (Index index : common.getTargetIndexes()) {
                heartbeat.clearHeartbeat(store, index);
            }
        }
    }

    private void clearHeartbeatForIndex(FDBRecordStore store, Index index) {
        if (heartbeat != null) {
            heartbeat.clearHeartbeat(store, index);
        }
    }

    private void validateTypeStamp(final IndexBuildProto.IndexBuildIndexingStamp typeStamp,
                                   final IndexBuildProto.IndexBuildIndexingStamp expectedTypeStamp,
                                   Index index) {
        if (typeStamp == null && expectedTypeStamp.getMethod() == IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS) {
            // special case - null type stamp is considered a BY_RECORD
            return;
        }
        if (typeStamp == null || typeStamp.getMethod() != expectedTypeStamp.getMethod() || isTypeStampBlocked(typeStamp)) {
            throw new PartlyBuiltException(typeStamp, expectedTypeStamp,
                    index, common.getIndexerId(), "Indexing stamp had changed");
        }
    }

    private static boolean isTypeStampBlocked(final IndexBuildProto.IndexBuildIndexingStamp typeStamp) {
        return typeStamp.getBlock() &&
               (typeStamp.getBlockExpireEpochMilliSeconds() == 0 ||
                typeStamp.getBlockExpireEpochMilliSeconds() > System.currentTimeMillis());
    }

    @Nonnull
    SyntheticRecordFromStoredRecordPlan syntheticPlanForIndex(@Nonnull FDBRecordStore store, @Nonnull IndexingCommon.IndexContext indexContext) {
        if (!indexContext.isSynthetic) {
            throw new RecordCoreException("unable to create synthetic plan for non-synthetic index");
        }
        final RecordQueryPlanner queryPlanner = new RecordQueryPlanner(store.getRecordMetaData(), store.getRecordStoreState().withWriteOnlyIndexes(Collections.singletonList(indexContext.index.getName())));
        final SyntheticRecordPlanner syntheticPlanner = new SyntheticRecordPlanner(store, queryPlanner);
        return syntheticPlanner.forIndex(indexContext.index);
    }

    private CompletableFuture<Void> updateMaintainerBuilder(@Nonnull FDBRecordStore store,
                                                            FDBStoredRecord<Message> rec) {
        return forEachTargetIndexContext(indexContext -> {
            if (!indexContext.recordTypes.contains(rec.getRecordType())) {
                // This particular index is not affected by rec
                return AsyncUtil.DONE;
            }
            if (indexContext.isSynthetic) {
                // This particular index is synthetic, handle with care
                final SyntheticRecordFromStoredRecordPlan syntheticPlan = syntheticPlanForIndex(store, indexContext);
                final IndexMaintainer maintainer = store.getIndexMaintainer(indexContext.index);
                return syntheticPlan.execute(store, rec).forEachAsync(syntheticRecord -> maintainer.update(null, syntheticRecord), 1);
            }
            // update simple index
            return store.getIndexMaintainer(indexContext.index).update(null, rec);
        });
    }

    protected CompletableFuture<Void> iterateAllRanges(List<Object> additionalLogMessageKeyValues,
                                                       BiFunction<FDBRecordStore, AtomicLong,  CompletableFuture<Boolean>> iterateRange) {
        return iterateAllRanges(additionalLogMessageKeyValues, iterateRange, null);
    }

    protected CompletableFuture<Void> iterateAllRanges(List<Object> additionalLogMessageKeyValues,
                                                       BiFunction<FDBRecordStore, AtomicLong,  CompletableFuture<Boolean>> iterateRange,
                                                       @Nullable Function<FDBException, Optional<Boolean>> shouldReturnQuietly) {

        return AsyncUtil.whileTrue(() ->
                    throttle.buildCommitRetryAsync(iterateRange, shouldReturnQuietly, additionalLogMessageKeyValues, true)
                            .handle((hasMore, ex) -> {
                                if (ex == null) {
                                    final Set<Index> indexSet = throttle.getAndResetMergeRequiredIndexes();
                                    if (indexSet != null && !indexSet.isEmpty()) {
                                        return mergeIndexes(indexSet)
                                                .thenCompose(ignore -> doneOrThrottleDelayAndMaybeLogProgress(!hasMore, additionalLogMessageKeyValues));
                                    }
                                    return doneOrThrottleDelayAndMaybeLogProgress(!hasMore, additionalLogMessageKeyValues);
                                }
                                final RuntimeException unwrappedEx = getRunner().getDatabase().mapAsyncToSyncException(ex);
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info(KeyValueLogMessage.build("possibly non-fatal error encountered building range")
                                                    .addKeysAndValues(common.indexLogMessageKeyValues())
                                            .toString(), ex);
                                }
                                throw unwrappedEx;
                            }).thenCompose(Function.identity()),
                getRunner().getExecutor());
    }

    public CompletableFuture<Void> mergeIndexes() {
        return mergeIndexes(new HashSet<>(common.getTargetIndexes()));
    }

    private CompletableFuture<Void> mergeIndexes(Set<Index> indexSet) {
        return AsyncUtil.whenAll(indexSet.stream()
                .map(index -> getIndexingMerger(index).mergeIndex()
        ).collect(Collectors.toList()));
    }

    private synchronized IndexingMerger getIndexingMerger(Index index) {
        if (indexingMergerMap == null) {
            indexingMergerMap = new HashMap<>();
        }
        return indexingMergerMap.computeIfAbsent(index.getName(), k -> new IndexingMerger(index, common, policy.getInitialMergesCountLimit()));
    }

    private void deferAutoMergeDuringCommit(FDBRecordStore store) {
        // Always defer merges
        store.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
    }

    protected static boolean notAllRangesExhausted(Tuple cont, Tuple end) {
        // if cont isn't null, it means that the cursor was not exhausted
        // if end isn't null, it means that the range is a segment (i.e. closed or half-open interval) - the rangeSet may contain more unbuilt ranges
        return end != null || cont != null;
    }

    protected ScanProperties scanPropertiesWithLimits(boolean isIdempotent) {
        final IsolationLevel isolationLevel =
                isIdempotent ?
                IsolationLevel.SNAPSHOT :
                IsolationLevel.SERIALIZABLE;
        final boolean isReverse = policy.isReverseScanOrder();
        final ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel)
                .setReturnedRowLimit(getLimit() + (isReverse ? 0 : 1)); // always respect limit in this path; +1 allows a continuation item in forward scan

        return new ScanProperties(executeProperties.build(), isReverse);
    }

    // rebuildIndexAsync - builds the whole index inline (without committing)
    @Nonnull
    public CompletableFuture<Void> rebuildIndexAsync(@Nonnull FDBRecordStore store) {
        validateOrThrowEx(!policy.isReverseScanOrder(), "rebuild do not support reverse scan order");
        return forEachTargetIndex(index -> store.clearAndMarkIndexWriteOnly(index).thenCompose(bignore -> {
            // Insert the full range into the range set. (The internal rebuild method only indexes the records and
            // does not update the range set.) This is important because if marking the index as readable fails (for
            // example, because of uniqueness violations), we still want to record in the range set that the entire
            // range was built so that future index builds don't re-scan the record data and so that non-idempotent
            // indexes know to update the index on all record saves.
            IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(store, index);
            return rangeSet.insertRangeAsync(null, null);
        }))
                .thenCompose(vignore -> setIndexingTypeOrThrow(store, false))
                .thenCompose(vignore -> rebuildIndexInternalAsync(store))
                .whenComplete((ignore, ignoreEx) ->  clearHeartbeats(store));
    }

    abstract CompletableFuture<Void> rebuildIndexInternalAsync(FDBRecordStore store);

    protected void validateOrThrowEx(boolean isValid, @Nonnull String msg) {
        if (!isValid) {
            throw new ValidationException(msg,
                    LogMessageKeys.INDEX_NAME, common.getTargetIndexesNames(),
                    LogMessageKeys.SOURCE_INDEX, policy.getSourceIndex(),
                    LogMessageKeys.INDEXER_ID, common.getIndexerId());
        }
    }

    protected void validateSameMetadataOrThrow(FDBRecordStore store) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final RecordMetaDataProvider recordMetaDataProvider = common.getRecordStoreBuilder().getMetaDataProvider();
        if ( recordMetaDataProvider == null || !metaData.equals(recordMetaDataProvider.getRecordMetaData())) {
            throw new MetaDataException("Store does not have the same metadata");
        }
    }

    enum IndexingStampOperation {
        QUERY, BLOCK, UNBLOCK,
    }

    CompletableFuture<Map<String, IndexBuildProto.IndexBuildIndexingStamp>> performIndexingStampOperation(@Nullable IndexingStampOperation op,
                                                                                                          @Nullable String id,
                                                                                                          @Nullable Long ttlSeconds) {
        ConcurrentHashMap<String, IndexBuildProto.IndexBuildIndexingStamp> newStamps = new ConcurrentHashMap<>();
        return getRunner().runAsync(context -> openRecordStore(context).thenCompose(store ->
            forEachTargetIndex(index -> store.loadIndexingTypeStampAsync(index)
                    .thenApply(stamp -> performIndexingStampOperation(newStamps, store, index, stamp, op, id, ttlSeconds)))
        )).thenApply(ignore -> newStamps);
    }

    boolean performIndexingStampOperation(@Nonnull ConcurrentHashMap<String, IndexBuildProto.IndexBuildIndexingStamp> newStamps,
                                          @Nonnull FDBRecordStore store,
                                          @Nonnull Index index,
                                          @Nullable IndexBuildProto.IndexBuildIndexingStamp stamp,
                                          @Nullable IndexingStampOperation op,
                                          @Nullable String id,
                                          @Nullable  Long ttlSeconds) {

        if (op == null || stamp == null || op.equals(IndexingStampOperation.QUERY)) {
            newStamps.put(index.getName(), stamp != null ? stamp :
                                           IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                                                   .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.NONE)
                                                   .build());
            return false;
        }

        IndexBuildProto.IndexBuildIndexingStamp.Builder builder = stamp.toBuilder();

        if (op == IndexingStampOperation.BLOCK) {
            builder.setBlock(true);
            builder.setBlockID(id == null ? "" : id);
            if (ttlSeconds != null && ttlSeconds > 0) {
                builder.setBlockExpireEpochMilliSeconds(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(ttlSeconds));
            }
        }
        if (op == IndexingStampOperation.UNBLOCK &&
                (id == null || id.isEmpty() || id.equals(stamp.getBlockID()))) {
            builder.setBlock(false);
        }
        final IndexBuildProto.IndexBuildIndexingStamp newStamp = builder.build();
        store.saveIndexingTypeStamp(index, newStamp);
        newStamps.put(index.getName(), newStamp);
        return true;
    }

    public CompletableFuture<Map<UUID, IndexBuildProto.IndexBuildHeartbeat>> getIndexingHeartbeats(int maxCount) {
        return getRunner().runAsync(context -> openRecordStore(context)
                        .thenCompose(store -> IndexingHeartbeat.getIndexingHeartbeats(store, common.getPrimaryIndex(), maxCount)));
    }

    public CompletableFuture<Integer> clearIndexingHeartbeats(long minAgeMilliseconds, int maxIteration) {
        return getRunner().runAsync(context -> openRecordStore(context)
                .thenCompose(store -> IndexingHeartbeat.clearIndexingHeartbeats(store, common.getPrimaryIndex(), minAgeMilliseconds, maxIteration)));
    }

    /**
     * Thrown when the indexing process fails to meet a precondition.
     */
    @SuppressWarnings("serial")
    public static class ValidationException extends RecordCoreException {
        ValidationException(@Nonnull String msg, @Nullable Object ... keyValues) {
            super(msg, keyValues);
        }
    }

    public static boolean isValidationException(@Nullable Throwable ex) {
        for (Throwable current = ex;
                current != null;
                current = current.getCause()) {
            if (current instanceof ValidationException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Thrown when the indexing process exceeds the time limit.
     */
    @SuppressWarnings("serial")
    public static class TimeLimitException extends RecordCoreException {
        TimeLimitException(@Nonnull String msg, @Nullable Object ... keyValues) {
            super(msg, keyValues);
        }
    }

    /**
     * thrown when partly built by another method.
     */
    @SuppressWarnings("serial")
    public static class PartlyBuiltException extends RecordCoreException {
        final IndexBuildProto.IndexBuildIndexingStamp savedStamp;
        final IndexBuildProto.IndexBuildIndexingStamp expectedStamp;
        final String indexName;

        public PartlyBuiltException(IndexBuildProto.IndexBuildIndexingStamp savedStamp,
                                    IndexBuildProto.IndexBuildIndexingStamp expectedStamp,
                                    Index index,
                                    UUID uuid,
                                    @Nonnull String msg) {
            super(msg,
                    LogMessageKeys.INDEX_NAME, index,
                    LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                    LogMessageKeys.EXPECTED, stampToString(expectedStamp),
                    LogMessageKeys.ACTUAL, stampToString(savedStamp),
                    LogMessageKeys.INDEXER_ID, uuid);
            this.savedStamp = savedStamp;
            this.expectedStamp = expectedStamp;
            this.indexName = index.getName();
        }

        public boolean wasBlocked() {
            return savedStamp.getBlock();
        }

        public IndexBuildProto.IndexBuildIndexingStamp getSavedStamp() {
            return savedStamp;
        }

        public String getSavedStampString() {
            return stampToString(getSavedStamp());
        }

        public IndexBuildProto.IndexBuildIndexingStamp getExpectedStamp() {
            return expectedStamp;
        }

        public String getExpectedStampString() {
            return stampToString(getExpectedStamp());
        }

        public static String stampToString(IndexBuildProto.IndexBuildIndexingStamp stamp) {
            if (stamp == null) {
                return "IndexingStamp(<null>)";
            }
            final StringBuilder str = new StringBuilder("IndexingStamp(")
                    .append(stamp.getMethod())
                    .append(", target:")
                    .append(stamp.getTargetIndexList());
            if (stamp.getBlock()) {
                str.append(", blocked");
                String id = stamp.getBlockID();
                if (!id.isEmpty()) {
                    str.append(", blockId{").append(id).append("} ");
                }
                long expirationMillis = stamp.getBlockExpireEpochMilliSeconds();
                if (expirationMillis > 0) {
                    try {
                        str.append(", blockExpires{").append(Instant.ofEpochMilli(expirationMillis)).append("}");
                    } catch (DateTimeException ignore) {
                        str.append(", blockExpires{value=").append(expirationMillis).append("}");
                    }
                }
            }
            return str.append(")").toString();
        }

        public String getIndexName() {
            return indexName;
        }
    }

    public static PartlyBuiltException getAPartlyBuiltExceptionIfApplicable(@Nullable Throwable ex) {
        return findException(ex, PartlyBuiltException.class);
    }

    /**
     * thrown if some or all of the indexes to build became readable (maybe by another process).
     */
    @SuppressWarnings("serial")
    public static class UnexpectedReadableException extends RecordCoreException {
        final boolean allReadable;

        public UnexpectedReadableException(boolean allReadable, @Nonnull String msg, @Nullable Object ... keyValues) {
            super(msg, keyValues);
            this.allReadable = allReadable;
        }
    }

    public static UnexpectedReadableException getUnexpectedReadableIfApplicable(@Nullable Throwable ex) {
        return findException(ex, UnexpectedReadableException.class);
    }

    protected static <T> T findException(@Nullable Throwable ex, Class<T> classT) {
        Set<Throwable> seenSet = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable current = ex;
                current != null && !seenSet.contains(current);
                current = current.getCause()) {
            if (classT.isInstance(current)) {
                return classT.cast(current);
            }
            seenSet.add(current);
        }
        return null;
    }

    protected static boolean shouldLessenWork(@Nullable FDBException ex) {
        // These error codes represent a list of errors that can occur if there is too much work to be done
        // in a single transaction.
        if (ex == null) {
            return false;
        }
        final Set<Integer> lessenWorkCodes = new HashSet<>(Arrays.asList(
                FDBError.TIMED_OUT.code(),
                FDBError.TRANSACTION_TOO_OLD.code(),
                FDBError.NOT_COMMITTED.code(),
                FDBError.TRANSACTION_TIMED_OUT.code(),
                FDBError.COMMIT_READ_INCOMPLETE.code(),
                FDBError.TRANSACTION_TOO_LARGE.code()));
        return lessenWorkCodes.contains(ex.getCode());
    }
}



