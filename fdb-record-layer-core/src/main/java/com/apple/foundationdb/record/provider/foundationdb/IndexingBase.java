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
import com.apple.foundationdb.Range;
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.StoreTimerSnapshot;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.synchronizedsession.SynchronizedSession;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A base class for different types of online indexing process.
 */
@API(API.Status.INTERNAL)
public abstract class IndexingBase {

    private static final Object INDEX_BUILD_LOCK_KEY = 0L;
    private static final Object INDEX_BUILD_SCANNED_RECORDS = 1L;
    private static final Object INDEX_BUILD_TYPE_VERSION = 2L;
    private static final Object INDEX_SCRUBBED_INDEX_RANGES = 3L;
    private static final Object INDEX_SCRUBBED_RECORDS_RANGES = 4L;

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
    private long lastTypeStampCheckMillis;
    private Map<String, IndexingMerger> indexingMergerMap = null;

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
        IndexState expectedIndexState = isScrubber ? IndexState.READABLE : IndexState.WRITE_ONLY;
        this.throttle = new IndexingThrottle(common, expectedIndexState);
        this.startingTimeMillis = System.currentTimeMillis();
        this.lastTypeStampCheckMillis = startingTimeMillis;
    }

    // helper functions
    protected FDBDatabaseRunner getRunner() {
        return common.getRunner();
    }

    @Nonnull
    private static Subspace indexBuildSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, Object key) {
        return store.getUntypedRecordStore().indexBuildSubspace(index).subspace(Tuple.from(key));
    }

    @Nonnull
    protected static Subspace indexBuildLockSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_LOCK_KEY);
    }

    @Nonnull
    protected static Subspace indexBuildScannedRecordsSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_SCANNED_RECORDS);
    }

    @Nonnull
    protected static Subspace indexBuildTypeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        return indexBuildSubspace(store, index, INDEX_BUILD_TYPE_VERSION);
    }

    @Nonnull
    public static Subspace indexScrubIndexRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        // This subspace holds the scrubbed ranges of the index itself (when looking for dangling entries)
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_INDEX_RANGES);
    }

    @Nonnull
    public static Subspace indexScrubRecordsRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        // This subspace hods the scrubbed ranges of the records (when looking for missing index entries)
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_RECORDS_RANGES);
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

    // Turn a (possibly null) key into its tuple representation.
    @Nullable
    protected static Tuple convertOrNull(@Nullable Key.Evaluated key) {
        return (key == null) ? null : key.toTuple();
    }

    @Nonnull
    protected CompletableFuture<FDBStoredRecord<Message>> recordIfInIndexedTypes(FDBStoredRecord<Message> rec) {
        return CompletableFuture.completedFuture( rec != null && common.getAllRecordTypes().contains(rec.getRecordType()) ? rec : null);
    }

    // buildIndexAsync - the main indexing function. Builds and commits indexes asynchronously; throttling to avoid overloading the system.
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Void> buildIndexAsync(boolean markReadable, boolean useSyncLock) {
        KeyValueLogMessage message = KeyValueLogMessage.build("build index online",
                LogMessageKeys.SHOULD_MARK_READABLE, markReadable);
        long startNanos = System.nanoTime();
        final CompletableFuture<Void> buildIndexAsyncFuture;
        FDBDatabaseRunner runner = getRunner();
        Index index = common.getPrimaryIndex();
        if (runner.getTimer() != null) {
            lastProgressSnapshot = StoreTimerSnapshot.from(runner.getTimer());
        }
        if (useSyncLock) {
            buildIndexAsyncFuture = runner
                    .runAsync(context -> openRecordStore(context).thenApply(store -> indexBuildLockSubspace(store, index)),
                            common.indexLogMessageKeyValues("IndexingBase::indexBuildLockSubspace"))
                    .thenCompose(lockSubspace -> runner.startSynchronizedSessionAsync(lockSubspace, common.config.getLeaseLengthMillis()))
                    .thenCompose(synchronizedRunner -> {
                        message.addKeyAndValue(LogMessageKeys.SESSION_ID, synchronizedRunner.getSessionId());
                        return runWithSynchronizedRunnerAndEndSession(synchronizedRunner,
                                () -> handleStateAndDoBuildIndexAsync(markReadable, message));
                    });
        } else {
            message.addKeyAndValue(LogMessageKeys.SESSION_ID, "none");
            common.setSynchronizedSessionRunner(null);
            buildIndexAsyncFuture = handleStateAndDoBuildIndexAsync(markReadable, message);
        }
        return buildIndexAsyncFuture.whenComplete((vignore, ex) -> {
            message.addKeysAndValues(indexingLogMessageKeyValues()) // add these here to pick up state accumulated during build
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .addKeyAndValue(LogMessageKeys.TOTAL_MICROS, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos));
            if (LOGGER.isWarnEnabled() && (ex != null)) {
                message.addKeyAndValue(LogMessageKeys.RESULT, "failure");
                LOGGER.warn(message.toString(), ex);
            } else if (LOGGER.isInfoEnabled()) {
                message.addKeyAndValue(LogMessageKeys.RESULT, "success");
                LOGGER.info(message.toString());
            }
        });
    }

    @SuppressWarnings("PMD.CloseResource")
    private <T> CompletableFuture<T> runWithSynchronizedRunnerAndEndSession(
            @Nonnull SynchronizedSessionRunner newSynchronizedRunner, @Nonnull Supplier<CompletableFuture<T>> runnable) {
        final SynchronizedSessionRunner currentSynchronizedRunner1 = common.getSynchronizedSessionRunner();
        if (currentSynchronizedRunner1 == null) {
            common.setSynchronizedSessionRunner(newSynchronizedRunner);
            return MoreAsyncUtil.composeWhenComplete(runnable.get(), (result, ex) -> {
                final SynchronizedSessionRunner currentSynchronizedRunner2 = common.getSynchronizedSessionRunner();
                if (newSynchronizedRunner.equals(currentSynchronizedRunner2)) {
                    common.setSynchronizedSessionRunner(null);
                } else {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn(KeyValueLogMessage.build("synchronizedSessionRunner was modified during the run",
                                LogMessageKeys.SESSION_ID, newSynchronizedRunner.getSessionId(),
                                LogMessageKeys.INDEXER_SESSION_ID, currentSynchronizedRunner2 == null ? null : currentSynchronizedRunner2.getSessionId())
                                .addKeysAndValues(common.indexLogMessageKeyValues())
                                .toString());
                    }
                }
                return newSynchronizedRunner.endSessionAsync();
            }, getRunner().getDatabase()::mapAsyncToSyncException);
        } else {
            return newSynchronizedRunner.endSessionAsync().thenApply(vignore -> {
                throw new RecordCoreException("another synchronized session is running on the indexer",
                        LogMessageKeys.SESSION_ID, newSynchronizedRunner.getSessionId(),
                        LogMessageKeys.INDEXER_SESSION_ID, currentSynchronizedRunner1.getSessionId());
            });
        }
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
                validateOrThrowEx(indexState == IndexState.READABLE, "Scrubber was called for a non-readable index. Index:" + primaryIndex.getName() + " State: " + indexState);
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
        ).thenCompose(this::markIndexReadable).thenApply(ignore -> null);
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
        return common.getNonSynchronizedRunner().runAsync(context -> openRecordStore(context).thenCompose(store ->
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
                markIndexReadableSingleTarget(index, anythingChanged, runtimeExceptionAtomicReference)
        ).thenApply(ignore -> {
            RuntimeException ex = runtimeExceptionAtomicReference.get();
            if (ex != null) {
                throw ex;
            }
            return anythingChanged.get();
        });
    }

    private CompletableFuture<Boolean> markIndexReadableSingleTarget(Index index, AtomicBoolean anythingChanged,
                                                                     AtomicReference<RuntimeException> runtimeExceptionAtomicReference) {
        // An extension function to reduce markIndexReadable's complexity
        return common.getNonSynchronizedRunner().runAsync(context ->
                common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync()
                        .thenCompose(store ->
                                policy.shouldAllowUniquePendingState(store) ?
                                store.markIndexReadableOrUniquePending(index) :
                                store.markIndexReadable(index))
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

        return forEachTargetIndex(index -> setIndexingTypeOrThrow(store, continuedBuild, index, indexingTypeStamp));
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
                        throw newPartlyBuiltException(continuedBuild, savedStamp, newStamp, index);
                    }
                    if (areSimilar(newStamp, savedStamp)) {
                        // Similar stamps, replace it
                        store.saveIndexingTypeStamp(index, newStamp);
                        return AsyncUtil.DONE;
                    }
                    // Here: check if type conversion is allowed
                    if (continuedBuild && shouldAllowTypeConversionContinue(newStamp, savedStamp)) {
                        // Special case: partly built by another indexing method, but may be continued with the current one
                        if (savedStamp.getMethod().equals(IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS)) {
                            // Here: throw an exception if there is an active multi-target session that includes this index
                            final String otherPrimaryIndexName = savedStamp.getTargetIndex(0);
                            if (!otherPrimaryIndexName.equals(common.getPrimaryIndex().getName())) {
                                // Note: For protection, avoid breaking an active multi-target session. This leads to a certain
                                // inconsistency for buildIndex that is called with a false `useSyncLock` - sync lock will be
                                // checked during a method conversion, but not during a simple "same method" continue.
                                return throwIfSyncedLock(otherPrimaryIndexName, store, newStamp, savedStamp)
                                        .thenCompose(ignore -> {
                                            store.saveIndexingTypeStamp(index, newStamp);
                                            return AsyncUtil.DONE;
                                        });
                            }
                        }
                        store.saveIndexingTypeStamp(index, newStamp);
                        return AsyncUtil.DONE;
                    }
                    // Here: conversion is not allowed, yet there might be a case of a WRITE_ONLY index that hadn't scanned any records.
                    if (forceStampOverwrite) {  // and a continued Build
                        // check if partly built
                        return isWriteOnlyButNoRecordScanned(store, index)
                                .thenCompose(noRecordScanned ->
                                throwUnlessNoRecordWasScanned(noRecordScanned, store, index, newStamp,
                                        savedStamp, continuedBuild));
                    }
                    // fall down to exception
                    throw newPartlyBuiltException(continuedBuild, savedStamp, newStamp, index);
                });
    }

    private boolean shouldAllowTypeConversionContinue(IndexBuildProto.IndexBuildIndexingStamp newStamp, IndexBuildProto.IndexBuildIndexingStamp savedStamp) {
        return policy.shouldAllowTypeConversionContinue(newStamp.getMethod(), savedStamp.getMethod());
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

    CompletableFuture<Void> throwIfSyncedLock(String otherIndexName, FDBRecordStore store, IndexBuildProto.IndexBuildIndexingStamp newStamp, IndexBuildProto.IndexBuildIndexingStamp savedStamp) {
        final Index otherIndex = store.getRecordMetaData().getIndex(otherIndexName);
        final Subspace mainLockSubspace = indexBuildLockSubspace(store, otherIndex);
        return SynchronizedSession.checkActiveSessionExists(store.ensureContextActive(), mainLockSubspace)
                        .thenApply(hasActiveSession -> {
                            if (Boolean.TRUE.equals(hasActiveSession)) {
                                throw new SynchronizedSessionLockedException("Failed to takeover indexing while part of a multi-target with an existing session in progress")
                                        .addLogInfo(LogMessageKeys.SUBSPACE, mainLockSubspace)
                                        .addLogInfo(LogMessageKeys.PRIMARY_INDEX, otherIndexName)
                                        .addLogInfo(LogMessageKeys.EXPECTED, PartlyBuiltException.stampToString(newStamp))
                                        .addLogInfo(LogMessageKeys.ACTUAL, PartlyBuiltException.stampToString(savedStamp));
                            }
                            return null;
                        });

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
        final IndexBuildProto.IndexBuildIndexingStamp fakeSavedStamp = IndexingByRecords.compileIndexingTypeStamp();
        throw newPartlyBuiltException(true, fakeSavedStamp, indexingTypeStamp, index);
    }

    @Nonnull
    private CompletableFuture<Void> throwUnlessNoRecordWasScanned(boolean noRecordScanned,
                                                                  FDBRecordStore store,
                                                                  Index index,
                                                                  IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp,
                                                                  IndexBuildProto.IndexBuildIndexingStamp savedStamp,
                                                                  boolean continuedBuild) {
        // Ditto (a complicated way to reduce complexity)
        if (noRecordScanned) {
            // we can safely overwrite the previous type stamp
            store.saveIndexingTypeStamp(index, indexingTypeStamp);
            return AsyncUtil.DONE;
        }
        // A force overwrite cannot be allowed when partly built
        throw newPartlyBuiltException(continuedBuild, savedStamp, indexingTypeStamp, index);
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Void> setScrubberTypeOrThrow(FDBRecordStore store) {
        // HERE: The index must be readable, checked by the caller
        //   if scrubber had already run and still have missing ranges, do nothing
        //   else: clear ranges and overwrite type-stamp
        IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp = getIndexingTypeStamp(store);
        validateOrThrowEx(indexingTypeStamp.getMethod().equals(IndexBuildProto.IndexBuildIndexingStamp.Method.SCRUB_REPAIR),
                "Not a scrubber type-stamp");

        final Index index = common.getIndex(); // Note: the scrubbers do not support multi target (yet)
        IndexingRangeSet indexRangeSet = IndexingRangeSet.forScrubbingIndex(store, index);
        IndexingRangeSet recordsRangeSet = IndexingRangeSet.forScrubbingRecords(store, index);
        final CompletableFuture<Range> indexRangeFuture = indexRangeSet.firstMissingRangeAsync();
        final CompletableFuture<Range> recordRangeFuture = recordsRangeSet.firstMissingRangeAsync();
        return indexRangeFuture.thenCompose(indexRange -> {
            if (indexRange == null) {
                // Here: no un-scrubbed index range was left for this call. We will
                // erase the 'ranges' data to allow a fresh index re-scrubbing.
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(KeyValueLogMessage.build("Reset scrubber's index range")
                            .addKeysAndValues(common.indexLogMessageKeyValues())
                            .toString());
                }
                indexRangeSet.clear();
            }
            return recordRangeFuture.thenAccept(recordRange -> {
                if (recordRange == null) {
                    // Here: no un-scrubbed records range was left for this call. We will
                    // erase the 'ranges' data to allow a fresh records re-scrubbing.
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(KeyValueLogMessage.build("Reset scrubber's records range")
                                .addKeysAndValues(common.indexLogMessageKeyValues())
                                .toString());
                    }
                    recordsRangeSet.clear();
                }
            });
        });
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

    RecordCoreException newPartlyBuiltException(boolean continuedBuild,
                                                IndexBuildProto.IndexBuildIndexingStamp savedStamp,
                                                IndexBuildProto.IndexBuildIndexingStamp expectedStamp,
                                                Index index) {
        return new PartlyBuiltException(savedStamp, expectedStamp, index, common.getUuid(),
                savedStamp.getBlock() ?
                "This index was partly built, and blocked" :
                "This index was partly built by another method");
    }

    // Helpers for implementing modules. Some of them are public to support unit-testing.
    protected CompletableFuture<Boolean> doneOrThrottleDelayAndMaybeLogProgress(boolean done, SubspaceProvider subspaceProvider, List<Object> additionalLogMessageKeyValues) {
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
                    subspaceProvider.logKey(), subspaceProvider,
                    LogMessageKeys.DELAY, toWait)
                    .addKeysAndValues(additionalLogMessageKeyValues != null ? additionalLogMessageKeyValues : Collections.emptyList())
                    .addKeysAndValues(indexingLogMessageKeyValues())
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .addKeysAndValues(throttle.logMessageKeyValues())
                    .addKeysAndValues(metricsDiff == null ? Collections.emptyMap() : metricsDiff.getKeysAndValues())
                    .toString());
        }

        validateTimeLimit(toWait);

        CompletableFuture<Boolean> delay = MoreAsyncUtil.delayedFuture(toWait, TimeUnit.MILLISECONDS).thenApply(vignore3 -> true);
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

    public long getTotalRecordsScannedSuccessfully() {
        return throttle.getTotalRecordsScannedSuccessfully();
    }

    protected void timerIncrement(FDBStoreTimer.Counts event) {
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
                            final Subspace scannedRecordsSubspace = indexBuildScannedRecordsSubspace(store, index);
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
        final long minimalInterval = policy.getCheckIndexingMethodFrequencyMilliseconds();
        if (minimalInterval < 0 || isScrubber) {
            return AsyncUtil.DONE;
        }
        if (minimalInterval > 0) {
            final long now = System.currentTimeMillis();
            if (now < lastTypeStampCheckMillis + minimalInterval) {
                return AsyncUtil.DONE;
            }
            lastTypeStampCheckMillis = now;
        }
        final IndexBuildProto.IndexBuildIndexingStamp expectedTypeStamp = getIndexingTypeStamp(store);
        return forEachTargetIndex(index ->
                store.loadIndexingTypeStampAsync(index)
                        .thenAccept(typeStamp -> validateTypeStamp(typeStamp, expectedTypeStamp, index)));
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
                    index, common.getUuid(), "Indexing stamp had changed");
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
                                                       BiFunction<FDBRecordStore, AtomicLong,  CompletableFuture<Boolean>> iterateRange,
                                                       @Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace) {
        return iterateAllRanges(additionalLogMessageKeyValues, iterateRange, subspaceProvider, subspace, null);
    }

    protected CompletableFuture<Void> iterateAllRanges(List<Object> additionalLogMessageKeyValues,
                                                       BiFunction<FDBRecordStore, AtomicLong,  CompletableFuture<Boolean>> iterateRange,
                                                       @Nonnull SubspaceProvider subspaceProvider, @Nonnull Subspace subspace,
                                                       @Nullable Function<FDBException, Optional<Boolean>> shouldReturnQuietly) {

        return AsyncUtil.whileTrue(() ->
                    throttle.buildCommitRetryAsync(iterateRange, shouldReturnQuietly, additionalLogMessageKeyValues, true)
                            .handle((hasMore, ex) -> {
                                if (ex == null) {
                                    final Set<Index> indexSet = throttle.getAndResetMergeRequiredIndexes();
                                    if (indexSet != null && !indexSet.isEmpty()) {
                                        return mergeIndexes(indexSet, subspaceProvider)
                                                .thenCompose(ignore -> doneOrThrottleDelayAndMaybeLogProgress(!hasMore, subspaceProvider, additionalLogMessageKeyValues));
                                    }
                                    return doneOrThrottleDelayAndMaybeLogProgress(!hasMore, subspaceProvider, additionalLogMessageKeyValues);
                                }
                                final RuntimeException unwrappedEx = getRunner().getDatabase().mapAsyncToSyncException(ex);
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info(KeyValueLogMessage.of("possibly non-fatal error encountered building range",
                                            LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack())), ex);
                                }
                                throw unwrappedEx;
                            }).thenCompose(Function.identity()),
                getRunner().getExecutor());
    }

    public CompletableFuture<Void> mergeIndexes() {
        return mergeIndexes(new HashSet<>(common.getTargetIndexes()), common.getRecordStoreBuilder().subspaceProvider);
    }

    private CompletableFuture<Void> mergeIndexes(Set<Index> indexSet, @Nullable SubspaceProvider subspaceProvider) {
        return AsyncUtil.whenAll(indexSet.stream()
                .map(index -> getIndexingMerger(index).mergeIndex(subspaceProvider)
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
                .thenCompose(vignore -> rebuildIndexInternalAsync(store));
    }

    abstract CompletableFuture<Void> rebuildIndexInternalAsync(FDBRecordStore store);

    protected void validateOrThrowEx(boolean isValid, @Nonnull String msg) {
        if (!isValid) {
            throw new ValidationException(msg,
                    LogMessageKeys.INDEX_NAME, common.getTargetIndexesNames(),
                    LogMessageKeys.SOURCE_INDEX, policy.getSourceIndex(),
                    LogMessageKeys.INDEXER_ID, common.getUuid());
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

    public static boolean isTimeLimitException(@Nullable Throwable ex) {
        for (Throwable current = ex;
                current != null;
                current = current.getCause()) {
            if (current instanceof TimeLimitException) {
                return true;
            }
        }
        return false;
    }

    /**
     * thrown when partly built by another method.
     */
    @SuppressWarnings("serial")
    public static class  PartlyBuiltException extends RecordCoreException {
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
                if (id != null && !id.isEmpty()) {
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



