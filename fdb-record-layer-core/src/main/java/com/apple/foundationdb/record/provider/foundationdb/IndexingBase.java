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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordFromStoredRecordPlan;
import com.apple.foundationdb.record.query.plan.synthetic.SyntheticRecordPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
    private boolean forceStampOverwrite = false;
    private final long startingTimeMillis;

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
    protected static Subspace indexScrubIndexRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
        // This subspace holds the scrubbed ranges of the index itself (when looking for dangling entries)
        return indexBuildSubspace(store, index, INDEX_SCRUBBED_INDEX_RANGES);
    }

    @Nonnull
    protected static Subspace indexScrubRecordsRangeSubspace(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index) {
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
    public CompletableFuture<Void> buildIndexAsync(boolean markReadable) {
        KeyValueLogMessage message = KeyValueLogMessage.build("build index online",
                LogMessageKeys.SHOULD_MARK_READABLE, markReadable)
                .addKeysAndValues(indexingLogMessageKeyValues())
                .addKeysAndValues(common.indexLogMessageKeyValues());
        final CompletableFuture<Void> buildIndexAsyncFuture;
        FDBDatabaseRunner runner = common.getRunner();
        Index index = common.getPrimaryIndex();
        if (common.isUseSynchronizedSession()) {
            buildIndexAsyncFuture = runner
                    .runAsync(context -> openRecordStore(context).thenApply(store -> indexBuildLockSubspace(store, index)),
                            common.indexLogMessageKeyValues("IndexingBase::indexBuildLockSubspace"))
                    .thenCompose(lockSubspace -> runner.startSynchronizedSessionAsync(lockSubspace, common.getLeaseLengthMillis()))
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
            if (LOGGER.isWarnEnabled() && (ex != null)) {
                message.addKeyAndValue(LogMessageKeys.RESULT, "failure");
                LOGGER.warn(message.toString(), ex);
            } else if (LOGGER.isInfoEnabled()) {
                message.addKeyAndValue(LogMessageKeys.RESULT, "success");
                LOGGER.info(message.toString());
            }
        });
    }

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
                forceStampOverwrite = true; // The code can work without this line, but it'll save probing the missing ranges
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
        return getRunner().runAsync(context -> openRecordStore(context).thenCompose(store ->
            forEachTargetIndex(index -> {
                if (store.isIndexReadable(index)) {
                    return AsyncUtil.DONE;
                }
                final RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(index));
                return rangeSet.missingRanges(store.ensureContextActive()).iterator().onHasNext()
                        .thenCompose(hasNext -> {
                            if (hasNext) {
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
        AtomicBoolean allReadable = new AtomicBoolean(true);
        return getRunner().runAsync(context -> openRecordStore(context)
                .thenCompose(store -> forEachTargetIndex(index ->
                        store.markIndexReadable(index).thenApply(built -> {
                            if (!built) {
                                allReadable.set(false);
                            }
                            return null;
                        }))
                        .thenApply(ignore -> allReadable.get())
                ), common.indexLogMessageKeyValues("IndexingBase::markReadable"));
    }

    public void enforceStampOverwrite() {
        forceStampOverwrite = true; // must overwrite a previous indexing method's stamp
    }

    @Nonnull
    private CompletableFuture<Void> setIndexingTypeOrThrow(FDBRecordStore store, boolean continuedBuild) {
        // continuedBuild is set if this session isn't a continuation of a previous indexing
        Transaction transaction = store.getContext().ensureActive();
        IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp = getIndexingTypeStamp(store);

        return forEachTargetIndex(index -> setIndexingTypeOrThrow(store, continuedBuild, transaction, index, indexingTypeStamp));
    }

    @Nonnull
    private CompletableFuture<Void> setIndexingTypeOrThrow(FDBRecordStore store, boolean continuedBuild, Transaction transaction, Index index, IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp) {
        byte[] stampKey = indexBuildTypeSubspace(store, index).getKey();
        if (forceStampOverwrite && !continuedBuild) {
            // Fresh session + overwrite = no questions asked
            transaction.set(stampKey, indexingTypeStamp.toByteArray());
            return AsyncUtil.DONE;
        }
        return transaction.get(stampKey)
                .thenCompose(bytes -> {
                    if (bytes == null) {
                        if (continuedBuild && indexingTypeStamp.getMethod() !=
                                              IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS) {
                            // backward compatibility - maybe continuing an old BY_RECORD session
                            return isWriteOnlyButNoRecordScanned(store, index)
                                    .thenCompose(noRecordScanned -> throwAsByRecordsUnlessNoRecordWasScanned(noRecordScanned, transaction, index, stampKey, indexingTypeStamp));
                        }
                        // Here: either not a continuedBuild (new session), or a BY_RECORD session (allowed to overwrite the null stamp)
                        transaction.set(stampKey, indexingTypeStamp.toByteArray());
                        return AsyncUtil.DONE;
                    }
                    // Here: has non-null type stamp
                    IndexBuildProto.IndexBuildIndexingStamp savedStamp = parseTypeStampOrThrow(bytes);
                    if (indexingTypeStamp.equals(savedStamp)) {
                        // A matching stamp is already there - One less thing to worry about
                        return AsyncUtil.DONE;
                    }
                    if (continuedBuild &&
                            indexingTypeStamp.getMethod() == IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS &&
                            savedStamp.getMethod() == IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS) {
                        // Special case: partly built with multi target, but may be continued indexing on its own
                        transaction.set(stampKey, indexingTypeStamp.toByteArray());
                        return AsyncUtil.DONE;
                    }
                    if (forceStampOverwrite) {  // and a continued Build
                        // check if partly built
                        return isWriteOnlyButNoRecordScanned(store, index)
                                .thenCompose(noRecordScanned ->
                                throwUnlessNoRecordWasScanned(noRecordScanned, transaction, index, stampKey, indexingTypeStamp,
                                        savedStamp, continuedBuild));
                    }
                    // fall down to exception
                    throw newPartlyBuildException(continuedBuild, savedStamp, indexingTypeStamp, index);
                });
    }

    @Nonnull
    private CompletableFuture<Void> throwAsByRecordsUnlessNoRecordWasScanned(boolean noRecordScanned, Transaction transaction,
                                                                             Index index, byte[] stampKey,
                                                                             IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp) {
        // A complicated way to reduce complexity.
        if (noRecordScanned) {
            // an empty type stamp, and nothing was indexed - it is safe to write stamp
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.build("no scanned ranges - continue indexing")
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .toString());
            }
            transaction.set(stampKey, indexingTypeStamp.toByteArray());
            return AsyncUtil.DONE;
        }
        // Here: there is no type stamp, but indexing is ongoing. For backward compatibility reasons, we'll consider it a BY_RECORDS stamp
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.build("continuation with null type stamp, assuming previous by-records scan")
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .toString());
        }
        final IndexBuildProto.IndexBuildIndexingStamp fakeSavedStamp = IndexingByRecords.compileIndexingTypeStamp();
        throw newPartlyBuildException(true, fakeSavedStamp, indexingTypeStamp, index);
    }

    @Nonnull
    private CompletableFuture<Void> throwUnlessNoRecordWasScanned(boolean noRecordScanned, Transaction transaction,
                                                                  Index index, byte[] stampKey,
                                                                  IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp,
                                                                  IndexBuildProto.IndexBuildIndexingStamp savedStamp,
                                                                  boolean continuedBuild) {
        // Ditto (a complicated way to reduce complexity)
        if (noRecordScanned) {
            // we can safely overwrite the previous type stamp
            transaction.set(stampKey, indexingTypeStamp.toByteArray());
            return AsyncUtil.DONE;
        }
        // A force overwrite cannot be allowed when partly built
        throw newPartlyBuildException(continuedBuild, savedStamp, indexingTypeStamp, index);
    }

    @Nonnull
    private CompletableFuture<Void> setScrubberTypeOrThrow(FDBRecordStore store) {
        // HERE: The index must be readable, checked by the caller
        //   if scrubber had already run and still have missing ranges, do nothing
        //   else: clear ranges and overwrite type-stamp
        Transaction tr = store.getContext().ensureActive();
        IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp = getIndexingTypeStamp(store);
        validateOrThrowEx(indexingTypeStamp.getMethod().equals(IndexBuildProto.IndexBuildIndexingStamp.Method.SCRUB_REPAIR),
                "Not a scrubber type-stamp");

        final Index index = common.getIndex(); // Note: the scrubbers do not support multi target (yet)
        final Subspace indexesRangeSubspace = indexScrubIndexRangeSubspace(store, index);
        final Subspace recordsRangeSubspace = indexScrubRecordsRangeSubspace(store, index);
        RangeSet indexRangeSet = new RangeSet(indexesRangeSubspace);
        RangeSet recordsRangeSet = new RangeSet(recordsRangeSubspace);
        AsyncIterator<Range> indexRanges = indexRangeSet.missingRanges(tr).iterator();
        AsyncIterator<Range> recordsRanges = recordsRangeSet.missingRanges(tr).iterator();
        return indexRanges.onHasNext().thenCompose(hasNextIndex -> {
            if (Boolean.FALSE.equals(hasNextIndex)) {
                // Here: no un-scrubbed index range was left for this call. We will
                // erase the 'ranges' data to allow a fresh index re-scrubbing.
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.build("Reset scrubber's index range")
                            .addKeysAndValues(common.indexLogMessageKeyValues())
                            .toString());
                }
                tr.clear(indexesRangeSubspace.range());
            }
            return recordsRanges.onHasNext().thenAccept(hasNextRecord -> {
                if (Boolean.FALSE.equals(hasNextRecord)) {
                    // Here: no un-scrubbed records range was left for this call. We will
                    // erase the 'ranges' data to allow a fresh records re-scrubbing.
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(KeyValueLogMessage.build("Reset scrubber's records range")
                                .addKeysAndValues(common.indexLogMessageKeyValues())
                                .toString());
                    }
                    tr.clear(recordsRangeSubspace.range());
                }
            });
        });
    }

    @Nonnull
    abstract IndexBuildProto.IndexBuildIndexingStamp getIndexingTypeStamp(FDBRecordStore store);

    abstract CompletableFuture<Void> buildIndexInternalAsync();

    private IndexBuildProto.IndexBuildIndexingStamp parseTypeStampOrThrow(byte[] bytes) {
        try {
            return IndexBuildProto.IndexBuildIndexingStamp.parseFrom(bytes);
        } catch (InvalidProtocolBufferException ex) {
            RecordCoreException protoEx = new RecordCoreException("invalid indexing type stamp",
                    LogMessageKeys.INDEX_NAME, common.getTargetIndexesNames(),
                    LogMessageKeys.INDEXER_ID, common.getUuid(),
                    LogMessageKeys.ACTUAL, bytes);
            protoEx.initCause(ex);
            throw protoEx;
        }
    }

    private CompletableFuture<Boolean> isWriteOnlyButNoRecordScanned(FDBRecordStore store, Index index) {
        RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(index));
        AsyncIterator<Range> ranges = rangeSet.missingRanges(store.ensureContextActive()).iterator();
        return ranges.onHasNext().thenCompose(hasNext -> {
                    if (hasNext) {
                        final Range range = ranges.next();
                        return CompletableFuture.completedFuture(RangeSet.isFirstKey(range.begin) && RangeSet.isFinalKey(range.end));
                    }
                    return AsyncUtil.READY_FALSE; // fully built - no missing ranges
                }
        );
    }

    RecordCoreException newPartlyBuildException(boolean continuedBuild,
                                                IndexBuildProto.IndexBuildIndexingStamp savedStamp,
                                                IndexBuildProto.IndexBuildIndexingStamp indexingTypeStamp,
                                                Index index) {
        return new PartlyBuiltException(savedStamp,
                "This index was partly built by another method",
                LogMessageKeys.INDEX_NAME, index,
                LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                LogMessageKeys.INDEXER_ID, common.getUuid(),
                LogMessageKeys.CONTINUED_BUILD, continuedBuild,
                LogMessageKeys.EXPECTED, indexingTypeStamp,
                LogMessageKeys.ACTUAL, savedStamp);
    }

    // Helpers for implementing modules. Some of them are public to support unit-testing.
    protected CompletableFuture<Boolean> throttleDelayAndMaybeLogProgress(SubspaceProvider subspaceProvider, List<Object> additionalLogMessageKeyValues) {
        int limit = getLimit();
        int recordsPerSecond = common.config.getRecordsPerSecond();
        int toWait = (recordsPerSecond == IndexingCommon.UNLIMITED) ? 0 : 1000 * limit / recordsPerSecond;

        if (LOGGER.isInfoEnabled() && shouldLogBuildProgress()) {
            LOGGER.info(KeyValueLogMessage.build("Built Range",
                    subspaceProvider.logKey(), subspaceProvider,
                    LogMessageKeys.LIMIT, limit,
                    LogMessageKeys.DELAY, toWait)
                    .addKeysAndValues(additionalLogMessageKeyValues)
                    .addKeysAndValues(common.indexLogMessageKeyValues())
                    .toString());
        }

        validateTimeLimit(toWait);
        return MoreAsyncUtil.delayedFuture(toWait, TimeUnit.MILLISECONDS).thenApply(vignore3 -> true);
    }

    private void validateTimeLimit(int toWait) {
        final long timeLimitMilliseconds = common.config.getTimeLimitMilliseconds();
        if (timeLimitMilliseconds == OnlineIndexer.Config.UNLIMITED_TIME) {
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
        if (interval == 0 || interval < (now - timeOfLastProgressLogMillis)) {
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
                                                          boolean limitControl,
                                                          @Nullable List<Object> additionalLogMessageKeyValues) {
        return throttle.buildCommitRetryAsync(buildFunction, limitControl, additionalLogMessageKeyValues);
    }

    protected static void timerIncrement(@Nullable FDBStoreTimer timer, FDBStoreTimer.Counts event) {
        // helper function to reduce complexity
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

    protected  <T> CompletableFuture<Void> iterateRangeOnly(@Nonnull FDBRecordStore store,
                                                            @Nonnull RecordCursor<T> cursor,
                                                            @Nonnull BiFunction<FDBRecordStore, RecordCursorResult<T>, CompletableFuture<FDBStoredRecord<Message>>> getRecordToIndex,
                                                            @Nonnull AtomicReference<RecordCursorResult<T>> nextResultCont,
                                                            @Nonnull AtomicBoolean hasMore,
                                                            @Nullable AtomicLong recordsScanned,
                                                            final boolean isIdempotent) {
        final FDBStoreTimer timer = getRunner().getTimer();
        final FDBRecordContext context = store.getContext();

        // Need to do this each transaction because other index enabled state might have changed. Could cache based on that.
        // Copying the state also guards against changes made by other online building from check version.
        AtomicLong recordsScannedCounter = new AtomicLong();

        final AtomicReference<RecordCursorResult<T>> nextResult = new AtomicReference<>(null);
        return AsyncUtil.whileTrue(() -> cursor.onNext().thenCompose(result -> {
            RecordCursorResult<T> currResult;
            final boolean isExhausted;
            if (result.hasNext()) {
                // has next, process one previous item (if exists)
                currResult = nextResult.get();
                nextResult.set(result);
                if (currResult == null) {
                    // that was the first item, nothing to process
                    return AsyncUtil.READY_TRUE;
                }
                isExhausted = false;
            } else {
                // end of the cursor list
                timerIncrement(timer, FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_COUNT);
                if (!result.getNoNextReason().isSourceExhausted()) {
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
            timerIncrement(timer, FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
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
                        timerIncrement(timer, FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED);

                        final CompletableFuture<Void> updateMaintainer = updateMaintainerBuilder(store, rec);
                        if (isExhausted) {
                            // we've just processed the last item
                            hasMore.set(false);
                            return updateMaintainer.thenApply(vignore -> false);
                        }
                        return updateMaintainer.thenCompose(vignore ->
                                context.getApproximateTransactionSize().thenApply(size -> {
                                    if (size >= common.config.getMaxWriteLimitBytes()) {
                                        // the transaction becomes too big - stop iterating
                                        timerIncrement(timer, FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RANGES_BY_SIZE);
                                        nextResultCont.set(nextResult.get());
                                        hasMore.set(true);
                                        return false;
                                    }
                                    return true;
                                }));
                    });
        }), cursor.getExecutor())
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

    private CompletableFuture<Void> updateMaintainerBuilder(@Nonnull FDBRecordStore store,
                                                            FDBStoredRecord<Message> rec) {
        return forEachTargetIndexContext(indexContext -> {
            if (!indexContext.recordTypes.contains(rec.getRecordType())) {
                // This particular index is not affected by rec
                return AsyncUtil.DONE;
            }
            if (indexContext.isSynthetic) {
                // This particular index is synthetic, handle with care
                final SyntheticRecordPlanner syntheticPlanner = new SyntheticRecordPlanner(store.getRecordMetaData(), store.getRecordStoreState().withWriteOnlyIndexes(Collections.singletonList(indexContext.index.getName())));
                final SyntheticRecordFromStoredRecordPlan syntheticPlan = syntheticPlanner.forIndex(indexContext.index);
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

        return AsyncUtil.whileTrue(() ->
                    buildCommitRetryAsync(iterateRange,
                            true,
                            additionalLogMessageKeyValues)
                            .handle((hasMore, ex) -> {
                                if (ex == null) {
                                    if (Boolean.FALSE.equals(hasMore)) {
                                        // all done
                                        return AsyncUtil.READY_FALSE;
                                    }
                                    return throttleDelayAndMaybeLogProgress(subspaceProvider, Collections.emptyList());
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

    // rebuildIndexAsync - builds the whole index inline (without committing)
    @Nonnull
    public CompletableFuture<Void> rebuildIndexAsync(@Nonnull FDBRecordStore store) {
        return forEachTargetIndex(index -> store.clearAndMarkIndexWriteOnly(index).thenCompose(bignore -> {
            // Insert the full range into the range set. (The internal rebuild method only indexes the records and
            // does not update the range set.) This is important because if marking the index as readable fails (for
            // example, because of uniqueness violations), we still want to record in the range set that the entire
            // range was built so that future index builds don't re-scan the record data and so that non-idempotent
            // indexes know to update the index on all record saves.
            Transaction tr = store.ensureContextActive();
            RangeSet rangeSet = new RangeSet(store.indexRangeSubspace(index));
            return rangeSet.insertRange(tr, null, null);
        })).thenCompose(vignore -> rebuildIndexInternalAsync(store));
    }

    abstract CompletableFuture<Void> rebuildIndexInternalAsync(FDBRecordStore store);

    // These throttle methods are externalized for testing usage only
    @Nonnull
    <R> CompletableFuture<R> throttledRunAsync(@Nonnull final Function<FDBRecordStore, CompletableFuture<R>> function,
                                               @Nonnull final BiFunction<R, Throwable, Pair<R, Throwable>> handlePostTransaction,
                                               @Nullable final BiConsumer<FDBException, List<Object>> handleLessenWork,
                                               @Nullable final List<Object> additionalLogMessageKeyValues) {
        return throttle.throttledRunAsync(function, handlePostTransaction, handleLessenWork, additionalLogMessageKeyValues);
    }

    void decreaseLimit(@Nonnull FDBException fdbException,
                       @Nullable List<Object> additionalLogMessageKeyValues) {
        throttle.decreaseLimit(fdbException, additionalLogMessageKeyValues);
    }

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
     * thrown when IndexFromIndex validation fails.
     */
    @SuppressWarnings("serial")
    public static class  PartlyBuiltException extends RecordCoreException {
        final IndexBuildProto.IndexBuildIndexingStamp savedStamp;

        public PartlyBuiltException(@Nonnull IndexBuildProto.IndexBuildIndexingStamp savedStamp, @Nonnull String msg, @Nullable Object ... keyValues) {
            super(msg, keyValues);
            this.savedStamp = savedStamp;
        }
    }

    public static PartlyBuiltException getAPartlyBuildExceptionIfApplicable(@Nullable Throwable ex) {
        for (Throwable current = ex;
                current != null;
                current = current.getCause()) {
            if (current instanceof PartlyBuiltException) {
                return (PartlyBuiltException) current;
            }
        }
        return null;
    }
}



