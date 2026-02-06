/*
 * OnlineIndexer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingHeartbeat;
import com.apple.foundationdb.synchronizedsession.SynchronizedSession;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Index.decodeSubspaceKey;

/**
 * Builds an index online, i.e., concurrently with other database operations. In order to minimize
 * the impact that these operations have with other operations, this attempts to minimize the
 * priorities of its transactions. Additionally, it attempts to limit the amount of work it will
 * done in a fashion that will decrease as the number of failures for a given build attempt increases.
 *
 * <p>
 * As ranges of elements are rebuilt, the fact that the range has rebuilt is added to a {@link RangeSet}
 * associated with the index being built. This {@link RangeSet} is used to (a) coordinate work between
 * different builders that might be running on different machines to ensure that the same work isn't
 * duplicated and to (b) make sure that non-idempotent indexes (like <code>COUNT</code> or <code>SUM_LONG</code>)
 * don't update themselves (or fail to update themselves) incorrectly.
 * </p>
 *
 * <p>
 * Unlike many other features in the Record Layer core, this has a retry loop.
 * </p>
 *
 * <p>Build an index immediately in the current transaction:</p>
 * <pre><code>
 * try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
 *     indexBuilder.rebuildIndex(recordStore);
 * }
 * </code></pre>
 *
 * <p>Build an index synchronously in the multiple transactions:</p>
 * <pre><code>
 * try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
 *     indexBuilder.buildIndex();
 * }
 * </code></pre>
 */
@API(API.Status.UNSTABLE)
public class OnlineIndexer implements AutoCloseable {
    /**
     * Constant indicating that there should be no limit to some usually limited operation.
     */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    public static final int INDEXING_ATTEMPTS_RECURSION_LIMIT = 5; // Safety net - our algorithm should never reach this depth

    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexer.class);

    @Nonnull private final IndexingCommon common;
    @Nullable private IndexingBase indexer = null;

    @Nonnull private final FDBDatabaseRunner runner;
    @Nonnull private final Index index; // First target index is used for locks
    @Nonnull private IndexingPolicy indexingPolicy;
    private boolean fallbackToRecordsScan = false;

    @SuppressWarnings("squid:S00107")
    OnlineIndexer(@Nonnull FDBDatabaseRunner runner,
                  @Nonnull FDBRecordStore.Builder recordStoreBuilder,
                  @Nonnull List<Index> targetIndexes,
                  @Nullable Collection<RecordType> recordTypes,
                  @Nullable UnaryOperator<OnlineIndexOperationConfig> configLoader,
                  @Nonnull OnlineIndexOperationConfig config,
                  boolean trackProgress,
                  @Nonnull IndexingPolicy indexingPolicy) {
        this.runner = runner;
        this.index = targetIndexes.get(0);
        this.indexingPolicy = indexingPolicy;

        this.common = new IndexingCommon(runner, recordStoreBuilder,
                targetIndexes, recordTypes, configLoader, config,
                trackProgress);
    }

    @Nonnull
    private CompletableFuture<Void> indexingLauncher(Supplier<CompletableFuture<Void>> indexingFunc) {
        return indexingLauncher(indexingFunc, 0);
    }

    @Nonnull
    private CompletableFuture<Void> indexingLauncher(Supplier<CompletableFuture<Void>> indexingFunc, int attemptCount) {
        return indexingLauncher(indexingFunc, attemptCount, null);
    }

    @Nonnull
    private CompletableFuture<Void> indexingLauncher(Supplier<CompletableFuture<Void>> indexingFunc, int attemptCount, @Nullable IndexingPolicy requestedPolicy) {
        // The launcher calls the indexing function, letting the results to be handled by the catcher.
        // The catcher may, on some cases, call the launcher in its retry path. The attemptCount limits the recursion level as a safety net.
        return AsyncUtil.composeHandle( indexingFunc.get(),
                (ignore, ex) -> indexingCatcher(ex, indexingFunc, attemptCount + 1, requestedPolicy));
    }

    @Nonnull
    private CompletableFuture<Void> indexingCatcher(Throwable ex, Supplier<CompletableFuture<Void>> indexingFunc, int attemptCount, @Nullable IndexingPolicy requestedPolicy) {
        // (skeleton function, a little long but broken to distinct cases)
        if (ex == null) {
            // A happy index it is
            return AsyncUtil.DONE;
        }

        if (attemptCount > INDEXING_ATTEMPTS_RECURSION_LIMIT) {
            // Safety net, this should never happen
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error(KeyValueLogMessage.build("Too many indexing attempts",
                        LogMessageKeys.CURR_ATTEMPT, attemptCount)
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .toString());
            }
            throw FDBExceptions.wrapException(ex);
        }

        // clear the indexer, forcing indexingLauncher - if called again - to build a new indexer according to
        // the modified parameters.
        indexer = null;

        final IndexingBase.PartlyBuiltException partlyBuiltException = IndexingBase.getAPartlyBuiltExceptionIfApplicable(ex);
        if (partlyBuiltException != null) {
            // An ongoing indexing process with a different method type was found. Some precondition cases should be handled.
            IndexBuildProto.IndexBuildIndexingStamp conflictingIndexingTypeStamp = partlyBuiltException.getSavedStamp();
            IndexingPolicy.DesiredAction desiredAction = indexingPolicy.getIfMismatchPrevious();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(KeyValueLogMessage.build("conflicting indexing type stamp",
                                LogMessageKeys.CURR_ATTEMPT, attemptCount,
                                LogMessageKeys.INDEXING_POLICY_DESIRED_ACTION, desiredAction,
                                LogMessageKeys.ACTUAL, partlyBuiltException.getSavedStampString(),
                                LogMessageKeys.EXPECTED, partlyBuiltException.getExpectedStampString())
                        .addKeysAndValues(common.indexLogMessageKeyValues())
                        .toString());
            }

            if (desiredAction == IndexingPolicy.DesiredAction.CONTINUE) {
                // Make an effort to finish indexing. Attempt continuation of the previous method
                // Here: match the policy to the previous run
                IndexBuildProto.IndexBuildIndexingStamp.Method method = conflictingIndexingTypeStamp.getMethod();
                if (method == IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS && !common.isMultiTarget()) {
                    // Partly built by records. The fallback indicator should handle the policy
                    fallbackToRecordsScan = true;
                    return indexingLauncher(indexingFunc, attemptCount);
                }
                if (method == IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS && !common.isMultiTarget()) {
                    // Partly built by records, in multi target mode. We only allow a fallback from multi target
                    // to a single target, but not to a subset.
                    fallbackToRecordsScan = true;
                    return indexingLauncher(indexingFunc, attemptCount);
                }
                if (method == IndexBuildProto.IndexBuildIndexingStamp.Method.BY_INDEX && !common.isMultiTarget()) {
                    // Partly built by index. Retry with the old policy, but preserve the requested policy - in case the old one fails.
                    Object sourceIndexSubspaceKey = decodeSubspaceKey(conflictingIndexingTypeStamp.getSourceIndexSubspaceKey());
                    IndexingPolicy origPolicy = indexingPolicy;
                    indexingPolicy = origPolicy.toBuilder()
                            .setSourceIndexSubspaceKey(sourceIndexSubspaceKey)
                            .build();
                    return indexingLauncher(indexingFunc, attemptCount, origPolicy);
                }
                // No other methods (yet). This line should never be reached.
                throw partlyBuiltException;
            }

            if (desiredAction == IndexingPolicy.DesiredAction.REBUILD) {
                // Here: Just rebuild
                indexingPolicy = indexingPolicy.toBuilder()
                        .setIfWriteOnly(IndexingPolicy.DesiredAction.REBUILD)
                        .build();
                return indexingLauncher(indexingFunc, attemptCount);
            }

            // Error it is
            throw partlyBuiltException;
        }

        if (indexingPolicy.isByIndex() && IndexingByIndex.isValidationException(ex)) {
            // Validation failed - the source index cannot be used for records scanning
            if (requestedPolicy != null) {
                // We tried to continue a previous indexing session, but failed. The best recovery, as it seems,
                // is to rebuild with requested policy.
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.build("The previous method's source index isn't usable. Rebuild by the requested policy",
                            LogMessageKeys.CURR_ATTEMPT, attemptCount)
                            .addKeysAndValues(common.indexLogMessageKeyValues())
                            .toString());
                }
                // rebuild by the requested policy
                indexingPolicy = requestedPolicy.toBuilder()
                        .setIfWriteOnly(IndexingPolicy.DesiredAction.REBUILD)
                        .build();
                return indexingLauncher(indexingFunc, attemptCount);
            }

            if (! indexingPolicy.isForbidRecordScan() && ! fallbackToRecordsScan) {
                // requested by-index failed, and record scan is allowed. Build by records.
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(KeyValueLogMessage.build("Fallback to a by-record scan",
                            LogMessageKeys.CURR_ATTEMPT, attemptCount)
                            .addKeysAndValues(common.indexLogMessageKeyValues())
                            .toString());
                }
                // build by records
                fallbackToRecordsScan = true;
                return indexingLauncher(indexingFunc, attemptCount);
            }
        }

        if (indexingPolicy.isMutual()) {
            IndexingBase.UnexpectedReadableException unexpectedReadableException = IndexingBase.getUnexpectedReadableIfApplicable(ex);
            if (unexpectedReadableException != null) {
                if (unexpectedReadableException.allReadable) {
                    // All are readable
                    return AsyncUtil.DONE;
                }
                // Some are readable, probably by another process. Call regular indexing to check/mark readable
                fallbackToRecordsScan = true;
                return indexingLauncher(indexingFunc, attemptCount);
            }
        }

        // No handling, throw the error.
        throw FDBExceptions.wrapException(ex);
    }

    @Nonnull
    private IndexingByIndex getIndexerByIndex() {
        if (! (indexer instanceof IndexingByIndex)) { // this covers null pointer
            indexer = new IndexingByIndex(common, indexingPolicy);
        }
        return (IndexingByIndex)indexer;
    }

    @Nonnull
    private IndexingMultiTargetByRecords getIndexerMultiTargetByRecords() {
        if (! (indexer instanceof IndexingMultiTargetByRecords)) {
            indexer = new IndexingMultiTargetByRecords(common, indexingPolicy);
        }
        return (IndexingMultiTargetByRecords)indexer;
    }

    @Nonnull
    private IndexingMutuallyByRecords getMutualIndexerByRecords() {
        if (! (indexer instanceof IndexingMutuallyByRecords)) {
            indexer = new IndexingMutuallyByRecords(common, indexingPolicy, indexingPolicy.mutualIndexingBoundaries);
        }
        return (IndexingMutuallyByRecords)indexer;
    }

    @Nonnull
    private IndexingBase getIndexer() {
        if (indexingPolicy.isMutual() && !fallbackToRecordsScan) {
            return getMutualIndexerByRecords();
        }
        if (indexingPolicy.isByIndex() && !common.isMultiTarget() && !fallbackToRecordsScan) {
            return getIndexerByIndex();
        }
        // default
        IndexingBase indexingBase = getIndexerMultiTargetByRecords();
        if (fallbackToRecordsScan) {
            indexingBase.enforceStampOverwrite();
        }
        return indexingBase;
    }

    /**
     * Get the current config parameters of the online indexer.
     * @return the config parameters of the online indexer
     */
    @Nonnull
    @VisibleForTesting
    OnlineIndexOperationConfig getConfig() {
        return common.config;
    }

    /**
     * Get the number of times the configuration was loaded.
     * @return the number of times the {@code configLoader} was invoked
     */
    @VisibleForTesting
    int getConfigLoaderInvocationCount() {
        return common.getConfigLoaderInvocationCount();
    }

    /**
     * Get the current number of records to process in one transaction.
     * This may go up or down during committing failures or successes.
     * @return the current number of records to process in one transaction
     */
    public int getLimit() {
        return getIndexer().getLimit();
    }

    @SuppressWarnings("squid:S1452")
    private CompletableFuture<FDBRecordStore> openRecordStore(@Nonnull FDBRecordContext context) {
        return common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync();
    }

    @Override
    public void close() {
        common.close();
    }

    @VisibleForTesting
    <R> CompletableFuture<R> buildCommitRetryAsync(@Nonnull BiFunction<FDBRecordStore, AtomicLong, CompletableFuture<R>> buildFunction,
                                                   @Nullable List<Object> additionalLogMessageKeyValues) {
        // test only - emulates duringRangesIteration=true
        return getIndexer().buildCommitRetryAsync(buildFunction, additionalLogMessageKeyValues, true);
    }

    @VisibleForTesting
    public CompletableFuture<Void> eraseIndexingTypeStampTestOnly() {
        // test only(!)
        return getRunner().runAsync(context -> openRecordStore(context).thenCompose(store -> {
            Transaction transaction = store.getContext().ensureActive();
            for (Index targetIndex: common.getTargetIndexes()) {
                byte[] stampKey = IndexingSubspaces.indexBuildTypeSubspace(store, targetIndex).getKey();
                transaction.clear(stampKey);
            }
            return AsyncUtil.DONE;
        }));
    }

    /**
     * Transactionally rebuild an entire index. This will (1) delete any data in the index that is
     * already there and (2) rebuild the entire key range for the given index. It will attempt to
     * do this within a single transaction, and it may fail if there are too many records, so this
     * is only safe to do for small record stores.
     *
     * Many large use-cases should use the {@link #buildIndexAsync} method along with temporarily
     * changing an index to write-only mode while the index is being rebuilt.
     *
     * @param store the record store in which to rebuild the index
     * @return a future that will be ready when the build has completed
     */
    @Nonnull
    public CompletableFuture<Void> rebuildIndexAsync(@Nonnull FDBRecordStore store) {
        return indexingLauncher(() -> getIndexer().rebuildIndexAsync(store));
    }

    /**
     * Transactionally rebuild an entire index.
     * Synchronous version of {@link #rebuildIndexAsync}
     *
     * @param store the record store in which to rebuild the index
     * @see #buildIndex
     */
    public void rebuildIndex(@Nonnull FDBRecordStore store) {
        asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, rebuildIndexAsync(store));
    }

    /**
     * If applicable, merge the target indexes as a background maintenance. This experimental feature completes
     * {@link FDBRecordStore#getIndexDeferredMaintenanceControl()} and {@link IndexDeferredMaintenanceControl}.
     * @return a future with the merge index operation
     */
    @API(API.Status.EXPERIMENTAL)
    public CompletableFuture<Void> mergeIndexAsync() {
        return indexingLauncher(() -> getIndexer().mergeIndexes());
    }

    /**
     * If applicable, merge the target indexes as a background maintenance. This experimental feature completes
     * {@link FDBRecordStore#getIndexDeferredMaintenanceControl()} and {@link IndexDeferredMaintenanceControl}.
     */
    @API(API.Status.EXPERIMENTAL)
    public void mergeIndex() {
        asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_MERGE_INDEX, mergeIndexAsync());
    }

    /**
     * <em>Deprecated</em>. The functionality of this function can be done with {@link #blockIndexBuilds}
     * Stop any ongoing online index build (only if it uses {@link SynchronizedSession}s) by forcefully releasing
     * the lock.
     * @return a future that will be ready when the lock is released
     * @see SynchronizedSession#endAnySession
     */
    @API(API.Status.DEPRECATED)
    public CompletableFuture<Void> stopOngoingOnlineIndexBuildsAsync() {
        return runner.runAsync(context -> openRecordStore(context).thenAccept(recordStore ->
                stopOngoingOnlineIndexBuilds(recordStore, index)),
                common.indexLogMessageKeyValues("OnlineIndexer::stopOngoingOnlineIndexBuilds"));
    }

    /**
     * <em>Deprecated</em>. The functionality of this function can be done with {@link #blockIndexBuilds}
     * Synchronous/blocking version of {@link #stopOngoingOnlineIndexBuildsAsync()}.
     */
    @API(API.Status.DEPRECATED)
    public void stopOngoingOnlineIndexBuilds() {
        runner.asyncToSync(FDBStoreTimer.Waits.WAIT_STOP_ONLINE_INDEX_BUILD, stopOngoingOnlineIndexBuildsAsync());
    }

    /**
     * <em>Deprecated</em>. The functionality of this function can be done with {@link #blockIndexBuilds}
     * Stop any ongoing online index build (only if it uses {@link SynchronizedSession}s) by forcefully releasing
     * the lock.
     * @param recordStore record store whose index builds need to be stopped
     * @param index the index whose builds need to be stopped
     */
    @API(API.Status.DEPRECATED)
    public static void stopOngoingOnlineIndexBuilds(@Nonnull FDBRecordStore recordStore, @Nonnull Index index) {
        SynchronizedSession.endAnySession(recordStore.ensureContextActive(), IndexingSubspaces.indexBuildLockSubspace(recordStore, index));
    }

    /**
     * Synchronous/blocking version of {@link #checkAnyOngoingOnlineIndexBuildsAsync()}.
     * @return <code>true</code> if the index is being built and <code>false</code> otherwise
     */
    public boolean checkAnyOngoingOnlineIndexBuilds() {
        return runner.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_ONGOING_ONLINE_INDEX_BUILD, checkAnyOngoingOnlineIndexBuildsAsync());
    }

    /**
     * Check if the provided index is being built. Note that with shared heartbeats, this function will now return true for any active session - mutual or exclusive.
     * @return a future that will complete to <code>true</code> if the index is being built and <code>false</code> otherwise
     */
    public CompletableFuture<Boolean> checkAnyOngoingOnlineIndexBuildsAsync() {
        return runner.runAsync(context -> openRecordStore(context).thenCompose(recordStore ->
                checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, common.config.getLeaseLengthMillis())),
                common.indexLogMessageKeyValues("OnlineIndexer::checkAnyOngoingOnlineIndexBuilds"));
    }

    /**
     * Check if the provided index is being built. Note that with shared heartbeats, this function will now return true for any active session - mutual or exclusive.
     * Where "active session" is determined by an indexing heartbeat that is less than {@link OnlineIndexOperationConfig#DEFAULT_LEASE_LENGTH_MILLIS} old.
     * @param recordStore record store whose index builds need to be checked
     * @param index the index to check for ongoing index builds
     * @return a future that will complete to <code>true</code> if the index is being built and <code>false</code> otherwise
     */
    public static CompletableFuture<Boolean> checkAnyOngoingOnlineIndexBuildsAsync(@Nonnull FDBRecordStore recordStore, @Nonnull Index index) {
        return checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, OnlineIndexOperationConfig.DEFAULT_LEASE_LENGTH_MILLIS);
    }

    /**
     * Check if the provided index is being built. Note that with shared heartbeats, this function will now return true for any active session - mutual or exclusive.
     * @param recordStore record store whose index builds need to be checked
     * @param index the index to check for ongoing index builds
     * @param leasingMilliseconds max heartbeat age to be considered an "active session"
     * @return a future that will complete to <code>true</code> if the index is being built and <code>false</code> otherwise
     */
    public static CompletableFuture<Boolean> checkAnyOngoingOnlineIndexBuildsAsync(@Nonnull FDBRecordStore recordStore, @Nonnull Index index, long leasingMilliseconds) {
        return IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0)
                .thenApply(list -> {
                    long activeTime = System.currentTimeMillis() + leasingMilliseconds;
                    return list.values().stream().anyMatch(item -> item.getHeartbeatTimeMilliseconds() < activeTime);
                });
    }

    /**
     * Builds an index across multiple transactions.
     * <p>
     * If the indexing session is not mutual, it will stop with {@link com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException}
     * if there is another active indexing session on the same index. It first checks and updates index states and
     * clear index data respecting the {@link IndexStatePrecondition} being set. It then builds the index across
     * multiple transactions honoring the rate-limiting parameters set in the constructor of this class. It also retries
     * any retriable errors that it encounters while it runs the build. At the end, it marks the index readable in the
     * store.
     * </p>
     * @return a future that will be ready when the build has completed
     * @throws com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException the build is stopped
     * because there may be another build running actively on this index.
     */
    @Nonnull
    public CompletableFuture<Void> buildIndexAsync() {
        return buildIndexAsync(true);
    }

    @VisibleForTesting
    @Nonnull
    CompletableFuture<Void> buildIndexAsync(boolean markReadable) {
        return indexingLauncher(() -> getIndexer().buildIndexAsync(markReadable));
    }

    /**
     * Builds an index across multiple transactions.
     * Synchronous version of {@link #buildIndexAsync}.
     * @param markReadable whether to mark the index as readable after building the index
     */
    public void buildIndex(boolean markReadable) {
        asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, buildIndexAsync(markReadable));
    }

    /**
     * Builds an index across multiple transactions.
     * Synchronous version of {@link #buildIndexAsync}.
     */
    public void buildIndex() {
        asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, buildIndexAsync());
    }

    /**
     * Mark the index as readable if it is built.
     * @return a future that will complete to <code>true</code> if all the target indexes are readable or marked readable
     * and <code>false</code> otherwise
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public CompletableFuture<Boolean> markReadableIfBuilt() {
        return getIndexer().markReadableIfBuilt();
    }

    /**
     * Mark the index as readable.
     * @return a future that will either complete exceptionally if any of the target indexes can not
     * be made readable or will contain <code>true</code> if the store was modified and <code>false</code>
     * otherwise
     */
    @API(API.Status.EXPERIMENTAL)
    @Nonnull
    public CompletableFuture<Boolean> markReadable() {
        return getIndexer().markIndexReadable(true);
    }

    /**
     * Query the current indexing session.
     * Note that this query cannot tell if another process is actively indexing now, but whether or not an index is in
     * the process of being built (and how).
     * @return a map of target indexes and their "indexing stamps".
     */
    @API(API.Status.EXPERIMENTAL)
    public Map<String, IndexBuildProto.IndexBuildIndexingStamp> queryIndexingStamps() {
        return indexingStamp(IndexingBase.IndexingStampOperation.QUERY, null, null);
    }

    /**
     * Block partly built indexes, preventing continuation.
     * Active indexing sessions will check for this block during every iterating transaction, which still requires the caller to
     * wait a few seconds before assuming that all indexing had stopped.
     * @param id if non-null, will be added to the "indexing stamp" as an id/hint for the blocking reason.
     * @param ttlSeconds if non-null, the block will automatically expire after this value (in seconds).
     * @return a map of target indexes and their "indexing stamps" after the change.
     */
    @API(API.Status.EXPERIMENTAL)
    public Map<String, IndexBuildProto.IndexBuildIndexingStamp> blockIndexBuilds(@Nullable String id, @Nullable Long ttlSeconds)  {
        return indexingStamp(IndexingBase.IndexingStampOperation.BLOCK, id, ttlSeconds);
    }

    /**
     * Unblock partly built indexes, allowing continuation.
     * @param id if non-null nor empty, unblock the indexes only if this matches the id in the "indexing stamp", as was
     * set by {@link #blockIndexBuilds(String, Long)}.
     * @return  a map of target indexes and their "indexing stamps" after the change.
     */
    @API(API.Status.EXPERIMENTAL)
    public Map<String, IndexBuildProto.IndexBuildIndexingStamp> unblockIndexBuilds(@Nullable String id) {
        return indexingStamp(IndexingBase.IndexingStampOperation.UNBLOCK, id, null);
    }

    private Map<String, IndexBuildProto.IndexBuildIndexingStamp> indexingStamp(@Nullable IndexingBase.IndexingStampOperation op, @Nullable String id, @Nullable Long ttlSeconds) {
        // any indexer will do
        return asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_TYPESTAMP_OPERATION,
                getIndexer().performIndexingStampOperation(op, id, ttlSeconds));
    }

    /**
     * Get the current indexing heartbeats map for a given index (single target or primary index).
     * Each indexing session, while active, updates a heartbeat at every transaction during the indexing's iteration. These
     * heartbeats can be used to query active indexing sessions.
     * Indexing sessions will attempt to clear their own heartbeat before returning (successfully or exceptionally). However,
     * the heartbeat clearing may fail in the DB access level.
     * When an index becomes readable any existing heartbeat will be deleted.
     * Note that heartbeats that cannot be decrypted will show in the returned map as having creation time of 0 and its info will
     * be set to {@link IndexingHeartbeat#INVALID_HEARTBEAT_INFO}.
     * @param maxCount safety valve to limit number items to read. Typically set to zero to keep unlimited.
     * @return map of session ids to {@link IndexBuildProto.IndexBuildHeartbeat}
     */
    @API(API.Status.EXPERIMENTAL)
    public Map<UUID, IndexBuildProto.IndexBuildHeartbeat> getIndexingHeartbeats(int maxCount) {
        return asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_READ_HEARTBEATS,
                getIndexer().getIndexingHeartbeats(maxCount));
    }

    /**
     * Clear old indexing heartbeats for a given index (single target or primary index).
     * Typically, heartbeats are deleted either at the end of an indexing sessions or when the index becomes readable. This
     * cleanup function can be used if, for any reason, the heartbeats could not be deleted from the database at the end of a session.
     * @param minAgeMilliseconds minimum heartbeat age (time elapsed since the last heartbeat, in milliseconds) to clear.
     * @param maxIteration safety valve to limit number of items to check. Typically set to zero to keep unlimited
     * @return number of cleared heartbeats
     */
    @API(API.Status.EXPERIMENTAL)
    public int clearIndexingHeartbeats(long minAgeMilliseconds, int maxIteration) {
        return asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_CLEAR_HEARTBEATS,
                getIndexer().clearIndexingHeartbeats(minAgeMilliseconds, maxIteration));
    }

    /**
     * Wait for an asynchronous task to complete. This returns the result from the future or propagates
     * the error if the future completes exceptionally.
     *
     * @param event the event being waited on (for instrumentation purposes)
     * @param async the asynchronous task to wait on
     * @param <T> the task's return type
     * @return the result of the asynchronous task
     *
     */
    @API(API.Status.INTERNAL)
    public <T> T asyncToSync(@Nonnull StoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        return getRunner().asyncToSync(event, async);
    }

    @API(API.Status.INTERNAL)
    @VisibleForTesting
    // Public could use IndexBuildState.getTotalRecordsScanned instead.
    long getTotalRecordsScanned() {
        return common.getTotalRecordsScanned().get();
    }

    private FDBDatabaseRunner getRunner() {
        return runner;
    }

    /**
     * Builder for {@link OnlineIndexer}.
     *
     * <pre><code>
     * OnlineIndexer.newBuilder().setRecordStoreBuilder(recordStoreBuilder).setIndex(index).build()
     * </code></pre>
     *
     * <pre><code>
     * OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setSubspace(subspace).setIndex(index).build()
     * </code></pre>
     *
     */
    @API(API.Status.UNSTABLE)
    public static class Builder extends OnlineIndexOperationBaseBuilder<Builder> {
        @Nonnull
        private List<Index> targetIndexes = new ArrayList<>();
        @Nullable
        private Collection<RecordType> recordTypes;

        private IndexingPolicy indexingPolicy = null;
        private IndexingPolicy.Builder indexingPolicyBuilder = null;
        private IndexStatePrecondition indexStatePrecondition = null;

        protected Builder() {
        }

        @Override
        Builder self() {
            return this;
        }

        /**
         * Set the index to be built.
         * @param index the index to be built
         * @return this builder
         *
         */
        @Nonnull
        public Builder setIndex(@Nullable Index index) {
            if (!this.targetIndexes.isEmpty()) {
                throw new IndexingBase.ValidationException("setIndex may not be used when other target indexes are already set");
            }
            if (index != null) {
                addTargetIndex(index);
            }
            return this;
        }

        /**
         * Set the index to be built.
         * @param indexName the index to be built
         * @return this builder
         *
         */
        @Nonnull
        public Builder setIndex(@Nonnull String indexName) {
            if (!this.targetIndexes.isEmpty()) {
                throw new IndexingBase.ValidationException("setIndex may not be used when other target indexes are already set");
            }
            return addTargetIndex(indexName);
        }

        /**
         * Replace any previous target indexes list with this new one.
         * @param indexes list of target indexes
         * @return this builder
         */
        public Builder setTargetIndexes(@Nonnull List<Index> indexes) {
            this.targetIndexes = new ArrayList<>(indexes);
            return this;
        }

        /**
         * Replace any previous target indexes list with this new one.
         * @param indexes list of target index names
         * @return this builder
         */
        public Builder setTargetIndexesByName(@Nonnull List<String> indexes) {
            final RecordMetaData metaData = getRecordMetaData();
            return setTargetIndexes(indexes.stream().map(metaData::getIndex).collect(Collectors.toList()));
        }

        /**
         * Add one target index to the target indexes list. The online indexer will attempt building
         * all the target indexes within a single records scan. Hence, at least one target index should
         * be defined.
         * @param index an index to add
         * @return this builder
         */
        public Builder addTargetIndex(@Nonnull Index index) {
            this.targetIndexes.add(index);
            return this;
        }

        /**
         * Add one target index to the target indexes list. The online indexer will attempt building
         * all the target indexes within a single records scan. Hence, at least one target index should
         * be defined.
         * @param indexName an index's name to add
         * @return this builder
         */
        public Builder addTargetIndex(@Nonnull String indexName) {
            final RecordMetaData metaData = getRecordMetaData();
            return addTargetIndex(metaData.getIndex(indexName));
        }

        /**
         * Get the explicit set of record types to be indexed.
         *
         * Normally, all record types associated with the chosen index will be indexed.
         * @return the record types to be indexed
         */
        @Nullable
        public Collection<RecordType> getRecordTypes() {
            return recordTypes;
        }

        /**
         * Set the explicit set of record types to be indexed.
         *
         * Normally, record types are inferred from {@link #addTargetIndex(Index)}. Setting the types
         * explicitly is not allowed with multi targets indexing.
         * @param recordTypes the record types to be indexed or {@code null} to infer from the index
         * @return this builder
         */
        @Nonnull
        public Builder setRecordTypes(@Nullable Collection<RecordType> recordTypes) {
            this.recordTypes = recordTypes;
            return this;
        }

        /**
         * Set how should {@link #buildIndexAsync()} (or its variations) build the index based on its state. Normally
         * this should be {@link IndexStatePrecondition#BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY} if the index is
         * not corrupted.
         * @see IndexStatePrecondition
         * @param indexStatePrecondition build option to use
         * @return this builder
         *
         * @deprecated use {@link IndexingPolicy.Builder} instead. Example:
         * <p>
         *   setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
         *                         .setIfDisabled(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
         *                         .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.CONTINUE)
         *                         .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
         *                         .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR));
         * </p>
         * For backward compatibility, calling this function in any order will override the {@link IndexingPolicy.Builder}
         * methods: setIfDisabled, setIfWriteOnly, setIfMismatchPrevious, setIfReadable
         */
        @Deprecated
        public Builder setIndexStatePrecondition(@Nonnull IndexStatePrecondition indexStatePrecondition) {
            this.indexStatePrecondition = indexStatePrecondition;
            return this;
        }

        /**
         * Add an {@link IndexingPolicy} policy. If set, this policy will determine how the index should be
         * built.
         * For backward compatibility, the use of deprecated {@link #indexStatePrecondition} may override some policy values.
         * @param indexingPolicy see {@link IndexingPolicy}
         * @return this Builder
         */
        public Builder setIndexingPolicy(@Nullable final IndexingPolicy indexingPolicy) {
            this.indexingPolicyBuilder = null;
            if (indexingPolicy == null) {
                this.indexingPolicy = IndexingPolicy.DEFAULT;
            } else {
                this.indexingPolicy = indexingPolicy;
            }
            return this;
        }

        /**
         * Add an {@link IndexingPolicy.Builder}. If set, this policy will determine how the index should be
         * built.
         * For backward compatibility, the use of deprecated {@link #indexStatePrecondition} may override some policy values.
         * @param builder an IndexingPolicy builder.
         * @return this Builder
         */
        public Builder setIndexingPolicy(@Nonnull final IndexingPolicy.Builder builder) {
            this.indexingPolicy = null;
            this.indexingPolicyBuilder = builder;
            return this;
        }

        /**
         * Build an {@link OnlineIndexer}.
         * @return a new online indexer
         */
        public OnlineIndexer build() {
            determineIndexingPolicy();
            validate();
            OnlineIndexOperationConfig conf = getConfig();
            return new OnlineIndexer(getRunner(), getRecordStoreBuilder(), targetIndexes, recordTypes,
                    getConfigLoader(), conf, isTrackProgress(), indexingPolicy);
        }

        private void determineIndexingPolicy() {
            // Here: Supporting dual backward compatibilities:
            // 1. Allow the use of indexStatePrecondition
            // 2. Allow the use of an explicit index policy (instead of the index policy builder)
            if (indexStatePrecondition != null) {
                if (indexingPolicy != null) {
                    indexingPolicyBuilder = indexingPolicy.toBuilder();
                }
                if (indexingPolicyBuilder == null) {
                    indexingPolicyBuilder = IndexingPolicy.newBuilder();
                }
                indexingPolicyBuilder
                        .setIfDisabled(indexStatePrecondition.ifDisabled)
                        .setIfWriteOnly(indexStatePrecondition.ifWriteOnly)
                        .setIfMismatchPrevious(indexStatePrecondition.ifMismatchPrevious)
                        .setIfReadable(indexStatePrecondition.ifReadable);
            }
            if (indexingPolicyBuilder != null) {
                indexingPolicy = indexingPolicyBuilder.build();
            }
            if (indexingPolicy == null) {
                indexingPolicy = IndexingPolicy.DEFAULT;
            }
        }

        private void validate() {
            final RecordMetaData metaData = getRecordMetaData();
            validateIndexSetting(metaData);
            validateTypes(metaData);
            validateLimits();
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        private void validateIndexSetting(RecordMetaData metaData) {
            if (this.targetIndexes.isEmpty()) {
                throw new MetaDataException("index must be set");
            }
            // Remove duplications (if any)
            if (targetIndexes.size() > 1) {
                if (indexingPolicy.isByIndex()) {
                    // TODO: support multi target indexing by index
                    throw new IndexingBase.ValidationException("Indexing multi targets by a source index is not supported (yet)");
                }
                Collection<Index> set = new HashSet<>(targetIndexes);
                if (set.size() < targetIndexes.size()) {
                    targetIndexes = new ArrayList<>(set);
                }
            }
            if (indexingPolicy.isMutual() && indexingPolicy.isByIndex()) {
                throw new IndexingBase.ValidationException("Indexing mutually by a source index is not supported (yet)");
            }
            targetIndexes.sort(Comparator.comparing(Index::getName));
            for (Index index : targetIndexes) {
                if (!metaData.hasIndex(index.getName()) || index != metaData.getIndex(index.getName())) {
                    throw new MetaDataException("Index " + index.getName() + " not contained within specified metadata");
                }
            }
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        private void validateTypes(RecordMetaData metaData) {
            if (recordTypes != null) {
                for (RecordType recordType : recordTypes) {
                    if (recordType != metaData.getIndexableRecordType(recordType.getName())) {
                        throw new MetaDataException("Record type " + recordType.getName() + " not contained within specified metadata");
                    }
                }
            }
        }

    }

    /**
     * A builder for the indexing policy.
     */
    public static class IndexingPolicy {
        public static final IndexingPolicy DEFAULT = IndexingPolicy.newBuilder().build();

        @Nullable private final String sourceIndex;
        @Nullable private final Object sourceIndexSubspaceKey; // overrides the sourceIndex
        private final boolean forbidRecordScan;
        private final DesiredAction ifDisabled;
        private final DesiredAction ifWriteOnly;
        private final DesiredAction ifMismatchPrevious;
        private final DesiredAction ifReadable;
        private final boolean allowUniquePendingState;
        private final Set<TakeoverTypes> allowedTakeoverSet;
        private final boolean mutualIndexing;
        private final List<Tuple> mutualIndexingBoundaries;
        private final boolean allowUnblock;
        private final String allowUnblockId;
        private final long initialMergesCountLimit;
        private final boolean reverseScanOrder;

        /**
         * Possible actions when an index is already partially built.
         */
        public enum DesiredAction {
            ERROR,
            REBUILD,
            CONTINUE,
            MARK_READABLE,
        }

        /**
         * Possible conversion from one indexing method to another.
         */
        public enum TakeoverTypes {
            /**
             * Allow a single target indexing continuation when the index was partly built with other indexes (as multi target).
             */
            MULTI_TARGET_TO_SINGLE,
            /**
             * Allow a single target indexing continuation when the index was partly built as mutual indexing (either single or multi target).
             */
            MUTUAL_TO_SINGLE,
            /**
             * Allow converting (single or multi target) by-records to mutual.
             * Note that single-target-mutual is a private case of multi-target-mutual. The non-mutual distinction is used for backward compatibility.
             */
            BY_RECORDS_TO_MUTUAL,
        }

        /**
         * Create a policy for the indexing session.
         * @param sourceIndex Build the index from a source index. Source index must be readable, idempotent, and fully cover the target index
         * @param sourceIndexSubspaceKey if non-null, overrides the sourceIndex param
         * @param forbidRecordScan forbid fallback to a by-records scan
         * @param ifDisabled desired action if the existing index state is DISABLED
         * @param ifWriteOnly desired action if the existing index state is WRITE_ONLY (i.e. partly built)
         * @param ifMismatchPrevious desired action if the index is partly built, but by a different method then currently requested
         * @param ifReadable desired action if the existing index state is READABLE (i.e. already built)
         * @param allowUniquePendingState if false, forbid {@link IndexState#READABLE_UNIQUE_PENDING} state
         * @param allowedTakeoverSet a subset of {@link TakeoverTypes}, with allowed indexing type conversion
         * @param mutualIndexing if true, use mutual indexing (i.e., index in a way that allows other processes to cooperatively build the index)
         * @param mutualIndexingBoundaries if present, use this predefined list of ranges. Else, split ranges by shards
         * @param allowUnblock if true, allow unblocking
         * @param allowUnblockId if preset, allow unblocking only if the block ID matches this param
         * @param initialMergesCountLimit the initial max merges count for index merger
         * @param reverseScanOrder if true, scan records in reverse order
         */
        @SuppressWarnings("squid:S00107") // too many parameters
        private IndexingPolicy(@Nullable String sourceIndex, @Nullable Object sourceIndexSubspaceKey, boolean forbidRecordScan,
                               DesiredAction ifDisabled, DesiredAction ifWriteOnly, DesiredAction ifMismatchPrevious, DesiredAction ifReadable,
                               boolean allowUniquePendingState, Set<TakeoverTypes> allowedTakeoverSet,
                               boolean mutualIndexing, List<Tuple> mutualIndexingBoundaries,
                               boolean allowUnblock, String allowUnblockId,
                               long initialMergesCountLimit,
                               boolean reverseScanOrder) {
            this.sourceIndex = sourceIndex;
            this.forbidRecordScan = forbidRecordScan;
            this.sourceIndexSubspaceKey = sourceIndexSubspaceKey;
            this.ifDisabled = ifDisabled;
            this.ifWriteOnly = ifWriteOnly;
            this.ifMismatchPrevious = ifMismatchPrevious;
            this.ifReadable = ifReadable;
            this.allowUniquePendingState = allowUniquePendingState;
            this.allowedTakeoverSet = allowedTakeoverSet;
            this.mutualIndexing = mutualIndexing;
            this.mutualIndexingBoundaries = mutualIndexingBoundaries;
            this.allowUnblock = allowUnblock;
            this.allowUnblockId = allowUnblockId;
            this.initialMergesCountLimit = initialMergesCountLimit;
            this.reverseScanOrder = reverseScanOrder;
        }

        /**
         * Check if active.
         * @return True if active
         */
        public boolean isByIndex() {
            return sourceIndex != null || sourceIndexSubspaceKey != null;
        }

        /**
         * If active, get the source index.
         * @return source index name
         */
        @Nullable
        public String getSourceIndex() {
            return sourceIndex;
        }

        @Nullable
        public Object getSourceIndexSubspaceKey() {
            return sourceIndexSubspaceKey;
        }

        /**
         * If another indexing method is not possible, indicate if a fallback to records scan is forbidden.
         * @return  {@code true} if a record scan is forbidden
         */
        public boolean isForbidRecordScan() {
            return forbidRecordScan;
        }

        /**
         * In this mode, assume concurrently indexing with other entities.
         * @return true if mutual
         */
        public boolean isMutual() {
            return mutualIndexing;
        }

        /**
         * Create an indexing policy builder.
         * @return a new {@link IndexingPolicy} builder
         */
        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Create an indexing policy builder from existing object.
         * @return an {@link IndexingPolicy} builder, defaulted to the existing object's values.
         */
        @Nonnull
        public Builder toBuilder() {
            return newBuilder()
                    .setSourceIndex(sourceIndex)
                    .setSourceIndexSubspaceKey(sourceIndexSubspaceKey)
                    .setForbidRecordScan(forbidRecordScan)
                    .setIfDisabled(ifDisabled)
                    .setIfWriteOnly(ifWriteOnly)
                    .setIfMismatchPrevious(ifMismatchPrevious)
                    .setIfReadable(ifReadable)
                    .allowUniquePendingState(allowUniquePendingState)
                    .allowTakeoverContinue(allowedTakeoverSet)
                    .setMutualIndexing(mutualIndexing)
                    .setMutualIndexingBoundaries(mutualIndexingBoundaries)
                    .setAllowUnblock(allowUnblock, allowUnblockId)
                    .setInitialMergesCountLimit(initialMergesCountLimit)
                    .setReverseScanOrder(reverseScanOrder)
                    ;
        }

        /**
         * The desired action if the index is disabled.
         * @return requested {@link DesiredAction}
         */
        public DesiredAction getIfDisabled() {
            return ifDisabled;
        }

        /**
         * The desired action if the index is in write only mode.
         * @return requested {@link DesiredAction}
         */
        public DesiredAction getIfWriteOnly() {
            return ifWriteOnly;
        }

        /**
         * The desired action if the index is in partly built, but the requested policy mismatches the previous one.
         * @return requested {@link DesiredAction}
         */
        public DesiredAction getIfMismatchPrevious() {
            return ifMismatchPrevious;
        }

        /**
         * The desired action if the index is in readable mode (i.e. already built).
         * @return requested {@link DesiredAction}
         */
        public DesiredAction getIfReadable() {
            return ifReadable;
        }

        /**
         * Get the appropriate desired action for a given index state.
         * @param state A given index state
         * @return The appropriate {@link DesiredAction}
         */
        public DesiredAction getStateDesiredAction(IndexState state) {
            switch (state) {
                case DISABLED:      return getIfDisabled();
                case WRITE_ONLY:    return getIfWriteOnly();
                case READABLE:      return getIfReadable();
                case READABLE_UNIQUE_PENDING: return DesiredAction.MARK_READABLE;
                default: throw new RecordCoreException("bad index state: " + state);
            }
        }

        /**
         * If true, mark readable (after indexing) should allow the {@link IndexState#READABLE_UNIQUE_PENDING} index state.
         * @param store the relevant store - used to check if matching format-version.
         * @return true if allowed
         */
        public boolean shouldAllowUniquePendingState(FDBRecordStore store) {
            return allowUniquePendingState
                    && store.getFormatVersionEnum().isAtLeast(FormatVersion.READABLE_UNIQUE_PENDING);
        }

        /**
         * If true, allow - in some specific cases - to continue building an index that was partly built by a different indexing method.
         * (See {@link Builder#allowTakeoverContinue(Collection)} and {@link Builder#allowTakeoverContinue(boolean)}).
         * @param newStamp the new (attempting to continue) indexing type stamp
         * @param oldStamp the old (previously used) indexing type stamp
         * @return true if allowed
         */
        public boolean shouldAllowTypeConversionContinue(IndexBuildProto.IndexBuildIndexingStamp newStamp,
                                                         IndexBuildProto.IndexBuildIndexingStamp oldStamp) {
            final IndexBuildProto.IndexBuildIndexingStamp.Method newMethod = newStamp.getMethod();
            final IndexBuildProto.IndexBuildIndexingStamp.Method oldMethod = oldStamp.getMethod();
            if (newMethod == IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS) {
                // Here: if allowed, an index that was partly built as a set will be continued as a single. Note that other target
                // indexes from the original set may have already continued individually.
                if (oldMethod == IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS) {
                    return isTypeConversionAllowed(TakeoverTypes.MULTI_TARGET_TO_SINGLE);
                }
                if (oldMethod == IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS) {
                    return isTypeConversionAllowed(TakeoverTypes.MUTUAL_TO_SINGLE);
                }
            }
            if (newMethod == IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS) {
                if (!isTypeConversionAllowed(TakeoverTypes.BY_RECORDS_TO_MUTUAL)) {
                    // This conversion is not allowed, check no further
                    return false;
                }
                if (oldMethod == IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS) {
                    // This conversion should be allowed if:
                    // a) The new mutual indexing set is identical to the old one
                    // b) The mutual indexing continues only one target index (splitting it to one at a tiem)
                    return newStamp.getTargetIndexCount() == 1 ||
                            (newStamp.getTargetIndexCount() == oldStamp.getTargetIndexCount() &&
                                    new HashSet<>(newStamp.getTargetIndexList()).containsAll(oldStamp.getTargetIndexList()));
                }

                if (oldMethod == IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS) {
                    // Note that single-target-mutual is a private case of multi-target-mutual
                    return newStamp.getTargetIndexCount() == 1; // No real need to check the stamp's index name
                }
            }
            return false;
        }

        private boolean isTypeConversionAllowed(TakeoverTypes takeoverType) {
            return allowedTakeoverSet != null && allowedTakeoverSet.contains(takeoverType);
        }


        /**
         * If true, allow indexing continuation of a blocked partly built index.
         * @param stampBlockId if non-null and non-empty, allow unblock only if matches the existing stamp's block id
         * @return true if unblock is allowed
         */
        public boolean shouldAllowUnblock(String stampBlockId) {
            return allowUnblock &&
                   (allowUnblockId == null || allowUnblockId.isEmpty() || allowUnblockId.equals(stampBlockId));
        }

        /**
         * <em>Deprecated</em> and unused.
         * If negative, avoid checks. Else, minimal interval between checks.
         * @return minimal interval in milliseconds.
         */
        @API(API.Status.DEPRECATED)
        public long getCheckIndexingMethodFrequencyMilliseconds() {
            return 0;
        }

        /**
         * Get the initial merges count limit for {@link #mergeIndex()} and the indexing process.
         * The default is 0 - which means unlimited.
         * @return the initial merges count limit.
         */
        @API(API.Status.EXPERIMENTAL)
        public long getInitialMergesCountLimit() {
            return this.initialMergesCountLimit;
        }

        /**
         * Get caller's request to scan records in reverse order while indexing.
         * @return true if reverse scan order was requested
         */
        public boolean isReverseScanOrder() {
            return reverseScanOrder;
        }

        /**
         * Builder for {@link IndexingPolicy}.
         *
         * <pre><code>
         * OnlineIndexer.IndexingPolicy.newBuilder().setSourceIndex("src_index").build()
         * </code></pre>
         *
         * Forbid fallback:
         * <pre><code>
         * OnlineIndexer.IndexingPolicy.newBuilder().setSourceIndex("src_index").forbidRecordScan().build()
         * </code></pre>
         *
         */
        @API(API.Status.UNSTABLE)
        public static class Builder {
            boolean forbidRecordScan = false;
            String sourceIndex = null;
            private Object sourceIndexSubspaceKey = null;
            private DesiredAction ifDisabled = DesiredAction.REBUILD;
            private DesiredAction ifWriteOnly = DesiredAction.CONTINUE;
            private DesiredAction ifMismatchPrevious = DesiredAction.CONTINUE;
            private DesiredAction ifReadable = DesiredAction.CONTINUE;
            private boolean doAllowUniquePendingState = false;
            private Set<TakeoverTypes> allowedTakeoverSet = null;
            private boolean useMutualIndexing = false;
            private List<Tuple> useMutualIndexingBoundaries = null;
            private boolean allowUnblock = false;
            private String allowUnblockId = null;
            private long initialMergesCountLimit = 0;
            private boolean reverseScanOrder = false;

            protected Builder() {
            }

            /**
             * Use this source index to scan records for indexing.
             * Some sanity checks will be performed, but it is the caller's responsibility to verify that this
             * source-index covers <em>all</em> the relevant records for the target-index. Also, note that
             * if the {@linkplain OnlineIndexer.Builder#setIndex(Index) target index} is not idempotent,
             * the index build will not be executed using the given source index unless the store's
             * format version is at least {@link FormatVersion#CHECK_INDEX_BUILD_TYPE_DURING_UPDATE},
             * as concurrent updates to the index during such a build on older format versions can
             * result in corrupting the index. On older format versions, the indexer will throw an
             * exception and the build may fall back to building the index by a records scan depending
             * on the value given to {@link #setForbidRecordScan(boolean)}.
             *
             * @param sourceIndex an existing, readable, index.
             * @return this builder
             */
            public Builder setSourceIndex(@Nonnull final String sourceIndex) {
                this.sourceIndex = sourceIndex;
                return this;
            }

            /**
             * Use this source index's subspace key to scan records for indexing.
             * This subspace key, if set, overrides the {@link #setSourceIndex(String)} option (a typical use would be
             * one or the other).
             *
             * @param sourceIndexSubspaceKey an existing, readable, index.
             * @return this builder
             */
            public Builder setSourceIndexSubspaceKey(@Nullable final Object sourceIndexSubspaceKey) {
                this.sourceIndexSubspaceKey = sourceIndexSubspaceKey;
                return this;
            }

            /**
             * If set to {@code true}, throw an exception when the requested indexing method cannot be used.
             * If {@code false} (also the default), allow a fallback to records scan.
             *
             * @param forbidRecordScan if true, do not allow fallback to a record scan method
             * @return this builder
             */
            public Builder setForbidRecordScan(boolean forbidRecordScan) {
                this.forbidRecordScan = forbidRecordScan;
                return this;
            }

            /**
             * Same as calling {@link #setForbidRecordScan(boolean)} with the value {@code true}.
             * @return this builder
             */
            public Builder forbidRecordScan() {
                this.forbidRecordScan = true;
                return this;
            }


            /**
             * Set the desired action if the index is disabled.
             * @param ifDisabled CONTINUE: build the index (effectively, it's the same as rebuild)
             *                   REBUILD: rebuild the index.
             *                   ERROR: reject (with exception).
             * @return this builder
             */
            public Builder setIfDisabled(final DesiredAction ifDisabled) {
                this.ifDisabled = ifDisabled;
                return this;
            }

            /**
             * Set the desired action if the index is in a write-only mode (i.e. partly built).
             * @param ifWriteOnly CONTINUE: (default) attempt to continue the previous indexing process
             *                    REBUILD: clear the existing data and rebuild
             *                    ERROR: reject (with exception).
             * @return this builder
             */
            public Builder setIfWriteOnly(final DesiredAction ifWriteOnly) {
                this.ifWriteOnly = ifWriteOnly;
                return this;
            }

            /**
             * Set the desired action if the index is in a write-only mode but the previous indexing method mismatches the requested one.
             * @param ifMismatchPrevious CONTINUE: (default) attempt to continue the previous indexing process, using the previous policy
             *                       REBUILD: clear the existing data and rebuild, using the new policy
             *                       ERROR: reject (with exception).
             * @return this builder
             */
            public Builder setIfMismatchPrevious(final DesiredAction ifMismatchPrevious) {
                this.ifMismatchPrevious = ifMismatchPrevious;
                return this;
            }

            /**
             * Set the desired action if the index is readable.
             * @param ifReadable CONTINUE: (default) do nothing.
             *                   REBUILD: clear the existing data and rebuild.
             *                   ERROR: reject (with exception)
             * @return this builder
             */
            public Builder setIfReadable(final DesiredAction ifReadable) {
                this.ifReadable = ifReadable;
                return this;
            }

            /**
             * Call {@link #allowUniquePendingState(boolean)} with default true.
             * @return this builder
             */
            public Builder allowUniquePendingState() {
                return this.allowUniquePendingState(true);
            }

            /**
             * After indexing a unique index, if requested to mark the index as readable ({@link #buildIndex() }), but
             * the index contain duplicates, this function will determine the next behavior:
             *    allow=true: mark the index as {@link IndexState#READABLE_UNIQUE_PENDING}.
             *    allow=false (default, backward compatible): throw  an exception. Note that if multiple indexes
             *                have duplications, the exception will only refer to an arbitrary one. Other target indexes (if
             *                indexed successfully) will be marked as readable.
             *    This state might not be compatible with older code. Please verify that all instances are up-to-date before allowing it.
             * @param allow allow a {@link IndexState#READABLE_UNIQUE_PENDING} index states.
             * @return this builder
             */
            public Builder allowUniquePendingState(boolean allow) {
                this.doAllowUniquePendingState = allow;
                return this;
            }

            /**
             * Call {@link #allowTakeoverContinue(boolean)} with default true.
             * @return this builder
             */
            public Builder allowTakeoverContinue() {
                return this.allowTakeoverContinue(true);
            }

            /**
             * In some special cases, an indexing method is allowed to continue building an index that was partly
             * built by another method. If the other indexing session is still running, a "takeover" may cause it to fail.
             * Note that it goes one way - once there is a "takeover", there is no way to continue building with the previous method.
             * The current supported takeovers are:
             * <ul>
             * <li>"Single index by records" may continue a multi index session.</li>
             * <li>"Single index by records" may continue a mutually built session.</li>
             *  </ul>
             *  This function will allow/disallow all possible conversion types. {@link #allowTakeoverContinue(Collection)} supports allowing a subset of these conversions.
             *  Note - if {@link #allowTakeoverContinue(Collection)} is called with a non-null argument, it will override this function's argument.
             * @param allow if true, allow takeover.
             * @return this builder
             */
            public Builder allowTakeoverContinue(boolean allow) {
                this.allowedTakeoverSet = allow ? EnumSet.allOf(TakeoverTypes.class) : EnumSet.noneOf(TakeoverTypes.class);
                return this;
            }

            /**
             * In some special cases, an indexing method is allowed to continue building an index that was partly
             * built by another method. If the other indexing session is still running, a "takeover" may cause it to fail.
             * Note that it goes one way - once there is a "takeover", there is no way to continue building with the previous method.
             *  Usage exmaple:
             *  {@code builder.allowTakeoverContinue(List.of(TakeroverTypes.MULTI_TARGET_TO_SINGLE, TakeroverTypes.BY_INDEX_TO_BY_RECORDS));}
             *  Note - If called with a non-null argument, it will override {@link #allowTakeoverContinue(boolean)}
             *
             * @param allowedSet - the types of conversion to allow. Null or empty set will allow no conversion.
             * @return this builder
             */
            public Builder allowTakeoverContinue(@Nullable Collection<TakeoverTypes> allowedSet) {
                this.allowedTakeoverSet = allowedSet == null ? null : EnumSet.copyOf(allowedSet);
                return this;
            }

            /**
             * <em>Deprecated</em> - for better consistency, the type stamp will now be validated during every iterating transaction.
             * During indexing, the indexer can check the current indexing stamp and throw an exception if it had changed.
             * This may happen if another indexing type takes over or by an indexing block (see {@link #indexingStamp}).
             * The argument may be:
             *  * -1: never check
             *  *  0: check during every transaction
             *  *  N: check, but never more frequently than every N milliseconds
             *  The default value is 60000 (wait at least 60 seconds between checks).
             * @param frequency : If negative, avoid checks. Else, minimal interval between checks
             * @return this builder.
             */
            @API(API.Status.DEPRECATED)
            public Builder checkIndexingStampFrequencyMilliseconds(long frequency) {
                // No-op
                return this;
            }

            /**
             * Call {@link #setMutualIndexing(boolean)} with default true.
             * @return this builder
             */
            @API(API.Status.EXPERIMENTAL)
            public Builder setMutualIndexing() {
                this.useMutualIndexing = true;
                return this;
            }

            /**
             * If set, allow mutual (parallel) indexing. In this state, the indexer will assume that other indexers, called
             * by other threads/processes/systems with the exact same parameters, are attempting to concurrently build this
             * index. To allow that, the indexer will:
             * <ol>
             *   <li> Divide the records space to fragments, then iterate the fragments in a way that minimizes the interference, while
             *      indexing each fragment independently.</li>
             *   <li> Handle indexing conflicts, when occurred.</li>
             *  </ol>
             * The caller may use any number of concurrent indexers as desired. By default, the fragments are
             * split by approximate FDB shard boundaries (the boundaries can also be preset by {@link #setMutualIndexingBoundaries(List)}).
             *
             * @param useMutualIndexing if true, allow this state.
             * @return this builder
             */
            @API(API.Status.EXPERIMENTAL)
            public Builder setMutualIndexing(final boolean useMutualIndexing) {
                this.useMutualIndexing = useMutualIndexing;
                if (!useMutualIndexing) {
                    useMutualIndexingBoundaries = null;
                }
                return this;
            }

            /**
             * Same as {@link #setMutualIndexing()}, but will use a pre-defined set of keys to split
             * the records space to fragments. {@code null} can be used to clear this value.
             * To ensure defining the full records range, the boundaries list can begin and end with a `null`.
             * @param primaryKeysBoundaries set of primary keys that will be used to split the records space to fragments.
             * @return this builder
             */
            @API(API.Status.EXPERIMENTAL)
            public Builder setMutualIndexingBoundaries(final List<Tuple> primaryKeysBoundaries) {
                if (primaryKeysBoundaries == null || primaryKeysBoundaries.isEmpty()) {
                    this.useMutualIndexingBoundaries = null;
                } else {
                    this.useMutualIndexingBoundaries = new ArrayList<>(primaryKeysBoundaries);
                }
                return this;
            }

            /**
             * If the index is partly built and blocked, allowed (or disallow) unblocking before indexing.
             * @param allowUnblock if true, unblock (if blocked) and continue.
             * @param allowUnblockId if non-null and non-empty, unblock only if the indexing stamp's id matches it. Will not continue if not blocked.
             * @return this builder
             */
            @API(API.Status.EXPERIMENTAL)
            public Builder setAllowUnblock(boolean allowUnblock, @Nullable String allowUnblockId) {
                this.allowUnblock = allowUnblock;
                this.allowUnblockId = allowUnblockId;
                return this;
            }

            /**
             * Call {@link #setAllowUnblock(boolean, String)} without block-id.
             * @param allowUnblock if true, allow unblock and continue
             * @return this builder
             */
            @API(API.Status.EXPERIMENTAL)
            public Builder setAllowUnblock(boolean allowUnblock) {
                return setAllowUnblock(allowUnblock, null);
            }


            /**
             * Set the initial merges count limit for {@link #mergeIndex()} and the indexing process.
             * The default is 0 - which means unlimited.
             * @param initialMergesCountLimit the initial merges count limit
             * @return this builder
             */
            public Builder setInitialMergesCountLimit(long initialMergesCountLimit) {
                this.initialMergesCountLimit = initialMergesCountLimit;
                return this;
            }


            /**
             * Set a reverse records scan order for indexing. Calling it will make sense only if the scan order matters.
             * Note that reverse scan order is supported only for ByIndex and MultiTarget index builds (but not rebuild).
             * @param reverseScanOrder if true, scan records in reverse order
             * @return this builder
             */
            public Builder setReverseScanOrder(final boolean reverseScanOrder) {
                this.reverseScanOrder = reverseScanOrder;
                return this;
            }

            public IndexingPolicy build() {
                if (useMutualIndexingBoundaries != null) {
                    useMutualIndexing = true;
                }
                return new IndexingPolicy(sourceIndex, sourceIndexSubspaceKey, forbidRecordScan,
                        ifDisabled, ifWriteOnly, ifMismatchPrevious, ifReadable,
                        doAllowUniquePendingState, allowedTakeoverSet,
                        useMutualIndexing, useMutualIndexingBoundaries, allowUnblock, allowUnblockId,
                        initialMergesCountLimit, reverseScanOrder);
            }
        }
    }

    /**
     * Create an online indexer builder.
     * @return a new online indexer builder
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create an online indexer for the given record store and index.
     * @param recordStore record store in which to index
     * @param index name of index to build
     * @return a new online indexer
     */
    @Nonnull
    public static OnlineIndexer forRecordStoreAndIndex(@Nonnull FDBRecordStore recordStore, @Nonnull String index) {
        return newBuilder().setRecordStore(recordStore).setIndex(index).build();
    }

    /**
     * This defines in which situations the index should be built. {@link #BUILD_IF_DISABLED},
     * {@link #BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY}, {@link #BUILD_IF_DISABLED_REBUILD_IF_WRITE_ONLY},
     * {@link #FORCE_BUILD}, {@link #BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY_REBUILD_IF_POLICY_CHANGED},
     * and {@link #BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY_ERROR_IF_POLICY_CHANGED}
     * are sorted in a way so that each option will build the index in more situations than the
     * ones before it.
     * <p>
     * Of these, {@link #BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY} is recommended if there is no reason to believe
     * current index data is corrupted.
     * </p>
     */
    public enum IndexStatePrecondition {
        /**
         * Only build if the index is disabled.
         */
        BUILD_IF_DISABLED(IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.ERROR, IndexingPolicy.DesiredAction.ERROR, IndexingPolicy.DesiredAction.ERROR),
        /**
         * Build if the index is disabled; Continue build if the index is write-only and the requested
         * method is the same as the previous one. Else throw a RecordCoreException exception.
         */
        BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY_ERROR_IF_POLICY_CHANGED(IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.CONTINUE, IndexingPolicy.DesiredAction.ERROR, IndexingPolicy.DesiredAction.CONTINUE),
        /**
         * Build if the index is disabled; Continue build if the index is write-only.
         * If the index is write-only and partly built, continue building it according the previous indexing policy (ignoring
         * the new requested one, if conflicting).
         * Alternatives for this continuation are provided by the options {@link #BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY_REBUILD_IF_POLICY_CHANGED}
         * and {@link #BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY_ERROR_IF_POLICY_CHANGED}
         *
         * <p>
         * Recommended. This should be sufficient if current index data is not corrupted.
         * </p>
         */
        BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY(IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.CONTINUE, IndexingPolicy.DesiredAction.CONTINUE, IndexingPolicy.DesiredAction.CONTINUE),
        /**
         * Build if the index is disabled; Continue build if the index is write-only - only if the requested
         * method matches the previous one, else restart the built.
         */
        BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY_REBUILD_IF_POLICY_CHANGED(IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.CONTINUE, IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.CONTINUE),
        /**
         * Build if the index is disabled; Rebuild if the index is write-only.
         */
        BUILD_IF_DISABLED_REBUILD_IF_WRITE_ONLY(IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.CONTINUE),
        /**
         * Rebuild the index anyway, no matter if it is disabled or write-only or readable.
         */
        FORCE_BUILD(IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.REBUILD, IndexingPolicy.DesiredAction.REBUILD),
        /**
         * Continue build only if the index is write-only and the requested method matches the previous one; Never rebuild.
         * To use this option to build an index, one should mark the index as write-only and clear the existing
         * index entries before building. This option is provided to make {@link #buildIndexAsync()} (or its
         * variations) behave same as what it did before version 2.8.90.0, which is not recommended.
         * {@link #BUILD_IF_DISABLED_CONTINUE_BUILD_IF_WRITE_ONLY} should be adopted instead.
         */
        ERROR_IF_DISABLED_CONTINUE_IF_WRITE_ONLY(IndexingPolicy.DesiredAction.ERROR, IndexingPolicy.DesiredAction.CONTINUE, IndexingPolicy.DesiredAction.ERROR, IndexingPolicy.DesiredAction.ERROR),
        ;
        public final IndexingPolicy.DesiredAction ifDisabled;
        public final IndexingPolicy.DesiredAction ifWriteOnly;
        public final IndexingPolicy.DesiredAction ifMismatchPrevious;
        public final IndexingPolicy.DesiredAction ifReadable;

        IndexStatePrecondition(IndexingPolicy.DesiredAction ifDisabled, IndexingPolicy.DesiredAction ifWriteOnly, IndexingPolicy.DesiredAction ifMismatchPrevious, IndexingPolicy.DesiredAction ifReadable) {
            this.ifDisabled = ifDisabled;
            this.ifWriteOnly = ifWriteOnly;
            this.ifMismatchPrevious = ifMismatchPrevious;
            this.ifReadable = ifReadable;
        }
    }
}
