/*
 * IndexStateManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.FormerIndex;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingHeartbeat;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.LoggableException;
import com.google.common.base.Suppliers;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Manages index state queries, transitions, and rebuild orchestration for an {@link FDBRecordStore}.
 *
 * <p>
 * This is a package-private helper extracted from {@code FDBRecordStore} to reduce its size and
 * improve cohesion. All index lifecycle operations — querying state, marking indexes as readable/
 * write-only/disabled, rebuilding indexes during version checks — are handled here.
 * </p>
 */
@API(API.Status.INTERNAL)
class IndexStateManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexStateManager.class);

    @Nonnull
    private final FDBRecordStore store;

    IndexStateManager(@Nonnull FDBRecordStore store) {
        this.store = store;
    }

    // region State Read/Write Control

    void beginRecordStoreStateRead() {
        store.recordStoreStateRef.get().beginRead();
    }

    void endRecordStoreStateRead() {
        store.recordStoreStateRef.get().endRead();
    }

    void beginRecordStoreStateWrite() {
        store.recordStoreStateRef.get().beginWrite();
    }

    void endRecordStoreStateWrite() {
        store.recordStoreStateRef.get().endWrite();
    }

    // endregion

    // region Index State Query

    @Nonnull
    IndexState getIndexState(@Nonnull Index index) {
        return getIndexState(index.getName());
    }

    @Nonnull
    IndexState getIndexState(@Nonnull String indexName) {
        store.addIndexStateReadConflict(indexName);
        return store.getRecordStoreState().getState(indexName);
    }

    boolean isIndexReadable(@Nonnull Index index) {
        return isIndexReadable(index.getName());
    }

    boolean isIndexReadable(@Nonnull String indexName) {
        return getIndexState(indexName).equals(IndexState.READABLE);
    }

    boolean isIndexReadableUniquePending(@Nonnull Index index) {
        return isIndexReadableUniquePending(index.getName());
    }

    boolean isIndexReadableUniquePending(@Nonnull String indexName) {
        return getIndexState(indexName).equals(IndexState.READABLE_UNIQUE_PENDING);
    }

    boolean isIndexScannable(@Nonnull Index index) {
        return isIndexScannable(index.getName());
    }

    boolean isIndexScannable(@Nonnull String indexName) {
        return getIndexState(indexName).isScannable();
    }

    boolean isIndexWriteOnly(@Nonnull Index index) {
        return isIndexWriteOnly(index.getName());
    }

    boolean isIndexWriteOnly(@Nonnull String indexName) {
        return getIndexState(indexName).equals(IndexState.WRITE_ONLY);
    }

    boolean isIndexDisabled(@Nonnull Index index) {
        return isIndexDisabled(index.getName());
    }

    boolean isIndexDisabled(@Nonnull String indexName) {
        return getIndexState(indexName).equals(IndexState.DISABLED);
    }

    @Nonnull
    Map<Index, IndexState> getAllIndexStates() {
        final RecordStoreState localRecordStoreState = store.getRecordStoreState();
        localRecordStoreState.beginRead();
        try {
            store.addStoreStateReadConflict();
            return store.getRecordMetaData().getAllIndexes().stream()
                    .collect(Collectors.toMap(Function.identity(), localRecordStoreState::getState));
        } finally {
            localRecordStoreState.endRead();
        }
    }

    // endregion

    // region Index State Mutation

    @SuppressWarnings("PMD.CloseResource")
    void updateIndexState(@Nonnull String indexName, byte[] indexKey, @Nonnull IndexState indexState) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("index state change",
                    LogMessageKeys.INDEX_NAME, indexName,
                    LogMessageKeys.TARGET_INDEX_STATE, indexState.name(),
                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)
            ));
        }
        if (store.recordStoreStateRef.get() == null) {
            throw store.uninitializedStoreException("cannot update index state on an uninitialized store");
        }
        beginRecordStoreStateWrite();
        try {
            store.context.setDirtyStoreState(true);
            if (store.isStateCacheableInternal()) {
                store.context.setMetaDataVersionStamp();
            }
            Transaction tr = store.context.ensureActive();
            if (IndexState.READABLE.equals(indexState)) {
                tr.clear(indexKey);
            } else {
                tr.set(indexKey, Tuple.from(indexState.code()).pack());
            }
            store.recordStoreStateRef.updateAndGet(state -> {
                state.setState(indexName, indexState);
                return state;
            });
        } finally {
            endRecordStoreStateWrite();
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Boolean> markIndexNotReadable(@Nonnull String indexName, @Nonnull IndexState indexState) {
        if (store.recordStoreStateRef.get() == null) {
            return store.preloadRecordStoreStateAsync().thenCompose(vignore -> markIndexNotReadable(indexName, indexState));
        }

        store.addIndexStateReadConflict(indexName);

        beginRecordStoreStateWrite();
        boolean haveFuture = false;
        try {
            byte[] indexKey = store.indexStateSubspace().pack(indexName);
            Transaction tr = store.context.ensureActive();
            CompletableFuture<Boolean> future = tr.get(indexKey).thenCompose(previous -> {
                if (previous == null) {
                    IndexingRangeSet indexRangeSet = IndexingRangeSet.forIndexBuild(store, store.getRecordMetaData().getIndex(indexName));
                    return indexRangeSet.isEmptyAsync().thenCompose(empty -> {
                        if (empty) {
                            return indexRangeSet.insertRangeAsync(null, null);
                        } else {
                            return AsyncUtil.READY_FALSE;
                        }
                    }).thenApply(ignore -> {
                        updateIndexState(indexName, indexKey, indexState);
                        return true;
                    });
                } else if (!Tuple.fromBytes(previous).get(0).equals(indexState.code())) {
                    updateIndexState(indexName, indexKey, indexState);
                    return AsyncUtil.READY_TRUE;
                } else {
                    return AsyncUtil.READY_FALSE;
                }
            }).whenComplete((b, t) -> endRecordStoreStateWrite());
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
            }
        }
    }

    @Nonnull
    CompletableFuture<Boolean> markIndexWriteOnly(@Nonnull String indexName) {
        return markIndexNotReadable(indexName, IndexState.WRITE_ONLY);
    }

    @Nonnull
    CompletableFuture<Boolean> markIndexWriteOnly(@Nonnull Index index) {
        return markIndexWriteOnly(index.getName());
    }

    @Nonnull
    CompletableFuture<Boolean> markIndexDisabled(@Nonnull String indexName) {
        return markIndexDisabled(store.getRecordMetaData().getIndex(indexName));
    }

    @Nonnull
    CompletableFuture<Boolean> markIndexDisabled(@Nonnull Index index) {
        return markIndexNotReadable(index.getName(), IndexState.DISABLED).thenApply(changed -> {
            if (changed) {
                clearIndexData(index);
            }
            return changed;
        });
    }

    @Nonnull
    CompletableFuture<Boolean> markIndexReadableOrUniquePending(@Nonnull Index index) {
        return markIndexReadable(index, true);
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Boolean> markIndexReadable(@Nonnull Index index) {
        return markIndexReadable(index, false);
    }

    @Nonnull
    private CompletableFuture<Boolean> markIndexReadable(@Nonnull Index index, boolean allowUniquePending) {
        if (store.recordStoreStateRef.get() == null) {
            return store.preloadRecordStoreStateAsync().thenCompose(vignore -> markIndexReadable(index, allowUniquePending));
        }

        store.addIndexStateReadConflict(index.getName());

        beginRecordStoreStateWrite();
        boolean haveFuture = false;
        try {
            @SuppressWarnings("PMD.CloseResource")
            Transaction tr = store.ensureContextActive();
            byte[] indexKey = store.indexStateSubspace().pack(index.getName());
            CompletableFuture<Boolean> future = tr.get(indexKey).thenCompose(previous -> {
                if (previous != null) {
                    return checkAndUpdateBuiltIndexState(index, indexKey, allowUniquePending);
                } else {
                    return AsyncUtil.READY_FALSE;
                }
            }).whenComplete((b, t) -> endRecordStoreStateWrite()).thenApply(this::addRemoveReplacedIndexesCommitCheckIfChanged);
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
            }
        }
    }

    @Nonnull
    CompletableFuture<Boolean> markIndexReadable(@Nonnull String indexName) {
        return markIndexReadable(store.getRecordMetaData().getIndex(indexName));
    }

    private CompletableFuture<Boolean> checkAndUpdateBuiltIndexState(Index index, byte[] indexKey, boolean allowUniquePending) {
        CompletableFuture<Optional<Range>> builtFuture = firstUnbuiltRange(index);
        CompletableFuture<Optional<RecordIndexUniquenessViolation>> uniquenessFuture;
        if (index.isUnique()) {
            uniquenessFuture = store.whenAllIndexUniquenessCommitChecks(index)
                    .thenCompose(vignore -> store.scanUniquenessViolations(index, 1).first());
        } else {
            uniquenessFuture = CompletableFuture.completedFuture(Optional.empty());
        }
        return CompletableFuture.allOf(builtFuture, uniquenessFuture).thenApply(vignore -> {
            Optional<Range> firstUnbuilt = store.context.join(builtFuture);
            Optional<RecordIndexUniquenessViolation> uniquenessViolation = store.context.join(uniquenessFuture);

            if (firstUnbuilt.isPresent()) {
                throw new FDBRecordStore.IndexNotBuiltException("Attempted to make unbuilt index readable", firstUnbuilt.get(),
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        "unbuiltRangeBegin", ByteArrayUtil2.loggable(firstUnbuilt.get().begin),
                        "unbuiltRangeEnd", ByteArrayUtil2.loggable(firstUnbuilt.get().end),
                        store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context),
                        LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            } else if (uniquenessViolation.isPresent()) {
                if (allowUniquePending) {
                    if (isIndexReadableUniquePending(index)) {
                        return false;   // Unchanged
                    }
                    updateIndexState(index.getName(), indexKey, IndexState.READABLE_UNIQUE_PENDING);
                    return true;
                }
                RecordIndexUniquenessViolation wrapped = new RecordIndexUniquenessViolation("Uniqueness violation when making index readable",
                        uniquenessViolation.get());
                wrapped.addLogInfo(
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context));
                throw wrapped;
            } else {
                updateIndexState(index.getName(), indexKey, IndexState.READABLE);
                clearReadableIndexBuildData(index);
                return true;
            }
        });
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Boolean> uncheckedMarkIndexReadable(@Nonnull String indexName) {
        if (store.recordStoreStateRef.get() == null) {
            return store.preloadRecordStoreStateAsync().thenCompose(vignore -> uncheckedMarkIndexReadable(indexName));
        }

        store.addIndexStateReadConflict(indexName);

        beginRecordStoreStateWrite();
        boolean haveFuture = false;
        try {
            Transaction tr = store.ensureContextActive();
            byte[] indexKey = store.indexStateSubspace().pack(indexName);
            CompletableFuture<Boolean> future = tr.get(indexKey).thenApply(previous -> {
                if (previous != null) {
                    updateIndexState(indexName, indexKey, IndexState.READABLE);
                    return true;
                } else {
                    return false;
                }
            }).whenComplete((b, t) -> endRecordStoreStateWrite()).thenApply(this::addRemoveReplacedIndexesCommitCheckIfChanged);
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
            }
        }
    }

    @Nonnull
    CompletableFuture<Void> clearAndMarkIndexWriteOnly(@Nonnull String indexName) {
        return clearAndMarkIndexWriteOnly(store.getRecordMetaData().getIndex(indexName));
    }

    @Nonnull
    CompletableFuture<Void> clearAndMarkIndexWriteOnly(@Nonnull Index index) {
        return markIndexWriteOnly(index)
                .thenRun(() -> clearIndexData(index));
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Optional<Range>> firstUnbuiltRange(@Nonnull Index index) {
        if (!store.getRecordMetaData().hasIndex(index.getName())) {
            throw new MetaDataException("Index " + index.getName() + " does not exist in meta-data.");
        }
        IndexingRangeSet rangeSet = IndexingRangeSet.forIndexBuild(store, index);
        return rangeSet.firstMissingRangeAsync().thenApply(Optional::ofNullable);
    }

    // endregion

    // region Replaced Index Management

    boolean addRemoveReplacedIndexesCommitCheckIfChanged(boolean changed) {
        if (changed) {
            final String commitCheckName = "removeReplacedIndexes_" + ByteArrayUtil2.toHexString(store.getSubspace().pack());
            store.getRecordContext().getOrCreateCommitCheck(commitCheckName, name -> this::removeReplacedIndexes);
        }
        return changed;
    }

    @Nonnull
    CompletableFuture<Boolean> removeReplacedIndexesIfChanged(boolean changed) {
        if (changed) {
            return removeReplacedIndexes().thenApply(vignore -> true);
        } else {
            return AsyncUtil.READY_FALSE;
        }
    }

    @Nonnull
    CompletableFuture<Void> removeReplacedIndexes() {
        if (store.recordStoreStateRef.get() == null) {
            return store.preloadRecordStoreStateAsync().thenCompose(vignore -> removeReplacedIndexes());
        }

        beginRecordStoreStateRead();
        final RecordMetaData metaData = store.getRecordMetaData();
        final List<Index> indexesToRemove = new ArrayList<>();
        try {
            for (Index index : metaData.getAllIndexes()) {
                final List<String> replacedByNames = index.getReplacedByIndexNames();
                if (!replacedByNames.isEmpty()) {
                    if (replacedByNames.stream()
                            .allMatch(replacedByName -> metaData.hasIndex(replacedByName) && isIndexReadable(replacedByName))) {
                        indexesToRemove.add(index);
                    }
                }
            }
        } finally {
            endRecordStoreStateRead();
        }

        if (indexesToRemove.isEmpty()) {
            return AsyncUtil.DONE;
        }

        beginRecordStoreStateWrite();
        boolean haveFuture = false;
        try {
            final List<CompletableFuture<Boolean>> indexRemoveFutures = new ArrayList<>(indexesToRemove.size());
            for (Index index : indexesToRemove) {
                indexRemoveFutures.add(markIndexDisabled(index));
            }
            CompletableFuture<Void> future = AsyncUtil.whenAll(indexRemoveFutures)
                    .whenComplete((vignore, errIgnore) -> endRecordStoreStateWrite());
            haveFuture = true;
            return future;
        } finally {
            if (!haveFuture) {
                endRecordStoreStateWrite();
            }
        }
    }

    // endregion

    // region Index Data Management

    @SuppressWarnings("PMD.CloseResource")
    void clearIndexData(@Nonnull Index index) {
        store.context.clear(Range.startsWith(store.indexSubspace(index).pack()));
        store.context.clear(store.indexSecondarySubspace(index).range());
        IndexingRangeSet.forIndexBuild(store, index).clear();
        store.context.clear(store.indexUniquenessViolationsSubspace(index).range());
        IndexingSubspaces.eraseAllIndexingDataButTheLock(store.context, store, index);
    }

    private void clearReadableIndexBuildData(Index index) {
        IndexingRangeSet.forIndexBuild(store, index).clear();
        IndexingHeartbeat.clearAllHeartbeats(store, index);
    }

    @SuppressWarnings("PMD.CloseResource")
    void removeFormerIndex(FormerIndex formerIndex) {
        if (LOGGER.isDebugEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("removing index",
                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context),
                    LogMessageKeys.SUBSPACE_KEY, formerIndex.getSubspaceKey());
            if (formerIndex.getFormerName() != null) {
                msg.addKeyAndValue(LogMessageKeys.INDEX_NAME, formerIndex.getFormerName());
            }
            LOGGER.debug(msg.toString());
        }
        final long startTime = System.nanoTime();
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_KEY, formerIndex.getSubspaceTupleKey())));
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_SECONDARY_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_RANGE_SPACE_KEY, formerIndex.getSubspaceTupleKey())));
        final String formerIndexName = formerIndex.getFormerName();
        if (formerIndexName != null) {
            updateIndexState(formerIndexName, store.getSubspace().pack(Tuple.from(FDBRecordStore.INDEX_STATE_SPACE_KEY, formerIndexName)), IndexState.READABLE);
        }
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_UNIQUENESS_VIOLATIONS_KEY, formerIndex.getSubspaceTupleKey())));
        if (store.getTimer() != null) {
            store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.REMOVE_FORMER_INDEX, startTime);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    void vacuumReadableIndexesBuildData() {
        Map<Index, IndexState> indexStates = getAllIndexStates();
        for (Map.Entry<Index, IndexState> entry : indexStates.entrySet()) {
            if (entry.getValue().equals(IndexState.READABLE)) {
                clearReadableIndexBuildData(entry.getKey());
            }
        }
    }

    @Nonnull
    Map<Index, List<RecordType>> getIndexesToBuild() {
        if (store.recordStoreStateRef.get() == null) {
            throw store.uninitializedStoreException("cannot get indexes to build on uninitialized store");
        }
        final Map<Index, List<RecordType>> indexesToBuild = store.getRecordMetaData().getIndexesToBuildSince(-1);
        beginRecordStoreStateRead();
        try {
            indexesToBuild.keySet().removeIf(this::isIndexReadable);
            return indexesToBuild;
        } finally {
            endRecordStoreStateRead();
        }
    }

    // endregion

    // region Rebuild Index Methods

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    CompletableFuture<Void> rebuildAllIndexes() {
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_KEY)));
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_SECONDARY_SPACE_KEY)));
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_RANGE_SPACE_KEY)));
        store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.INDEX_UNIQUENESS_VIOLATIONS_KEY)));
        List<CompletableFuture<Void>> work = new LinkedList<>();
        addRebuildRecordCountsJob(work);
        return rebuildIndexes(store.getRecordMetaData().getIndexesToBuildSince(-1), Collections.emptyMap(), work,
                FDBRecordStore.RebuildIndexReason.REBUILD_ALL, null);
    }

    private CompletableFuture<Map<Index, IndexState>> rebuildIndexesGetDesiredIndexStates(
            @Nonnull List<CompletableFuture<Void>> preWork,
            @Nonnull Map<Index, CompletableFuture<IndexState>> newStates) {
        final ConcurrentHashMap<Index, IndexState> desiredIndexStates = new ConcurrentHashMap<>();
        final List<CompletableFuture<Void>> allWork = new ArrayList<>(preWork);
        for (Map.Entry<Index, CompletableFuture<IndexState>> entry : newStates.entrySet()) {
            allWork.add(entry.getValue().thenAccept(state -> desiredIndexStates.put(entry.getKey(), state)));
        }
        return AsyncUtil.whenAll(allWork).thenApply(ignore -> desiredIndexStates);
    }

    @Nonnull
    CompletableFuture<Void> rebuildIndexes(@Nonnull Map<Index, List<RecordType>> indexes,
                                           @Nonnull Map<Index, CompletableFuture<IndexState>> newStates,
                                           @Nonnull List<CompletableFuture<Void>> work,
                                           @Nonnull FDBRecordStore.RebuildIndexReason reason,
                                           @Nullable Integer oldMetaDataVersion) {
        return rebuildIndexesGetDesiredIndexStates(work, newStates).thenCompose(desiredIndexStates ->
                rebuildIndexes(indexes, desiredIndexStates, reason, oldMetaDataVersion));
    }

    @Nonnull
    CompletableFuture<Void> rebuildIndexes(@Nonnull Map<Index, List<RecordType>> indexes,
                                           @Nonnull Map<Index, IndexState> desiredIndexStates,
                                           @Nonnull FDBRecordStore.RebuildIndexReason reason,
                                           @Nullable Integer oldMetaDataVersion) {
        List<CompletableFuture<Void>> work = new ArrayList<>();
        Iterator<Map.Entry<Index, List<RecordType>>> indexIter = indexes.entrySet().iterator();
        return AsyncUtil.whileTrue(() -> {
            Iterator<CompletableFuture<Void>> workIter = work.iterator();
            while (workIter.hasNext()) {
                CompletableFuture<Void> workItem = workIter.next();
                if (workItem.isDone()) {
                    store.context.asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, workItem);
                    workIter.remove();
                }
            }
            while (work.size() < FDBRecordStore.MAX_PARALLEL_INDEX_REBUILD) {
                if (indexIter.hasNext()) {
                    Map.Entry<Index, List<RecordType>> indexItem = indexIter.next();
                    Index index = indexItem.getKey();
                    List<RecordType> recordTypes = indexItem.getValue();
                    IndexState indexState = desiredIndexStates.getOrDefault(index, IndexState.READABLE);
                    final StringBuilder errMessageBuilder = new StringBuilder("unable to ");
                    final CompletableFuture<Void> rebuildOrMarkIndexSafely = MoreAsyncUtil.handleOnException(
                            () -> rebuildOrMarkIndex(index, indexState, recordTypes, reason, oldMetaDataVersion, errMessageBuilder),
                            exception -> {
                                logExceptionAsWarn(KeyValueLogMessage.build(errMessageBuilder.toString(),
                                        LogMessageKeys.INDEX_NAME, index.getName()
                                ), exception);
                                return markIndexDisabled(index).thenApply(b -> null);
                            });
                    work.add(rebuildOrMarkIndexSafely);
                } else {
                    break;
                }
            }
            if (work.isEmpty()) {
                return AsyncUtil.READY_FALSE;
            }
            return AsyncUtil.whenAny(work).thenApply(v -> true);
        }, store.getExecutor());
    }

    boolean areAllRecordTypesSince(@Nullable Collection<RecordType> recordTypes, @Nullable Integer oldMetaDataVersion) {
        return oldMetaDataVersion != null && (oldMetaDataVersion == -1 || (recordTypes != null && recordTypes.stream().allMatch(recordType -> {
            Integer sinceVersion = recordType.getSinceVersion();
            return sinceVersion != null && sinceVersion > oldMetaDataVersion;
        })));
    }

    CompletableFuture<Void> rebuildOrMarkIndex(@Nonnull Index index, @Nonnull IndexState indexState,
                                               @Nullable List<RecordType> recordTypes,
                                               @Nonnull FDBRecordStore.RebuildIndexReason reason,
                                               @Nullable Integer oldMetaDataVersion,
                                               @Nonnull StringBuilder errMessageBuilder) {
        if (indexState != IndexState.DISABLED && areAllRecordTypesSince(recordTypes, oldMetaDataVersion)) {
            errMessageBuilder.append("rebuild index with no records");
            return rebuildIndexWithNoRecord(index, reason);
        }

        switch (indexState) {
            case WRITE_ONLY:
                errMessageBuilder.append("clear and mark index write only");
                return clearAndMarkIndexWriteOnly(index).thenApply(b -> null);
            case DISABLED:
                errMessageBuilder.append("mark index disabled");
                return markIndexDisabled(index).thenApply(b -> null);
            case READABLE:
            default:
                errMessageBuilder.append("rebuild index");
                return rebuildIndex(index, reason);
        }
    }

    @Nonnull
    private CompletableFuture<Void> rebuildIndexWithNoRecord(@Nonnull final Index index,
                                                              @Nonnull FDBRecordStore.RebuildIndexReason reason) {
        final boolean newStore = reason == FDBRecordStore.RebuildIndexReason.NEW_STORE;
        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage msg = KeyValueLogMessage.build("rebuilding index with no record",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                    LogMessageKeys.REASON, reason.name(),
                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context),
                    LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            if (newStore) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(msg.toString());
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(msg.toString());
                }
            }
        }

        return markIndexReadable(index).thenApply(b -> null);
    }

    @Nonnull
    CompletableFuture<Void> rebuildIndex(@Nonnull Index index) {
        return rebuildIndex(index, FDBRecordStore.RebuildIndexReason.EXPLICIT);
    }

    @Nonnull
    @SuppressWarnings({"squid:S2095", "PMD.CloseResource"})
    CompletableFuture<Void> rebuildIndex(@Nonnull final Index index, @Nonnull FDBRecordStore.RebuildIndexReason reason) {
        final boolean newStore = reason == FDBRecordStore.RebuildIndexReason.NEW_STORE;
        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage msg = KeyValueLogMessage.build("rebuilding index",
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                    LogMessageKeys.REASON, reason.name(),
                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context),
                    LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey());
            if (newStore) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(msg.toString());
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(msg.toString());
                }
            }
        }

        long startTime = System.nanoTime();
        OnlineIndexer indexBuilder = OnlineIndexer.newBuilder().setRecordStore(store).setIndex(index).build();
        CompletableFuture<Void> future = indexBuilder.rebuildIndexAsync(store)
                .thenCompose(vignore -> markIndexReadable(index))
                .handle((b, t) -> {
                    if (t != null) {
                        logExceptionAsWarn(KeyValueLogMessage.build("rebuilding index failed",
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_VERSION, index.getLastModifiedVersion(),
                                LogMessageKeys.REASON, reason.name(),
                                LogMessageKeys.SUBSPACE_KEY, index.getSubspaceKey()), t);
                    }
                    indexBuilder.close();
                    return null;
                });

        return store.context.instrument(FDBStoreTimer.Events.REBUILD_INDEX,
                store.context.instrument(reason.event, future, startTime),
                startTime);
    }

    // endregion

    // region Rebuild Orchestration (checkVersion path)

    @SuppressWarnings("PMD.GuardLogStatement")
    CompletableFuture<Void> checkPossiblyRebuild(@Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                                  @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                  @Nonnull boolean[] dirty) {
        final int oldFormatVersion = info.getFormatVersion();
        final int newFormatVersion = Math.max(oldFormatVersion, store.formatVersion.getValueForSerialization());
        final boolean formatVersionChanged = oldFormatVersion != newFormatVersion;
        store.formatVersion = FormatVersion.getFormatVersion(newFormatVersion);

        final boolean newStore = oldFormatVersion == 0;
        final int oldMetaDataVersion = newStore ? -1 : info.getMetaDataversion();
        final RecordMetaData metaData = store.getRecordMetaData();
        final int newMetaDataVersion = metaData.getVersion();
        if (oldMetaDataVersion > newMetaDataVersion) {
            CompletableFuture<Void> ret = new CompletableFuture<>();
            ret.completeExceptionally(new RecordStoreStaleMetaDataVersionException("Local meta-data has stale version",
                    LogMessageKeys.LOCAL_VERSION, newMetaDataVersion,
                    LogMessageKeys.STORED_VERSION, oldMetaDataVersion,
                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
            return ret;
        }
        final boolean metaDataVersionChanged = oldMetaDataVersion != newMetaDataVersion;

        if (!formatVersionChanged && !metaDataVersionChanged) {
            return AsyncUtil.DONE;
        }

        if (LOGGER.isInfoEnabled()) {
            if (newStore) {
                LOGGER.info(KeyValueLogMessage.of("new record store",
                        LogMessageKeys.FORMAT_VERSION, newFormatVersion,
                        LogMessageKeys.META_DATA_VERSION, newMetaDataVersion,
                        store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
            } else {
                if (formatVersionChanged) {
                    LOGGER.info(KeyValueLogMessage.of("format version changed",
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, newFormatVersion,
                            store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
                }
                if (metaDataVersionChanged) {
                    LOGGER.info(KeyValueLogMessage.of("meta-data version changed",
                            LogMessageKeys.OLD_VERSION, oldMetaDataVersion,
                            LogMessageKeys.NEW_VERSION, newMetaDataVersion,
                            store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
                }
            }
        }

        dirty[0] = true;
        return checkRebuild(userVersionChecker, info, metaData);
    }

    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Void> checkRebuild(@Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                                  @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                  @Nonnull RecordMetaData metaData) {
        final List<CompletableFuture<Void>> work = new LinkedList<>();

        final int oldFormatVersion = info.getFormatVersion();
        if (oldFormatVersion != store.formatVersion.getValueForSerialization()) {
            info.setFormatVersion(store.formatVersion.getValueForSerialization());
            if ((oldFormatVersion >= FormatVersion.getMinimumVersion().getValueForSerialization()
                    && oldFormatVersion < FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX.getValueForSerialization()
                    && store.formatVersion.isAtLeast(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX)
                    && !metaData.isSplitLongRecords())) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("unsplit records stored at old format",
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, store.formatVersion,
                            store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
                }
                info.setOmitUnsplitRecordSuffix(true);
                store.omitUnsplitRecordSuffix = true;
            }
            if (oldFormatVersion >= FormatVersion.getMinimumVersion().getValueForSerialization()
                    && oldFormatVersion < FormatVersion.SAVE_VERSION_WITH_RECORD.getValueForSerialization()
                    && metaData.isStoreRecordVersions() && !store.useOldVersionFormat()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("migrating record versions to new format",
                            LogMessageKeys.OLD_VERSION, oldFormatVersion,
                            LogMessageKeys.NEW_VERSION, store.formatVersion,
                            store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
                }
                addConvertRecordVersions(work);
            }
        }

        final boolean newStore = oldFormatVersion == 0;
        final int oldMetaDataVersion = newStore ? -1 : info.getMetaDataversion();
        final int newMetaDataVersion = metaData.getVersion();
        final boolean metaDataVersionChanged = oldMetaDataVersion != newMetaDataVersion;
        if (metaDataVersionChanged) {
            if (!metaData.isStoreRecordVersions() && !newStore
                    && store.useOldVersionFormat()) {
                final Transaction tr = store.ensureContextActive();
                tr.clear(store.getSubspace().subspace(Tuple.from(FDBRecordStore.RECORD_VERSION_KEY)).range());
            }
            info.setMetaDataversion(newMetaDataVersion);
        }

        final boolean rebuildRecordCounts = checkPossiblyRebuildRecordCounts(metaData, info, work, oldFormatVersion);

        if (!metaDataVersionChanged) {
            return work.isEmpty() ? AsyncUtil.DONE : AsyncUtil.whenReady(work.get(0));
        }

        for (FormerIndex formerIndex : metaData.getFormerIndexesSince(oldMetaDataVersion)) {
            removeFormerIndex(formerIndex);
        }

        return checkRebuildIndexes(userVersionChecker, info, oldFormatVersion, metaData, oldMetaDataVersion, rebuildRecordCounts, work);
    }

    private CompletableFuture<Void> checkRebuildIndexes(@Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                                         @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                                         int oldFormatVersion, @Nonnull RecordMetaData metaData,
                                                         int oldMetaDataVersion, boolean rebuildRecordCounts,
                                                         List<CompletableFuture<Void>> work) {
        final boolean newStore = oldFormatVersion == 0;
        final Map<Index, List<RecordType>> indexes = metaData.getIndexesToBuildSince(oldMetaDataVersion);
        handleNoLongerUniqueIndex(metaData, work, indexes);
        if (!indexes.isEmpty()) {
            RecordType singleRecordTypeWithPrefixKey = singleRecordTypeWithPrefixKey(indexes);
            final AtomicLong recordCountRef = new AtomicLong(-1);
            final Supplier<CompletableFuture<Long>> lazyRecordCount = getAndRememberFutureLong(recordCountRef,
                    () -> store.getRecordCountForRebuildIndexes(newStore, rebuildRecordCounts, indexes, singleRecordTypeWithPrefixKey));
            AtomicLong recordsSizeRef = new AtomicLong(-1);
            final Supplier<CompletableFuture<Long>> lazyRecordsSize = getAndRememberFutureLong(recordsSizeRef,
                    () -> getRecordSizeForRebuildIndexes(singleRecordTypeWithPrefixKey));
            if (singleRecordTypeWithPrefixKey == null
                    && store.formatVersion.isAtLeast(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX)
                    && store.omitUnsplitRecordSuffix) {
                work.add(lazyRecordCount.get().thenAccept(recordCount -> {
                    if (recordCount == 0) {
                        if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
                            KeyValueLogMessage msg = KeyValueLogMessage.build("upgrading unsplit format on empty store",
                                    LogMessageKeys.NEW_FORMAT_VERSION, store.formatVersion,
                                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context));
                            if (newStore) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(msg.toString());
                                }
                            } else {
                                if (LOGGER.isInfoEnabled()) {
                                    LOGGER.info(msg.toString());
                                }
                            }
                        }
                        store.omitUnsplitRecordSuffix = !store.formatVersion.isAtLeast(FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX);
                        info.clearOmitUnsplitRecordSuffix();
                        store.addRecordsReadConflict();
                    }
                }));
            }

            Map<Index, CompletableFuture<IndexState>> newStates = getStatesForRebuildIndexes(
                    userVersionChecker, indexes, lazyRecordCount, lazyRecordsSize, newStore, oldMetaDataVersion, oldFormatVersion);
            return rebuildIndexes(indexes, newStates, work,
                    newStore ? FDBRecordStore.RebuildIndexReason.NEW_STORE : FDBRecordStore.RebuildIndexReason.FEW_RECORDS,
                    oldMetaDataVersion).thenRun(() -> {
                maybeLogIndexesNeedingRebuilding(newStates, recordCountRef, recordsSizeRef, rebuildRecordCounts, newStore);
                store.context.increment(FDBStoreTimer.Counts.INDEXES_NEED_REBUILDING, newStates.entrySet().size());
            });
        } else {
            return work.isEmpty() ? AsyncUtil.DONE : AsyncUtil.whenAll(work);
        }
    }

    private void handleNoLongerUniqueIndex(@Nonnull final RecordMetaData metaData,
                                            @Nonnull final List<CompletableFuture<Void>> work,
                                            @Nonnull final Map<Index, List<RecordType>> indexesToBuildSince) {
        for (Index index : metaData.getAllIndexes()) {
            if (!indexesToBuildSince.containsKey(index) &&
                    !index.isUnique()) {
                final IndexState indexState = getIndexState(index);
                if (indexState == IndexState.READABLE_UNIQUE_PENDING || indexState == IndexState.WRITE_ONLY) {
                    final CompletableFuture<Void> uniquenessFuture = AsyncUtil.getAll(store.getRecordContext().removeCommitChecks(
                            commitCheck -> {
                                if (commitCheck instanceof IndexUniquenessCommitCheck) {
                                    return ((IndexUniquenessCommitCheck)commitCheck).getIndexSubspace().equals(store.indexSubspace(index));
                                } else {
                                    return false;
                                }
                            },
                            err -> err instanceof RecordIndexUniquenessViolation))
                            .thenCompose(vignore -> store.getIndexMaintainer(index).clearUniquenessViolations());
                    if (indexState == IndexState.READABLE_UNIQUE_PENDING) {
                        work.add(uniquenessFuture
                                .thenCompose(vignore -> markIndexReadable(index, false))
                                .thenApply(vignore2 -> null));
                    } else {
                        work.add(uniquenessFuture);
                    }
                }
            }
        }
    }

    static Supplier<CompletableFuture<Long>> getAndRememberFutureLong(@Nonnull AtomicLong ref,
                                                                      @Nonnull Supplier<CompletableFuture<Long>> lazyFuture) {
        return Suppliers.memoize(() -> lazyFuture.get().whenComplete((val, err) -> {
            if (err == null) {
                ref.set(val);
            }
        }));
    }

    @Nonnull
    @SuppressWarnings({"PMD.EmptyCatchBlock", "PMD.CloseResource"})
    CompletableFuture<Long> getRecordCountForRebuildIndexesInternal(boolean newStore, boolean rebuildRecordCounts,
                                                             @Nonnull Map<Index, List<RecordType>> indexes,
                                                             @Nullable RecordType singleRecordTypeWithPrefixKey) {
        final IndexQueryabilityFilter indexQueryabilityFilter = index -> !indexes.containsKey(index);
        if (singleRecordTypeWithPrefixKey != null) {
            try {
                return store.getSnapshotRecordCountForRecordType(singleRecordTypeWithPrefixKey.getName(), indexQueryabilityFilter);
            } catch (RecordCoreException ex) {
                // No such index; have to use total record count.
            }
        }
        if (!rebuildRecordCounts) {
            try {
                return store.getSnapshotRecordCount(EmptyKeyExpression.EMPTY, Key.Evaluated.EMPTY, indexQueryabilityFilter);
            } catch (RecordCoreException ex) {
                // Probably this was from the lack of appropriate index on count; treat like rebuildRecordCounts = true.
            }
        }
        final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(1)
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();
        final ScanProperties scanProperties = new ScanProperties(executeProperties);
        final RecordCursor<FDBStoredRecord<Message>> records;
        if (singleRecordTypeWithPrefixKey == null) {
            records = store.scanRecords(null, scanProperties);
        } else {
            records = store.scanRecords(TupleRange.allOf(singleRecordTypeWithPrefixKey.getRecordTypeKeyTuple()), null, scanProperties);
        }
        return records.onNext().thenApply(result -> {
            if (result.hasNext()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(KeyValueLogMessage.of("version check scan found non-empty store",
                            store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
                }
                return Long.MAX_VALUE;
            } else {
                if (newStore ? LOGGER.isDebugEnabled() : LOGGER.isInfoEnabled()) {
                    KeyValueLogMessage msg = KeyValueLogMessage.build("version check scan found empty store",
                            store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context));
                    if (newStore) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(msg.toString());
                        }
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info(msg.toString());
                        }
                    }
                }
                return 0L;
            }
        });
    }

    @Nonnull
    private CompletableFuture<Long> getRecordSizeForRebuildIndexes(@Nullable RecordType singleRecordTypeWithPrefixKey) {
        if (singleRecordTypeWithPrefixKey == null) {
            return store.estimateRecordsSizeAsync();
        } else {
            return store.estimateRecordsSizeAsync(TupleRange.allOf(singleRecordTypeWithPrefixKey.getRecordTypeKeyTuple()));
        }
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    RecordType singleRecordTypeWithPrefixKey(@Nonnull Map<Index, List<RecordType>> indexes) {
        RecordType recordType = null;
        for (List<RecordType> entry : indexes.values()) {
            Collection<RecordType> types = entry != null ? entry : store.getRecordMetaData().getRecordTypes().values();
            if (types.size() != 1) {
                return null;
            }
            RecordType type1 = entry != null ? entry.get(0) : types.iterator().next();
            if (recordType == null) {
                if (!type1.primaryKeyHasRecordTypePrefix()) {
                    return null;
                }
                recordType = type1;
            } else if (type1 != recordType) {
                return null;
            }
        }
        return recordType;
    }

    @Nonnull
    Map<Index, CompletableFuture<IndexState>> getStatesForRebuildIndexes(
            @Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker,
            @Nonnull Map<Index, List<RecordType>> indexes,
            @Nonnull Supplier<CompletableFuture<Long>> lazyRecordCount,
            @Nonnull Supplier<CompletableFuture<Long>> lazyRecordsSize,
            boolean newStore,
            int oldMetaDataVersion,
            int oldFormatVersion) {
        Map<Index, CompletableFuture<IndexState>> newStates = new HashMap<>();
        for (Map.Entry<Index, List<RecordType>> entry : indexes.entrySet()) {
            Index index = entry.getKey();
            List<RecordType> recordTypes = entry.getValue();
            boolean indexOnNewRecordTypes = areAllRecordTypesSince(recordTypes, oldMetaDataVersion);
            CompletableFuture<IndexState> stateFuture = userVersionChecker == null ?
                    lazyRecordCount.get().thenApply(recordCount -> FDBRecordStore.disabledIfTooManyRecordsForRebuild(recordCount, indexOnNewRecordTypes)) :
                    userVersionChecker.needRebuildIndex(index, lazyRecordCount, lazyRecordsSize, indexOnNewRecordTypes);
            if (IndexTypes.VERSION.equals(index.getType())
                    && !newStore
                    && oldFormatVersion < FormatVersion.SAVE_VERSION_WITH_RECORD.getValueForSerialization()
                    && !store.useOldVersionFormat()) {
                stateFuture = stateFuture.thenApply(state -> {
                    if (IndexState.READABLE.equals(state)) {
                        return IndexState.DISABLED;
                    }
                    return state;
                });
            }
            newStates.put(index, stateFuture);
        }
        return newStates;
    }

    private void maybeLogIndexesNeedingRebuilding(@Nonnull Map<Index, CompletableFuture<IndexState>> newStates,
                                                   @Nonnull AtomicLong recordCountRef,
                                                   @Nonnull AtomicLong recordsSizeRef,
                                                   boolean rebuildRecordCounts,
                                                   boolean newStore) {
        if (LOGGER.isDebugEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build("indexes need rebuilding",
                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context));

            long recordCount = recordCountRef.get();
            if (recordCount >= 0L) {
                msg.addKeyAndValue(LogMessageKeys.RECORD_COUNT, recordCount == Long.MAX_VALUE ? "unknown" : Long.toString(recordCount));
            }
            long recordsSize = recordsSizeRef.get();
            if (recordsSize >= 0L) {
                msg.addKeyAndValue(LogMessageKeys.RECORDS_SIZE_ESTIMATE, Long.toString(recordsSize));
            }

            if (rebuildRecordCounts) {
                msg.addKeyAndValue(LogMessageKeys.REBUILD_RECORD_COUNTS, "true");
            }
            Map<String, List<String>> stateNames = new HashMap<>();
            for (Map.Entry<Index, CompletableFuture<IndexState>> stateEntry : newStates.entrySet()) {
                final String stateName;
                if (MoreAsyncUtil.isCompletedNormally(stateEntry.getValue())) {
                    stateName = stateEntry.getValue().join().getLogName();
                } else {
                    stateName = "UNKNOWN";
                }
                stateNames.compute(stateName, (key, names) -> {
                    if (names == null) {
                        names = new ArrayList<>();
                    }
                    names.add(stateEntry.getKey().getName());
                    return names;
                });
            }
            msg.addKeysAndValues(stateNames);
            if (newStore) {
                msg.addKeyAndValue(LogMessageKeys.NEW_STORE, "true");
            }
            LOGGER.debug(msg.toString());
        }
    }

    // endregion

    // region Record Count Rebuild

    @SuppressWarnings("PMD.CloseResource")
    boolean checkPossiblyRebuildRecordCounts(@Nonnull RecordMetaData metaData,
                                              @Nonnull RecordMetaDataProto.DataStoreInfo.Builder info,
                                              @Nonnull List<CompletableFuture<Void>> work,
                                              int oldFormatVersion) {
        boolean existingStore = oldFormatVersion > 0;
        KeyExpression countKeyExpression = metaData.getRecordCountKey();

        boolean rebuildRecordCounts =
                (existingStore && oldFormatVersion < FormatVersion.RECORD_COUNT_ADDED.getValueForSerialization())
                || (countKeyExpression != null && store.formatVersion.isAtLeast(FormatVersion.RECORD_COUNT_KEY_ADDED) &&
                        (!info.hasRecordCountKey() || !KeyExpression.fromProto(info.getRecordCountKey()).equals(countKeyExpression)))
                || (countKeyExpression == null && info.hasRecordCountKey());

        if (rebuildRecordCounts) {
            if (existingStore) {
                store.context.clear(store.getSubspace().range(Tuple.from(FDBRecordStore.RECORD_COUNT_KEY)));
            }

            if (store.formatVersion.isAtLeast(FormatVersion.RECORD_COUNT_KEY_ADDED)) {
                if (countKeyExpression != null) {
                    info.setRecordCountKey(countKeyExpression.toKeyExpression());
                } else {
                    info.clearRecordCountKey();
                }
            }

            if (existingStore) {
                addRebuildRecordCountsJob(work);
            }
        }
        return rebuildRecordCounts;
    }

    @SuppressWarnings("PMD.CloseResource")
    void addRebuildRecordCountsJob(List<CompletableFuture<Void>> work) {
        final KeyExpression recordCountKey = store.getRecordMetaData().getRecordCountKey();
        if (recordCountKey == null ||
                store.getRecordStoreState().getStoreHeader().getRecordCountState() == RecordMetaDataProto.DataStoreInfo.RecordCountState.DISABLED) {
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("recounting all records",
                    store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context)));
        }
        final Map<Key.Evaluated, Long> counts = new HashMap<>();
        final RecordCursor<FDBStoredRecord<Message>> records = store.scanRecords(null, ScanProperties.FORWARD_SCAN);
        CompletableFuture<Void> future = records.forEach(rec -> {
            Key.Evaluated subkey = recordCountKey.evaluateSingleton(rec);
            counts.compute(subkey, (k, v) -> (v == null) ? 1 : v + 1);
        }).thenApply(vignore -> {
            final Transaction tr = store.ensureContextActive();
            final byte[] bytes = new byte[8];
            final ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            for (Map.Entry<Key.Evaluated, Long> entry : counts.entrySet()) {
                buf.putLong(entry.getValue());
                tr.set(store.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_COUNT_KEY).addAll(entry.getKey().toTupleAppropriateList())),
                        bytes);
                buf.clear();
            }
            return null;
        });
        future = store.context.instrument(FDBStoreTimer.Events.RECOUNT_RECORDS, future);
        work.add(future);
    }

    @SuppressWarnings("PMD.CloseResource")
    void addConvertRecordVersions(@Nonnull List<CompletableFuture<Void>> work) {
        if (store.useOldVersionFormat()) {
            throw new RecordCoreException("attempted to convert record versions when still using older format");
        }
        final Subspace legacyVersionSubspace = store.getLegacyVersionSubspace();

        KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(legacyVersionSubspace)
                .setContext(store.getRecordContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build();
        CompletableFuture<Void> workFuture = kvCursor.forEach(kv -> {
            final Tuple primaryKey = legacyVersionSubspace.unpack(kv.getKey());
            final FDBRecordVersion version = FDBRecordVersion.fromBytes(kv.getValue(), false);
            final byte[] newKeyBytes = store.getSubspace().pack(store.recordVersionKey(primaryKey));
            final byte[] newValueBytes = SplitHelper.packVersion(version);
            store.ensureContextActive().set(newKeyBytes, newValueBytes);
        }).thenAccept(ignore -> store.ensureContextActive().clear(legacyVersionSubspace.range()));
        work.add(workFuture);
    }

    // endregion

    // region Helpers

    void logExceptionAsWarn(KeyValueLogMessage message, Throwable exception) {
        if (LOGGER.isWarnEnabled()) {
            for (Throwable ex = exception;
                    ex != null;
                    ex = ex.getCause()) {
                if (ex instanceof LoggableException) {
                    message.addKeysAndValues(((LoggableException)ex).getLogInfo());
                }
            }
            message.addKeyAndValue(store.subspaceProvider.logKey(), store.subspaceProvider.toString(store.context));
            LOGGER.warn(message.toString(), exception);
        }
    }

    // endregion
}
