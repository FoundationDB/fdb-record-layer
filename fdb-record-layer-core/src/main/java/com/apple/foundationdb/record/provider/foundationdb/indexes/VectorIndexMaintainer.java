/*
 * VectorIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.AsyncLockCursor;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.locking.LockIdentifier;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An index maintainer for a {@link com.apple.foundationdb.record.metadata.IndexTypes#VECTOR vector} index. The
 * maintainer is engine-neutral: it owns the continuation/cursor machinery and locking, delegates the prefix
 * skip-scan to {@link PrefixSkipScanHelper}, and translates engine results into {@link IndexEntry index entries},
 * while delegating the actual vector-structure work
 * (search, insert, delete) to a {@link VectorIndexEngine}. The engine — an
 * {@link com.apple.foundationdb.async.hnsw.HNSW HNSW} graph or a
 * {@link com.apple.foundationdb.async.guardiann.Guardiann Guardiann} clustered structure — is selected by the
 * {@link com.apple.foundationdb.record.metadata.IndexOptions#VECTOR_ENGINE} index option.
 */
@API(API.Status.EXPERIMENTAL)
public class VectorIndexMaintainer extends StandardIndexMaintainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexMaintainer.class);

    // Prefix for the per-index commit-check key that disables the index when a negative task count is observed. One
    // check per index name, so a merge that trips the invariant more than once in a transaction disables it just once.
    private static final String DISABLE_INDEX_COMMIT_HOOK = "disableVectorIndexOnNegativeTaskCount:";

    // Per-drain time budget used when the driver supplies none (IndexDeferredMaintenanceControl.getTimeQuotaMillis()
    // defaults to 0). Mirrors Lucene's agile-commit default and stays well under FoundationDB's ~5s transaction limit,
    // so a single drain commits before the transaction ages out rather than overrunning and being rolled back.
    private static final long DEFAULT_MERGE_TIME_QUOTA_MILLIS = 4000L;

    // A merge invocation examines at most this many outstanding prefixes before choosing one to claim, rather than
    // scanning every prefix to pick the "fairest" one — bounding the per-invocation read cost independent of the
    // partition count. A prefix beyond the window is reached on a later invocation: because a prefix only drains to
    // zero and drops out, the window slides forward until every outstanding prefix has fallen inside it.
    private static final int MAX_PREFIXES_EXAMINED = 16;

    @Nonnull
    private final VectorIndexEngine engine;

    public VectorIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.engine = VectorIndexEngine.fromIndex(state.index, state.store.indexSecondarySubspace(state.index));
    }

    @Nonnull
    private VectorIndexEngine getEngine() {
        return engine;
    }

    /**
     * Scan the vector index.
     * @param scanBounds the {@link VectorIndexScanBounds bounds} of the scan to perform
     * @param continuation any continuation from a previous scan invocation
     * @param scanProperties skip, limit and other properties of the scan
     * @return a {@link RecordCursor} of index entries
     */
    @Nonnull
    @Override
    @SuppressWarnings("resource")
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds, @Nullable final byte[] continuation,
                                         @Nonnull final ScanProperties scanProperties) {
        if (!scanBounds.getScanType().equals(IndexScanType.BY_DISTANCE)) {
            throw new RecordCoreException("Can only scan vector index by value.");
        }
        if (!(scanBounds instanceof final VectorIndexScanBounds vectorIndexScanBounds)) {
            throw new RecordCoreException("Need proper vector index scan bounds.");
        }

        final KeyWithValueExpression keyWithValueExpression = getKeyWithValueExpression(state.index.getRootExpression());
        final int prefixSize = keyWithValueExpression.getSplitPoint();

        final ExecuteProperties executeProperties = scanProperties.getExecuteProperties();
        final ScanProperties innerScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndLimit);
        final Subspace indexSubspace = getIndexSubspace();
        @Nullable final FDBStoreTimer timer = getTimer();

        //
        // If there is a {@code prefix > 0}, then we model the scan as a flatmap over the distinct prefixes as the outer
        // and the correlated per-partition vector search as the inner.
        //
        if (prefixSize > 0) {
            //
            // Skip-scan through the prefixes in a way that we only consider each distinct prefix. That skip scan
            // forms the outer of a join with an inner that searches the partition's vector structure for that prefix
            // using the query vector of the scan bounds.
            //
            return RecordCursor.flatMapPipelined(
                            PrefixSkipScanHelper.prefixSkipScan(state, prefixSize, timer,
                                    VectorIndexHelper.Events.VECTOR_SKIP_SCAN,
                                    vectorIndexScanBounds.getPrefixRange(), innerScanProperties),
                            (prefixTuple, innerContinuation) -> {
                                Verify.verify(prefixTuple.size() == prefixSize);
                                final Subspace partitionSubspace = indexSubspace.subspace(prefixTuple);

                                return scanSinglePartition(prefixTuple, innerContinuation, partitionSubspace,
                                        vectorIndexScanBounds, scanProperties);
                            },
                            continuation,
                            state.store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        } else {
            //
            // As {@code prefix == 0}, there only is exactly one prefix ({@code null}). While it is possible to also
            // just do a flatmap over some non-existing outer, it's probably more efficient to just do a plain scan
            // of the single partition here.
            //
            return scanSinglePartition(null, continuation,
                    indexSubspace, vectorIndexScanBounds, scanProperties)
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        }
    }

    /**
     * Scan one partition of the vector index, i.e. the one vector structure that holds the data for the partition
     * identified by {@code prefixTuple}.
     * @param prefixTuple the tuple identifying the partition
     * @param continuation the continuation for this scan or {@code null} if this is the first execution
     * @param partitionSubspace the subspace where the partition's vector structure resides
     * @param vectorIndexScanBounds the bounds for this scan
     * @param scanProperties the scan properties for this scan
     * @return a {@link RecordCursor} returning the index entries for this scan
     */
    @Nonnull
    @SuppressWarnings("resource")
    private RecordCursor<IndexEntry> scanSinglePartition(@Nullable final Tuple prefixTuple,
                                                         @Nullable final byte[] continuation,
                                                         @Nonnull final Subspace partitionSubspace,
                                                         @Nonnull final VectorIndexScanBounds vectorIndexScanBounds,
                                                         @Nonnull final ScanProperties scanProperties) {
        if (continuation != null) {
            final RecordCursorProto.VectorIndexScanContinuation parsedContinuation =
                    Continuation.fromBytes(continuation);
            final ImmutableList.Builder<IndexEntry> indexEntriesBuilder = ImmutableList.builder();
            for (int i = 0; i < parsedContinuation.getIndexEntriesCount(); i++) {
                final RecordCursorProto.VectorIndexScanContinuation.IndexEntry indexEntryProto =
                        parsedContinuation.getIndexEntries(i);
                indexEntriesBuilder.add(new IndexEntry(state.index,
                        Tuple.fromBytes(indexEntryProto.getKey().toByteArray()),
                        Tuple.fromBytes(indexEntryProto.getValue().toByteArray())));
            }
            final ImmutableList<IndexEntry> indexEntries = indexEntriesBuilder.build();
            return new ListCursor<>(indexEntries, parsedContinuation.getInnerContinuation().toByteArray())
                    .mapResult(result ->
                            result.withContinuation(new Continuation(indexEntries, result.getContinuation())));
        }

        final boolean snapshot = scanProperties.getExecuteProperties().getIsolationLevel().isSnapshot();
        return new LazyCursor<>(
                state.context.acquireReadLock(new LockIdentifier(partitionSubspace))
                        .thenApply(lock ->
                                new AsyncLockCursor<>(lock,
                                        new LazyCursor<>(
                                                kNearestNeighborSearch(prefixTuple, partitionSubspace, snapshot,
                                                        vectorIndexScanBounds),
                                                getExecutor()))),
                state.context.getExecutor());
    }

    @SuppressWarnings({"resource", "checkstyle:MethodName"})
    @Nonnull
    private CompletableFuture<RecordCursor<IndexEntry>>
            kNearestNeighborSearch(@Nullable final Tuple prefixTuple,
                                   @Nonnull final Subspace partitionSubspace,
                                   final boolean snapshot,
                                   @Nonnull final VectorIndexScanBounds vectorIndexScanBounds) {
        return getEngine().search(state.context, snapshot, partitionSubspace, vectorIndexScanBounds)
                .thenApply(resultEntries -> {
                    final ImmutableList.Builder<IndexEntry> nearestNeighborEntriesBuilder = ImmutableList.builder();
                    for (final ResultEntry nearestNeighbor : resultEntries) {
                        nearestNeighborEntriesBuilder.add(toIndexEntry(prefixTuple, nearestNeighbor));
                    }
                    final ImmutableList<IndexEntry> nearestNeighborsEntries = nearestNeighborEntriesBuilder.build();
                    return new ListCursor<>(getExecutor(), nearestNeighborsEntries, 0)
                            .mapResult(result -> {
                                final RecordCursorContinuation continuation = result.getContinuation();
                                if (continuation.isEnd()) {
                                    return result;
                                }
                                return result.withContinuation(new Continuation(nearestNeighborsEntries, continuation));
                            });
                });
    }

    @Nonnull
    private IndexEntry toIndexEntry(@Nullable final Tuple prefixTuple, @Nonnull final ResultEntry resultEntry) {
        final List<Object> keyItems = Lists.newArrayList();
        if (prefixTuple != null) {
            keyItems.addAll(prefixTuple.getItems());
        }
        keyItems.addAll(resultEntry.primaryKey().getItems());
        final List<Object> valueItems = Lists.newArrayList();
        final RealVector vector = resultEntry.vector();
        valueItems.add(vector == null ? null : vector.getRawData());
        return new IndexEntry(state.index, Tuple.fromList(keyItems),
                Tuple.fromList(valueItems));
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanType scanType, @Nonnull final TupleRange range,
                                         @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        throw new IllegalStateException("index maintainer does not support this scan api");
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        Verify.verify(indexEntries.size() == 1);
        final IndexEntry indexEntry = indexEntries.get(0);
        return updateIndexEntry(new IndexEntry(state.index, indexEntry.getKey(), indexEntry.getValue(),
                savedRecord.getPrimaryKey()), remove);
    }

    @Nonnull
    private CompletableFuture<Void> updateIndexEntry(@Nonnull final IndexEntry indexEntry, final boolean remove) {
        final KeyWithValueExpression keyWithValueExpression = getKeyWithValueExpression(state.index.getRootExpression());
        final int prefixSize = keyWithValueExpression.getColumnSize();
        final Subspace indexSubspace = getIndexSubspace();

        final byte[] vectorBytes = indexEntry.getValue().getBytes(0);
        if (vectorBytes == null) {
            //
            // If there is no vector (e.g. vector is NULL), we don't even need to index it.
            //
            return AsyncUtil.DONE;
        }

        final Tuple prefixKey = indexEntry.getKey();
        final Subspace partitionSubspace;
        if (prefixSize > 0) {
            partitionSubspace = indexSubspace.subspace(prefixKey);
        } else {
            partitionSubspace = indexSubspace;
        }
        final VectorIndexTaskCounts taskCounts = getEngine().getTaskCounts();
        final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
        final boolean maintainInTransaction = mergeControl.shouldAutoMergeDuringCommit();
        // Assemble the task-event registers this write should notify: the outstanding-work count register (when this
        // engine tracks counts) and, when the engine wants a caller-driven merge (Guardiann, not draining
        // in-transaction), a MaintenanceControlRegister that flags the index as needing a background merge on enqueue —
        // via the store's IndexDeferredMaintenanceControl, exactly as Lucene does. compose() collapses 0/1 registers.
        final ImmutableList.Builder<TaskEventRegister> registers = ImmutableList.builder();
        if (taskCounts != null) {
            registers.add(taskCounts.registerFor(prefixKey));
        }
        final MaintenanceControlRegister maintenanceControlRegister =
                getEngine().signalsMergeRequiredToCaller(maintainInTransaction)
                ? new MaintenanceControlRegister(mergeControl, state.index) : null;
        if (maintenanceControlRegister != null) {
            registers.add(maintenanceControlRegister);
        }
        final TaskEventRegister register = TaskEventRegister.compose(registers.build());
        final CompletableFuture<Void> writeFuture = state.context.doWithWriteLock(new LockIdentifier(partitionSubspace),
                () -> {
                    final List<Object> primaryKeyParts = Lists.newArrayList(indexEntry.getPrimaryKey().getItems());
                    state.index.trimPrimaryKey(primaryKeyParts);
                    final Tuple trimmedPrimaryKey = Tuple.fromList(primaryKeyParts);
                    final RealVector vector = RealVector.fromBytes(vectorBytes);
                    if (remove) {
                        return getEngine().delete(state.context, partitionSubspace, trimmedPrimaryKey, vector, register,
                                maintainInTransaction);
                    } else {
                        return getEngine().insert(state.context, partitionSubspace, trimmedPrimaryKey, vector, register,
                                maintainInTransaction);
                    }
                });
        if (maintenanceControlRegister == null) {
            // HNSW (does everything inline, tracks no work) or an update that drains in-transaction: nothing to signal.
            return writeFuture;
        }
        // Keep the merge-required signal self-healing. An update that enqueued a task has already flagged the index
        // (MaintenanceControlRegister fired on enqueue), so nothing more to do. An update that enqueued nothing has not —
        // yet a backlog left by an earlier transaction (or a signal that never led to a merge) must still get merged, so
        // re-raise the flag whenever a snapshot read finds outstanding work. The read is conflict-free and only has to
        // catch a committed backlog; the in-transaction enqueue case is handled by the register above.
        return writeFuture.thenCompose(ignore -> {
            if (maintenanceControlRegister.wasSignaled()) {
                return AsyncUtil.DONE;
            }
            return hasOutstandingWork().thenAccept(hasWork -> {
                if (hasWork) {
                    mergeControl.setMergeRequiredIndexes(state.index);
                }
            });
        });
    }

    @Override
    public boolean isPendingWriteQueueAllowed() {
        return true;
    }

    @Override
    @Nonnull
    public <M extends Message> Any serializePendingWriteQueue(@Nullable final FDBIndexableRecord<M> oldRecord,
                                                              @Nullable final FDBIndexableRecord<M> newRecord) {
        // Serialize the computed index entries rather than the whole record.
        // The maintenance filter is applied here (via filteredIndexEntries), so filtered-out entries are never
        // deferred onto the queue.
        final IndexBuildProto.OldAndNewIndexEntries.Builder builder = IndexBuildProto.OldAndNewIndexEntries.newBuilder();
        final List<IndexEntry> oldEntries = filteredIndexEntries(oldRecord);
        if (oldEntries != null) {
            Verify.verify(oldEntries.size() == 1);
            builder.addOldEntries(toProto(oldEntries.get(0), oldRecord.getPrimaryKey()));
        }
        final List<IndexEntry> newEntries = filteredIndexEntries(newRecord);
        if (newEntries != null) {
            Verify.verify(newEntries.size() == 1);
            builder.addNewEntries(toProto(newEntries.get(0), newRecord.getPrimaryKey()));
        }
        return Any.pack(builder.build());
    }

    @Override
    @Nonnull
    public CompletableFuture<Void> updateFromQueue(@Nonnull final Any data) {
        final IndexBuildProto.OldAndNewIndexEntries entries;
        try {
            entries = data.unpack(IndexBuildProto.OldAndNewIndexEntries.class);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("failed to parse vector index pending write queue entry data", ex);
        }
        CompletableFuture<Void> future = AsyncUtil.DONE;
        for (final IndexBuildProto.IndexEntry entry : entries.getOldEntriesList()) {
            future = future.thenCompose(ignore -> updateIndexEntry(fromProto(entry), true));
        }
        for (final IndexBuildProto.IndexEntry entry : entries.getNewEntriesList()) {
            future = future.thenCompose(ignore -> updateIndexEntry(fromProto(entry), false));
        }
        return future;
    }

    @Nonnull
    private IndexBuildProto.IndexEntry toProto(@Nonnull final IndexEntry entry, @Nonnull final Tuple primaryKey) {
        return IndexBuildProto.IndexEntry.newBuilder()
                .setKey(ByteString.copyFrom(entry.getKey().pack()))
                .setValue(ByteString.copyFrom(entry.getValue().pack()))
                .setPrimaryKey(ByteString.copyFrom(primaryKey.pack()))
                .build();
    }

    @Nonnull
    private IndexEntry fromProto(@Nonnull final IndexBuildProto.IndexEntry entry) {
        return new IndexEntry(state.index,
                Tuple.fromBytes(entry.getKey().toByteArray()),
                Tuple.fromBytes(entry.getValue().toByteArray()),
                Tuple.fromBytes(entry.getPrimaryKey().toByteArray()));
    }

    @Override
    public boolean canDeleteWhere(@Nonnull final QueryToKeyMatcher matcher, @Nonnull final Key.Evaluated evaluated) {
        if (!super.canDeleteWhere(matcher, evaluated)) {
            return false;
        }
        return evaluated.size() <= getKeyWithValueExpression(state.index.getRootExpression()).getColumnSize();
    }

    @Override
    public CompletableFuture<Void> deleteWhere(@Nonnull final Transaction tr, @Nonnull final Tuple prefix) {
        Verify.verify(getKeyWithValueExpression(state.index.getRootExpression()).getColumnSize() >= prefix.size());
        final VectorIndexTaskCounts taskCounts = getEngine().getTaskCounts();
        if (taskCounts != null) {
            // super.deleteWhere clears only the primary index subspace; drop the outstanding-work counts for the
            // group(s) being removed too (they live in the secondary subspace). Also clear any merge lease under the
            // prefix and write-conflict the delete-guard so a concurrent merge's blind lease acquire cannot orphan a
            // lease into the emptied group (see VectorIndexMergeLock.addDeleteWhereConflicts).
            taskCounts.clearPrefix(tr, prefix);
            VectorIndexMergeLock.addDeleteWhereConflicts(tr, state.store.indexSecondarySubspace(state.index), prefix);
        }
        return super.deleteWhere(tr, prefix);
    }

    /**
     * Merges this index by draining its deferred maintenance backlog, coordinating with other concurrent merges via
     * a per-partition lease so at most one merge drains a given prefix at a time — and, crucially, so a second merge
     * sees a claim (committed) and skips the prefix <em>before</em> doing the expensive drain, rather than racing into
     * it and rolling one back. HNSW and other inline engines defer no work and merge to a no-op.
     * <p>
     * Because claiming and draining must not share a transaction (the claim has to commit and become visible first),
     * each invocation does at most one of: <b>drain</b> a prefix this process already holds a lease on (re-verified
     * this invocation), or <b>claim</b> one free/stale prefix and return so the claim commits (the next invocation
     * drains it). Prefixes held live by another owner are skipped; when only those remain, the run stops and their
     * holders finish them (a crashed holder's lease expires for a future run to reclaim). Each invocation runs in its
     * own transaction and reports {@code mergesTried}/{@code mergesFound} so {@code IndexingMerger} re-invokes it (the
     * "scan again" step) until there is nothing left to claim or drain.
     *
     * @return a future that completes when this transaction's claim or drain has been staged
     */
    @Nonnull
    @Override
    @SuppressWarnings({"PMD.CloseResource", "resource"}) // async iterator is closed explicitly
    public CompletableFuture<Void> mergeIndex() {
        final VectorIndexTaskCounts taskCounts = getEngine().getTaskCounts();
        if (taskCounts == null) {
            return AsyncUtil.DONE;
        }
        final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
        // Record the step so IndexingMerger.handleFailure retries a transient failure with a smaller budget.
        mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
        final int taskBudget = mergeControl.getMergesLimit() > 0
                               ? (int)Math.min(mergeControl.getMergesLimit(), Integer.MAX_VALUE) : 1;
        final Subspace indexSubspace = getIndexSubspace();
        // The merge lease is keyed by a stable owner id so this process can re-verify and keep a prefix's lease across
        // the driver's re-invocations. IndexingMerger always sets it from the indexing session (the only production
        // path here), so a missing id means mergeIndex() was invoked outside that path — a programming error rather
        // than something to paper over with a throwaway id (which could not re-verify its own claim). Fail fast.
        final UUID ownerId = mergeControl.getMergeSessionId();
        if (ownerId == null) {
            throw new RecordCoreException("vector index merge requires a merge session id on the "
                    + "IndexDeferredMaintenanceControl; drive the merge through the OnlineIndexer / IndexingMerger");
        }
        final VectorIndexMergeLock lock =
                new VectorIndexMergeLock(state.store.indexSecondarySubspace(state.index), ownerId,
                        VectorIndexMergeLock.DEFAULT_LEASE_WINDOW_MILLIS, System::currentTimeMillis);

        final CloseableAsyncIterator<PrefixTaskCount> prefixes =
                taskCounts.prefixesWithOutstandingWork(state.context.readTransaction(true), getExecutor());

        // Examine at most MAX_PREFIXES_EXAMINED outstanding prefixes rather than every one. Prefer a prefix I already
        // hold a (committed) lease on — that is my drain target this invocation, so stop as soon as I find it.
        // Otherwise, collect the free/stale prefixes seen within the window and later claim one at random, so a
        // problematic prefix is not chosen on every invocation and the other prefixes still get worked. A prefix held
        // live by another owner is skipped. A process holds at most one prefix at a time, so a held prefix is finished
        // before a new one is claimed — hence we look for an owned prefix (within the window) before settling for a
        // free one; because a prefix only drains to zero and drops out, an owned prefix drifts toward the front and
        // stays inside the window across invocations.
        final AtomicReference<PrefixTaskCount> drainTarget = new AtomicReference<>();
        final List<Tuple> freeCandidates = Lists.newArrayList();
        final AtomicInteger examined = new AtomicInteger();
        return AsyncUtil.whileTrue(() -> prefixes.onHasNext().thenCompose(hasNext -> {
            if (!hasNext || examined.get() >= MAX_PREFIXES_EXAMINED) {
                return AsyncUtil.READY_FALSE;
            }
            final PrefixTaskCount prefixTaskCount = prefixes.next();
            examined.incrementAndGet();
            return lock.currentOwner(state.context, prefixTaskCount.prefix()).thenApply(owner -> {
                if (ownerId.equals(owner)) {
                    drainTarget.set(prefixTaskCount); // my prefix -> drain it; stop scanning
                    return false;
                }
                if (owner == null) {
                    freeCandidates.add(prefixTaskCount.prefix()); // free/stale -> a claim candidate
                }
                return true;
            });
        }), getExecutor()).whenComplete((ignore, err) -> {
            // Release the prefix scan's range read once scanning is done, on success or failure, before draining/claiming.
            prefixes.close();
        }).thenCompose(ignore -> {
            final PrefixTaskCount owned = drainTarget.get();
            if (owned != null) {
                return drainOwnedPrefix(lock, owned, indexSubspace, taskCounts, taskBudget, mergeControl);
            }
            if (!freeCandidates.isEmpty()) {
                // Claim a uniformly random candidate in THIS transaction and return so the claim commits before any
                // expensive drain; the next invocation re-verifies ownership and drains. Signal more-work so the
                // driver comes back.
                final Tuple candidate = freeCandidates.get(ThreadLocalRandom.current().nextInt(freeCandidates.size()));
                lock.acquire(state.context, candidate);
                reportProgress(mergeControl, 0, true);
                return AsyncUtil.DONE;
            }
            // Nothing this process can do: every examined prefix is held live by another owner (or none remain). Stop;
            // the holders finish theirs and a crashed holder's lease expires for a future run to reclaim. If free work
            // remains beyond the window, a later merge (re-triggered by the outstanding-work signal) picks it up.
            reportProgress(mergeControl, 0, false);
            return AsyncUtil.DONE;
        }).exceptionally(err -> {
            // A negative task count is an impossible, corrupt state (see NegativeTaskCountException) surfaced by either
            // the prefix discovery scan or the drain's countFor. Rather than fail the merge and have the driver retry
            // forever on bad accounting, disable the index (deferred to a commit hook, since that takes the store-state
            // write lock) and stop the driver by reporting no progress; complete normally so the transaction commits
            // and the disable takes effect. Any other failure propagates unchanged.
            if (FDBExceptions.isOrHasCause(err, VectorIndexTaskCounts.NegativeTaskCountException.class)) {
                disableIndexOnNegativeTaskCount();
                reportProgress(mergeControl, 0, false);
                return null;
            }
            throw err instanceof CompletionException ? (CompletionException)err : new CompletionException(err);
        });
    }

    /**
     * Drains a bounded batch from a partition this process already holds a lease on (re-verified this invocation),
     * refreshing the lease first so it does not expire mid-work, and releasing it once the partition's queue is empty.
     * Always signals more-work afterward: this partition may have leftover and other partitions may still need
     * claiming, so the driver runs again; a later invocation that finds nothing to do reports equal counts and stops.
     */
    @Nonnull
    private CompletableFuture<Void> drainOwnedPrefix(@Nonnull final VectorIndexMergeLock lock,
                                                     @Nonnull final PrefixTaskCount owned,
                                                     @Nonnull final Subspace indexSubspace,
                                                     @Nonnull final VectorIndexTaskCounts taskCounts,
                                                     final int taskBudget,
                                                     @Nonnull final IndexDeferredMaintenanceControl mergeControl) {
        final Tuple prefix = owned.prefix();
        lock.acquire(state.context, prefix); // refresh the lease timestamp so it stays live while we drain
        // Mirror updateIndexKeys: an unpartitioned index (empty prefix) lives directly in the index subspace.
        final Subspace partitionSubspace = prefix.isEmpty() ? indexSubspace : indexSubspace.subspace(prefix);
        final int requested = (int)Math.min(owned.count(), taskBudget);
        // Bound the drain by time as well as by count. The driver hands a per-invocation budget via the control's
        // timeQuotaMillis (IndexingMerger halves it on a transaction-too-old failure and lets it recover on success);
        // when it hands none, seed the default and write it back so that adaptive feedback has something to work with.
        // The engine always runs at least one task, so forward progress never stalls even under a tight budget.
        long timeQuotaMillis = mergeControl.getTimeQuotaMillis();
        if (timeQuotaMillis <= 0L) {
            timeQuotaMillis = DEFAULT_MERGE_TIME_QUOTA_MILLIS;
            mergeControl.setTimeQuotaMillis(timeQuotaMillis);
        }
        final long deadlineMillis = System.currentTimeMillis() + timeQuotaMillis;
        final TaskCountRegister register = taskCounts.registerFor(prefix);
        return getEngine().executeDeferredTasks(state.context, partitionSubspace, requested, register, deadlineMillis)
                .thenCompose(executedForPrefix -> {
                    // Release the lease only once the partition is truly empty (a read-your-writes-aware snapshot read
                    // reflects this drain's mutations, including any follow-up tasks it enqueued); otherwise keep it so
                    // we continue this same partition next invocation.
                    return taskCounts.countFor(state.context.readTransaction(true), prefix)
                            .thenCompose(remaining -> {
                                final CompletableFuture<Void> released =
                                        remaining <= 0L ? lock.release(state.context, prefix) : AsyncUtil.DONE;
                                return released.thenAccept(ignore -> reportProgress(mergeControl, executedForPrefix,
                                        true));
                            });
                });
    }

    /**
     * Reports the merge outcome to the driver: {@code mergesTried} is what we executed; {@code mergesFound} is one more
     * than that when there is (or may be) more to do, which is how {@code IndexingMerger} decides to re-invoke.
     */
    private static void reportProgress(@Nonnull final IndexDeferredMaintenanceControl mergeControl, final int executed,
                                       final boolean moreWork) {
        mergeControl.setMergesTried(executed);
        mergeControl.setMergesFound(moreWork ? executed + 1 : executed);
    }

    /**
     * Registers a pre-commit hook that disables this index because a deferred-task count decoded to a negative value —
     * a corrupt state that cannot occur while the counter stays coupled to the task space (see
     * {@link VectorIndexTaskCounts.NegativeTaskCountException}), so the maintenance bookkeeping can no longer be
     * trusted. Disabling is deferred to a commit check (mirroring {@code IndexingPendingWriteQueue})
     * because {@code markIndexDisabled} takes the record-store-state write lock, which cannot be acquired while the merge
     * runs under the state read lock. {@code markIndexDisabled} also clears the index data, so the planner refuses the
     * index until it is rebuilt. Keyed per index name so repeated trips within one transaction disable it just once.
     */
    private void disableIndexOnNegativeTaskCount() {
        state.store.getContext().getOrCreateCommitCheck(DISABLE_INDEX_COMMIT_HOOK + state.index.getName(),
                name -> () -> state.store.markIndexDisabled(state.index).thenAccept(changed -> {
                    if (Boolean.TRUE.equals(changed)) {
                        state.store.getContext()
                                .increment(FDBStoreTimer.Counts.VECTOR_INDEX_DISABLED_ON_NEGATIVE_TASK_COUNT);
                        if (LOGGER.isWarnEnabled()) {
                            LOGGER.warn(KeyValueLogMessage.of(
                                    "disabled vector index because a deferred-task count went negative",
                                    LogMessageKeys.INDEX_NAME, state.index.getName()));
                        }
                    } else if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn(KeyValueLogMessage.of(
                                "vector index deferred-task count went negative but the index was already disabled",
                                LogMessageKeys.INDEX_NAME, state.index.getName()));
                    }
                }));
    }

    /**
     * Whether this index has any outstanding deferred-maintenance work, via an O(1) snapshot read of the index-wide
     * total. Always {@code false} for engines that do not defer work (HNSW).
     * @return a future that is {@code true} iff some partition has outstanding tasks
     */
    @Nonnull
    CompletableFuture<Boolean> hasOutstandingWork() {
        final VectorIndexTaskCounts taskCounts = getEngine().getTaskCounts();
        if (taskCounts == null) {
            return AsyncUtil.READY_FALSE;
        }
        return taskCounts.hasOutstandingWork(state.context.readTransaction(true));
    }

    /**
     * Narrows the index's root key expression to the {@link KeyWithValueExpression} that every vector index is required
     * to have: the split point separates the partition prefix from the indexed vector column. The validator enforces
     * this shape at metadata time, so a failure here means an index slipped through with an unsupported structure.
     *
     * @param root the index's root key expression
     * @return the root as a {@link KeyWithValueExpression}
     * @throws RecordCoreException if the root is not a {@link KeyWithValueExpression}
     */
    @Nonnull
    private static KeyWithValueExpression getKeyWithValueExpression(@Nonnull final KeyExpression root) {
        if (root instanceof KeyWithValueExpression) {
            return (KeyWithValueExpression)root;
        }
        throw new RecordCoreException("structure of vector index is not supported");
    }

    private static final class Continuation implements RecordCursorContinuation {
        @Nonnull
        private final List<IndexEntry> indexEntries;
        @Nonnull
        private final RecordCursorContinuation innerContinuation;

        @Nullable
        private ByteString cachedByteString;
        @Nullable
        private byte[] cachedBytes;

        private Continuation(@Nonnull final List<IndexEntry> indexEntries,
                             @Nonnull final RecordCursorContinuation innerContinuation) {
            this.indexEntries = ImmutableList.copyOf(indexEntries);
            this.innerContinuation = innerContinuation;
        }

        @Nonnull
        public List<IndexEntry> getIndexEntries() {
            return indexEntries;
        }

        @Nonnull
        public RecordCursorContinuation getInnerContinuation() {
            return innerContinuation;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (isEnd()) {
                return ByteString.EMPTY;
            }

            if (cachedByteString == null) {
                final RecordCursorProto.VectorIndexScanContinuation.Builder builder =
                        RecordCursorProto.VectorIndexScanContinuation.newBuilder();
                for (final var indexEntry : getIndexEntries()) {
                    builder.addIndexEntries(RecordCursorProto.VectorIndexScanContinuation.IndexEntry.newBuilder()
                            .setKey(ByteString.copyFrom(indexEntry.getKey().pack()))
                            .setValue(ByteString.copyFrom(indexEntry.getValue().pack()))
                            .build());
                }

                cachedByteString = builder
                        .setInnerContinuation(Objects.requireNonNull(innerContinuation.toByteString()))
                        .build()
                        .toByteString();
            }
            return cachedByteString;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (isEnd()) {
                return null;
            }
            if (cachedBytes == null) {
                cachedBytes = toByteString().toByteArray();
            }
            return cachedBytes;
        }

        @Override
        public boolean isEnd() {
            return getInnerContinuation().isEnd();
        }

        @Nonnull
        private static RecordCursorProto.VectorIndexScanContinuation fromBytes(@Nonnull byte[] continuationBytes) {
            try {
                return RecordCursorProto.VectorIndexScanContinuation.parseFrom(continuationBytes);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("error parsing continuation", ex)
                        .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuationBytes));
            }
        }
    }
}
