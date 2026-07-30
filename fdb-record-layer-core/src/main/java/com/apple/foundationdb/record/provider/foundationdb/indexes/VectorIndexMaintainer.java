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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
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
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.locking.LockIdentifier;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * An index maintainer for a {@link com.apple.foundationdb.record.metadata.IndexTypes#VECTOR vector} index. The
 * maintainer is engine-neutral: it owns the continuation/cursor machinery, the prefix skip-scan, locking, and the
 * translation of engine results into {@link IndexEntry index entries}, while delegating the actual vector-structure work
 * (search, insert, delete) to a {@link VectorIndexEngine}. The engine — an
 * {@link com.apple.foundationdb.async.hnsw.HNSW HNSW} graph or a
 * {@link com.apple.foundationdb.async.guardiann.Guardiann Guardiann} clustered structure — is selected by the
 * {@link com.apple.foundationdb.record.metadata.IndexOptions#VECTOR_ENGINE} index option.
 */
@API(API.Status.EXPERIMENTAL)
public class VectorIndexMaintainer extends StandardIndexMaintainer {
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
            return RecordCursor.flatMapPipelined(prefixSkipScan(prefixSize, timer, vectorIndexScanBounds, innerScanProperties),
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

    @Nonnull
    private Function<byte[], RecordCursor<Tuple>> prefixSkipScan(final int prefixSize,
                                                                 @Nullable final StoreTimer timer,
                                                                 @Nonnull final VectorIndexScanBounds vectorIndexScanBounds,
                                                                 @Nonnull final ScanProperties innerScanProperties) {
        Verify.verify(prefixSize > 0);
        return outerContinuation -> {
            final ChainedCursor<Tuple> chainedCursor = new ChainedCursor<>(state.context,
                    lastKeyOptional -> nextPrefixTuple(vectorIndexScanBounds.getPrefixRange(),
                            prefixSize, lastKeyOptional.orElse(null), innerScanProperties),
                    Tuple::pack,
                    Tuple::fromBytes,
                    outerContinuation,
                    innerScanProperties);
            return timer == null
                   ? chainedCursor
                   : timer.instrument(VectorIndexHelper.Events.VECTOR_SKIP_SCAN, chainedCursor);
        };
    }

    @SuppressWarnings({"resource", "PMD.CloseResource"})
    private CompletableFuture<Optional<Tuple>> nextPrefixTuple(@Nonnull final TupleRange prefixRange,
                                                               final int prefixSize,
                                                               @Nullable final Tuple lastPrefixTuple,
                                                               @Nonnull final ScanProperties scanProperties) {
        final Subspace indexSubspace = getIndexSubspace();
        final KeyValueCursor cursor;
        if (lastPrefixTuple == null) {
            cursor = KeyValueCursor.Builder.withSubspace(indexSubspace)
                    .setContext(state.context)
                    .setRange(prefixRange)
                    .setContinuation(null)
                    .setScanProperties(scanProperties.setStreamingMode(CursorStreamingMode.ITERATOR)
                            .with(innerExecuteProperties -> innerExecuteProperties.setReturnedRowLimit(1)))
                    .build();
        } else {
            KeyValueCursor.Builder builder = KeyValueCursor.Builder.withSubspace(indexSubspace)
                    .setContext(state.context)
                    .setContinuation(null)
                    .setScanProperties(scanProperties)
                    .setScanProperties(scanProperties.setStreamingMode(CursorStreamingMode.ITERATOR)
                            .with(innerExecuteProperties -> innerExecuteProperties.setReturnedRowLimit(1)));

            cursor = builder.setLow(indexSubspace.pack(lastPrefixTuple), EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(prefixRange.getHigh(), prefixRange.getHighEndpoint())
                    .build();
        }

        return cursor.onNext().thenApply(next -> {
            cursor.close();
            if (next.hasNext()) {
                final KeyValue kv = Objects.requireNonNull(next.get());
                return Optional.of(TupleHelpers.subTuple(indexSubspace.unpack(kv.getKey()), 0, prefixSize));
            }
            return Optional.empty();
        });
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        Verify.verify(indexEntries.size() == 1);
        final KeyWithValueExpression keyWithValueExpression = getKeyWithValueExpression(state.index.getRootExpression());
        final int prefixSize = keyWithValueExpression.getColumnSize();
        final Subspace indexSubspace = getIndexSubspace();
        final var indexEntry = indexEntries.get(0);

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
        @Nullable final TaskCountRegister register = taskCounts == null ? null : taskCounts.registerFor(prefixKey);
        return state.context.doWithWriteLock(new LockIdentifier(partitionSubspace), () -> {
            final List<Object> primaryKeyParts = Lists.newArrayList(savedRecord.getPrimaryKey().getItems());
            state.index.trimPrimaryKey(primaryKeyParts);
            final Tuple trimmedPrimaryKey = Tuple.fromList(primaryKeyParts);
            final RealVector vector = RealVector.fromBytes(vectorBytes);
            if (remove) {
                return getEngine().delete(state.context, partitionSubspace, trimmedPrimaryKey, vector, register);
            } else {
                return getEngine().insert(state.context, partitionSubspace, trimmedPrimaryKey, vector, register);
            }
        });
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
            // group(s) being removed too (they live in the secondary subspace).
            taskCounts.clearPrefix(tr, prefix);
        }
        return super.deleteWhere(tr, prefix);
    }

    /**
     * Merges this index by paying down its deferred maintenance backlog. Guardiann performs its structural maintenance
     * (split/merge/reassign/collapse) as tasks that inserts and deletes only nibble at, so the {@code OnlineIndexer}
     * merge path drains the rest here. Discovery is a single snapshot read of the outstanding-work counter (no read
     * conflicts); only the partitions that actually have queued tasks are visited.
     * <p>
     * Each invocation runs in its own transaction and drains at most {@code mergeControl.getMergesLimit()} tasks (one
     * when the limit is "unlimited", so a single transaction stays small). It then reports {@code mergesTried} and
     * {@code mergesFound} to the {@link IndexDeferredMaintenanceControl} so {@code IndexingMerger} re-invokes it until
     * every queue is drained. Engines that do all their work inline (HNSW) have no task counter and merge to a no-op —
     * this must not throw, since the merge driver calls it for every target index.
     *
     * @return a future that completes when this transaction's slice of the backlog has been drained
     */
    @Nonnull
    @Override
    public CompletableFuture<Void> mergeIndex() {
        final VectorIndexTaskCounts taskCounts = getEngine().getTaskCounts();
        if (taskCounts == null) {
            return AsyncUtil.DONE;
        }
        final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
        // Record the step so IndexingMerger.handleFailure retries a transient failure with a smaller budget rather than
        // treating it as fatal.
        mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
        // The driver's per-transaction budget. A limit of 0 means "unlimited" elsewhere; here that becomes a single
        // task, so one merge transaction stays small and the driver keeps pumping.
        final int taskBudget = mergeControl.getMergesLimit() > 0
                               ? (int)Math.min(mergeControl.getMergesLimit(), Integer.MAX_VALUE)
                               : 1;
        final Subspace indexSubspace = getIndexSubspace();
        final AsyncIterator<PrefixTaskCount> prefixes =
                taskCounts.prefixesWithOutstandingWork(state.context.readTransaction(true), getExecutor());

        final AtomicInteger remaining = new AtomicInteger(taskBudget);
        final AtomicInteger executed = new AtomicInteger();
        final AtomicBoolean moreWork = new AtomicBoolean(false);
        return AsyncUtil.whileTrue(() -> prefixes.onHasNext().thenCompose(hasNext -> {
            if (!hasNext) {
                return AsyncUtil.READY_FALSE;
            }
            final PrefixTaskCount prefixTaskCount = prefixes.next();
            if (remaining.get() <= 0) {
                // Budget spent but at least one more partition still has work: tell the driver to run again.
                moreWork.set(true);
                return AsyncUtil.READY_FALSE;
            }
            final Tuple prefix = prefixTaskCount.prefix();
            // Mirror updateIndexKeys: an unpartitioned index (empty prefix) lives directly in the index subspace.
            final Subspace partitionSubspace = prefix.isEmpty() ? indexSubspace : indexSubspace.subspace(prefix);
            // prefixTaskCount.count() is the EXPECTED number of queued tasks for this partition; ask for at most the
            // remaining budget of them.
            final int requested = (int)Math.min(prefixTaskCount.count(), remaining.get());
            final TaskCountRegister register = taskCounts.registerFor(prefix);
            // No in-context write lock: the merge runs in its own dedicated transaction, so nothing else in this
            // context races the drain. Cross-transaction races are handled by FDB conflict detection on the task queue.
            return getEngine().executeDeferredTasks(state.context, partitionSubspace, requested, register)
                    .thenApply(executedForPrefix -> {
                        // Enqueue and execute keep the counter in lock-step with the queue, so the engine must run
                        // exactly the expected number of tasks. Verify that invariant rather than trusting it blindly.
                        Verify.verify(executedForPrefix == requested,
                                "vector merge expected to run %s deferred task(s) but ran %s", requested,
                                executedForPrefix);
                        executed.addAndGet(requested);
                        remaining.addAndGet(-requested);
                        if (requested < prefixTaskCount.count()) {
                            // A full budget-limited batch with more still queued for this partition — run again.
                            moreWork.set(true);
                        }
                        return true;
                    });
        }), getExecutor()).thenApply(ignore -> {
            // Drive the IndexingMerger loop: it re-invokes mergeIndex while mergesFound > mergesTried, so report one
            // extra "found" whenever work remains and equal counts once every queue is drained.
            mergeControl.setMergesTried(executed.get());
            mergeControl.setMergesFound(moreWork.get() ? executed.get() + 1 : executed.get());
            return null;
        });
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
