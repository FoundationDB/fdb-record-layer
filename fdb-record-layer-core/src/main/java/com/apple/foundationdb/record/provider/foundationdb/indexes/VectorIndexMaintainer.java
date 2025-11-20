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
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.hnsw.Config;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.async.hnsw.Node;
import com.apple.foundationdb.async.hnsw.NodeReference;
import com.apple.foundationdb.async.hnsw.OnReadListener;
import com.apple.foundationdb.async.hnsw.OnWriteListener;
import com.apple.foundationdb.async.hnsw.ResultEntry;
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
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
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
import java.util.function.Function;

/**
 * An index maintainer for keeping an {@link HNSW}.
 */
@API(API.Status.EXPERIMENTAL)
public class VectorIndexMaintainer extends StandardIndexMaintainer {
    @Nonnull
    private final Config config;

    public VectorIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.config = VectorIndexHelper.getConfig(state.index);
    }

    @Nonnull
    public Config getConfig() {
        return config;
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
        if (!(scanBounds instanceof VectorIndexScanBounds)) {
            throw new RecordCoreException("Need proper vector index scan bounds.");
        }
        final VectorIndexScanBounds vectorIndexScanBounds = (VectorIndexScanBounds)scanBounds;

        final KeyWithValueExpression keyWithValueExpression = getKeyWithValueExpression(state.index.getRootExpression());
        final int prefixSize = keyWithValueExpression.getSplitPoint();

        final ExecuteProperties executeProperties = scanProperties.getExecuteProperties();
        final ScanProperties innerScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndLimit);
        final Subspace indexSubspace = getIndexSubspace();
        final FDBStoreTimer timer = Objects.requireNonNull(state.context.getTimer());

        //
        // If there is a {@code prefix > 0}, then we model the scan as a flatmap over the distinct prefixes as the outer
        // and the correlated HNSW search as the inner.
        //
        if (prefixSize > 0) {
            //
            // Skip-scan through the prefixes in a way that we only consider each distinct prefix. That skip scan
            // forms the outer of a join with an inner that searches the R-tree for that prefix using the
            // spatial predicates of the scan bounds.
            //
            return RecordCursor.flatMapPipelined(prefixSkipScan(prefixSize, timer, vectorIndexScanBounds, innerScanProperties),
                            (prefixTuple, innerContinuation) -> {
                                Verify.verify(prefixTuple.size() == prefixSize);
                                final Subspace hnswSubspace = indexSubspace.subspace(prefixTuple);

                                return scanSinglePartition(prefixTuple, innerContinuation, hnswSubspace,
                                        timer, vectorIndexScanBounds);
                            },
                            continuation,
                            state.store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        } else {
            //
            // As {@code prefix == 0}, there only is exactly one prefix ({@code null}). While it is possible to also
            // just do a flatmap over some non-existing outer, it's probably more efficient to just do a plain scan
            // of the HNSW here.
            //
            return scanSinglePartition(null, continuation, indexSubspace, timer, vectorIndexScanBounds)
                    .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
        }
    }

    /**
     * Scan one partition of the vector index, i.e. the one HNSW that holds the data for the partition identified
     * by {@code prefixTuple}.
     * @param prefixTuple the tuple identifying the partition
     * @param continuation the continuation for this scan or {@code null} if this is the first execution
     * @param hnswSubspace the subspace where the HNSW resides.
     * @param timer the times
     * @param vectorIndexScanBounds the bounds for this scan.
     * @return a {@link RecordCursor} returning the index entries for this scan.
     */
    @Nonnull
    @SuppressWarnings("resource")
    private RecordCursor<IndexEntry> scanSinglePartition(@Nullable final Tuple prefixTuple,
                                                         @Nullable final byte[] continuation,
                                                         @Nonnull final Subspace hnswSubspace,
                                                         @Nonnull final FDBStoreTimer timer,
                                                         @Nonnull final VectorIndexScanBounds vectorIndexScanBounds) {
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

        final HNSW hnsw = new HNSW(hnswSubspace, getExecutor(), getConfig(),
                OnWriteListener.NOOP, new OnRead(timer));
        final ReadTransaction transaction = state.context.readTransaction(false);
        return new LazyCursor<>(
                state.context.acquireReadLock(new LockIdentifier(hnswSubspace))
                        .thenApply(lock ->
                                new AsyncLockCursor<>(lock,
                                        new LazyCursor<>(
                                                kNearestNeighborSearch(prefixTuple, hnsw, transaction,
                                                        vectorIndexScanBounds),
                                                getExecutor()))),
                state.context.getExecutor());
    }

    @SuppressWarnings({"resource", "checkstyle:MethodName"})
    @Nonnull
    private CompletableFuture<RecordCursor<IndexEntry>>
            kNearestNeighborSearch(@Nullable final Tuple prefixTuple,
                                   @Nonnull final HNSW hnsw,
                                   @Nonnull final ReadTransaction transaction,
                                   @Nonnull final VectorIndexScanBounds vectorIndexScanBounds) {
        return hnsw.kNearestNeighborsSearch(transaction, vectorIndexScanBounds.getAdjustedLimit(),
                        efSearch(vectorIndexScanBounds), returnVectors(hnsw.getConfig(), vectorIndexScanBounds),
                        Objects.requireNonNull(vectorIndexScanBounds.getQueryVector()))
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
        keyItems.addAll(resultEntry.getPrimaryKey().getItems());
        final List<Object> valueItems = Lists.newArrayList();
        final RealVector vector = resultEntry.getVector();
        valueItems.add(vector == null ? null : resultEntry.getVector().getRawData());
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
                                                                 @Nonnull final StoreTimer timer,
                                                                 @Nonnull final VectorIndexScanBounds vectorIndexScanBounds,
                                                                 @Nonnull final ScanProperties innerScanProperties) {
        Verify.verify(prefixSize > 0);
        return outerContinuation -> timer.instrument(MultiDimensionalIndexHelper.Events.MULTIDIMENSIONAL_SKIP_SCAN,
                new ChainedCursor<>(state.context,
                        lastKeyOptional -> nextPrefixTuple(vectorIndexScanBounds.getPrefixRange(),
                                prefixSize, lastKeyOptional.orElse(null), innerScanProperties),
                        Tuple::pack,
                        Tuple::fromBytes,
                        outerContinuation,
                        innerScanProperties));
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
        final Subspace rtSubspace;
        if (prefixSize > 0) {
            rtSubspace = indexSubspace.subspace(prefixKey);
        } else {
            rtSubspace = indexSubspace;
        }
        return state.context.doWithWriteLock(new LockIdentifier(rtSubspace), () -> {
            final List<Object> primaryKeyParts = Lists.newArrayList(savedRecord.getPrimaryKey().getItems());
            state.index.trimPrimaryKey(primaryKeyParts);
            final Tuple trimmedPrimaryKey = Tuple.fromList(primaryKeyParts);
            final FDBStoreTimer timer = Objects.requireNonNull(getTimer());
            final HNSW hnsw =
                    new HNSW(rtSubspace, getExecutor(), getConfig(), new OnWrite(timer), OnReadListener.NOOP);
            if (remove) {
                throw new UnsupportedOperationException("not implemented");
            } else {
                return hnsw.insert(state.transaction, trimmedPrimaryKey,
                        RealVector.fromBytes(vectorBytes));
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
        return super.deleteWhere(tr, prefix);
    }

    /**
     * TODO.
     */
    @Nonnull
    private static KeyWithValueExpression getKeyWithValueExpression(@Nonnull final KeyExpression root) {
        if (root instanceof KeyWithValueExpression) {
            return (KeyWithValueExpression)root;
        }
        throw new RecordCoreException("structure of vector index is not supported");
    }

    private int efSearch(@Nonnull final VectorIndexScanBounds scanBounds) {
        final VectorIndexScanOptions scanOptions = scanBounds.getVectorIndexScanOptions();
        final Integer efSearchOptionValue = scanOptions.getOption(VectorIndexScanOptions.HNSW_EF_SEARCH);
        if (efSearchOptionValue != null) {
            return efSearchOptionValue;
        }
        final var k = scanBounds.getAdjustedLimit();
        return Math.min(Math.max(4 * k, 64), Math.max(k, 400));
    }

    private boolean returnVectors(@Nonnull final Config config, @Nonnull final VectorIndexScanBounds scanBounds) {
        final VectorIndexScanOptions scanOptions = scanBounds.getVectorIndexScanOptions();
        final Boolean returnVectorsValue = scanOptions.getOption(VectorIndexScanOptions.HNSW_RETURN_VECTORS);
        if (returnVectorsValue != null) {
            return returnVectorsValue;
        }

        //
        // If we use RaBitQ, the vectors returned must be reconstructed which means we potentially wasted computation
        // resources if the user didn't explicitly ask for it. If RaBitQ is not used, the vectors returned are identical
        // to their inserted counterparts. We also already fetched them, so returning them is free.
        //
        return !config.isUseRaBitQ();
    }

    static class OnRead implements OnReadListener {
        @Nonnull
        private final FDBStoreTimer timer;

        public OnRead(@Nonnull final FDBStoreTimer timer) {
            this.timer = timer;
        }

        @Override
        public <N extends NodeReference, T extends Node<N>> CompletableFuture<T> onAsyncRead(@Nonnull CompletableFuture<T> future) {
            return timer.instrument(VectorIndexHelper.Events.VECTOR_SCAN, future);
        }

        @Override
        public void onNodeRead(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_READS);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_READS);
            }
        }

        @Override
        public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
            final int keyLength = key.length;
            final int valueLength = value == null ? 0 : value.length;

            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, keyLength);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, valueLength);

            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_READ_BYTES);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_READ_BYTES);
            }
        }
    }

    static class OnWrite implements OnWriteListener {
        @Nonnull
        private final FDBStoreTimer timer;

        public OnWrite(@Nonnull final FDBStoreTimer timer) {
            this.timer = timer;
        }

        @Override
        public void onNodeWritten(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_WRITES);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_WRITES);
            }
        }

        @Override
        public void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
            final int keyLength = key.length;
            final int valueLength = value.length;

            final int totalLength = keyLength + valueLength;
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES, keyLength);
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES, valueLength);

            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_WRITE_BYTES, totalLength);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_WRITE_BYTES, totalLength);
            }
        }
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
                            .setValue(ByteString.copyFrom(indexEntry.getKey().pack()))
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
