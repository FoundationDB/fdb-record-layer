/*
 * MultidimensionalIndexMaintainer.java
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
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.rtree.ChildSlot;
import com.apple.foundationdb.async.rtree.ItemSlot;
import com.apple.foundationdb.async.rtree.Node;
import com.apple.foundationdb.async.rtree.NodeHelpers;
import com.apple.foundationdb.async.rtree.OnReadListener;
import com.apple.foundationdb.async.rtree.OnWriteListener;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.async.rtree.RTreeHilbertCurveHelpers;
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
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.AsyncIteratorCursor;
import com.apple.foundationdb.record.cursors.AsyncLockCursor;
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.locking.LockIdentifier;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An index maintainer for keeping a {@link RTree}.
 */
@API(API.Status.EXPERIMENTAL)
public class MultidimensionalIndexMaintainer extends StandardIndexMaintainer {
    private static final byte nodeSlotIndexSubspaceIndicator = 0x00;
    @Nonnull
    private final RTree.Config config;

    public MultidimensionalIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.config = MultiDimensionalIndexHelper.getConfig(state.index);
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds, @Nullable final byte[] continuation,
                                         @Nonnull final ScanProperties scanProperties) {
        if (!scanBounds.getScanType().equals(IndexScanType.BY_VALUE)) {
            throw new RecordCoreException("Can only scan multidimensional index by value.");
        }
        if (!(scanBounds instanceof MultidimensionalIndexScanBounds)) {
            throw new RecordCoreException("Need proper multidimensional index scan bounds.");
        }
        final MultidimensionalIndexScanBounds mDScanBounds = (MultidimensionalIndexScanBounds)scanBounds;

        final DimensionsKeyExpression dimensionsKeyExpression = getDimensionsKeyExpression(state.index.getRootExpression());
        final int prefixSize = dimensionsKeyExpression.getPrefixSize();

        final ExecuteProperties executeProperties = scanProperties.getExecuteProperties();
        final ScanProperties innerScanProperties = scanProperties.with(ExecuteProperties::clearSkipAndLimit);
        final CursorLimitManager cursorLimitManager = new CursorLimitManager(state.context, innerScanProperties);
        final Subspace indexSubspace = getIndexSubspace();
        final Subspace nodeSlotIndexSubspace = getNodeSlotIndexSubspace();
        final FDBStoreTimer timer = Objects.requireNonNull(state.context.getTimer());

        //
        // Skip-scan through the prefixes in a way that we only consider each distinct prefix. That skip scan
        // forms the outer of a join with an inner that searches the R-tree for that prefix using the
        // spatial predicates of the scan bounds.
        //
        return RecordCursor.flatMapPipelined(prefixSkipScan(prefixSize, timer, mDScanBounds, innerScanProperties),
                        (prefixTuple, innerContinuation) -> {
                            final Subspace rtSubspace;
                            final Subspace rtNodeSlotIndexSubspace;
                            if (prefixTuple != null) {
                                Verify.verify(prefixTuple.size() == prefixSize);
                                rtSubspace = indexSubspace.subspace(prefixTuple);
                                rtNodeSlotIndexSubspace = nodeSlotIndexSubspace.subspace(prefixTuple);
                            } else {
                                rtSubspace = indexSubspace;
                                rtNodeSlotIndexSubspace = nodeSlotIndexSubspace;
                            }

                            final Continuation parsedContinuation = Continuation.fromBytes(innerContinuation);
                            final BigInteger lastHilbertValue =
                                    parsedContinuation == null ? null : parsedContinuation.getLastHilbertValue();
                            final Tuple lastKey = parsedContinuation == null ? null : parsedContinuation.getLastKey();

                            final RTree rTree = new RTree(rtSubspace, rtNodeSlotIndexSubspace, getExecutor(), config,
                                    RTreeHilbertCurveHelpers::hilbertValue, NodeHelpers::newRandomNodeId,
                                    OnWriteListener.NOOP, new OnRead(cursorLimitManager, timer));
                            final ReadTransaction transaction = state.context.readTransaction(true);
                            return new LazyCursor<>(state.context.acquireReadLock(new LockIdentifier(rtSubspace))
                                    .thenApply(lock -> new AsyncLockCursor<>(lock, new ItemSlotCursor(getExecutor(),
                                            rTree.scan(transaction, lastHilbertValue, lastKey,
                                                    mDScanBounds::overlapsMbrApproximately,
                                                    (low, high) -> mDScanBounds.getSuffixRange().overlaps(low, high)),
                                            cursorLimitManager, timer))), state.context.getExecutor())
                                    .filter(itemSlot -> lastHilbertValue == null || lastKey == null ||
                                                        itemSlot.compareHilbertValueAndKey(lastHilbertValue, lastKey) > 0)
                                    .filter(itemSlot -> mDScanBounds.containsPosition(itemSlot.getPosition()))
                                    .filter(itemSlot -> mDScanBounds.getSuffixRange().contains(itemSlot.getKeySuffix()))
                                    .map(itemSlot -> {
                                        final List<Object> keyItems = Lists.newArrayList();
                                        if (prefixTuple != null) {
                                            keyItems.addAll(prefixTuple.getItems());
                                        }
                                        keyItems.addAll(itemSlot.getPosition().getCoordinates().getItems());
                                        keyItems.addAll(itemSlot.getKeySuffix().getItems());
                                        return new IndexEntry(state.index, Tuple.fromList(keyItems), itemSlot.getValue());
                                    });
                        },
                        continuation,
                        state.store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanType scanType, @Nonnull final TupleRange range,
                                         @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        throw new RecordCoreException("index maintainer does not support this scan api");
    }

    @Nonnull
    private Function<byte[], RecordCursor<Tuple>> prefixSkipScan(final int prefixSize,
                                                                 @Nonnull final StoreTimer timer,
                                                                 @Nonnull final MultidimensionalIndexScanBounds mDScanBounds,
                                                                 @Nonnull final ScanProperties innerScanProperties) {
        final Function<byte[], RecordCursor<Tuple>> outerFunction;
        if (prefixSize > 0) {
            outerFunction = outerContinuation -> timer.instrument(MultiDimensionalIndexHelper.Events.MULTIDIMENSIONAL_SKIP_SCAN,
                    new ChainedCursor<>(state.context,
                            lastKeyOptional -> nextPrefixTuple(mDScanBounds.getPrefixRange(),
                                    prefixSize, lastKeyOptional.orElse(null), innerScanProperties),
                            Tuple::pack,
                            Tuple::fromBytes,
                            outerContinuation,
                            innerScanProperties));
        } else {
            outerFunction = outerContinuation -> RecordCursor.fromFuture(CompletableFuture.completedFuture(null));
        }
        return outerFunction;
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
        final DimensionsKeyExpression dimensionsKeyExpression = getDimensionsKeyExpression(state.index.getRootExpression());
        final int prefixSize = dimensionsKeyExpression.getPrefixSize();
        final int dimensionsSize = dimensionsKeyExpression.getDimensionsSize();
        final Subspace indexSubspace = getIndexSubspace();
        final Subspace nodeSlotIndexSubspace = getNodeSlotIndexSubspace();
        final var futures = indexEntries.stream().map(indexEntry -> {
            final var indexKeyItems = indexEntry.getKey().getItems();
            final Tuple prefixKey = Tuple.fromList(indexKeyItems.subList(0, prefixSize));

            final Subspace rtSubspace;
            final Subspace rtNodeSlotIndexSubspace;
            if (prefixSize > 0) {
                rtSubspace = indexSubspace.subspace(prefixKey);
                rtNodeSlotIndexSubspace = nodeSlotIndexSubspace.subspace(prefixKey);
            } else {
                rtSubspace = indexSubspace;
                rtNodeSlotIndexSubspace = nodeSlotIndexSubspace;
            }
            return state.context.doWithWriteLock(new LockIdentifier(rtSubspace), () -> {
                final RTree.Point point =
                        validatePoint(new RTree.Point(Tuple.fromList(indexKeyItems.subList(prefixSize, prefixSize + dimensionsSize))));

                final List<Object> primaryKeyParts = Lists.newArrayList(savedRecord.getPrimaryKey().getItems());
                state.index.trimPrimaryKey(primaryKeyParts);
                final List<Object> keySuffixParts =
                        Lists.newArrayList(indexKeyItems.subList(prefixSize + dimensionsSize, indexKeyItems.size()));
                keySuffixParts.addAll(primaryKeyParts);
                final Tuple keySuffix = Tuple.fromList(keySuffixParts);
                final FDBStoreTimer timer = Objects.requireNonNull(getTimer());
                final RTree rTree = new RTree(rtSubspace, rtNodeSlotIndexSubspace, getExecutor(), config,
                        RTreeHilbertCurveHelpers::hilbertValue, NodeHelpers::newRandomNodeId, new OnWrite(timer),
                        OnReadListener.NOOP);
                if (remove) {
                    return rTree.delete(state.transaction, point, keySuffix);
                } else {
                    return rTree.insertOrUpdate(state.transaction,
                            point,
                            keySuffix,
                            indexEntry.getValue());
                }
            });
        }).collect(Collectors.toList());
        return AsyncUtil.whenAll(futures);
    }

    @Override
    public boolean canDeleteWhere(@Nonnull final QueryToKeyMatcher matcher, @Nonnull final Key.Evaluated evaluated) {
        if (!super.canDeleteWhere(matcher, evaluated)) {
            return false;
        }
        return evaluated.size() <= getDimensionsKeyExpression(state.index.getRootExpression()).getPrefixSize();
    }

    @Override
    public CompletableFuture<Void> deleteWhere(@Nonnull final Transaction tr, @Nonnull final Tuple prefix) {
        Verify.verify(getDimensionsKeyExpression(state.index.getRootExpression()).getPrefixSize() >= prefix.size());
        return super.deleteWhere(tr, prefix).thenApply(v -> {
            // NOTE: Range.startsWith(), Subspace.range() and so on cover keys *strictly* within the range, but we sometimes
            // store data at the prefix key itself.
            final Subspace nodeSlotIndexSubspace = getNodeSlotIndexSubspace();
            final byte[] key = nodeSlotIndexSubspace.pack(prefix);
            state.context.clear(new Range(key, ByteArrayUtil.strinc(key)));
            return v;
        });
    }

    @Nonnull
    private Subspace getNodeSlotIndexSubspace() {
        return getSecondarySubspace().subspace(Tuple.from(nodeSlotIndexSubspaceIndicator));
    }

    /**
     * Traverse from the root of a key expression of a multidimensional index to the {@link DimensionsKeyExpression}.
     * @param root the root {@link KeyExpression} of the index definition
     * @return a {@link DimensionsKeyExpression}
     */
    @Nonnull
    public static DimensionsKeyExpression getDimensionsKeyExpression(@Nonnull final KeyExpression root) {
        if (root instanceof KeyWithValueExpression) {
            KeyExpression innerKey = ((KeyWithValueExpression)root).getInnerKey();
            while (innerKey instanceof ThenKeyExpression) {
                innerKey = ((ThenKeyExpression)innerKey).getChildren().get(0);
            }
            if (innerKey instanceof DimensionsKeyExpression) {
                return (DimensionsKeyExpression)innerKey;
            }
            throw new RecordCoreException("structure of multidimensional index is not supported");
        }
        return (DimensionsKeyExpression)root;
    }

    @Nonnull
    private static RTree.Point validatePoint(@Nonnull RTree.Point point) {
        for (int d = 0; d < point.getNumDimensions(); d ++) {
            Object coordinate = point.getCoordinate(d);
            Preconditions.checkArgument(coordinate == null || coordinate instanceof Long,
                    "dimension coordinates must be of type long");
        }
        return point;
    }

    static class OnRead implements OnReadListener {
        @Nonnull
        private final CursorLimitManager cursorLimitManager;
        @Nonnull
        private final FDBStoreTimer timer;

        public OnRead(@Nonnull final CursorLimitManager cursorLimitManager,
                      @Nonnull final FDBStoreTimer timer) {
            this.cursorLimitManager = cursorLimitManager;
            this.timer = timer;
        }

        @Override
        public <T extends Node> CompletableFuture<T> onAsyncRead(@Nonnull final CompletableFuture<T> future) {
            return timer.instrument(MultiDimensionalIndexHelper.Events.MULTIDIMENSIONAL_SCAN, future);
        }

        @Override
        public void onNodeRead(@Nonnull final Node node) {
            switch (node.getKind()) {
                case LEAF:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_LEAF_NODE_READS);
                    break;
                case INTERMEDIATE:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_INTERMEDIATE_NODE_READS);
                    break;
                default:
                    throw new RecordCoreException("unsupported kind of node");
            }
        }

        @Override
        public void onKeyValueRead(@Nonnull final Node node, @Nullable final byte[] key, @Nullable final byte[] value) {
            final int keyLength = key == null ? 0 : key.length;
            final int valueLength = value == null ? 0 : value.length;

            final int totalLength = keyLength + valueLength;
            cursorLimitManager.reportScannedBytes(totalLength);
            cursorLimitManager.tryRecordScan();
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, keyLength);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, valueLength);

            switch (node.getKind()) {
                case LEAF:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_LEAF_NODE_READ_BYTES, totalLength);
                    break;
                case INTERMEDIATE:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_INTERMEDIATE_NODE_READ_BYTES, totalLength);
                    break;
                default:
                    throw new RecordCoreException("unsupported kind of node");
            }
        }

        @Override
        public void onChildNodeDiscard(@Nonnull final ChildSlot childSlot) {
            timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_CHILD_NODE_DISCARDS);
        }
    }

    static class OnWrite implements OnWriteListener {
        @Nonnull
        private final FDBStoreTimer timer;

        public OnWrite(@Nonnull final FDBStoreTimer timer) {
            this.timer = timer;
        }

        @Override
        public <T extends Node> CompletableFuture<T> onAsyncReadForWrite(@Nonnull final CompletableFuture<T> future) {
            return timer.instrument(MultiDimensionalIndexHelper.Events.MULTIDIMENSIONAL_MODIFICATION, future);
        }

        @Override
        public void onNodeWritten(@Nonnull final Node node) {
            switch (node.getKind()) {
                case LEAF:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_LEAF_NODE_WRITES);
                    break;
                case INTERMEDIATE:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_INTERMEDIATE_NODE_WRITES);
                    break;
                default:
                    throw new RecordCoreException("unsupported kind of node");
            }
        }

        @Override
        public void onKeyValueWritten(@Nonnull final Node node, @Nullable final byte[] key, @Nullable final byte[] value) {
            final int keyLength = key == null ? 0 : key.length;
            final int valueLength = value == null ? 0 : value.length;

            final int totalLength = keyLength + valueLength;
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES, keyLength);
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES, valueLength);

            switch (node.getKind()) {
                case LEAF:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_LEAF_NODE_WRITE_BYTES, totalLength);
                    break;
                case INTERMEDIATE:
                    timer.increment(FDBStoreTimer.Counts.MULTIDIMENSIONAL_INTERMEDIATE_NODE_WRITE_BYTES, totalLength);
                    break;
                default:
                    throw new RecordCoreException("unsupported kind of node");
            }
        }
    }

    static class ItemSlotCursor extends AsyncIteratorCursor<ItemSlot> {
        @Nonnull
        private final CursorLimitManager cursorLimitManager;
        @Nonnull
        private final FDBStoreTimer timer;

        public ItemSlotCursor(@Nonnull final Executor executor, @Nonnull final AsyncIterator<ItemSlot> iterator,
                              @Nonnull final CursorLimitManager cursorLimitManager, @Nonnull final FDBStoreTimer timer) {
            super(executor, iterator);
            this.cursorLimitManager = cursorLimitManager;
            this.timer = timer;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<ItemSlot>> onNext() {
            if (nextResult != null && !nextResult.hasNext()) {
                // This guard is needed to guarantee that if onNext is called multiple times after the cursor has
                // returned a result without a value, then the same NoNextReason is returned each time. Without this guard,
                // one might return SCAN_LIMIT_REACHED (for example) after returning a result with SOURCE_EXHAUSTED because
                // of the tryRecordScan check.
                return CompletableFuture.completedFuture(nextResult);
            } else if (cursorLimitManager.tryRecordScan()) {
                return iterator.onHasNext().thenApply(hasNext -> {
                    if (hasNext) {
                        final ItemSlot itemSlot = iterator.next();
                        timer.increment(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY);
                        timer.increment(FDBStoreTimer.Counts.LOAD_KEY_VALUE);
                        valuesSeen++;
                        nextResult = RecordCursorResult.withNextValue(itemSlot, new Continuation(itemSlot.getHilbertValue(), itemSlot.getKey()));
                    } else {
                        // Source iterator is exhausted.
                        nextResult = RecordCursorResult.exhausted();
                    }
                    return nextResult;
                });
            } else { // a limit must have been exceeded
                final Optional<NoNextReason> stoppedReason = cursorLimitManager.getStoppedReason();
                if (stoppedReason.isEmpty()) {
                    throw new RecordCoreException("limit manager stopped cursor but did not report a reason");
                }
                Verify.verifyNotNull(nextResult, "should have seen at least one record");
                nextResult = RecordCursorResult.withoutNextValue(nextResult.getContinuation(), stoppedReason.get());
                return CompletableFuture.completedFuture(nextResult);
            }
        }
    }

    private static class Continuation implements RecordCursorContinuation {
        @Nullable
        final BigInteger lastHilbertValue;
        @Nullable
        final Tuple lastKey;
        @Nullable
        private ByteString cachedByteString;
        @Nullable
        private byte[] cachedBytes;

        private Continuation(@Nullable final BigInteger lastHilbertValue, @Nullable final Tuple lastKey) {
            this.lastHilbertValue = lastHilbertValue;
            this.lastKey = lastKey;
        }

        @Nullable
        public BigInteger getLastHilbertValue() {
            return lastHilbertValue;
        }

        @Nullable
        public Tuple getLastKey() {
            return lastKey;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (isEnd()) {
                return ByteString.EMPTY;
            }

            if (cachedByteString == null) {
                cachedByteString = RecordCursorProto.MultidimensionalIndexScanContinuation.newBuilder()
                        .setLastHilbertValue(ByteString.copyFrom(Objects.requireNonNull(lastHilbertValue).toByteArray()))
                        .setLastKey(ByteString.copyFrom(Objects.requireNonNull(lastKey).pack()))
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
            return lastHilbertValue == null || lastKey == null;
        }

        @Nullable
        private static Continuation fromBytes(@Nullable byte[] continuationBytes) {
            if (continuationBytes != null) {
                final RecordCursorProto.MultidimensionalIndexScanContinuation parsed;
                try {
                    parsed = RecordCursorProto.MultidimensionalIndexScanContinuation.parseFrom(continuationBytes);
                } catch (InvalidProtocolBufferException ex) {
                    throw new RecordCoreException("error parsing continuation", ex)
                            .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuationBytes));
                }
                return new Continuation(new BigInteger(parsed.getLastHilbertValue().toByteArray()),
                        Tuple.fromBytes(parsed.getLastKey().toByteArray()));
            } else {
                return null;
            }
        }
    }
}
