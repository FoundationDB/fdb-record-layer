/*
 * RankIndexMaintainer.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.RTree;
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
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * An index maintainer for keeping a {@link com.apple.foundationdb.async.RTree}.
 */
@API(API.Status.EXPERIMENTAL)
public class MultidimensionalIndexMaintainer extends StandardIndexMaintainer {
    private final RTree.Config config;

    public MultidimensionalIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.config = RTreeIndexHelper.getConfig(state.index);
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
        final int columnSize = dimensionsKeyExpression.getColumnSize();

        final CursorLimitManager cursorLimitManager = new CursorLimitManager(state.context, scanProperties);

        final Function<byte[], RecordCursor<IndexEntry>> outerFunction;
        if (prefixSize > 0) {
            outerFunction = outerContinuation -> scan(mDScanBounds.getPrefixRange(), outerContinuation, scanProperties);
        } else {
            outerFunction = outerContinuation -> RecordCursor.fromFuture(CompletableFuture.completedFuture(null));
        }

        return RecordCursor.flatMapPipelined(outerFunction,
                (outerIndexEntry, innerContinuation) -> {
                    Subspace rtSubspace = getSecondarySubspace();
                    final Tuple prefixKeyPart;
                    if (outerIndexEntry != null) {
                        prefixKeyPart = outerIndexEntry.getKey();
                        Verify.verify(prefixKeyPart.size() == prefixSize);
                        rtSubspace = rtSubspace.subspace(prefixKeyPart);
                    } else {
                        prefixKeyPart = null;
                    }

                    final Continuation parsedContinuation = Continuation.fromBytes(innerContinuation);
                    final BigInteger lastHilbertValue =
                            parsedContinuation == null ? null : parsedContinuation.getLastHilbertValue();
                    final Tuple lastKey = parsedContinuation == null ? null : parsedContinuation.getLastKey();

                    final ExecuteProperties executeProperties = scanProperties.getExecuteProperties();
                    final FDBStoreTimer timer = Objects.requireNonNull(state.context.getTimer());
                    final RTree rTree = new RTree(rtSubspace, getExecutor(), config, RTree::newRandomNodeId,
                            new OnReadLimiter(cursorLimitManager, timer));
                    final ReadTransaction transaction = state.context.readTransaction(true);
                    final var dimensionRanges = mDScanBounds.getDimensionRanges();
                    final ItemSlotCursor itemSlotCursor = new ItemSlotCursor(getExecutor(),
                            rTree.scan(transaction, lastHilbertValue, lastKey, mbr -> rangesOverlapWithMbr(dimensionRanges, mbr)),
                            cursorLimitManager, timer);
                    return itemSlotCursor
                            .filter(itemSlot -> lastHilbertValue == null || lastKey == null ||
                                                itemSlot.compareHilbertValueAndKey(lastHilbertValue, lastKey) >= 0)
                            .filter(itemSlot -> rangesContainPosition(dimensionRanges, itemSlot.getPosition()))
                            .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit())
                            .map(itemSlot -> {
                                final List<Object> keyItems = Lists.newArrayListWithExpectedSize(columnSize);
                                if (prefixKeyPart != null) {
                                    keyItems.addAll(prefixKeyPart.getItems());
                                }
                                keyItems.addAll(itemSlot.getPosition().getCoordinates().getItems());
                                keyItems.addAll(itemSlot.getKey().getItems());
                                Verify.verify(keyItems.size() == columnSize);
                                return new IndexEntry(state.index, Tuple.fromList(keyItems), itemSlot.getValue());
                            });
                },
                continuation,
                state.store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD));
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanType scanType, @Nonnull final TupleRange range, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        throw new RecordCoreException("index maintainer does not support this scan api");
    }

    @Nonnull
    private DimensionsKeyExpression getDimensionsKeyExpression(@Nonnull final KeyExpression root) {
        if (root instanceof KeyWithValueExpression) {
            return (DimensionsKeyExpression)((KeyWithValueExpression)root).getInnerKey();
        }
        return (DimensionsKeyExpression)root;
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final DimensionsKeyExpression dimensionsKeyExpression = getDimensionsKeyExpression(state.index.getRootExpression());
        final int prefixSize = dimensionsKeyExpression.getPrefixSize();
        final int dimensionsSize = dimensionsKeyExpression.getDimensionsSize();
        final Subspace extraSubspace = getSecondarySubspace();
        final Map<Subspace, CompletableFuture<Void>> rankFutures = Maps.newHashMapWithExpectedSize(indexEntries.size());
        for (final IndexEntry indexEntry : indexEntries) {
            final var indexKeyItems = indexEntry.getKey().getItems();
            final Tuple prefixKey = Tuple.fromList(indexKeyItems.subList(0, prefixSize));
            // Maintain an ordinary B-tree index.
            updatePrimaryIndexForPrefix(savedRecord, remove, prefixKey);

            final Subspace rtSubspace;
            if (prefixSize > 0) {
                rtSubspace = extraSubspace.subspace(prefixKey);
            } else {
                rtSubspace = extraSubspace;
            }

            // It is unsafe to have two concurrent updates to the same R-tree, so ensure that at most
            // one update per prefix key is ongoing at any given time
            final Function<Void, CompletableFuture<Void>> futureSupplier =
                    vignore -> {
                        final RTree.Point point =
                                new RTree.Point(Tuple.fromList(indexKeyItems.subList(prefixSize, dimensionsSize)));
                        final BigInteger hilbertValue = RTreeIndexHelper.hilbertValue(point);

                        final List<Object> primaryKeyParts = Lists.newArrayList(savedRecord.getPrimaryKey().getItems());
                        state.index.trimPrimaryKey(primaryKeyParts);
                        final List<Object> itemKeyParts = Lists.newArrayList(indexKeyItems.subList(prefixSize + dimensionsSize, indexKeyItems.size()));
                        itemKeyParts.addAll(primaryKeyParts);
                        final Tuple itemKey = Tuple.fromList(itemKeyParts);
                        final RTree rTree = new RTree(rtSubspace, getExecutor(), config, RTree::newRandomNodeId, RTree.OnReadListener.NOOP);
                        if (remove) {
                            return rTree.delete(state.transaction,
                                    hilbertValue,
                                    itemKey);
                        } else {
                            return rTree.insertOrUpdate(state.transaction,
                                    point,
                                    hilbertValue,
                                    itemKey,
                                    indexEntry.getValue());
                        }
                    };
            final CompletableFuture<Void> existingFuture = rankFutures.get(rtSubspace);
            if (existingFuture == null) {
                rankFutures.put(rtSubspace, futureSupplier.apply(null));
            } else {
                rankFutures.put(rtSubspace, existingFuture.thenCompose(futureSupplier));
            }
        }
        return AsyncUtil.whenAll(rankFutures.values());
    }

    /**
     * Store a single key in the primary index.
     * @param <M> the message type of the record
     * @param savedRecord the record being indexed
     * @param remove <code>true</code> if removing from index
     * @param prefixKey the key for the prefix
     */
    private <M extends Message> void updatePrimaryIndexForPrefix(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                 final boolean remove,
                                                                 @Nonnull final Tuple prefixKey) {
        final long startTime = System.nanoTime();
        final byte[] keyBytes = state.indexSubspace.pack(prefixKey);
        final Tuple value = new Tuple();
        final byte[] valueBytes = value.pack();
        if (remove) {
            state.transaction.clear(keyBytes);
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.DELETE_INDEX_ENTRY, startTime);
                state.store.countKeyValue(FDBStoreTimer.Counts.DELETE_INDEX_KEY, FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES,
                        FDBStoreTimer.Counts.DELETE_INDEX_VALUE_BYTES, keyBytes, valueBytes);
            }
        } else {
            checkKeyValueSizes(savedRecord, prefixKey, value, keyBytes, valueBytes);
            state.transaction.set(keyBytes, valueBytes);
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.SAVE_INDEX_ENTRY, startTime);
                state.store.countKeyValue(FDBStoreTimer.Counts.SAVE_INDEX_KEY, FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES,
                        FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES, keyBytes, valueBytes);
            }
        }
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        return super.deleteWhere(tr, prefix).thenApply(v -> {
            final Subspace extraSubspace = getSecondarySubspace();
            final byte[] key = extraSubspace.pack(prefix);
            tr.clear(key, ByteArrayUtil.strinc(key));
            return v;
        });
    }
    
    private static boolean rangesOverlapWithMbr(@Nonnull final List<TupleRange> dimensionRanges, @Nonnull final RTree.Rectangle mbr) {
        Preconditions.checkArgument(mbr.getNumDimensions() == dimensionRanges.size());

        for (int d = 0; d < mbr.getNumDimensions(); d++) {
            final Tuple lowTuple = Tuple.from(mbr.getLow(d));
            final Tuple highTuple = Tuple.from(mbr.getHigh(d));

            final TupleRange dimensionRange = dimensionRanges.get(d);

            switch (dimensionRange.getLowEndpoint()) {
                case TREE_START:
                    break;
                case RANGE_INCLUSIVE:
                case RANGE_EXCLUSIVE:
                    final Tuple dimensionLow = Objects.requireNonNull(dimensionRange.getLow());
                    if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                            TupleHelpers.compare(highTuple, dimensionLow) < 0) {
                        return false;
                    }
                    if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                            TupleHelpers.compare(highTuple, dimensionLow) <= 0) {
                        return false;
                    }
                    break;
                case TREE_END:
                case CONTINUATION:
                case PREFIX_STRING:
                default:
                    throw new RecordCoreException("do not support endpoint " + dimensionRange.getLowEndpoint());
            }

            switch (dimensionRange.getHighEndpoint()) {
                case TREE_END:
                    break;
                case RANGE_INCLUSIVE:
                case RANGE_EXCLUSIVE:
                    final Tuple dimensionHigh = Objects.requireNonNull(dimensionRange.getHigh());
                    if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                            TupleHelpers.compare(lowTuple, dimensionHigh) > 0) {
                        return false;
                    }
                    if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                            TupleHelpers.compare(highTuple, dimensionHigh) >= 0) {
                        return false;
                    }
                    break;
                case TREE_START:
                case CONTINUATION:
                case PREFIX_STRING:
                default:
                    throw new RecordCoreException("do not support endpoint " + dimensionRange.getHighEndpoint());
            }
        }
        return true;
    }

    private static boolean rangesContainPosition(@Nonnull final List<TupleRange> dimensionRanges, @Nonnull final RTree.Point point) {
        Preconditions.checkArgument(point.getNumDimensions() == dimensionRanges.size());

        for (int d = 0; d < point.getNumDimensions(); d++) {
            final Tuple coordinate = Tuple.from(point.getCoordinate(d));

            final TupleRange dimensionRange = dimensionRanges.get(d);

            switch (dimensionRange.getLowEndpoint()) {
                case TREE_START:
                    break;
                case RANGE_INCLUSIVE:
                case RANGE_EXCLUSIVE:
                    final Tuple dimensionLow = Objects.requireNonNull(dimensionRange.getLow());
                    if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                            TupleHelpers.compare(coordinate, dimensionLow) < 0) {
                        return false;
                    }
                    if (dimensionRange.getLowEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                            TupleHelpers.compare(coordinate, dimensionLow) <= 0) {
                        return false;
                    }
                    break;
                case TREE_END:
                case CONTINUATION:
                case PREFIX_STRING:
                default:
                    throw new RecordCoreException("do not support endpoint " + dimensionRange.getLowEndpoint());
            }

            switch (dimensionRange.getHighEndpoint()) {
                case TREE_END:
                    break;
                case RANGE_INCLUSIVE:
                case RANGE_EXCLUSIVE:
                    final Tuple dimensionHigh = Objects.requireNonNull(dimensionRange.getHigh());
                    if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                            TupleHelpers.compare(coordinate, dimensionHigh) > 0) {
                        return false;
                    }
                    if (dimensionRange.getHighEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                            TupleHelpers.compare(coordinate, dimensionHigh) >= 0) {
                        return false;
                    }
                    break;
                case TREE_START:
                case CONTINUATION:
                case PREFIX_STRING:
                default:
                    throw new RecordCoreException("do not support endpoint " + dimensionRange.getHighEndpoint());
            }
        }
        return true;
    }

    static class OnReadLimiter implements RTree.OnReadListener {
        @Nonnull
        private final CursorLimitManager cursorLimitManager;
        @Nonnull
        private final FDBStoreTimer timer;

        public OnReadLimiter(@Nonnull final CursorLimitManager cursorLimitManager, @Nonnull final FDBStoreTimer timer) {
            this.cursorLimitManager = cursorLimitManager;
            this.timer = timer;
        }

        @Override
        public void onRead(@Nonnull final byte[] nodeId, @Nonnull final RTree.Kind nodeKind,
                           @Nonnull final List<KeyValue> keyValues) {
            final int accumulatedKeysSize =
                    keyValues.stream()
                            .mapToInt(keyValue -> keyValue.getKey().length)
                            .sum();

            final int accumulatedValuesSize =
                    keyValues.stream()
                            .mapToInt(keyValue -> keyValue.getValue().length)
                            .sum();
            cursorLimitManager.reportScannedBytes(accumulatedKeysSize + accumulatedValuesSize);
            cursorLimitManager.tryRecordScan();

            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, accumulatedKeysSize);
            timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, accumulatedValuesSize);
        }
    }

    static class ItemSlotCursor extends AsyncIteratorCursor<RTree.ItemSlot> {
        @Nonnull
        private final CursorLimitManager cursorLimitManager;
        @Nonnull
        private final FDBStoreTimer timer;

        public ItemSlotCursor(@Nonnull final Executor executor, @Nonnull final AsyncIterator<RTree.ItemSlot> iterator,
                              @Nonnull final CursorLimitManager cursorLimitManager, @Nonnull final FDBStoreTimer timer) {
            super(executor, iterator);
            this.cursorLimitManager = cursorLimitManager;
            this.timer = timer;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<RTree.ItemSlot>> onNext() {
            if (nextResult != null && !nextResult.hasNext()) {
                // This guard is needed to guarantee that if onNext is called multiple times after the cursor has
                // returned a result without a value, then the same NoNextReason is returned each time. Without this guard,
                // one might return SCAN_LIMIT_REACHED (for example) after returning a result with SOURCE_EXHAUSTED because
                // of the tryRecordScan check.
                return CompletableFuture.completedFuture(nextResult);
            } else if (cursorLimitManager.tryRecordScan()) {
                return iterator.onHasNext().thenApply(hasNext -> {
                    if (hasNext) {
                        final RTree.ItemSlot itemSlot = iterator.next();
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
                        Tuple.fromBytes(parsed.toByteArray()));
            } else {
                return null;
            }
        }
    }
}
