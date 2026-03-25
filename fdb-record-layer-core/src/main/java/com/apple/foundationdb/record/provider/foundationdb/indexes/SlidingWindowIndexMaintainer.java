/*
 * SlidingWindowIndexMaintainer.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.SlidingWindowKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An index maintainer that keeps only the top-N records based on a globally-scoped window key.
 * This extends {@link StandardIndexMaintainer} and overrides {@link #updateIndexKeys} to enforce
 * the sliding window, while delegating scan and other operations to the standard implementation.
 *
 * <p>The tertiary subspace is partitioned into two sub-subspaces to avoid key collisions:</p>
 * <ul>
 *     <li>Data subspace {@code (0)}: {@code [window_key_values..., primary_key...] → [primary_key packed]}</li>
 *     <li>Meta subspace {@code (1)}: {@code ["count"] → long}</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class SlidingWindowIndexMaintainer extends StandardIndexMaintainer {

    protected enum Type {
        MIN(Comparator.naturalOrder()),
        MAX(Comparator.reverseOrder()),
        ;

        @Nonnull
        private final Comparator<Tuple> valueComparator;

        Type(@Nonnull Comparator<Tuple> valueComparator) {
            this.valueComparator = valueComparator;
        }

        public boolean shouldEvict(@Nonnull Tuple oldValue, @Nonnull Tuple newValue) {
            return valueComparator.compare(oldValue, newValue) > 0;
        }

        /**
         * Read the eviction candidate from the data subspace: a single entry scan.
         * MAX keeps highest values, so the worst is the lowest (forward scan).
         * MIN keeps lowest values, so the worst is the highest (reverse scan).
         */
        @Nonnull
        public CompletableFuture<KeyValue> getEvictionCandidate(@Nonnull Subspace dataSubspace,
                                                                 @Nonnull Transaction tr) {
            final boolean reverse = this == MIN;
            return tr.getRange(dataSubspace.range(), 1, reverse)
                    .asList()
                    .thenApply(entries -> entries.isEmpty() ? null : entries.get(0));
        }
    }


    private static final Tuple DATA_SUBSPACE_KEY = Tuple.from(0);
    private static final Tuple META_SUBSPACE_KEY = Tuple.from(1);
    private static final Tuple COUNT_KEY = Tuple.from("count");

    private final Type extremumType;

    private final int windowSize;
    @Nonnull
    private final KeyExpression windowKey;
    private final int windowKeyColumnSize;

    public SlidingWindowIndexMaintainer(@Nonnull IndexMaintainerState state) {
        super(state);
        final SlidingWindowKeyExpression rootExpression = (SlidingWindowKeyExpression) state.index.getRootExpression();
        this.windowKey = rootExpression.getWindowKey();
        this.windowKeyColumnSize = windowKey.getColumnSize();
        this.windowSize = getWindowSize(state.index);
        this.extremumType = getExtremumType(state.index);
    }

    protected static int getWindowSize(@Nonnull Index index) {
        String windowSizeOption = index.getOption(IndexOptions.SLIDING_WINDOW_SIZE_OPTION);
        if (windowSizeOption == null) {
            throw new MetaDataException("sliding window size not specified",
                    LogMessageKeys.INDEX_NAME, index.getName());
        }
        int size = Integer.parseInt(windowSizeOption);
        if (size <= 0) {
            throw new MetaDataException("sliding window size must be positive",
                    LogMessageKeys.INDEX_NAME, index.getName());
        }
        return size;
    }

    @Nonnull
    protected static Type getExtremumType(@Nonnull Index index) {
        String orderOption = index.getOption(IndexOptions.SLIDING_WINDOW_ORDER_OPTION);
        if (orderOption == null) {
            return Type.MIN;
        }
        switch (orderOption.toUpperCase()) {
            case "MIN":
                return Type.MIN;
            case "MAX":
                return Type.MAX;
            default:
                throw new MetaDataException("invalid sliding window order: " + orderOption,
                        LogMessageKeys.INDEX_NAME, index.getName());
        }
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        if (!scanType.equals(IndexScanType.BY_VALUE)) {
            throw new RecordCoreException("Can only scan sliding window index by value.");
        }
        return scan(range, continuation, scanProperties);
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        if (remove) {
            return handleDelete(savedRecord, indexEntries);
        } else {
            return handleInsert(savedRecord, indexEntries);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private <M extends Message> CompletableFuture<Void> handleInsert(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                      @Nonnull final List<IndexEntry> indexEntries) {
        final Subspace tertiarySubspace = getTertiarySubspace();
        final Subspace dataSubspace = tertiarySubspace.subspace(DATA_SUBSPACE_KEY);
        final Subspace metaSubspace = tertiarySubspace.subspace(META_SUBSPACE_KEY);
        final Transaction tr = state.store.ensureContextActive();
        final Tuple primaryKey = savedRecord.getPrimaryKey();

        // Evaluate window key
        final Key.Evaluated windowEval = windowKey.evaluateSingleton(savedRecord);
        final Tuple windowValue = windowEval.toTuple();

        // Read counter
        final byte[] counterKey = metaSubspace.pack(COUNT_KEY);
        return tr.get(counterKey).thenCompose(counterBytes -> {
            final long count = counterBytes == null ? 0L : decodeLong(counterBytes);

            if (count < windowSize) {
                // Window not full yet: insert into primary index and window subspace
                return super.updateIndexKeys(savedRecord, false, indexEntries).thenApply(vignore -> {
                    writeWindowEntry(tr, dataSubspace, windowValue, primaryKey);
                    tr.set(counterKey, encodeLong(count + 1));
                    return null;
                });
            } else {
                // Window is full: read the eviction candidate (single entry, O(1))
                return extremumType.getEvictionCandidate(dataSubspace, tr).thenCompose(worstKV -> {
                    if (worstKV == null) {
                        // Should not happen if count >= windowSize, but handle gracefully
                        return super.updateIndexKeys(savedRecord, false, indexEntries).thenApply(vignore -> {
                            writeWindowEntry(tr, dataSubspace, windowValue, primaryKey);
                            tr.set(counterKey, encodeLong(count + 1));
                            return null;
                        });
                    }

                    final Tuple worstEntryKey = dataSubspace.unpack(worstKV.getKey());
                    final Tuple worstWindowTuple = TupleHelpers.subTuple(worstEntryKey, 0, windowKeyColumnSize);
                    final Tuple newWindowTuple = windowValue;

                    if (!extremumType.shouldEvict(worstWindowTuple, newWindowTuple)) {
                        // New record is not better than the worst in the window: skip
                        return AsyncUtil.DONE;
                    }

                    // New record is "better" than the worst: evict worst, insert new
                    final Tuple worstPrimaryKey = Tuple.fromBytes(worstKV.getValue());
                    tr.clear(worstKV.getKey());

                    return state.store.loadRecordAsync(worstPrimaryKey).thenCompose(oldRecord -> {
                        CompletableFuture<Void> removeFuture;
                        if (oldRecord != null) {
                            List<IndexEntry> oldEntries = evaluateIndex(oldRecord);
                            if (oldEntries != null && !oldEntries.isEmpty()) {
                                removeFuture = super.updateIndexKeys(oldRecord, true, oldEntries);
                            } else {
                                removeFuture = AsyncUtil.DONE;
                            }
                        } else {
                            removeFuture = AsyncUtil.DONE;
                        }

                        return removeFuture.thenCompose(vignore -> {
                            return super.updateIndexKeys(savedRecord, false, indexEntries).thenApply(vignore2 -> {
                                writeWindowEntry(tr, dataSubspace, windowValue, primaryKey);
                                return null;
                            });
                        });
                    });
                });
            }
        });
    }

    private void writeWindowEntry(@Nonnull Transaction tr, @Nonnull Subspace dataSubspace,
                                  @Nonnull Tuple windowValue, @Nonnull Tuple primaryKey) {
        final Tuple windowEntryKey = windowValue.addAll(primaryKey);
        tr.set(dataSubspace.pack(windowEntryKey), primaryKey.pack());
    }

    @SuppressWarnings("PMD.CloseResource")
    @Nonnull
    private <M extends Message> CompletableFuture<Void> handleDelete(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                      @Nonnull final List<IndexEntry> indexEntries) {
        final Subspace tertiarySubspace = getTertiarySubspace();
        final Subspace dataSubspace = tertiarySubspace.subspace(DATA_SUBSPACE_KEY);
        final Subspace metaSubspace = tertiarySubspace.subspace(META_SUBSPACE_KEY);
        final Transaction tr = state.store.ensureContextActive();
        final Tuple primaryKey = savedRecord.getPrimaryKey();

        // Evaluate window key
        final Key.Evaluated windowEval = windowKey.evaluateSingleton(savedRecord);
        final Tuple windowValue = windowEval.toTuple();

        // Check if this record is in the data subspace
        final Tuple windowEntryKey = windowValue.addAll(primaryKey);
        final byte[] packedWindowKey = dataSubspace.pack(windowEntryKey);

        return tr.get(packedWindowKey).thenCompose(existingValue -> {
            if (existingValue == null) {
                // Record was never in the window, no-op
                return AsyncUtil.DONE;
            }
            // Remove from data subspace
            tr.clear(packedWindowKey);

            // Decrement counter
            final byte[] counterKey = metaSubspace.pack(COUNT_KEY);
            return tr.get(counterKey).thenCompose(counterBytes -> {
                final long count = counterBytes == null ? 0L : decodeLong(counterBytes);
                tr.set(counterKey, encodeLong(Math.max(0, count - 1)));

                // Remove from primary index via super
                return super.updateIndexKeys(savedRecord, true, indexEntries);
            });
        });
    }

    @Override
    public boolean isIdempotent() {
        return false;
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        return super.deleteWhere(tr, prefix).thenApply(v -> {
            final Subspace windowSubspace = getTertiarySubspace();
            state.context.clear(windowSubspace.subspace(prefix).range());
            return v;
        });
    }

    private static byte[] encodeLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    private static long decodeLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
