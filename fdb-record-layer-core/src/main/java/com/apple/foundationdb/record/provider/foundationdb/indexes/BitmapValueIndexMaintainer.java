/*
 * BitmapValueIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexFunctionHelper;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An index maintainer for storing bitmaps of which records meet a specific condition.
 *
 * <p>
 * The index is defined on a position field, grouped by zero or more value conditions.
 * The position should be an integer that is unique and reasonably dense, since compression of the index is traded off in favor of simplicity and concurrent updates.
 * If the position is not set, then the item is not indexed at all.
 * </p>
 *
 * <p>
 * Instead of one key per record, the index stores one bit per record in a fixed-size bitmap aligned on even multiples of the position key.
 * Empty (that is, all zero) bitmaps are not stored.
 * </p>
 *
 * <p>
 * The position bitmaps behave as aggregate functions, additionally restricted by ranges of the position, as though position (or its modulus)
 * were another grouping column. It is possible to roll-up the stored bitmaps into a single, bigger, bitmap. But because these are rather large,
 * it is generally preferable to scan the index and deal with the bitmaps in order as stored. If the position
 * endpoints are not aligned to the bitmap quantum, the first and last bitmaps will be suitably trimmed.
 * </p>
 *
 * <p>
 * A {@code unique} option means that the index <em>checks</em> uniqueness of the position, with the expense of an additional
 * read at update time. Also, when a uniqueness violation does occur, it is not possible to know what other record caused it.
 * A separate unique index on the position field, if it is not the primary key, is therefore generally preferable.
 * </p>
 *
 */
@API(API.Status.EXPERIMENTAL)
public class BitmapValueIndexMaintainer extends StandardIndexMaintainer {
    public static final String AGGREGATE_FUNCTION_NAME = FunctionNames.BITMAP_VALUE;

    public static final int DEFAULT_ENTRY_SIZE = 10_000;
    public static final int MAX_ENTRY_SIZE = 250_000;

    private final int entrySize;
    private final boolean unique;

    public BitmapValueIndexMaintainer(IndexMaintainerState state) {
        super(state);
        final String sizeArgument = state.index.getOption(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION);
        entrySize = sizeArgument != null ? Integer.parseInt(sizeArgument) : DEFAULT_ENTRY_SIZE;
        if (entrySize > MAX_ENTRY_SIZE) {
            throw new RecordCoreArgumentException("entry size option is too large")
                    .addLogInfo("entrySize", entrySize, "maxEntrySize", MAX_ENTRY_SIZE);
        }
        unique = state.index.isUnique();
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        if (!scanType.equals(IndexScanType.BY_GROUP)) {
            throw new RecordCoreException("Can only scan bitmap index by group.");
        }
        final int groupPrefixSize = getGroupingCount();
        final long startPosition;
        if (range.getLow() != null && range.getLow().size() > groupPrefixSize && range.getLow().get(groupPrefixSize) != null) {
            if (range.getLowEndpoint() == EndpointType.RANGE_EXCLUSIVE) {
                startPosition = range.getLow().getLong(groupPrefixSize) + 1;
            } else {
                startPosition = range.getLow().getLong(groupPrefixSize);
            }
            if (startPosition % entrySize != 0) {
                range = new TupleRange(range.getLow().popBack().add(startPosition - Math.floorMod(startPosition, (long)entrySize)),
                        range.getHigh(), EndpointType.RANGE_INCLUSIVE, range.getHighEndpoint());
            }
        } else {
            startPosition = Long.MIN_VALUE;
        }
        final long endPosition;
        if (range.getHigh() != null && range.getHigh().size() > groupPrefixSize && range.getHigh().get(groupPrefixSize) != null) {
            if (range.getHighEndpoint() == EndpointType.RANGE_INCLUSIVE) {
                endPosition = range.getHigh().getLong(groupPrefixSize) + 1;
            } else {
                endPosition = range.getHigh().getLong(groupPrefixSize);
            }
            if (endPosition % entrySize != 0) {
                range = new TupleRange(range.getLow(),
                        range.getHigh().popBack().add(endPosition + Math.floorMod(entrySize - endPosition, (long)entrySize)),
                        range.getLowEndpoint(), EndpointType.RANGE_INCLUSIVE);
            }
        } else {
            endPosition = Long.MAX_VALUE;
        }
        return scan(range, continuation, scanProperties).map(indexEntry -> {
            final long entryStart = indexEntry.getKey().getLong(groupPrefixSize);
            final byte[] entryBitmap = indexEntry.getValue().getBytes(0);
            final long entryEnd = entryStart + entryBitmap.length * 8;
            if (entryStart < startPosition || entryEnd > endPosition) {
                final long trimmedStart = Math.max(entryStart, startPosition);
                final long trimmedEnd = Math.min(entryEnd, endPosition);
                if (trimmedStart < trimmedEnd) {
                    final Tuple trimmedKey = indexEntry.getKey().popBack().add(trimmedStart);
                    final byte[] trimmedBitmap = new byte[((int)(trimmedEnd - trimmedStart) + 7) / 8];
                    for (long i = trimmedStart; i < trimmedEnd; i++) {
                        int offset = (int)(i - entryStart);
                        if ((entryBitmap[offset / 8] & (byte)(1 << (offset % 8))) != 0) {
                            int trimmedOffset = (int)(i - trimmedStart);
                            trimmedBitmap[trimmedOffset / 8] |= (byte)(1 << (trimmedOffset % 8));
                        }
                    }
                    final Tuple subValue = Tuple.from(trimmedBitmap);
                    return Optional.of(new IndexEntry(indexEntry.getIndex(), trimmedKey, subValue));
                } else {
                    return Optional.<IndexEntry>empty();
                }
            } else {
                return Optional.of(indexEntry);
            }
        }).filter(Optional::isPresent).map(Optional::get);
    }

    @Override
    @Nonnull
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final int groupPrefixSize = getGroupingCount();
        final List<CompletableFuture<Void>> futures = unique && !remove ? new ArrayList<>(indexEntries.size()) : null;
        for (IndexEntry indexEntry : indexEntries) {
            final long startTime = System.nanoTime();
            final Tuple groupKey = TupleHelpers.subTuple(indexEntry.getKey(), 0, groupPrefixSize);
            Object positionObject = indexEntry.getKey().get(groupPrefixSize);
            if (positionObject == null) {
                continue;
            }
            if (!(positionObject instanceof Number)) {
                // This should be prevented by the checks in the meta-data builder, but just in case
                throw new RecordCoreException("position field in index entry is not a number").addLogInfo(
                        LogMessageKeys.KEY, indexEntry.getKey(),
                        LogMessageKeys.INDEX_NAME, state.index.getName(),
                        LogMessageKeys.INDEX_TYPE, state.index.getType());
            }
            long position = ((Number) positionObject).longValue();
            final int offset = (int)Math.floorMod(position, (long)entrySize);
            position -= offset;
            final byte[] key = state.indexSubspace.pack(groupKey.add(position));
            // This has to be the same size every time, with all the unset bits, or else it gets truncated.
            // We really could use a new mutation that took a linear bit position to set / clear and only did length extension or something like that.
            final byte[] bitmap = new byte[(entrySize + 7) / 8];
            if (remove) {
                if (state.store.isIndexWriteOnly(state.index)) {
                    // If the index isn't built, it's possible this key wasn't reached.
                    // So initialize it to zeros (or leave it alone).
                    state.transaction.mutate(MutationType.BIT_OR, key, bitmap);
                }
                // Otherwise the entry must already exist for us to be removing it,
                // so there is no danger that this will store all (but one) ones in a new key.
                Arrays.fill(bitmap, (byte)0xFF);
                bitmap[offset / 8] &= (byte)~(1 << (offset % 8));
                state.transaction.mutate(MutationType.BIT_AND, key, bitmap);
                Arrays.fill(bitmap, (byte)0x00);
                state.transaction.mutate(MutationType.COMPARE_AND_CLEAR, key, bitmap);
            } else {
                if (unique) {
                    // Snapshot read to see if the bit is already set.
                    CompletableFuture<Void> future = state.transaction.snapshot().get(key).thenAccept(existing -> {
                        if (existing != null && (existing[offset / 8] & (byte)(1 << (offset % 8))) != 0) {
                            throw new RecordIndexUniquenessViolation(state.index, indexEntry, savedRecord.getPrimaryKey(),
                                    null);  // Unfortunately, we don't know the other key.
                        }
                    });
                    futures.add(future);
                    // Then arrange to conflict for the position with any concurrent update.
                    final byte[] conflictKey = new Subspace(key).pack(offset);
                    state.transaction.addReadConflictKey(conflictKey);
                    state.transaction.addWriteConflictKey(conflictKey);
                }
                bitmap[offset / 8] |= (byte)(1 << (offset % 8));
                state.transaction.mutate(MutationType.BIT_OR, key, bitmap);
            }
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.MUTATE_INDEX_ENTRY, startTime);
            }
        }
        return futures != null ? AsyncUtil.whenAll(futures) : AsyncUtil.DONE;
    }

    @Override
    @Nonnull
    protected Tuple decodeValue(@Nonnull byte[] value) {
        return Tuple.from(value);  // The byte array itself is the value.
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        return AGGREGATE_FUNCTION_NAME.equals(function.getName()) &&
                IndexFunctionHelper.isGroupPrefix(function.getOperand(), state.index.getRootExpression());
    }

    @Override
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationveLevel) {
        if (!AGGREGATE_FUNCTION_NAME.equals(function.getName())) {
            throw new MetaDataException("this index does not support aggregate function: " + function);
        }
        final RecordCursor<IndexEntry> cursor = scan(IndexScanType.BY_GROUP, range,
                null, new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(isolationveLevel).build()));
        final int groupPrefixSize = getGroupingCount();
        long startPosition = 0;
        if (range.getLow() != null && range.getLow().size() > groupPrefixSize) {
            startPosition = range.getLow().getLong(groupPrefixSize);
        }
        int size = entrySize;
        if (range.getHigh() != null && range.getHigh().size() > groupPrefixSize) {
            long endPosition = range.getHigh().getLong(groupPrefixSize);
            if (size > endPosition - startPosition) {
                // Narrow size to what can actually be passed through from scan.
                size = (int)(endPosition - startPosition);
            }
        }
        return cursor.reduce(new BitmapAggregator(startPosition, size), (combined, kv) -> combined.append(kv.getKey().getLong(kv.getKeySize() - 1), kv.getValue().getBytes(0)))
                .thenApply(combined -> Tuple.from(combined.asByteArray()));
    }

    private static class BitmapAggregator {
        private final long offset;
        private ByteBuffer buffer;

        public BitmapAggregator() {
            this(0, DEFAULT_ENTRY_SIZE);
        }

        public BitmapAggregator(long offset, int size) {
            this.offset = offset;
            this.buffer = ByteBuffer.allocate(size);
        }

        public BitmapAggregator append(long position, @Nonnull byte[] bytes) {
            position -= offset;
            if (position < 0) {
                throw new RecordCoreException("For negative positions, must specify negative range start");
            }
            if (position % 8 != 0) {
                throw new RecordCoreException("Position must be on even byte boundary");
            }
            if (position > (long)Integer.MAX_VALUE * 8) {
                throw new RecordCoreException("For large positions, must specify large range start");
            }
            int bytePosition = (int)(position / 8);
            if (bytePosition + bytes.length > buffer.capacity()) {
                ByteBuffer newBuffer = ByteBuffer.allocate(bytePosition + bytes.length);
                buffer.flip();
                newBuffer.put(buffer);
                buffer = newBuffer;
            }
            buffer.position(bytePosition);
            buffer.put(bytes);
            return this;
        }

        @Nonnull
        public byte[] asByteArray() {
            return buffer.array();
        }
    }


}
