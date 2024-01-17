/*
 * PermutedMinMaxIndexMaintainer.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexFunctionHelper;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

/**
 * An index that maintains an extremum value in a way that can be enumerated by that value rather than by group.
 *
 * This is done by specifying a number of grouping fields that are <em>permuted</em> to after the value.
 * This number is specified by the {@link IndexOptions#PERMUTED_SIZE_OPTION} index option.
 *
 * For example, an {@code PERMUTED_MAX} index on <code>field(val).groupBy(concatenateFields(group, subgroup))</code> with a permuted size of {@code 1} can,
 * for an ordered range of {@code group} value(s) (including just a single group), list the maximum {@code value} for each {@code subgroup}, ordered by that maximum.
 */
@API(API.Status.EXPERIMENTAL)
public class PermutedMinMaxIndexMaintainer extends StandardIndexMaintainer {
    protected enum Type {
        MIN, MAX
    }

    private final Type type;
    private final int permutedSize;

    public PermutedMinMaxIndexMaintainer(@Nonnull IndexMaintainerState state) {
        super(state);
        type = getType(state.index);
        permutedSize = getPermutedSize(state.index);
    }

    protected static int getPermutedSize(@Nonnull Index index) {
        String permutedSizeOption = index.getOption(IndexOptions.PERMUTED_SIZE_OPTION);
        if (permutedSizeOption == null) {
            throw new MetaDataException("permuted size not specified", LogMessageKeys.INDEX_NAME, index.getName());
        }
        return Integer.parseInt(permutedSizeOption);
    }

    protected static Type getType(@Nonnull Index index) {
        if (IndexTypes.PERMUTED_MIN.equals(index.getType())) {
            return Type.MIN;
        }
        if (IndexTypes.PERMUTED_MAX.equals(index.getType())) {
            return Type.MAX;
        }
        throw new MetaDataException("Unknown index type for " + index);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        if (scanType.equals(IndexScanType.BY_VALUE)) {
            return scan(range, continuation, scanProperties);
        }
        if (scanType.equals(IndexScanType.BY_GROUP)) {
            final Subspace permutedSubspace = getSecondarySubspace();
            final RecordCursor<KeyValue> keyValues = KeyValueCursor.Builder.withSubspace(permutedSubspace)
                    .setContext(state.context)
                    .setRange(range)
                    .setContinuation(continuation)
                    .setScanProperties(scanProperties)
                    .build();
            return keyValues.map(kv -> {
                state.store.countKeyValue(FDBStoreTimer.Counts.LOAD_INDEX_KEY, FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES,
                        kv);
                return unpackKeyValue(permutedSubspace, kv);
            });
        }
        throw new RecordCoreException("Can only scan permuted index by value or group.");
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final int groupPrefixSize = getGroupingCount();
        final int totalSize = state.index.getColumnSize();
        final Subspace permutedSubspace = getSecondarySubspace();
        for (IndexEntry indexEntry : indexEntries) {
            final Tuple groupKey = TupleHelpers.subTuple(indexEntry.getKey(), 0, groupPrefixSize);
            final Tuple value = TupleHelpers.subTuple(indexEntry.getKey(), groupPrefixSize, totalSize);
            final int permutePosition = groupPrefixSize - permutedSize;
            final Tuple groupPrefix = TupleHelpers.subTuple(groupKey, 0, permutePosition);
            final Tuple groupSuffix = TupleHelpers.subTuple(groupKey, permutePosition, groupPrefixSize);
            if (remove) {
                // First remove from ordinary tree.
                return updateOneKeyAsync(savedRecord, remove, indexEntry).thenCompose(vignore -> {
                    final byte[] permutedKeyToRemove = permutedSubspace.pack(groupPrefix.addAll(value).addAll(groupSuffix));
                    // See if value is the current minimum/maximum.
                    return state.store.ensureContextActive().get(permutedKeyToRemove).thenCompose(permutedValueExists -> {
                        if (permutedValueExists == null) {
                            return AsyncUtil.DONE;  // No, nothing more to do.
                        }
                        return getExtremum(groupKey).thenApply(extremum -> {
                            if (extremum == null) {
                                // No replacement, just remove.
                                state.store.ensureContextActive().clear(permutedKeyToRemove);
                            } else {
                                final Tuple remainingValue = TupleHelpers.subTuple(extremum, groupPrefixSize, totalSize);
                                if (!value.equals(remainingValue)) {
                                    // New extremum: remove existing and store it.
                                    final byte[] permutedKeyToAdd = permutedSubspace.pack(groupPrefix.addAll(remainingValue).addAll(groupSuffix));
                                    final Transaction tr = state.store.ensureContextActive();
                                    tr.clear(permutedKeyToRemove);
                                    tr.set(permutedKeyToAdd, TupleHelpers.EMPTY.pack());
                                }
                            }
                            return null;
                        });
                    });
                });
            } else {
                // Get existing minimum/maximum.
                return getExtremum(groupKey).thenApply(extremum -> {
                    final boolean addPermuted;
                    if (extremum == null) {
                        addPermuted = true; // New group.
                    } else {
                        final Tuple currentValue = TupleHelpers.subTuple(extremum, groupPrefixSize, totalSize);
                        int compare = value.compareTo(currentValue);
                        addPermuted = type == Type.MIN ? compare < 0 : compare > 0;
                        // Replace if new value is better.
                        if (addPermuted) {
                            final byte[] permutedKeyToRemove = permutedSubspace.pack(groupPrefix.addAll(currentValue).addAll(groupSuffix));
                            state.store.ensureContextActive().clear(permutedKeyToRemove);
                        }
                    }
                    if (addPermuted) {
                        final byte[] permutedKeyToAdd = permutedSubspace.pack(groupPrefix.addAll(value).addAll(groupSuffix));
                        state.store.ensureContextActive().set(permutedKeyToAdd, TupleHelpers.EMPTY.pack());
                    }
                    return null;
                }).thenCompose(vignore -> updateOneKeyAsync(savedRecord, remove, indexEntry));  // Ordinary is second.
            }
        }
        return AsyncUtil.DONE;
    }

    // Return the min/max key matching the given group key or {@code null} if there are not entries for the group.
    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Tuple> getExtremum(@Nonnull Tuple groupKey) {
        final RecordCursor<IndexEntry> scan = scan(TupleRange.allOf(groupKey), null,
                (type == Type.MIN ? ScanProperties.FORWARD_SCAN : ScanProperties.REVERSE_SCAN)
                        .with(props -> props.clearState().setReturnedRowLimit(1)));
        return scan.first().thenApply(first -> first.map(IndexEntry::getKey).orElse(null));
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull final IndexAggregateFunction function) {
        return function.getName().equals(type.name().toLowerCase(Locale.ROOT))
               && IndexFunctionHelper.isGroupPrefix(function.getOperand(), state.index.getRootExpression());
    }

    @Nonnull
    @Override
    @SuppressWarnings({"PMD.CloseResource", "PMD.UseTryWithResources"}) // PMD cannot determine resource is closed
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull final IndexAggregateFunction function,
                                                              @Nonnull final TupleRange range,
                                                              @Nonnull final IsolationLevel isolationLevel) {
        if (!canEvaluateAggregateFunction(function)) {
            throw new RecordCoreArgumentException("Cannot execute aggregate function")
                    .addLogInfo(LogMessageKeys.FUNCTION, function.getName())
                    .addLogInfo(LogMessageKeys.KEY_EXPRESSION, function.getOperand())
                    .addLogInfo(LogMessageKeys.INDEX_NAME, state.index.getName());
        }
        final int valueStart = getGroupingCount() - permutedSize;
        final int valueEnd = state.index.getColumnSize() - permutedSize;
        ScanProperties scanProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(isolationLevel)
                .build()
                .asScanProperties(false);
        TupleRange unpermutedRange = trimToUnpermutedPrefix(range);
        RecordCursor<Tuple> cursor = null;
        boolean asyncWork = false;
        try {
            RecordCursor<IndexEntry> entryCursor = scan(IndexScanType.BY_GROUP, unpermutedRange, null, scanProperties);
            if (!unpermutedRange.equals(range)) {
                entryCursor = entryCursor.filter(entry -> {
                    Tuple groupPrefix = TupleHelpers.subTuple(entry.getKey(), 0, valueStart);
                    Tuple groupSuffix = TupleHelpers.subTuple(entry.getKey(), valueEnd, entry.getKeySize());
                    Tuple group = groupPrefix.addAll(groupSuffix);
                    return range.contains(group);
                });
            }
            cursor = entryCursor.map(entry -> TupleHelpers.subTuple(entry.getKey(), valueStart, valueEnd));
            CompletableFuture<Tuple> valueFuture = cursor.reduce(null, (Tuple accum, Tuple value) -> {
                if (accum == null) {
                    return value;
                } else {
                    int comparison = value.compareTo(accum);
                    if (comparison < 0 && type == Type.MIN || comparison > 0 && type == Type.MAX) {
                        return value;
                    } else {
                        return accum;
                    }
                }
            });
            asyncWork = true;
            final RecordCursor<Tuple> finalCursor = cursor;
            return valueFuture.whenComplete((ignore, eignore) -> finalCursor.close());
        } finally {
            // Close the cursor in the case if an error creating the future.
            if (cursor != null && !asyncWork) {
                cursor.close();
            }
        }
    }

    @Nonnull
    private TupleRange trimToUnpermutedPrefix(@Nonnull TupleRange range) {
        int unpermutedSize = getGroupingCount() - permutedSize;
        EndpointType lowEndpoint = range.getLowEndpoint();
        @Nullable Tuple low = range.getLow();
        if (lowEndpoint != EndpointType.TREE_START && low != null && low.size() > unpermutedSize) {
            low = TupleHelpers.subTuple(low, 0, unpermutedSize);
            lowEndpoint = EndpointType.RANGE_INCLUSIVE;
        }
        EndpointType highEndpoint = range.getHighEndpoint();
        @Nullable Tuple high = range.getHigh();
        if (highEndpoint != EndpointType.TREE_END && high != null && high.size() > unpermutedSize) {
            high = TupleHelpers.subTuple(high, 0, unpermutedSize);
            highEndpoint = EndpointType.RANGE_INCLUSIVE;
        }
        return new TupleRange(low, high, lowEndpoint, highEndpoint);
    }

    @Override
    public boolean canDeleteWhere(@Nonnull final QueryToKeyMatcher matcher, @Nonnull final Key.Evaluated evaluated) {
        if (!super.canDeleteWhere(matcher, evaluated)) {
            return false;
        }
        final int unpermutedSize = getGroupingCount() - permutedSize;
        return evaluated.size() <= unpermutedSize;
    }

    @Override
    public CompletableFuture<Void> deleteWhere(Transaction tr, @Nonnull Tuple prefix) {
        return super.deleteWhere(tr, prefix).thenApply(v -> {
            final Subspace permutedSubspace = getSecondarySubspace();
            tr.clear(permutedSubspace.subspace(prefix).range());
            return v;
        });
    }
}
