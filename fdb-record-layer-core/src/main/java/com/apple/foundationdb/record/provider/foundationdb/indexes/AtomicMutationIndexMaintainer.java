/*
 * AtomicMutationIndexMaintainer.java
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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexFunctionHelper;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * An index that maintains an aggregate value in a low-contention way.
 * <p>
 * Normally, when two transactions read, modify, and write the same location, they conflict. This makes a straightforward
 * implementation of most aggregate indexes inefficient. Ones that use the atomic mutation feature of FDB avoid this problem.
 * </p>
 */
@API(API.Status.MAINTAINED)
public class AtomicMutationIndexMaintainer extends StandardIndexMaintainer {
    protected final AtomicMutation mutation;

    public AtomicMutationIndexMaintainer(IndexMaintainerState state) {
        super(state);
        mutation = getAtomicMutation(state.index);
    }

    protected AtomicMutationIndexMaintainer(IndexMaintainerState state, AtomicMutation mutation) {
        super(state);
        this.mutation = mutation;
    }

    protected static boolean getClearWhenZero(@Nonnull Index index) {
        return index.getBooleanOption(IndexOptions.CLEAR_WHEN_ZERO, false);
    }

    @SuppressWarnings({"deprecation", "squid:CallToDeprecatedMethod"})
    protected static AtomicMutation getAtomicMutation(@Nonnull Index index) {
        if (IndexTypes.COUNT.equals(index.getType())) {
            return getClearWhenZero(index) ? AtomicMutation.Standard.COUNT_CLEAR_WHEN_ZERO : AtomicMutation.Standard.COUNT;
        }
        if (IndexTypes.COUNT_UPDATES.equals(index.getType())) {
            return AtomicMutation.Standard.COUNT_UPDATES;
        }
        if (IndexTypes.COUNT_NOT_NULL.equals(index.getType())) {
            return getClearWhenZero(index) ? AtomicMutation.Standard.COUNT_NOT_NULL_CLEAR_WHEN_ZERO : AtomicMutation.Standard.COUNT_NOT_NULL;
        }
        if (IndexTypes.SUM.equals(index.getType())) {
            return getClearWhenZero(index) ? AtomicMutation.Standard.SUM_LONG_CLEAR_WHEN_ZERO : AtomicMutation.Standard.SUM_LONG;
        }
        if (IndexTypes.MIN_EVER_TUPLE.equals(index.getType())) {
            return AtomicMutation.Standard.MIN_EVER_TUPLE;
        }
        if (IndexTypes.MAX_EVER_TUPLE.equals(index.getType())) {
            return AtomicMutation.Standard.MAX_EVER_TUPLE;
        }
        if (IndexTypes.MIN_EVER_LONG.equals(index.getType()) || IndexTypes.MIN_EVER.equals(index.getType())) {
            return AtomicMutation.Standard.MIN_EVER_LONG;
        }
        if (IndexTypes.MAX_EVER_LONG.equals(index.getType()) || IndexTypes.MAX_EVER.equals(index.getType())) {
            return AtomicMutation.Standard.MAX_EVER_LONG;
        }
        if (IndexTypes.MAX_EVER_VERSION.equals(index.getType())) {
            return AtomicMutation.Standard.MAX_EVER_VERSION;
        }
        throw new MetaDataException("Unknown index type for " + index);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                            @Nonnull TupleRange range,
                                            @Nullable byte[] continuation,
                                            @Nonnull ScanProperties scanProperties) {
        if (!scanType.equals(IndexScanType.BY_GROUP)) {
            throw new RecordCoreException("Can only scan aggregate index by group.");
        }
        return scan(range, continuation, scanProperties);
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final MutationType mutationType = mutation.getMutationType();
        final int groupPrefixSize = getGroupingCount();
        for (IndexEntry indexEntry : indexEntries) {
            long startTime = System.nanoTime();
            final Tuple groupKey;
            final IndexEntry groupedValue;
            if (groupPrefixSize <= 0) {
                groupKey = TupleHelpers.EMPTY;
                groupedValue = indexEntry;
            } else if (groupPrefixSize == indexEntry.getKeySize()) {
                groupKey = indexEntry.getKey();
                groupedValue = indexEntry.subKey(0, 0);
            } else {
                groupKey = TupleHelpers.subTuple(indexEntry.getKey(), 0, groupPrefixSize);
                groupedValue = indexEntry.subKey(groupPrefixSize, indexEntry.getKeySize());
            }
            final byte[] param = mutation.getMutationParam(groupedValue, remove);
            if (param == null) {
                continue;
            }

            // disallow negative values in certain index types, currently MAX_EVER_LONG and MIN_EVER_LONG
            // this is a bit of a hack here, but it's the minimally invasive way and most explicit way to do this
            if (!mutation.allowsNegative()) {
                Number numVal = (Number) groupedValue.getKeyValue(0);
                if (numVal != null && numVal.longValue() < 0) {
                    throw new RecordCoreException("Attempted update of MAX_EVER_LONG or MIN_EVER_LONG index with negative value");
                }
            }

            final byte[] key = state.indexSubspace.pack(groupKey);
            if (AtomicMutation.Standard.MAX_EVER_VERSION.equals(mutation)) {
                if (groupedValue.getKey().hasIncompleteVersionstamp()) {
                    // With an incomplete versionstamp, we need to call SET_VERSIONSTAMPED_VALUE.
                    // If multiple records (with possibly different local versions) are written with the same
                    // grouping key in the same context, we want to only write the one with the maximum
                    // local version. Choosing the one with the maximum byte representation will do this
                    // as all incomplete versionstamps are serialized with identical fake global versions.
                    state.context.updateVersionMutation(MutationType.SET_VERSIONSTAMPED_VALUE, key, param,
                            (oldParam, newParam) -> ByteArrayUtil.compareUnsigned(oldParam, newParam) < 0 ? newParam : oldParam);
                } else {
                    state.transaction.mutate(MutationType.BYTE_MAX, key, param);
                }
            } else {
                state.transaction.mutate(mutationType, key, param);
                final byte[] compareAndClear = mutation.getCompareAndClearParam();
                if (compareAndClear != null) {
                    state.transaction.mutate(MutationType.COMPARE_AND_CLEAR, key, compareAndClear);
                }
            }
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(FDBStoreTimer.Events.MUTATE_INDEX_ENTRY, startTime);
            }
        }
        return AsyncUtil.DONE;
    }

    @Override
    protected Tuple decodeValue(@Nonnull byte[] value) {
        switch (mutation.getMutationType()) {
            case ADD:
            case BIT_AND:
            case BIT_OR:
            case BIT_XOR:
            case MIN:
            case MAX:
                return Tuple.from(AtomicMutation.Standard.decodeUnsignedLong(value));
            default:
                return super.decodeValue(value);
        }
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        /*
        System.out.println("matchesAggregateFunction:" + matchesAggregateFunction(function));
        System.out.println("functionOperand:" + function.getOperand());

        System.out.println("state.index:" + state.index);
        System.out.println("indexRoot:" + state.index.getRootExpression());


        System.out.println("functionOperand groupedKey:" + IndexFunctionHelper.getGroupedKey(function.getOperand()));
        System.out.println("indexRoot groupedKey:" + IndexFunctionHelper.getGroupedKey(state.index.getRootExpression()));
        System.out.println("functionOperand groupingKey:" + IndexFunctionHelper.getGroupingKey(function.getOperand()));
        System.out.println("indexRoot groupingKey:" + IndexFunctionHelper.getGroupingKey(state.index.getRootExpression()));
        System.out.println("groupedKey equal:" + IndexFunctionHelper.getGroupedKey(function.getOperand()).equals(IndexFunctionHelper.getGroupedKey(state.index.getRootExpression())));
        System.out.println("groupingkey isPrefix:" + IndexFunctionHelper.getGroupingKey(function.getOperand()).isPrefixKey(IndexFunctionHelper.getGroupingKey(state.index.getRootExpression())));

         */
        return matchesAggregateFunction(function) &&
               IndexFunctionHelper.isGroupPrefix(function.getOperand(), state.index.getRootExpression());
    }

    @Override
    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationveLevel) {
        if (!matchesAggregateFunction(function)) {
            throw new MetaDataException("this index does not support aggregate function: " + function);
        }
        final RecordCursor<IndexEntry> cursor = scan(IndexScanType.BY_GROUP, range,
                null, new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(isolationveLevel).build()));
        final BiFunction<Tuple, Tuple, Tuple> aggregator = mutation.getAggregator();
        return cursor.reduce(mutation.getIdentity(), (accum, kv) -> aggregator.apply(accum, kv.getValue()));
    }

    protected boolean matchesAggregateFunction(@Nonnull IndexAggregateFunction function) {
        String functionName = function.getName();
        String indexType = state.index.getType();
        return functionName.equals(indexType) ||
               (FunctionNames.MAX_EVER.equals(functionName) && (IndexTypes.MAX_EVER_LONG.equals(indexType) || IndexTypes.MAX_EVER_TUPLE.equals(indexType))) ||
               (FunctionNames.MIN_EVER.equals(functionName) && (IndexTypes.MIN_EVER_LONG.equals(indexType) || IndexTypes.MIN_EVER_TUPLE.equals(indexType)));
    }

    @Override
    public boolean isIdempotent() {
        return mutation.isIdempotent();
    }

    @Override
    public boolean skipUpdateForUnchangedKeys() {
        return !IndexTypes.COUNT_UPDATES.equals(state.index.getType());
    }

    // NOTE: It is possible to convert _LONG entries to _TUPLE using something like
    // maintainer1.scan(...).forEach(maintainer2::saveIndexEntryAsKeyValue).
    // This might require multiple scans in transactions if there are too many groups.
}
