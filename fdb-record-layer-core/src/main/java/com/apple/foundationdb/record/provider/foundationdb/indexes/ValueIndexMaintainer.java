/*
 * ValueIndexMaintainer.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.ConcatCursor;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRawRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * An index maintainer for an ordinary index by value, implementing ordered enumeration of records within a range of indexed values.
 *
 * When more than one field is indexed, records are ordered lexicographically.
 */
@API(API.Status.STABLE)
public class ValueIndexMaintainer extends StandardIndexMaintainer {
    public ValueIndexMaintainer(IndexMaintainerState state) {
        super(state);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        if (!scanType.equals(IndexScanType.BY_VALUE)) {
            throw new RecordCoreException("Can only scan standard index by value.");
        }
        return scan(range, continuation, scanProperties);
    }

    /**
     * Validate entries in the index. It scans the index and checks if the record associated with each index entry exists.
     * @param continuation any continuation from a previous validation invocation
     * @param scanProperties skip, limit and other properties of the validation (use default values if <code>null</code>)
     * @return a cursor over index entries that have no associated records
     */
    @Nonnull
    @Override
    public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable byte[] continuation,
                                                           @Nullable ScanProperties scanProperties) {
        if (scanProperties == null) {
            scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(Integer.MAX_VALUE)
                    // For index entry validation, it does not hurt to have a weaker isolation.
                    .setIsolationLevel(IsolationLevel.SNAPSHOT)
                    .build());
        }
        return new ConcatCursor<>(state.context, scanProperties,
                (context, scanProperties1, continuation1) -> validateOrphanEntries(continuation1, scanProperties1),
                (context, scanProperties2, continuation2) -> validateMissingEntries(continuation2, scanProperties2),
                continuation);
    }

    @Override
    public boolean canEvaluateAggregateFunction(@Nonnull IndexAggregateFunction function) {
        System.out.println("root expression:" + state.index.getRootExpression());
        System.out.println("ungrouped:" + ungroupedAggregateOperand(function.getOperand()));
        return (FunctionNames.MIN.equals(function.getName()) ||
                FunctionNames.MAX.equals(function.getName())) &&
                ungroupedAggregateOperand(function.getOperand()).isPrefixKey(state.index.getRootExpression());
    }

    protected static KeyExpression ungroupedAggregateOperand(@Nonnull KeyExpression key) {
        if (key instanceof GroupingKeyExpression) {
            return ((GroupingKeyExpression)key).getWholeKey();
        } else {
            return key;
        }
    }

    @Override
    @Nonnull
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull IndexAggregateFunction function,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull final IsolationLevel isolationLevel) {
        final boolean reverse;
        if (FunctionNames.MIN.equals(function.getName())) {
            reverse = false;
        } else if (FunctionNames.MAX.equals(function.getName())) {
            reverse = true;
        } else {
            throw new MetaDataException("do not index aggregate function: " + function);
        }
        final int totalSize = function.getOperand().getColumnSize();
        final int groupSize = totalSize - (function.getOperand() instanceof GroupingKeyExpression ?
                ((GroupingKeyExpression) function.getOperand()).getGroupedCount() :
                1);
        return scan(IndexScanType.BY_VALUE, range, null, new ScanProperties(ExecuteProperties.newBuilder()
                .setReturnedRowLimit(1)
                .setIsolationLevel(isolationLevel)
                .build(), reverse))
                .first()
                .thenApply(kvo -> kvo.map(kv -> TupleHelpers.subTuple(kv.getKey(), groupSize, totalSize)).orElse(null));
    }

    @Nonnull
    @Override
    public RecordCursor<FDBIndexedRawRecord> scanRemoteFetch(@Nonnull final IndexScanBounds scanBounds,
                                                             @Nullable final byte[] continuation,
                                                             @Nonnull final ScanProperties scanProperties,
                                                             int commonPrimaryKeyLength) {
        return scanRemoteFetchByValue(scanBounds, continuation, scanProperties, commonPrimaryKeyLength);
    }
}
