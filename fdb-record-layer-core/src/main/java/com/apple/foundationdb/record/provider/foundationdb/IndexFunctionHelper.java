/*
 * IndexFunctionHelper.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class for finding appropriate indexes for {@link IndexRecordFunction} and {@link IndexAggregateFunction}.
 */
@API(API.Status.INTERNAL)
public class IndexFunctionHelper {
    private IndexFunctionHelper() {
    }

    /**
     * Find an index that can evaluate the given function against the given record.
     * @param store the record store containing the record
     * @param function the function to evaluate
     * @param record the record to evaluate against
     * @return an index maintainer that can evaluate the function or {@code Optional.empty()}
     */
    public static Optional<IndexMaintainer> indexMaintainerForRecordFunction(@Nonnull FDBRecordStore store,
                                                                             @Nonnull IndexRecordFunction<?> function,
                                                                             @Nonnull FDBRecord<?> record) {
        final String recordType = record.getRecordType().getName();
        return indexMaintainerForRecordFunction(store, function, Collections.singletonList(recordType));
    }

    /**
     * Find an index that can evaluate the given function against all of the given record types. 
     * @param store the record store containing the record types
     * @param function the function to evaluate
     * @param recordTypeNames the names of all the record types for which the function will be evaluated
     * @return an index maintainer that can evaluate the function or {@code Optional.empty()}
     */
    public static Optional<IndexMaintainer> indexMaintainerForRecordFunction(@Nonnull FDBRecordStore store,
                                                                             @Nonnull IndexRecordFunction<?> function,
                                                                             @Nonnull List<String> recordTypeNames) {
        if (function.getIndex() != null) {
            final Index index = store.getRecordMetaData().getIndex(function.getIndex());
            if (store.getRecordStoreState().isReadable(index)) {
                return Optional.of(store.getIndexMaintainer(index));
            }
        }
        return indexesForRecordTypes(store, recordTypeNames)
                .filter(store::isIndexReadable)
                .map(store::getIndexMaintainer)
                .filter(i -> i.canEvaluateRecordFunction(function))
                // Prefer the one that does it in the fewest number of columns.
                .min(Comparator.comparing(i -> i.state.index.getColumnSize()));
    }

    public static Optional<IndexMaintainer> indexMaintainerForAggregateFunction(@Nonnull FDBRecordStore store,
                                                                                @Nonnull IndexAggregateFunction function,
                                                                                @Nonnull List<String> recordTypeNames) {
        if (function.getIndex() != null) {
            final Index index = store.getRecordMetaData().getIndex(function.getIndex());
            if (store.getRecordStoreState().isReadable(index)) {
                return Optional.of(store.getIndexMaintainer(index));
            }
        }
        return indexesForRecordTypes(store, recordTypeNames)
                .filter(store::isIndexReadable)
                .map(store::getIndexMaintainer)
                .filter(i -> i.canEvaluateAggregateFunction(function))
                // Prefer the one that does it in the fewest number of columns, because that will mean less rolling-up.
                .min(Comparator.comparing(i -> i.state.index.getColumnSize()));
    }

    /**
     * The indexes that apply to <em>exactly</em> the given types, no more, no less.
     * @param store the store containing the record types
     * @param recordTypeNames the names of the record types for which indexes are needed
     * @return a stream of indexes
     */
    public static Stream<Index> indexesForRecordTypes(@Nonnull FDBRecordStore store,
                                                      @Nonnull List<String> recordTypeNames) {
        final RecordMetaData metaData = store.getRecordMetaData();
        if (recordTypeNames.isEmpty()) {
            return metaData.getUniversalIndexes().stream();
        } else if (recordTypeNames.size() == 1) {
            return metaData.getRecordType(recordTypeNames.get(0)).getIndexes().stream();
        } else {
            final Set<RecordType> asSet = recordTypeNames.stream().map(metaData::getRecordType).collect(Collectors.toSet());
            return asSet.iterator().next().getMultiTypeIndexes().stream()
                    .filter(i -> asSet.equals(new HashSet<>(store.getRecordMetaData().recordTypesForIndex(i))));
        }
    }

    /**
     * Is the operand for this function compatible with this index?
     * The <em>grouped</em> part must match. The <em>grouping</em> part must be a prefix.
     * @param functionOperand the operand to the function
     * @param indexRoot the index's expression
     * @return {@code true} if the operand is compatible with the index
     */
    public static boolean isGroupPrefix(@Nonnull KeyExpression functionOperand, @Nonnull KeyExpression indexRoot) {
        return getGroupedKey(functionOperand).equals(getGroupedKey(indexRoot)) &&
            getGroupingKey(functionOperand).isPrefixKey(getGroupingKey(indexRoot));
    }

    public static KeyExpression getGroupedKey(@Nonnull KeyExpression key) {
        if (!(key instanceof GroupingKeyExpression)) {
            return key;
        }
        GroupingKeyExpression grouping = (GroupingKeyExpression) key;
        return getGroupingSubkey(grouping, grouping.getGroupingCount(), grouping.getColumnSize());
    }

    public static KeyExpression getGroupingKey(@Nonnull KeyExpression key) {
        if (!(key instanceof GroupingKeyExpression)) {
            return EmptyKeyExpression.EMPTY;
        }
        GroupingKeyExpression grouping = (GroupingKeyExpression) key;
        return getGroupingSubkey(grouping, 0, grouping.getGroupingCount());
    }

    protected static KeyExpression getGroupingSubkey(GroupingKeyExpression grouping, int start, int end) {
        if (start == end) {
            return EmptyKeyExpression.EMPTY;
        }
        final KeyExpression key = grouping.getWholeKey();
        if (!(key instanceof ThenKeyExpression)) {
            if (start == 0 && end == 1) {
                return key;
            }
            throw new RecordCoreException("grouping breaks apart key other than Then");
        }
        ThenKeyExpression then = (ThenKeyExpression)key;
        List<KeyExpression> children = then.getChildren();
        if (end == start + 1) {
            return children.get(start);
        }
        return new ThenKeyExpression(children.subList(start, end));
    }

    public static IndexAggregateFunction count(@Nonnull KeyExpression by) {
        return new IndexAggregateFunction(FunctionNames.COUNT,
                new GroupingKeyExpression(by, 0),
                null);
    }

    public static IndexAggregateFunction countUpdates(@Nonnull KeyExpression by) {
        return new IndexAggregateFunction(FunctionNames.COUNT_UPDATES,
                new GroupingKeyExpression(by, 0),
                null);
    }

    public static Optional<IndexAggregateFunction> bindAggregateFunction(@Nonnull FDBRecordStore store,
                                                                         @Nonnull IndexAggregateFunction function,
                                                                         @Nonnull List<String> recordTypeNames) {
        return indexMaintainerForAggregateFunction(store, function, recordTypeNames)
                .map(i -> function.cloneWithIndex(i.state.index.getName()));
    }
}
