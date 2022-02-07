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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexAggregateFunctionCall;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.google.common.base.Verify;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
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

    public static Optional<IndexMaintainer> indexMaintainerForAggregateFunction(
            @Nonnull FDBRecordStore store,
            @Nonnull IndexAggregateFunction function,
            @Nonnull List<String> recordTypeNames,
            @Nonnull IndexQueryabilityFilter indexFilter) {
        if (function.getIndex() != null) {
            final Index index = store.getRecordMetaData().getIndex(function.getIndex());
            if (store.getRecordStoreState().isReadable(index)) {
                return Optional.of(store.getIndexMaintainer(index));
            }
        }
        return indexesForRecordTypes(store, recordTypeNames)
                .filter(store::isIndexReadable)
                .filter(indexFilter::isQueryable)
                .map(store::getIndexMaintainer)
                .filter(i -> i.canEvaluateAggregateFunction(function))
                // Prefer the one that does it in the fewest number of columns, because that will mean less rolling-up.
                .min(Comparator.comparing(i -> i.state.index.getColumnSize()));
    }

    /**
     * Bind a {@link IndexAggregateFunctionCall} to an index and return a resulting {@link IndexAggregateFunction} and
     * {@link IndexMaintainer}. During the binding process we enumerate all indexes and for each such index we enumerate
     * all permutations of grouping expressions in order to find an index that can be used to evaluated the aggregate
     * function call.
     * @param store the store containing the record types
     * @param functionCall an {@link IndexAggregateFunctionCall}
     * @param recordTypeNames the names of the record types for which indexes need to be considered
     * @param indexFilter a filter to restrict which indexes can be used when planning
     * @return an optional of a bound {@link IndexAggregateFunction} and a {@link IndexMaintainer} if a matching index
     *         was found, {@code Optional.empty()} otherwise.
     */
    protected static Optional<Pair<IndexAggregateFunction, IndexMaintainer>> bindIndexForPermutableAggregateFunctionCall(
            @Nonnull FDBRecordStore store,
            @Nonnull IndexAggregateFunctionCall functionCall,
            @Nonnull List<String> recordTypeNames,
            @Nonnull IndexQueryabilityFilter indexFilter) {
        Verify.verify(functionCall.isGroupingPermutable());
        return indexesForRecordTypes(store, recordTypeNames)
                .filter(store::isIndexReadable)
                .filter(indexFilter::isQueryable)
                .flatMap(index ->
                        functionCall.enumerateIndexAggregateFunctionCandidates(index.getName())
                                .flatMap(indexAggregateFunction -> {
                                    final IndexMaintainer indexMaintainer = store.getIndexMaintainer(index);
                                    return indexMaintainer.canEvaluateAggregateFunction(indexAggregateFunction)
                                           ? Stream.of(Pair.of(indexAggregateFunction, indexMaintainer))
                                           : Stream.empty();
                                }))
                .min(Comparator.comparing(pair -> {
                    // use the minimum spanning index that matches
                    final IndexMaintainer indexMaintainer = pair.getRight();
                    return indexMaintainer.state.index.getColumnSize();
                }));
    }

    /**
     * The indexes that apply to <em>exactly</em> the given types, no more, no less.
     * @param store the store containing the record types
     * @param recordTypeNames the names of the record types for which indexes are needed
     * @return a stream of indexes
     */
    public static Stream<Index> indexesForRecordTypes(@Nonnull FDBRecordStore store,
                                                      @Nonnull List<String> recordTypeNames) {
        return indexesForRecordTypes(store.getRecordMetaData(), recordTypeNames);
    }

    /**
     * The indexes that apply to <em>exactly</em> the given types, no more, no less.
     * @param metaData the meta-data containing the record types
     * @param recordTypeNames the names of the record types for which indexes are needed
     * @return a stream of indexes
     */
    public static Stream<Index> indexesForRecordTypes(@Nonnull RecordMetaData metaData,
                                                      @Nonnull Collection<String> recordTypeNames) {
        if (recordTypeNames.isEmpty()) {
            return metaData.getUniversalIndexes().stream();
        } else if (recordTypeNames.size() == 1) {
            return metaData.getRecordType(recordTypeNames.iterator().next()).getIndexes().stream();
        } else {
            final Set<RecordType> asSet = recordTypeNames.stream().map(metaData::getRecordType).collect(Collectors.toSet());
            return asSet.iterator().next().getMultiTypeIndexes().stream()
                    .filter(i -> asSet.equals(new HashSet<>(metaData.recordTypesForIndex(i))));
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
        if (functionOperand.equals(indexRoot)) {
            return true;
        }
        return getGroupedKey(functionOperand).equals(getGroupedKey(indexRoot)) &&
            getGroupingKey(functionOperand).isPrefixKey(getGroupingKey(indexRoot));
    }

    public static KeyExpression getGroupedKey(@Nonnull KeyExpression key) {
        if (!(key instanceof GroupingKeyExpression)) {
            return key;
        }
        GroupingKeyExpression grouping = (GroupingKeyExpression) key;
        return groupKey(getSubKey(grouping.getWholeKey(), grouping.getGroupingCount(), grouping.getColumnSize()));
    }

    public static KeyExpression getGroupingKey(@Nonnull KeyExpression key) {
        if (!(key instanceof GroupingKeyExpression)) {
            return EmptyKeyExpression.EMPTY;
        }
        GroupingKeyExpression grouping = (GroupingKeyExpression) key;
        return groupKey(getSubKey(grouping.getWholeKey(), 0, grouping.getGroupingCount()));
    }

    protected static KeyExpression groupKey(@Nonnull KeyExpression key) {
        if (key instanceof ThenKeyExpression) {
            final List<KeyExpression> children = ((ThenKeyExpression) key).getChildren();
            int n = children.size();
            while (n > 0 && children.get(n - 1) instanceof EmptyKeyExpression) {
                n--;
            }
            if (n == 0) {
                return Key.Expressions.empty();
            } else if (n == 1) {
                return children.get(0);
            } else if (n < children.size()) {
                return new ThenKeyExpression(children.subList(0, n));
            }
        }
        return key;
    }

    protected static KeyExpression getSubKey(KeyExpression key, int start, int end) {
        if (start == end) {
            return EmptyKeyExpression.EMPTY;
        }
        if (start == 0 && end == key.getColumnSize()) {
            return key;
        }
        if (key instanceof ThenKeyExpression) {
            return getThenSubKey((ThenKeyExpression)key, start, end);
        }
        if (key instanceof NestingKeyExpression) {
            final NestingKeyExpression nesting = (NestingKeyExpression)key;
            return new NestingKeyExpression(nesting.getParent(), getSubKey(nesting.getChild(), start, end));
        }
        throw new RecordCoreException("grouping breaks apart key other than Then");
    }

    protected static KeyExpression getThenSubKey(ThenKeyExpression then, int columnStart, int columnEnd) {
        final List<KeyExpression> children = then.getChildren();
        int columnPosition = 0;
        int startChildPosition = -1;
        int startChildStart = -1;
        int startChildEnd = -1;
        for (int childPosition = 0; childPosition < children.size(); childPosition++) {
            final KeyExpression child = children.get(childPosition);
            final int childColumns = child.getColumnSize();
            if (startChildPosition < 0 && columnPosition + childColumns > columnStart) {
                startChildPosition = childPosition;
                startChildStart = columnStart - columnPosition;
                startChildEnd = childColumns;
            }
            if (columnPosition + childColumns >= columnEnd) {
                int endChildEnd = columnEnd - columnPosition;
                if (childPosition == startChildPosition) {
                    // Just one child spans column start, end.
                    if (startChildPosition == 0 && endChildEnd == childColumns) {
                        return child;
                    } else {
                        return getSubKey(child, startChildStart, endChildEnd);
                    }
                }
                if (startChildStart == 0 && endChildEnd == childColumns) {
                    return new ThenKeyExpression(children.subList(startChildPosition, childPosition + 1));
                }
                final List<KeyExpression> keys = new ArrayList<>(childPosition - startChildPosition + 1);
                keys.add(getSubKey(children.get(startChildPosition), startChildStart, startChildEnd));
                if (childPosition > startChildPosition + 1) {
                    keys.addAll(children.subList(startChildPosition + 1, childPosition));
                }
                keys.add(getSubKey(child, 0, endChildEnd));
                return new ThenKeyExpression(keys);
            }
            columnPosition += childColumns;
        }
        throw new RecordCoreException("column counts are not consistent");
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
                                                                         @Nonnull List<String> recordTypeNames,
                                                                         @Nonnull IndexQueryabilityFilter indexFilter) {
        return indexMaintainerForAggregateFunction(store, function, recordTypeNames, indexFilter)
                .map(i -> function.cloneWithIndex(i.state.index.getName()));
    }

    /**
     * (Public) method to bind an {@link IndexAggregateFunctionCall} to an index resulting in an
     * {@link IndexAggregateFunction} if successful.
     *
     * See {@link #bindIndexForPermutableAggregateFunctionCall(FDBRecordStore, IndexAggregateFunctionCall, List, IndexQueryabilityFilter)} for details.
     *
     * @param store the store containing the record types
     * @param functionCall an {@link IndexAggregateFunctionCall}
     * @param recordTypeNames the names of the record types for which indexes need to be considered
     * @param indexFilter a filter used to restrict candidate indexes
     * @return an optional of a bound {@link IndexAggregateFunction} if a matching index
     *         was found, {@code Optional.empty()} otherwise.
     */
    public static Optional<IndexAggregateFunction> bindAggregateFunctionCall(@Nonnull FDBRecordStore store,
                                                                             @Nonnull IndexAggregateFunctionCall functionCall,
                                                                             @Nonnull List<String> recordTypeNames,
                                                                             @Nonnull IndexQueryabilityFilter indexFilter) {
        if (functionCall.isGroupingPermutable()) {
            return bindIndexForPermutableAggregateFunctionCall(store, functionCall, recordTypeNames, indexFilter)
                    .map(Pair::getLeft);
        } else {
            return bindAggregateFunction(store, functionCall.toIndexAggregateFunction(null), recordTypeNames, indexFilter);
        }
    }

    static class IndexRecordFunctionWithSubrecordValues<T> extends IndexRecordFunction<T> {
        private final int scalarPrefixCount;
        @Nonnull
        private final QueryToKeyMatcher.Match match;

        protected IndexRecordFunctionWithSubrecordValues(@Nonnull IndexRecordFunction<T> recordFunction, @Nonnull Index index,
                                                         int scalarPrefixCount, @Nonnull QueryToKeyMatcher.Match match) {
            super(recordFunction.getName(), recordFunction.getOperand(), index.getName());
            this.scalarPrefixCount = scalarPrefixCount;
            this.match = match;
        }

        public int getScalarPrefixCount() {
            return scalarPrefixCount;
        }

        public Key.Evaluated getValues(@Nonnull FDBRecordStore store, @Nonnull EvaluationContext context) {
            return match.getEquality(store, context);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            final IndexRecordFunctionWithSubrecordValues<?> that = (IndexRecordFunctionWithSubrecordValues<?>)o;
            return scalarPrefixCount == that.scalarPrefixCount && match.equals(that.match);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), scalarPrefixCount, match);
        }
    }

    /**
     * Given an index record function and condition on repeated fields (such as a key for a map-like field), return a function suitable for use with
     * {@link #recordFunctionIndexEntry} to get the matching index entry.
     * @param store store against which function will be evaluated
     * @param recordFunction function to be evaluated
     * @param record record against which to evaluate
     * @param condition condition on fields of index entry
     * @param <T> return type of function
     * @return a new function that remembers the condition for matching
     */
    @Nonnull
    public static <T> IndexRecordFunction<T> recordFunctionWithSubrecordCondition(@Nonnull FDBRecordStore store,
                                                                                  @Nonnull IndexRecordFunction<T> recordFunction,
                                                                                  @Nonnull FDBRecord<?> record,
                                                                                  @Nonnull QueryComponent condition) {
        final IndexMaintainer indexMaintainer = indexMaintainerForRecordFunction(store, recordFunction, record)
                .orElseThrow(() -> new RecordCoreException("Record function " + recordFunction +
                                                           " requires appropriate index on " + record.getRecordType().getName()));
        final Index index = indexMaintainer.state.index;
        final List<KeyExpression> indexFields = index.getRootExpression().normalizeKeyForPositions();
        int scalarPrefixCount = 0;
        KeyExpression firstRepeated = null;
        for (KeyExpression indexField : indexFields) {
            if (indexField.createsDuplicates()) {
                firstRepeated = indexField;
                break;
            }
            scalarPrefixCount++;
        }
        if (firstRepeated == null) {
            throw new RecordCoreException("Record function " + recordFunction +
                                          " condition " + condition +
                                          " does not select a repeated field in " + indexMaintainer.state.index.getName());
        }
        final QueryToKeyMatcher matcher = new QueryToKeyMatcher(condition);
        final QueryToKeyMatcher.Match match = matcher.matchesSatisfyingQuery(firstRepeated);
        if (match.getType() != QueryToKeyMatcher.MatchType.EQUALITY) {
            throw new RecordCoreException("Record function " + recordFunction +
                                          " condition " + condition +
                                          " does not match " + indexMaintainer.state.index.getName());
        }
        return new IndexRecordFunctionWithSubrecordValues<>(recordFunction, index, scalarPrefixCount, match);
    }

    /**
     * Get the index entry for use by the given index to evaluate the given record function.
     *
     * In most cases, this is the same as {@link KeyExpression#evaluateSingleton}. But if {@code recordFunction} is
     * the result of {@link #recordFunctionWithSubrecordCondition}, a matching entry will be found.
     * @param store store against which function will be evaluated
     * @param index index for which to evaluate
     * @param context context for parameter bindings
     * @param recordFunction record function for which to evaluate
     * @param record record against which to evaluate
     * @param groupSize grouping size for the given index
     * @return an index entry or {@code null} if none matches a bound condition
     */
    @Nullable
    public static Key.Evaluated recordFunctionIndexEntry(@Nonnull FDBRecordStore store,
                                                         @Nonnull Index index,
                                                         @Nonnull EvaluationContext context,
                                                         @Nullable IndexRecordFunction<?> recordFunction,
                                                         @Nonnull FDBRecord<?> record,
                                                         int groupSize) {
        final KeyExpression expression = index.getRootExpression();
        if (!(recordFunction instanceof IndexRecordFunctionWithSubrecordValues)) {
            return expression.evaluateSingleton(record);
        }
        final IndexRecordFunctionWithSubrecordValues<?> recordFunctionWithSubrecordValues = (IndexRecordFunctionWithSubrecordValues<?>)recordFunction;
        final int scalarPrefixCount = recordFunctionWithSubrecordValues.getScalarPrefixCount();
        final List<Object> toMatch = recordFunctionWithSubrecordValues.getValues(store, context).values();
        List<Object> prev = null;
        Key.Evaluated match = null;
        for (Key.Evaluated key : expression.evaluate(record)) {
            final List<Object> subrecord = key.values();
            for (int i = 0; i < groupSize; i++) {
                if (i < scalarPrefixCount) {
                    if (prev != null) {
                        if (!Objects.equals(prev.get(i), subrecord.get(i))) {
                            throw new RecordCoreException("All subrecords should match for non-constrained keys");
                        }
                    }
                } else {
                    if (toMatch.get(i - scalarPrefixCount).equals(subrecord.get(i))) {
                        if (match != null) {
                            throw new RecordCoreException("More than one matching subrecord");
                        }
                        match = key;
                    }
                }
            }
            prev = subrecord;
        }
        return match;
    }
}
