/*
 * IndexAggregateFunctionCall.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.BaseField;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Class to represent a construct {@code AGG by Set.of(col1, col2, ...)}. This is the unbound equivalent of
 * an {@link IndexAggregateFunction} which itself is bound to an index and represents its group-by columns as a list of
 * {@link KeyExpression} that match the order imposed by bound index rather than a set.
 *
 * There are two use cases for this class:
 * <ol>
 * <li> regular aggregation functions that need the enumeration of permutation of the grouping expressions in order to
 *      bind the call to a {@link IndexAggregateFunction}. The logic implementing this use case calls
 *      {@link #enumerateIndexAggregateFunctionCandidates(String)} at some point.
 * </li>
 * <li> aggregation-oriented operators that function more like closures over a group (see
 *      {@link com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexAggregate} for such an example)
 *      in which cases it may be needed to propagate the original {@link GroupingKeyExpression} to the binding phase
 *      thus eliminating the possibility of an enumeration of permutations of grouping columns altogether.
 *      In those cases it may be simply impossible to compute an equivalent list of such grouping columns from the original
 *      grouping key expression without losing some of the nesting/repeating information.
 * </li>
 * </ol>
 *
 * Note that if a client of this class intends to use an aggregate function call for regular aggregation functions and
 * intends to call methods such as {@link #enumerateIndexAggregateFunctionCandidates(String)}, the following must hold
 * in order to yield meaningful results:
 *
 * <pre>
 * {@code groupingKeyExpression = concat(groupingExpressions, groupedExpression).group(groupedExpression.getColumnSize())}
 * </pre>
 *
 * which ideally is true, but may be violated in the presence of repeated structures.
 */
@SuppressWarnings("unused")
@API(API.Status.INTERNAL)
public class IndexAggregateFunctionCall {
    /**
     * Function name used in this function call.
     */
    @Nonnull
    private final String functionName;

    /**
     * Grouping key expression that holds information over both the grouping and the grouped part of the expression
     * this function call ranges over.
     *
     * In the normal use case this expression is computed by reconstructing a {@link GroupingKeyExpression} using
     * the iterable of grouping expressions and the grouped expression that is handed in (to certain overloaded
     * constructors).
     *
     * If required, the client can use {@link #IndexAggregateFunctionCall(String, GroupingKeyExpression)} in order
     * to invert that logic in which case the grouping key expressions and the grouped expression is computed
     * from the {@link GroupingKeyExpression} passed in to the respective constructor. Additionally, this field
     * holds the original grouping key expression handed in thus preserving the intricate structure of nested repeated
     * fields, etc.
     *
     * See {@link #toIndexAggregateFunction(String)} which uses this field directly.
     */
    @Nonnull
    private final GroupingKeyExpression groupingKeyExpression;

    /**
     * A set of grouping expressions. Note that the implementing set is an instance of class {@link ImmutableSet} which
     * provides iteration in insertion order. That property may be useful for clients that attempt to bind
     * calls to index aggregate functions using the grouping keys as queried (written).
     */
    @Nonnull
    private final Set<KeyExpression> groupingExpressions;

    /**
     * The grouped expression, i.e., the expression that captures what is computed per group.
     */
    @Nonnull
    private final KeyExpression groupedExpression;

    /**
     * Indicator signalling if the grouping expressions can be permutated in this function call without changing its
     * semantics.
     */
    private final boolean isGroupingPermutable;

    /**
     * Overloaded constructor using a {@link GroupingKeyExpression} that represents both the grouping expressions and
     * the grouped expression. The constructor is necessary to allow function calls that can use nested repeated
     * structures to adequately represent function calls over those structures.
     * @param functionName function name of the call
     * @param groupingKeyExpression the grouping key expression
     */
    public IndexAggregateFunctionCall(@Nonnull String functionName,
                                      @Nonnull GroupingKeyExpression groupingKeyExpression) {
        this(functionName,
                groupingKeyExpression,
                groupingKeyExpression.getGroupingSubKey().normalizeKeyForPositions(),
                groupingKeyExpression.getGroupedSubKey());
    }

    protected IndexAggregateFunctionCall(@Nonnull String functionName,
                                         @Nonnull GroupingKeyExpression groupingKeyExpression,
                                         @Nonnull Iterable<KeyExpression> groupingExpressions,
                                         @Nonnull KeyExpression groupedExpression) {
        this.functionName = functionName;
        this.groupingKeyExpression = groupingKeyExpression;
        // note that Guava's ImmutableSet iterates in insertion order
        this.groupingExpressions = groupingKeyExpression.getGroupingCount() == 0
                                   ? ImmutableSet.of()
                                   : ImmutableSet.copyOf(groupingExpressions);
        this.groupedExpression = groupedExpression;
        this.isGroupingPermutable = groupingKeyExpression.getGroupingSubKey().equals(EmptyKeyExpression.EMPTY) ||
                                    groupingKeyExpression.hasLosslessNormalization();
    }

    @Nonnull
    public String getFunctionName() {
        return functionName;
    }

    @Nonnull
    public GroupingKeyExpression getGroupingKeyExpression() {
        return groupingKeyExpression;
    }

    @Nonnull
    public Set<KeyExpression> getGroupingExpressions() {
        return groupingExpressions;
    }

    @Nonnull
    public KeyExpression getGroupedExpression() {
        return groupedExpression;
    }

    public boolean isGroupingPermutable() {
        return isGroupingPermutable;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexAggregateFunctionCall)) {
            return false;
        }
        final IndexAggregateFunctionCall that = (IndexAggregateFunctionCall)o;
        return Objects.equal(getFunctionName(),
                that.getFunctionName()) &&
               Objects.equal(getGroupingKeyExpression(), that.getGroupingKeyExpression()) &&
               Objects.equal(getGroupingExpressions(), that.getGroupingExpressions()) &&
               Objects.equal(getGroupedExpression(), that.getGroupedExpression());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getFunctionName(), getGroupingKeyExpression(), getGroupingExpressions(), getGroupedExpression());
    }

    @Nonnull
    public IndexAggregateFunctionCall withNewGroupingKeyExpression(@Nonnull final GroupingKeyExpression groupingKeyExpression) {
        return new IndexAggregateFunctionCall(getFunctionName(), groupingKeyExpression);
    }

    @Nonnull
    public IndexAggregateFunctionCall withNewExpressions(@Nonnull List<KeyExpression> groupingKeysPermutation, @Nonnull KeyExpression groupedExpression) {
        return withNewGroupingKeyExpression(toGroupingKeyExpression(groupingKeysPermutation, groupedExpression));
    }

    @Nonnull
    public QueryComponent applyCondition(@Nonnull QueryComponent filterCondition) {
        return filterCondition;
    }

    /**
     * Method to create a stream of enumerated {@link IndexAggregateFunction}s that represent the bound versions of this
     * function call.
     * In order to bind the function call, we need to associate the aggregate function call with an index and also
     * pin the order of grouping key expressions in a way that is compatible with that index. Note that the returned
     * index aggregate functions are merely candidates for actual index-compatible ones as we don't check here the
     * conditions that constitute a compatible index. On the contrary, this method is intended to be used by the logic
     * that builds the actual function call for later consumption.
     * @param indexName indexName to use
     * @return a {@link Stream} of {@link IndexAggregateFunction}s
     */
    public Stream<IndexAggregateFunction> enumerateIndexAggregateFunctionCandidates(@Nonnull String indexName) {
        if (isGroupingPermutable) {
            final EnumeratingIterable<KeyExpression> groupingPermutations =
                    TopologicalSort.permutations(getGroupingExpressions());
            return StreamSupport.stream(groupingPermutations.spliterator(), false)
                    .map(groupingPermutation -> toIndexAggregateFunction(indexName, groupingPermutation));
        } else {
            return Stream.of(toIndexAggregateFunction(indexName));
        }
    }

    /**
     * Method to create a {@link IndexAggregateFunction} that represents the bound version of this function call.
     * In order to bind the function call, we need to associate the aggregate function call with an index and also
     * pin the order of grouping key expressions in a way that is compatible with that index.
     * @param indexName index to use
     * @param groupingKeysPermutation permutation of key expressions to use that is compatible with the aggregate index
     * @return a new {@link IndexAggregateFunction}
     */
    @Nonnull
    protected IndexAggregateFunction toIndexAggregateFunction(@Nonnull String indexName, @Nonnull List<KeyExpression> groupingKeysPermutation) {
        return toIndexAggregateFunction(indexName, toGroupingKeyExpression(groupingKeysPermutation));
    }

    /**
     * Method to create a {@link IndexAggregateFunction} that represents the bound version of this function call.
     * In order to bind the function call, we need to associate the aggregate function call with an index and also
     * pin the order of grouping key expressions in a way that is compatible with that index. The order of the grouping
     * key expressions is implicitly given by the structure of the {@code groupingKeyExpression}.
     * @param indexName index to use, index is allowed to be {@code null} as some aggregate functions scan more than one index
     * @return a new {@link IndexAggregateFunction}
     */
    @Nonnull
    public IndexAggregateFunction toIndexAggregateFunction(@Nullable String indexName) {
        return toIndexAggregateFunction(indexName, groupingKeyExpression);
    }

    /**
     * Method to create a {@link IndexAggregateFunction} that represents the bound version of this function call.
     * In order to bind the function call, we need to associate the aggregate function call with an index and also
     * pin the order of grouping key expressions in a way that is compatible with that index. The order of the grouping
     * key expressions is implicitly given by the structure of the {@code groupingKeyExpression}.
     * @param indexName index to use, index is allowed to be {@code null} as some aggregate functions scan more than one index
     * @param groupingKeyExpression the grouping key expression to use
     * @return a new {@link IndexAggregateFunction}
     */
    @Nonnull
    protected IndexAggregateFunction toIndexAggregateFunction(@Nullable String indexName, @Nonnull GroupingKeyExpression groupingKeyExpression) {
        return new IndexAggregateFunction(getFunctionName(), groupingKeyExpression, indexName);
    }

    @Nonnull
    protected GroupingKeyExpression toGroupingKeyExpression(@Nonnull List<KeyExpression> groupingKeysPermutation) {
        return toGroupingKeyExpression(groupingKeysPermutation, groupedExpression);
    }

    @Nonnull
    public static GroupingKeyExpression toGroupingKeyExpression(@Nonnull List<KeyExpression> groupingKeysPermutation, @Nonnull KeyExpression groupedExpression) {
        final KeyExpression keyPart;

        if (groupingKeysPermutation.isEmpty()) {
            keyPart = EmptyKeyExpression.EMPTY;
        } else if (groupingKeysPermutation.size() == 1) {
            keyPart = Iterables.getOnlyElement(groupingKeysPermutation);
        } else {
            keyPart = Key.Expressions.concat(groupingKeysPermutation);
        }

        if (groupedExpression.getColumnSize() == 0) {
            // empty grouped expression
            return new GroupingKeyExpression(keyPart, 0);
        } else {
            return Key.Expressions.concat(keyPart, groupedExpression).group(groupedExpression.getColumnSize());
        }
    }

    /**
     * Helper method to extract a set of key expressions that are bound through an equality comparison in the
     * query component passed in.
     * @param queryComponent the query component
     * @return a set of {@link KeyExpression}s where each element is a key expression of a field (i.e. a
     * {@link com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression}) or a simple nesting field
     * (i.e. a {@link com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression}) that is bound
     * through an equality comparison.
     */
    @Nonnull
    public static Set<KeyExpression> extractEqualityBoundFields(@Nonnull QueryComponent queryComponent) {
        return extractFieldPaths(queryComponent,
                fieldWithComparison -> fieldWithComparison.getComparison().getType() == Comparisons.Type.EQUALS ||
                                       fieldWithComparison.getComparison().getType() == Comparisons.Type.IS_NULL);
    }

    /**
     * Helper method to extract a set of key expressions that are bound through some comparison in the
     * query component passed in.
     * @param queryComponent the query component
     * @param predicate a predicate used for filtering each encountered {@link FieldWithComparison}
     * @return a set of {@link KeyExpression}s where each element is a key expression of a field (i.e. a
     * {@link com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression}) or a simple nesting field
     * (i.e. a {@link com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression}) that is bound
     * through some comparison
     */
    @Nonnull
    public static Set<KeyExpression> extractFieldPaths(@Nonnull QueryComponent queryComponent, @Nonnull final Predicate<FieldWithComparison> predicate) {
        if (queryComponent instanceof BaseField) {
            final BaseField baseField = (BaseField)queryComponent;
            if (baseField instanceof NestedField) {
                final NestedField nestedField = (NestedField)baseField;
                final Set<KeyExpression> nestedExpressions = extractFieldPaths(nestedField.getChild(), predicate);
                return nestedExpressions.stream()
                        .map(nestedExpression -> Key.Expressions.field(nestedField.getFieldName()).nest(nestedExpression))
                        .collect(ImmutableSet.toImmutableSet());
            }
            if (baseField instanceof FieldWithComparison) {
                final FieldWithComparison fieldWithComparison = (FieldWithComparison)baseField;

                if (predicate.test(fieldWithComparison)) {
                    return ImmutableSet.of(Key.Expressions.field(fieldWithComparison.getFieldName()));
                }
            }
            return ImmutableSet.of();
        } else if (queryComponent instanceof AndComponent) {
            final Set<KeyExpression> boundFields = Sets.newHashSet();
            final AndComponent andComponent = (AndComponent)queryComponent;
            andComponent.getChildren().forEach(child -> boundFields.addAll(extractEqualityBoundFields(child)));
            return boundFields;
        }
        return ImmutableSet.of();
    }
}
