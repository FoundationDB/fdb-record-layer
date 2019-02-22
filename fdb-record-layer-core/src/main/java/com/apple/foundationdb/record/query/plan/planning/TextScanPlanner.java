/*
 * TextScanPlanner.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.TextScan;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;

/**
 * A utility class for choosing a {@link TextScan} object to satisfy a
 * text query.
 */
@API(API.Status.INTERNAL)
public class TextScanPlanner {

    private static final ImmutableSet<Comparisons.Type> COMPARISONS_NOT_REQUIRING_POSITIONS = ImmutableSet.of(
            Comparisons.Type.TEXT_CONTAINS_ALL,
            Comparisons.Type.TEXT_CONTAINS_ANY,
            Comparisons.Type.TEXT_CONTAINS_PREFIX
    );

    /**
     * Determine if the index is using a tokenizer that matches the comparison.
     * If the comparison does not specify a text tokenizer, then whatever the
     * index has is good enough. If the comparison does specify a tokenizer,
     * then this makes sure that the index has a name that matches.
     *
     * @param comparison the comparison which might restrict the tokenizer choice
     * @param index the index to check if it has a compatible tokenizer
     * @return <code>true</code> if the index uses a tokenizer that the comparison finds acceptable
     */
    private static boolean matchesTokenizer(@Nonnull Comparisons.TextComparison comparison, @Nonnull Index index) {
        if (comparison.getTokenizerName() != null) {
            String indexTokenizerName = index.getOption(IndexOptions.TEXT_TOKENIZER_NAME_OPTION);
            if (indexTokenizerName == null) {
                indexTokenizerName = DefaultTextTokenizer.NAME;
            }
            return comparison.getTokenizerName().equals(indexTokenizerName);
        } else {
            return true;
        }
    }

    /**
     * Returns whether the index has enough position information to satisfy the query.
     * Some text queries do not require the position list information that is stored
     * by default by text indexes. If this is the case, then it returns <code>true</code>
     * regardless of the properties of the index. If the query <i>does</i> require position
     * information (e.g., phrase queries), then this returns <code>true</code> if and only
     * if the index stores positions, i.e., if it does <i>not</i> set the
     * "{@value IndexOptions#TEXT_OMIT_POSITIONS_OPTION}" option to <code>true</code>.
     *
     * @param comparison the comparison which might require position information
     * @param index the index to check the position option of
     * @return <code>true</code> if the index contains enough position information for the given comparison
     */
    private static boolean containsPositionsIfNecessary(@Nonnull Comparisons.TextComparison comparison, @Nonnull Index index) {
        return COMPARISONS_NOT_REQUIRING_POSITIONS.contains(comparison.getType()) || !index.getBooleanOption(IndexOptions.TEXT_OMIT_POSITIONS_OPTION, false);
    }

    @Nullable
    private static TextScan getScanForField(@Nonnull Index index, @Nonnull FieldKeyExpression textExpression, @Nonnull FieldWithComparison filter,
                                            @Nullable ScanComparisons groupingComparisons, boolean hasSort, @Nullable FilterSatisfiedMask filterMask) {
        final Comparisons.TextComparison comparison;
        if (filter.getComparison() instanceof Comparisons.TextComparison) {
            comparison = (Comparisons.TextComparison) filter.getComparison();
        } else {
            return null;
        }
        if (!matchesTokenizer(comparison, index) || !containsPositionsIfNecessary(comparison, index)) {
            return null;
        }
        if (hasSort) {
            // Inequality text comparisons will return results sorted
            // by token, so reasoning about any kind of sort except
            // maybe by the (equality) grouping key is hard.
            return null;
        }
        if (filter.getFieldName().equals(textExpression.getFieldName())) {
            // Found matching expression
            if (filterMask != null) {
                filterMask.setSatisfied(true);
                filterMask.setExpression(textExpression);
            }
            return new TextScan(index, groupingComparisons, comparison, null);
        }
        return null;
    }

    @Nullable
    private static TextScan getScanForAndFilter(@Nonnull Index index, @Nonnull KeyExpression textExpression, @Nonnull AndComponent filter,
                                                @Nullable ScanComparisons groupingComparisons, boolean hasSort, @Nullable FilterSatisfiedMask filterMask) {
        // Iterate through each of the filters
        final Iterator<FilterSatisfiedMask> subFilterMasks = filterMask != null ? filterMask.getChildren().iterator() : null;
        for (QueryComponent subFilter : filter.getChildren()) {
            final FilterSatisfiedMask childMask = subFilterMasks != null ? subFilterMasks.next() : null;
            TextScan childScan = getScanForFilter(index, textExpression, subFilter, groupingComparisons, hasSort, childMask);
            if (childScan != null) {
                if (filterMask != null && filterMask.getExpression() == null) {
                    filterMask.setExpression(textExpression);
                }
                return childScan;
            }
        }
        return null;
    }

    @Nullable
    private static TextScan getScanForNestedField(@Nonnull Index index, @Nonnull NestingKeyExpression textExpression, @Nonnull NestedField filter,
                                                  @Nullable ScanComparisons groupingComparisons, boolean hasSort, @Nullable FilterSatisfiedMask filterMask) {
        if (textExpression.getParent().getFanType().equals(KeyExpression.FanType.None)
                && textExpression.getParent().getFieldName().equals(filter.getFieldName())) {
            final FilterSatisfiedMask childMask = filterMask != null ? filterMask.getChild(filter.getChild()) : null;
            TextScan foundScan = getScanForFilter(index, textExpression.getChild(), filter.getChild(), groupingComparisons, hasSort, childMask);
            if (foundScan != null && filterMask != null && filterMask.getExpression() == null) {
                filterMask.setExpression(textExpression);
            }
            return foundScan;
        }
        return null;
    }

    @Nullable
    private static TextScan getScanForRepeatedNestedField(@Nonnull Index index, @Nonnull NestingKeyExpression textExpression, @Nonnull OneOfThemWithComponent filter,
                                                          @Nullable ScanComparisons groupingComparisons, boolean hasSort, @Nullable FilterSatisfiedMask filterMask) {

        if (textExpression.getParent().getFanType().equals(KeyExpression.FanType.FanOut)
                && textExpression.getParent().getFieldName().equals(filter.getFieldName())) {
            // This doesn't work for certain complex queries on child fields.
            // There are more details in this issue and there is an example of mis-planned query in the
            // unit tests for this class:
            // FIXME: Full Text: The Planner doesn't always correctly handle ands with nesteds (https://github.com/FoundationDB/fdb-record-layer/issues/53)
            FilterSatisfiedMask childMask = filterMask != null ? filterMask.getChild(filter.getChild()) : null;
            TextScan foundScan = getScanForFilter(index, textExpression.getChild(), filter.getChild(), groupingComparisons, hasSort, childMask);
            if (foundScan != null && filterMask != null && filterMask.getExpression() != null) {
                filterMask.setExpression(textExpression);
            }
            return foundScan;
        }
        return null;
    }

    @Nullable
    private static TextScan getScanForFilter(@Nonnull Index index, @Nonnull KeyExpression textExpression, @Nonnull QueryComponent filter,
                                             @Nullable ScanComparisons groupingComparisons, boolean hasSort, @Nullable FilterSatisfiedMask filterMask) {
        if (filter instanceof AndComponent) {
            return getScanForAndFilter(index, textExpression, (AndComponent)filter, groupingComparisons, hasSort, filterMask);
        } else if (textExpression instanceof FieldKeyExpression && filter instanceof FieldWithComparison) {
            return getScanForField(index, (FieldKeyExpression)textExpression, (FieldWithComparison)filter, groupingComparisons, hasSort, filterMask);
        } else if (textExpression instanceof NestingKeyExpression) {
            if (filter instanceof NestedField) {
                return getScanForNestedField(index, (NestingKeyExpression)textExpression, (NestedField)filter, groupingComparisons, hasSort, filterMask);
            } else if (filter instanceof OneOfThemWithComponent) {
                return getScanForRepeatedNestedField(index, (NestingKeyExpression)textExpression, (OneOfThemWithComponent)filter, groupingComparisons, hasSort, filterMask);

            }
        }
        return null;
    }

    /**
     * Get a scan that matches a filter in the list of filters provided. It looks to satisfy the grouping
     * key of the index, and then it looks for a text filter within the list of filters and checks to
     * see if the given index is compatible with the filter. If it is, it will construct a scan that
     * satisfies that filter using the index.
     *
     * @param index the text index to check
     * @param filter a filter that the query must satisfy
     * @param hasSort whether the query has a sort associated with it
     * @param filterMask a mask over the filter containing state about which filters have been satisfied
     * @return a text scan or <code>null</code> if none is found
     */
    @Nullable
    public static TextScan getScanForQuery(@Nonnull Index index, @Nonnull QueryComponent filter, boolean hasSort, @Nonnull FilterSatisfiedMask filterMask) {
        final KeyExpression indexExpression = index.getRootExpression();
        final KeyExpression groupedKey;
        final FilterSatisfiedMask localMask = FilterSatisfiedMask.of(filter);
        final ScanComparisons groupingComparisons;
        if (indexExpression instanceof GroupingKeyExpression) {
            // Grouping expression present. Make sure this is satisfied.
            final KeyExpression groupingKey = ((GroupingKeyExpression)indexExpression).getGroupingSubKey();
            groupedKey = ((GroupingKeyExpression)indexExpression).getGroupedSubKey();
            QueryToKeyMatcher groupingQueryMatcher = new QueryToKeyMatcher(filter);
            QueryToKeyMatcher.Match groupingMatch = groupingQueryMatcher.matchesCoveringKey(groupingKey, localMask);
            if (!groupingMatch.getType().equals(QueryToKeyMatcher.MatchType.EQUALITY)) {
                return null;
            }
            groupingComparisons = new ScanComparisons(groupingMatch.getEqualityComparisons(), Collections.emptyList());
        } else {
            // Grouping expression not present. Use first column.
            groupedKey = indexExpression;
            groupingComparisons = null;
        }

        final KeyExpression textExpression = groupedKey.getSubKey(0, 1);
        final TextScan foundScan = getScanForFilter(index, textExpression, filter, groupingComparisons, hasSort, localMask);
        if (foundScan != null) {
            filterMask.mergeWith(localMask);
            return foundScan;
        }
        return null;
    }

    private TextScanPlanner() {
        // This is a utility class containing only static methods
    }
}
