/*
 * ViewExpressionComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A data structure that represents a {@link ViewExpression} with a {@link ComparisonRange} on some of the
 * {@link Element}s in its sort order.
 *
 * <p>
 * A {@code ViewExpressionComparisons} is an analogue of a {@link ScanComparisons} that maintains information about which
 * elements in the sort order of a {@link ViewExpression} correspond to each comparison. It maintains a list of
 * {@link ComparisonRange}s for each of the elements. The {@link #matchWith(ElementPredicate)} method can then take
 * a {@link ElementPredicate} and try to add it to the existing comparisons.
 * </p>
 *
 * <p>
 * The {@code ViewExpressionComparisons} encapsulates the core logic of marking parts of a complex key expression as
 * satisfied by various types of {@code Comparisons.Comparison}s and attempting to match more comparisons to the
 * remaining parts of the expression. This includes the "virtual" {@code Comparisons.SortComparison} which is used only
 * to represent requested sort orders for index planning.
 * </p>
 *
 * <p>
 * While this sounds a lot like a whole query planner, the scope of a {@code ViewExpressionComparisons} is intentionally
 * very limited. In particular, it supports matching with an {@link ElementPredicate} rather than a more
 * general {@link com.apple.foundationdb.record.query.predicates.QueryPredicate} As a result, the logic for handling
 * Boolean operations and other complexities does not belong in {@code ViewExpressionComparisons}.
 * </p>
 */

public class ViewExpressionComparisons {
    @Nonnull
    private final ViewExpression viewExpression;
    @Nonnull
    private final List<ComparisonRange> comparisonRanges;
    @Nonnull
    private final Set<Source> sourcesWithComparisons;

    public ViewExpressionComparisons(@Nonnull ViewExpression viewExpression) {
        this.viewExpression = viewExpression;
        this.comparisonRanges = ImmutableList.of();
        this.sourcesWithComparisons = ImmutableSet.of();

    }

    private ViewExpressionComparisons(@Nonnull ViewExpression viewExpression,
                                      @Nonnull List<ComparisonRange> comparisonRanges,
                                      @Nonnull Set<Source> sourcesWithComparisons) {
        this.viewExpression = viewExpression;
        this.comparisonRanges = comparisonRanges;
        this.sourcesWithComparisons = sourcesWithComparisons;
    }

    @Nonnull
    public Optional<ViewExpressionComparisons> matchWith(@Nonnull ElementPredicate component) {
        Element currentElement;
        for (int currentChild = 0; currentChild < viewExpression.getOrderBy().size(); currentChild++) {
            currentElement = viewExpression.getOrderBy().get(currentChild);
            ComparisonRange currentComparisonRange = getComparisonRangeAt(currentChild);
            final Optional<ComparisonRange> possibleRange = currentElement.matchWith(currentComparisonRange, component);
            if (possibleRange.isPresent()) {
                // Found a match, so stop looking.
                return Optional.of(withComparisonRangeAt(currentChild, possibleRange.get()));
            }

            Optional<ViewExpressionComparisons> matched = currentElement.matchSourcesWith(this, component.getElement());
            if (matched.isPresent()) {
                final Optional<ComparisonRange> possibleRange2 = ComparisonRange.EMPTY.tryToAdd(component.getComparison());
                final int index = currentChild;
                return possibleRange2.map(range -> matched.get().withComparisonRangeAt(index, range));
            }

            if (!currentComparisonRange.getRangeType().equals(ComparisonRange.Type.EQUALITY)) {
                // One of the following cases applies:
                // (1) There's no recorded match for this child. We have to match the current component to this
                //     child, otherwise we can't match it at all (at least until other matches happen first).
                // (2) We already have an inequality match for this child. We can try to match the current child, but
                //     we can't add comparisons to a later child, so we have to stop if this match fails.
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Nonnull
    public Optional<ViewExpressionComparisons> matchWithSort(@Nonnull List<Element> sortOrder) {
        Optional<ViewExpressionComparisons> incremental = Optional.of(this);
        for (Element element : sortOrder) {
            if (!incremental.isPresent()) {
                return incremental;
            }
            incremental = incremental.get().matchWith(new ElementPredicate(element, SortComparison.SORT)); // TODO inefficient
        }
        return incremental;
    }

    @Nonnull
    private ComparisonRange getComparisonRangeAt(int index) {
        if (index < comparisonRanges.size()) {
            return comparisonRanges.get(index);
        } else {
            return ComparisonRange.EMPTY;
        }
    }

    @Nonnull
    public boolean hasComparison(@Nonnull Source source) {
        return sourcesWithComparisons.contains(source);
    }

    public int getUnmatchedFieldCount() {
        int count = viewExpression.getOrderBy().size();
        for (ComparisonRange range : comparisonRanges) {
            if (!range.isEmpty()) {
                count--;
            }
        }
        return count;
    }

    public boolean hasOrderBySourceWithoutComparison() {
        return !viewExpression.getOrderBy().stream()
                .flatMap(element -> element.getAncestralSources().stream())
                .allMatch(this::hasComparison);
    }

    @Nonnull
    private ViewExpressionComparisons withComparisonRangeAt(int index, @Nonnull ComparisonRange comparisonRange) {
        final Set<Source> newSourcesWithComparisons;
        if (index >= comparisonRanges.size()) { // possibly have a new source
            newSourcesWithComparisons = ImmutableSet.<Source>builderWithExpectedSize(comparisonRanges.size() + 1)
                    .addAll(sourcesWithComparisons)
                    .addAll(viewExpression.getOrderBy().get(index).getAncestralSources())
                    .build();
        } else {
            newSourcesWithComparisons = sourcesWithComparisons;
        }

        final ImmutableList.Builder<ComparisonRange> newRanges =
                ImmutableList.<ComparisonRange>builderWithExpectedSize(comparisonRanges.size())
                        .addAll(comparisonRanges.subList(0, index))
                        .add(comparisonRange);
        if (index < comparisonRanges.size()) {
            newRanges.addAll(comparisonRanges.subList(index + 1, comparisonRanges.size()));
        }
        return new ViewExpressionComparisons(viewExpression, newRanges.build(), newSourcesWithComparisons);
    }

    /**
     * Replace the given duplicate source with the given original source everywhere that it occurs in these view
     * expression comparisons.
     * @param originalSource a source to replace all occurrences of the duplicate source with
     * @param duplicateSource a source to replace with the original source
     * @return a copy of these comparisons with all occurrences of the duplicate source replaced with the original source
     */
    @Nonnull
    public ViewExpressionComparisons withSourcesMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        return new ViewExpressionComparisons(
                viewExpression.withSourceMappedInto(originalSource, duplicateSource),
                comparisonRanges,
                sourcesWithComparisons.stream()
                        .map(source -> source.withSourceMappedInto(originalSource, duplicateSource))
                        .collect(Collectors.toSet()));
    }

    /**
     * Convert these comparisons to {@link ScanComparisons} for query execution.
     * @return these comparisons as {@code ScanComparisons}
     */
    @Nonnull
    public ScanComparisons toScanComparisons() {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }
}

