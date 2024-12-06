/*
 * MaxMatchMap.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.MaxMatchMapSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents a max match between a (rewritten) query result {@link Value} and the candidate result {@link Value}.
 */
public class MaxMatchMap {
    @Nonnull
    private final BiMap<Value, Value> mapping;
    @Nonnull
    private final Value queryResultValue; // in terms of the candidate quantifiers.
    @Nonnull
    private final Value candidateResultValue;
    @Nonnull
    private final QueryPlanConstraint queryPlanConstraint;
    @Nonnull
    private final ValueEquivalence valueEquivalence;

    /**
     * Creates a new instance of {@link MaxMatchMap}.
     * @param mapping the {@link Value} mapping
     * @param queryResult the query result from which the mapping keys originate
     * @param candidateResult the candidate result from which the mapping values originate
     * @param valueEquivalence a {@link ValueEquivalence} that was used to match up query and candidate values
     */
    MaxMatchMap(@Nonnull final Map<Value, Value> mapping,
                @Nonnull final Value queryResult,
                @Nonnull final Value candidateResult,
                @Nonnull final QueryPlanConstraint queryPlanConstraint,
                @Nonnull final ValueEquivalence valueEquivalence) {
        this.mapping = ImmutableBiMap.copyOf(mapping);
        this.queryResultValue = queryResult;
        this.candidateResultValue = candidateResult;
        this.queryPlanConstraint = queryPlanConstraint;
        this.valueEquivalence = valueEquivalence;
    }

    @Nonnull
    public Map<Value, Value> getMap() {
        return mapping;
    }

    @Nonnull
    public Value getCandidateResultValue() {
        return candidateResultValue;
    }

    @Nonnull
    public Value getQueryResultValue() {
        return queryResultValue;
    }

    @Nonnull
    public QueryPlanConstraint getQueryPlanConstraint() {
        return queryPlanConstraint;
    }

    @Nonnull
    public ValueEquivalence getValueEquivalence() {
        return valueEquivalence;
    }

    @Nonnull
    public Optional<Value> translateQueryValueMaybe(@Nonnull final CorrelationIdentifier candidateAlias) {
        final var candidateResultValue = getCandidateResultValue();
        final var pulledUpCandidateSide =
                candidateResultValue.pullUp(mapping.values(),
                        AliasMap.emptyMap(),
                        ImmutableSet.of(), candidateAlias);
        //
        // We now have the right side pulled up, specifically we have a map from each candidate value below,
        // to a candidate value pulled up along the candidateAlias. We also have this max match map, which
        // encapsulates a map from query values to candidate value.
        // In other words we have in this max match map m1 := MAP(queryValues over q -> candidateValues over q') and
        // we just computed m2 := MAP(candidateValues over p' -> candidateValues over candidateAlias). We now
        // chain these two maps to get m1 ○ m2 := MAP(queryValues over q -> candidateValues over candidateAlias).
        // As we will use this map in the subsequent step to look up values over semantic equivalency using
        // equivalencesMap, we immediately create m1 ○ m2 using a boundEquivalence based on equivalencesMap.
        //
        final var pulledUpMaxMatchMapBuilder =
                ImmutableMap.<Value, Value>builder();
        for (final var entry : mapping.entrySet()) {
            final var queryPart = entry.getKey();
            final var candidatePart = entry.getValue();
            final var pulledUpdateCandidatePart = pulledUpCandidateSide.get(candidatePart);
            if (pulledUpdateCandidatePart == null) {
                return Optional.empty();
            }
            pulledUpMaxMatchMapBuilder.put(queryPart, pulledUpdateCandidatePart);
        }
        final var pulledUpMaxMatchMap = pulledUpMaxMatchMapBuilder.build();

        final var queryResultValueFromBelow = getQueryResultValue();
        final var translatedQueryResultValue = Objects.requireNonNull(queryResultValueFromBelow.replace(value -> {
            final var maxMatchValue = pulledUpMaxMatchMap.get(value);
            return maxMatchValue == null ? value : maxMatchValue;
        }));
        return Optional.of(translatedQueryResultValue);
    }

    @Nonnull
    public Optional<MaxMatchMap> adjustMaybe(@Nonnull final CorrelationIdentifier upperCandidateAlias,
                                             @Nonnull final Value upperCandidateResultValue,
                                             @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        final var translatedQueryValueOptional =
                translateQueryValueMaybe(upperCandidateAlias);
        return translatedQueryValueOptional.map(value -> MaxMatchMap.calculate(value, upperCandidateResultValue,
                rangedOverAliases));
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code queryResultValue} that has an exact match in the
     * {@code candidateValue}.
     *
     * @param queryResultValue the query result {@code Value}.
     * @param candidateResultValue the candidate result {@code Value} we want to search for maximum matches.
     * @param rangedOverAliases a set of aliases that should be considered constant
     *
     * @return a {@link MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final Value queryResultValue,
                                        @Nonnull final Value candidateResultValue,
                                        @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        return calculate(queryResultValue,
                candidateResultValue,
                rangedOverAliases,
                ValueEquivalence.empty());
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code rewrittenQueryValue} that has an exact match in the
     * {@code candidateValue}.
     * <br>
     * For certain shapes of {@code Value}s, multiple matches can be found, this method is guaranteed to always find the
     * maximum part of the candidate sub-{@code Value} that matches with a sub-{@code Value} on the query side. For
     * example, assume we have a query {@code Value} {@code R = RCV(s+t)} and a candidate {@code Value} that is
     * {@code R` = RCV(s, t, (s+t))}, with R ≡ R`. We could have the following matches:
     * <ul>
     *     <li>{@code R.0 --> R`.0 + R`.1}</li>
     *     <li>{@code R -> R`.0.0}</li>
     * </ul>
     * The first match is not <i>maximum</i>, because it involves matching smaller constituents of the index and summing
     * them together, the other match however, is much better because it matches the entire query {@code Value} with a
     * single part of the index. The algorithm will always prefer the maximum match.
     *
     * @param queryResultValue the query result {@code Value}
     * @param candidateResultValue the candidate result {@code Value} we want to search for maximum matches
     * @param rangedOverAliases a set of aliases that should be considered constant
     * @param valueEquivalence an {@link ValueEquivalence} that informs the logic about equivalent value subtrees
     *
     * @return a {@link  MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final Value queryResultValue,
                                        @Nonnull final Value candidateResultValue,
                                        @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                        @Nonnull final ValueEquivalence valueEquivalence) {
        final var resultsMap =
                recurseQueryResultValue(queryResultValue, candidateResultValue, rangedOverAliases,
                        valueEquivalence, ImmutableBiMap.of(), -1, new ArrayDeque<>(), Integer.MAX_VALUE, new HashSet<>());

        // pick a match which has the minimum max depth among all the matches

        int currentMaxDepth = Integer.MAX_VALUE;
        MaxMatchMap bestMaxMatchMap = null;
        for (final var resultEntry : resultsMap.entrySet()) {
            final var result = resultEntry.getValue();
            if (result.getMaxDepth() < currentMaxDepth) {
                bestMaxMatchMap = new MaxMatchMap(result.getValueMap(), resultEntry.getKey(), candidateResultValue,
                        result.getQueryPlanConstraint(), valueEquivalence);
                currentMaxDepth = result.getMaxDepth();
            }
        }
        return Objects.requireNonNull(bestMaxMatchMap);
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Map<Value, RecursionResult> recurseQueryResultValue(@Nonnull final Value currentQueryValue,
                                                                       @Nonnull final Value candidateResultValue,
                                                                       @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                                                       @Nonnull final ValueEquivalence valueEquivalence,
                                                                       @Nonnull final BiMap<Value, Value> knownValueMap,
                                                                       final int descendOrdinal,
                                                                       @Nonnull final Deque<IncrementalValueMatcher> matchers,
                                                                       final int maxDepthBound,
                                                                       @Nonnull final Set<Value> expandedValues) {

        //
        // Localize all given matchers and add one for this level. Keep a boolean variable indicating if there are
        // any potential parent matchers that can still match something in the subtree.
        //
        final var localMatchers = new ArrayDeque<IncrementalValueMatcher>();
        boolean anyParentsMatching = false;
        if (descendOrdinal >= 0) {
            for (IncrementalValueMatcher matcher : matchers) {
                anyParentsMatching |= matcher.anyMatches();
                final var descendedMatcher = matcher.descend(descendOrdinal);
                localMatchers.addLast(descendedMatcher);
            }
        }

        //
        // If there are potential matches above this currentQueryValue, we cannot impose an upper bound for the max
        // depth as the potential match above could be the best match of all.
        //
        final int adjustedMaxDepthBound = anyParentsMatching ? Integer.MAX_VALUE : maxDepthBound;

        //
        // Create a matcher for this level
        //
        final var currentMatcher = IncrementalValueMatcher.initial(currentQueryValue, candidateResultValue);
        localMatchers.push(currentMatcher);
        final var isCurrentMatching = currentMatcher.anyMatches();

        //
        // Keep a results map that maps from a Value variant to a recursion result. Note that if we have don't have
        // a potential parent match we can make local decisions in this method, specifically we can prune the variants
        // to consider early on.
        //
        final var resultsMap = new LinkedHashMap<Value, RecursionResult>();

        //
        // This variable will be set to a proper (non-negative) integer after recursing into the subtree and
        // matching this current value. It denotes the current maximum dept from the current value to the furthest
        // match in a subtree OR it can be Integer.MAX_VALUE if there are no matches.
        //
        int currentMaxDepth = -1;

        final var children = currentQueryValue.getChildren();
        if (Iterables.isEmpty(children)) {
            final var resultForCurrent =
                    computeForCurrent(adjustedMaxDepthBound, currentQueryValue, candidateResultValue, rangedOverAliases,
                            valueEquivalence, ImmutableList.of());
            resultsMap.put(currentQueryValue, resultForCurrent);
            currentMaxDepth = resultForCurrent.getMaxDepth();
        } else {
            final BooleanWithConstraint isFound;
            if (!anyParentsMatching && isCurrentMatching) {
                final var matchingPair =
                        findMatchingCandidateValue(currentQueryValue,
                                candidateResultValue,
                                valueEquivalence);
                isFound = Objects.requireNonNull(matchingPair.getLeft());
                if  (isFound.isTrue()) {
                    resultsMap.put(currentQueryValue, RecursionResult.of(ImmutableMap.of(currentQueryValue,
                            Objects.requireNonNull(matchingPair.getRight())), 0, isFound.getConstraint()));
                    currentMaxDepth = 0;
                }
            } else {
                isFound = BooleanWithConstraint.falseValue();
            }

            if (isFound.isFalse()) {
                //
                // Recurse into the children of the current query value.
                //
                final var childrenResultsBuilder = ImmutableList.<Collection<Map.Entry<Value, RecursionResult>>>builder();
                for (final var child : children) {
                    final var childrenResultsMap =
                            recurseQueryResultValue(child, candidateResultValue, rangedOverAliases, valueEquivalence,
                                    knownValueMap, 0, localMatchers,
                                    adjustedMaxDepthBound == Integer.MAX_VALUE ? Integer.MAX_VALUE : adjustedMaxDepthBound - 1,
                                    expandedValues);

                    childrenResultsBuilder.add(childrenResultsMap.entrySet());
                }

                final var childrenResults = childrenResultsBuilder.build();
                for (final var childrenResultEntries : CrossProduct.crossProduct(childrenResults)) {
                    boolean areAllChildrenSame = true;
                    final var childrenValuesBuilder = ImmutableList.<Value>builder();
                    int i = 0;
                    for (final var currentChildrenIterator = children.iterator();
                             currentChildrenIterator.hasNext(); i++) {
                        final Value child = currentChildrenIterator.next();
                        final var childrenResultsEntry = childrenResultEntries.get(i);
                        final var key = childrenResultsEntry.getKey();
                        if (child != key) {
                            areAllChildrenSame = false;
                        }
                        childrenValuesBuilder.add(key);
                    }
                    Verify.verify(i == childrenResultEntries.size());

                    final Value resultQueryValue;
                    if (!areAllChildrenSame) {
                        resultQueryValue = currentQueryValue.withChildren(childrenValuesBuilder.build());
                    } else {
                        resultQueryValue = currentQueryValue;
                    }

                    final var resultForCurrent =
                            computeForCurrent(adjustedMaxDepthBound, resultQueryValue, candidateResultValue,
                                    rangedOverAliases, valueEquivalence, childrenResultEntries);
                    resultsMap.put(resultQueryValue, resultForCurrent);
                    currentMaxDepth = Math.max(currentMaxDepth, resultForCurrent.getMaxDepth());
                }
            }
        }

        Verify.verify(currentMaxDepth >= 0);

        //
        // Try to transform the current query value into a more matchable shape -- and recurse with that new tree
        //
        if ((anyParentsMatching || currentMaxDepth > 0) &&
                !expandedValues.contains(currentQueryValue)) {
            try {
                expandedValues.add(currentQueryValue);
                final var expandedCurrentQueryValues =
                        Simplification.simplifyCurrent(currentQueryValue,
                                AliasMap.emptyMap(), rangedOverAliases, MaxMatchMapSimplificationRuleSet.instance());
                for (final var expandedCurrentQueryValue : expandedCurrentQueryValues) {
                    final var expandedResultsMap =
                            recurseQueryResultValue(expandedCurrentQueryValue, candidateResultValue,
                                    rangedOverAliases, valueEquivalence, knownValueMap, descendOrdinal,
                                    matchers, currentMaxDepth, expandedValues);
                    for (final var valueRecursionResultEntry : expandedResultsMap.entrySet()) {
                        final var recursionResult = valueRecursionResultEntry.getValue();
                        if (anyParentsMatching || recursionResult.getMaxDepth() < currentMaxDepth) {
                            final var key = valueRecursionResultEntry.getKey();
                            resultsMap.put(key, recursionResult);
                            currentMaxDepth = recursionResult.getMaxDepth();
                        }
                    }
                }
            } finally {
                expandedValues.remove(currentQueryValue);
            }
        }

        return resultsMap;
    }

    @Nonnull
    private static RecursionResult computeForCurrent(final int maxDepthBound,
                                                     @Nonnull final Value resultQueryValue,
                                                     @Nonnull final Value candidateResultValue,
                                                     @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                                     @Nonnull final ValueEquivalence valueEquivalence,
                                                     @Nonnull final List<Map.Entry<Value, RecursionResult>> childrenResultEntries) {
        if (maxDepthBound == 0) {
            return RecursionResult.notMatched();
        }

        final var matchingPair =
                findMatchingCandidateValue(resultQueryValue,
                        candidateResultValue,
                        valueEquivalence);
        final var isFound = Objects.requireNonNull(matchingPair.getLeft());
        if (isFound.isTrue()) {
            return RecursionResult.of(ImmutableMap.of(resultQueryValue,
                            Objects.requireNonNull(matchingPair.getRight())), 0, isFound.getConstraint());
        }

        var childrenConstraint = QueryPlanConstraint.tautology();
        final var childrenResultsMap = new LinkedHashMap<Value, Value>();
        int childrenMaxDepth = -1;
        for (final var childrenResultsEntry : childrenResultEntries) {
            final var childValue = childrenResultsEntry.getKey();
            final var childResult =
                    childrenResultsEntry.getValue();
            if (!childResult.isMatched()) {
                return RecursionResult.notMatched();
            }

            final var childValueMap = childResult.getValueMap();
            if (childValueMap.isEmpty() &&
                    !Sets.intersection(childValue.getCorrelatedTo(), rangedOverAliases).isEmpty()) {
                return RecursionResult.notMatched();
            }

            childrenResultsMap.putAll(childValueMap);
            childrenMaxDepth = Math.max(childrenMaxDepth, childResult.getMaxDepth());
            childrenConstraint = childrenConstraint.compose(childResult.getQueryPlanConstraint());
        }
        childrenMaxDepth = childrenMaxDepth == -1 ? Integer.MAX_VALUE : childrenMaxDepth;
        return RecursionResult.of(childrenResultsMap,
                childrenMaxDepth == Integer.MAX_VALUE ? Integer.MAX_VALUE : childrenMaxDepth + 1, childrenConstraint);
    }

    @Nonnull
    private static Pair<BooleanWithConstraint, Value> findMatchingCandidateValue(@Nonnull final Value currentQueryValue,
                                                                                 @Nonnull final Value candidateResultValue,
                                                                                 @Nonnull final ValueEquivalence valueEquivalence) {
        for (final var currentCandidateValue : candidateResultValue
                // when traversing the candidate in pre-order, only descend into structures that can be referenced
                // from the top expression. For example, RCV's components can be referenced however an Arithmetic
                // operator's children can not be referenced.
                // It is crucial to do this in pre-order to guarantee matching the maximum (sub-)value of the candidate.
                .preOrderIterable(v -> v instanceof RecordConstructorValue)) {
            final var semanticEquals =
                    currentQueryValue.semanticEquals(currentCandidateValue, valueEquivalence);
            if (semanticEquals.isTrue()) {
                return Pair.of(semanticEquals, currentCandidateValue);
            }
        }
        return Pair.of(BooleanWithConstraint.falseValue(), null);
    }

    private static class IncrementalValueMatcher {
        @Nonnull
        private final Value currentQueryValue;

        @Nonnull
        private final Supplier<List<NonnullPair<Value, QueryPlanConstraint>>> matchingCandidateValuesSupplier;

        private IncrementalValueMatcher(@Nonnull final Value currentQueryValue,
                                        @Nonnull final Supplier<List<NonnullPair<Value, QueryPlanConstraint>>> matchingCandidateValuesSupplier) {
            this.currentQueryValue = currentQueryValue;
            this.matchingCandidateValuesSupplier = Suppliers.memoize(matchingCandidateValuesSupplier::get);
        }

        @Nonnull
        public Value getCurrentQueryValue() {
            return currentQueryValue;
        }

        @Nonnull
        public List<NonnullPair<Value, QueryPlanConstraint>> getMatchingCandidateValues() {
            return matchingCandidateValuesSupplier.get();
        }

        public boolean anyMatches() {
            return !getMatchingCandidateValues().isEmpty();
        }

        @Nonnull
        public IncrementalValueMatcher descend(final int descendOrdinal) {
            final var currentQueryValue =
                    Iterables.get(getCurrentQueryValue().getChildren(),
                            descendOrdinal);

            return new IncrementalValueMatcher(currentQueryValue,
                    () -> {
                        final var matchingCandidateValuesBuilder =
                                ImmutableList.<NonnullPair<Value, QueryPlanConstraint>>builder();
                        for (final var matchingCandidateValuePair : getMatchingCandidateValues()) {
                            final var currentCandidateValue =
                                    Iterables.get(matchingCandidateValuePair.getLeft().getChildren(),
                                            descendOrdinal, null);
                            if (currentCandidateValue != null) {
                                final var currentEqualsWithoutChildren =
                                        currentQueryValue.equalsWithoutChildren(currentCandidateValue);
                                if (currentEqualsWithoutChildren.isTrue()) {
                                    matchingCandidateValuesBuilder.add(
                                            NonnullPair.of(currentCandidateValue, matchingCandidateValuePair.getRight()
                                                    .compose(currentEqualsWithoutChildren.getConstraint())));
                                }
                            }
                        }
                        return matchingCandidateValuesBuilder.build();
                    });
        }

        @Nonnull
        public static IncrementalValueMatcher initial(@Nonnull final Value queryRootValue,
                                                      @Nonnull final Value candidateRootValue) {
            return new IncrementalValueMatcher(queryRootValue,
                    () -> Streams.stream(candidateRootValue.preOrderIterable(candidateValue -> candidateValue instanceof RecordConstructorValue))
                            .flatMap(candidatevalue ->
                                    queryRootValue.equalsWithoutChildren(candidatevalue)
                                            .mapToOptional(Function.identity())
                                            .stream()
                                            .map(queryPlanConstraint ->
                                                    NonnullPair.<Value, QueryPlanConstraint>of(candidatevalue, queryPlanConstraint)))
                            .collect(ImmutableList.toImmutableList()));
        }
    }

    private static class RecursionResult {
        private static final RecursionResult NOT_MATCHED =
                new RecursionResult(ImmutableMap.of(), Integer.MAX_VALUE, QueryPlanConstraint.tautology());

        @Nonnull
        private final Map<Value, Value> valueMap;
        private final int maxDepth;
        @Nonnull
        private final QueryPlanConstraint queryPlanConstraint;

        private RecursionResult(@Nonnull final Map<Value, Value> valueMap,
                                final int maxDepth,
                                @Nonnull final QueryPlanConstraint queryPlanConstraint) {
            this.valueMap = valueMap;
            this.queryPlanConstraint = queryPlanConstraint;
            this.maxDepth = maxDepth;
        }

        @Nonnull
        public Map<Value, Value> getValueMap() {
            return valueMap;
        }

        public int getMaxDepth() {
            return maxDepth;
        }

        public boolean isMatched() {
            return maxDepth < Integer.MAX_VALUE;
        }

        @Nonnull
        public QueryPlanConstraint getQueryPlanConstraint() {
            return queryPlanConstraint;
        }

        public static RecursionResult of(@Nonnull final Map<Value, Value> valueMap,
                                         final int maxDepth,
                                         @Nonnull final QueryPlanConstraint queryPlanConstraint) {
            return new RecursionResult(valueMap, maxDepth, queryPlanConstraint);
        }

        public static RecursionResult notMatched() {
            return NOT_MATCHED;
        }
    }
}
