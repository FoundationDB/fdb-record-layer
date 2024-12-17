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
import com.apple.foundationdb.record.query.plan.cascades.DefaultFormatter;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedRecordValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Value.ExplainInfo;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.MaxMatchMapSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Represents a max match between a (rewritten) query result {@link Value} and the candidate result {@link Value}.
 */
public class MaxMatchMap {
    private static final Logger logger = LoggerFactory.getLogger(MaxMatchMap.class);

    @Nonnull
    private final Map<Value, Value> mapping;
    @Nonnull
    private final Value queryValue; // in terms of the candidate quantifiers.
    @Nonnull
    private final Value candidateValue;
    @Nonnull
    private final Set<CorrelationIdentifier> rangedOverAliases;
    @Nonnull
    private final QueryPlanConstraint queryPlanConstraint;
    @Nonnull
    private final ValueEquivalence valueEquivalence;

    /**
     * Creates a new instance of {@link MaxMatchMap}.
     * @param mapping the {@link Value} mapping
     * @param queryValue the query result from which the mapping keys originate
     * @param candidateValue the candidate result from which the mapping values originate
     * @param valueEquivalence a {@link ValueEquivalence} that was used to match up query and candidate values
     */
    MaxMatchMap(@Nonnull final Map<Value, Value> mapping,
                @Nonnull final Value queryValue,
                @Nonnull final Value candidateValue,
                @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                @Nonnull final QueryPlanConstraint queryPlanConstraint,
                @Nonnull final ValueEquivalence valueEquivalence) {
        this.mapping = ImmutableMap.copyOf(mapping);
        this.queryValue = queryValue;
        this.candidateValue = candidateValue;
        this.rangedOverAliases = ImmutableSet.copyOf(rangedOverAliases);
        this.queryPlanConstraint = queryPlanConstraint;
        this.valueEquivalence = valueEquivalence;
    }

    @Nonnull
    public Map<Value, Value> getMap() {
        return mapping;
    }

    @Nonnull
    public Value getQueryValue() {
        return queryValue;
    }

    @Nonnull
    public Value getCandidateValue() {
        return candidateValue;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getRangedOverAliases() {
        return rangedOverAliases;
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
        final var candidateValue = getCandidateValue();
        final var pulledUpCandidateValueMap =
                candidateValue.pullUp(ImmutableSet.copyOf(mapping.values()), // values may contain duplicates
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
            final var pulledUpdateCandidatePart = pulledUpCandidateValueMap.get(candidatePart);
            if (pulledUpdateCandidatePart == null) {
                return Optional.empty();
            }
            pulledUpMaxMatchMapBuilder.put(queryPart, pulledUpdateCandidatePart);
        }
        final var pulledUpMaxMatchMap = pulledUpMaxMatchMapBuilder.build();

        final var queryResultValueFromBelow = getQueryValue();
        final var translatedQueryResultValue = Objects.requireNonNull(queryResultValueFromBelow.replace(value -> {
            final var maxMatchValue = pulledUpMaxMatchMap.get(value);
            return maxMatchValue == null ? value : maxMatchValue;
        }));
        if (!Sets.intersection(rangedOverAliases, translatedQueryResultValue.getCorrelatedTo()).isEmpty()) {
            return Optional.empty();
        }
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

    @Override
    public String toString() {
        return "M³(" +
                "mapping=" + mapping + ", queryValue=" + queryValue + ", candidateValue=" + candidateValue +
                ", queryPlanConstraint=" + queryPlanConstraint + ")";
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code queryValue} that has an exact match in the
     * {@code candidateValue}.
     *
     * @param queryValue the query result {@code Value}.
     * @param candidateValue the candidate result {@code Value} we want to search for maximum matches.
     * @param rangedOverAliases a set of aliases that should be considered constant
     *
     * @return a {@link MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final Value queryValue,
                                        @Nonnull final Value candidateValue,
                                        @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        return calculate(queryValue,
                candidateValue,
                rangedOverAliases,
                ValueEquivalence.empty());
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code queryValue} that has an exact match in the
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
     * @param queryValue the query result {@code Value}
     * @param candidateValue the candidate result {@code Value} we want to search for maximum matches
     * @param rangedOverAliases a set of aliases that should be considered constant
     * @param valueEquivalence an {@link ValueEquivalence} that informs the logic about equivalent value subtrees
     *
     * @return a {@link  MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final Value queryValue,
                                        @Nonnull final Value candidateValue,
                                        @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                        @Nonnull final ValueEquivalence valueEquivalence) {
        if (logger.isDebugEnabled()) {
            logger.debug("calculate begin queryValue={}, candidateValue={}", queryValue, candidateValue);
        }
        final var resultsMap =
                recurseQueryResultValue(queryValue, candidateValue, rangedOverAliases,
                        valueEquivalence, new IdentityHashMap<>(), -1, new ArrayDeque<>(),
                        Integer.MAX_VALUE, new HashSet<>());

        //
        // Pick a match which has the minimum max depth among all the matches.
        //
        int currentMaxDepth = Integer.MAX_VALUE;
        MaxMatchMap bestMaxMatchMap = null;
        for (final var resultEntry : resultsMap.entrySet()) {
            final var result = resultEntry.getValue();
            if (result.getMaxDepth() < currentMaxDepth) {
                bestMaxMatchMap = new MaxMatchMap(result.getValueMap(), resultEntry.getKey(), candidateValue,
                        rangedOverAliases, result.getQueryPlanConstraint(), valueEquivalence);
                currentMaxDepth = result.getMaxDepth();
            }
        }

        if (bestMaxMatchMap == null) {
            bestMaxMatchMap = new MaxMatchMap(ImmutableMap.of(), queryValue, candidateValue, rangedOverAliases,
                    QueryPlanConstraint.tautology(), valueEquivalence);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("calculate end bestMaxMatchMap={}", bestMaxMatchMap);
        }

        return bestMaxMatchMap;
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Map<Value, RecursionResult> recurseQueryResultValue(@Nonnull final Value currentQueryValue,
                                                                       @Nonnull final Value candidateValue,
                                                                       @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                                                       @Nonnull final ValueEquivalence valueEquivalence,
                                                                       @Nonnull final IdentityHashMap<Value, Map<Value, RecursionResult>> knownValueMap,
                                                                       final int descendOrdinal,
                                                                       @Nonnull final Deque<IncrementalValueMatcher> matchers,
                                                                       final int maxDepthBound,
                                                                       @Nonnull final Set<Value> expandedValues) {
        if (maxDepthBound == 0) {
            return ImmutableMap.of(currentQueryValue, RecursionResult.notMatched());
        }

        //
        // Localize all given matchers and add one for this level. Keep a boolean variable indicating if there are
        // any potential parent matchers that can still match something in the subtree.
        //
        final var localMatchers = new ArrayDeque<IncrementalValueMatcher>();
        boolean anyParentsMatching = false;
        if (descendOrdinal >= 0) {
            for (final var matcher : matchers) {
                anyParentsMatching |= matcher.anyMatches();
                final var descendedMatcher = matcher.descend(currentQueryValue, descendOrdinal);
                localMatchers.addLast(descendedMatcher);
            }
        }

        if (!anyParentsMatching) {
            if (knownValueMap.containsKey(currentQueryValue)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("getting memoized info value={}", currentQueryValue);
                }
                return knownValueMap.get(currentQueryValue);
            }
        }

        //
        // If any parents have any potential matches, we need to generate all possible expansions for this subtree as
        // we need to exhaustively find the case where the entire subtree rooted in one of the parents matches. If no
        // such potential matches exist, we know that a match in this frame immediately also is the maximum match.
        //

        //
        // Create a matcher for this level.
        //
        final var currentMatcher =
                IncrementalValueMatcher.initial(currentQueryValue, candidateValue);
        localMatchers.push(currentMatcher);
        final var isCurrentMatching = currentMatcher.anyMatches();

        //
        // Keep a results map that maps from a Value variant to a recursion result. Note that if we have don't have
        // a potential parent match we can make local decisions in this method, specifically, we can prune the variants
        // that are being generated early on.
        //
        final var bestMatches = new BestMatches(currentQueryValue, !anyParentsMatching);

        final var children = currentQueryValue.getChildren();
        if (Iterables.isEmpty(children)) {
            final var resultForCurrent =
                    computeForCurrent(maxDepthBound, currentQueryValue, candidateValue, rangedOverAliases,
                            valueEquivalence, ImmutableList.of());
            bestMatches.put(currentQueryValue, resultForCurrent);
        } else {
            final BooleanWithConstraint isFound;
            if (!anyParentsMatching && isCurrentMatching) {
                //
                // We know that no parents are matching but that the current level is potentially matching. Try to
                // properly match the current query value and immediately return if successful as this match also is
                // the maximum match.
                //
                final var matchingPair =
                        findMatchingCandidateValue(currentQueryValue,
                                candidateValue,
                                valueEquivalence);
                isFound = Objects.requireNonNull(matchingPair.getLeft());
                if  (isFound.isTrue()) {
                    bestMatches.put(currentQueryValue, RecursionResult.of(ImmutableMap.of(currentQueryValue,
                            Objects.requireNonNull(matchingPair.getRight())), 0, isFound.getConstraint()));
                }
            } else {
                isFound = BooleanWithConstraint.falseValue();
            }

            if (isFound.isFalse()) {
                //
                // Compute the max depth bound for the subsequent recursion.
                //
                final int childrenMaxDepthBound =
                        (maxDepthBound == Integer.MAX_VALUE ||
                                 localMatchers.stream().anyMatch(IncrementalValueMatcher::anyMatches))
                        ? Integer.MAX_VALUE
                        : maxDepthBound - 1;

                //
                // Recurse into the children of the current query value.
                //
                final var childrenResultsBuilder =
                        ImmutableList.<Collection<Map.Entry<Value, RecursionResult>>>builder();
                int i = 0;
                for (final var childrenIterator = children.iterator();
                         childrenIterator.hasNext(); i++) {
                    final var child = childrenIterator.next();

                    if (logger.isDebugEnabled()) {
                        logger.debug("recursing into child max_depth_bound={}, value={}", childrenMaxDepthBound, child);
                    }
                    final var childrenResultsMap =
                            recurseQueryResultValue(child, candidateValue, rangedOverAliases, valueEquivalence,
                                    knownValueMap, i, localMatchers, childrenMaxDepthBound, expandedValues);

                    childrenResultsBuilder.add(childrenResultsMap.entrySet());
                }

                final var childrenResults = childrenResultsBuilder.build();
                for (final var childrenResultEntries : CrossProduct.crossProduct(childrenResults)) {
                    boolean areAllChildrenSame = true;
                    final var childrenValuesBuilder = ImmutableList.<Value>builder();
                    i = 0;
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
                            computeForCurrent(maxDepthBound, resultQueryValue, candidateValue,
                                    rangedOverAliases, valueEquivalence, childrenResultEntries);
                    bestMatches.put(resultQueryValue, resultForCurrent);
                }
            }
        }

        Verify.verify(!bestMatches.isEmpty());
        Verify.verify(bestMatches.isEmpty() || bestMatches.getCurrentMaxDepth() >= 0);

        //
        // Try to transform the current query value into a more matchable shape -- and recurse with that new tree
        //
        if ((anyParentsMatching || bestMatches.getCurrentMaxDepth() > 0) &&
                !expandedValues.contains(currentQueryValue)) {
            try {
                expandedValues.add(currentQueryValue);
                final var expandedCurrentQueryValues =
                        Simplification.simplifyCurrent(currentQueryValue,
                                AliasMap.emptyMap(), rangedOverAliases, MaxMatchMapSimplificationRuleSet.instance());
                for (final var expandedCurrentQueryValue : expandedCurrentQueryValues) {
                    final var currentMaxDepthBound =
                            anyParentsMatching
                            ? Integer.MAX_VALUE
                            : (bestMatches.getCurrentMaxDepth() == Integer.MAX_VALUE ? maxDepthBound : bestMatches.getCurrentMaxDepth());

                    if (logger.isDebugEnabled()) {
                        logger.debug("recursing into variant max_depth_bound={}, value={}", currentMaxDepthBound,
                                expandedCurrentQueryValue);
                    }

                    final var expandedResultsMap =
                            recurseQueryResultValue(expandedCurrentQueryValue, candidateValue,
                                    rangedOverAliases, valueEquivalence, knownValueMap, descendOrdinal,
                                    matchers,
                                    currentMaxDepthBound,
                                    expandedValues);
                    for (final var expandedResultsEntry : expandedResultsMap.entrySet()) {
                        bestMatches.put(expandedResultsEntry.getKey(), expandedResultsEntry.getValue());
                    }
                }
            } finally {
                expandedValues.remove(currentQueryValue);
            }
        }

        Verify.verify(bestMatches.getCurrentMaxDepth() == Integer.MAX_VALUE ||
                bestMatches.getCurrentMaxDepth() <= maxDepthBound);

        final var resultMap = bestMatches.toResultMap();
        if (!anyParentsMatching && !expandedValues.contains(currentQueryValue)) {
            if (logger.isDebugEnabled()) {
                logger.debug("memoizing value={}", currentQueryValue);
            }
            knownValueMap.put(currentQueryValue, resultMap);
        }
        return resultMap;
    }

    @Nonnull
    private static RecursionResult computeForCurrent(final int maxDepthBound,
                                                     @Nonnull final Value resultQueryValue,
                                                     @Nonnull final Value candidateValue,
                                                     @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                                     @Nonnull final ValueEquivalence valueEquivalence,
                                                     @Nonnull final List<Map.Entry<Value, RecursionResult>> childrenResultEntries) {
        Verify.verify(maxDepthBound > 0);

        final var matchingPair =
                findMatchingCandidateValue(resultQueryValue,
                        candidateValue,
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
            final var childValueMap = childResult.getValueMap();
            if (childValueMap.isEmpty()) {
                if (!Sets.intersection(childValue.getCorrelatedTo(), rangedOverAliases).isEmpty()) {
                    return RecursionResult.notMatched();
                }
                // childValue is constant with respect to rangedOverAliases
                childrenMaxDepth = Math.max(childrenMaxDepth, 0);
            } else {
                childrenMaxDepth = Math.max(childrenMaxDepth, childResult.getMaxDepth());
            }

            if (maxDepthBound < Integer.MAX_VALUE && maxDepthBound - 1 < childrenMaxDepth) {
                return RecursionResult.notMatched();
            }

            childrenResultsMap.putAll(childValueMap);
            childrenConstraint = childrenConstraint.compose(childResult.getQueryPlanConstraint());
        }
        childrenMaxDepth = childrenMaxDepth == -1 ? Integer.MAX_VALUE : childrenMaxDepth;
        return RecursionResult.of(childrenResultsMap,
                childrenMaxDepth == Integer.MAX_VALUE ? Integer.MAX_VALUE : childrenMaxDepth + 1, childrenConstraint);
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Pair<BooleanWithConstraint, Value> findMatchingCandidateValue(@Nonnull final Value currentQueryValue,
                                                                                 @Nonnull final Value candidateValue,
                                                                                 @Nonnull final ValueEquivalence valueEquivalence) {
        for (final var currentCandidateValue : candidateValue
                // when traversing the candidate in pre-order, only descend into structures that can be referenced
                // from the top expression. For example, RCV's components can be referenced however an Arithmetic
                // operator's children can not be referenced.
                // It is crucial to do this in pre-order to guarantee matching the maximum (sub-)value of the candidate.
                .preOrderIterable(v -> v instanceof RecordConstructorValue)) {
            if (currentCandidateValue == candidateValue) {
                if (currentQueryValue instanceof QuantifiedRecordValue) {
                    return Pair.of(BooleanWithConstraint.alwaysTrue(), currentCandidateValue);
                }
            }

            final var semanticEquals =
                    currentQueryValue.semanticEquals(currentCandidateValue, valueEquivalence);
            if (semanticEquals.isTrue()) {
                return Pair.of(semanticEquals, currentCandidateValue);
            }
        }
        return Pair.of(BooleanWithConstraint.falseValue(), null);
    }

    private static class BestMatches {
        @Nonnull
        private final Value currentQueryValue;
        private final boolean isPruning;
        private int currentMaxDepth = -1;
        @Nullable
        private Map<Value, RecursionResult> matchMap;
        @Nullable
        private Value value;
        @Nullable
        private RecursionResult recursionResult;

        public BestMatches(@Nonnull final Value currentQueryValue, final boolean isPruning) {
            this.currentQueryValue = currentQueryValue;
            this.isPruning = isPruning;
            if (!isPruning) {
                this.matchMap = new LinkedHashMap<>();
            }
        }

        public boolean isPruning() {
            return isPruning;
        }

        public boolean isEmpty() {
            return getCurrentMaxDepth() == -1;
        }

        public int getCurrentMaxDepth() {
            return currentMaxDepth;
        }

        @Nonnull
        private Map<Value, RecursionResult> getMatchMap() {
            return Objects.requireNonNull(matchMap);
        }

        @Nonnull
        public Value getValue() {
            return Objects.requireNonNull(value);
        }

        @Nonnull
        public RecursionResult getRecursionResult() {
            return Objects.requireNonNull(recursionResult);
        }

        @Nonnull
        public Map<Value, RecursionResult> toResultMap() {
            if (isEmpty()) {
                return ImmutableMap.of(currentQueryValue, RecursionResult.notMatched());
            }
            if (isPruning()) {
                return ImmutableMap.of(getValue(), getRecursionResult());
            }
            return getMatchMap();
        }

        public void put(@Nonnull final Value newQueryValue,
                        @Nonnull final RecursionResult newRecursionResult) {
            if (isPruning()) {
                if (recursionResult == null || newRecursionResult.getMaxDepth() < currentMaxDepth) {
                    this.value = newQueryValue;
                    this.recursionResult = newRecursionResult;
                    this.currentMaxDepth = newRecursionResult.getMaxDepth();
                }
            } else {
                if (currentMaxDepth == -1 || newRecursionResult.getMaxDepth() < currentMaxDepth) {
                    currentMaxDepth = newRecursionResult.getMaxDepth();
                }
                getMatchMap().put(newQueryValue, newRecursionResult);
            }
        }
    }

    private abstract static class IncrementalValueMatcher {
        private static final ExplainInfo UNMATCHED = ExplainInfo.of("■");
        @Nonnull
        private final Value currentQueryValue;

        @Nonnull
        private final Supplier<List<NonnullPair<Value, QueryPlanConstraint>>> matchingCandidateValuesSupplier;

        private IncrementalValueMatcher(@Nonnull final Value currentQueryValue) {
            this.currentQueryValue = currentQueryValue;
            this.matchingCandidateValuesSupplier = Suppliers.memoize(this::computeMatchingCandidateValues);
        }

        @Nonnull
        public Value getCurrentQueryValue() {
            return currentQueryValue;
        }

        @Nonnull
        public List<NonnullPair<Value, QueryPlanConstraint>> getMatchingCandidateValues() {
            return matchingCandidateValuesSupplier.get();
        }

        @Nonnull
        public abstract List<NonnullPair<Value, QueryPlanConstraint>> computeMatchingCandidateValues();

        @Nonnull
        public abstract ExplainInfo explain(@Nonnull Formatter formatter,
                                            @Nonnull Iterable<Function<Formatter, ExplainInfo>> explainFunctions);

        public boolean anyMatches() {
            return !getMatchingCandidateValues().isEmpty();
        }

        @Override
        public String toString() {
            final var explainString = explain(new DefaultFormatter(),
                    Collections.nCopies(Iterables.size(currentQueryValue.getChildren()),
                            formatter -> UNMATCHED)).getExplainString();
            return anyMatches() ? explainString : explainString + " → ∅";
        }

        @Nonnull
        public IncrementalValueMatcher descend(@Nonnull final Value currentQueryValue, final int descendOrdinal) {
            final var parent = this;
            return new IncrementalValueMatcher(currentQueryValue) {
                @Nonnull
                @Override
                public List<NonnullPair<Value, QueryPlanConstraint>> computeMatchingCandidateValues() {
                    final var matchingCandidateValuesBuilder =
                            ImmutableList.<NonnullPair<Value, QueryPlanConstraint>>builder();
                    for (final var matchingCandidateValuePair : parent.getMatchingCandidateValues()) {
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
                }

                @Nonnull
                @Override
                public ExplainInfo explain(@Nonnull final Formatter formatter,
                                           @Nonnull final Iterable<Function<Formatter, ExplainInfo>> explainFunctions) {
                    Verify.verify(Iterables.size(explainFunctions) == Iterables.size(currentQueryValue.getChildren()));
                    final var parentExplainFunctionsBuilder =
                            ImmutableList.<Function<Formatter, ExplainInfo>>builder();
                    final var parentSize = Iterables.size(parent.getCurrentQueryValue().getChildren());
                    for (int i = 0; i < parentSize; i ++) {
                        if (i != descendOrdinal) {
                            parentExplainFunctionsBuilder.add(f -> UNMATCHED);
                        } else {
                            parentExplainFunctionsBuilder.add(f -> currentQueryValue.explain(formatter, explainFunctions));
                        }
                    }
                    return parent.explain(formatter, parentExplainFunctionsBuilder.build());
                }
            };
        }

        @Nonnull
        public static IncrementalValueMatcher initial(@Nonnull final Value queryRootValue,
                                                      @Nonnull final Value candidateRootValue) {
            return new IncrementalValueMatcher(queryRootValue) {
                @Nonnull
                @Override
                public List<NonnullPair<Value, QueryPlanConstraint>> computeMatchingCandidateValues() {
                    return Streams.stream(candidateRootValue.preOrderIterable(candidateValue -> candidateValue instanceof RecordConstructorValue))
                            .flatMap(candidateValue -> {
                                if (candidateValue == candidateRootValue) {
                                    if (getCurrentQueryValue() instanceof QuantifiedRecordValue) {
                                        return Stream.of(NonnullPair.<Value, QueryPlanConstraint>of(candidateValue,
                                                QueryPlanConstraint.tautology()));
                                    }
                                }
                                return getCurrentQueryValue().equalsWithoutChildren(candidateValue)
                                        .mapToOptional(Function.identity())
                                        .stream()
                                        .map(queryPlanConstraint ->
                                                NonnullPair.<Value, QueryPlanConstraint>of(candidateValue,
                                                        queryPlanConstraint));
                            })
                            .collect(ImmutableList.toImmutableList());
                }

                @Nonnull
                @Override
                public ExplainInfo explain(@Nonnull final Formatter formatter,
                                           @Nonnull final Iterable<Function<Formatter, ExplainInfo>> explainFunctions) {
                    return getCurrentQueryValue().explain(formatter, explainFunctions);
                }
            };
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
            Verify.verify(maxDepth >= 0);
            return new RecursionResult(valueMap, maxDepth, queryPlanConstraint);
        }

        public static RecursionResult notMatched() {
            return NOT_MATCHED;
        }
    }
}
