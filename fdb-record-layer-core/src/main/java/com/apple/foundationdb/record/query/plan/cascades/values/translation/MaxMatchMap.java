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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedRecordValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.MaxMatchMapSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
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
 * Represents a maximum match between a query result {@link Value} and a candidate result {@link Value}.
 * <br>
 * A query {@link Value} tree is matched to a candidate {@link Value} to establish correspondences between the
 * query side and the candidate side that will then allow us to express the query side in terms of the candidate side.
 * <br>
 * For instance, assume a query {@link Value} tree
 * <pre>
 * {@code
 *    rcv(fv(qov(q), a) as a, fv(qov(q), b) as b, fv(qov(q), c) as c) or shorter
 *    rcv(q.a as a, q.b as b, q.c as c)
 * }
 * </pre>
 * and a candidate {@link Value} tree:
 * <pre>
 * {@code
 *    rcv(q.c as c, q.b as b, q.a as a)
 * }
 * </pre>
 * These {@link Value} are not the same, however, they are somewhat similar. In fact there are subtrees in the query
 * value that we can relate to an equal subtree of the candidate side. We can find many such relationships or
 * correspondences between the subtrees of the query value and subtrees of the candidate value. We want to find
 * correspondences where the subtrees that are being related cover a maximum subtree, that is there are no
 * correspondences between subtrees that include this subtree.
 * <br>
 * In the example above we can say that the query side value and the candidate value define maximum matches between
 * {@code q.a -> q.a}, {@code q.b -> q.b}, and {@code q.b -> q.b}. If the candidate side had been
 * {@code rcv(q.a as a, q.b as b, q.c as c)}, the maximum matches between query side and candidate side would just
 * have been {@code rcv(q.a as a, q.b as b, q.c as c) -> rcv(q.a as a, q.b as b, q.c as c)} as the entire tree under
 * the root would have matched. Even though {@code q.a -> q.a}, {@code q.b -> q.b}, and {@code q.c -> q.c} are also
 * matches, they are not maximal matches.
 * <br>
 * There are some restrictions on what we can call a match (or correspondence) of a subtree of the query side value to
 * a subtree of the candidate value in addition to the semantic equality of the related subtrees. A query side subtree
 * can only match a semantically equivalent candidate side subtree if that subtree is <em>reachable</em> from the root
 * of the candidate value. A subtree {@code x} of a {@link Value} {@code v} is defined as reachable from {@code v} iff
 * there is some known way to apply other values to {@code v} to retrieve {@code x} itself. So if {@code v} is
 * {@code rcv(q.c as c, q.b as b, q.a as a)} and {@code x} is {@code q.b} then {@code x (q.b)} is reachable from
 * {@code v} because we can apply {@code .b} to {@code v}: {@code rcv(q.c as c, q.b as b, q.a as a).b} which simplifies
 * to {@code q.b}, that is {@code x}. This approach works nicely for invertible functions like record construction and
 * field accesses. It does not work for other operations such as arbitrary arithmetic operations, e.g. if {@code v}
 * instead is {@code q.c + q.b + q.a} then {@code q.b} is not reachable from {@code v} as we don't know the quantities
 * added to {@code q.b}. This example showcases a destructive operation that cannot be inverted or reversed.
 * <br>
 * Continuing from the modifying the example above, it can be observed that the value tree {@code q.b} is reachable from
 * the root {@code v} at {@code rcv(q.c + q.b + q.c, q.c as c, q.a as a)} as we can retrieve {@code q.b} by computing
 * {@code v._0 - v.c - v.a}. We cannot compute the way how to reach {@code x} from {@code v} (yet). Thus, for the
 * purpose of the logic in this class we treat these unattainable cases as if they were unreachable, and only say that a
 * subtree is reachable from a root if we can actually synthesize the value tree to reach {@code x} from {@code v}.
 * Specifically, we currently only really understand record construction and field accesses as their inverse.
 * <br>
 * In order for the actual maximum matches map or short {@link MaxMatchMap} to be useful, every leaf value on the query
 * side that is not constant must be contained in a subtree that is part of a correspondence (match) in the max match
 * map. For instance, a query value {@code rcv(q.a as a, q.b as b, q.c as c, q.d as d)} cannot be really be matched to
 * {@code rcv(q.a as a, q.b as b, q.c as c)} as {@code q.d} would not be covered by any matches.
 * <br>
 * The purpose of all of this matching is to be able to perform a pull-up: How can we express the query side in terms
 * of {@code v} itself? In an example where the query value is {@code rcv(q.a as a, q.b as b, q.c as c)} is matched
 * to the candidate value {@code rcv(q.c as _0, q.b as _1, q.a as _2)} via the mappings {@code q.a -> q._2},
 * {@code q.b -> q._1}, and {@code q.c -> q._0}, we can express the query value only in terms of the reference
 * {@code v}: All right sides of each mapping is reachable from {@code v}. Therefore, we can express all the right sides
 * in terms of {@code v} yielding another map {@code q.a -> v._2}, {@code q.b -> v._1}, and {@code q.c -> v._0}. Using
 * the query side value, we can then substitute the subtrees contained in this new map with their corresponding right
 * sides: {@code rcv(v._2 as a, v._1 as b, v._0 as c)} yielding a new query side value that expresses the same semantic
 * meaning as the original one, however, in terms of {@code v} instead of {@code q}.
 */
public class MaxMatchMap {
    private static final Logger logger = LoggerFactory.getLogger(MaxMatchMap.class);

    /**
     * This map holds all correspondences between subtrees of {@link #queryValue} and reachable subtrees of
     * {@link #candidateValue}. The following (maximum) invariance also holds: If there is a mapping {@code v -> v'}
     * present in this map, then there is no mapping for any {@code w} in this map, where {@code w} is a sub value
     * of {@code v}.
     */
    @Nonnull
    private final Map<Value, Value> mapping;
    /**
     * Query side {@link Value} tree.
     */
    @Nonnull
    private final Value queryValue; // in terms of the candidate quantifiers.
    /**
     * Candidate side {@link Value} tree.
     */
    @Nonnull
    private final Value candidateValue;
    /**
     * A set of aliases that refers to quantifiers are owned by the expression the candidate {@link Value} lives in.
     * These aliases are not deep correlations, i.e. they are not constant.
     */
    @Nonnull
    private final Set<CorrelationIdentifier> rangedOverAliases;
    /**
     * A query plan constraint that must be fulfilled for this max match map to be correct. In other words, these
     * constraints are assumptions we base the construction of the map on. If the assumptions cannot be guaranteed to
     * hold, the contents of this max match map cannot be trusted and should be discarded.
     */
    @Nonnull
    private final QueryPlanConstraint queryPlanConstraint;
    /**
     * A value equivalence used to establish equality.
     */
    @Nonnull
    private final ValueEquivalence valueEquivalence;

    /**
     * Creates a new instance of {@link MaxMatchMap}.
     * @param mapping the {@link Value} mappings from query side to candidate side
     * @param queryValue the query result from which the mapping keys originate
     * @param candidateValue the candidate result from which the mapping values originate
     * @param rangedOverAliases a set of aliases that should not be considered constant
     * @param queryPlanConstraint a query plan constraint synthesized when the mapping was computed
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

    /**
     * Express the {@link #queryValue} in terms of the {@link #candidateValue} which is now referred to via
     * the {@code candidateAlias} passed in.
     * @param candidateAlias the alias we should use to refer to the {@link #candidateValue}
     * @return an optional containing the translated query value or {@code Optional.empty()}.
     */
    @Nonnull
    public Optional<Value> translateQueryValueMaybe(@Nonnull final CorrelationIdentifier candidateAlias) {
        final var candidateValue = getCandidateValue();
        final var pulledUpCandidateValueMap =
                candidateValue.pullUp(ImmutableSet.copyOf(mapping.values()), // values may contain duplicates
                        EvaluationContext.empty(),
                        AliasMap.emptyMap(),
                        ImmutableSet.of(), candidateAlias);
        //
        // We now have the right side pulled up, specifically we have a map from each candidate value below,
        // to a candidate value pulled up along the candidateAlias. We also have this max match map, which
        // encapsulates a map from query values to candidate value.
        // In other words we have in this max match map m1 := MAP(queryValues over q -> candidateValues over q') and
        // we just computed m2 := MAP(candidateValues over q' -> candidateValues over candidateAlias). We now
        // chain these two maps to get m1 ○ m2 := MAP(queryValues over q -> candidateValues over candidateAlias).
        //
        final var pulledUpMaxMatchMapBuilder =
                ImmutableMap.<Value, Value>builder();
        for (final var entry : mapping.entrySet()) {
            final var queryPart = entry.getKey();
            final var candidatePart = entry.getValue();
            final var pulledUpdateCandidatePart = Iterables.getOnlyElement(pulledUpCandidateValueMap.get(candidatePart));
            if (pulledUpdateCandidatePart == null) {
                return Optional.empty();
            }
            pulledUpMaxMatchMapBuilder.put(queryPart, pulledUpdateCandidatePart);
        }
        final var pulledUpMaxMatchMap = pulledUpMaxMatchMapBuilder.build();

        //
        // Translate the subtrees in query value with the candidate subtrees we just pulled up.
        //
        final var queryResultValueFromBelow = getQueryValue();
        final var translatedQueryResultValue = Objects.requireNonNull(queryResultValueFromBelow.replace(value -> {
            final var maxMatchValue = pulledUpMaxMatchMap.get(value);
            return maxMatchValue == null ? value : maxMatchValue;
        }));

        //
        // If there are still quantifier references in the value tree that are not constant, we failed to translate
        // the query value.
        //
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
        return translatedQueryValueOptional.map(value -> MaxMatchMap.compute(value, upperCandidateResultValue,
                rangedOverAliases));
    }

    @Nonnull
    public Optional<RegularTranslationMap> pullUpMaybe(@Nonnull final CorrelationIdentifier queryAlias,
                                                       @Nonnull final CorrelationIdentifier candidateAlias) {
        final var translatedQueryValueOptional = translateQueryValueMaybe(candidateAlias);
        return translatedQueryValueOptional
                .map(translatedQueryValue ->
                        RegularTranslationMap.builder()
                                .when(queryAlias).then(TranslationMap.TranslationFunction.adjustValueType(translatedQueryValue))
                                .build());
    }

    @Override
    public String toString() {
        return "M³(" +
                "mapping=" + mapping + ", queryValue=" + queryValue + ", candidateValue=" + candidateValue +
                ", queryPlanConstraint=" + queryPlanConstraint + ")";
    }

    /**
     * Computes the maximum sub-{@link Value}s in {@code queryValue} that have an exact match in the
     * {@code candidateValue}.
     *
     * @param queryValue the query result {@code Value}.
     * @param candidateValue the candidate result {@code Value} we want to search for maximum matches.
     * @param rangedOverAliases a set of aliases that should be considered constant
     *
     * @return a {@link MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap compute(@Nonnull final Value queryValue,
                                      @Nonnull final Value candidateValue,
                                      @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        return compute(queryValue,
                candidateValue,
                rangedOverAliases,
                ValueEquivalence.empty());
    }

    @Nonnull
    public static MaxMatchMap compute(@Nonnull final Value queryValue,
                                      @Nonnull final Value candidateValue,
                                      @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                      @Nonnull final ValueEquivalence valueEquivalence) {
        return compute(queryValue, candidateValue, rangedOverAliases, valueEquivalence,
                ignored -> Optional.empty());
    }

    /**
     * Computes the maximum sub-{@link Value}s in {@code queryValue} that have an exact match in the
     * {@code candidateValue}.
     * <br>
     * For certain shapes of {@link Value}s, multiple matches can be found, this method is guaranteed to always find the
     * maximum part of the query sub-{@link Value} that matches with a sub-{@link Value} on the candidate side.
     *
     * @param queryValue the query result {@code Value}
     * @param candidateValue the candidate result {@code Value} we want to search for maximum matches
     * @param rangedOverAliases a set of aliases that should be considered constant
     * @param valueEquivalence an {@link ValueEquivalence} that informs the logic about equivalent value subtrees
     * @param unmatchedHandlerFunction function that is invoked if a match between a query {@link Value} and a candidate
     *        {@link Value} cannot be established
     * @return a {@link  MaxMatchMap} of all maximum matches. Note that the returned {@link MaxMatchMap} always exists,
     *         it is possible, however, that it might not contain all necessary mappings to perform a pull-up when it
     *         is used.
     */
    @Nonnull
    public static MaxMatchMap compute(@Nonnull final Value queryValue,
                                      @Nonnull final Value candidateValue,
                                      @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                      @Nonnull final ValueEquivalence valueEquivalence,
                                      @Nonnull final Function<Value, Optional<Value>> unmatchedHandlerFunction) {
        if (logger.isTraceEnabled()) {
            logger.trace("calculate begin queryValue={}, candidateValue={}", queryValue, candidateValue);
        }

        MaxMatchMap bestMaxMatchMap = null;
        try {
            final var maxMatchOptional =
                    shortCircuitMaybe(queryValue, candidateValue, rangedOverAliases, valueEquivalence);
            if (maxMatchOptional.isPresent()) {
                return maxMatchOptional.get();
            }

            //
            // Call the recursive part of the matching logic.
            //
            final var resultsMap =
                    recurseQueryResultValue(queryValue, candidateValue, rangedOverAliases,
                            valueEquivalence, unmatchedHandlerFunction, new IdentityHashMap<>(), -1,
                            new ArrayDeque<>(), Integer.MAX_VALUE, new HashSet<>());

            //
            // Pick a match which has the minimum max depth among all the matches.
            //
            int currentMaxDepth = Integer.MAX_VALUE;
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
                        QueryPlanConstraint.noConstraint(), valueEquivalence);
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("calculate end bestMaxMatchMap={}", bestMaxMatchMap);
            }
        }

        return bestMaxMatchMap;
    }

    @Nonnull
    private static Optional<MaxMatchMap> shortCircuitMaybe(final @Nonnull Value queryValue,
                                                           final @Nonnull Value candidateValue,
                                                           final @Nonnull Set<CorrelationIdentifier> rangedOverAliases,
                                                           final @Nonnull ValueEquivalence valueEquivalence) {
        if (rangedOverAliases.size() != 1) {
            return Optional.empty();
        }

        final var singularRangedOverAlias = Iterables.getOnlyElement(rangedOverAliases);

        //
        // Attempt to short-circuit if the candidate is simple
        //
        if ((!(candidateValue instanceof QuantifiedObjectValue)) ||
                !((QuantifiedObjectValue)candidateValue).getAlias()
                        .equals(singularRangedOverAlias)) {
            return Optional.empty();
        }

        if (!queryValue.getCorrelatedTo().contains(singularRangedOverAlias)) {
            return Optional.empty();
        }

        for (final var value : queryValue.preOrderIterable()) {
            if (value instanceof QuantifiedValue) {
                if (!(value instanceof QuantifiedObjectValue)) {
                    return Optional.empty();
                }
            }
        }

        return Optional.of(new MaxMatchMap(ImmutableMap.of(candidateValue, candidateValue),
                queryValue, candidateValue, rangedOverAliases, QueryPlanConstraint.noConstraint(),
                valueEquivalence));
    }

    /**
     * Recursion logic to match {@code queryValue} to {@code candidateValue}. Without any optimization,
     * we recursively carry out the following steps.
     * <ul>
     *     <li> Recurse into the children of the {@code queryValue}. The result will be a map from values to
     *          {@link MatchResult} for each child of {@link #queryValue}. The keys are variants of the child
     *          value that are paired up with the matching result that contains the actual correspondences to be used
     *          in the eventual {@link MaxMatchMap} that is being computed. Form the cross-product of all these
     *          entries to create variants for this level. Each variant maintains its own matches. Add these variants
     *          to our own results.
     *     </li>
     *     <li> Apply a set of simplifications on the current root of the query value to further generate new variants
     *          that can be matched and recurse for each variant. Add the results of that recursion to the our results.
     *     </li>
     * </ul>
     * Applying just these two steps indiscriminately is wasteful and can lead to computational problems or memory
     * exhaustion for even simple cases. In particular, we cannot know a priori which expansions performed by the
     * simplification engine are beneficial and which ones are not and will just lead to rather pointless searching in
     * without any meaningful results.
     * <br>
     * In order to limit the search space we employ a number of techniques:
     * <ul>
     *     <li>
     *         We define a property called max depth that is defined as the maximum number of steps from the current
     *         root in {@code queryValue} that is a match to a subtree in {@code candidateValue}. This is a quality
     *         measure and allows us to prune variants with higher maximum depth.
     *     </li>
     *     <li>
     *         We limit the algorithm to only search for matches up to a certain depth. This allows us to use
     *         branch-and-bound while recursing into subtrees.
     *     </li>
     *     <li>
     *         We never expand (simplify) a value tree more than once. We maintain a map to track which values have been
     *         expanded already.
     *     </li>
     *     <li>
     *         We memoize the results of a recursion if possible in order to potentially reuse them later.
     *     </li>
     * </ul>
     * Unfortunately, this matching problem is not always local meaning that if we don't find a match between the
     * generated variants and the candidate value, we cannot just dismiss these variants as there might be some
     * parent value that through the help of one these variants may become a better match. That can only happen,
     * however, if any parent is already partially matching some reachable subtree on the candidate side. If, on the
     * other hand, that is not the case, the problem becomes local, and we can prune variants as we like.
     * <br>
     * The logic in this method determines whether the problem is local or not and reacts accordingly. Since that
     * is not particularly straightforward, the code is quite dense and very probably can be iterated upon.
     * @param currentQueryValue the current query value which is a sub {@link Value} of the root query side value
     * @param candidateValue the candidate value (root)
     * @param rangedOverAliases a set of aliases that are not constant
     * @param valueEquivalence a value equivalence
     * @param knownValueMap a memoization map of values that we computed the result for already
     * @param descendOrdinal the ordinal number of the child we descended through to get to {@code currentQueryValue}
     *        from its direct parent
     * @param matchers matchers for all parent values
     * @param maxDepthBound the maximum depth we consider whe searching for matches or {@link Integer#MAX_VALUE} if
     *        there is no maximum depth.
     * @param expandedValues a set of values we already have expanded and should not attempt to expand again.
     * @return a map from variants ({@link Value}s) to {@link MatchResult}s).
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Map<Value, MatchResult> recurseQueryResultValue(@Nonnull final Value currentQueryValue,
                                                                   @Nonnull final Value candidateValue,
                                                                   @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                                                   @Nonnull final ValueEquivalence valueEquivalence,
                                                                   @Nonnull final Function<Value, Optional<Value>> unmatchedHandlerFunction,
                                                                   @Nonnull final IdentityHashMap<Value, Map<Value, MatchResult>> knownValueMap,
                                                                   final int descendOrdinal,
                                                                   @Nonnull final Deque<IncrementalValueMatcher> matchers,
                                                                   final int maxDepthBound,
                                                                   @Nonnull final Set<Value> expandedValues) {
        if (maxDepthBound == 0) {
            return ImmutableMap.of(currentQueryValue, MatchResult.notMatched());
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
                if (logger.isTraceEnabled()) {
                    logger.trace("getting memoized info value={}", currentQueryValue);
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
        // Keep a results map that maps from a Value variant to a match result. Note that if we don't have a potential
        // parent match we can make local decisions in this method, specifically, we can prune the variants that are
        // being generated early on.
        //
        final var bestMatches = new BestMatches(currentQueryValue, !anyParentsMatching);

        final var children = currentQueryValue.getChildren();
        if (Iterables.isEmpty(children)) {
            final var resultForCurrent =
                    computeForCurrent(maxDepthBound, currentQueryValue, candidateValue, rangedOverAliases,
                            valueEquivalence, unmatchedHandlerFunction, ImmutableList.of());
            bestMatches.put(currentQueryValue, resultForCurrent);
        } else {
            final ConstrainedBoolean isFound;
            if (!anyParentsMatching && isCurrentMatching) {
                //
                // We know that no parents are matching but that the current level is potentially matching. Try to
                // properly match the current query value and immediately return if successful as this match also is
                // the maximum match.
                //
                final var matchingPair =
                        findMatchingReachableCandidateValue(currentQueryValue,
                                candidateValue,
                                valueEquivalence,
                                unmatchedHandlerFunction);
                isFound = Objects.requireNonNull(matchingPair.getLeft());
                if  (isFound.isTrue()) {
                    bestMatches.put(currentQueryValue, MatchResult.of(ImmutableMap.of(currentQueryValue,
                            Objects.requireNonNull(matchingPair.getRight())), 0, isFound.getConstraint()));
                }
            } else {
                isFound = ConstrainedBoolean.falseValue();
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
                        ImmutableList.<Collection<Map.Entry<Value, MatchResult>>>builder();
                int i = 0;
                for (final var childrenIterator = children.iterator();
                         childrenIterator.hasNext(); i++) {
                    final var child = childrenIterator.next();

                    if (logger.isTraceEnabled()) {
                        logger.trace("recursing into child max_depth_bound={}, value={}", childrenMaxDepthBound, child);
                    }
                    final var childrenResultsMap =
                            recurseQueryResultValue(child, candidateValue, rangedOverAliases, valueEquivalence,
                                    unmatchedHandlerFunction, knownValueMap, i, localMatchers, childrenMaxDepthBound,
                                    expandedValues);

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
                            computeForCurrent(maxDepthBound, resultQueryValue, candidateValue, rangedOverAliases,
                                    valueEquivalence, unmatchedHandlerFunction, childrenResultEntries);
                    bestMatches.put(resultQueryValue, resultForCurrent);
                }
            }
        }

        Verify.verify(!bestMatches.isEmpty());
        Verify.verify(bestMatches.getCurrentMaxDepth() >= 0);

        //
        // Try to transform the current query value into a more matchable shape -- and recurse with that new tree.
        // Only attempt to create variants if any parents are potentially matching (as we need to enumerate all
        // potential subtrees) OR if we have not found a perfect match (depth == 0) yet. Also don't attempt to generate
        // variants if we are in the middle of an expansion of the same value.
        //
        if ((anyParentsMatching || bestMatches.getCurrentMaxDepth() > 0) &&
                !expandedValues.contains(currentQueryValue)) {
            try {
                expandedValues.add(currentQueryValue);
                final var constrainedExpandedCurrentQueryValues =
                        Simplification.simplifyCurrent(currentQueryValue,
                                EvaluationContext.empty(), AliasMap.emptyMap(), rangedOverAliases,
                                MaxMatchMapSimplificationRuleSet.instance());
                for (final var constrainedExpandedCurrentQueryValue : constrainedExpandedCurrentQueryValues) {
                    // there should never be any actual constraints on the result
                    final var expandedCurrentQueryValue =
                            constrainedExpandedCurrentQueryValue.getUnconstrained();
                    final var currentMaxDepthBound =
                            anyParentsMatching
                            ? Integer.MAX_VALUE
                            : (bestMatches.getCurrentMaxDepth() == Integer.MAX_VALUE ? maxDepthBound : bestMatches.getCurrentMaxDepth());

                    if (logger.isTraceEnabled()) {
                        logger.trace("recursing into variant max_depth_bound={}, value={}", currentMaxDepthBound,
                                expandedCurrentQueryValue);
                    }

                    final var expandedResultsMap =
                            recurseQueryResultValue(expandedCurrentQueryValue, candidateValue,
                                    rangedOverAliases, valueEquivalence, unmatchedHandlerFunction, knownValueMap,
                                    descendOrdinal, matchers, currentMaxDepthBound, expandedValues);
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

        // memoize the result is appropriate
        if (!anyParentsMatching && !expandedValues.contains(currentQueryValue)) {
            if (logger.isTraceEnabled()) {
                logger.trace("memoizing value={}", currentQueryValue);
            }
            knownValueMap.put(currentQueryValue, resultMap);
        }
        return resultMap;
    }

    /**
     * Compute a {@link MatchResult} for the current {@code resultQueryValue} given the results from recursing
     * into its children.
     * @param maxDepthBound the maximum depth bound
     * @param resultQueryValue the current result query value at consideration
     * @param candidateValue the candidate value
     * @param rangedOverAliases a set of aliases that are not constant
     * @param valueEquivalence a value equivalence
     * @param childrenResultEntries the result entries computed for the children
     * @return a match result that is either a perfect match of depth {@code 0} or a non-perfect match that contains
     *         the mappings of all children with an adjusted maximum depth
     */
    @Nonnull
    private static MatchResult computeForCurrent(final int maxDepthBound,
                                                 @Nonnull final Value resultQueryValue,
                                                 @Nonnull final Value candidateValue,
                                                 @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                                 @Nonnull final ValueEquivalence valueEquivalence,
                                                 @Nonnull final Function<Value, Optional<Value>> unmatchedHandlerFunction,
                                                 @Nonnull final List<Map.Entry<Value, MatchResult>> childrenResultEntries) {
        Verify.verify(maxDepthBound > 0);

        final var matchingPair =
                findMatchingReachableCandidateValue(resultQueryValue,
                        candidateValue,
                        valueEquivalence,
                        unmatchedHandlerFunction);
        final var isFound = Objects.requireNonNull(matchingPair.getLeft());
        if (isFound.isTrue()) {
            return MatchResult.of(ImmutableMap.of(resultQueryValue,
                            Objects.requireNonNull(matchingPair.getRight())), 0, isFound.getConstraint());
        }

        var childrenConstraint = QueryPlanConstraint.noConstraint();
        final var childrenResultsMap = new LinkedHashMap<Value, Value>();
        int childrenMaxDepth = -1;
        for (final var childrenResultsEntry : childrenResultEntries) {
            final var childValue = childrenResultsEntry.getKey();
            final var childResult =
                    childrenResultsEntry.getValue();
            final var childValueMap = childResult.getValueMap();
            if (childValueMap.isEmpty()) {
                if (!Sets.intersection(childValue.getCorrelatedTo(), rangedOverAliases).isEmpty()) {
                    return MatchResult.notMatched();
                }
                // childValue is constant with respect to rangedOverAliases
                childrenMaxDepth = Math.max(childrenMaxDepth, 0);
            } else {
                childrenMaxDepth = Math.max(childrenMaxDepth, childResult.getMaxDepth());
            }

            if (maxDepthBound < Integer.MAX_VALUE && maxDepthBound - 1 < childrenMaxDepth) {
                return MatchResult.notMatched();
            }

            childrenResultsMap.putAll(childValueMap);
            childrenConstraint = childrenConstraint.compose(childResult.getQueryPlanConstraint());
        }
        childrenMaxDepth = childrenMaxDepth == -1 ? Integer.MAX_VALUE : childrenMaxDepth;
        return MatchResult.of(childrenResultsMap,
                childrenMaxDepth == Integer.MAX_VALUE ? Integer.MAX_VALUE : childrenMaxDepth + 1, childrenConstraint);
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Pair<ConstrainedBoolean, Value> findMatchingReachableCandidateValue(@Nonnull final Value currentQueryValue,
                                                                                       @Nonnull final Value candidateValue,
                                                                                       @Nonnull final ValueEquivalence valueEquivalence,
                                                                                          @Nonnull final Function<Value, Optional<Value>> unmatchedHandlerFunction) {
        for (final var currentCandidateValue : candidateValue
                // when traversing the candidate in pre-order, only descend into structures that can be referenced
                // from the top expression. For example, rcv's components can be referenced however an arithmetic
                // operator's children can not be referenced.
                // It is crucial to do this in pre-order to guarantee matching the maximum (sub-)value of the candidate.
                .preOrderIterable(v -> v instanceof RecordConstructorValue)) {
            if (currentCandidateValue == candidateValue) {
                if (currentQueryValue instanceof QuantifiedRecordValue) {
                    return Pair.of(ConstrainedBoolean.alwaysTrue(), currentCandidateValue);
                }
            }

            final var semanticEquals =
                    currentQueryValue.semanticEquals(currentCandidateValue, valueEquivalence);
            if (semanticEquals.isTrue()) {
                return Pair.of(semanticEquals, currentCandidateValue);
            }
        }

        final var unmatchedHandlerResult = unmatchedHandlerFunction.apply(currentQueryValue);
        return unmatchedHandlerResult.map(value -> Pair.of(ConstrainedBoolean.alwaysTrue(), value))
                .orElseGet(() -> Pair.of(ConstrainedBoolean.falseValue(), null));
    }

    /**
     * Helper class to encapsulate away details we have to consider when a new obtained match is added to the result
     * set of matches. When we are pruning the new match may or may not replace the current best match. If we are not
     * pruning, the new match just gets added to a map of matches as all matches are returned.
     */
    private static class BestMatches {
        @Nonnull
        private final Value currentQueryValue;
        private final boolean isPruning;
        private int currentMaxDepth = -1;
        @Nullable
        private Map<Value, MatchResult> matchMap;
        @Nullable
        private Value value;
        @Nullable
        private MatchResult matchResult;

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
        private Map<Value, MatchResult> getMatchMap() {
            return Objects.requireNonNull(matchMap);
        }

        @Nonnull
        public Value getValue() {
            return Objects.requireNonNull(value);
        }

        @Nonnull
        public MatchResult getMatchResult() {
            return Objects.requireNonNull(matchResult);
        }

        @Nonnull
        public Map<Value, MatchResult> toResultMap() {
            if (isEmpty()) {
                return ImmutableMap.of(currentQueryValue, MatchResult.notMatched());
            }
            if (isPruning()) {
                return ImmutableMap.of(getValue(), getMatchResult());
            }
            return getMatchMap();
        }

        public void put(@Nonnull final Value newQueryValue,
                        @Nonnull final MatchResult newMatchResult) {
            if (isPruning()) {
                if (matchResult == null || newMatchResult.getMaxDepth() < currentMaxDepth) {
                    this.value = newQueryValue;
                    this.matchResult = newMatchResult;
                    this.currentMaxDepth = newMatchResult.getMaxDepth();
                }
            } else {
                if (currentMaxDepth == -1 || newMatchResult.getMaxDepth() < currentMaxDepth) {
                    currentMaxDepth = newMatchResult.getMaxDepth();
                }
                getMatchMap().put(newQueryValue, newMatchResult);
            }
        }
    }

    /**
     * Class that functions as a partial matcher between a {@link Value} on the query and a reachable {@link Value} on
     * the candidate side.
     * <br>
     * Normally, we establish semantic equality between two {@link Value}s by considering not just the values themselves
     * but the subtrees rooted at these values. In order to compute semantic equality, we call
     * {@link Value#equalsWithoutChildren(Value)} and if successful recurse into the subtrees and establish semantic
     * equality for the children. The approach this class takes is somewhat the other way around. For a value
     * {@link #currentQueryValue} and an initial candidate side root value, this class establishes all potential matches
     * that are reachable from the candidate root value for this value (sans subtree). These matches may not turn out
     * to be correct full matches but so far we cannot disprove them to be correct. We then descend into a child on the
     * query side and try to find all candidate side sub values using the already established potential matches whose
     * child at the same child slot also potentially matches. This can be repeated. Naturally, the number of potential
     * matches decreases as we descend a path on the query side that incurs fewer and fewer matching counterparts on the
     * candidate side. Eventually, we end up at a leaf value and still have potential matches or the list of potential
     * matches is empty and any further descending will only continue to produce empty match lists.
     * <br>
     * Example 1: We attempt to match {@code rcv(rcv(q.x, q.y), rcv(q.a, q.b))} to {@code rcv(q.x, rcv(q.a, q.c))}.
     * On the top level, the {@code rcv(■, ■)} on the query side potentially matches the {@code rcv(q.x, rcv(q.a, q.c))}
     * on the candidate side at root level ({@code ■} denoting an opaque, not-yet considered subtree). Let's say, we now
     * descend into the second child on the query side. {@code rcv(■, rcv(■, ■))} still partially matches
     * {@code rcv(■, rcv(q.a, q.c))} on the candidate side. We now descend into the first child of the query side and
     * establish that there is still a match between: {@code rcv(■, rcv(q.a, ■))} and {@code rcv(■, rcv(q.a, ■))} on the
     * candidate side. If we had descended into the second child instead, we would have not matched anything, though:
     * {@code rcv(■, rcv(■, q.b))} does not match {@code rcv(■, rcv(■, q.c))}.
     * <br>
     * Example 2: We attempt to match {@code rcv(q.a, q.b))} to {@code rcv(q.x, rcv(q.a, q.b), rcv(q.a, q.c))}.
     * On top level, nothing matches to the top level of the candidate side. We can, however, establish two
     * reachable potential matches for {@code rcv(■, ■)} on the candidate side: {@code rcv(q.a, q.b)} and
     * {@code rcv(q.a, q.c)}. When we now descend further into the second child on the query side, one of these two
     * matches is invalidated: {@code rcv(■, q.b)} still matches {@code rcv(■, q.b)}, but does not match
     * {@code rcv(■, q.c)}
     */
    private abstract static class IncrementalValueMatcher {
        private static final ExplainTokensWithPrecedence UNMATCHED =
                ExplainTokensWithPrecedence.of(new ExplainTokens().addToString("■"));
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
        public abstract ExplainTokensWithPrecedence explain(@Nonnull Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers);

        public boolean anyMatches() {
            return !getMatchingCandidateValues().isEmpty();
        }

        @Override
        public String toString() {
            final var explainTokens =
                    explain(Collections.nCopies(Iterables.size(currentQueryValue.getChildren()),
                            () -> UNMATCHED)).getExplainTokens();
            final var explainString = explainTokens.render(DefaultExplainFormatter.forDebugging()).toString();
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
                public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
                    Verify.verify(Iterables.size(explainSuppliers) == Iterables.size(currentQueryValue.getChildren()));
                    final var parentExplainFunctionsBuilder =
                            ImmutableList.<Supplier<ExplainTokensWithPrecedence>>builder();
                    final var parentSize = Iterables.size(parent.getCurrentQueryValue().getChildren());
                    for (int i = 0; i < parentSize; i ++) {
                        if (i != descendOrdinal) {
                            parentExplainFunctionsBuilder.add(() -> UNMATCHED);
                        } else {
                            parentExplainFunctionsBuilder.add(() -> currentQueryValue.explain(explainSuppliers));
                        }
                    }
                    return parent.explain(parentExplainFunctionsBuilder.build());
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
                                                QueryPlanConstraint.noConstraint()));
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
                public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
                    return getCurrentQueryValue().explain(explainSuppliers);
                }
            };
        }
    }

    /**
     * Helper class to capture the result of matching one variant on the query side to the candidate side.
     */
    private static class MatchResult {
        private static final MatchResult NOT_MATCHED =
                new MatchResult(ImmutableMap.of(), Integer.MAX_VALUE, QueryPlanConstraint.noConstraint());

        @Nonnull
        private final Map<Value, Value> valueMap;
        private final int maxDepth;
        @Nonnull
        private final QueryPlanConstraint queryPlanConstraint;

        private MatchResult(@Nonnull final Map<Value, Value> valueMap,
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

        public static MatchResult of(@Nonnull final Map<Value, Value> valueMap,
                                     final int maxDepth,
                                     @Nonnull final QueryPlanConstraint queryPlanConstraint) {
            Verify.verify(maxDepth >= 0);
            return new MatchResult(valueMap, maxDepth, queryPlanConstraint);
        }

        public static MatchResult notMatched() {
            return NOT_MATCHED;
        }
    }
}
