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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
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
 * A maximum match between a query value and a candidate value: a set of matches between subtrees of the two values.
 * With these matches, the planner can compute the query value from the result of the candidate, see
 * {@link #translateQueryValueMaybe}. The short name of a max match map is M³.
 *
 * <p>Terms:
 * <ul>
 *     <li>The <em>query value</em> is the value that the planner wants to compute. The <em>candidate value</em> is the
 *         result value of a match candidate, for example the record that an index scan returns.</li>
 *     <li>A <em>node</em> is a value in a value tree. A <em>subtree</em> is a node together with all nodes below it. A
 *         <em>leaf</em> is a node without children. The <em>root</em> is the node at the top of the tree.</li>
 *     <li>The <em>ranged-over aliases</em> are the aliases of the quantifiers that the expression of the candidate
 *         ranges over. A value is <em>constant</em> if it does not refer to a ranged-over alias. A constant value does
 *         not need a match: its result is the same for each record of the candidate.</li>
 *     <li>A candidate subtree is <em>reachable</em> if a value can compute it from the result of the candidate, see
 *         below.</li>
 *     <li>A <em>match</em> relates a query subtree to a reachable candidate subtree that is semantically equal to it.
 *         Semantic equality uses the {@link ValueEquivalence} of the computation. This class writes a match as
 *         {@code x -> y}. A caller can also give an unmatched handler, see
 *         {@link #compute(Value, Value, Set, ValueEquivalence, Function)}. Then a match can also relate a query
 *         subtree to a value that the handler returns for it.</li>
 *     <li>A set of matches <em>covers</em> a query subtree if each leaf of the subtree that is not constant is part of
 *         the query subtree of a match.</li>
 *     <li>The <em>max depth</em> of a set of matches that covers a query subtree {@code x} is the largest number of
 *         steps from {@code x} down to the query subtree of a match. The max depth is 0 if {@code x} itself is the
 *         query subtree of a match. A smaller max depth means larger matched subtrees.</li>
 *     <li>A <em>variant</em> of a query subtree is a value that is semantically equal to the subtree but has a
 *         different tree. For example, if {@code q} is a record with the fields {@code a} and {@code b}, then
 *         {@code rcv(q.a as a, q.b as b)} is a variant of {@code q}. Semantic equality compares two trees node by node,
 *         so a variant can match a candidate subtree that the original subtree does not match.</li>
 * </ul>
 * A max match map contains the set of matches that covers the query value with the smallest max depth. Its
 * {@link #getQueryValue() query value} is the variant of the query value that the matches refer to.
 *
 * <p>Example: the query value is
 * <pre>
 * {@code
 *    rcv(fv(qov(q), a) as a, fv(qov(q), b) as b, fv(qov(q), c) as c) or shorter
 *    rcv(q.a as a, q.b as b, q.c as c)
 * }
 * </pre>
 * and the candidate value is
 * <pre>
 * {@code
 *    rcv(q.c as c, q.b as b, q.a as a)
 * }
 * </pre>
 * The two values are not equal, because the fields have a different order. But each field of the query value is equal
 * to a field of the candidate value. So the matches are {@code q.a -> q.a}, {@code q.b -> q.b}, and
 * {@code q.c -> q.c}, with a max depth of 1. If the candidate value is {@code rcv(q.a as a, q.b as b, q.c as c)}, the
 * only match is {@code rcv(q.a as a, q.b as b, q.c as c) -> rcv(q.a as a, q.b as b, q.c as c)}, with a max depth of 0.
 * The matches of the fields are also matches in that case, but they are not <em>maximal</em>: a match of a larger
 * subtree contains them.
 *
 * <p>Reachability: a query subtree can only match a candidate subtree that is reachable from the root of the candidate
 * value. A subtree {@code x} of a value {@code v} is reachable from {@code v} if a value can compute {@code x} from the
 * result of {@code v}. For example, {@code q.b} is reachable from {@code v = rcv(q.c as c, q.b as b, q.a as a)},
 * because {@code v.b} gives {@code q.b}. A field access is the inverse of a record construction, so the children of a
 * record constructor are reachable. Other operations do not have an inverse in general. For example, {@code q.b} is not
 * reachable from {@code q.c + q.b + q.a}, because the sum does not tell the value of {@code q.b}. In
 * {@code v = rcv(q.c + q.b + q.a, q.c as c, q.a as a)}, the value {@code v._0 - v.c - v.a} gives {@code q.b}, but this
 * class cannot find such a value. So this class uses a narrow definition: a candidate subtree is reachable if the path
 * from the root of the candidate value to it goes through record constructors only.
 *
 * <p>Coverage: a max match map is only useful if its matches cover the query value. For example, the query value
 * {@code rcv(q.a as a, q.b as b, q.c as c, q.d as d)} cannot match {@code rcv(q.a as a, q.b as b, q.c as c)}, because
 * no match covers {@code q.d}.
 *
 * <p>Pull-up: the purpose of the matches is to express the query value in terms of a reference {@code v} to the result
 * of the candidate. For example, the query value {@code rcv(q.a as a, q.b as b, q.c as c)} matches the candidate value
 * {@code rcv(q.c as _0, q.b as _1, q.a as _2)} with the matches {@code q.a -> q.a}, {@code q.b -> q.b}, and
 * {@code q.c -> q.c}. Each candidate subtree of a match is reachable from {@code v}: {@code v._2} gives {@code q.a},
 * {@code v._1} gives {@code q.b}, and {@code v._0} gives {@code q.c}. The pull-up replaces each matched query subtree
 * with the value that gives its candidate subtree. The result is {@code rcv(v._2 as a, v._1 as b, v._0 as c)}. It has
 * the same result as the query value, but it refers to {@code v} only, not to {@code q}.
 */
public class MaxMatchMap {
    private static final Logger logger = LoggerFactory.getLogger(MaxMatchMap.class);

    /**
     * The matches, from subtrees of {@link #queryValue} to reachable subtrees of {@link #candidateValue} or to values
     * of the unmatched handler. The matches are maximal: if this map contains a match {@code x -> y}, it contains no
     * match for a subtree of {@code x}.
     */
    @Nonnull
    private final Map<Value, Value> mapping;
    /**
     * The variant of the query value that the keys of {@link #mapping} are subtrees of. Like the candidate value, it
     * refers to the quantifiers of the candidate: in matching, the caller translates the query value to these
     * quantifiers before it computes the max match map.
     */
    @Nonnull
    private final Value queryValue;
    /**
     * The candidate value. The matches relate query subtrees to reachable subtrees of it.
     */
    @Nonnull
    private final Value candidateValue;
    /**
     * The aliases of the quantifiers that the expression of the candidate ranges over. A value that refers to one of
     * these aliases is not constant, so a match must cover it. These aliases are not deep correlations.
     */
    @Nonnull
    private final Set<CorrelationIdentifier> rangedOverAliases;
    /**
     * The constraint that must be true for the matches to be correct. Semantic equality can depend on assumptions, for
     * example on the value of a constant. This constraint contains the assumptions of all matches. If the planner
     * cannot guarantee the constraint, it must not use this max match map.
     */
    @Nonnull
    private final QueryPlanConstraint queryPlanConstraint;
    /**
     * The value equivalence of the computation. Two values are semantically equal if their trees are equal node by
     * node, or if this equivalence defines them as equal. An example of the second case is two quantified values whose
     * aliases an alias map relates.
     */
    @Nonnull
    private final ValueEquivalence valueEquivalence;

    /**
     * Creates a new instance of {@link MaxMatchMap}.
     *
     * @param mapping the matches, see {@link #getMap()}
     * @param queryValue the variant of the query value that the keys of {@code mapping} are subtrees of
     * @param candidateValue the candidate value that the matches refer to
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over, which are not constant
     * @param queryPlanConstraint the constraint that the matches depend on
     * @param valueEquivalence the {@link ValueEquivalence} that the computation of the matches used
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

    /**
     * Returns the matches, from subtrees of {@link #getQueryValue()} to reachable subtrees of
     * {@link #getCandidateValue()} or to values of the unmatched handler.
     *
     * @return the matches
     */
    @Nonnull
    public Map<Value, Value> getMap() {
        return mapping;
    }

    /**
     * Returns the variant of the query value that the matches refer to. It can differ from the query value that was
     * given to {@link #compute}, because the computation can replace parts of the query value with variants.
     *
     * @return the variant of the query value that the keys of {@link #getMap()} are subtrees of
     */
    @Nonnull
    public Value getQueryValue() {
        return queryValue;
    }

    /**
     * Returns the candidate value.
     *
     * @return the candidate value that the matches refer to
     */
    @Nonnull
    public Value getCandidateValue() {
        return candidateValue;
    }

    /**
     * Returns the aliases of the quantifiers that the candidate ranges over.
     *
     * @return the aliases that are not constant
     */
    @Nonnull
    public Set<CorrelationIdentifier> getRangedOverAliases() {
        return rangedOverAliases;
    }

    /**
     * Returns the constraint that must be true for the matches to be correct.
     *
     * @return the constraint that the matches depend on
     */
    @Nonnull
    public QueryPlanConstraint getQueryPlanConstraint() {
        return queryPlanConstraint;
    }

    /**
     * Returns the value equivalence that the computation of the matches used.
     *
     * @return the value equivalence
     */
    @Nonnull
    public ValueEquivalence getValueEquivalence() {
        return valueEquivalence;
    }

    /**
     * Expresses the query value in terms of a reference to the result of the candidate, see the pull-up in the
     * documentation of this class. The reference is the quantified value of {@code candidateAlias}. The method has
     * three steps:
     * <ol>
     *     <li>It pulls up each candidate subtree of a match along {@code candidateAlias}. The pulled-up value computes
     *         the candidate subtree from the reference. For example, the pulled-up value of {@code q.b} in
     *         {@code rcv(q.a as a, q.b as b)} is {@code v.b} if {@code candidateAlias} is {@code v}.</li>
     *     <li>It replaces each query subtree of a match in the query value with the pulled-up value of its candidate
     *         subtree.</li>
     *     <li>It checks that the result refers to no ranged-over alias. If the result still refers to one, the matches
     *         do not cover the query value, and the method fails.</li>
     * </ol>
     *
     * @param candidateAlias the alias of the reference to the result of the candidate
     * @return the translated query value, or {@code Optional.empty()} if a candidate subtree of a match cannot be
     *         pulled up or if the matches do not cover the query value
     */
    @Nonnull
    public Optional<Value> translateQueryValueMaybe(@Nonnull final CorrelationIdentifier candidateAlias) {
        final var candidateValue = getCandidateValue();
        final var pulledUpCandidateValueMap =
                candidateValue.pullUp(ImmutableSet.copyOf(mapping.values()), // matches can share a candidate subtree
                        EvaluationContext.empty(),
                        AliasMap.emptyMap(),
                        ImmutableSet.of(), candidateAlias);
        //
        // The map that we just computed relates each candidate subtree of a match to its pulled-up value over
        // candidateAlias. The matches relate query subtrees to candidate subtrees. Chain the two maps: in other
        // words, the matches are m1 := MAP(queryValues over q -> candidateValues over q'), the pulled-up values are
        // m2 := MAP(candidateValues over q' -> candidateValues over candidateAlias), and the chained map is
        // m1 ○ m2 := MAP(queryValues over q -> candidateValues over candidateAlias).
        //
        final var pulledUpMaxMatchMapBuilder =
                ImmutableMap.<Value, Value>builder();
        for (final var entry : mapping.entrySet()) {
            final var queryPart = entry.getKey();
            final var candidatePart = entry.getValue();
            final var pulledUpdateCandidatePart = pulledUpCandidateValueMap.get(candidatePart);
            if (pulledUpdateCandidatePart.isEmpty()) {
                return Optional.empty();
            }
            pulledUpMaxMatchMapBuilder.put(queryPart, Iterables.getOnlyElement(pulledUpdateCandidatePart));
        }
        final var pulledUpMaxMatchMap = pulledUpMaxMatchMapBuilder.build();

        //
        // Replace each matched query subtree in the query value with the pulled-up value of its candidate subtree.
        //
        final var queryResultValueFromBelow = getQueryValue();
        final var translatedQueryResultValue = Objects.requireNonNull(queryResultValueFromBelow.replace(value -> {
            final var maxMatchValue = pulledUpMaxMatchMap.get(value);
            return maxMatchValue == null ? value : maxMatchValue;
        }));

        //
        // If the result still refers to a ranged-over alias, a part of the query value that is not constant has no
        // match, and the translation fails.
        //
        if (!Sets.intersection(rangedOverAliases, translatedQueryResultValue.getCorrelatedTo()).isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(translatedQueryResultValue);
    }

    /**
     * Adjusts this max match map to an expression of the candidate that ranges over the current candidate expression.
     * The upper expression refers to the result of the current candidate through {@code upperCandidateAlias}. The
     * method translates the query value along that alias, see {@link #translateQueryValueMaybe}. Then it computes the
     * max match map of the translated query value against the result value of the upper expression.
     *
     * @param upperCandidateAlias the alias through which the upper expression refers to the current candidate
     * @param upperCandidateResultValue the result value of the upper expression
     * @param rangedOverAliases the aliases of the quantifiers that the upper expression ranges over
     * @return the max match map for the upper expression, or {@code Optional.empty()} if the translation fails
     */
    @Nonnull
    public Optional<MaxMatchMap> adjustMaybe(@Nonnull final CorrelationIdentifier upperCandidateAlias,
                                             @Nonnull final Value upperCandidateResultValue,
                                             @Nonnull final Set<CorrelationIdentifier> rangedOverAliases) {
        final var translatedQueryValueOptional =
                translateQueryValueMaybe(upperCandidateAlias);
        return translatedQueryValueOptional.map(value -> MaxMatchMap.compute(value, upperCandidateResultValue,
                rangedOverAliases));
    }

    /**
     * Creates a translation map that replaces each reference to {@code queryAlias} with the query value, translated
     * along {@code candidateAlias}, see {@link #translateQueryValueMaybe}.
     *
     * @param queryAlias the alias whose references the translation map replaces
     * @param candidateAlias the alias of the reference to the result of the candidate
     * @return the translation map, or {@code Optional.empty()} if the translation of the query value fails
     */
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
     * Computes the max match map of {@code queryValue} against {@code candidateValue}, with an empty value
     * equivalence. See {@link #compute(Value, Value, Set, ValueEquivalence, Function)}.
     *
     * @param queryValue the query value
     * @param candidateValue the candidate value
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over, which are not constant
     * @return the max match map
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

    /**
     * Computes the max match map of {@code queryValue} against {@code candidateValue}, without an unmatched handler.
     * See {@link #compute(Value, Value, Set, ValueEquivalence, Function)}.
     *
     * @param queryValue the query value
     * @param candidateValue the candidate value
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over, which are not constant
     * @param valueEquivalence the value equivalence that semantic equality uses
     * @return the max match map
     */
    @Nonnull
    public static MaxMatchMap compute(@Nonnull final Value queryValue,
                                      @Nonnull final Value candidateValue,
                                      @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                      @Nonnull final ValueEquivalence valueEquivalence) {
        return compute(queryValue, candidateValue, rangedOverAliases, valueEquivalence,
                ignored -> Optional.empty());
    }

    /**
     * Relates the query value as a whole to the candidate value, without a search. For two semantically equal values,
     * {@link #compute(Value, Value, Set, ValueEquivalence)} gives the same result: a match of the two roots has a max
     * depth of 0, which is the smallest max depth. Use this method if the caller knows that the two values are
     * semantically equal.
     *
     * @param queryValue the query value
     * @param candidateValue the candidate value, which must be semantically equal to {@code queryValue}
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over, which are not constant
     * @param valueEquivalence the {@link ValueEquivalence} under which the two values are semantically equal
     * @return a {@link MaxMatchMap} with the single match {@code queryValue -> candidateValue}
     * @throws com.google.common.base.VerifyException if the two values are not semantically equal
     */
    @Nonnull
    public static MaxMatchMap identityOf(@Nonnull final Value queryValue,
                                         @Nonnull final Value candidateValue,
                                         @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                         @Nonnull final ValueEquivalence valueEquivalence) {
        final var isSemanticallyEqual = queryValue.semanticEquals(candidateValue, valueEquivalence);
        Verify.verify(isSemanticallyEqual.isTrue(), "query value is not semantically equal to candidate value");
        return new MaxMatchMap(ImmutableMap.of(queryValue, candidateValue), queryValue, candidateValue,
                rangedOverAliases, isSemanticallyEqual.getConstraint(), valueEquivalence);
    }

    /**
     * Computes the max match map of {@code queryValue} against {@code candidateValue}: the set of matches that covers
     * {@code queryValue}, or a variant of it, with the smallest max depth.
     *
     * <p>The method first tries {@code shortCircuitMaybe}. That method handles the case where the candidate value is
     * the record of the only ranged-over quantifier. Otherwise, the method calls {@code recurseQueryResultValue} for
     * the root of the query value. The recursion returns the best variant of the query value, which is the variant with
     * the smallest max depth, and the matches that cover it. If the matches do not cover any variant, the max match map
     * has no matches.
     *
     * @param queryValue the query value
     * @param candidateValue the candidate value
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over, which are not constant
     * @param valueEquivalence the value equivalence that semantic equality uses
     * @param unmatchedHandlerFunction the function that the computation calls for a query value that is not equal to
     *        any reachable candidate subtree. If the function returns a value, the query value matches that value.
     * @return the max match map. A max match map always exists, but its matches can fail to cover the query value.
     *         Then {@link #translateQueryValueMaybe} fails.
     * @throws com.google.common.base.VerifyException if {@code valueEquivalence} is not the empty equivalence, the
     *         equivalence of an alias map, the equivalence of constants, or a composition of these
     */
    @Nonnull
    public static MaxMatchMap compute(@Nonnull final Value queryValue,
                                      @Nonnull final Value candidateValue,
                                      @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                      @Nonnull final ValueEquivalence valueEquivalence,
                                      @Nonnull final Function<Value, Optional<Value>> unmatchedHandlerFunction) {
        Verify.verify(relatesLeavesOnly(valueEquivalence), "value equivalence must relate leaves only");
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
            // Run the recursion for the root of the query value.
            //
            final var bestMatches =
                    recurseQueryResultValue(queryValue, candidateValue, rangedOverAliases,
                            valueEquivalence, unmatchedHandlerFunction, new IdentityHashMap<>(), -1,
                            new ArrayDeque<>(), Integer.MAX_VALUE, new HashSet<>());

            //
            // Use the best variant of the query value, the variant with the smallest max depth, if matches cover it.
            //
            final var bestMatchResult = bestMatches.getMatchResult();
            if (bestMatchResult.isMatched()) {
                bestMaxMatchMap = new MaxMatchMap(bestMatchResult.getValueMap(), bestMatches.getValue(),
                        candidateValue, rangedOverAliases, bestMatchResult.getQueryPlanConstraint(), valueEquivalence);
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

    /**
     * Returns whether a value equivalence relates leaves only. The recursion asks the equivalence only about some
     * values, see {@link #recurseQueryResultValue}. That is sufficient for an equivalence that relates leaves only, but
     * not for an equivalence that also relates other values. The method accepts the empty equivalence, the equivalence
     * of an alias map, the equivalence of constants, and compositions of these. It rejects all other equivalences.
     *
     * @param valueEquivalence the value equivalence
     * @return {@code true} if the value equivalence and each of its parts relate leaves only
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static boolean relatesLeavesOnly(@Nonnull final ValueEquivalence valueEquivalence) {
        if (valueEquivalence instanceof ValueEquivalence.ThenEquivalence thenEquivalence) {
            return relatesLeavesOnly(thenEquivalence.getFirst()) && relatesLeavesOnly(thenEquivalence.getThen());
        }
        return valueEquivalence == ValueEquivalence.empty() ||
                valueEquivalence instanceof ValueEquivalence.AliasMapBackedValueEquivalence ||
                valueEquivalence instanceof ValueEquivalence.ConstantValueEquivalence;
    }

    /**
     * Computes the max match map without a search if the candidate value is simple, that is if the candidate value is
     * the quantified object value of the only ranged-over alias. Then the result of the candidate is the record of that
     * quantifier, without a change. So each reference to the quantifier in the query value can refer to the result of
     * the candidate instead. The max match map has the single match {@code candidateValue -> candidateValue}. Its key
     * is the quantified object value that the query value contains, and the pull-up replaces each occurrence of it.
     *
     * <p>The method requires that the query value refers to the quantifier. It also requires that each quantified value
     * in the query value is a {@link QuantifiedObjectValue}. Other quantified values, for example a
     * {@link QuantifiedRecordValue}, are not equal to the key of the match, so the pull-up cannot replace them.
     *
     * @param queryValue the query value
     * @param candidateValue the candidate value
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over
     * @param valueEquivalence the value equivalence of the computation
     * @return the max match map, or {@code Optional.empty()} if the candidate value is not simple or if the query value
     *         does not meet the requirements
     */
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
        // The candidate value must be the quantified object value of the only ranged-over alias.
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
     * The recursion of the matching. Each call is one <em>level</em> of the recursion. A level handles one query
     * subtree at one position in the query value, the <em>current value</em>. A level calls itself for each child of
     * the current value and for each expansion of the current value.
     *
     * <p>The recursion has two goals:
     * <ol>
     *     <li><em>Find matches further down.</em> The current value can match a reachable candidate subtree as a whole.
     *         If it does not, matches of subtrees further down can cover it. So the level finds its <em>best
     *         variant</em>: the variant with the smallest max depth, together with the matches that cover it. The
     *         parent uses the best variant for its own partial match.</li>
     *     <li><em>Complete potential matches that started further up.</em> An ancestor of the current value can
     *         potentially match a candidate subtree: the roots of the two are equal, but the nodes below are not
     *         compared yet. The ancestor is equal to the candidate subtree node by node only if each child of the
     *         ancestor has a variant that is equal to the child of the candidate subtree at the same position. So the
     *         level finds such a variant, a <em>witness</em>, for each candidate node at its position. The parent uses
     *         the witnesses for its own full matches and for its own witnesses.</li>
     * </ol>
     * Both goals need the expansions of the current value. An expansion can match better than the current value
     * (goal 1). An expansion can also be equal to a candidate node that the current value is not equal to (goal 2). For
     * example, the field value {@code q.header} cannot be equal to a record constructor of the candidate, but its
     * expansion {@code rcv(q.header.h0 as h0, q.header.h1 as h1)} can.
     *
     * <p>Terms of the recursion:
     * <ul>
     *     <li>A <em>potential match</em> of a query node is a candidate node that is
     *         {@link Value#equalsWithoutChildren} equal to it. The nodes below the two nodes are not compared yet, so a
     *         potential match can turn out to be no match. An {@link IncrementalValueMatcher} keeps potential
     *         matches.</li>
     *     <li>The <em>own potential matches</em> of the current value are the reachable candidate subtrees that
     *         potentially match it, see {@link IncrementalValueMatcher#initial}.</li>
     *     <li>A <em>target</em> of the current value is a candidate node at the position of the current value under a
     *         potential match of an ancestor. The matcher of the ancestor descends along the path from the ancestor to
     *         the current value. It keeps a candidate node only if each query node on that path is
     *         {@link Value#equalsWithoutChildren} equal to the candidate node at the same position, see
     *         {@link IncrementalValueMatcher#descend}. A target does not have to be reachable.</li>
     *     <li>An <em>expansion</em> of the current value is a variant that the rules of
     *         {@link MaxMatchMapSimplificationRuleSet} produce from it. For example, a field value of a record type
     *         expands to a record constructor of its fields. Each expansion is the current value of a level of its
     *         own, at the same position.</li>
     *     <li>A <em>combination</em> is a variant of the current value that keeps the root of the current value and
     *         replaces its children with variants of the children.</li>
     *     <li>A <em>full match</em> is a variant that is itself the query subtree of a match. Its max depth is 0. A
     *         <em>partial match</em> is a variant that the matches of its children cover. Its max depth is the largest
     *         max depth of its children plus one.</li>
     *     <li>A <em>witness</em> of the current value for a candidate node {@code t} is a variant of the current value
     *         that is equal to {@code t}, together with the constraint of that equality. The witness is the combination
     *         of the witnesses of the children for the children of {@code t}, see {@link #witnessMaybe}. Its equality
     *         follows from the way it is built, so no call to {@link Value#semanticEquals} is necessary.</li>
     * </ul>
     *
     * <p>A level does these steps:
     * <ol>
     *     <li>It descends the matchers of its ancestors to its position. The candidate nodes in the descended matchers
     *         are its targets.</li>
     *     <li>If no ancestor has a potential match, goal 2 does not apply: no ancestor can match as a whole through
     *         this position. Then the level uses a memoized result if one exists. If the current value has own
     *         potential matches, the level first tries to match the current value as a whole, see
     *         {@code findMatchingReachableCandidateValue}. If that succeeds, the match is the best variant, because
     *         its max depth is 0, and the level does not recurse into the children.</li>
     *     <li>It recurses into each child. The result of a child is its best variant and its witnesses.</li>
     *     <li>It builds the combination of the best variants of the children (goal 1). If this combination does not
     *         match as a whole, its max depth is the largest max depth of its children plus one. So no other
     *         combination that does not match as a whole has a smaller max depth.</li>
     *     <li>If the combination of the best variants is not a full match, the level does this for each own potential
     *         match {@code t}: it combines the witnesses of the children for the children of {@code t}. If all these
     *         witnesses exist, the combination is a full match of {@code t}, see {@link #witnessMaybe}.</li>
     *     <li>For each target {@code t}, it builds the same kind of combination. If all the witnesses exist, the level
     *         keeps the combination as its witness for {@code t} (goal 2).</li>
     *     <li>If an ancestor has a potential match, or if the level has no full match yet, it recurses into each
     *         expansion of the current value, at the same position. It keeps the best variant of an expansion if that
     *         variant has a smaller max depth, and it adds the witnesses of the expansion.</li>
     * </ol>
     * A leaf has no children. So for a leaf, steps 3 to 5 reduce to one comparison of the leaf with the reachable
     * candidate subtrees, and in step 6 the leaf is its own witness for each target that has no children.
     *
     * <p>So a level builds at most {@code 1 + m + n} combinations, where {@code m} is the number of its own potential
     * matches and {@code n} is the number of its targets. A combination of the best variant of one child and the
     * witness of another child is not necessary. If such a combination is not equal to a candidate node, the
     * combination of the best variants is at least as good. If it is equal to a candidate node {@code t} node by node,
     * all its children are equal to the children of {@code t}. Then it is equal to the combination of the witnesses
     * for {@code t}. This argument is about equality node by node. The value equivalence can also define two values as
     * equal. The level asks the equivalence only in {@code findMatchingReachableCandidateValue}, for the current value
     * and for the combination of the best variants, and in {@link #witnessMaybe}, for the children of a witness. That
     * is sufficient if the equivalence relates leaves only, as the equivalence of an alias map does.
     * {@link #compute(Value, Value, Set, ValueEquivalence, Function)} accepts only such equivalences, see
     * {@link #relatesLeavesOnly}.
     *
     * <p>Three more rules limit the search:
     * <ul>
     *     <li>The max depth bound. If no ancestor has a potential match, a variant replaces the best variant so far
     *         only if its max depth is smaller. So the level of an expansion gets the max depth of the best variant so
     *         far as its bound. If no variant has a match yet, it gets the bound of the current level. If an ancestor
     *         has a potential match, the expansion gets no bound, because goal 2 needs all witnesses. A level with the
     *         bound {@code b} gives the bound {@code b - 1} to its children, because the max depth of a partial match
     *         is the largest max depth of its children plus one. If a matcher of the level has a potential match, the
     *         children get no bound, because a child can be part of a full match further up. A level with a bound of
     *         0 returns at once without a match: no max depth is smaller than 0.</li>
     *     <li>A level does not expand a value whose expansion is in progress. This prevents an endless recursion. For
     *         example, the expansion of {@code q.header} contains {@code q.header.h0}, the expansion of
     *         {@code q.header.h0} contains {@code q.header}, and that value expands to the same record constructor
     *         again.</li>
     *     <li>The level memoizes its result if no ancestor has a potential match and if the expansion of the current
     *         value is not in progress. Then the result does not depend on the matchers of the ancestors, and the rule
     *         above did not stop the level from expanding the current value.</li>
     * </ul>
     *
     * @param currentQueryValue the current value: a subtree of the query value or of a variant of it
     * @param candidateValue the candidate value (root)
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over, which are not constant
     * @param valueEquivalence the value equivalence of the computation
     * @param unmatchedHandlerFunction the function to call for a variant that is not equal to any reachable candidate
     *        subtree, see {@link #compute(Value, Value, Set, ValueEquivalence, Function)}
     * @param knownValueMap the memoized results of levels, by identity of the current value
     * @param descendOrdinal the position of the current value among the children of its parent, or {@code -1} for the
     *        root of the query value
     * @param matchers the matchers of the ancestors, which the level descends to its position
     * @param maxDepthBound the bound of the max depth, see above, or {@link Integer#MAX_VALUE} if there is no bound
     * @param expandedValues the values whose expansion is in progress, which the level must not expand again
     * @return the best variant of the current value with its {@link MatchResult}, and the witnesses of the current
     *         value for its targets
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static BestMatches recurseQueryResultValue(@Nonnull final Value currentQueryValue,
                                                       @Nonnull final Value candidateValue,
                                                       @Nonnull final Set<CorrelationIdentifier> rangedOverAliases,
                                                       @Nonnull final ValueEquivalence valueEquivalence,
                                                       @Nonnull final Function<Value, Optional<Value>> unmatchedHandlerFunction,
                                                       @Nonnull final IdentityHashMap<Value, BestMatches> knownValueMap,
                                                       final int descendOrdinal,
                                                       @Nonnull final Deque<IncrementalValueMatcher> matchers,
                                                       final int maxDepthBound,
                                                       @Nonnull final Set<Value> expandedValues) {
        if (maxDepthBound == 0) {
            return BestMatches.notMatched(currentQueryValue);
        }

        //
        // Descend the matchers of the ancestors to the position of the current value. anyParentsMatching tells
        // whether an ancestor has a potential match at all. In that case, goal 2 applies: the level must find
        // witnesses, and it must not prune the expansions of the current value.
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
        // The candidate nodes in the descended matchers are the targets of the current value (goal 2). An ancestor can
        // match its potential match as a whole only if the current value has a witness for the target at this
        // position.
        //
        final var targetValues =
                anyParentsMatching ? matchingCandidateValues(localMatchers) : ImmutableList.<Value>of();

        //
        // Create the matcher of the current value. Its potential matches are the own potential matches of the current
        // value. The matchers of the children descend from it, so these potential matches become targets of the
        // children.
        //
        final var currentMatcher =
                IncrementalValueMatcher.initial(currentQueryValue, candidateValue);
        localMatchers.push(currentMatcher);
        final var isCurrentMatching = currentMatcher.anyMatches();

        //
        // Keep the best variant (goal 1) and the witnesses for the targets (goal 2).
        //
        final var bestMatches = new BestMatches(currentQueryValue);

        final var children = ImmutableList.copyOf(currentQueryValue.getChildren());
        if (children.isEmpty()) {
            //
            // A leaf matches a reachable candidate subtree as a whole, or it does not match. It is its own witness for
            // each target without children, because the matcher compared the leaf with the target already.
            //
            final var resultForCurrent =
                    computeForCurrent(maxDepthBound, currentQueryValue, candidateValue, rangedOverAliases,
                            valueEquivalence, unmatchedHandlerFunction, ImmutableList.of());
            bestMatches.put(currentQueryValue, resultForCurrent);
            putWitnesses(bestMatches, currentQueryValue, children, ImmutableList.of(), targetValues,
                    valueEquivalence);
        } else {
            final ConstrainedBoolean isFound;
            if (!anyParentsMatching && isCurrentMatching) {
                //
                // No ancestor has a potential match, but the current value has own potential matches. Try to match
                // the current value as a whole. A full match has a max depth of 0, so it is the best variant, and no
                // ancestor needs a witness. In that case, the level does not recurse into the children.
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
                // Compute the max depth bound of the children. If a matcher has a potential match, a child can be part
                // of a full match further up, so there is no bound.
                //
                final int childrenMaxDepthBound =
                        (maxDepthBound == Integer.MAX_VALUE ||
                                 localMatchers.stream().anyMatch(IncrementalValueMatcher::anyMatches))
                        ? Integer.MAX_VALUE
                        : maxDepthBound - 1;

                //
                // Recurse into the children of the current value. The result of each child is its best variant and its
                // witnesses.
                //
                final var childrenBestMatchesBuilder = ImmutableList.<BestMatches>builder();
                int i = 0;
                for (final var child : children) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("recursing into child max_depth_bound={}, value={}", childrenMaxDepthBound, child);
                    }
                    childrenBestMatchesBuilder.add(
                            recurseQueryResultValue(child, candidateValue, rangedOverAliases, valueEquivalence,
                                    unmatchedHandlerFunction, knownValueMap, i, localMatchers, childrenMaxDepthBound,
                                    expandedValues));
                    i++;
                }
                final var childrenBestMatches = childrenBestMatchesBuilder.build();

                //
                // Goal 1: combine the best variants of the children. The max depth of a combination that does not
                // match as a whole is the largest max depth of its children plus one. So no other combination that
                // does not match as a whole has a smaller max depth.
                //
                boolean areAllChildrenSame = true;
                final var childrenValuesBuilder = ImmutableList.<Value>builder();
                final var childrenResultEntriesBuilder = ImmutableList.<Map.Entry<Value, MatchResult>>builder();
                for (i = 0; i < children.size(); i++) {
                    final var childBestMatches = childrenBestMatches.get(i);
                    final var key = childBestMatches.getValue();
                    if (children.get(i) != key) {
                        areAllChildrenSame = false;
                    }
                    childrenValuesBuilder.add(key);
                    childrenResultEntriesBuilder.add(Maps.immutableEntry(key, childBestMatches.getMatchResult()));
                }

                final Value resultQueryValue;
                if (!areAllChildrenSame) {
                    resultQueryValue = currentQueryValue.withChildren(childrenValuesBuilder.build());
                } else {
                    resultQueryValue = currentQueryValue;
                }

                final var resultForCurrent =
                        computeForCurrent(maxDepthBound, resultQueryValue, candidateValue, rangedOverAliases,
                                valueEquivalence, unmatchedHandlerFunction, childrenResultEntriesBuilder.build());
                bestMatches.put(resultQueryValue, resultForCurrent);

                //
                // A full match of an own potential match can need variants of the children other than their best
                // variants. For each own potential match, combine the witnesses of the children for the children of the
                // potential match. The combination of the best variants can be a full match already. Then no other
                // full match can be better, and this step is not necessary.
                //
                if (bestMatches.getCurrentMaxDepth() > 0) {
                    for (final var matchingCandidateValuePair : currentMatcher.getMatchingCandidateValues()) {
                        final var matchingCandidateValue = matchingCandidateValuePair.getLeft();
                        final var witness =
                                witnessMaybe(currentQueryValue, children, childrenBestMatches, matchingCandidateValue,
                                        valueEquivalence);
                        if (witness != null) {
                            final var witnessValue = witness.getLeft();
                            bestMatches.put(witnessValue, MatchResult.of(ImmutableMap.of(witnessValue,
                                    matchingCandidateValue), 0, witness.getRight()));
                        }
                    }
                }

                //
                // Goal 2: for each target, combine the witnesses of the children for the children of the target. The
                // result is the witness of the current value for the target.
                //
                putWitnesses(bestMatches, currentQueryValue, children, childrenBestMatches, targetValues,
                        valueEquivalence);
            }
        }

        Verify.verify(!bestMatches.isEmpty());
        Verify.verify(bestMatches.getCurrentMaxDepth() >= 0);

        //
        // Recurse into the expansions of the current value. An expansion can have a better best variant (goal 1), or
        // witnesses for targets that the current value has no witness for (goal 2). Expand only if an ancestor has a
        // potential match (goal 2) or if no full match exists yet (goal 1). Do not expand a value whose expansion is in
        // progress further up.
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
                    // an expansion of this rule set has no constraints
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

                    final var expandedBestMatches =
                            recurseQueryResultValue(expandedCurrentQueryValue, candidateValue,
                                    rangedOverAliases, valueEquivalence, unmatchedHandlerFunction, knownValueMap,
                                    descendOrdinal, matchers, currentMaxDepthBound, expandedValues);
                    bestMatches.put(expandedBestMatches.getValue(), expandedBestMatches.getMatchResult());
                    bestMatches.putWitnesses(expandedBestMatches);
                }
            } finally {
                expandedValues.remove(currentQueryValue);
            }
        }

        Verify.verify(bestMatches.getCurrentMaxDepth() == Integer.MAX_VALUE ||
                bestMatches.getCurrentMaxDepth() <= maxDepthBound);

        // memoize the result if it depends only on the current value
        if (!anyParentsMatching && !expandedValues.contains(currentQueryValue)) {
            if (logger.isTraceEnabled()) {
                logger.trace("memoizing value={}", currentQueryValue);
            }
            knownValueMap.put(currentQueryValue, bestMatches);
        }
        return bestMatches;
    }

    /**
     * Returns the candidate nodes of the given matchers. Each node occurs once, by identity, in the order of the
     * matchers. For the descended matchers of a level, these candidate nodes are the targets of the level.
     *
     * @param matchers the matchers
     * @return the candidate nodes of the matchers
     */
    @Nonnull
    private static List<Value> matchingCandidateValues(@Nonnull final Iterable<IncrementalValueMatcher> matchers) {
        final Set<Value> seenCandidateValues = Sets.newIdentityHashSet();
        final var matchingCandidateValuesBuilder = ImmutableList.<Value>builder();
        for (final var matcher : matchers) {
            for (final var matchingCandidateValuePair : matcher.getMatchingCandidateValues()) {
                final var matchingCandidateValue = matchingCandidateValuePair.getLeft();
                if (seenCandidateValues.add(matchingCandidateValue)) {
                    matchingCandidateValuesBuilder.add(matchingCandidateValue);
                }
            }
        }
        return matchingCandidateValuesBuilder.build();
    }

    /**
     * Builds the witnesses of the current value for its targets and adds them to {@code bestMatches}, see
     * {@link #witnessMaybe}. A target that has a witness already keeps it.
     *
     * @param bestMatches the result of the level, which receives the witnesses
     * @param currentQueryValue the current value
     * @param children the children of the current value
     * @param childrenBestMatches the results of the levels of the children
     * @param targetValues the targets of the current value
     * @param valueEquivalence the value equivalence of the computation
     */
    private static void putWitnesses(@Nonnull final BestMatches bestMatches,
                                     @Nonnull final Value currentQueryValue,
                                     @Nonnull final List<? extends Value> children,
                                     @Nonnull final List<BestMatches> childrenBestMatches,
                                     @Nonnull final List<Value> targetValues,
                                     @Nonnull final ValueEquivalence valueEquivalence) {
        for (final var targetValue : targetValues) {
            if (bestMatches.getWitness(targetValue) == null) {
                final var witness =
                        witnessMaybe(currentQueryValue, children, childrenBestMatches, targetValue, valueEquivalence);
                if (witness != null) {
                    bestMatches.putWitness(targetValue, witness.getLeft(), witness.getRight());
                }
            }
        }
    }

    /**
     * Builds the witness of the current value for a candidate node. The witness is a variant of the current value that
     * is equal to the candidate node, together with the constraint of that equality. The method builds it as follows:
     * <ul>
     *     <li>The root of the witness is the root of the current value. It must be {@link Value#equalsWithoutChildren}
     *         equal to the candidate node, and the candidate node must have as many children as the current
     *         value.</li>
     *     <li>The child at position {@code i} of the witness is the witness of child {@code i} of the current value for
     *         child {@code i} of the candidate node. If child {@code i} has no such witness, but the value equivalence
     *         defines child {@code i} as equal to child {@code i} of the candidate node, the child itself is the child
     *         of the witness. The equivalence covers the whole subtree of that child.</li>
     *     <li>If each child of the witness is the original child, the witness is the current value itself.</li>
     * </ul>
     * So {@link Value#semanticEquals} finds the witness equal to the candidate node. But the method does not walk the
     * two trees: it compares only the roots, and the witnesses of the children cover the nodes below. If a child has
     * neither a witness nor an equivalence, the recursion found no variant of that child that is equal to the child of
     * the candidate node. Then the current value has no witness for the candidate node.
     *
     * @param currentQueryValue the current value
     * @param children the children of the current value
     * @param childrenBestMatches the results of the levels of the children
     * @param candidateValue a candidate node that potentially matches the current value
     * @param valueEquivalence the value equivalence of the computation
     * @return the witness and the constraint of the equality, or {@code null} if there is no witness
     */
    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static NonnullPair<Value, QueryPlanConstraint> witnessMaybe(@Nonnull final Value currentQueryValue,
                                                                        @Nonnull final List<? extends Value> children,
                                                                        @Nonnull final List<BestMatches> childrenBestMatches,
                                                                        @Nonnull final Value candidateValue,
                                                                        @Nonnull final ValueEquivalence valueEquivalence) {
        final var equalsWithoutChildren = currentQueryValue.equalsWithoutChildren(candidateValue);
        if (equalsWithoutChildren.isFalse()) {
            return null;
        }
        final var candidateChildren = ImmutableList.copyOf(candidateValue.getChildren());
        if (candidateChildren.size() != children.size()) {
            return null;
        }

        var queryPlanConstraint = equalsWithoutChildren.getConstraint();
        boolean areAllChildrenSame = true;
        final var witnessChildrenBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < children.size(); i++) {
            final var child = children.get(i);
            final var candidateChild = candidateChildren.get(i);
            final var childWitness = childrenBestMatches.get(i).getWitness(candidateChild);
            final Value witnessChild;
            if (childWitness != null) {
                // the level of the child found a variant that is equal to the child of the candidate node
                witnessChild = childWitness.getLeft();
                queryPlanConstraint = queryPlanConstraint.compose(childWitness.getRight());
            } else {
                // the matchers do not use the value equivalence, so ask it directly
                final var isDefinedEqual = valueEquivalence.isDefinedEqual(child, candidateChild);
                if (isDefinedEqual.isFalse()) {
                    return null;
                }
                witnessChild = child;
                queryPlanConstraint = queryPlanConstraint.compose(isDefinedEqual.getConstraint());
            }
            if (witnessChild != child) {
                areAllChildrenSame = false;
            }
            witnessChildrenBuilder.add(witnessChild);
        }

        //
        // The witness value is the variant of the current value that is equal to the candidate node. Do not create a
        // new value if no child changed.
        //
        final var witnessValue =
                areAllChildrenSame ? currentQueryValue : currentQueryValue.withChildren(witnessChildrenBuilder.build());
        return NonnullPair.of(witnessValue, queryPlanConstraint);
    }

    /**
     * Computes the {@link MatchResult} of one variant of the current value from the results of its children. The
     * rules are:
     * <ul>
     *     <li>If the variant is equal to a reachable candidate subtree, or if the unmatched handler gives a value for
     *         it, the result is that single match. Its max depth is 0.</li>
     *     <li>Otherwise, the result is the matches of the children, which cover the variant as a partial match. Its max
     *         depth is the largest max depth of the children plus one.</li>
     *     <li>A child without matches is acceptable if it is constant, because a constant needs no match. It counts as
     *         a child with a max depth of 0. A child without matches that is not constant leaves a part of the variant
     *         uncovered, so the result is {@link MatchResult#notMatched()}.</li>
     *     <li>A variant without children that does not match has a max depth of {@link Integer#MAX_VALUE}: it has no
     *         match.</li>
     *     <li>If the max depth is larger than {@code maxDepthBound}, the result is
     *         {@link MatchResult#notMatched()}.</li>
     * </ul>
     *
     * @param maxDepthBound the bound of the max depth, see {@link #recurseQueryResultValue}
     * @param resultQueryValue the variant of the current value
     * @param candidateValue the candidate value
     * @param rangedOverAliases the aliases of the quantifiers that the candidate ranges over, which are not constant
     * @param valueEquivalence the value equivalence of the computation
     * @param unmatchedHandlerFunction the function to call if the variant is not equal to any reachable candidate
     *        subtree
     * @param childrenResultEntries the children of the variant, each with its {@link MatchResult}
     * @return the result of the variant: a full match with a max depth of 0, a partial match with the matches of the
     *         children, or {@link MatchResult#notMatched()}
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
                // a constant child needs no match: its result is the same for each record of the candidate
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

    /**
     * Finds a reachable candidate subtree that is semantically equal to a query value. The rules are:
     * <ul>
     *     <li>The method walks the candidate value in pre-order and descends into record constructors only. So it
     *         visits exactly the reachable candidate subtrees. It returns the first one that is equal to the query
     *         value.</li>
     *     <li>A {@link QuantifiedRecordValue} stands for the whole record of its quantifier. The method treats it as
     *         equal to the root of the candidate value, without a comparison.</li>
     *     <li>If no reachable candidate subtree is equal to the query value, the method calls the unmatched handler. If
     *         the handler returns a value, the query value matches that value.</li>
     * </ul>
     *
     * @param currentQueryValue the query value, a variant of the current value of a level
     * @param candidateValue the candidate value
     * @param valueEquivalence the value equivalence of the computation
     * @param unmatchedHandlerFunction the function to call if no reachable candidate subtree is equal to the query
     *        value
     * @return the result of the comparison together with the matching value, or false together with {@code null} if
     *         there is no match
     */
    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Pair<ConstrainedBoolean, Value> findMatchingReachableCandidateValue(@Nonnull final Value currentQueryValue,
                                                                                       @Nonnull final Value candidateValue,
                                                                                       @Nonnull final ValueEquivalence valueEquivalence,
                                                                                          @Nonnull final Function<Value, Optional<Value>> unmatchedHandlerFunction) {
        for (final var currentCandidateValue : candidateValue
                // Walk the candidate value in pre-order, and descend only into the values whose children are
                // reachable: the children of a record constructor are reachable, but the children of other values,
                // for example of an arithmetic value, are not. The pre-order is important: it visits a candidate
                // subtree before the subtrees inside it, so the method finds the largest candidate subtree that
                // matches.
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
     * The result of one level of {@link #recurseQueryResultValue}. It has one part for each goal of the recursion:
     * <ul>
     *     <li>Goal 1: the best variant of the current value, that is the variant with the smallest max depth, together
     *         with its {@link MatchResult}. A new variant replaces the best variant only if its max depth is smaller.
     *         So of several variants with the same max depth, the first one stays.</li>
     *     <li>Goal 2: the witnesses of the current value for its targets. A target has at most one witness, and the
     *         first one stays. All witnesses for a target are equal to it, so any one of them is correct for the
     *         parent.</li>
     * </ul>
     */
    private static class BestMatches {
        @Nonnull
        private final Value currentQueryValue;
        private int currentMaxDepth = -1;
        @Nullable
        private Value value;
        @Nullable
        private MatchResult matchResult;
        /**
         * The witnesses, from a target, by identity, to the witness value and the constraint of the equality. The map
         * is created when the first witness is added. A level without targets does not need it.
         */
        @Nullable
        private Map<Value, NonnullPair<Value, QueryPlanConstraint>> witnessesMap;

        public BestMatches(@Nonnull final Value currentQueryValue) {
            this.currentQueryValue = currentQueryValue;
        }

        /**
         * Returns whether no variant has been added yet.
         *
         * @return {@code true} if no variant has been added yet
         */
        public boolean isEmpty() {
            return getCurrentMaxDepth() == -1;
        }

        /**
         * Returns the max depth of the best variant.
         *
         * @return the max depth of the best variant, or {@code -1} if no variant has been added yet
         */
        public int getCurrentMaxDepth() {
            return currentMaxDepth;
        }

        /**
         * Returns the best variant.
         *
         * @return the best variant, or the current value if no variant has been added yet
         */
        @Nonnull
        public Value getValue() {
            return value == null ? currentQueryValue : value;
        }

        /**
         * Returns the result of the best variant.
         *
         * @return the result of the best variant, or {@link MatchResult#notMatched()} if no variant has been added yet
         */
        @Nonnull
        public MatchResult getMatchResult() {
            return matchResult == null ? MatchResult.notMatched() : matchResult;
        }

        /**
         * Adds a variant with its result. The variant becomes the best variant if no variant has been added yet, or if
         * its max depth is smaller than the max depth of the best variant.
         *
         * @param newQueryValue the variant
         * @param newMatchResult the result of the variant
         */
        public void put(@Nonnull final Value newQueryValue,
                        @Nonnull final MatchResult newMatchResult) {
            if (matchResult == null || newMatchResult.getMaxDepth() < currentMaxDepth) {
                this.value = newQueryValue;
                this.matchResult = newMatchResult;
                this.currentMaxDepth = newMatchResult.getMaxDepth();
            }
        }

        /**
         * Returns the witness for a target.
         *
         * @param targetValue the target, compared by identity
         * @return the witness value and the constraint of the equality, or {@code null} if the target has no witness
         */
        @Nullable
        public NonnullPair<Value, QueryPlanConstraint> getWitness(@Nonnull final Value targetValue) {
            return witnessesMap == null ? null : witnessesMap.get(targetValue);
        }

        /**
         * Adds a witness for a target, if the target has no witness yet.
         *
         * @param targetValue the target, compared by identity
         * @param witnessValue the variant of the current value that is equal to the target
         * @param queryPlanConstraint the constraint of the equality
         */
        public void putWitness(@Nonnull final Value targetValue,
                               @Nonnull final Value witnessValue,
                               @Nonnull final QueryPlanConstraint queryPlanConstraint) {
            if (witnessesMap == null) {
                witnessesMap = new IdentityHashMap<>();
            }
            witnessesMap.putIfAbsent(targetValue, NonnullPair.of(witnessValue, queryPlanConstraint));
        }

        /**
         * Adds the witnesses of another result for the targets that have no witness yet. The other result is the
         * result of an expansion at the same position, so its targets are candidate nodes at the same position.
         *
         * @param other the result of an expansion of the current value
         */
        public void putWitnesses(@Nonnull final BestMatches other) {
            if (other.witnessesMap != null) {
                for (final var witnessEntry : other.witnessesMap.entrySet()) {
                    final var witness = witnessEntry.getValue();
                    putWitness(witnessEntry.getKey(), witness.getLeft(), witness.getRight());
                }
            }
        }

        /**
         * Returns a result without a match, for a level that cannot add a match.
         *
         * @param currentQueryValue the current value
         * @return a result whose best variant is the current value, without a match, and without witnesses
         */
        @Nonnull
        public static BestMatches notMatched(@Nonnull final Value currentQueryValue) {
            final var bestMatches = new BestMatches(currentQueryValue);
            bestMatches.put(currentQueryValue, MatchResult.notMatched());
            return bestMatches;
        }
    }

    /**
     * Keeps the potential matches of a query node: the candidate nodes that can turn out to be equal to it.
     *
     * <p>{@link Value#semanticEquals} compares two trees from the top down. It compares the two roots with
     * {@link Value#equalsWithoutChildren}, and then it compares the children. A matcher does the same comparison one
     * node at a time, along the path that the recursion takes:
     * <ul>
     *     <li>The {@link #initial} matcher of a query node keeps the reachable candidate subtrees whose roots are
     *         {@link Value#equalsWithoutChildren} equal to the query node.</li>
     *     <li>When the recursion goes from a query node to one of its children, {@link #descend} takes the child at the
     *         same position of each potential match. It keeps that candidate node only if it is
     *         {@link Value#equalsWithoutChildren} equal to the child of the query node.</li>
     * </ul>
     * So each candidate node in a descended matcher agrees with the query value on the path from the query node of the
     * initial matcher to the current position. The nodes off that path are not compared yet, so a potential match can
     * turn out to be no match. The number of potential matches can only decrease along the path. When it is 0, it stays
     * 0.
     *
     * <p>The recursion uses the matchers for both of its goals, see {@link #recurseQueryResultValue}. Whether an
     * ancestor has a potential match tells a level if goal 2 applies. The candidate nodes of the descended matchers are
     * the targets of the level.
     *
     * <p>A matcher compares with {@link Value#equalsWithoutChildren} only. It does not use the value equivalence. So a
     * matcher does not find two values that are equal only by the equivalence, for example two quantified values whose
     * aliases an alias map relates.
     *
     * <p>In the examples, the symbol {@code ■} stands for a subtree that is not compared yet. Assume that {@code q.b}
     * and {@code q.c} have the same type, and that the columns of the record constructors have no field names. Then two
     * record constructors have the same fields if their columns have the same types.
     *
     * <p>Example 1: the query value {@code rcv(q.x, rcv(q.a, q.b))} and the candidate value
     * {@code rcv(q.x, rcv(q.a, q.c))}. At the root, the query node {@code rcv(■, ■)} potentially matches the candidate
     * root {@code rcv(q.x, rcv(q.a, q.c))}. The recursion descends into the second child of the query value. The
     * query node {@code rcv(■, rcv(■, ■))} still potentially matches {@code rcv(■, rcv(q.a, q.c))}. The recursion
     * descends into the first child of that node. {@code rcv(■, rcv(q.a, ■))} still potentially matches
     * {@code rcv(■, rcv(q.a, ■))}. If the recursion descends into the second child of that node instead, no potential
     * match is left: {@code rcv(■, rcv(■, q.b))} does not potentially match {@code rcv(■, rcv(■, q.c))}.
     *
     * <p>Example 2: the query value {@code rcv(q.a, q.b)} and the candidate value
     * {@code rcv(q.x, rcv(q.a, q.b), rcv(q.a, q.c))}. The query root does not potentially match the candidate root,
     * because the two roots have different fields. But the query root {@code rcv(■, ■)} potentially matches two
     * reachable candidate subtrees: {@code rcv(q.a, q.b)} and {@code rcv(q.a, q.c)}. The recursion descends into the
     * second child of the query value. Then only one potential match is left: {@code rcv(■, q.b)} still potentially
     * matches {@code rcv(■, q.b)}, but it does not potentially match {@code rcv(■, q.c)}.
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

        /**
         * Returns the query node of this matcher.
         *
         * @return the query node whose potential matches this matcher keeps
         */
        @Nonnull
        public Value getCurrentQueryValue() {
            return currentQueryValue;
        }

        /**
         * Returns the potential matches. The matcher computes them once, when this method is called first.
         *
         * @return the candidate nodes that potentially match the query node, each with the constraint of the equalities
         *         on the path so far
         */
        @Nonnull
        public List<NonnullPair<Value, QueryPlanConstraint>> getMatchingCandidateValues() {
            return matchingCandidateValuesSupplier.get();
        }

        /**
         * Computes the potential matches, see {@link #initial} and {@link #descend}.
         *
         * @return the candidate nodes that potentially match the query node, each with the constraint of the equalities
         *         on the path so far
         */
        @Nonnull
        public abstract List<NonnullPair<Value, QueryPlanConstraint>> computeMatchingCandidateValues();

        /**
         * Explains the path of this matcher for debugging. The explanation shows the query node of the initial matcher.
         * The subtrees off the path show as {@code ■}, and the children of the query node of this matcher show the
         * given explanations.
         *
         * @param explainSuppliers the explanations of the children of the query node of this matcher
         * @return the explanation
         */
        @Nonnull
        public abstract ExplainTokensWithPrecedence explain(@Nonnull Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers);

        /**
         * Returns whether the query node has a potential match.
         *
         * @return {@code true} if there is at least one potential match
         */
        public boolean anyMatches() {
            return !getMatchingCandidateValues().isEmpty();
        }

        /**
         * Returns the path of this matcher for debugging. The symbol {@code ■} stands for a subtree that is not
         * compared, and {@code → ∅} marks a matcher without potential matches.
         *
         * @return the path of this matcher
         */
        @Override
        public String toString() {
            final var explainTokens =
                    explain(Collections.nCopies(Iterables.size(currentQueryValue.getChildren()),
                            () -> UNMATCHED)).getExplainTokens();
            final var explainString = explainTokens.render(DefaultExplainFormatter.forDebugging()).toString();
            return anyMatches() ? explainString : explainString + " → ∅";
        }

        /**
         * Returns the matcher for a child of the query node. For each potential match of this matcher, the new matcher
         * takes the child at the same position. It keeps that candidate node only if it is
         * {@link Value#equalsWithoutChildren} equal to the child of the query node. The candidate node does not have to
         * be reachable.
         *
         * @param currentQueryValue the child of the query node, or a variant of that child
         * @param descendOrdinal the position of the child among the children of the query node
         * @return the matcher for the child
         */
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

        /**
         * Returns the matcher of a query node at the start of a path, with the own potential matches of the query node.
         * These are the reachable candidate subtrees whose roots are {@link Value#equalsWithoutChildren} equal to the
         * query node, in the pre-order of the candidate value. A {@link QuantifiedRecordValue} potentially matches the
         * root of the candidate value, as in {@code findMatchingReachableCandidateValue}.
         *
         * @param queryRootValue the query node
         * @param candidateRootValue the candidate value (root)
         * @return the matcher of the query node
         */
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
     * The result of the matching of one variant of a query subtree: the matches that cover the variant, their max
     * depth, and the constraint that the matches depend on. A max depth of {@link Integer#MAX_VALUE} means that the
     * matches do not cover the variant, see {@link #notMatched()}.
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

        /**
         * Returns the matches.
         *
         * @return the matches, from subtrees of the variant to reachable candidate subtrees or to values of the
         *         unmatched handler
         */
        @Nonnull
        public Map<Value, Value> getValueMap() {
            return valueMap;
        }

        /**
         * Returns the max depth of the matches.
         *
         * @return the max depth, or {@link Integer#MAX_VALUE} if the matches do not cover the variant
         */
        public int getMaxDepth() {
            return maxDepth;
        }

        /**
         * Returns whether the matches cover the variant.
         *
         * @return {@code true} if the max depth is smaller than {@link Integer#MAX_VALUE}
         */
        public boolean isMatched() {
            return maxDepth < Integer.MAX_VALUE;
        }

        /**
         * Returns the constraint that the matches depend on.
         *
         * @return the composed constraint of all matches
         */
        @Nonnull
        public QueryPlanConstraint getQueryPlanConstraint() {
            return queryPlanConstraint;
        }

        /**
         * Creates a result.
         *
         * @param valueMap the matches
         * @param maxDepth the max depth of the matches, or {@link Integer#MAX_VALUE} if they do not cover the variant
         * @param queryPlanConstraint the constraint that the matches depend on
         * @return the result
         */
        public static MatchResult of(@Nonnull final Map<Value, Value> valueMap,
                                     final int maxDepth,
                                     @Nonnull final QueryPlanConstraint queryPlanConstraint) {
            Verify.verify(maxDepth >= 0);
            return new MatchResult(valueMap, maxDepth, queryPlanConstraint);
        }

        /**
         * Returns the result of a variant that the matches do not cover.
         *
         * @return a result without matches and with a max depth of {@link Integer#MAX_VALUE}
         */
        public static MatchResult notMatched() {
            return NOT_MATCHED;
        }
    }
}
