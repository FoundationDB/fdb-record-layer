/*
 * MatchIntermediateRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.IterableHelpers;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier.Existential;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.BoundMatch;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;

/**
 * Expression-based transformation rule that matches any non-leaf expression (called an intermediate expression)
 * to a candidate expression in a {@link MatchCandidate}.
 * It yields matches of type {@link PartialMatch}. This rule further seeds the memoization
 * structure for partial matches that is kept as part of {@link Reference}. It prepares further rules such as
 * other applications of {@link MatchIntermediateRule} and {@link AdjustMatchRule}.
 *
 * <p>
 * As an intermediate expression has children (and the candidate expression also has at least one child) we
 * need to match up the quantifiers of the query expression to the quantifiers of the possible candidate expression
 * in order to determine if the query expression is in fact subsumed by the candidate. The property of subsumption
 * is defined as the ability of replacing the query expression with the candidate expression with the additional
 * application of compensation. Equivalence between expressions is stronger than subsumption in a way that if two
 * expressions are semantically equal, the compensation is considered to be a no op.
 * </p>
 *
 * <h2>Example 1</h2>
 *
 * <pre>
 * {@code
 * query expression                    candidate expression
 *
 *      +-----------+                       +-----------+
 *      |   Union   |                       |   Union   |
 *      +-----------+                       +-----------+
 *      /     |     \                       /     |     \
 *     /      |      \                     /      |      \
 * +----+  +----+  +----+              +----+  +----+  +----+
 * | c1 |  | c2 |  | c3 |              | ca |  | cb |  | cc |
 * +----+  +----+  +----+              +----+  +----+  +----+
 * }
 * </pre>
 *
 * <p>
 * The matching logic between these two expressions needs to first establish if there is a mapping from
 * {@code c1, c2, c3} to {@code c1, cb, cc} such that the query expression can be subsumed by the candidate expression.
 * In the example, we use union expressions that only define subsumption through equivalency. In other words,
 * is there a mapping between the sets of quantifiers the expressions range over such that query expression and candidate
 * expression are equal? It may be that no such mapping exist in which case subsumption cannot be established and this
 * rule does not yield matches. It can also be that there are multiple such matches in which case we yield more than
 * one partial match back to the planner. The expected "successful" outcome would be for this rule to yield exactly one
 * match.
 * </p>
 *
 * <p>
 * The described problem is referred to as exact matching. To make matters more complicated, it can be that the query
 * expression can be subsumed by the candidate expression even though the sets of quantifiers do not have the same
 * cardinality. This is referred to as non-exact matching.
 * </p>
 *
 * <h2>Example 2</h2>
 *
 * <pre>
 * {@code
 * query expression                    candidate expression
 *
 *     +-----------+                     +-----------+
 *     |   Select  |                     |   Select  |
 *     +-----------+                     +-----------+
 *      /        \ exists                /     |     \
 *     /          \                     /      |      \
 * +----+      +----+              +----+  +----+  +----+
 * | c1 |      | c2 |              | ca |  | cb |  | cc |
 * +----+      +----+              +----+  +----+  +----+
 * }
 * </pre>
 *
 * <p>
 * For simplicity let us further assume the subtrees underneath {@code c1} and {@code ca} as well as
 * the subtrees underneath {@code c2} and {@code cb} are already determined to be semantically equivalent.
 * In this example we yield two partial matches, one for {@code c1 -> ca} and one for {@code c1 -> ca, c2 -> cb}.
 * That is because the query expression can be replaced by the candidate expression in both cases (albeit with
 * different compensations).
 * </p>
 *
 * <p>
 * Discussion as to why they are matching: for further explanations see {@link RelationalExpression#subsumedBy}. First note
 * that the quantifier over {@code c2} is existential (of type {@link Existential}. That also means that this quantifier
 * does not ever positively contribute to the cardinality of the select expression. It only filters out the outer if
 * the inner does not produce any records. In some sense it is very similar to a predicate. In fact it is a predicate
 * defined on a sub query. Now if {@code ca} is known to subsume {@code c1} we can replace the query expression with
 * the candidate expression if we also reapply the existential predicate (first match case). For the match that also
 * maps {@code c2} to {@code cb} we also know that the existential predicate over {@code c2} is filtering out a record
 * when {@code cb} does not produce any records. On the flip side if there is more than one record being produced by the
 * sub query on the query side the quantifier over {@code cb} produces multiple records that now do contribute to the
 * cardinality on the candidate side. That will need to be corrected by distinct-ing the output of the select expression
 * if the match is utilized later on.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class MatchIntermediateRule extends CascadesRule<RelationalExpression> {
    private static final BindingMatcher<Quantifier> quantifierMatcher = QuantifierMatchers.anyQuantifier();
    private static final BindingMatcher<RelationalExpression> root =
            RelationalExpressionMatchers.ofTypeOwning(RelationalExpression.class, all(quantifierMatcher));

    public MatchIntermediateRule() {
        super(root);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final RelationalExpression expression = bindings.get(root);
        final List<? extends Quantifier> quantifiers = bindings.getAll(quantifierMatcher);
        final ImmutableList<? extends Reference> rangesOverRefs =
                quantifiers.stream()
                        .map(Quantifier::getRangesOver)
                        .collect(ImmutableList.toImmutableList());

        // form union of all possible match candidates that this rule application should look at
        final Set<MatchCandidate> childMatchCandidates = new LinkedIdentitySet<>();
        for (int i = 0; i < rangesOverRefs.size(); i++) {
            final Reference rangesOverGroup = rangesOverRefs.get(i);
            childMatchCandidates.addAll(rangesOverGroup.getMatchCandidates());
        }

        // go through all match candidates
        for (final MatchCandidate matchCandidate : childMatchCandidates) {
            final SetMultimap<Reference, RelationalExpression> refToExpressionMap =
                    matchCandidate.findReferencingExpressions(rangesOverRefs);

            // go through all reference paths, i.e., (ref, expression) pairs
            for (final Map.Entry<Reference, RelationalExpression> entry : refToExpressionMap.entries()) {
                final Reference candidateReference = entry.getKey();
                final RelationalExpression candidateExpression = entry.getValue();
                // match this expression with the candidate expression and yield zero to n new partial matches
                final Iterable<BoundMatch<MatchInfo>> boundMatchInfos =
                        matchWithCandidate(expression,
                                matchCandidate,
                                candidateExpression,
                                call.getEvaluationContext());
                boundMatchInfos.forEach(boundMatchInfo ->
                        call.yieldPartialMatch(boundMatchInfo.getAliasMap(),
                                matchCandidate,
                                expression,
                                candidateReference,
                                boundMatchInfo.getMatchResult()));
            }
        }
    }

    /**
     * Method to match an expression with a candidate expression.
     * @param expression an expression
     * @param matchCandidate a match candidate to match against
     * @param candidateExpression a candidate expression which must be part of {@code matchCandidate}
     * @return an {@link Iterable} of bound {@link MatchInfo}s where each match info represents a math under the bound
     *         mappings (between query expression and candidate expression).
     */
    @Nonnull
    private Iterable<BoundMatch<MatchInfo>> matchWithCandidate(@Nonnull RelationalExpression expression,
                                                               @Nonnull MatchCandidate matchCandidate,
                                                               @Nonnull RelationalExpression candidateExpression,
                                                               @Nonnull final EvaluationContext context) {
        Verify.verify(!expression.getQuantifiers().isEmpty());
        Verify.verify(!candidateExpression.getQuantifiers().isEmpty());

        return expression.match(candidateExpression,
                AliasMap.emptyMap(),
                expression.getQuantifiers(),
                candidateExpression.getQuantifiers(),
                quantifier -> constraintsForQuantifier(matchCandidate, quantifier),
                (quantifier, otherQuantifier, aliasMap) -> matchQuantifiers(matchCandidate, quantifier, otherQuantifier, aliasMap),
                ((boundCorrelatedToMap, boundMatches) -> combineMatches(expression, candidateExpression, boundCorrelatedToMap, boundMatches, context)));

    }

    /**
     * Constraints lambda for {@link RelationalExpression#match}. We only consider quantifiers that range over references
     * that associated {@link PartialMatch}es between the query and the given match candidate.
     *
     * @param matchCandidate a match candidate
     * @param quantifier a quantifier of the owned by the query expression
     * @return a {@link Collection} of {@link AliasMap}s containing possible mappings for this quantifier
     */
    @Nonnull
    private Collection<AliasMap> constraintsForQuantifier(@Nonnull final MatchCandidate matchCandidate,
                                                          @Nonnull final Quantifier quantifier) {
        final Set<PartialMatch> partialMatchesForCandidate = quantifier.getRangesOver().getPartialMatchesForCandidate(matchCandidate);
        if (partialMatchesForCandidate.isEmpty()) {
            return ImmutableList.of(AliasMap.emptyMap());
        }
        return partialMatchesForCandidate.stream()
                .map(PartialMatch::getBoundAliasMap)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Match lambda for {@link RelationalExpression#match}.
     *
     * @param matchCandidate a match candidate
     * @param quantifier a quantifier of the owned by the query expression
     * @param candidateQuantifier a quantifier of the owned by the candidate expression
     * @return an {@link Iterable} of {@link PartialMatchWithQuantifier}s containing matches that can be
     *         pulled up by {@code quantifier}
     */
    @Nonnull
    private Iterable<PartialMatchWithQuantifier> matchQuantifiers(@Nonnull final MatchCandidate matchCandidate,
                                                                  @Nonnull final Quantifier quantifier,
                                                                  @Nonnull final Quantifier candidateQuantifier,
                                                                  @Nonnull final AliasMap aliasMap) {
        final Reference rangesOver = quantifier.getRangesOver();
        final Reference otherRangesOver = candidateQuantifier.getRangesOver();

        final Set<PartialMatch> partialMatchesForCandidate = rangesOver.getPartialMatchesForCandidate(matchCandidate);
        return partialMatchesForCandidate.stream()
                .filter(partialMatch -> partialMatch.getCandidateRef() == otherRangesOver && partialMatch.getBoundAliasMap().isCompatible(aliasMap))
                .map(partialMatch -> PartialMatchWithQuantifier.of(partialMatch, quantifier))
                .collect(Collectors.toList());
    }

    /**
     * Combine lambda for {@link RelationalExpression#match}. This method calls the subsumption logic.
     *
     * @param expression a query expression
     * @param candidateExpression an expression to be matched used by a match candidate
     * @param boundCorrelatedToMap an alias map containing mappings only for correlated aliases
     * @param boundMatches an {@link Iterable} of matches that forms the conceptual cross-product of all partial
     *        matches for children references (compatible with the given alias map) that have been pulled up by
     *        {@link #matchQuantifiers}.
     * @return an {@link Iterable} of bound {@link MatchInfo}s that can be used to yield new partial matches between
     *         {@code expression} and {@code candidateExpression}.
     */
    @Nonnull
    private Iterable<BoundMatch<MatchInfo>> combineMatches(@Nonnull RelationalExpression expression,
                                                           @Nonnull RelationalExpression candidateExpression,
                                                           @Nonnull final AliasMap boundCorrelatedToMap,
                                                           @Nonnull final Iterable<BoundMatch<EnumeratingIterable<PartialMatchWithQuantifier>>> boundMatches,
                                                           @Nonnull final EvaluationContext context) {
        return () ->
                StreamSupport.stream(boundMatches.spliterator(), false)
                        .flatMap(boundMatch ->
                                boundMatch.getMatchResultOptional()
                                        .map(matchResultIterable ->
                                                IterableHelpers.flatMap(matchResultIterable, matchResult -> {
                                                    final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap = partialMatchMap(matchResult);
                                                    return expression.subsumedBy(candidateExpression, boundMatch.getAliasMap(), partialMatchMap, context);
                                                }))
                                        .map(matchesWithCompensation -> StreamSupport.stream(matchesWithCompensation.spliterator(), false))
                                        .orElseGet(Stream::empty))
                        .map(matchInfo -> BoundMatch.withAliasMapAndMatchResult(boundCorrelatedToMap, matchInfo))
                        .iterator();
    }

    /**
     * Internal helper to create a map based on an {@link Iterable} of {@link PartialMatchWithQuantifier}s that is indexed
     * by quantifiers (by identity).
     * @param partialMatchWithQuantifiers the iterable to be converted
     * @return a map mapping from identity(quantifier) to {@link PartialMatch}
     */
    @Nonnull
    private IdentityBiMap<Quantifier, PartialMatch> partialMatchMap(final Iterable<PartialMatchWithQuantifier> partialMatchWithQuantifiers) {
        return StreamSupport.stream(partialMatchWithQuantifiers.spliterator(), false)
                .collect(IdentityBiMap.toImmutableIdentityBiMap(PartialMatchWithQuantifier::getQuantifier,
                        PartialMatchWithQuantifier::getPartialMatch,
                        (v1, v2) -> {
                            throw new RecordCoreException("matching produced duplicate quantifiers");
                        }));
    }

    /**
     * Partial match with a quantifier pulled up along with the partial match during matching.
     */
    public static class PartialMatchWithQuantifier {
        @Nonnull
        private final PartialMatch partialMatch;
        @Nonnull
        private final Quantifier quantifier;

        private PartialMatchWithQuantifier(@Nonnull final PartialMatch partialMatch, @Nonnull final Quantifier quantifier) {
            this.partialMatch = partialMatch;
            this.quantifier = quantifier;
        }

        @Nonnull
        public static PartialMatchWithQuantifier of(@Nonnull final PartialMatch partialMatch, @Nonnull final Quantifier quantifier) {
            return new PartialMatchWithQuantifier(partialMatch, quantifier);
        }

        @Nonnull
        public PartialMatch getPartialMatch() {
            return partialMatch;
        }

        @Nonnull
        public Quantifier getQuantifier() {
            return quantifier;
        }
    }
}
