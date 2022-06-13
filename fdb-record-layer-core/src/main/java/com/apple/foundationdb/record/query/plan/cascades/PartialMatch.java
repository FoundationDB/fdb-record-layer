/*
 * PartialMatch.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueComparisonRangePredicate;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Case class to represent a partial match. A partial match is stored in a multi map in {@link GroupExpressionRef}s that
 * are part of a query graph. A partial match is never stored in a {@link GroupExpressionRef} of a match candidate.
 * A partial match establishes the a relation ships between a reference {@code ref1} in a query graph and a reference
 * {@code ref2} in a match candidate. It should be interpreted as there is a partial match between {@code ref1} and
 * {@code ref2} if {@code ref1} is result-equivalent to {@code ref2'} under the bindings in the alias map returned by
 * ({@link #getBoundAliasMap()}) where {@code ref2'} is obtained by is applying compensation to {@code ref2}.
 * If there is no compensation this simplifies to: there is a partial match between {@code ref1} and
 * {@code ref2} if {@code ref1} is result-equivalent to {@code ref2} under the bindings in the alias map returned by
 * ({@link #getBoundAliasMap()}).
 * It is the task of a matching algorithm (see {@link CascadesPlanner}) to establish {@link PartialMatch}es
 * between query graph and match candidate graphs. Once there is a partial match for the top-most reference in the
 * match candidate, we can replace the reference on the query graph side with a scan over the match candidate's
 * materialized version (e.g. an index) and subsequent compensation.
 */
public class PartialMatch {
    /**
     * Alias map of all bound correlated references.
     */
    @Nonnull
    private final AliasMap boundAliasMap;

    /**
     * Match candidate.
     */
    @Nonnull
    private final MatchCandidate matchCandidate;

    /**
     * Expression reference in query graph.
     */
    @Nonnull
    private final ExpressionRef<? extends RelationalExpression> queryRef;

    /**
     * Expression in query graph.
     */
    @Nonnull
    private final RelationalExpression queryExpression;

    /**
     * Expression reference in match candidate graph.
     */
    @Nonnull
    private final ExpressionRef<? extends RelationalExpression> candidateRef;

    /**
     * Compensation operator that can be applied to the scan of the materialized version of the match candidate.
     */
    @Nonnull
    private final MatchInfo matchInfo;

    @Nonnull
    private final Supplier<Map<CorrelationIdentifier, ComparisonRange>> boundParameterPrefixMapSupplier;

    @Nonnull
    private final Supplier<Set<QueryPredicate>> bindingPredicatesSupplier;

    public PartialMatch(@Nonnull final AliasMap boundAliasMap,
                        @Nonnull final MatchCandidate matchCandidate,
                        @Nonnull final ExpressionRef<? extends RelationalExpression> queryRef,
                        @Nonnull final RelationalExpression queryExpression,
                        @Nonnull final ExpressionRef<? extends RelationalExpression> candidateRef,
                        @Nonnull final MatchInfo matchInfo) {
        this.boundAliasMap = boundAliasMap;
        this.matchCandidate = matchCandidate;
        this.queryRef = queryRef;
        this.queryExpression = queryExpression;
        this.candidateRef = candidateRef;
        this.matchInfo = matchInfo;
        this.boundParameterPrefixMapSupplier = Suppliers.memoize(this::computeBoundParameterPrefixMap);
        this.bindingPredicatesSupplier = Suppliers.memoize(this::computeBindingQueryPredicates);
    }

    @Nonnull
    public AliasMap getBoundAliasMap() {
        return boundAliasMap;
    }

    @Nonnull
    public MatchCandidate getMatchCandidate() {
        return matchCandidate;
    }

    @Nonnull
    public ExpressionRef<? extends RelationalExpression> getQueryRef() {
        return queryRef;
    }

    @Nonnull
    public RelationalExpression getQueryExpression() {
        return queryExpression;
    }

    @Nonnull
    public ExpressionRef<? extends RelationalExpression> getCandidateRef() {
        return candidateRef;
    }

    @Nonnull
    public MatchInfo getMatchInfo() {
        return matchInfo;
    }

    public int getNumBoundParameterPrefix() {
        return boundParameterPrefixMapSupplier.get().size();
    }

    @Nonnull
    public Map<CorrelationIdentifier, ComparisonRange> getBoundParameterPrefixMap() {
        return boundParameterPrefixMapSupplier.get();
    }

    @Nonnull
    private Map<CorrelationIdentifier, ComparisonRange> computeBoundParameterPrefixMap() {
        return getMatchCandidate().computeBoundParameterPrefixMap(getMatchInfo());
    }

    @Nonnull
    public Set<Quantifier> computeUnmatchedQuantifiers(@Nonnull final RelationalExpression relationalExpression) {
        final Set<Quantifier> unmatchedQuantifiers = new LinkedIdentitySet<>();
        for (final Quantifier quantifier : relationalExpression.getQuantifiers()) {
            if (matchInfo.getChildPartialMatch(quantifier.getAlias()).isEmpty()) {
                unmatchedQuantifiers.add(quantifier);
            }
        }
        return unmatchedQuantifiers;
    }

    @Nonnull
    private Set<QueryPredicate> computeBindingQueryPredicates() {
        final var boundParameterPrefixMap = getBoundParameterPrefixMap();
        final var bindingQueryPredicates = Sets.<QueryPredicate>newIdentityHashSet();

        //
        // Go through all accumulated parameter bindings -- find the query predicates binding the parameters. Those
        // query predicates are binding the parameters by means of a placeholder on the candidate side (they themselves
        // should be of class Placeholder). Note that there could be more than one query predicate mapping to a candidate
        // predicate.
        //
        for (final var entry : matchInfo.getAccumulatedPredicateMap().entries()) {
            final var predicateMapping = entry.getValue();
            final var candidatePredicate = predicateMapping.getCandidatePredicate();
            if (!(candidatePredicate instanceof ValueComparisonRangePredicate.Placeholder)) {
                continue;
            }

            final var placeholder = (ValueComparisonRangePredicate.Placeholder)candidatePredicate;
            if (boundParameterPrefixMap.containsKey(placeholder.getAlias())) {
                bindingQueryPredicates.add(predicateMapping.getQueryPredicate());
            }
        }

        return bindingQueryPredicates;
    }

    @Nonnull
    public final Set<QueryPredicate> getBindingPredidcates() {
        return bindingPredicatesSupplier.get();
    }

    @Nonnull
    public Compensation compensate() {
        return queryExpression.compensate(this, getBoundParameterPrefixMap());
    }

    @Nonnull
    public Compensation compensate(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        return queryExpression.compensate(this, boundParameterPrefixMap);
    }

    /**
     * Determine if compensation can be applied at a later time, e.g. at the top of the matched DAG. Mostly, that
     * can only be done if the compensation for this match is just a predicate that the MQT cannot handle. Whenever
     * we need to compensate for unmatched quantifiers and value computations, we need compensate right away (for now).
     * @return {@code true} if compensation can be deferred, {@code false} otherwise.
     */
    public boolean compensationCanBeDeferred() {
        return computeUnmatchedQuantifiers(getQueryExpression()).stream().anyMatch(quantifier -> quantifier instanceof Quantifier.ForEach) ||
               matchInfo.getRemainingComputationValueOptional().isEmpty();
    }

    @Nonnull
    public static Collection<MatchInfo> matchesFromMap(@Nonnull IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return partialMatchMap.values()
                .stream()
                .map(IdentityBiMap::unwrap)
                .map(Objects::requireNonNull)
                .map(PartialMatch::getMatchInfo)
                .collect(ImmutableList.toImmutableList());
    }
}
