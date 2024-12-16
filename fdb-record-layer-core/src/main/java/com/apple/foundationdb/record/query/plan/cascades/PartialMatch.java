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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Case class to represent a partial match. A partial match is stored in a multi-map in {@link Reference}s that
 * are part of a query graph. A partial match is never stored in a {@link Reference} of a match candidate.
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
    private final Reference queryRef;

    /**
     * Expression in query graph.
     */
    @Nonnull
    private final RelationalExpression queryExpression;

    /**
     * Expression reference in match candidate graph.
     */
    @Nonnull
    private final Reference candidateRef;

    /**
     * Compensation operator that can be applied to the scan of the materialized version of the match candidate.
     */
    @Nonnull
    private final MatchInfo matchInfo;

    @Nonnull
    private final Supplier<Map<CorrelationIdentifier, ComparisonRange>> boundParameterPrefixMapSupplier;

    @Nonnull
    private final Supplier<Set<QueryPredicate>> boundPlaceholdersSupplier;

    @Nonnull
    private final Supplier<Set<Quantifier>> matchedQuantifiersSupplier;

    @Nonnull
    private final Supplier<Set<Quantifier>> unmatchedQuantifiersSupplier;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> compensatedAliasesSupplier;

    @Nonnull
    private final Supplier<PredicateMap> accumulatedPredicateMapSupplier;

    @Nonnull
    private final Map<QueryPredicate, Optional<PredicateMapping>> memoizedPulledUpPredicateMap;

    public PartialMatch(@Nonnull final AliasMap boundAliasMap,
                        @Nonnull final MatchCandidate matchCandidate,
                        @Nonnull final Reference queryRef,
                        @Nonnull final RelationalExpression queryExpression,
                        @Nonnull final Reference candidateRef,
                        @Nonnull final MatchInfo matchInfo) {
        this.boundAliasMap = boundAliasMap;
        this.matchCandidate = matchCandidate;
        this.queryRef = queryRef;
        this.queryExpression = queryExpression;
        this.candidateRef = candidateRef;
        this.matchInfo = matchInfo;
        this.boundParameterPrefixMapSupplier = Suppliers.memoize(this::computeBoundParameterPrefixMap);
        this.boundPlaceholdersSupplier = Suppliers.memoize(this::computeBoundPlaceholders);
        this.matchedQuantifiersSupplier = Suppliers.memoize(this::computeMatchedQuantifiers);
        this.unmatchedQuantifiersSupplier = Suppliers.memoize(this::computeUnmatchedQuantifiers);
        this.compensatedAliasesSupplier = Suppliers.memoize(this::computeCompensatedAliases);
        this.accumulatedPredicateMapSupplier = Suppliers.memoize(this::computeAccumulatedPredicateMap);
        this.memoizedPulledUpPredicateMap = new LinkedIdentityMap<>();
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
    public Reference getQueryRef() {
        return queryRef;
    }

    @Nonnull
    public RelationalExpression getQueryExpression() {
        return queryExpression;
    }

    @Nonnull
    public Reference getCandidateRef() {
        return candidateRef;
    }

    @Nonnull
    public MatchInfo getMatchInfo() {
        return matchInfo;
    }

    @Nonnull
    public MatchInfo.RegularMatchInfo getRegularMatchInfo() {
        return matchInfo.getRegularMatchInfo();
    }

    public int getNumBoundParameterPrefix() {
        return getBoundParameterPrefixMap().size();
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
    public Set<Quantifier> getMatchedQuantifiers() {
        return matchedQuantifiersSupplier.get();
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private Set<Quantifier> computeMatchedQuantifiers() {
        return queryExpression.getQuantifiers()
                .stream()
                .filter(quantifier -> matchInfo.getRegularMatchInfo()
                        .getChildPartialMatchMaybe(quantifier.getAlias()).isPresent())
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
    }

    @Nonnull
    public Set<Quantifier> getUnmatchedQuantifiers() {
        return unmatchedQuantifiersSupplier.get();
    }

    @Nonnull
    private Set<Quantifier> computeUnmatchedQuantifiers() {
        return queryExpression.getQuantifiers()
                .stream()
                .filter(quantifier -> matchInfo.getRegularMatchInfo()
                        .getChildPartialMatchMaybe(quantifier.getAlias()).isEmpty())
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
    }

    @Nonnull
    public final Set<QueryPredicate> getBoundPlaceholders() {
        return boundPlaceholdersSupplier.get();
    }

    @Nonnull
    private Set<QueryPredicate> computeBoundPlaceholders() {
        final var boundParameterPrefixMap = getBoundParameterPrefixMap();
        final var boundPlaceholders = Sets.<QueryPredicate>newIdentityHashSet();

        //
        // Go through all accumulated parameter bindings -- find the query predicates binding the parameters. Those
        // query predicates are binding the parameters by means of a placeholder on the candidate side (they themselves
        // should be of class Placeholder). Note that there could be more than one query predicate mapping to a candidate
        // predicate.
        //
        for (final var entry : getAccumulatedPredicateMap().entries()) {
            final var predicateMapping = entry.getValue();

            if (predicateMapping.getMappingKind() != PredicateMapping.MappingKind.REGULAR_IMPLIES_CANDIDATE) {
                continue;
            }

            final var candidatePredicate = predicateMapping.getCandidatePredicate();
            if (!(candidatePredicate instanceof Placeholder)) {
                continue;
            }

            final var placeholder = (Placeholder)candidatePredicate;
            if (boundParameterPrefixMap.containsKey(placeholder.getParameterAlias())) {
                boundPlaceholders.add(predicateMapping.getCandidatePredicate());
            }
        }

        return boundPlaceholders;
    }

    /**
     * Return a set of aliases that this partial match is responsible for covering, that is either the matches
     * replacement or compensation will take care of the aliases in the returned set.
     * @return a set of compensated aliases
     */
    @Nonnull
    public final Set<CorrelationIdentifier> getCompensatedAliases() {
        return compensatedAliasesSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCompensatedAliases() {
        final var compensatedAliasesBuilder = ImmutableSet.<CorrelationIdentifier>builder();
        final var ownedAliases = Quantifiers.aliasToQuantifierMap(queryExpression.getQuantifiers()).keySet();

        queryExpression.getMatchedQuantifiers(this)
                .stream()
                .map(Quantifier::getAlias)
                .forEach(compensatedAliasesBuilder::add);

        // TODO This should not yield any further quantifiers. Maybe this needs to be removed.
        final var predicatesMap = matchInfo.getRegularMatchInfo().getPredicateMap();
        for (final QueryPredicate queryPredicate : predicatesMap.keySet()) {
            final var predicateCorrelatedTo = queryPredicate.getCorrelatedTo();
            predicateCorrelatedTo.stream()
                    .filter(ownedAliases::contains)
                    .forEach(compensatedAliasesBuilder::add);
        }

        return compensatedAliasesBuilder.build();
    }

    @Nonnull
    public PredicateMap getAccumulatedPredicateMap() {
        return accumulatedPredicateMapSupplier.get();
    }

    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private PredicateMap computeAccumulatedPredicateMap() {
        final var targetBuilder = PredicateMap.builder();
        final var regularMatchInfo = getRegularMatchInfo();
        targetBuilder.putAll(regularMatchInfo.getPredicateMap());

        for (final Equivalence.Wrapper<PartialMatch> partialMatchWrapper : regularMatchInfo.getPartialMatchMap().values()) {
            final PartialMatch childPartialMatch = Objects.requireNonNull(partialMatchWrapper.get());
            targetBuilder.putAll(childPartialMatch.getAccumulatedPredicateMap());
        }
        return targetBuilder.build();
    }

    @Nonnull
    public Map<QueryPredicate, PredicateMapping> getPulledUpPredicateMappings(@Nonnull final Predicate<QueryPredicate> predicateFilter) {
        final var interestingPredicates =
                getAccumulatedPredicateMap().getMap()
                        .keySet()
                        .stream()
                        .filter(predicateFilter)
                        .collect(LinkedIdentitySet.toLinkedIdentitySet());

        return getPulledUpPredicateMappings(interestingPredicates);
    }

    @Nonnull
    public Map<QueryPredicate, PredicateMapping> getPulledUpPredicateMappings(@Nonnull final Set<QueryPredicate> interestingPredicates) {
        final var resultMap = new LinkedIdentityMap<QueryPredicate, PredicateMapping>();
        final var unmemoizedInterestingPredicates = new LinkedIdentitySet<QueryPredicate>();
        for (final var interestingPredicate : interestingPredicates) {
            final var memoizedTranslatedPredicateMappingOptional =
                    memoizedPulledUpPredicateMap.get(interestingPredicate);
            if (memoizedTranslatedPredicateMappingOptional != null) {
                memoizedTranslatedPredicateMappingOptional.ifPresent(predicateMapping ->
                        resultMap.put(interestingPredicate, predicateMapping));
            } else {
                unmemoizedInterestingPredicates.add(interestingPredicate);
            }
        }

        final var candidateExpression = candidateRef.get();
        final var resultMapFromMatchInfo =
                getMatchInfo().collectPulledUpPredicateMappings(candidateExpression, unmemoizedInterestingPredicates);
        for (final var interestingPredicate : unmemoizedInterestingPredicates) {
            final var memoizedPulledUpPredicateMapping =
                    resultMapFromMatchInfo.get(interestingPredicate);
            if (memoizedPulledUpPredicateMapping == null) {
                memoizedPulledUpPredicateMap.put(interestingPredicate, Optional.empty());
            } else {
                memoizedPulledUpPredicateMap.put(interestingPredicate, Optional.of(memoizedPulledUpPredicateMapping));
                resultMap.put(interestingPredicate, memoizedPulledUpPredicateMapping);
            }
        }
        return resultMap;
    }

    @Nonnull
    public Compensation compensateCompleteMatch() {
        return queryExpression.compensate(this, getBoundParameterPrefixMap(), null, Quantifier.uniqueID());
    }

    @Nonnull
    public Compensation compensate(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nonnull final PullUp pullUp,
                                   @Nonnull final CorrelationIdentifier nestingAlias) {
        return queryExpression.compensate(this, boundParameterPrefixMap, pullUp, nestingAlias);
    }

    @Nonnull
    public Compensation compensateExistential(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        return queryExpression.compensate(this, boundParameterPrefixMap, null, Quantifier.uniqueID());
    }

    @Nonnull
    public PullUp nestPullUp(@Nullable final PullUp pullUp, @Nonnull final CorrelationIdentifier nestingAlias) {
        var currentMatchInfo = getMatchInfo();
        var currentCandidateRef = candidateRef;
        var currentPullUp = pullUp;
        var currentNestingAlias = nestingAlias;
        while (true) {
            final var nestingVisitor =
                    PullUp.visitor(currentPullUp, currentNestingAlias);
            final var currentCandidateExpression = currentCandidateRef.get();
            currentPullUp = nestingVisitor.visit(currentCandidateRef.get());
            if (!currentMatchInfo.isAdjusted()) {
                break;
            }

            final List<? extends Quantifier> currentQuantifiers = currentCandidateExpression.getQuantifiers();
            Verify.verify(currentQuantifiers.size() == 1);
            final Quantifier currentQuantifier = currentQuantifiers.get(0);
            currentNestingAlias = currentQuantifier.getAlias();
            currentCandidateRef = currentQuantifier.getRangesOver();

            currentMatchInfo = ((MatchInfo.AdjustedMatchInfo)currentMatchInfo).getUnderlying();
        }
        return currentPullUp;
    }

    /**
     * Determine if compensation can be applied at a later time, e.g. at the top of the matched DAG. Mostly, that
     * can only be done if the compensation for this match is just a predicate that the MQT cannot handle. Whenever
     * we need to compensate for unmatched quantifiers and value computations, we need compensate right away (for now).
     * @return {@code true} if compensation can be deferred, {@code false} otherwise.
     */
    public boolean compensationCanBeDeferred() {
        return getUnmatchedQuantifiers().stream()
                       .noneMatch(quantifier -> quantifier instanceof Quantifier.ForEach);
    }

    @Nonnull
    public static Collection<MatchInfo> matchInfosFromMap(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return partialMatchMap.values()
                .stream()
                .map(IdentityBiMap::unwrap)
                .map(Objects::requireNonNull)
                .map(PartialMatch::getMatchInfo)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return getQueryExpression().getClass().getSimpleName() + "[" + getMatchCandidate().getName() + "]";
    }
}
