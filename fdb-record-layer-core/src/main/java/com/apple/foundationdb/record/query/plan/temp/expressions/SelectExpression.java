/*
 * SelectExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.temp.IterableHelpers;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.PredicateMap;
import com.apple.foundationdb.record.query.plan.temp.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.temp.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Sargable;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.base.Equivalence;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A select expression.
 */
@API(API.Status.EXPERIMENTAL)
public class SelectExpression implements RelationalExpressionWithChildren, RelationalExpressionWithPredicates, InternalPlannerGraphRewritable {
    @Nonnull
    private final Value resultValue;
    @Nonnull
    private final List<Quantifier> children;
    @Nonnull
    private final List<? extends QueryPredicate> predicates;

    public SelectExpression(@Nonnull Value resultValue,
                            @Nonnull List<? extends Quantifier> children,
                            @Nonnull List<? extends QueryPredicate> predicates) {
        this.resultValue = resultValue;
        this.children = ImmutableList.copyOf(children);
        this.predicates = predicates.isEmpty()
                          ? ImmutableList.of()
                          : partitionPredicates(predicates);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends QueryPredicate> getPredicates() {
        return predicates;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return children;
    }

    @Override
    public int getRelationalChildCount() {
        return children.size();
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Streams.concat(predicates.stream().flatMap(queryPredicate -> queryPredicate.getCorrelatedTo().stream()),
                        resultValue.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public SelectExpression rebase(@Nonnull final AliasMap translationMap) {
        return (SelectExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public SelectExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap, @Nonnull final List<Quantifier> rebasedQuantifiers) {
        List<QueryPredicate> rebasedPredicates = predicates.stream().map(p -> p.rebase(translationMap)).collect(Collectors.toList());
        final Value rebasedResultValue = resultValue.rebase(translationMap);
        return new SelectExpression(rebasedResultValue, rebasedQuantifiers, rebasedPredicates);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final List<? extends QueryPredicate> otherPredicates = ((SelectExpression)otherExpression).getPredicates();
        return semanticEqualsForResults(otherExpression, aliasMap) &&
               predicates.size() == otherPredicates.size() &&
               Streams.zip(predicates.stream(),
                       otherPredicates.stream(),
                       (queryPredicate, otherQueryPredicate) -> queryPredicate.semanticEquals(otherQueryPredicate, aliasMap))
                .allMatch(isSame -> isSame);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getPredicates(), getResultValue());
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap aliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        // TODO This method should be simplified by adding some structure to it.
        final Collection<MatchInfo> matchInfos = PartialMatch.matchesFromMap(partialMatchMap);

        Verify.verify(this != candidateExpression);

        if (getClass() != candidateExpression.getClass()) {
            return ImmutableList.of();
        }
        final SelectExpression otherSelectExpression = (SelectExpression)candidateExpression;

        // merge parameter maps -- early out if a binding clashes
        final ImmutableList<Map<CorrelationIdentifier, ComparisonRange>> parameterBindingMaps =
                matchInfos
                        .stream()
                        .map(MatchInfo::getParameterBindingMap)
                        .collect(ImmutableList.toImmutableList());
        final Optional<Map<CorrelationIdentifier, ComparisonRange>> mergedParameterBindingMapOptional =
                MatchInfo.tryMergeParameterBindings(parameterBindingMaps);
        if (!mergedParameterBindingMapOptional.isPresent()) {
            return ImmutableList.of();
        }
        final Map<CorrelationIdentifier, ComparisonRange> mergedParameterBindingMap = mergedParameterBindingMapOptional.get();
        
        final ImmutableSet.Builder<CorrelationIdentifier> matchedCorrelatedToBuilder = ImmutableSet.builder();
        // Loop through all child matches and reject a match if the children matches were unable to match all
        // for-each quantifiers. Also keep track of all aliases the matched quantifiers are correlated to.
        for (final Quantifier quantifier : getQuantifiers()) {
            if (partialMatchMap.containsKeyUnwrapped(quantifier)) {
                if (quantifier instanceof Quantifier.ForEach) {
                    // current quantifier is matched
                    final PartialMatch childPartialMatch = Objects.requireNonNull(partialMatchMap.getUnwrapped(quantifier));

                    if (!childPartialMatch.getQueryExpression()
                            .computeUnmatchedForEachQuantifiers(childPartialMatch).isEmpty()) {
                        return ImmutableList.of();
                    }
                }

                matchedCorrelatedToBuilder.addAll(quantifier.getCorrelatedTo());
            }
        }

        matchedCorrelatedToBuilder.addAll(getResultValue().getCorrelatedTo());

        final ImmutableSet<CorrelationIdentifier> matchedCorrelatedTo = matchedCorrelatedToBuilder.build();

        if (getQuantifiers()
                .stream()
                .anyMatch(quantifier -> quantifier instanceof Quantifier.ForEach && !partialMatchMap.containsKeyUnwrapped(quantifier))) {
            return ImmutableList.of();
        }

        final boolean allNonMatchedQuantifiersIndependent =
                getQuantifiers()
                        .stream()
                        .filter(quantifier -> !partialMatchMap.containsKeyUnwrapped(quantifier))
                        .noneMatch(quantifier -> matchedCorrelatedTo.contains(quantifier.getAlias()));

        if (!allNonMatchedQuantifiersIndependent) {
            return ImmutableList.of();
        }

        // Loop through all for-each quantifiers on the other side to ensure that they are all matched.
        // If any are not matched we cannot establish a match at all.
        final boolean allOtherForEachQuantifiersMatched =
                otherSelectExpression.getQuantifiers()
                        .stream()
                        .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                        .allMatch(quantifier -> aliasMap.containsTarget(quantifier.getAlias()));

        // TODO this is not really needed if we assign a property to the quantifier that allows us to reason about the
        //      the "default on empty" property as in does this quantifier flow a scalar result such as an "empty" value,
        //      a real value, or even null if the underlying graph evaluates to empty. The presence of such a property
        //      would help us here to make sure the additional non-matched quantifier is not eliminating records.
        if (!allOtherForEachQuantifiersMatched) {
            return ImmutableList.of();
        }

        //
        // Go through all matched existential quantifiers. Make sure that there is a top level exists() predicate
        // corresponding to  each  one.
        //
        if (getQuantifiers()
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.Existential && aliasMap.containsSource(quantifier.getAlias()))
                .anyMatch(quantifier -> getPredicates()
                        .stream()
                        .noneMatch(predicate -> predicate instanceof ExistsPredicate &&
                                                ((ExistsPredicate)predicate).getExistentialAlias().equals(quantifier.getAlias()))
                )) {
            return ImmutableList.of();
        }

        //
        // Map predicates on the query side to predicates on the candidate side. Record parameter bindings and/or
        // compensations for each mapped predicate.
        // A predicate on this side (the query side) can cause us to filter out rows, a mapped predicate (for that
        // predicate) can only filter out fewer rows which is correct and can be compensated for. The important part
        // is that we must not have predicates on the other (candidate) side at the end of this mapping process which
        // would mean that the candidate eliminates records that the query side may not eliminate. If we detect that
        // case we MUST not create a match.
        //
        final ImmutableList.Builder<Iterable<PredicateMapping>> predicateMappingsBuilder = ImmutableList.builder();

        //
        // Handle the "on empty" case, i.e., the case where there are no predicates on the query side that can
        // map to anything on the candidate side. Conceptually that could be handled with a fake tautology predicate
        // but this seems to be simpler albeit a little more verbose here. The important restriction in order to
        // produce a match is that the candidate side MUST NOT be filtering at all, as the query side is not either.
        //
        if (getPredicates().isEmpty()) {
            final boolean allNonFiltering = otherSelectExpression.getPredicates()
                    .stream()
                    .allMatch(queryPredicate -> queryPredicate instanceof Placeholder || queryPredicate.isTautology());
            if (allNonFiltering) {
                return MatchInfo.tryMerge(partialMatchMap, mergedParameterBindingMap, PredicateMap.empty())
                        .map(ImmutableList::of)
                        .orElse(ImmutableList.of());
            } else {
                return ImmutableList.of();
            }
        }

        for (final QueryPredicate predicate : getPredicates()) {
            final Set<PredicateMapping> impliedMappingsForPredicate =
                    predicate.findImpliedMappings(aliasMap, otherSelectExpression.getPredicates());
            predicateMappingsBuilder.add(impliedMappingsForPredicate);
        }

        //
        // We now have a multimap from predicates on the query side to predicates on the candidate side. In the trivial
        // case this multimap only contains singular mappings for a query predicate. If it doesn't we need to enumerate
        // through their cross product exhaustively. Each complete and non-contradictory element of that cross product
        // can lead to a match.
        //
        final EnumeratingIterable<PredicateMapping> crossedMappings =
                CrossProduct.crossProduct(predicateMappingsBuilder.build());

        return IterableHelpers.flatMap(crossedMappings,
                predicateMappings -> {
                    final Set<QueryPredicate> unmappedOtherPredicates = Sets.newIdentityHashSet();
                    unmappedOtherPredicates.addAll(otherSelectExpression.getPredicates());

                    final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap = Maps.newHashMap();
                    final PredicateMap.Builder predicateMapBuilder = PredicateMap.builder();

                    for (final PredicateMapping predicateMapping : predicateMappings) {
                        predicateMapBuilder.put(predicateMapping.getQueryPredicate(), predicateMapping);
                        unmappedOtherPredicates.remove(predicateMapping.getCandidatePredicate());

                        final Optional<CorrelationIdentifier> parameterAliasOptional = predicateMapping.getParameterAliasOptional();
                        final Optional<ComparisonRange> comparisonRangeOptional = predicateMapping.getComparisonRangeOptional();
                        if (parameterAliasOptional.isPresent() &&
                                comparisonRangeOptional.isPresent()) {
                            parameterBindingMap.put(parameterAliasOptional.get(), comparisonRangeOptional.get());
                        }
                    }

                    //
                    // Last chance for unmapped predicates - if there is a placeholder or a tautology on the other side that is still
                    // unmapped, we can (and should) remove it from the unmapped other set now. The reasoning is that this predicate is
                    // not filtering so it does not cause records to be filtered that are not filtered on the query side.
                    //
                    unmappedOtherPredicates
                            .removeIf(queryPredicate -> queryPredicate instanceof Placeholder || queryPredicate.isTautology());

                    if (!unmappedOtherPredicates.isEmpty()) {
                        return ImmutableList.of();
                    }

                    final Optional<? extends PredicateMap> predicateMapOptional = predicateMapBuilder.buildMaybe();
                    return predicateMapOptional
                            .map(predicateMap -> {
                                final Optional<Map<CorrelationIdentifier, ComparisonRange>> allParameterBindingMapOptional =
                                        MatchInfo.tryMergeParameterBindings(ImmutableList.of(mergedParameterBindingMap, parameterBindingMap));

                                return allParameterBindingMapOptional
                                        .flatMap(allParameterBindingMap -> MatchInfo.tryMerge(partialMatchMap, allParameterBindingMap, predicateMap))
                                        .map(ImmutableList::of)
                                        .orElse(ImmutableList.of());
                            })
                            .orElse(ImmutableList.of());
                });
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "SELECT " + resultValue,
                        getPredicates().isEmpty() ? ImmutableList.of() : ImmutableList.of("WHERE " + AndPredicate.and(getPredicates())),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public String toString() {
        return "SELECT " + resultValue + " WHERE " + AndPredicate.and(getPredicates());
    }

    private static List<? extends QueryPredicate> partitionPredicates(final List<? extends QueryPredicate> predicates) {
        final ImmutableList<QueryPredicate> flattenedAndPredicates =
                predicates.stream()
                        .flatMap(predicate -> flattenAndPredicate(predicate).stream())
                        .collect(ImmutableList.toImmutableList());

        // partition predicates in value-based predicates and non-value-based predicates
        final ImmutableList.Builder<PredicateWithValue> predicateWithValuesBuilder = ImmutableList.builder();
        final ImmutableList.Builder<QueryPredicate> resultPredicatesBuilder = ImmutableList.builder();

        for (final QueryPredicate flattenedAndPredicate : flattenedAndPredicates) {
            if (flattenedAndPredicate instanceof PredicateWithValue) {
                predicateWithValuesBuilder.add((PredicateWithValue)flattenedAndPredicate);
            } else {
                resultPredicatesBuilder.add(flattenedAndPredicate);
            }
        }

        final ImmutableList<PredicateWithValue> predicateWithValues = predicateWithValuesBuilder.build();

        final AliasMap boundIdentitiesMap = AliasMap.identitiesFor(
                flattenedAndPredicates.stream()
                        .flatMap(predicate -> predicate.getCorrelatedTo().stream())
                        .collect(ImmutableSet.toImmutableSet()));

        final BoundEquivalence boundEquivalence = new BoundEquivalence(boundIdentitiesMap);

        final Multimap<Equivalence.Wrapper<Value>, PredicateWithValue> partitionedPredicatesWithValues =
                predicateWithValues
                        .stream()
                        .collect(Multimaps.toMultimap(
                                predicate -> boundEquivalence.wrap(predicate.getValue()), Function.identity(), LinkedHashMultimap::create));

        partitionedPredicatesWithValues
                .asMap()
                .forEach((valueWrapper, predicatesOnValue) -> {
                    final Value value = Objects.requireNonNull(valueWrapper.get());
                    ComparisonRange resultRange = ComparisonRange.EMPTY;
                    for (final PredicateWithValue predicateOnValue : predicatesOnValue) {
                        if (predicateOnValue instanceof ValuePredicate) {
                            final Comparisons.Comparison comparison = ((ValuePredicate)predicateOnValue).getComparison();

                            final ComparisonRange.MergeResult mergeResult =
                                    resultRange.merge(comparison);

                            resultRange = mergeResult.getComparisonRange();

                            mergeResult.getResidualComparisons()
                                    .forEach(residualComparison ->
                                            resultPredicatesBuilder.add(value.withComparison(residualComparison)));
                        } else if (predicateOnValue instanceof Sargable) {
                            final Sargable valueComparisonRangePredicate = (Sargable)predicateOnValue;
                            final ComparisonRange comparisonRange = valueComparisonRangePredicate.getComparisonRange();

                            final ComparisonRange.MergeResult mergeResult =
                                    resultRange.merge(comparisonRange);

                            resultRange = mergeResult.getComparisonRange();

                            mergeResult.getResidualComparisons()
                                    .forEach(residualComparison ->
                                            resultPredicatesBuilder.add(value.withComparison(residualComparison)));
                        } else {
                            resultPredicatesBuilder.add(predicateOnValue);
                        }
                    }
                    if (!resultRange.isEmpty()) {
                        resultPredicatesBuilder.add(ValueComparisonRangePredicate.sargable(value, resultRange));
                    }
                });

        return resultPredicatesBuilder.build();
    }

    private static List<QueryPredicate> flattenAndPredicate(final QueryPredicate predicate) {
        final ImmutableList.Builder<QueryPredicate> result = ImmutableList.builder();

        if (predicate instanceof AndPredicate) {
            for (final QueryPredicate child : ((AndPredicate)predicate).getChildren()) {
                result.addAll(flattenAndPredicate(child));
            }
            return result.build();
        }
        return result.add(predicate).build();
    }

    @Override
    @SuppressWarnings({"java:S135", "java:S1066"})
    public Compensation compensate(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final Map<QueryPredicate, QueryPredicate> toBeReappliedPredicatesMap = Maps.newIdentityHashMap();
        final MatchInfo matchInfo = partialMatch.getMatchInfo();
        final PredicateMap predicateMap = matchInfo.getPredicateMap();

        //
        // The partial match we are called with here has child matches that have compensations on their own.
        // Given a pair of these matches that we reach along two for each quantifiers (forming a join) we have to
        // apply both compensations. The compensation class has a union method to combine two compensations in an
        // optimal way. We need to fold over all those compensations to form one child compensation. The tree that
        // is formed by partial matches therefore collapses into a chain of compensations.
        //
        final List<? extends Quantifier> quantifiers = getQuantifiers();
        final Compensation childCompensation = quantifiers
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                .flatMap(quantifier ->
                        matchInfo.getChildPartialMatch(quantifier)
                                .map(childPartialMatch -> childPartialMatch.compensate(boundParameterPrefixMap))
                                .map(Stream::of)
                                .orElse(Stream.empty()))
                .reduce(Compensation.noCompensation(), Compensation::union);

        //
        // The fact that we matched the partial match handed in must mean that the child compensation is not impossible.
        //
        Verify.verify(!childCompensation.isImpossible());

        //
        // Go through all predicates and invoke the reapplication logic for each associated mapping. Remember, each
        // predicate MUST have a mapping to the other side (which may just be a tautology). If something needs to be
        // reapplied that logic creates the correct predicates. The reapplication logic is also passed enough context
        // to skip reapplication in which case we won't do anything when compensation needs to be applied.
        //
        for (final QueryPredicate predicate : getPredicates()) {
            final Optional<PredicateMapping> predicateMappingOptional = predicateMap.getMappingOptional(predicate);
            Verify.verify(predicateMappingOptional.isPresent());

            final PredicateMapping predicateMapping = predicateMappingOptional.get();

            final Optional<QueryPredicate> reappliedPredicateOptional =
                    predicateMapping
                            .reapplyPredicateFunction()
                            .reapplyPredicateMaybe(matchInfo, boundParameterPrefixMap);

            reappliedPredicateOptional.ifPresent(reappliedPredicate -> toBeReappliedPredicatesMap.put(predicate, reappliedPredicate));
        }

        return Compensation.ofChildCompensationAndPredicateMap(childCompensation,
                toBeReappliedPredicatesMap,
                computeMappedQuantifiers(partialMatch),
                computeUnmatchedForEachQuantifiers(partialMatch));
    }
}
