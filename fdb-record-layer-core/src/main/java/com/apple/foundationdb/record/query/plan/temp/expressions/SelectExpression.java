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
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.temp.MatchWithCompensation;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicate;
import com.apple.foundationdb.record.query.plan.temp.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ConstantPredicate;
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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A select expression.
 */
@API(API.Status.EXPERIMENTAL)
public class SelectExpression implements RelationalExpressionWithChildren, RelationalExpressionWithPredicate, InternalPlannerGraphRewritable {
    @Nonnull
    private final List<Quantifier> children;
    @Nonnull
    private final List<QueryPredicate> predicates;

    public SelectExpression(@Nonnull List<Quantifier> children) {
        this(children, ImmutableList.of());
    }

    public SelectExpression(@Nonnull List<Quantifier> children, @Nonnull List<QueryPredicate> predicates) {
        this.children = children;
        this.predicates = predicates.isEmpty()
                          ? ImmutableList.of(ConstantPredicate.TRUE)
                          : groupPredicates(predicates);
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
    @Nonnull
    public QueryPredicate getPredicate() {
        return AndPredicate.and(predicates);
    }

    @Nonnull
    public List<QueryPredicate> getPredicates() {
        return predicates;
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return getPredicate().getCorrelatedTo();
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
        return new SelectExpression(rebasedQuantifiers, rebasedPredicates);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        return getPredicate().semanticEquals(((SelectExpression)otherExpression).getPredicate(), equivalencesMap);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getPredicate());
    }

    @Nonnull
    @Override
    public Iterable<MatchWithCompensation> subsumedBy(@Nonnull final RelationalExpression otherExpression,
                                                      @Nonnull final AliasMap aliasMap,
                                                      @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        final Collection<MatchWithCompensation> matchWithCompensations = PartialMatch.matchesFromMap(partialMatchMap);

        if (this == otherExpression) {
            return MatchWithCompensation.tryFromMatchMapWithAllPredicates(partialMatchMap, getPredicates())
                    .map(ImmutableList::of)
                    .orElse(ImmutableList.of());
        }
        if (getClass() != otherExpression.getClass()) {
            return ImmutableList.of();
        }
        final SelectExpression otherSelectExpression = (SelectExpression)otherExpression;

        // merge parameter maps -- early out if a binding clashes
        final ImmutableList<Map<CorrelationIdentifier, ComparisonRange>> parameterBindingMaps =
                matchWithCompensations
                        .stream()
                        .map(MatchWithCompensation::getParameterBindingMap)
                        .collect(ImmutableList.toImmutableList());
        final Optional<Map<CorrelationIdentifier, ComparisonRange>> mergedParameterBindingMapOptional =
                MatchWithCompensation.tryMergeParameterBindings(parameterBindingMaps);
        if (!mergedParameterBindingMapOptional.isPresent()) {
            return ImmutableList.of();
        }
        final Map<CorrelationIdentifier, ComparisonRange> mergedParameterBindingMap = mergedParameterBindingMapOptional.get();

        // 1. ensure all for each quantifiers on this and the other side are matched
        // TODO describe why for each on the other side can eliminate records in the join if the quantifier evaluates
        // TODO to empty
        //
        // 2. find all predicates referring to only matched quantifiers
        //    a) find all sargable predicates that can now be associated with placeholder predicates, resolve
        //       placeholder (parameter) to comparison ranges mappings
        //    b) find all exists(q) predicates and record that we need enforce distinctness for compensation and the exists()
        // 3. find all predicates referring to only unmatched quantifiers (the rest)
        //    a.) find predicates on unmatched quantifiers on this side and record to reapply those predicates for compensation
        //    b.) find predicates on unmatched quantifiers on the other side (candidate) and record that we need to enforce distinctness as compensation

        // loop through all for each quantifiers on this side to ensure that they are all matched
        final boolean allForEachQuantifiersMatched = getQuantifiers()
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                .allMatch(quantifier -> aliasMap.containsSource(quantifier.getAlias()));

        if (!allForEachQuantifiersMatched) {
            return ImmutableList.of();
        }

        // loop through all for each quantifiers on this side to ensure that they are all matched
        final boolean allOtherForEachQuantifiersMatched =
                otherSelectExpression.getQuantifiers()
                        .stream()
                        .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                        .allMatch(quantifier -> aliasMap.containsTarget(quantifier.getAlias()));

        if (!allOtherForEachQuantifiersMatched) {
            return ImmutableList.of();
        }

        final Map<Boolean, List<QueryPredicate>> predicatesOnQuantifiers =
                getPredicates()
                        .stream()
                        .collect(Collectors.partitioningBy(predicate -> predicate.getCorrelatedTo().stream().allMatch(aliasMap::containsSource)));

        final List<QueryPredicate> predicatesOnMatchedQuantifiers = predicatesOnQuantifiers.get(true);

        final Set<QueryPredicate> unmappedPredicates = Sets.newIdentityHashSet();
        unmappedPredicates.addAll(getPredicates());
        final Set<QueryPredicate> unmappedOtherPredicates = Sets.newIdentityHashSet();
        unmappedOtherPredicates.addAll(otherSelectExpression.getPredicates());

        final Equivalence<Object> identity = Equivalence.identity();
        final IdentityBiMap<QueryPredicate, QueryPredicate> mappedPredicatesMap = IdentityBiMap.create();
        final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap = Maps.newHashMap();

        final ImmutableListMultimap.Builder<CorrelationIdentifier, QueryPredicate> aliasToOtherPredicatesMapBuilder =
                ImmutableListMultimap.builder();
        for (final QueryPredicate otherPredicate : otherSelectExpression.getPredicates()) {
            final Set<CorrelationIdentifier> otherCorrelatedTo = otherPredicate.getCorrelatedTo();
            // we currently can only match local (non join) predicates
            if (otherCorrelatedTo.size() == 1) {
                @Nullable final CorrelationIdentifier sourceAlias =
                        aliasMap.getSource(Iterables.getOnlyElement(otherCorrelatedTo));
                if (sourceAlias != null) {
                    aliasToOtherPredicatesMapBuilder.put(sourceAlias, otherPredicate);
                }
            }
        }
        final ImmutableListMultimap<CorrelationIdentifier, QueryPredicate> aliasToOtherPredicatesMap =
                aliasToOtherPredicatesMapBuilder.build();

        for (final QueryPredicate predicate : getPredicates()) {
            if (predicate instanceof Sargable) {
                final Sargable sargablePredicate = (Sargable)predicate;
                final Set<CorrelationIdentifier> correlatedTo =
                        sargablePredicate.getValue()
                                .getCorrelatedTo();
                if (correlatedTo.size() == 1) {
                    final CorrelationIdentifier correlatedToAlias = Iterables.getOnlyElement(correlatedTo);

                    final ImmutableList<QueryPredicate> otherPredicates = aliasToOtherPredicatesMap.get(correlatedToAlias);

                    for (final QueryPredicate otherPredicate : otherPredicates) {
                        if (otherPredicate instanceof Placeholder) {
                            final Placeholder placeHolderPredicate = ((Placeholder)otherPredicate);
                            if (sargablePredicate
                                    .getValue()
                                    .semanticEquals(placeHolderPredicate.getValue(), aliasMap)) {
                                // we found a compatible association between a comparison range in the query and a
                                // parameter placeholder in the candidate - record the match but about if that match
                                // would lead to a clash in parameters (which should not happen)
                                final CorrelationIdentifier parameterAlias = placeHolderPredicate.getParameterAlias();
                                if (mergedParameterBindingMap.containsKey(parameterAlias) ||
                                        parameterBindingMap.containsKey(parameterAlias)) {
                                    // clash
                                    return ImmutableList.of();
                                }

                                parameterBindingMap.put(parameterAlias, sargablePredicate.getComparisonRange());
                                mappedPredicatesMap.put(identity.wrap(sargablePredicate),
                                        identity.wrap(placeHolderPredicate));
                                unmappedOtherPredicates.remove(placeHolderPredicate);
                            }
                        }
                    }
                    // TODO if the previous loop didn't at least find one matching predicate on the other side, we need
                    // TODO to reapply the filter
                    unmappedPredicates.remove(sargablePredicate);
                }
            } else if (predicate instanceof ExistsPredicate) {
                // We do know that this predicate may refer to a matched or unmatched quantifier.
                final ExistsPredicate existsPredicate = (ExistsPredicate)predicate;
                final CorrelationIdentifier existentialAlias =
                        existsPredicate.getExistentialAlias();
                final ImmutableList<QueryPredicate> otherPredicates = aliasToOtherPredicatesMap.get(existentialAlias);

                for (final QueryPredicate otherPredicate : otherPredicates) {
                    if (otherPredicate instanceof ExistsPredicate) {
                        final ExistsPredicate otherExistsPredicate = (ExistsPredicate)otherPredicate;
                        Verify.verify(otherExistsPredicate.getExistentialAlias().equals(aliasMap.getTarget(existentialAlias)));
                        mappedPredicatesMap.put(identity.wrap(existsPredicate), identity.wrap(otherExistsPredicate));
                        unmappedOtherPredicates.remove(otherExistsPredicate);
                    }
                }
                // TODO if the previous loop didn't at least find one matching predicate on the other side, we need
                // TODO to reapply the filter
                unmappedPredicates.remove(existsPredicate);
            }
        }

        // Last chance for unmapped predicates - if there is a placeholder on the other side, we can (and should) remove it
        // from the unmapped other set now. The reasoning is that this predicate is not filtering (i.e. false) if there is
        // input for the matched quantifier quantifier, meaning the range is unlimited and the the predicate is a tautology.
        unmappedOtherPredicates
                .removeIf(predicate -> predicate instanceof Placeholder ||
                                       (predicate instanceof ConstantPredicate && ((ConstantPredicate)predicate).getValue()));

        if (!unmappedOtherPredicates.isEmpty()) {
            return ImmutableSet.of();
        }

        final List<QueryPredicate> predicatesOnUnmatchedQuantifiers = predicatesOnQuantifiers.get(false);
        // TODO reapply all unmatched predicates

        // at this moment there shouldn't be any unmapped predicates left on this side
        Verify.verify(unmappedPredicates.isEmpty());

        final Optional<Map<CorrelationIdentifier, ComparisonRange>> allParameterBindingMapOptional =
                MatchWithCompensation.tryMergeParameterBindings(ImmutableList.of(mergedParameterBindingMap, parameterBindingMap));

        return allParameterBindingMapOptional
                .flatMap(allParameterBindingMap -> MatchWithCompensation.tryMerge(partialMatchMap, allParameterBindingMap, mappedPredicatesMap))
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "Select",
                        ImmutableList.of(toString()),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public String toString() {
        return "WHERE " + getPredicate();
    }

    @SuppressWarnings("UnstableApiUsage")
    private static List<QueryPredicate> groupPredicates(final List<QueryPredicate> predicates) {
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

        final HashMultimap<Equivalence.Wrapper<Value>, PredicateWithValue> groupedPredicatesWithValues =
                predicateWithValues
                        .stream()
                        .collect(Multimaps.toMultimap(
                                predicate -> boundEquivalence.wrap(predicate.getValue()), Function.identity(), HashMultimap::create));

        groupedPredicatesWithValues
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
                                            resultPredicatesBuilder.add(new ValuePredicate(value, residualComparison)));
                        } else if (predicateOnValue instanceof Sargable) {
                            final Sargable valueComparisonRangePredicate = (Sargable)predicateOnValue;
                            final ComparisonRange comparisonRange = valueComparisonRangePredicate.getComparisonRange();

                            final ComparisonRange.MergeResult mergeResult =
                                    resultRange.merge(comparisonRange);

                            resultRange = mergeResult.getComparisonRange();

                            mergeResult.getResidualComparisons()
                                    .forEach(residualComparison ->
                                            resultPredicatesBuilder.add(new ValuePredicate(value, residualComparison)));
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
}
