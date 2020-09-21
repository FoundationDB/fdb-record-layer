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
import com.apple.foundationdb.record.query.plan.temp.MatchWithCompensation;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicate;
import com.apple.foundationdb.record.query.plan.temp.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.base.Equivalence;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
                                                      @Nonnull final Map<Quantifier, PartialMatch> partialMatchMap) {
        if (this == otherExpression) {
            return ImmutableList.of(MatchWithCompensation.fromOthers(PartialMatch.matchesFromMap(partialMatchMap)));
        }
        if (getClass() != otherExpression.getClass()) {
            return ImmutableList.of();
        }

        final Map<? extends Class<?>, List<QueryPredicate>> collect =
                getPredicates()
                        .stream()
                        .collect(Collectors.groupingBy(Object::getClass, Collectors.toList()));

        

        if (equalsWithoutChildren(otherExpression, aliasMap)) {
            return ImmutableList.of(MatchWithCompensation.from());
        } else {
            return ImmutableList.of();
        }
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
                        } else if (predicateOnValue instanceof ValueComparisonRangePredicate) {
                            final ValueComparisonRangePredicate valueComparisonRangePredicate = (ValueComparisonRangePredicate)predicateOnValue;
                            if (valueComparisonRangePredicate.getComparisonRange() != null) {
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
                        } else {
                            resultPredicatesBuilder.add(predicateOnValue);
                        }
                    }
                    if (!resultRange.isEmpty()) {
                        resultPredicatesBuilder.add(ValueComparisonRangePredicate.withComparisonRange(value, resultRange));
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
