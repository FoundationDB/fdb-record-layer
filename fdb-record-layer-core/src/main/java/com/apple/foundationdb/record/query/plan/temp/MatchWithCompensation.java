/*
 * MatchWithCompensation.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * This class represents the result of matching one expression against a candidate.
 */
public class MatchWithCompensation {
    /**
     * Compensation operator that can be applied to the scan of the materialized version of the match candidate.
     */
    @Nonnull
    private final UnaryOperator<ExpressionRef<RelationalExpression>> compensationOperator;

    /**
     * Parameter bindings for this match.
     */
    @Nonnull
    private final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap;

    @Nonnull
    private final IdentityBiMap<Quantifier, PartialMatch> quantifierPartialMatchMap;

    @Nonnull
    private final IdentityBiMap<QueryPredicate, QueryPredicate> predicateMap;

    @Nonnull
    private final Set<QueryPredicate> needCompensationPredicates;

    private MatchWithCompensation(@Nonnull final UnaryOperator<ExpressionRef<RelationalExpression>> compensationOperator,
                                  @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                  @Nonnull final IdentityBiMap<Quantifier, PartialMatch> quantifierPartialMatchMap,
                                  @Nonnull final IdentityBiMap<QueryPredicate, QueryPredicate> predicateMap,
                                  @Nonnull final Set<QueryPredicate> needCompensationPredicates) {
        this.compensationOperator = compensationOperator;
        this.parameterBindingMap = ImmutableMap.copyOf(parameterBindingMap);
        this.quantifierPartialMatchMap = quantifierPartialMatchMap.toImmutable();
        this.predicateMap = predicateMap.toImmutable();
        this.needCompensationPredicates = Sets.newIdentityHashSet();
        this.needCompensationPredicates.addAll(needCompensationPredicates);
    }

    @Nonnull
    public UnaryOperator<ExpressionRef<RelationalExpression>> getCompensationOperator() {
        return compensationOperator;
    }

    @Nonnull
    public Map<CorrelationIdentifier, ComparisonRange> getParameterBindingMap() {
        return parameterBindingMap;
    }

    @Nonnull
    public IdentityBiMap<Quantifier, PartialMatch> getQuantifierPartialMatchMap() {
        return quantifierPartialMatchMap;
    }

    @Nonnull
    public IdentityBiMap<QueryPredicate, QueryPredicate> getPredicateMap() {
        return predicateMap;
    }

    @Nonnull
    public static Optional<MatchWithCompensation> tryFromMatchMap(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return tryMerge(partialMatchMap, ImmutableMap.of(), IdentityBiMap.create(), ImmutableSet.of());
    }

    @Nonnull
    public static Optional<MatchWithCompensation> tryMerge(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                                           @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                                           @Nonnull final IdentityBiMap<QueryPredicate, QueryPredicate> predicateMap,
                                                           @Nonnull final Set<QueryPredicate> needCompensationPredicates) {
        final ImmutableList.Builder<UnaryOperator<ExpressionRef<RelationalExpression>>> compensationOperatorsBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Map<CorrelationIdentifier, ComparisonRange>> parameterMapsBuilder = ImmutableList.builder();

        final Collection<MatchWithCompensation> matchWithCompensations = PartialMatch.matchesFromMap(partialMatchMap);

        matchWithCompensations.forEach(matchWithCompensation -> {
            compensationOperatorsBuilder.add(matchWithCompensation.getCompensationOperator());
            parameterMapsBuilder.add(matchWithCompensation.getParameterBindingMap());
        });

        parameterMapsBuilder.add(parameterBindingMap);

        final UnaryOperator<ExpressionRef<RelationalExpression>> compensationOperator =
                applySequentially(compensationOperatorsBuilder.build());
        final Optional<Map<CorrelationIdentifier, ComparisonRange>> mergedParameterBindingsOptional =
                tryMergeParameterBindings(parameterMapsBuilder.build());
        return mergedParameterBindingsOptional
                .map(mergedParameterBindings -> new MatchWithCompensation(compensationOperator,
                        mergedParameterBindings,
                        partialMatchMap,
                        predicateMap,
                        needCompensationPredicates));
    }

    @Nonnull
    public static Optional<MatchWithCompensation> tryFromMatchMapWithAllPredicates(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap, @Nonnull Collection<QueryPredicate> predicates) {
        final IdentityBiMap<QueryPredicate, QueryPredicate> predicatesMap = IdentityBiMap.create();
        predicates.forEach(queryPredicate -> predicatesMap.putUnwrapped(queryPredicate, queryPredicate));
        return tryMerge(partialMatchMap, ImmutableMap.of(), predicatesMap.toImmutable(), ImmutableSet.of());
    }

    public static Optional<Map<CorrelationIdentifier, ComparisonRange>> tryMergeParameterBindings(final Collection<Map<CorrelationIdentifier, ComparisonRange>> parameterBindingMaps) {
        final Map<CorrelationIdentifier, ComparisonRange> resultMap = Maps.newHashMap();

        for (final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap : parameterBindingMaps) {
            for (final Map.Entry<CorrelationIdentifier, ComparisonRange> entry : parameterBindingMap.entrySet()) {
                if (resultMap.containsKey(entry.getKey())) {
                    if (!resultMap.get(entry.getKey()).equals(entry.getValue())) {
                        return Optional.empty();
                    }
                } else {
                    resultMap.put(entry.getKey(), entry.getValue());
                }
            }
        }

        return Optional.of(resultMap);
    }

    private static UnaryOperator<ExpressionRef<RelationalExpression>> applySequentially(final Collection<UnaryOperator<ExpressionRef<RelationalExpression>>> compensationOperators) {
        final Iterator<UnaryOperator<ExpressionRef<RelationalExpression>>> iterator = compensationOperators.iterator();

        if (!iterator.hasNext()) {
            return UnaryOperator.identity();
        }

        UnaryOperator<ExpressionRef<RelationalExpression>> result = iterator.next();
        while (iterator.hasNext()) {
            final UnaryOperator<ExpressionRef<RelationalExpression>> next = iterator.next();
            result = chainCompensations(result, next);
        }
        return result;
    }

    private static UnaryOperator<ExpressionRef<RelationalExpression>> chainCompensations(final UnaryOperator<ExpressionRef<RelationalExpression>> first, final UnaryOperator<ExpressionRef<RelationalExpression>> second) {
        return e -> second.apply(first.apply(e));
    }
}
