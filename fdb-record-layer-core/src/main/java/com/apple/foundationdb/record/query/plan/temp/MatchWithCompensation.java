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
import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

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
    private final IdentityBiMap<Quantifier, PartialMatch> quantifierToPartialMatchMap;

    @Nonnull
    private final Supplier<Map<CorrelationIdentifier, PartialMatch>> aliasToPartialMatchMapSupplier;

    @Nonnull
    private final IdentityBiMap<QueryPredicate, QueryPredicate> predicateMap;

    @Nonnull
    private final Supplier<IdentityBiMap<QueryPredicate, QueryPredicate>> accumulatedPredicateMapSupplier;

    @Nonnull
    private final Set<QueryPredicate> unmappedPredicates;

    @Nonnull
    private final List<BoundKeyPart> boundKeyParts;

    private final boolean isReverse;

    private MatchWithCompensation(@Nonnull final UnaryOperator<ExpressionRef<RelationalExpression>> compensationOperator,
                                  @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                  @Nonnull final IdentityBiMap<Quantifier, PartialMatch> quantifierToPartialMatchMap,
                                  @Nonnull final IdentityBiMap<QueryPredicate, QueryPredicate> predicateMap,
                                  @Nonnull final Set<QueryPredicate> unmappedPredicates,
                                  @Nonnull final List<BoundKeyPart> boundKeyParts,
                                  final boolean isReverse) {
        this.compensationOperator = compensationOperator;
        this.parameterBindingMap = ImmutableMap.copyOf(parameterBindingMap);
        this.quantifierToPartialMatchMap = quantifierToPartialMatchMap.toImmutable();
        this.aliasToPartialMatchMapSupplier = Suppliers.memoize(() -> {
            final ImmutableMap.Builder<CorrelationIdentifier, PartialMatch> mapBuilder = ImmutableMap.builder();
            quantifierToPartialMatchMap.forEachUnwrapped(((quantifier, partialMatch) -> mapBuilder.put(quantifier.getAlias(), partialMatch)));
            return mapBuilder.build();
        });
        this.predicateMap = predicateMap.toImmutable();
        this.accumulatedPredicateMapSupplier = Suppliers.memoize(() -> {
            final IdentityBiMap<QueryPredicate, QueryPredicate> target = IdentityBiMap.create();
            collectPredicateMappings(target);
            return target;
        });

        this.unmappedPredicates = Sets.newIdentityHashSet();
        this.unmappedPredicates.addAll(unmappedPredicates);
        this.boundKeyParts = ImmutableList.copyOf(boundKeyParts);
        this.isReverse = isReverse;
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
    public Optional<PartialMatch> getChildPartialMatch(@Nonnull final Quantifier quantifier) {
        return Optional.ofNullable(quantifierToPartialMatchMap.getUnwrapped(quantifier));
    }

    @Nonnull
    public Optional<PartialMatch> getChildPartialMatch(@Nonnull final CorrelationIdentifier alias) {
        return Optional.ofNullable(aliasToPartialMatchMapSupplier.get().get(alias));
    }

    @Nonnull
    public IdentityBiMap<QueryPredicate, QueryPredicate> getPredicateMap() {
        return predicateMap;
    }

    @Nonnull
    public IdentityBiMap<QueryPredicate, QueryPredicate> getAccumulatedPredicateMap() {
        return accumulatedPredicateMapSupplier.get();
    }

    private void collectPredicateMappings(@Nonnull IdentityBiMap<QueryPredicate, QueryPredicate> target) {
        target.putAll(predicateMap);

        for (final Equivalence.Wrapper<PartialMatch> partialMatchWrapper : quantifierToPartialMatchMap.values()) {
            final PartialMatch partialMatch = Objects.requireNonNull(partialMatchWrapper.get());
            partialMatch.getMatchWithCompensation().collectPredicateMappings(target);
        }
    }

    @Nonnull
    public Set<QueryPredicate> getUnmappedPredicates() {
        return unmappedPredicates;
    }

    @Nonnull
    public List<BoundKeyPart> getBoundKeyParts() {
        return boundKeyParts;
    }

    public boolean isReverse() {
        return isReverse;
    }

    public MatchWithCompensation withOrderingInfo(@Nonnull final List<BoundKeyPart> boundKeyParts,
                                                  final boolean isReverse) {
        return new MatchWithCompensation(compensationOperator,
                parameterBindingMap,
                quantifierToPartialMatchMap,
                predicateMap,
                unmappedPredicates,
                boundKeyParts,
                isReverse);
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

        final Set<Quantifier> regularQuantifiers = partialMatchMap.keySet()
                .stream()
                .map(Equivalence.Wrapper::get)
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach || quantifier instanceof Quantifier.Physical)
                .collect(Collectors.toCollection(Sets::newIdentityHashSet));

        final List<BoundKeyPart> boundKeyParts;
        final boolean isReverse;
        if (regularQuantifiers.size() == 1) {
            final Quantifier regularQuantifier = Iterables.getOnlyElement(regularQuantifiers);
            final PartialMatch partialMatch = Objects.requireNonNull(partialMatchMap.getUnwrapped(regularQuantifier));
            boundKeyParts = partialMatch.getMatchWithCompensation().getBoundKeyParts();
            isReverse = partialMatch.getMatchWithCompensation().isReverse();
        } else {
            boundKeyParts = ImmutableList.of();
            isReverse = false;
        }

        final UnaryOperator<ExpressionRef<RelationalExpression>> compensationOperator =
                applySequentially(compensationOperatorsBuilder.build());
        final Optional<Map<CorrelationIdentifier, ComparisonRange>> mergedParameterBindingsOptional =
                tryMergeParameterBindings(parameterMapsBuilder.build());
        return mergedParameterBindingsOptional
                .map(mergedParameterBindings -> new MatchWithCompensation(compensationOperator,
                        mergedParameterBindings,
                        partialMatchMap,
                        predicateMap,
                        needCompensationPredicates,
                        boundKeyParts,
                        isReverse));
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
