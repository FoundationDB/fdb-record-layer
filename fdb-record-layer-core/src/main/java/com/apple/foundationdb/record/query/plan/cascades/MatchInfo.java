/*
 * MatchInfo.java
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
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class represents the result of matching one expression against a candidate.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class MatchInfo {
    /**
     * Parameter bindings for this match.
     */
    @Nonnull
    private final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap;

    @Nonnull
    private final IdentityBiMap<Quantifier, PartialMatch> quantifierToPartialMatchMap;

    @Nonnull
    private final Supplier<Map<CorrelationIdentifier, PartialMatch>> aliasToPartialMatchMapSupplier;

    /**
     * Conjuncts the constraints from the predicate map into a single {@link QueryPlanConstraint}.
     */
    @Nonnull
    private final Supplier<QueryPlanConstraint> constraintsSupplier;

    @Nonnull
    private final PredicateMap predicateMap;

    @Nonnull
    private final Supplier<PredicateMap> accumulatedPredicateMapSupplier;

    /**
     * This contains all the predicates in the {@code predicateMap} in addition to all predicates that are pulled up
     * from children match info.
     */
    @Nonnull
    private final PredicateMap accumulatedPredicateMap;

    @Nonnull
    private final List<MatchedOrderingPart> matchedOrderingParts;

    @Nonnull
    private final Optional<Value> remainingComputationValueOptional;

    /**
     * A map of maximum matches between the query result {@code Value} and the corresponding candidate's result
     * {@code Value}.
     */
    @Nonnull
    private final Optional<MaxMatchMap> maxMatchMapOptional;

    /**
     * Field to hold additional query plan constraints that need to be imposed on the potentially realized match.
     */
    @Nonnull
    private final QueryPlanConstraint additionalPlanConstraint;

    private MatchInfo(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                      @Nonnull final IdentityBiMap<Quantifier, PartialMatch> quantifierToPartialMatchMap,
                      @Nonnull final PredicateMap predicateMap,
                      @Nonnull final PredicateMap accumulatedPredicateMap,
                      @Nonnull final List<MatchedOrderingPart> matchedOrderingParts,
                      @Nonnull final Optional<Value> remainingComputationValueOptional,
                      @Nonnull final Optional<MaxMatchMap> maxMatchMapOptional,
                      @Nonnull final QueryPlanConstraint additionalPlanConstraint) {
        this.parameterBindingMap = ImmutableMap.copyOf(parameterBindingMap);
        this.quantifierToPartialMatchMap = quantifierToPartialMatchMap.toImmutable();
        this.aliasToPartialMatchMapSupplier = Suppliers.memoize(() -> {
            final ImmutableMap.Builder<CorrelationIdentifier, PartialMatch> mapBuilder = ImmutableMap.builder();
            quantifierToPartialMatchMap.forEachUnwrapped(((quantifier, partialMatch) -> mapBuilder.put(quantifier.getAlias(), partialMatch)));
            return mapBuilder.build();
        });
        this.accumulatedPredicateMap = accumulatedPredicateMap;
        this.constraintsSupplier = Suppliers.memoize(this::computeConstraints);
        this.predicateMap = predicateMap;
        this.accumulatedPredicateMapSupplier = Suppliers.memoize(() -> {
            final PredicateMap.Builder targetBuilder = PredicateMap.builder();
            collectPredicateMappings(targetBuilder);
            return targetBuilder.build();
        });

        this.matchedOrderingParts = ImmutableList.copyOf(matchedOrderingParts);
        this.remainingComputationValueOptional = remainingComputationValueOptional;
        this.maxMatchMapOptional = maxMatchMapOptional;
        this.additionalPlanConstraint = additionalPlanConstraint;
    }

    @Nonnull
    public Map<CorrelationIdentifier, ComparisonRange> getParameterBindingMap() {
        return parameterBindingMap;
    }

    @Nonnull
    public IdentityBiMap<Quantifier, PartialMatch> getQuantifierToPartialMatchMap() {
        return quantifierToPartialMatchMap;
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
    public PredicateMap getPredicateMap() {
        return predicateMap;
    }

    @Nonnull
    public QueryPlanConstraint getConstraint() {
        return constraintsSupplier.get();
    }

    @Nonnull
    public PredicateMap getAccumulatedPredicateMap() {
        return accumulatedPredicateMapSupplier.get();
    }

    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void collectPredicateMappings(@Nonnull PredicateMap.Builder targetBuilder) {
        targetBuilder.putAll(predicateMap);

        for (final Equivalence.Wrapper<PartialMatch> partialMatchWrapper : quantifierToPartialMatchMap.values()) {
            final PartialMatch partialMatch = Objects.requireNonNull(partialMatchWrapper.get());
            partialMatch.getMatchInfo().collectPredicateMappings(targetBuilder);
        }
    }

    @Nonnull
    public List<MatchedOrderingPart> getMatchedOrderingParts() {
        return matchedOrderingParts;
    }

    @Nonnull
    public Optional<Value> getRemainingComputationValueOptional() {
        return remainingComputationValueOptional;
    }

    @Nonnull
    public Optional<MaxMatchMap> getMaxMatchMapOptional() {
        return maxMatchMapOptional;
    }

    @Nonnull
    public QueryPlanConstraint getAdditionalPlanConstraint() {
        return additionalPlanConstraint;
    }

    @Nonnull
    public MatchInfo withOrderingInfo(@Nonnull final List<MatchedOrderingPart> matchedOrderingParts) {
        return new MatchInfo(parameterBindingMap,
                quantifierToPartialMatchMap,
                predicateMap,
                accumulatedPredicateMap,
                matchedOrderingParts,
                remainingComputationValueOptional,
                maxMatchMapOptional,
                additionalPlanConstraint);
    }

    @Nonnull
    private QueryPlanConstraint computeConstraints() {
        final var childConstraints = quantifierToPartialMatchMap.values().stream().map(
                partialMatch -> partialMatch.get().getMatchInfo().getConstraint()).collect(Collectors.toList());
        final var constraints = predicateMap.getMap()
                .values()
                .stream()
                .map(PredicateMultiMap.PredicateMapping::getConstraint)
                .collect(Collectors.toUnmodifiableList());
        final var allConstraints =
                ImmutableList.<QueryPlanConstraint>builder()
                        .addAll(constraints)
                        .addAll(childConstraints)
                        .add(additionalPlanConstraint)
                        .build();
        return QueryPlanConstraint.composeConstraints(allConstraints);
    }

    @Nonnull
    public static Optional<MatchInfo> tryFromMatchMap(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                                      @Nonnull final Optional<MaxMatchMap> maxMatchMap) {
        return tryMerge(partialMatchMap, ImmutableMap.of(), PredicateMap.empty(), PredicateMap.empty(),
                Optional.empty(), maxMatchMap, QueryPlanConstraint.tautology());
    }

    @Nonnull
    public static Optional<MatchInfo> tryMerge(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                               @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                               @Nonnull final PredicateMap predicateMap,
                                               @Nonnull final PredicateMap accumulatedPredicateMap,
                                               @Nonnull final Optional<Value> remainingComputationValueOptional,
                                               @Nonnull final Optional<MaxMatchMap> maxMatchMap,
                                               @Nonnull final QueryPlanConstraint additionalPlanConstraint) {
        final var parameterMapsBuilder = ImmutableList.<Map<CorrelationIdentifier, ComparisonRange>>builder();
        final var matchInfos = PartialMatch.matchInfosFromMap(partialMatchMap);

        matchInfos.forEach(matchInfo -> parameterMapsBuilder.add(matchInfo.getParameterBindingMap()));
        parameterMapsBuilder.add(parameterBindingMap);

        final var regularQuantifiers = partialMatchMap.keySet()
                .stream()
                .map(Equivalence.Wrapper::get)
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach || quantifier instanceof Quantifier.Physical)
                .collect(Collectors.toCollection(Sets::newIdentityHashSet));
        
        final List<MatchedOrderingPart> orderingParts;
        if (regularQuantifiers.size() == 1) {
            final var regularQuantifier = Iterables.getOnlyElement(regularQuantifiers);
            final var partialMatch = Objects.requireNonNull(partialMatchMap.getUnwrapped(regularQuantifier));
            orderingParts = partialMatch.getMatchInfo().getMatchedOrderingParts();
        } else {
            orderingParts = ImmutableList.of();
        }

        final Optional<Map<CorrelationIdentifier, ComparisonRange>> mergedParameterBindingsOptional =
                tryMergeParameterBindings(parameterMapsBuilder.build());

        final var remainingComputations = regularQuantifiers.stream()
                .map(key -> Objects.requireNonNull(partialMatchMap.getUnwrapped(key))) // always guaranteed
                .map(partialMatch -> partialMatch.getMatchInfo().getRemainingComputationValueOptional())
                .filter(Optional::isPresent)
                .collect(ImmutableList.toImmutableList());

        if (!remainingComputations.isEmpty()) {
            // We found a remaining computation among the child matches -> we cannot merge!
            return Optional.empty();
        }

        return mergedParameterBindingsOptional
                .map(mergedParameterBindings -> new MatchInfo(mergedParameterBindings,
                        partialMatchMap,
                        predicateMap,
                        accumulatedPredicateMap,
                        orderingParts,
                        remainingComputationValueOptional,
                        maxMatchMap,
                        additionalPlanConstraint));
    }

    public static Optional<Map<CorrelationIdentifier, ComparisonRange>> tryMergeParameterBindings(final Collection<Map<CorrelationIdentifier, ComparisonRange>> parameterBindingMaps) {
        final Map<CorrelationIdentifier, ComparisonRange> resultMap = Maps.newHashMap();

        for (final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap : parameterBindingMaps) {
            for (final Map.Entry<CorrelationIdentifier, ComparisonRange> entry : parameterBindingMap.entrySet()) {
                if (resultMap.containsKey(entry.getKey())) {
                    // try to merge the comparisons
                    final var mergeResult = resultMap.get(entry.getKey()).merge(entry.getValue());
                    if (mergeResult.getResidualComparisons().isEmpty()) {
                        resultMap.replace(entry.getKey(), mergeResult.getComparisonRange());
                    } else if (!resultMap.get(entry.getKey()).equals(entry.getValue())) {
                        return Optional.empty();
                    }
                } else {
                    resultMap.put(entry.getKey(), entry.getValue());
                }
            }
        }

        return Optional.of(resultMap);
    }
}
