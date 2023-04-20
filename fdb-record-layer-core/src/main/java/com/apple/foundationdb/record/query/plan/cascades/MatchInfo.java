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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    private final Supplier<Optional<QueryPlanConstraint>> capturedConstraintsSupplier;

    @Nonnull
    private final PredicateMap predicateMap;

    @Nonnull
    private final Supplier<PredicateMap> accumulatedPredicateMapSupplier;

    @Nonnull
    private final List<MatchedOrderingPart> matchedOrderingParts;

    @Nonnull
    private final Optional<Value> remainingComputationValueOptional;

    private MatchInfo(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                      @Nonnull final IdentityBiMap<Quantifier, PartialMatch> quantifierToPartialMatchMap,
                      @Nonnull final PredicateMap predicateMap,
                      @Nonnull final List<MatchedOrderingPart> matchedOrderingParts,
                      @Nonnull final Optional<Value> remainingComputationValueOptional) {
        this.parameterBindingMap = ImmutableMap.copyOf(parameterBindingMap);
        this.quantifierToPartialMatchMap = quantifierToPartialMatchMap.toImmutable();
        this.aliasToPartialMatchMapSupplier = Suppliers.memoize(() -> {
            final ImmutableMap.Builder<CorrelationIdentifier, PartialMatch> mapBuilder = ImmutableMap.builder();
            quantifierToPartialMatchMap.forEachUnwrapped(((quantifier, partialMatch) -> mapBuilder.put(quantifier.getAlias(), partialMatch)));
            return mapBuilder.build();
        });
        this.capturedConstraintsSupplier = Suppliers.memoize(this::capturedConstraintCollectorMaybe);
        this.predicateMap = predicateMap;
        this.accumulatedPredicateMapSupplier = Suppliers.memoize(() -> {
            final PredicateMap.Builder targetBuilder = PredicateMap.builder();
            collectPredicateMappings(targetBuilder);
            return targetBuilder.build();
        });

        this.matchedOrderingParts = ImmutableList.copyOf(matchedOrderingParts);
        this.remainingComputationValueOptional = remainingComputationValueOptional;
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
    public Optional<QueryPlanConstraint> getConstraintMaybe() {
        return capturedConstraintsSupplier.get();
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
    public ImmutableMap<CorrelationIdentifier, QueryPredicate> getParameterPredicateMap() {
        return getAccumulatedPredicateMap()
                .entries()
                .stream()
                .filter(entry -> {
                    final PredicateMultiMap.PredicateMapping predicateMapping = entry.getValue();
                    return predicateMapping.getParameterAliasOptional().isPresent();
                })
                .collect(ImmutableMap.toImmutableMap(entry -> {
                    final PredicateMultiMap.PredicateMapping predicateMapping = entry.getValue();
                    return Objects.requireNonNull(predicateMapping
                            .getParameterAliasOptional()
                            .orElseThrow(() -> new RecordCoreException("parameter alias should have been set")));
                }, entry -> Objects.requireNonNull(entry.getKey())));
    }

    @Nonnull
    public List<MatchedOrderingPart> getMatchedOrderingParts() {
        return matchedOrderingParts;
    }

    @Nonnull
    public Optional<Value> getRemainingComputationValueOptional() {
        return remainingComputationValueOptional;
    }

    @Nullable
    public QueryPredicate getCandidatePredicateForMatchedOrderingPart(final MatchedOrderingPart matchedOrderingPart) {
        if (matchedOrderingPart.getQueryPredicate() == null) {
            Verify.verify(matchedOrderingPart.getComparisonRangeType() == ComparisonRange.Type.EMPTY);
            return null;
        }

        return getAccumulatedPredicateMap()
                .getMappingOptional(matchedOrderingPart.getQueryPredicate())
                .map(PredicateMultiMap.PredicateMapping::getCandidatePredicate)
                .orElseThrow(() -> new IllegalStateException("mapping must be present"));
    }

    public Optional<CorrelationIdentifier> getParameterAliasForMatchedOrderingPart(final MatchedOrderingPart matchedOrderingPart) {
        @Nullable final var candidatePredicate = getCandidatePredicateForMatchedOrderingPart(matchedOrderingPart);
        if (candidatePredicate == null) {
            return Optional.empty();
        }

        if (!(candidatePredicate instanceof Placeholder)) {
            return Optional.empty();
        }

        return Optional.of(((Placeholder)candidatePredicate).getParameterAlias());
    }

    /**
     * Derive if a scan is reverse by looking at all the bound key parts in this match info. The planner structures
     * are laid out in a way that they could theoretically support a scan direction by key part. In reality, we only
     * support a direction for a scan. Consequently, we only allow that either none or all of the key parts indicate
     * {@link OrderingPart#isReverse()}.
     * @return {@code Optional.of(false)} if all bound key parts indicate a forward ordering,
     *         {@code Optional.of(true)} if all bound key parts indicate a reverse ordering,
     *         {@code Optional.empty()} otherwise. The caller should deal with that result accordingly.
     *         Note that if there are no bound key parts at all, this method will return {@code Optional.of(false)}.
     */
    @Nonnull
    public Optional<Boolean> deriveReverseScanOrder() {
        var numReverse  = 0;
        for (var orderingPart : matchedOrderingParts) {
            if (orderingPart.isReverse()) {
                numReverse ++;
            }
        }

        if (numReverse == 0) {
            return Optional.of(false); // forward
        } else if (numReverse == matchedOrderingParts.size()) {
            return Optional.of(true); // reverse
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    public MatchInfo withOrderingInfo(@Nonnull final List<MatchedOrderingPart> matchedOrderingParts) {
        return new MatchInfo(parameterBindingMap,
                quantifierToPartialMatchMap,
                predicateMap,
                matchedOrderingParts,
                remainingComputationValueOptional);
    }

    @Nonnull
    private Optional<QueryPlanConstraint> capturedConstraintCollectorMaybe() {
        final var childConstraints = quantifierToPartialMatchMap.values().stream().map(
                partialMatch -> partialMatch.get().getMatchInfo().capturedConstraintCollectorMaybe()).flatMap(Optional::stream).collect(Collectors.toList());
        final var constraints = predicateMap.getMap()
                .values()
                .stream()
                .flatMap(predicate -> predicate.getConstraint().stream())
                .collect(Collectors.toUnmodifiableList());
        if (constraints.isEmpty() && childConstraints.isEmpty()) {
            return Optional.empty();
        }
        final var allConstraints = ImmutableList.<QueryPlanConstraint>builder().addAll(constraints).addAll(childConstraints).build();
        return Optional.of(QueryPlanConstraint.compose(allConstraints));
    }

    @Nonnull
    public static Optional<MatchInfo> tryFromMatchMap(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return tryMerge(partialMatchMap, ImmutableMap.of(), PredicateMap.empty(), Optional.empty());
    }

    @Nonnull
    public static Optional<MatchInfo> tryMerge(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                               @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                               @Nonnull final PredicateMap predicateMap,
                                               @Nonnull Optional<Value> remainingComputationValueOptional) {
        final var parameterMapsBuilder = ImmutableList.<Map<CorrelationIdentifier, ComparisonRange>>builder();
        final var matchInfos = PartialMatch.matchesFromMap(partialMatchMap);

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
                        orderingParts,
                        remainingComputationValueOptional));
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
}
