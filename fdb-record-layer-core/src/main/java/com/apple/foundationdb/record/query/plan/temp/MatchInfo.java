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

package com.apple.foundationdb.record.query.plan.temp;

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
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class represents the result of matching one expression against a candidate.
 */
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

    @Nonnull
    private final PredicateMap predicateMap;

    @Nonnull
    private final Supplier<PredicateMap> accumulatedPredicateMapSupplier;

    @Nonnull
    private final List<BoundKeyPart> boundKeyParts;

    private final boolean isReverse;

    private MatchInfo(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                      @Nonnull final IdentityBiMap<Quantifier, PartialMatch> quantifierToPartialMatchMap,
                      @Nonnull final PredicateMap predicateMap,
                      @Nonnull final List<BoundKeyPart> boundKeyParts,
                      final boolean isReverse) {
        this.parameterBindingMap = ImmutableMap.copyOf(parameterBindingMap);
        this.quantifierToPartialMatchMap = quantifierToPartialMatchMap.toImmutable();
        this.aliasToPartialMatchMapSupplier = Suppliers.memoize(() -> {
            final ImmutableMap.Builder<CorrelationIdentifier, PartialMatch> mapBuilder = ImmutableMap.builder();
            quantifierToPartialMatchMap.forEachUnwrapped(((quantifier, partialMatch) -> mapBuilder.put(quantifier.getAlias(), partialMatch)));
            return mapBuilder.build();
        });
        this.predicateMap = predicateMap;
        this.accumulatedPredicateMapSupplier = Suppliers.memoize(() -> {
            final PredicateMap.Builder targetBuilder = PredicateMap.builder();
            collectPredicateMappings(targetBuilder);
            return targetBuilder.build();
        });

        this.boundKeyParts = ImmutableList.copyOf(boundKeyParts);
        this.isReverse = isReverse;
    }

    @Nonnull
    public Map<CorrelationIdentifier, ComparisonRange> getParameterBindingMap() {
        return parameterBindingMap;
    }

    @Nonnull
    public List<PartialMatch> getChildPartialMatches() {
        return quantifierToPartialMatchMap.values()
                .stream().map(Equivalence.Wrapper::get)
                .collect(ImmutableList.toImmutableList());
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
    public PredicateMap getAccumulatedPredicateMap() {
        return accumulatedPredicateMapSupplier.get();
    }

    private void collectPredicateMappings(@Nonnull PredicateMap.Builder targetBuilder) {
        targetBuilder.putAll(predicateMap);

        for (final Equivalence.Wrapper<PartialMatch> partialMatchWrapper : quantifierToPartialMatchMap.values()) {
            final PartialMatch partialMatch = Objects.requireNonNull(partialMatchWrapper.get());
            partialMatch.getMatchInfo().collectPredicateMappings(targetBuilder);
        }
    }

    @Nonnull
    public List<BoundKeyPart> getBoundKeyParts() {
        return boundKeyParts;
    }

    public boolean isReverse() {
        return isReverse;
    }

    public MatchInfo withOrderingInfo(@Nonnull final List<BoundKeyPart> boundKeyParts,
                                      final boolean isReverse) {
        return new MatchInfo(parameterBindingMap,
                quantifierToPartialMatchMap,
                predicateMap,
                boundKeyParts,
                isReverse);
    }

    @Nonnull
    public static Optional<MatchInfo> tryFromMatchMap(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return tryMerge(partialMatchMap, ImmutableMap.of(), PredicateMap.empty());
    }

    @Nonnull
    public static Optional<MatchInfo> tryMerge(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                               @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                               @Nonnull final PredicateMap predicateMap) {
        final ImmutableList.Builder<Map<CorrelationIdentifier, ComparisonRange>> parameterMapsBuilder = ImmutableList.builder();

        final Collection<MatchInfo> matchInfos = PartialMatch.matchesFromMap(partialMatchMap);

        matchInfos.forEach(matchInfo -> parameterMapsBuilder.add(matchInfo.getParameterBindingMap()));
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
            boundKeyParts = partialMatch.getMatchInfo().getBoundKeyParts();
            isReverse = partialMatch.getMatchInfo().isReverse();
        } else {
            boundKeyParts = ImmutableList.of();
            isReverse = false;
        }

        final Optional<Map<CorrelationIdentifier, ComparisonRange>> mergedParameterBindingsOptional =
                tryMergeParameterBindings(parameterMapsBuilder.build());
        return mergedParameterBindingsOptional
                .map(mergedParameterBindings -> new MatchInfo(mergedParameterBindings,
                        partialMatchMap,
                        predicateMap,
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
}
