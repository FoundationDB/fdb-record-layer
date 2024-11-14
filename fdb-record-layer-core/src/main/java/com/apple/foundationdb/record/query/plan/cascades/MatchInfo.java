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
public interface MatchInfo {
    @Nonnull
    List<MatchedOrderingPart> getMatchedOrderingParts();

    @Nonnull
    MaxMatchMap getMaxMatchMap();

    boolean isAdjusted();

    default boolean isRegular() {
        return !isAdjusted();
    }

    @Nonnull
    RegularMatchInfo getRegularMatchInfo();

    @Nonnull
    default AdjustedBuilder adjustedBuilder() {
        return new AdjustedBuilder(this,
                getMatchedOrderingParts(),
                getMaxMatchMap());
    }

    /**
     * Implementation of {@link MatchInfo} that represents a match between two expressions.
     */
    class RegularMatchInfo implements MatchInfo {
        /**
         * Parameter bindings for this match.
         */
        @Nonnull
        private final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap;

        @Nonnull
        private final AliasMap bindingAliasMap;

        @Nonnull
        private final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap;

        @Nonnull
        private final Supplier<Map<CorrelationIdentifier, PartialMatch>> aliasToPartialMatchMapSupplier;

        /**
         * Conjuncts the constraints from the predicate map into a single {@link QueryPlanConstraint}.
         */
        @Nonnull
        private final Supplier<QueryPlanConstraint> constraintsSupplier;

        @Nonnull
        private final PredicateMultiMap predicateMap;

        @Nonnull
        private final Supplier<PredicateMap> accumulatedPredicateMapSupplier;

        @Nonnull
        private final List<MatchedOrderingPart> matchedOrderingParts;

        /**
         * A map of maximum matches between the query result {@code Value} and the corresponding candidate's result
         * {@code Value}.
         */
        @Nonnull
        private final MaxMatchMap maxMatchMap;

        /**
         * Field to hold additional query plan constraints that need to be imposed on the potentially realized match.
         */
        @Nonnull
        private final QueryPlanConstraint additionalPlanConstraint;

        private RegularMatchInfo(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                 @Nonnull final AliasMap bindingAliasMap,
                                 @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                 @Nonnull final PredicateMultiMap predicateMap,
                                 @Nonnull final List<MatchedOrderingPart> matchedOrderingParts,
                                 @Nonnull final MaxMatchMap maxMatchMap,
                                 @Nonnull final QueryPlanConstraint additionalPlanConstraint) {
            this.parameterBindingMap = ImmutableMap.copyOf(parameterBindingMap);
            this.bindingAliasMap = bindingAliasMap;
            this.partialMatchMap = partialMatchMap.toImmutable();
            this.aliasToPartialMatchMapSupplier = Suppliers.memoize(() -> {
                final ImmutableMap.Builder<CorrelationIdentifier, PartialMatch> mapBuilder = ImmutableMap.builder();
                partialMatchMap.forEachUnwrapped(((quantifier, partialMatch) -> mapBuilder.put(quantifier.getAlias(), partialMatch)));
                return mapBuilder.build();
            });
            this.constraintsSupplier = Suppliers.memoize(this::computeConstraints);
            this.predicateMap = predicateMap;
            this.accumulatedPredicateMapSupplier = Suppliers.memoize(() -> {
                final PredicateMap.Builder targetBuilder = PredicateMap.builder();
                collectPredicateMappings(targetBuilder);
                return targetBuilder.build();
            });

            this.matchedOrderingParts = ImmutableList.copyOf(matchedOrderingParts);
            this.maxMatchMap = maxMatchMap;
            this.additionalPlanConstraint = additionalPlanConstraint;
        }

        @Nonnull
        public Map<CorrelationIdentifier, ComparisonRange> getParameterBindingMap() {
            return parameterBindingMap;
        }

        @Nonnull
        public AliasMap getBindingAliasMap() {
            return bindingAliasMap;
        }

        @Nonnull
        public IdentityBiMap<Quantifier, PartialMatch> getPartialMatchMap() {
            return partialMatchMap;
        }

        @Nonnull
        public Optional<PartialMatch> getChildPartialMatchMaybe(@Nonnull final Quantifier quantifier) {
            return Optional.ofNullable(partialMatchMap.getUnwrapped(quantifier));
        }

        @Nonnull
        public Optional<PartialMatch> getChildPartialMatchMaybe(@Nonnull final CorrelationIdentifier alias) {
            return Optional.ofNullable(aliasToPartialMatchMapSupplier.get().get(alias));
        }

        @Nonnull
        public PredicateMultiMap getPredicateMap() {
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
        private void collectPredicateMappings(@Nonnull PredicateMultiMap.Builder targetBuilder) {
            targetBuilder.putAll(predicateMap);

            for (final Equivalence.Wrapper<PartialMatch> partialMatchWrapper : partialMatchMap.values()) {
                final PartialMatch partialMatch = Objects.requireNonNull(partialMatchWrapper.get());
                partialMatch.getRegularMatchInfo().collectPredicateMappings(targetBuilder);
            }
        }

        @Nonnull
        public List<MatchedOrderingPart> getMatchedOrderingParts() {
            return matchedOrderingParts;
        }

        @Nonnull
        public MaxMatchMap getMaxMatchMap() {
            return maxMatchMap;
        }

        @Nonnull
        public QueryPlanConstraint getAdditionalPlanConstraint() {
            return additionalPlanConstraint;
        }

        @Override
        public boolean isAdjusted() {
            return false;
        }

        @Nonnull
        @Override
        public RegularMatchInfo getRegularMatchInfo() {
            return this;
        }

        @Nonnull
        private QueryPlanConstraint computeConstraints() {
            final var childConstraints = partialMatchMap.values().stream().map(
                    partialMatch -> partialMatch.get().getRegularMatchInfo().getConstraint()).collect(Collectors.toList());
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
        public static Optional<MatchInfo> tryFromMatchMap(@Nonnull final AliasMap bindingAliasMap,
                                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                                          @Nonnull final MaxMatchMap maxMatchMap) {
            return tryMerge(bindingAliasMap, partialMatchMap, ImmutableMap.of(), PredicateMap.empty(),
                    maxMatchMap, maxMatchMap.getQueryPlanConstraint());
        }

        @Nonnull
        public static Optional<MatchInfo> tryMerge(@Nonnull final AliasMap bindingAliasMap,
                                                   @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap,
                                                   @Nonnull final PredicateMultiMap predicateMap,
                                                   @Nonnull final MaxMatchMap maxMatchMap,
                                                   @Nonnull final QueryPlanConstraint additionalPlanConstraint) {
            final var parameterMapsBuilder = ImmutableList.<Map<CorrelationIdentifier, ComparisonRange>>builder();
            final var matchInfos = PartialMatch.matchInfosFromMap(partialMatchMap);

            matchInfos.forEach(matchInfo -> parameterMapsBuilder.add(matchInfo.getRegularMatchInfo().getParameterBindingMap()));
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

            return mergedParameterBindingsOptional
                    .map(mergedParameterBindings -> new RegularMatchInfo(mergedParameterBindings,
                            bindingAliasMap,
                            partialMatchMap,
                            predicateMap,
                            orderingParts,
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

    /**
     * A {@link MatchInfo} that is based on another underlying {@link MatchInfo}.
     */
    class AdjustedMatchInfo implements MatchInfo {
        @Nonnull
        private final MatchInfo underlying;

        @Nonnull
        private final List<MatchedOrderingPart> matchedOrderingParts;

        /**
         * A map of maximum matches between the query result {@code Value} and the corresponding candidate's result
         * {@code Value}.
         */
        @Nonnull
        private final MaxMatchMap maxMatchMap;

        public AdjustedMatchInfo(@Nonnull final MatchInfo underlying,
                                 @Nonnull final List<MatchedOrderingPart> matchedOrderingParts,
                                 @Nonnull final MaxMatchMap maxMatchMap) {
            this.underlying = underlying;
            this.matchedOrderingParts = matchedOrderingParts;
            this.maxMatchMap = maxMatchMap;
        }

        @Nonnull
        public MatchInfo getUnderlying() {
            return underlying;
        }

        @Nonnull
        @Override
        public List<MatchedOrderingPart> getMatchedOrderingParts() {
            return matchedOrderingParts;
        }

        @Nonnull
        @Override
        public MaxMatchMap getMaxMatchMap() {
            return maxMatchMap;
        }

        @Override
        public boolean isAdjusted() {
            return true;
        }

        @Nonnull
        @Override
        public RegularMatchInfo getRegularMatchInfo() {
            return underlying.getRegularMatchInfo();
        }
    }

    /**
     * Builder for a {@link MatchInfo}.
     */
    @SuppressWarnings("unused")
    class AdjustedBuilder {
        @Nonnull
        private final MatchInfo underlying;

        @Nonnull
        private List<MatchedOrderingPart> matchedOrderingParts;

        /**
         * A map of maximum matches between the query result {@code Value} and the corresponding candidate's result
         * {@code Value}.
         */
        @Nonnull
        private MaxMatchMap maxMatchMap;

        private AdjustedBuilder(@Nonnull final MatchInfo underlying,
                                @Nonnull final List<MatchedOrderingPart> matchedOrderingParts,
                                @Nonnull final MaxMatchMap maxMatchMap) {
            this.underlying = underlying;
            this.matchedOrderingParts = matchedOrderingParts;
            this.maxMatchMap = maxMatchMap;
        }

        @Nonnull
        public List<MatchedOrderingPart> getMatchedOrderingParts() {
            return matchedOrderingParts;
        }

        public AdjustedBuilder setMatchedOrderingParts(@Nonnull final List<MatchedOrderingPart> matchedOrderingParts) {
            this.matchedOrderingParts = matchedOrderingParts;
            return this;
        }

        @Nonnull
        public MaxMatchMap getMaxMatchMap() {
            return maxMatchMap;
        }

        public AdjustedBuilder setMaxMatchMap(@Nonnull final MaxMatchMap maxMatchMap) {
            this.maxMatchMap = maxMatchMap;
            return this;
        }

        @Nonnull
        public MatchInfo build() {
            return new AdjustedMatchInfo(
                    underlying,
                    matchedOrderingParts,
                    maxMatchMap);
        }
    }
}
