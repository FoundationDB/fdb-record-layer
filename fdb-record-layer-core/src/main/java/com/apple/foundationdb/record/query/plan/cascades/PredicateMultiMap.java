/*
 * PredicateMultiMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueComparisonRangePredicate.Sargable;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Map that maps from a {@link QueryPredicate} of a query to a {@link QueryPredicate} of a {@link MatchCandidate}.
 * The each mapping itself has other pieces of information associated with it:
 *
 * <ul>
 *     <li> a {@link CompensatePredicateFunction} which can be used to compensate the implied candidate predicate or</li>
 *     <li> a {@link CorrelationIdentifier} which denotes that this mapping binds the respective parameter in
 *          the match candidate </li>
 * </ul>
 */
public class PredicateMultiMap {
    /**
     * Backing multimap.
     */
    @Nonnull
    private final ImmutableSetMultimap<QueryPredicate, PredicateMapping> map;

    /**
     * Functional interface to reapply a predicate if necessary.
     */
    @FunctionalInterface
    public interface CompensatePredicateFunction {
        @Nonnull
        Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull final PartialMatch partialMatch,
                                                                             @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap);
    }

    /**
     * Functional interface to reapply a predicate if necessary.
     */
    @FunctionalInterface
    public interface ExpandCompensationFunction {
        @Nonnull
        GraphExpansion applyCompensation(@Nonnull final TranslationMap translationMap);
    }

    /**
     * Mapping class.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class PredicateMapping {
        @Nonnull
        private final QueryPredicate queryPredicate;
        @Nonnull
        private final QueryPredicate candidatePredicate;
        @Nonnull
        private final CompensatePredicateFunction compensatePredicateFunction;
        @Nonnull
        private final Optional<CorrelationIdentifier> parameterAliasOptional;

        public PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                @Nonnull final QueryPredicate candidatePredicate,
                                @Nonnull final CompensatePredicateFunction compensatePredicateFunction) {
            this(queryPredicate, candidatePredicate, compensatePredicateFunction, Optional.empty());
        }

        public PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                @Nonnull final QueryPredicate candidatePredicate,
                                @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                @Nonnull final CorrelationIdentifier parameterAlias) {
            this(queryPredicate, candidatePredicate, compensatePredicateFunction, Optional.of(parameterAlias));
        }

        private PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                 @Nonnull final QueryPredicate candidatePredicate,
                                 @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                 @Nonnull final Optional<CorrelationIdentifier> parameterAlias) {
            this.queryPredicate = queryPredicate;
            this.candidatePredicate = candidatePredicate;
            this.compensatePredicateFunction = compensatePredicateFunction;
            this.parameterAliasOptional = parameterAlias;
        }

        @Nonnull
        public QueryPredicate getQueryPredicate() {
            return queryPredicate;
        }

        @Nonnull
        public QueryPredicate getCandidatePredicate() {
            return candidatePredicate;
        }

        @Nonnull
        public CompensatePredicateFunction compensatePredicateFunction() {
            return compensatePredicateFunction;
        }

        @Nonnull
        public Optional<CorrelationIdentifier> getParameterAliasOptional() {
            return parameterAliasOptional;
        }

        @NonNull
        public Optional<ComparisonRange> getComparisonRangeOptional() {
            if (!parameterAliasOptional.isPresent() || !(queryPredicate instanceof Sargable)) {
                return Optional.empty();
            }

            final Sargable sargablePredicate = (Sargable)this.queryPredicate;

            return Optional.of(sargablePredicate.getComparisonRange());
        }
    }

    protected PredicateMultiMap(@Nonnull final SetMultimap<QueryPredicate, PredicateMapping> map) {
        this.map = ImmutableSetMultimap.copyOf(map);
    }

    protected ImmutableSetMultimap<QueryPredicate, PredicateMapping> getMap() {
        return map;
    }

    public Set<PredicateMapping> get(@Nonnull final QueryPredicate queryPredicate) {
        return map.get(queryPredicate);
    }

    public Set<Map.Entry<QueryPredicate, PredicateMapping>> entries() {
        return map.entries();
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    private static Optional<SetMultimap<QueryPredicate, PredicateMapping>> checkConflicts(@Nonnull final SetMultimap<QueryPredicate, PredicateMapping> map) {
        final Set<QueryPredicate> seenCandidatePredicates = Sets.newIdentityHashSet();
        for (final QueryPredicate queryPredicate : map.keySet()) {
            final Set<PredicateMapping> candidatePredicateMappings = map.get(queryPredicate);
            for (final PredicateMapping candidatePredicateMapping : candidatePredicateMappings) {
                final QueryPredicate candidatePredicate = candidatePredicateMapping.getCandidatePredicate();
                if (seenCandidatePredicates.contains(candidatePredicate)) {
                    return Optional.empty();
                }
                seenCandidatePredicates.add(candidatePredicate);
            }
        }
        return Optional.of(map);
    }

    /**
     * Builder class for a predicate maps.
     */
    public static class Builder {
        private final SetMultimap<QueryPredicate, PredicateMapping> map;

        public Builder() {
            map = Multimaps.newSetMultimap(Maps.newIdentityHashMap(), Sets::newIdentityHashSet);
        }

        protected SetMultimap<QueryPredicate, PredicateMapping> getMap() {
            return map;
        }

        public boolean put(@Nonnull final QueryPredicate queryPredicate,
                           @Nonnull final QueryPredicate candidatePredicate,
                           @Nonnull final CompensatePredicateFunction compensatePredicateFunction) {
            return put(queryPredicate, new PredicateMapping(queryPredicate, candidatePredicate, compensatePredicateFunction));
        }

        public boolean put(@Nonnull final QueryPredicate queryPredicate,
                           @Nonnull final QueryPredicate candidatePredicate,
                           @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                           @Nonnull final CorrelationIdentifier parameterAlias) {
            return put(queryPredicate, new PredicateMapping(queryPredicate, candidatePredicate, compensatePredicateFunction, parameterAlias));
        }

        public boolean put(@Nonnull final QueryPredicate queryPredicate,
                           @Nonnull final PredicateMapping predicateMapping) {
            return map.put(queryPredicate, predicateMapping);
        }

        public boolean putAll(@Nonnull final PredicateMap otherMap) {
            boolean isModified = false;
            for (final Map.Entry<QueryPredicate, PredicateMapping> entry : otherMap.getMap().entries()) {
                isModified = map.put(entry.getKey(), entry.getValue()) || isModified;
            }

            return isModified;
        }

        public boolean putAll(@Nonnull final QueryPredicate queryPredicate, @Nonnull final Set<PredicateMapping> predicateMappings) {
            boolean isModified = false;
            for (final PredicateMapping predicateMapping : predicateMappings) {
                isModified = map.put(queryPredicate, predicateMapping) || isModified;
            }

            return isModified;
        }

        public Optional<SetMultimap<QueryPredicate, PredicateMapping>> checkCorrectness() {
            return checkConflicts(map);
        }

        public PredicateMultiMap build() {
            return new PredicateMultiMap(checkCorrectness().orElseThrow(() -> new IllegalArgumentException("conflicts in mapping")));
        }

        public Optional<? extends PredicateMultiMap> buildMaybe() {
            return checkCorrectness()
                    .map(PredicateMultiMap::new);
        }
    }
}
