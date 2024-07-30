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

import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Map that maps from a {@link QueryPredicate} of a query to a {@link QueryPredicate} of a {@link MatchCandidate}.
 * Each mapping itself has other pieces of information associated with it:
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
        CompensatePredicateFunction NO_COMPENSATION_NEEDED =
                (partialMatch, boundPrefixMap) -> Optional.empty();
        
        @Nonnull
        Optional<ExpandCompensationFunction> injectCompensationFunctionMaybe(@Nonnull PartialMatch partialMatch,
                                                                             @Nonnull Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap);

        @Nonnull
        static CompensatePredicateFunction noCompensationNeeded() {
            return NO_COMPENSATION_NEEDED;
        }
    }

    /**
     * Functional interface to reapply a predicate if necessary.
     */
    @FunctionalInterface
    public interface ExpandCompensationFunction {
        @Nonnull
        Set<QueryPredicate> applyCompensationForPredicate(@Nonnull TranslationMap translationMap);
    }

    /**
     * Mapping class.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class PredicateMapping {

        /**
         * Kind of mapping.
         */
        public enum MappingKind {
            REGULAR_IMPLIES_CANDIDATE,
            OR_TERM_IMPLIES_CANDIDATE
        }

        @Nonnull
        private final MappingKey mappingKey;
        @Nonnull
        private final CompensatePredicateFunction compensatePredicateFunction;
        @Nonnull
        private final Optional<CorrelationIdentifier> parameterAliasOptional;
        @Nonnull
        private final QueryPlanConstraint constraint;

        @Nonnull
        private final Optional<QueryPredicate> translatedQueryPredicateOptional;

        private PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                 @Nonnull final QueryPredicate candidatePredicate,
                                 @Nonnull final MappingKind mappingKind,
                                 @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                 @Nonnull final Optional<CorrelationIdentifier> parameterAlias,
                                 @Nonnull final QueryPlanConstraint constraint,
                                 @Nonnull final Optional<QueryPredicate> translatedQueryPredicateOptional) {
            this.mappingKey = new MappingKey(queryPredicate, candidatePredicate, mappingKind);
            this.compensatePredicateFunction = compensatePredicateFunction;
            this.parameterAliasOptional = parameterAlias;
            this.constraint = constraint;
            this.translatedQueryPredicateOptional = translatedQueryPredicateOptional;
        }

        @Nonnull
        public QueryPredicate getQueryPredicate() {
            return mappingKey.getQueryPredicate();
        }

        @Nonnull
        public QueryPredicate getCandidatePredicate() {
            return mappingKey.getCandidatePredicate();
        }

        @Nonnull
        public MappingKind getMappingKind() {
            return mappingKey.getMappingKind();
        }

        @Nonnull
        public MappingKey getMappingKey() {
            return mappingKey;
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
            final var queryPredicate = getQueryPredicate();
            if (parameterAliasOptional.isEmpty() || !(queryPredicate instanceof PredicateWithValueAndRanges && ((PredicateWithValueAndRanges)queryPredicate).isSargable())) {
                return Optional.empty();
            }
            final var predicateConjunctionPredicate = (PredicateWithValueAndRanges)queryPredicate;
            return Optional.of(Iterables.getOnlyElement(predicateConjunctionPredicate.getRanges()).asComparisonRange());
        }

        @Nonnull
        public QueryPlanConstraint getConstraint() {
            return constraint;
        }

        @Nonnull
        public Optional<QueryPredicate> getTranslatedQueryPredicate() {
            return translatedQueryPredicateOptional;
        }

        @Nonnull
        public PredicateMapping withTranslatedQueryPredicate(@Nonnull final Optional<QueryPredicate> translatedCandidatePredicate) {
            return new PredicateMapping(getQueryPredicate(), getCandidatePredicate(), getMappingKind(), compensatePredicateFunction, parameterAliasOptional, constraint, translatedCandidatePredicate);
        }

        @Nonnull
        public static PredicateMapping regularMappingWithoutCompensation(@Nonnull final QueryPredicate queryPredicate,
                                                                         @Nonnull final QueryPredicate candidatePredicate,
                                                                         @Nonnull final QueryPlanConstraint constraint) {
            return regularMapping(queryPredicate, candidatePredicate, CompensatePredicateFunction.noCompensationNeeded(),
                    Optional.empty(), constraint, Optional.empty());
        }

        @Nonnull
        public static PredicateMapping regularMapping(@Nonnull final QueryPredicate queryPredicate,
                                                      @Nonnull final QueryPredicate candidatePredicate,
                                                      @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                                      @Nonnull final Optional<QueryPredicate> translatedQueryPredicate) {
            return regularMapping(queryPredicate, candidatePredicate, compensatePredicateFunction,
                    Optional.empty(), QueryPlanConstraint.tautology(), translatedQueryPredicate);
        }

        @Nonnull
        public static PredicateMapping regularMapping(@Nonnull final QueryPredicate queryPredicate,
                                                      @Nonnull final QueryPredicate candidatePredicate,
                                                      @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                                      @Nonnull final Optional<CorrelationIdentifier> parameterAliasOptional,
                                                      @Nonnull final QueryPlanConstraint constraint,
                                                      @Nonnull final Optional<QueryPredicate> translatedQueryPredicate) {
            return new PredicateMapping(queryPredicate, candidatePredicate, MappingKind.REGULAR_IMPLIES_CANDIDATE,
                    compensatePredicateFunction, parameterAliasOptional, constraint, translatedQueryPredicate);
        }

        @Nonnull
        public static PredicateMapping orTermMapping(@Nonnull final QueryPredicate queryPredicate,
                                                     @Nonnull final QueryPredicate candidatePredicate,
                                                     @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                                     @Nonnull final Optional<QueryPredicate> translatedQueryPredicate) {
            return orTermMapping(queryPredicate, candidatePredicate, compensatePredicateFunction,
                    Optional.empty(), QueryPlanConstraint.tautology(), translatedQueryPredicate);
        }

        @Nonnull
        public static PredicateMapping orTermMapping(@Nonnull final QueryPredicate queryPredicate,
                                                     @Nonnull final QueryPredicate candidatePredicate,
                                                     @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                                     @Nonnull final Optional<CorrelationIdentifier> parameterAliasOptional,
                                                     @Nonnull final QueryPlanConstraint constraint,
                                                     @Nonnull final Optional<QueryPredicate> translatedQueryPredicate) {
            return new PredicateMapping(queryPredicate, candidatePredicate, MappingKind.OR_TERM_IMPLIES_CANDIDATE,
                    compensatePredicateFunction, parameterAliasOptional, constraint, translatedQueryPredicate);
        }

        /**
         * Class to capture the relationship between query predicate and candidate predicate.
         */
        public static class MappingKey {
            @Nonnull
            private final QueryPredicate queryPredicate;
            @Nonnull
            private final QueryPredicate candidatePredicate;
            @Nonnull
            private final MappingKind mappingKind;

            public MappingKey(@Nonnull final QueryPredicate queryPredicate, @Nonnull final QueryPredicate candidatePredicate, @Nonnull final MappingKind mappingKind) {
                this.queryPredicate = queryPredicate;
                this.candidatePredicate = candidatePredicate;
                this.mappingKind = mappingKind;
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
            public MappingKind getMappingKind() {
                return mappingKind;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof MappingKey)) {
                    return false;
                }
                final MappingKey that = (MappingKey)o;
                return Objects.equals(queryPredicate, that.queryPredicate) &&
                       Objects.equals(candidatePredicate, that.candidatePredicate) &&
                       mappingKind == that.mappingKind;
            }

            @Override
            public int hashCode() {
                return Objects.hash(queryPredicate, candidatePredicate, mappingKind);
            }
        }
    }

    protected PredicateMultiMap(@Nonnull final SetMultimap<QueryPredicate, PredicateMapping> map) {
        this.map = ImmutableSetMultimap.copyOf(map);
    }

    @Nonnull
    protected ImmutableSetMultimap<QueryPredicate, PredicateMapping> getMap() {
        return map;
    }

    public Set<PredicateMapping> get(@Nonnull final QueryPredicate queryPredicate) {
        return map.get(queryPredicate);
    }

    public Set<Map.Entry<QueryPredicate, PredicateMapping>> entries() {
        return map.entries();
    }

    public Set<QueryPredicate> keySet() {
        return map.keySet();
    }

    public Collection<PredicateMapping> values() {
        return map.values();
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
            map = Multimaps.newSetMultimap(new LinkedIdentityMap<>(), LinkedIdentitySet::new);
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
