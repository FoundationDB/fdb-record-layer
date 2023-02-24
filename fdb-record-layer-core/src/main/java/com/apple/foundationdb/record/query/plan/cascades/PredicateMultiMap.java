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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueWithRanges;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
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
        CompensatePredicateFunction UNDEFINED =
                (partialMatch, boundPrefixMap) -> {
                    throw new RecordCoreException("should not be called");
                };

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
        GraphExpansion applyCompensation(@Nonnull TranslationMap translationMap);
    }

    /**
     * Mapping class.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class PredicateMapping {
        /**
         * Classification for mapping.
         */
        public enum Kind {
            MAPPING(true),
            CORRELATED(false),
            UNCORRELATED(false);

            private final boolean hasMapping;

            Kind(final boolean hasMapping) {
                this.hasMapping = hasMapping;
            }

            public boolean hasMapping() {
                return hasMapping;
            }
        }

        @Nonnull
        private final Kind mappingKind;
        @Nonnull
        private final QueryPredicate queryPredicate;
        @Nonnull
        private final Optional<QueryPredicate> candidatePredicateOptional;
        @Nonnull
        private final CompensatePredicateFunction compensatePredicateFunction;
        @Nonnull
        private final Optional<CorrelationIdentifier> parameterAliasOptional;

        @Nonnull
        private final Optional<QueryPlanConstraint> constraint;

        public PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                @Nonnull final QueryPredicate candidatePredicate,
                                @Nonnull final CompensatePredicateFunction compensatePredicateFunction) {
            this(Kind.MAPPING, queryPredicate, Optional.of(candidatePredicate), compensatePredicateFunction, Optional.empty(), Optional.empty());
        }

        public PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                @Nonnull final QueryPredicate candidatePredicate,
                                @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                @Nonnull final CorrelationIdentifier parameterAlias) {
            this(Kind.MAPPING, queryPredicate, Optional.of(candidatePredicate), compensatePredicateFunction, Optional.of(parameterAlias), Optional.empty());
        }

        public PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                @Nonnull final QueryPredicate candidatePredicate,
                                @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                @Nonnull final Optional<CorrelationIdentifier> parameterAlias) {
            this(Kind.MAPPING, queryPredicate, Optional.of(candidatePredicate), compensatePredicateFunction, parameterAlias, Optional.empty());
        }

        public PredicateMapping(@Nonnull final QueryPredicate queryPredicate,
                                @Nonnull final QueryPredicate candidatePredicate,
                                @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                @Nonnull final Optional<CorrelationIdentifier> parameterAlias,
                                @Nonnull final Optional<QueryPlanConstraint> constraint) {
            this(Kind.MAPPING, queryPredicate, Optional.of(candidatePredicate), compensatePredicateFunction, parameterAlias, constraint);
        }

        private PredicateMapping(@Nonnull final Kind mappingKind,
                                 @Nonnull final QueryPredicate queryPredicate,
                                 @Nonnull final Optional<QueryPredicate> candidatePredicateOptional,
                                 @Nonnull final CompensatePredicateFunction compensatePredicateFunction,
                                 @Nonnull final Optional<CorrelationIdentifier> parameterAlias,
                                 @Nonnull final Optional<QueryPlanConstraint> constraint) {
            Verify.verify(mappingKind.hasMapping == candidatePredicateOptional.isPresent());
            this.mappingKind = mappingKind;
            this.queryPredicate = queryPredicate;
            this.candidatePredicateOptional = candidatePredicateOptional;
            this.compensatePredicateFunction = compensatePredicateFunction;
            this.parameterAliasOptional = parameterAlias;
            this.constraint = constraint;
        }

        @Nonnull
        public Kind getMappingKind() {
            return mappingKind;
        }

        public boolean hasMapping() {
            return candidatePredicateOptional.isPresent();
        }

        @Nonnull
        public QueryPredicate getQueryPredicate() {
            return queryPredicate;
        }

        @Nonnull
        public QueryPredicate getCandidatePredicate() {
            return candidatePredicateOptional.orElseThrow(() -> new IllegalArgumentException("check if mapping is mapped to candidate predicate first"));
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
            if (parameterAliasOptional.isEmpty() || !(queryPredicate instanceof ValueWithRanges && ((ValueWithRanges)queryPredicate).isSargable())) {
                return Optional.empty();
            }
            final var predicateConjunctionPredicate = (ValueWithRanges)this.queryPredicate;
            return Optional.of(Iterables.getOnlyElement(predicateConjunctionPredicate.getRanges()).asComparisonRange());
        }

        @Nonnull
        public Optional<QueryPlanConstraint> getConstraint() {
            return constraint;
        }

        @Nonnull
        public static PredicateMapping noMappingUncorrelated(@Nonnull final QueryPredicate queryPredicate) {
            return noMappingWithKind(Kind.UNCORRELATED, queryPredicate);
        }

        @Nonnull
        public static PredicateMapping noMappingCorrelated(@Nonnull final QueryPredicate queryPredicate) {
            return noMappingWithKind(Kind.CORRELATED, queryPredicate);
        }

        @Nonnull
        private static PredicateMapping noMappingWithKind(@Nonnull final Kind mappingKind, @Nonnull final QueryPredicate queryPredicate) {
            Verify.verify(!mappingKind.hasMapping());
            return new PredicateMapping(mappingKind, queryPredicate, Optional.empty(), CompensatePredicateFunction.UNDEFINED, Optional.empty(), Optional.empty());
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

    public Set<QueryPredicate> keySet() {
        return map.keySet();
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
