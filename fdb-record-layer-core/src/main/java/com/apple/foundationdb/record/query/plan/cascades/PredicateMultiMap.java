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
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.google.common.collect.ImmutableSetMultimap;
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
import java.util.function.Function;

/**
 * Map that maps from a {@link QueryPredicate} of a query to a {@link QueryPredicate} of a {@link MatchCandidate}.
 * Each mapping itself has other pieces of information associated with it:
 *
 * <ul>
 *     <li> a {@link PredicateCompensation} which can be used to compensate the implied candidate predicate or</li>
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
    public interface PredicateCompensation {
        @Nonnull
        PredicateCompensationFunction computeCompensationFunction(@Nonnull PartialMatch partialMatch,
                                                                  @Nonnull Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                  @Nonnull PullUp pullup);
    }

    /**
     * Functional interface to reapply a predicate if necessary.
     */
    public interface PredicateCompensationFunction {
        PredicateCompensationFunction NO_COMPENSATION_NEEDED =
                new PredicateCompensationFunction() {
                    @Override
                    public boolean isNeeded() {
                        return false;
                    }

                    @Override
                    public boolean isImpossible() {
                        return false;
                    }

                    @Nonnull
                    @Override
                    public Set<QueryPredicate> applyCompensationForPredicate(@Nonnull final CorrelationIdentifier baseAlias) {
                        throw new IllegalArgumentException("this method should not be called");
                    }
                };

        PredicateCompensationFunction IMPOSSIBLE_COMPENSATION =
                new PredicateCompensationFunction() {
                    @Override
                    public boolean isNeeded() {
                        return true;
                    }

                    @Override
                    public boolean isImpossible() {
                        return true;
                    }

                    @Nonnull
                    @Override
                    public Set<QueryPredicate> applyCompensationForPredicate(@Nonnull final CorrelationIdentifier baseAlias) {
                        throw new IllegalArgumentException("this method should not be called");
                    }
                };


        boolean isNeeded();

        boolean isImpossible();

        @Nonnull
        Set<QueryPredicate> applyCompensationForPredicate(@Nonnull CorrelationIdentifier baseAlias);

        @Nonnull
        static PredicateCompensationFunction of(@Nonnull final Function<CorrelationIdentifier, Set<QueryPredicate>> compensationFunction) {
            return new PredicateCompensationFunction() {
                @Override
                public boolean isNeeded() {
                    return true;
                }

                @Override
                public boolean isImpossible() {
                    return false;
                }

                @Nonnull
                @Override
                public Set<QueryPredicate> applyCompensationForPredicate(@Nonnull final CorrelationIdentifier baseAlias) {
                    return compensationFunction.apply(baseAlias);
                }
            };
        }

        @Nonnull
        static PredicateCompensationFunction noCompensationNeeded() {
            return NO_COMPENSATION_NEEDED;
        }

        @Nonnull
        static PredicateCompensationFunction impossibleCompensation() {
            return IMPOSSIBLE_COMPENSATION;
        }
    }

    /**
     * Functional interface to finally adjust the shape of the records returned by the index/match.
     */
    public interface ResultCompensationFunction {
        ResultCompensationFunction NO_COMPENSATION_NEEDED =
                new ResultCompensationFunction() {
                    @Override
                    public boolean isNeeded() {
                        return false;
                    }

                    @Override
                    public boolean isImpossible() {
                        return false;
                    }

                    @Nonnull
                    @Override
                    public Value applyCompensationForResult(@Nonnull final CorrelationIdentifier baseAlias) {
                        throw new IllegalArgumentException("this method should not be called");
                    }
                };

        ResultCompensationFunction IMPOSSIBLE_COMPENSATION =
                new ResultCompensationFunction() {
                    @Override
                    public boolean isNeeded() {
                        return true;
                    }

                    @Override
                    public boolean isImpossible() {
                        return true;
                    }

                    @Nonnull
                    @Override
                    public Value applyCompensationForResult(@Nonnull final CorrelationIdentifier baseAlias) {
                        throw new IllegalArgumentException("this method should not be called");
                    }
                };


        boolean isNeeded();

        boolean isImpossible();

        @Nonnull
        Value applyCompensationForResult(@Nonnull CorrelationIdentifier baseAlias);

        @Nonnull
        static ResultCompensationFunction of(@Nonnull final Function<CorrelationIdentifier, Value> compensationFunction) {
            return new ResultCompensationFunction() {
                @Override
                public boolean isNeeded() {
                    return true;
                }

                @Override
                public boolean isImpossible() {
                    return false;
                }

                @Nonnull
                @Override
                public Value applyCompensationForResult(@Nonnull final CorrelationIdentifier baseAlias) {
                    return compensationFunction.apply(baseAlias);
                }
            };
        }

        @Nonnull
        static ResultCompensationFunction noCompensationNeeded() {
            return NO_COMPENSATION_NEEDED;
        }

        @Nonnull
        static ResultCompensationFunction impossibleCompensation() {
            return IMPOSSIBLE_COMPENSATION;
        }
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
        private final PredicateCompensation predicateCompensation;
        @Nonnull
        private final Optional<CorrelationIdentifier> parameterAliasOptional;
        @Nonnull
        private final Optional<ComparisonRange> comparisonRangeOptional;
        @Nonnull
        private final QueryPlanConstraint constraint;
        @Nonnull
        private final QueryPredicate translatedQueryPredicate;

        private PredicateMapping(@Nonnull final QueryPredicate originalQueryPredicate,
                                 @Nonnull final QueryPredicate candidatePredicate,
                                 @Nonnull final MappingKind mappingKind,
                                 @Nonnull final PredicateCompensation predicateCompensation,
                                 @Nonnull final Optional<CorrelationIdentifier> parameterAlias,
                                 @Nonnull final Optional<ComparisonRange> comparisonRangeOptional,
                                 @Nonnull final QueryPlanConstraint constraint,
                                 @Nonnull final QueryPredicate translatedQueryPredicate) {
            this.mappingKey = new MappingKey(originalQueryPredicate, candidatePredicate, mappingKind);
            this.predicateCompensation = predicateCompensation;
            this.parameterAliasOptional = parameterAlias;
            this.comparisonRangeOptional = comparisonRangeOptional;
            this.constraint = constraint;
            this.translatedQueryPredicate = translatedQueryPredicate;
        }

        @Nonnull
        public QueryPredicate getOriginalQueryPredicate() {
            return mappingKey.getOriginalQueryPredicate();
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
        public PredicateCompensation getPredicateCompensation() {
            return predicateCompensation;
        }

        @Nonnull
        public Optional<CorrelationIdentifier> getParameterAliasOptional() {
            return parameterAliasOptional;
        }

        @NonNull
        public Optional<ComparisonRange> getComparisonRangeOptional() {
            return comparisonRangeOptional;
        }

        @Nonnull
        public QueryPlanConstraint getConstraint() {
            return constraint;
        }

        @Nonnull
        public QueryPredicate getTranslatedQueryPredicate() {
            return translatedQueryPredicate;
        }

        @Nonnull
        public PredicateMapping withTranslatedQueryPredicate(@Nonnull final QueryPredicate translatedQueryPredicate) {
            return toBuilder().setTranslatedQueryPredicate(translatedQueryPredicate).build();
        }

        @Nonnull
        public Builder toBuilder() {
            return new Builder(getOriginalQueryPredicate(), getTranslatedQueryPredicate(), getCandidatePredicate(), getMappingKind())
                    .setPredicateCompensation(getPredicateCompensation())
                    .setParameterAliasOptional(getParameterAliasOptional())
                    .setConstraint(getConstraint())
                    .setTranslatedQueryPredicate(getTranslatedQueryPredicate());
        }

        @Nonnull
        public static PredicateMapping.Builder regularMappingBuilder(@Nonnull final QueryPredicate originalQueryPredicate,
                                                                     @Nonnull final QueryPredicate translatedQueryPredicate,
                                                                     @Nonnull final QueryPredicate candidatePredicate) {
            return new Builder(originalQueryPredicate, translatedQueryPredicate, candidatePredicate,
                    MappingKind.REGULAR_IMPLIES_CANDIDATE);
        }

        @Nonnull
        public static PredicateMapping.Builder orTermMappingBuilder(@Nonnull final QueryPredicate originalQueryPredicate,
                                                                    @Nonnull final QueryPredicate translatedQueryPredicate,
                                                                    @Nonnull final QueryPredicate candidatePredicate) {
            return new Builder(originalQueryPredicate, translatedQueryPredicate, candidatePredicate,
                    MappingKind.OR_TERM_IMPLIES_CANDIDATE);
        }

        /**
         * Class to capture the relationship between query predicate and candidate predicate.
         */
        public static class MappingKey {
            @Nonnull
            private final QueryPredicate originalQueryPredicate;
            @Nonnull
            private final QueryPredicate candidatePredicate;
            @Nonnull
            private final MappingKind mappingKind;

            public MappingKey(@Nonnull final QueryPredicate originalQueryPredicate, @Nonnull final QueryPredicate candidatePredicate, @Nonnull final MappingKind mappingKind) {
                this.originalQueryPredicate = originalQueryPredicate;
                this.candidatePredicate = candidatePredicate;
                this.mappingKind = mappingKind;
            }

            @Nonnull
            public QueryPredicate getOriginalQueryPredicate() {
                return originalQueryPredicate;
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
                return Objects.equals(originalQueryPredicate, that.originalQueryPredicate) &&
                       Objects.equals(candidatePredicate, that.candidatePredicate) &&
                       mappingKind == that.mappingKind;
            }

            @Override
            public int hashCode() {
                return Objects.hash(originalQueryPredicate, candidatePredicate, mappingKind);
            }
        }

        /**
         * Builder class for {@link PredicateMapping}.
         */
        public static class Builder {
            @Nonnull
            private final QueryPredicate originalQueryPredicate;
            @Nonnull
            private final QueryPredicate candidatePredicate;
            @Nonnull
            private final MappingKind mappingKind;
            @Nonnull
            private PredicateCompensation predicateCompensation;
            @Nonnull
            private Optional<CorrelationIdentifier> parameterAliasOptional;
            @Nonnull
            private Optional<ComparisonRange> comparisonRangeOptional;
            @Nonnull
            private QueryPlanConstraint constraint;
            @Nonnull
            private QueryPredicate translatedQueryPredicate;

            public Builder(@Nonnull final QueryPredicate originalQueryPredicate,
                           @Nonnull final QueryPredicate translatedQueryPredicate,
                           @Nonnull final QueryPredicate candidatePredicate,
                           @Nonnull final MappingKind mappingKind) {
                this.originalQueryPredicate = originalQueryPredicate;
                this.translatedQueryPredicate = translatedQueryPredicate;
                this.candidatePredicate = candidatePredicate;
                this.mappingKind = mappingKind;
                this.predicateCompensation =
                        (partialMatch, boundPrefixMap, pullUp) -> PredicateCompensationFunction.noCompensationNeeded();
                this.parameterAliasOptional = Optional.empty();
                this.comparisonRangeOptional = Optional.empty();
                this.constraint = QueryPlanConstraint.noConstraint();
            }

            @Nonnull
            public Builder setPredicateCompensation(@Nonnull final PredicateCompensation predicateCompensation) {
                this.predicateCompensation = predicateCompensation;
                return this;
            }

            @Nonnull
            public Builder setParameterAlias(@Nonnull final CorrelationIdentifier parameterAlias) {
                return setParameterAliasOptional(Optional.of(parameterAlias));
            }

            @Nonnull
            public Builder setParameterAliasOptional(@Nonnull final Optional<CorrelationIdentifier> parameterAliasOptional) {
                this.parameterAliasOptional = parameterAliasOptional;
                return this;
            }

            @Nonnull
            public Builder setComparisonRange(@Nonnull final ComparisonRange comparisonRange) {
                return setComparisonRangeOptional(Optional.of(comparisonRange));
            }

            @Nonnull
            public Builder setComparisonRangeOptional(@Nonnull final Optional<ComparisonRange> comparisonRangeOptional) {
                this.comparisonRangeOptional = comparisonRangeOptional;
                return this;
            }

            @Nonnull
            public Builder setSargable(@Nonnull final CorrelationIdentifier parameterAlias,
                                       @Nonnull final ComparisonRange comparisonRange) {
                return setParameterAlias(parameterAlias)
                        .setComparisonRange(comparisonRange);
            }

            @Nonnull
            public Builder setConstraint(@Nonnull final QueryPlanConstraint constraint) {
                this.constraint = constraint;
                return this;
            }

            @Nonnull
            public Builder setTranslatedQueryPredicate(@Nonnull final QueryPredicate translatedQueryPredicate) {
                this.translatedQueryPredicate = translatedQueryPredicate;
                return this;
            }

            @Nonnull
            public PredicateMapping build() {
                return new PredicateMapping(originalQueryPredicate, candidatePredicate, mappingKind,
                        predicateCompensation, parameterAliasOptional, comparisonRangeOptional, constraint,
                        translatedQueryPredicate);
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

        public boolean putAll(@Nonnull final PredicateMultiMap otherMap) {
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
