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

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithComparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistentialValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
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
    private final SetMultimap<QueryPredicate, PredicateMapping> map;

    private static Value replaceNewlyMatchedValues(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                   final Map<Value, Value> amendedMatchedAggregateMap,
                                                   final Value rootValue) {
        return Objects.requireNonNull(rootValue.replace((Function<Value, @Nullable Value>)currentValue -> {
            if (currentValue instanceof GroupByExpression.UnmatchedAggregateValue) {
                final var unmatchedId =
                        ((GroupByExpression.UnmatchedAggregateValue)currentValue).getUnmatchedId();
                final var queryValue =
                        Objects.requireNonNull(unmatchedAggregateMap.get(unmatchedId));
                final var translatedQueryValue =
                        amendedMatchedAggregateMap.get(queryValue);
                if (translatedQueryValue != null) {
                    return translatedQueryValue;
                }
            }
            return currentValue;
        }));
    }

    /**
     * Functional interface to reapply a predicate if necessary.
     */
    @FunctionalInterface
    public interface PredicateCompensation {
        PredicateCompensationFunction computeCompensationFunction(PartialMatch partialMatch,
                                                                  Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                  PullUp pullup);
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

                    @Override
                    public PredicateCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                               final Map<Value, Value> amendedMatchedAggregateMap) {
                        return this;
                    }

                    @Override
                    public Set<QueryPredicate> applyCompensationForPredicate(final TranslationMap translationMap) {
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

                    @Override
                    public PredicateCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                               final Map<Value, Value> amendedMatchedAggregateMap) {
                        return this;
                    }

                    @Override
                    public Set<QueryPredicate> applyCompensationForPredicate(final TranslationMap translationMap) {
                        throw new IllegalArgumentException("this method should not be called");
                    }
                };

        boolean isNeeded();

        boolean isImpossible();

        /**
         * Recreates this predicate compensation function, and if appropriate, allows this compensation function
         * to become possible.
         * @param unmatchedAggregateMap unmatched aggregated map; the resulting predicate compensation function
         *        can only become possible if the unmatched aggregates are not referenced by the predicate
         * @param amendedMatchedAggregateMap matched aggregate map (amended)
         * @return a new {@link PredicateCompensationFunction}
         */
        PredicateCompensationFunction amend(BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                            Map<Value, Value> amendedMatchedAggregateMap);

        Set<QueryPredicate> applyCompensationForPredicate(TranslationMap translationMap);

        static PredicateCompensationFunction ofPredicate(final QueryPredicate predicate) {
            return ofPredicate(predicate, false);
        }

        static PredicateCompensationFunction ofPredicate(final QueryPredicate predicate,
                                                         final boolean shouldSimplifyValues) {
            final var isImpossible = predicateContainsUncompensatableValues(predicate);

            return new PredicateCompensationFunction() {
                @Override
                public boolean isNeeded() {
                    return true;
                }

                @Override
                public boolean isImpossible() {
                    return isImpossible;
                }

                @Override
                public PredicateCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                           final Map<Value, Value> amendedMatchedAggregateMap) {
                    final var amendedTranslatedPredicateOptional =
                            predicate.replaceValuesMaybe(rootValue ->
                                    Optional.of(replaceNewlyMatchedValues(unmatchedAggregateMap, amendedMatchedAggregateMap,
                                            rootValue)));
                    Verify.verify(amendedTranslatedPredicateOptional.isPresent());
                    return ofPredicate(amendedTranslatedPredicateOptional.get(), true);
                }

                @Override
                public Set<QueryPredicate> applyCompensationForPredicate(final TranslationMap translationMap) {
                    return LinkedIdentitySet.of(predicate.translateCorrelations(translationMap, shouldSimplifyValues));
                }
            };
        }

        private static boolean predicateContainsUncompensatableValues(final QueryPredicate pulledUpPredicate) {
            if (pulledUpPredicate instanceof PredicateWithValue) {
                final var value = Objects.requireNonNull(((PredicateWithValue)pulledUpPredicate).getValue());
                if (value.preOrderStream()
                        .anyMatch(v -> v instanceof GroupByExpression.UnmatchedAggregateValue || v instanceof Value.IndexOnlyValue)) {
                    return true;
                }
            }

            if (pulledUpPredicate instanceof PredicateWithComparisons) {
                final var comparisons = ((PredicateWithComparisons)pulledUpPredicate).getComparisons();
                for (final var comparison : comparisons) {
                    if (comparison instanceof Comparisons.ValueComparison) {
                        // ValueComparison.getValue() always returns non-null, unlike the @Nullable default on the
                        // Comparison interface; cast to pick up the non-null override.
                        final var comparisonValue = ((Comparisons.ValueComparison)comparison).getValue();
                        if (comparisonValue.preOrderStream()
                                .anyMatch(v -> v instanceof GroupByExpression.UnmatchedAggregateValue || v instanceof Value.IndexOnlyValue)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        static PredicateCompensationFunction ofExistentialValuePredicate(final ExistentialValuePredicate existentialValuePredicate) {
            final var result = LinkedIdentitySet.of((QueryPredicate)existentialValuePredicate);

            return new PredicateCompensationFunction() {
                @Override
                public boolean isNeeded() {
                    return true;
                }

                @Override
                public boolean isImpossible() {
                    return false;
                }

                @Override
                public PredicateCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                           final Map<Value, Value> amendedMatchedAggregateMap) {
                    return this;
                }

                @Override
                public Set<QueryPredicate> applyCompensationForPredicate(final TranslationMap translationMap) {
                    return result;
                }
            };
        }

        static PredicateCompensationFunction ofChildrenCompensationFunctions(final List<PredicateCompensationFunction> childrenCompensationFunctions,
                                                                             final BiFunction<List<PredicateCompensationFunction>, TranslationMap, Set<QueryPredicate>> compensationFunction) {
            return new PredicateCompensationFunction() {
                @Override
                public boolean isNeeded() {
                    return true;
                }

                @Override
                public boolean isImpossible() {
                    return childrenCompensationFunctions.stream().anyMatch(PredicateCompensationFunction::isImpossible);
                }

                @Override
                public PredicateCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                           final Map<Value, Value> amendedMatchedAggregateMap) {
                    final var amendedChildrenCompensationFunctions =
                            childrenCompensationFunctions.stream()
                                    .map(childrenCompensationFunction ->
                                            childrenCompensationFunction.amend(unmatchedAggregateMap,
                                                    amendedMatchedAggregateMap))
                                    .collect(ImmutableList.toImmutableList());
                    return ofChildrenCompensationFunctions(amendedChildrenCompensationFunctions, compensationFunction);
                }

                @Override
                public Set<QueryPredicate> applyCompensationForPredicate(final TranslationMap translationMap) {
                    return compensationFunction.apply(childrenCompensationFunctions, translationMap);
                }
            };
        }

        static PredicateCompensationFunction noCompensationNeeded() {
            return NO_COMPENSATION_NEEDED;
        }

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

                    @Override
                    public ResultCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                            final Map<Value, Value> amendedMatchedAggregateMap) {
                        return this;
                    }

                    @Override
                    public Value applyCompensationForResult(final TranslationMap translationMap) {
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

                    @Override
                    public ResultCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                            final Map<Value, Value> amendedMatchedAggregateMap) {
                        return this;
                    }

                    @Override
                    public Value applyCompensationForResult(final TranslationMap translationMap) {
                        throw new IllegalArgumentException("this method should not be called");
                    }
                };

        boolean isNeeded();

        boolean isImpossible();

        /**
         * Recreates this result compensation function, and if appropriate, allows this compensation function
         * to become possible.
         * @param unmatchedAggregateMap unmatched aggregated map; the resulting predicate compensation function
         *        can only become possible if the unmatched aggregates are not referenced by the predicate
         * @param amendedMatchedAggregateMap matched aggregate map (amended)
         * @return a new {@link ResultCompensationFunction}
         */
        ResultCompensationFunction amend(BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                         Map<Value, Value> amendedMatchedAggregateMap);

        Value applyCompensationForResult(TranslationMap translationMap);

        static ResultCompensationFunction ofValue(final Value value) {
            return ofValue(value, false);
        }

        static ResultCompensationFunction ofValue(final Value value, final boolean shouldSimplifyValue) {
            final var isImpossible = valueContainsUnmatchedValues(value);

            return new ResultCompensationFunction() {
                @Override
                public boolean isNeeded() {
                    return true;
                }

                @Override
                public boolean isImpossible() {
                    return isImpossible;
                }

                @Override
                public ResultCompensationFunction amend(final BiMap<CorrelationIdentifier, Value> unmatchedAggregateMap,
                                                        final Map<Value, Value> amendedMatchedAggregateMap) {
                    final var amendedTranslatedQueryValue =
                            replaceNewlyMatchedValues(unmatchedAggregateMap, amendedMatchedAggregateMap, value);
                    return ofValue(amendedTranslatedQueryValue, true);
                }

                @Override
                public Value applyCompensationForResult(final TranslationMap translationMap) {
                    return value.translateCorrelations(translationMap, shouldSimplifyValue);
                }
            };
        }

        static ResultCompensationFunction noCompensationNeeded() {
            return NO_COMPENSATION_NEEDED;
        }

        static ResultCompensationFunction impossibleCompensation() {
            return IMPOSSIBLE_COMPENSATION;
        }

        private static boolean valueContainsUnmatchedValues(final Value pulledUpValue) {
            return pulledUpValue.preOrderStream()
                    .anyMatch(v -> v instanceof GroupByExpression.UnmatchedAggregateValue);
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

        private final MappingKey mappingKey;
        private final PredicateCompensation predicateCompensation;
        private final Optional<CorrelationIdentifier> parameterAliasOptional;
        private final Optional<ComparisonRange> comparisonRangeOptional;
        private final QueryPlanConstraint constraint;
        private final QueryPredicate translatedQueryPredicate;

        private PredicateMapping(final QueryPredicate originalQueryPredicate,
                                 final QueryPredicate candidatePredicate,
                                 final MappingKind mappingKind,
                                 final PredicateCompensation predicateCompensation,
                                 final Optional<CorrelationIdentifier> parameterAlias,
                                 final Optional<ComparisonRange> comparisonRangeOptional,
                                 final QueryPlanConstraint constraint,
                                 final QueryPredicate translatedQueryPredicate) {
            this.mappingKey = new MappingKey(originalQueryPredicate, candidatePredicate, mappingKind);
            this.predicateCompensation = predicateCompensation;
            this.parameterAliasOptional = parameterAlias;
            this.comparisonRangeOptional = comparisonRangeOptional;
            this.constraint = constraint;
            this.translatedQueryPredicate = translatedQueryPredicate;
        }

        public QueryPredicate getOriginalQueryPredicate() {
            return mappingKey.getOriginalQueryPredicate();
        }

        public QueryPredicate getCandidatePredicate() {
            return mappingKey.getCandidatePredicate();
        }

        public MappingKind getMappingKind() {
            return mappingKey.getMappingKind();
        }

        public MappingKey getMappingKey() {
            return mappingKey;
        }

        public PredicateCompensation getPredicateCompensation() {
            return predicateCompensation;
        }

        public Optional<CorrelationIdentifier> getParameterAliasOptional() {
            return parameterAliasOptional;
        }

        public Optional<ComparisonRange> getComparisonRangeOptional() {
            return comparisonRangeOptional;
        }

        public QueryPlanConstraint getConstraint() {
            return constraint;
        }

        public QueryPredicate getTranslatedQueryPredicate() {
            return translatedQueryPredicate;
        }

        public PredicateMapping withTranslatedQueryPredicate(final QueryPredicate translatedQueryPredicate) {
            return toBuilder().setTranslatedQueryPredicate(translatedQueryPredicate).build();
        }

        public Builder toBuilder() {
            return new Builder(getOriginalQueryPredicate(), getTranslatedQueryPredicate(), getCandidatePredicate(), getMappingKind())
                    .setPredicateCompensation(getPredicateCompensation())
                    .setParameterAliasOptional(getParameterAliasOptional())
                    .setConstraint(getConstraint())
                    .setTranslatedQueryPredicate(getTranslatedQueryPredicate());
        }

        public static PredicateMapping.Builder regularMappingBuilder(final QueryPredicate originalQueryPredicate,
                                                                     final QueryPredicate translatedQueryPredicate,
                                                                     final QueryPredicate candidatePredicate) {
            return new Builder(originalQueryPredicate, translatedQueryPredicate, candidatePredicate,
                    MappingKind.REGULAR_IMPLIES_CANDIDATE);
        }

        public static PredicateMapping.Builder orTermMappingBuilder(final QueryPredicate originalQueryPredicate,
                                                                    final QueryPredicate translatedQueryPredicate,
                                                                    final QueryPredicate candidatePredicate) {
            return new Builder(originalQueryPredicate, translatedQueryPredicate, candidatePredicate,
                    MappingKind.OR_TERM_IMPLIES_CANDIDATE);
        }

        /**
         * Class to capture the relationship between query predicate and candidate predicate.
         */
        public static class MappingKey {
            private final QueryPredicate originalQueryPredicate;
            private final QueryPredicate candidatePredicate;
            private final MappingKind mappingKind;

            public MappingKey(final QueryPredicate originalQueryPredicate, final QueryPredicate candidatePredicate, final MappingKind mappingKind) {
                this.originalQueryPredicate = originalQueryPredicate;
                this.candidatePredicate = candidatePredicate;
                this.mappingKind = mappingKind;
            }

            public QueryPredicate getOriginalQueryPredicate() {
                return originalQueryPredicate;
            }

            public QueryPredicate getCandidatePredicate() {
                return candidatePredicate;
            }

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
                // Note: This hash must be stable across JVMs, as `MappingKey` is stored in hash-based sets during planning.
                return Objects.hash(originalQueryPredicate, candidatePredicate, mappingKind.name());
            }
        }

        /**
         * Builder class for {@link PredicateMapping}.
         */
        public static class Builder {
            private final QueryPredicate originalQueryPredicate;
            private final QueryPredicate candidatePredicate;
            private final MappingKind mappingKind;
            private PredicateCompensation predicateCompensation;
            private Optional<CorrelationIdentifier> parameterAliasOptional;
            private Optional<ComparisonRange> comparisonRangeOptional;
            private QueryPlanConstraint constraint;
            private QueryPredicate translatedQueryPredicate;

            public Builder(final QueryPredicate originalQueryPredicate,
                           final QueryPredicate translatedQueryPredicate,
                           final QueryPredicate candidatePredicate,
                           final MappingKind mappingKind) {
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

            public Builder setPredicateCompensation(final PredicateCompensation predicateCompensation) {
                this.predicateCompensation = predicateCompensation;
                return this;
            }

            public Builder setParameterAlias(final CorrelationIdentifier parameterAlias) {
                return setParameterAliasOptional(Optional.of(parameterAlias));
            }

            public Builder setParameterAliasOptional(final Optional<CorrelationIdentifier> parameterAliasOptional) {
                this.parameterAliasOptional = parameterAliasOptional;
                return this;
            }

            public Builder setComparisonRange(final ComparisonRange comparisonRange) {
                return setComparisonRangeOptional(Optional.of(comparisonRange));
            }

            public Builder setComparisonRangeOptional(final Optional<ComparisonRange> comparisonRangeOptional) {
                this.comparisonRangeOptional = comparisonRangeOptional;
                return this;
            }

            public Builder setSargable(final CorrelationIdentifier parameterAlias,
                                       final ComparisonRange comparisonRange) {
                return setParameterAlias(parameterAlias)
                        .setComparisonRange(comparisonRange);
            }

            public Builder setConstraint(final QueryPlanConstraint constraint) {
                this.constraint = constraint;
                return this;
            }

            public Builder setTranslatedQueryPredicate(final QueryPredicate translatedQueryPredicate) {
                this.translatedQueryPredicate = translatedQueryPredicate;
                return this;
            }

            public PredicateMapping build() {
                return new PredicateMapping(originalQueryPredicate, candidatePredicate, mappingKind,
                        predicateCompensation, parameterAliasOptional, comparisonRangeOptional, constraint,
                        translatedQueryPredicate);
            }
        }
    }

    protected PredicateMultiMap(final SetMultimap<QueryPredicate, PredicateMapping> map) {
        SetMultimap<QueryPredicate, PredicateMapping> copy = Multimaps.newSetMultimap(new LinkedIdentityMap<>(), LinkedIdentitySet::new);
        map.entries().forEach(entry -> copy.put(entry.getKey(), entry.getValue()));
        this.map = Multimaps.unmodifiableSetMultimap(copy);
    }

    protected SetMultimap<QueryPredicate, PredicateMapping> getMap() {
        return map;
    }

    public Set<PredicateMapping> get(final QueryPredicate queryPredicate) {
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

    public static Builder builder() {
        return new Builder();
    }

    private static Optional<SetMultimap<QueryPredicate, PredicateMapping>> checkConflicts(final SetMultimap<QueryPredicate, PredicateMapping> map) {
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

        public boolean put(final QueryPredicate queryPredicate,
                           final PredicateMapping predicateMapping) {
            return map.put(queryPredicate, predicateMapping);
        }

        public boolean putAll(final PredicateMultiMap otherMap) {
            boolean isModified = false;
            for (final Map.Entry<QueryPredicate, PredicateMapping> entry : otherMap.getMap().entries()) {
                isModified = map.put(entry.getKey(), entry.getValue()) || isModified;
            }

            return isModified;
        }

        public boolean putAll(final QueryPredicate queryPredicate, final Set<PredicateMapping> predicateMappings) {
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
