/*
 * PredicateMap.java
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * Predicate map that enforces a constraint that a predicate can only be mapped to at most one candidate.
 */
public class PredicateMap extends PredicateMultiMap {
    private PredicateMap(@Nonnull final SetMultimap<QueryPredicate, PredicateMapping> map) {
        super(map);
    }

    public Optional<PredicateMapping> getMappingOptional(@Nonnull final QueryPredicate queryPredicate) {
        final Set<PredicateMapping> predicateEntries = getMap().get(queryPredicate);
        if (predicateEntries.size() != 1) {
            return Optional.empty();
        }

        return Optional.of(Iterables.getOnlyElement(predicateEntries));
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    public static PredicateMap empty() {
        return new Builder().build();
    }

    private static Optional<SetMultimap<QueryPredicate, PredicateMapping>> checkUniqueness(@Nonnull final SetMultimap<QueryPredicate, PredicateMapping> map) {
        final ImmutableSetMultimap.Builder<QueryPredicate, PredicateMapping> dedupedBuilder = ImmutableSetMultimap.builder();
        for (final QueryPredicate queryPredicate : map.keySet()) {
            final Set<PredicateMapping> candidatePredicateMappings = map.get(queryPredicate);
            if (candidatePredicateMappings.size() == 1) {
                dedupedBuilder.put(queryPredicate, Iterables.getOnlyElement(candidatePredicateMappings));
            } else {
                final Iterator<PredicateMapping> iterator = candidatePredicateMappings.iterator();
                final PredicateMapping first = iterator.next();
                while (iterator.hasNext()) {
                    if (!mappingsAreEquivalent(first, iterator.next())) {
                        return Optional.empty();
                    }
                }
                dedupedBuilder.put(queryPredicate, first);
            }
        }
        return Optional.of(dedupedBuilder.build());
    }

    private static boolean mappingsAreEquivalent(@Nonnull final PredicateMapping mapping1,
                                                 @Nonnull final PredicateMapping mapping2) {
        if (!mapping1.getMappingKind().equals(mapping2.getMappingKind())) {
            return false;
        }
        if (!mapping1.getParameterAliasOptional().equals(mapping2.getParameterAliasOptional())) {
            return false;
        }
        if (!mapping1.getComparisonRangeOptional().equals(mapping2.getComparisonRangeOptional())) {
            return false;
        }
        if (!mapping1.getConstraint().equals(mapping2.getConstraint())) {
            return false;
        }
        final var candidatePredicate1 = mapping1.getMappingKey().getCandidatePredicate();
        final var candidatePredicate2 = mapping2.getMappingKey().getCandidatePredicate();
        if (candidatePredicate1.getCorrelatedTo().size() != candidatePredicate2.getCorrelatedTo().size()) {
            return false;
        }
        final var equivalenceMap = AliasMap.builder().zip(ImmutableList.copyOf(candidatePredicate1.getCorrelatedTo()),
                ImmutableList.copyOf(candidatePredicate2.getCorrelatedTo()));
        return candidatePredicate1.semanticEquals(candidatePredicate2, equivalenceMap.build());
    }

    /**
     * Builder class for a predicate maps.
     */
    public static class Builder extends PredicateMultiMap.Builder {
        public Builder() {
        }

        @Override
        public Optional<SetMultimap<QueryPredicate, PredicateMapping>> checkCorrectness() {
            return super.checkCorrectness()
                    .flatMap(PredicateMap::checkUniqueness);
        }

        @Override
        public PredicateMap build() {
            return new PredicateMap(checkCorrectness().orElseThrow(() -> new IllegalArgumentException("conflicts in mapping")));
        }

        @Override
        public Optional<? extends PredicateMap> buildMaybe() {
            return checkCorrectness()
                    .map(PredicateMap::new);
        }
    }
}
