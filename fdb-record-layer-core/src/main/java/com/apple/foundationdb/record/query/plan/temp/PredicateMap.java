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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
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
        final ImmutableSet<PredicateMapping> predicateEntries = getMap().get(queryPredicate);
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
        for (final QueryPredicate queryPredicate : map.keySet()) {
            final Set<PredicateMapping> candidatePredicateMappings = map.get(queryPredicate);
            if (candidatePredicateMappings.size() != 1) {
                return Optional.empty();
            }
        }
        return Optional.of(map);
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
