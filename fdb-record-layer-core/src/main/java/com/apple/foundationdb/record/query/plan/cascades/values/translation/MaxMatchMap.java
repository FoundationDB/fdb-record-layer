/*
 * MaxMatchMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Correlated.BoundEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Represents a max match between a (rewritten) query result {@link Value} and the candidate result {@link Value}.
 */
public class MaxMatchMap {
    @Nonnull
    private final BiMap<Value, Value> mapping;
    @Nonnull
    private final Value queryResultValue; // in terms of the candidate quantifiers.
    @Nonnull
    private final Value candidateResultValue;
    @Nonnull
    private final AliasMap equivalencesMap;

    /**
     * Creates a new instance of {@link MaxMatchMap}.
     * @param equivalencesMap a map of aliases that are considered to be equivalent
     * @param mapping The {@link Value} mapping.
     * @param queryResult The query result from which the mapping keys originate.
     * @param candidateResult The candidate result from which the mapping values originate.
     */
    MaxMatchMap(@Nonnull final AliasMap equivalencesMap,
                @Nonnull final Map<Value, Value> mapping,
                @Nonnull final Value queryResult,
                @Nonnull final Value candidateResult) {
        this.equivalencesMap = equivalencesMap;
        this.mapping = ImmutableBiMap.copyOf(mapping);
        this.queryResultValue = queryResult;
        this.candidateResultValue = candidateResult;
    }

    @Nonnull
    public AliasMap getEquivalencesMap() {
        return equivalencesMap;
    }

    @Nonnull
    public Map<Value, Value> getMapping() {
        return mapping;
    }

    @Nonnull
    public Value getCandidateResultValue() {
        return candidateResultValue;
    }

    @Nonnull
    public Value getQueryResultValue() {
        return queryResultValue;
    }

    /**
     * This produces a translation map comprising a single item which replaces {@code queryCorrelation}
     * with the {@code queryResultValue} that is rewritten in terms of {@code candidateCorrelation} according
     * this map of maximum matches between the {@code queryResultValue} and the {@code candidateResultValue}.
     *
     * @param queryCorrelation The query correlation used as a translation source in the resulting translation
     *                         map.
     * @param candidateCorrelation The correlation, according to which, the {@code queryResultValue} will be rewritten.
     * @return A single-item translation map comprising a replacement of {@code queryCorrelation} with the
     * {@code queryResultValue} that is rewritten in terms of {@code candidateCorrelation} according this map of maximum
     * matches between the {@code queryResultValue} and the {@code candidateResultValue}.
     */
    @Nonnull
    public TranslationMap pullUpTranslationMap(@Nonnull final CorrelationIdentifier queryCorrelation,
                                               @Nonnull final CorrelationIdentifier candidateCorrelation) {
        final var translatedQueryValue = translateQueryValue(candidateCorrelation);
        return TranslationMap.builder()
                .when(queryCorrelation).then((src, quantifiedValue) -> translatedQueryValue)
                .build();
    }

    @Nonnull
    private Value translateQueryValue(@Nonnull final CorrelationIdentifier candidateCorrelation) {
        final var mapping = getMapping();
        final var candidateResultValue = getCandidateResultValue();
        final var pulledUpCandidateSide =
                candidateResultValue.pullUp(mapping.values(),
                        AliasMap.emptyMap(),
                        ImmutableSet.of(), candidateCorrelation);
        //
        // We now have the right side pulled up, specifically we have a map from each candidate value below,
        // to a candidate value pulled up along the candidateCorrelation. We also have this max match map, which
        // encapsulates a map from query values to candidate value.
        // In other words we have in this max match map m1 := MAP(queryValues over q -> candidateValues over q') and
        // we just computed m2 := MAP(candidateValues over p' -> candidateValues over candidateCorrelation). We now
        // chain these two maps to get m1 ○ m2 := MAP(queryValues over q -> candidateValues over candidateCorrelation).
        // As we will use this map in the subsequent step to look up values over semantic equivalency using
        // equivalencesMap, we immediately create m1 ○ m2 using a boundEquivalence based on equivalencesMap.
        //
        final var boundEquivalence = new BoundEquivalence<Value>(equivalencesMap);
        final var pulledUpMaxMatchMap = mapping.entrySet()
                .stream()
                .map(entry -> {
                    final var queryPart = entry.getKey();
                    final var candidatePart = entry.getValue();
                    final var pulledUpdateCandidatePart = pulledUpCandidateSide.get(candidatePart);
                    if (pulledUpdateCandidatePart == null) {
                        throw new RecordCoreException("could not pull up candidate part").addLogInfo("candidate_part", candidatePart);
                    }
                    return Map.entry(boundEquivalence.wrap(queryPart), pulledUpdateCandidatePart);
                }).collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        final var queryResultValueFromBelow = getQueryResultValue();
        return Verify.verifyNotNull(queryResultValueFromBelow.replace(valuePart -> {
            final var maxMatch = pulledUpMaxMatchMap.get(boundEquivalence.wrap(valuePart));
            return maxMatch == null ? valuePart : maxMatch;
        }));
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code rewrittenQueryValue} that has an exact match in the
     * {@code candidateValue}.
     *
     * @param queryResultValue the query result {@code Value}.
     * @param candidateResultValue the candidate result {@code Value} we want to search for maximum matches.
     *
     * @return a {@link  MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final Value queryResultValue,
                                        @Nonnull final Value candidateResultValue) {
        return calculate(AliasMap.emptyMap(),
                queryResultValue,
                candidateResultValue);
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code rewrittenQueryValue} that has an exact match in the
     * {@code candidateValue}.
     * <br>
     * For certain shapes of {@code Value}s, multiple matches can be found, this method is guaranteed to always find the
     * maximum part of the candidate sub-{@code Value} that matches with a sub-{@code Value} on the query side. For
     * example, assume we have a query {@code Value} {@code R = RCV(s+t)} and a candidate {@code Value} that is
     * {@code R` = RCV(s, t, (s+t))}, with R ≡ R`. We could have the following matches:
     * <ul>
     *     <li>{@code R.0 --> R`.0 + R`.1}</li>
     *     <li>{@code R -> R`.0.0}</li>
     * </ul>
     * The first match is not <i>maximum</i>, because it involves matching smaller constituents of the index and summing
     * them together, the other match however, is much better because it matches the entire query {@code Value} with a
     * single part of the index. The algorithm will always prefer the maximum match.
     *
     * @param equivalenceAliasMap an alias map that informs the logic about equivalent aliases
     * @param queryResultValue the query result {@code Value}.
     * @param candidateResultValue the candidate result {@code Value} we want to search for maximum matches.
     *
     * @return a {@link  MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final AliasMap equivalenceAliasMap,
                                        @Nonnull final Value queryResultValue,
                                        @Nonnull final Value candidateResultValue) {
        final var aliasesToBeAdded = Sets.difference(queryResultValue.getCorrelatedTo(), equivalenceAliasMap.sources());
        final var amendedEquivalenceMap = equivalenceAliasMap.toBuilder()
                .identitiesFor(aliasesToBeAdded)
                .build();

        final BiMap<Value, Value> newMapping = HashBiMap.create();
        queryResultValue.preOrderPruningIterator(queryValuePart -> {
            // look up the query sub values in the candidate value.
            final var match = Streams.stream(candidateResultValue
                            // when traversing the candidate in pre-order, only descend into structures that can be referenced
                            // from the top expression. For example, RCV's components can be referenced however an Arithmetic
                            // operator's children can not be referenced.
                            // It is crucial to do this in pre-order to guarantee matching the maximum (sub-)Value of the candidate.
                            .preOrderPruningIterator(v -> v instanceof RecordConstructorValue || v instanceof FieldValue))
                    .filter(candidateValuePart -> queryValuePart.semanticEquals(candidateValuePart, amendedEquivalenceMap))
                    .findAny();
            match.ifPresent(value -> newMapping.put(queryValuePart, value));
            // if match is empty, descend further and look for more fine-grained matches.
            return match.isEmpty();
        }).forEachRemaining(ignored -> {
            // nothing
        });
        return new MaxMatchMap(equivalenceAliasMap, newMapping, queryResultValue, candidateResultValue);
    }
}
