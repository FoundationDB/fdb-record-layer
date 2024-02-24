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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a max match between a (Rewritten) query result {@link Value} and the candidate result {@link Value}.
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

    @Nonnull
    public TranslationMap pullUpTranslationMap(@Nonnull final CorrelationIdentifier queryCorrelation,
                                               @Nonnull final CorrelationIdentifier candidateCorrelation) {
        final var translatedQueryValue = translatedQueryValue(candidateCorrelation);
        return TranslationMap.builder()
                .when(queryCorrelation)
                .then(candidateCorrelation, (src, tgt, quantifiedValue) -> translatedQueryValue)
                .build();
    }

    @Nonnull
    private Value translatedQueryValue(@Nonnull final CorrelationIdentifier candidateCorrelation) {
        final var belowMapping = getMapping();
        final var belowCandidateResultValue = getCandidateResultValue();
        final Map<Value, Value> pulledUpMaxMatchMap = belowMapping.entrySet().stream().map(entry -> {
            final var queryPart = entry.getKey();
            final var candidatePart = entry.getValue();
            final var pulledUpCandidatesMap =
                    belowCandidateResultValue.pullUp(ImmutableList.of(candidatePart),
                            AliasMap.identitiesFor(belowCandidateResultValue.getCorrelatedTo()),
                            ImmutableSet.of(), candidateCorrelation);
            final var pulledUpdateCandidatePart = pulledUpCandidatesMap.get(candidatePart);
            if (pulledUpdateCandidatePart == null) {
                throw new RecordCoreException(String.format("could not pull up %s", candidatePart));
            }
            return Map.entry(queryPart, pulledUpdateCandidatePart);
        }).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        final var queryResultValueFromBelow = getQueryResultValue();
        return Verify.verifyNotNull(queryResultValueFromBelow.replace(valuePart ->
                pulledUpMaxMatchMap
                        .entrySet()
                        .stream()
                        .filter(maxMatchMapItem -> maxMatchMapItem.getKey().semanticEquals(valuePart, equivalencesMap))
                        .map(Map.Entry::getValue)
                        .findAny()
                        .orElse(valuePart)));
    }

    /**
     * Calculates the maximum sub-{@link Value}s in {@code rewrittenQueryValue} that has an exact match in the
     * {@code candidateValue}.
     *
     * @param queryResultValue the query result {@code Value}.
     * @param candidateResulyValue the candidate result {@code Value} we want to search for maximum matches.
     *
     * @return a {@link  MaxMatchMap} of all maximum matches.
     */
    @Nonnull
    public static MaxMatchMap calculate(@Nonnull final AliasMap equivalenceAliasMap,
                                        @Nonnull final Value queryResultValue,
                                        @Nonnull final Value candidateResulyValue) {
        final BiMap<Value, Value> newMapping = HashBiMap.create();
        queryResultValue.preOrderPruningIterator(queryValuePart -> {
            // look up the query side sub values in the candidate value.
            final var match = Streams.stream(candidateResulyValue
                            // when traversing the candidate in pre-order, only descend into structures that can be referenced
                            // from the top expression. For example, RCV's components can be referenced however an Arithmetic
                            // operator's children can not be referenced.
                            .preOrderPruningIterator(v -> v instanceof RecordConstructorValue || v instanceof FieldValue))
                    .filter(candidateValuePart -> queryValuePart.semanticEquals(candidateValuePart, equivalenceAliasMap))
                    .findAny();
            match.ifPresent(value -> newMapping.put(queryValuePart, value));
            return match.isEmpty();
        }).forEachRemaining(ignored -> {
            // nothing
        });
        return new MaxMatchMap(equivalenceAliasMap, newMapping, queryResultValue, candidateResulyValue);
    }
}
