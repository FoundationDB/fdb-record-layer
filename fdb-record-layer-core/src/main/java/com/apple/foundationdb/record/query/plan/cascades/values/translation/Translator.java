/*
 * Translator.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Translates values.
 */
public class Translator {

    @Nullable
    private final Value holyValue;

    @Nonnull
    private final CorrelationIdentifier queryCorrelation;

    @Nonnull
    private final CorrelationIdentifier candidateCorrelation;

    // todo: constant aliases.
    private Translator(@Nullable final MaxMatchMap maxMatchMapBelow,
                       @Nonnull final CorrelationIdentifier queryCorrelation,
                       @Nonnull final CorrelationIdentifier candidateCorrelation) {
        this.queryCorrelation = queryCorrelation;
        this.candidateCorrelation = candidateCorrelation;
        this.holyValue = maxMatchMapBelow != null ? createHolyValue(maxMatchMapBelow) : null;
    }

    private Value createHolyValue(@Nonnull final MaxMatchMap maxMatchMap) {
        final var belowMapping = maxMatchMap.getMapping();
        final var belowCandidateResultValue = maxMatchMap.getCandidateResultValue();
        final Map<Value, Value> pulledUpMaxMatchMap = belowMapping.entrySet().stream().map(entry -> {
            final var queryPart = entry.getKey();
            final var candidatePart = entry.getValue();
            final var boundIdentitiesMap = AliasMap.identitiesFor(candidatePart.getCorrelatedTo());
            final var pulledUpCandidatesMap = belowCandidateResultValue.pullUp(List.of(candidatePart), boundIdentitiesMap, Set.of(), candidateCorrelation);
            final var pulledUpdateCandidatePart = pulledUpCandidatesMap.get(candidatePart);
            if (pulledUpdateCandidatePart == null) {
                throw new RecordCoreException(String.format("could not pull up %s", candidatePart));
            }
            return Map.entry(queryPart, pulledUpdateCandidatePart);
        }).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        final var translatedQueryValueFromBelow = maxMatchMap.getQueryResultValue();
        return Verify.verifyNotNull(translatedQueryValueFromBelow.replace(valuePart ->
                pulledUpMaxMatchMap.entrySet().stream().filter(maxMatchMapItem -> {
                    final var aliasMap = AliasMap.identitiesFor(Sets.union(maxMatchMapItem.getKey().getCorrelatedTo(), valuePart.getCorrelatedTo()));
                    return maxMatchMapItem.getKey().semanticEquals(valuePart, aliasMap);
                }).map(Map.Entry::getValue).findAny().orElse(valuePart)));
    }

    @Nonnull
    public MaxMatchMap calculateMaxMatches(@Nonnull final Value rewrittenQueryValue,
                                           @Nonnull final Value candidateValue) {
        final BiMap<Value, Value> newMapping = HashBiMap.create();
        rewrittenQueryValue.preOrderPruningIterator(queryValuePart -> {
            // now that we have rewritten this query value part using candidate value(s) we proceed to look it up in the candidate value.
            final var match = Streams.stream(candidateValue
                    .preOrderPruningIterator(v -> v instanceof Value.IsDescendible))
                    .filter(candidateValuePart -> candidateValuePart.semanticEquals(queryValuePart, AliasMap.identitiesFor(candidateValuePart.getCorrelatedTo())))
                    .findAny();
            match.ifPresent(value -> newMapping.put(queryValuePart, value));
            return match.isEmpty();
        }).forEachRemaining(ignored -> {
        });
        return new MaxMatchMap(newMapping, rewrittenQueryValue, candidateValue);
    }

    @Nonnull
    public Value translate(@Nonnull final Value value) {
        if (holyValue == null) {
            return value.translateCorrelations(TranslationMap.rebaseWithAliasMap(AliasMap.of(queryCorrelation, candidateCorrelation)));
        }
        final var result = value.translateCorrelations(TranslationMap.builder().when(queryCorrelation).then(candidateCorrelation, (src, tgt, quantifiedValue) -> holyValue).build());
        return result.simplify(AliasMap.emptyMap(), result.getCorrelatedTo());
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder for {@link Translator} objects.
     */
    public static class Builder {

        protected CorrelationIdentifier queryCorrelation;

        protected  CorrelationIdentifier candidateCorrelation;

        /**
         * Fluent builder for {@link Translator} objects.
         */
        public static class WithCorrelationsBuilder extends Builder {

            @Nullable
            private MaxMatchMap maxMatchMap;

            private WithCorrelationsBuilder(@Nonnull CorrelationIdentifier queryCorrelation, @Nonnull CorrelationIdentifier candidateCorrelation) {
                this.queryCorrelation = queryCorrelation;
                this.candidateCorrelation = candidateCorrelation;
            }

            @Nonnull
            public WithCorrelationsBuilder using(@Nonnull final MaxMatchMap maxMatchMap) {
                this.maxMatchMap = maxMatchMap;
                return this;
            }

            @Nonnull
            @Override
            public WithCorrelationsBuilder ofCorrelations(@Nonnull final CorrelationIdentifier queryCorrelation,
                                                          @Nonnull final CorrelationIdentifier candidateCorrelation) {
                this.queryCorrelation = queryCorrelation;
                this.candidateCorrelation = candidateCorrelation;
                return this;
            }

            @Nonnull
            public Translator build() {
                return new Translator(maxMatchMap, queryCorrelation, candidateCorrelation);
            }

        }

        @Nonnull
        public WithCorrelationsBuilder ofCorrelations(@Nonnull final CorrelationIdentifier queryCorrelation,
                                                      @Nonnull final CorrelationIdentifier candidateCorrelation) {
            return new WithCorrelationsBuilder(queryCorrelation, candidateCorrelation);
        }
    }
}
