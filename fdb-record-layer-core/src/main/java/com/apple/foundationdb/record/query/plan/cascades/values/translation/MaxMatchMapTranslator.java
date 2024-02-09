/*
 * MaxMatchMapTranslator.java
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

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This represents a {@link Translator} that relies on a given {@link MaxMatchMap} pertaining the referenced {@link Value}
 * under the give {@code queryCorrelation} and {@code candidateCorrelation} to translate a given {@link Value}.
 */
public class MaxMatchMapTranslator extends Translator {

    @Nonnull
    private final TranslationMap.Builder translationMapBuilder;

    @Nonnull
    private final TranslationMap translationMap;

    /**
     * Creates a new instance of {@link MaxMatchMapTranslator}.
     * @param maxMatchMapBelow The {@link MaxMatchMap} of matches of values referenced by the query and candidate correlations.
     * @param queryCorrelation The query correlation.
     * @param candidateCorrelation The candidate correlation.
     * @param constantAliases A list of constant aliases, i.e. aliases the remain unchanged after translating a {@link Value}.
     */
    public MaxMatchMapTranslator(@Nonnull final MaxMatchMap maxMatchMapBelow,
                                 @Nonnull final CorrelationIdentifier queryCorrelation,
                                 @Nonnull final CorrelationIdentifier candidateCorrelation,
                                 @Nonnull final AliasMap constantAliases) {
        super(constantAliases.derived().put(candidateCorrelation, candidateCorrelation).build());
        this.translationMapBuilder = TranslationMap.builder()
                .when(queryCorrelation)
                .then(candidateCorrelation, (src, tgt, quantifiedValue) -> getTranslatedQueryValue(maxMatchMapBelow, candidateCorrelation));
        this.translationMap = translationMapBuilder.build();
    }

    /**
     * Creates a new instance of {@link MaxMatchMapTranslator}.
     * @param aliasMap The alias map, comprising both constant aliases <li>and</li> the candidate correlation identity mapping.
     * @param translationMapBuilder The translation map builder used for {@link Value} translation.
     * <br>
     * Note: This constructor is meant to be used in the context of creating {@link CompositeTranslator}, that is
     * why it has package-local visibility.
     */
    MaxMatchMapTranslator(@Nonnull final AliasMap aliasMap,
                          @Nonnull final TranslationMap.Builder translationMapBuilder) {
        super(aliasMap);
        this.translationMapBuilder = translationMapBuilder;
        this.translationMap = translationMapBuilder.build();
    }

    @Nonnull
    private Value getTranslatedQueryValue(@Nonnull final MaxMatchMap maxMatchMap,
                                          @Nonnull final CorrelationIdentifier candidateCorrelation) {
        final var belowMapping = maxMatchMap.getMapping();
        final var belowCandidateResultValue = maxMatchMap.getCandidateResultValue();
        final Map<Value, Value> pulledUpMaxMatchMap = belowMapping.entrySet().stream().map(entry -> {
            final var queryPart = entry.getKey();
            final var candidatePart = entry.getValue();
            final var pulledUpCandidatesMap = belowCandidateResultValue.pullUp(List.of(candidatePart), getAliasMap(), Set.of(), candidateCorrelation);
            final var pulledUpdateCandidatePart = pulledUpCandidatesMap.get(candidatePart);
            if (pulledUpdateCandidatePart == null) {
                throw new RecordCoreException(String.format("could not pull up %s", candidatePart));
            }
            return Map.entry(queryPart, pulledUpdateCandidatePart);
        }).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

        final var translatedQueryValueFromBelow = maxMatchMap.getQueryResultValue();
        return Verify.verifyNotNull(translatedQueryValueFromBelow.replace(valuePart ->
                pulledUpMaxMatchMap
                        .entrySet()
                        .stream()
                        .filter(maxMatchMapItem -> maxMatchMapItem.getKey().semanticEquals(valuePart, getAliasMap()))
                        .map(Map.Entry::getValue)
                        .findAny()
                        .orElse(valuePart)));
    }

    @Nonnull
    @Override
    public Value translate(@Nonnull final Value value) {
        final var result = value.translateCorrelations(translationMap);
        return result.simplify(AliasMap.emptyMap(), result.getCorrelatedTo());
    }

    @Nonnull
    TranslationMap.Builder getTranslationMapBuilder() {
        return translationMapBuilder;
    }
}
