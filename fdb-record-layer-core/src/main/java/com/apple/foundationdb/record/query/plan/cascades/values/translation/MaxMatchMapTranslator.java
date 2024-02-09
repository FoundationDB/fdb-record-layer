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

public class MaxMatchMapTranslator extends Translator {

    @Nonnull
    private final TranslationMap.Builder translationMapBuilder;

    public MaxMatchMapTranslator(@Nonnull final MaxMatchMap maxMatchMapBelow,
                                 @Nonnull final CorrelationIdentifier queryCorrelation,
                                 @Nonnull final CorrelationIdentifier candidateCorrelation,
                                 @Nonnull final AliasMap constantAliases) {
        super(constantAliases.derived().put(candidateCorrelation, candidateCorrelation).build());
        this.translationMapBuilder = TranslationMap.builder()
                .when(queryCorrelation)
                .then(candidateCorrelation, (src, tgt, quantifiedValue) -> getTranslatedQueryValue(maxMatchMapBelow, candidateCorrelation));
    }

    public MaxMatchMapTranslator(@Nonnull final CorrelationIdentifier candidateCorrelation,
                                 @Nonnull final AliasMap constantAliases,
                                 @Nonnull TranslationMap.Builder translationMapBuilder) {
        super(constantAliases.derived().put(candidateCorrelation, candidateCorrelation).build());
        this.translationMapBuilder = translationMapBuilder;
    }

    MaxMatchMapTranslator(@Nonnull final AliasMap aliasMap,
                                 @Nonnull TranslationMap.Builder translationMapBuilder) {
        super(aliasMap);
        this.translationMapBuilder = translationMapBuilder;
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
        final var result = value.translateCorrelations(translationMapBuilder.build());
        return result.simplify(AliasMap.emptyMap(), result.getCorrelatedTo());
    }

    @Nonnull
    TranslationMap.Builder getTranslationMapBuilder() {
        return translationMapBuilder;
    }
}
