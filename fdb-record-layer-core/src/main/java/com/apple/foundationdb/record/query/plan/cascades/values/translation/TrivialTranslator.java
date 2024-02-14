/*
 * SimpleTranslator.java
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;

/**
 * A {@code Translator} that simply translates a given {@code Value} by replacing its {@code queryCorrelation} with the
 * specified {@code candidateCorrelation}. It does not perform any sub-{@link Value} replacement.
 */
public class TrivialTranslator extends Translator {

    @Nonnull
    private final AliasMap translationAliasMap;

    @Nonnull
    private final TranslationMap translationMap;

    /**
     * Creates a new instance of the {@link TrivialTranslator}.
     * @param queryCorrelation The query correlation to be replaced in the {@link Value}.
     * @param candidateCorrelation The replacement correlation.
     * @param constantAliases A list of constant aliases, i.e. aliases the remain unchanged after translating a {@link Value}.
     */
    public TrivialTranslator(@Nonnull final CorrelationIdentifier queryCorrelation,
                             @Nonnull final CorrelationIdentifier candidateCorrelation,
                             @Nonnull final AliasMap constantAliases) {
        super(constantAliases.toBuilder().put(candidateCorrelation, candidateCorrelation).build());
        this.translationAliasMap = AliasMap.of(queryCorrelation, candidateCorrelation);
        this.translationMap = TranslationMap.rebaseWithAliasMap(translationAliasMap);
    }

    /**
     * Creates a new instance of the {@link TrivialTranslator}.
     * @param aliasMap The alias map, comprising both constant aliases <li>and</li> the candidate correlation identity mapping.
     * @param translationAliasMap The alias translation map used to translate the correlations in the given {@link Value}.
     * <br>
     * Note: This constructor is meant to be used in the context of creating {@link CompositeTranslator}, that is
     * why it has package-local visibility.
     */
    TrivialTranslator(@Nonnull final AliasMap aliasMap,
                      @Nonnull final AliasMap translationAliasMap) {
        super(aliasMap);
        this.translationAliasMap = translationAliasMap;
        this.translationMap = TranslationMap.rebaseWithAliasMap(translationAliasMap);
    }

    @Nonnull
    @Override
    public Value translate(@Nonnull final Value value) {
        return value.translateCorrelations(translationMap);
    }

    @Nonnull
    AliasMap getTranslationAliasMap() {
        return translationAliasMap;
    }
}
