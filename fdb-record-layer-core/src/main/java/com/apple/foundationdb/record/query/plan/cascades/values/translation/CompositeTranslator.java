/*
 * CompositeTranslator.java
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
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;

/**
 * This class represents a functional composition of a number of {@link Translator}s.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class CompositeTranslator extends Translator {

    @Nonnull
    private final Optional<TrivialTranslator> trivialTranslatorOptional;

    @Nonnull
    private final Optional<MaxMatchMapTranslator> maxMatchMapTranslatorOptional;

    /**
     * Creates a new instance of {@link CompositeTranslator}.
     *
     * @param translators The translators which compose this translator.
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional for performance.
    public CompositeTranslator(@Nonnull final Collection<Translator> translators) {
        super(collectAliasMap(translators));
        Verify.verify(!translators.isEmpty());
        final var compositeTranslationAliasMapBuilder = AliasMap.builder();
        final var compositeTranslationMapBuilder = TranslationMap.builder();
        for (final var translator : translators) {
            if (translator instanceof TrivialTranslator) {
                compositeTranslationAliasMapBuilder.putAll(((TrivialTranslator)translator).getTranslationAliasMap());
            } else if (translator instanceof MaxMatchMapTranslator) {
                compositeTranslationMapBuilder.merge(((MaxMatchMapTranslator)translator).getTranslationMapBuilder());
            } else if (translator instanceof CompositeTranslator) {
                final var compositeTranslator = (CompositeTranslator)translator;
                compositeTranslator.trivialTranslatorOptional.map(trivialTranslator -> compositeTranslationAliasMapBuilder.putAll(trivialTranslator.getTranslationAliasMap()));
                compositeTranslator.maxMatchMapTranslatorOptional.map(maxMatchMapTranslator -> compositeTranslationMapBuilder.merge(maxMatchMapTranslator.getTranslationMapBuilder()));
            } else {
                throw new RecordCoreException(String.format("Unexpected translator type %s", translator.getClass().getSimpleName()));
            }
        }

        final var compositeTranslationAliasMap = compositeTranslationAliasMapBuilder.build();
        final var compositeTranslationMap = compositeTranslationMapBuilder.build();

        if (compositeTranslationAliasMap != AliasMap.emptyMap()) {
            trivialTranslatorOptional = Optional.of(new TrivialTranslator(getConstantAliasMap(), compositeTranslationAliasMap));
        } else {
            trivialTranslatorOptional = Optional.empty();
        }

        if (compositeTranslationMap != TranslationMap.empty()) {
            maxMatchMapTranslatorOptional = Optional.of(new MaxMatchMapTranslator(getConstantAliasMap(), compositeTranslationMapBuilder));
        } else {
            maxMatchMapTranslatorOptional = Optional.empty();
        }
    }

    @Nonnull
    private static AliasMap collectAliasMap(@Nonnull final Collection<Translator> translators) {
        final var aliasMapBuilder = AliasMap.builder();
        translators.forEach(translator -> aliasMapBuilder.putAll(translator.getConstantAliasMap()));
        return aliasMapBuilder.build();
    }

    @Nonnull
    @Override
    public Value translate(@Nonnull final Value value) {
        var result = value;
        if (trivialTranslatorOptional.isPresent()) {
            result = trivialTranslatorOptional.get().translate(result);
        }
        if (maxMatchMapTranslatorOptional.isPresent()) {
            result = maxMatchMapTranslatorOptional.get().translate(result);
        }
        return result;
    }
}
