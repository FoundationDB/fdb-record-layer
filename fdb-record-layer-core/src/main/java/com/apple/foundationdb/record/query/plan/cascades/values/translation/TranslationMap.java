/*
 * TranslationMap.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LeafValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecursivePriorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Map-like interface that is used to specify translations.
 */
public interface TranslationMap {
    @Nonnull
    Optional<AliasMap> getAliasMapMaybe();

    boolean definesOnlyIdentities();

    boolean containsSourceAlias(@Nullable CorrelationIdentifier sourceAlias);

    @Nonnull
    default CorrelationIdentifier getTargetOrDefault(@Nonnull final CorrelationIdentifier sourceAlias,
                                                     @Nonnull final CorrelationIdentifier defaultAlias) {
        final var targetFromMap = getTarget(sourceAlias);
        if (targetFromMap != null) {
            return targetFromMap;
        }
        return defaultAlias;
    }

    @Nullable
    CorrelationIdentifier getTarget(@Nonnull CorrelationIdentifier sourceAlias);

    @Nonnull
    Value applyTranslationFunction(@Nonnull CorrelationIdentifier sourceAlias,
                                   @Nonnull LeafValue leafValue);

    @Nonnull
    static RegularTranslationMap empty() {
        return RegularTranslationMap.empty();
    }

    @Nonnull
    static RegularTranslationMap.Builder regularBuilder() {
        return RegularTranslationMap.builder();
    }

    @Nonnull
    static RegularTranslationMap rebaseWithAliasMap(@Nonnull final AliasMap aliasMap) {
        return RegularTranslationMap.rebaseWithAliasMap(aliasMap);
    }

    @Nonnull
    static RegularTranslationMap ofAliases(@Nonnull final CorrelationIdentifier source,
                                           @Nonnull final CorrelationIdentifier target) {
        return RegularTranslationMap.ofAliases(source, target);
    }

    /**
     * Functional interface to specify the translation to take place when a {@link QuantifiedValue} is encountered.
     */
    @FunctionalInterface
    interface TranslationFunction {
        @Nonnull
        Value apply(@Nonnull CorrelationIdentifier sourceAlias,
                    @Nonnull LeafValue leafValue);

        @Nonnull
        static TranslationFunction adjustValueType(@Nonnull final Value translationTargetValue) {
            final var translationTargetType = translationTargetValue.getResultType();
            if (translationTargetValue instanceof QuantifiedObjectValue) {
                if (translationTargetType instanceof Type.Erasable &&
                        ((Type.Erasable)translationTargetType).isErased()) {
                    return (source, quantifiedValue) ->
                            QuantifiedObjectValue.of(((QuantifiedObjectValue)translationTargetValue).getAlias(),
                                    quantifiedValue.getResultType());
                }
            }
            Verify.verify(!(translationTargetType instanceof Type.Erasable) ||
                    !((Type.Erasable)translationTargetType).isErased());
            return (ignored, ignored2) -> {
                System.out.println(ignored2);
                if (ignored2 instanceof RecursivePriorValue) {
                    final var targetCorrelation = Iterables.getOnlyElement(translationTargetValue.getCorrelatedTo());
                    return ignored2.rebaseLeaf(targetCorrelation);
                }
                return translationTargetValue;
            };
        }
    }
}
