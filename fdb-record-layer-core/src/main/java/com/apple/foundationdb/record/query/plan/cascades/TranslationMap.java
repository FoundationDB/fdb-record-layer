/*
 * TranslationMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.values.LeafValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;

/**
 * Map used to specify translations.
 */
public class TranslationMap {

    @Nonnull
    private final Map<CorrelationIdentifier, TranslationTarget> aliasToTargetMap;

    private TranslationMap(@Nonnull final Map<CorrelationIdentifier, TranslationTarget> aliasToTargetMap) {
        this.aliasToTargetMap = ImmutableMap.copyOf(aliasToTargetMap);
    }

    public boolean containsSourceAlias(@Nonnull CorrelationIdentifier sourceAlias) {
        return aliasToTargetMap.containsKey(sourceAlias);
    }

    @Nonnull
    public CorrelationIdentifier getTargetAlias(@Nonnull final CorrelationIdentifier sourceAlias) {
        return Objects.requireNonNull(aliasToTargetMap.get(sourceAlias)).getTargetAlias();
    }

    @Nonnull
    public TranslationFunction getTranslationFunction(@Nonnull final CorrelationIdentifier sourceAlias) {
        return Objects.requireNonNull(aliasToTargetMap.get(sourceAlias)).getTranslationFunction();
    }

    @Nonnull
    public Value applyTranslationFunction(@Nonnull final CorrelationIdentifier sourceAlias,
                                          @Nonnull final LeafValue leafValue) {
        final var translationTarget = Objects.requireNonNull(aliasToTargetMap.get(sourceAlias));
        return translationTarget.getTranslationFunction().apply(sourceAlias, translationTarget.getTargetAlias(), leafValue);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    private static class TranslationTarget {
        @Nonnull
        private final CorrelationIdentifier targetAlias;
        @Nonnull
        private final TranslationFunction translationFunction;

        public TranslationTarget(@Nonnull final CorrelationIdentifier targetAlias, @Nonnull final TranslationFunction translationFunction) {
            this.targetAlias = targetAlias;
            this.translationFunction = translationFunction;
        }

        @Nonnull
        public CorrelationIdentifier getTargetAlias() {
            return targetAlias;
        }

        @Nonnull
        public TranslationFunction getTranslationFunction() {
            return translationFunction;
        }
    }

    /**
     * Functional interface to specify the translation to take place when a {@link QuantifiedValue} is encountered.
     */
    @FunctionalInterface
    public interface TranslationFunction {
        @Nonnull
        Value apply(@Nonnull final CorrelationIdentifier sourceAlias,
                    @Nonnull final CorrelationIdentifier targetAlias,
                    @Nonnull final LeafValue quantifiedValue);
    }

    /**
     * Builder class for a translation map.
     */
    public static class Builder {
        @Nonnull
        private final ImmutableMap.Builder<CorrelationIdentifier, TranslationTarget> translationMapBuilder;

        public Builder() {
            this.translationMapBuilder = ImmutableMap.builder();
        }

        public TranslationMap build() {
            return new TranslationMap(translationMapBuilder.build());
        }

        public Builder.When when(@Nonnull final CorrelationIdentifier sourceAlias) {
            return new When(sourceAlias);
        }

        public Builder rebaseWithAliasMap(@Nonnull final AliasMap aliasMap) {
            for (final var entry : aliasMap.entrySet()) {
                translationMapBuilder.put(entry.getKey(), new TranslationTarget(entry.getValue(),
                        ((sourceAlias, targetAlias, leafValue) -> leafValue.rebaseLeaf(targetAlias))));
            }
            return this;
        }

        /**
         * Class to provide fluent API, e.g. .when(...).then(...)
         */
        public class When {
            @Nonnull
            private final CorrelationIdentifier sourceAlias;

            public When(@Nonnull final CorrelationIdentifier sourceAlias) {
                this.sourceAlias = sourceAlias;
            }

            public Builder then(@Nonnull CorrelationIdentifier targetAlias, @Nonnull TranslationFunction translationFunction) {
                translationMapBuilder.put(sourceAlias, new TranslationTarget(targetAlias, translationFunction));
                return Builder.this;
            }
        }
    }
}
