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
import com.apple.foundationdb.record.query.plan.cascades.values.LeafValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Map used to specify translations.
 */
public class TranslationMap {
    @Nonnull
    private static final TranslationMap EMPTY = new TranslationMap(ImmutableMap.of());

    @Nonnull
    private final Map<CorrelationIdentifier, TranslationTarget> aliasToTargetMap;

    private TranslationMap(@Nonnull final Map<CorrelationIdentifier, TranslationTarget> aliasToTargetMap) {
        this.aliasToTargetMap = ImmutableMap.copyOf(aliasToTargetMap);
    }

    @Nonnull
    public Optional<AliasMap> getAliasMapMaybe() {
        return Optional.empty();
    }

    public boolean containsSourceAlias(@Nullable CorrelationIdentifier sourceAlias) {
        return aliasToTargetMap.containsKey(sourceAlias);
    }

    @Nonnull
    public Value applyTranslationFunction(@Nonnull final CorrelationIdentifier sourceAlias,
                                          @Nonnull final LeafValue leafValue) {
        final var translationTarget = Preconditions.checkNotNull(aliasToTargetMap.get(sourceAlias));
        return translationTarget.translate(sourceAlias, leafValue);
    }

    @Nonnull
    public Builder toBuilder() {
        return new Builder(aliasToTargetMap);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    public static TranslationMap empty() {
        return EMPTY;
    }

    @Nonnull
    public static TranslationMap rebaseWithAliasMap(@Nonnull final AliasMap aliasMap) {
        final var translationMapBuilder = ImmutableMap.<CorrelationIdentifier, TranslationTarget>builder();
        for (final var entry : aliasMap.entrySet()) {
            translationMapBuilder.put(entry.getKey(),
                    new TranslationTarget(((sourceAlias, leafValue) -> leafValue.rebaseLeaf(entry.getValue()))));
        }
        return new AliasMapBasedTranslationMap(translationMapBuilder.build(), aliasMap);
    }

    @Nonnull
    public static TranslationMap ofAliases(@Nonnull final CorrelationIdentifier source,
                                           @Nonnull final CorrelationIdentifier target) {
        return rebaseWithAliasMap(AliasMap.ofAliases(source, target));
    }

    @Nonnull
    public static TranslationMap compose(@Nonnull final Iterable<TranslationMap> translationMaps) {
        final var builder = TranslationMap.builder();
        for (final var translationMap : translationMaps) {
            builder.compose(translationMap);
        }
        return builder.build();
    }

    private static class AliasMapBasedTranslationMap extends TranslationMap {
        @Nonnull
        private final AliasMap aliasMap;

        public AliasMapBasedTranslationMap(@Nonnull final Map<CorrelationIdentifier, TranslationTarget> aliasToTargetMap,
                                           @Nonnull final AliasMap aliasMap) {
            super(aliasToTargetMap);
            this.aliasMap = aliasMap;
        }

        @Nonnull
        @Override
        public Optional<AliasMap> getAliasMapMaybe() {
            return Optional.of(aliasMap);
        }
    }
    
    private static class TranslationTarget {
        @Nonnull
        private final TranslationFunction translationFunction;

        public TranslationTarget(@Nonnull final TranslationFunction translationFunction) {
            this.translationFunction = translationFunction;
        }

        @Nonnull
        public Value translate(@Nonnull final CorrelationIdentifier sourceAlias,
                               @Nonnull final LeafValue leafValue) {
            return translationFunction.apply(sourceAlias, leafValue);
        }
    }

    /**
     * Functional interface to specify the translation to take place when a {@link QuantifiedValue} is encountered.
     */
    @FunctionalInterface
    public interface TranslationFunction {
        @Nonnull
        Value apply(@Nonnull CorrelationIdentifier sourceAlias,
                    @Nonnull LeafValue leafValue);
    }

    /**
     * Builder class for a translation map.
     */
    public static class Builder {
        @Nonnull
        private final Map<CorrelationIdentifier, TranslationTarget> aliasToTargetMap;

        public Builder() {
            this.aliasToTargetMap = Maps.newLinkedHashMap();
        }

        private Builder(@Nonnull final Map<CorrelationIdentifier, TranslationTarget> aliasToTargetMap) {
            this.aliasToTargetMap = Maps.newLinkedHashMap(aliasToTargetMap);
        }

        @Nonnull
        public TranslationMap build() {
            if (aliasToTargetMap.isEmpty()) {
                return TranslationMap.empty();
            }
            return new TranslationMap(aliasToTargetMap);
        }

        @Nonnull
        public Builder.When when(@Nonnull final CorrelationIdentifier sourceAlias) {
            return new When(sourceAlias);
        }

        @Nonnull
        public Builder.WhenAny whenAny(@Nonnull final Iterable<CorrelationIdentifier> sourceAliases) {
            return new WhenAny(sourceAliases);
        }

        @Nonnull
        public Builder compose(@Nonnull final TranslationMap other) {
            other.aliasToTargetMap
                    .forEach((key, value) -> {
                        Verify.verify(!aliasToTargetMap.containsKey(key));
                        aliasToTargetMap.put(key, value);
                    });
            return this;
        }

        /**
         * Class to provide fluent API, e.g. {@code .when(...).then(...)}
         */
        public class When {
            @Nonnull
            private final CorrelationIdentifier sourceAlias;

            public When(@Nonnull final CorrelationIdentifier sourceAlias) {
                this.sourceAlias = sourceAlias;
            }

            @Nonnull
            public Builder then(@Nonnull final TranslationFunction translationFunction) {
                aliasToTargetMap.put(sourceAlias, new TranslationTarget(translationFunction));
                return Builder.this;
            }
        }

        /**
         * Class to provide fluent API, e.g. {@code .when(...).then(...)}
         */
        public class WhenAny {
            @Nonnull
            private final Set<CorrelationIdentifier> sourceAliases;

            public WhenAny(@Nonnull final Iterable<CorrelationIdentifier> sourceAliases) {
                this.sourceAliases = ImmutableSet.copyOf(sourceAliases);
            }

            @Nonnull
            public Builder then(@Nonnull TranslationFunction translationFunction) {
                for (final CorrelationIdentifier sourceAlias : sourceAliases) {
                    aliasToTargetMap.put(sourceAlias, new TranslationTarget(translationFunction));
                }
                return Builder.this;
            }
        }
    }
}
