/*
 * RegularTranslationMap.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
 * Map used to specify translations using a proper immutable backing map.
 */
public class RegularTranslationMap implements TranslationMap {
    @Nonnull
    private static final RegularTranslationMap EMPTY = new RegularTranslationMap(ImmutableMap.of());

    @Nonnull
    private final Map<CorrelationIdentifier, TranslationFunction> aliasToFunctionMap;

    private RegularTranslationMap(@Nonnull final Map<CorrelationIdentifier, TranslationFunction> aliasToFunctionMap) {
        this.aliasToFunctionMap = ImmutableMap.copyOf(aliasToFunctionMap);
    }

    @Nonnull
    @Override
    public Optional<AliasMap> getAliasMapMaybe() {
        return Optional.empty();
    }

    @Override
    public boolean definesOnlyIdentities() {
        return getAliasMapMaybe().map(a -> a.definesOnlyIdentities() && aliasToFunctionMap.isEmpty())
                .orElseGet(aliasToFunctionMap::isEmpty);
    }

    @Override
    public boolean containsSourceAlias(@Nullable CorrelationIdentifier sourceAlias) {
        return aliasToFunctionMap.containsKey(sourceAlias);
    }

    @Nullable
    @Override
    public CorrelationIdentifier getTarget(@Nonnull final CorrelationIdentifier sourceAlias) {
        AliasMap aliasMap = getAliasMapMaybe().orElse(null);
        if (aliasMap == null) {
            return null;
        }
        return aliasMap.getTarget(sourceAlias);
    }

    @Nonnull
    @Override
    public Value applyTranslationFunction(@Nonnull final CorrelationIdentifier sourceAlias,
                                          @Nonnull final LeafValue leafValue) {
        final var translationFunction = Preconditions.checkNotNull(aliasToFunctionMap.get(sourceAlias));
        return translationFunction.apply(sourceAlias, leafValue);
    }

    @Nonnull
    public Builder toBuilder() {
        return new Builder(aliasToFunctionMap);
    }

    @Nonnull
    public static RegularTranslationMap empty() {
        return EMPTY;
    }

    @Nonnull
    public static RegularTranslationMap.Builder builder() {
        return new RegularTranslationMap.Builder();
    }

    @Nonnull
    public static RegularTranslationMap rebaseWithAliasMap(@Nonnull final AliasMap aliasMap) {
        final var translationMapBuilder =
                ImmutableMap.<CorrelationIdentifier, TranslationFunction>builder();
        for (final var entry : aliasMap.entrySet()) {
            translationMapBuilder.put(entry.getKey(),
                    (sourceAlias, leafValue) -> leafValue.rebaseLeaf(entry.getValue()));
        }
        return new AliasMapBasedTranslationMap(translationMapBuilder.build(), aliasMap);
    }

    @Nonnull
    public static RegularTranslationMap ofAliases(@Nonnull final CorrelationIdentifier source,
                                                  @Nonnull final CorrelationIdentifier target) {
        return rebaseWithAliasMap(AliasMap.ofAliases(source, target));
    }

    @Nonnull
    public static RegularTranslationMap compose(@Nonnull final Iterable<RegularTranslationMap> translationMaps) {
        final var builder = builder();
        for (final var translationMap : translationMaps) {
            builder.compose(translationMap);
        }
        return builder.build();
    }

    private static class AliasMapBasedTranslationMap extends RegularTranslationMap {
        @Nonnull
        private final AliasMap aliasMap;

        private AliasMapBasedTranslationMap(@Nonnull final Map<CorrelationIdentifier, TranslationFunction> aliasToFunctionMap,
                                            @Nonnull final AliasMap aliasMap) {
            super(aliasToFunctionMap);
            this.aliasMap = aliasMap;
        }

        @Nonnull
        @Override
        public Optional<AliasMap> getAliasMapMaybe() {
            return Optional.of(aliasMap);
        }
    }

    /**
     * Builder class for a translation map.
     */
    public static class Builder {
        @Nonnull
        private final Map<CorrelationIdentifier, TranslationFunction> aliasToFunctionMap;
        @Nonnull
        private final AliasMap.Builder aliasMapBuilder = AliasMap.builder();

        private Builder() {
            this(Maps.newLinkedHashMap(), AliasMap.emptyMap());
        }

        private Builder(@Nonnull final Map<CorrelationIdentifier, TranslationFunction> aliasToFunctionMap) {
            this(aliasToFunctionMap, AliasMap.emptyMap());
        }

        private Builder(@Nonnull final Map<CorrelationIdentifier, TranslationFunction> aliasToFunctionMap,
                        @Nonnull final AliasMap aliasMap) {
            this.aliasToFunctionMap = Maps.newLinkedHashMap(aliasToFunctionMap);
            this.aliasMapBuilder.putAll(aliasMap);
        }

        @Nonnull
        public RegularTranslationMap build() {
            if (aliasToFunctionMap.isEmpty()) {
                return AliasMapBasedTranslationMap.empty();
            }
            final var aliasMap = aliasMapBuilder.build();
            return new AliasMapBasedTranslationMap(aliasToFunctionMap, aliasMap);
        }

        @Nonnull
        public When when(@Nonnull final CorrelationIdentifier sourceAlias) {
            return new When(sourceAlias);
        }

        @Nonnull
        public WhenAny whenAny(@Nonnull final Iterable<CorrelationIdentifier> sourceAliases) {
            return new WhenAny(sourceAliases);
        }

        @Nonnull
        public Builder compose(@Nonnull final RegularTranslationMap other) {
            other.aliasToFunctionMap
                    .forEach((key, value) -> {
                        Verify.verify(!aliasToFunctionMap.containsKey(key));
                        aliasToFunctionMap.put(key, value);
                    });
            aliasMapBuilder.putAll(other.getAliasMapMaybe().orElse(AliasMap.emptyMap()));
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
                aliasToFunctionMap.put(sourceAlias, translationFunction);
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
                    aliasToFunctionMap.put(sourceAlias, translationFunction);
                }
                return Builder.this;
            }
        }
    }
}
