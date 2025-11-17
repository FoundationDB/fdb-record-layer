/*
 * VectorIndexScanOptions.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.planprotos.PVectorIndexScanOptions;
import com.apple.foundationdb.record.planprotos.PVectorIndexScanOptions.POptionEntry;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class VectorIndexScanOptions implements PlanHashable, PlanSerializable {
    public static final OptionKey<Integer> HNSW_EF_SEARCH = new OptionKey<>("hnswEfSearch", Integer.class);
    public static final OptionKey<Boolean> HNSW_RETURN_VECTORS = new OptionKey<>("hnswReturnVectors", Boolean.class);

    private static final VectorIndexScanOptions EMPTY = new VectorIndexScanOptions(ImmutableMap.of());

    private static final Map<String /* optionName */, OptionKey<?>> optionsNameMap =
            ImmutableMap.of(HNSW_EF_SEARCH.getOptionName(), HNSW_EF_SEARCH,
                    HNSW_RETURN_VECTORS.getOptionName(), HNSW_RETURN_VECTORS);

    @Nonnull
    private final Map<OptionKey<?>, Object> optionsMap;

    private VectorIndexScanOptions(@Nonnull final Map<OptionKey<?>, Object> optionsMap) {
        // creating an ordinary hashmap here since it is not null-averse
        this.optionsMap = optionsMap;
    }

    public boolean containsOption(@Nonnull final OptionKey<?> key) {
        return optionsMap.containsKey(key);
    }

    @Nullable
    public <T> T getOption(@Nonnull final OptionKey<T> key) {
        return key.getClazz().cast(optionsMap.get(key));
    }

    @Nonnull
    public VectorIndexScanOptions.Builder toBuilder() {
        return new Builder(optionsMap);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectPlanHash(hashMode, optionsMap);
    }

    @Nonnull
    @Override
    public PVectorIndexScanOptions toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PVectorIndexScanOptions.Builder scanOptionsBuilder = PVectorIndexScanOptions.newBuilder();
        for (final Map.Entry<OptionKey<?>, Object> entry : optionsMap.entrySet()) {
            scanOptionsBuilder.addOptionEntries(
                    POptionEntry.newBuilder()
                            .setKey(entry.getKey().getOptionName())
                            .setValue(LiteralKeyExpression.toProtoValue(entry.getValue())))
                            .build();
        }

        return scanOptionsBuilder.build();
    }

    @Nonnull
    public ExplainTokensWithPrecedence explain() {
        final var explainTokens =
                new ExplainTokens().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        optionsMap.entrySet()
                                .stream()
                                .map(entry ->
                                        new ExplainTokens().addIdentifier(entry.getKey().getOptionName()).addKeyword(":")
                                                .addWhitespace().addToString(entry.getValue()))
                                .collect(Collectors.toList()));

        return ExplainTokensWithPrecedence.of(new ExplainTokens().addOptionalWhitespace().addOpeningSquareBracket()
                .addNested(explainTokens).addOptionalWhitespace().addClosingSquareBracket());
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof VectorIndexScanOptions)) {
            return false;
        }
        final VectorIndexScanOptions that = (VectorIndexScanOptions)o;
        return Objects.equals(optionsMap, that.optionsMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(optionsMap);
    }

    @Override
    public String toString() {
        return explain().getExplainTokens().render(DefaultExplainFormatter.forDebugging()).toString();
    }

    @Nonnull
    public static VectorIndexScanOptions.Builder builder() {
        return new Builder();
    }

    @Nonnull
    public static VectorIndexScanOptions empty() {
        return EMPTY;
    }

    @Nonnull
    public static VectorIndexScanOptions fromProto(@Nonnull final PVectorIndexScanOptions vectorIndexScanOptionsProto) {
        final Map<OptionKey<?>, Object> optionsMap =
                Maps.newHashMapWithExpectedSize(vectorIndexScanOptionsProto.getOptionEntriesCount());
        for (int i = 0; i < vectorIndexScanOptionsProto.getOptionEntriesCount(); i ++) {
            final POptionEntry optionEntryProto = vectorIndexScanOptionsProto.getOptionEntries(i);
            optionsMap.put(Objects.requireNonNull(optionsNameMap.get(optionEntryProto.getKey())),
                    LiteralKeyExpression.fromProtoValue(optionEntryProto.getValue()));
        }
        return new VectorIndexScanOptions(optionsMap);
    }

    public static class Builder {
        @Nonnull
        private final Map<OptionKey<?>, Object> optionsMap;

        public Builder() {
            this(ImmutableMap.of());
        }

        public Builder(@Nonnull final Map<OptionKey<?>, Object> optionsMap) {
            this.optionsMap = Maps.newHashMap(optionsMap);
        }

        @Nonnull
        public <T> Builder putOption(@Nonnull final OptionKey<T> key, T value) {
            optionsMap.put(key, value);
            return this;
        }

        @Nonnull
        public <T> Builder removeOption(@Nonnull final OptionKey<T> key) {
            optionsMap.remove(key);
            return this;
        }

        @Nonnull
        public VectorIndexScanOptions build() {
            return new VectorIndexScanOptions(Maps.newHashMap(optionsMap));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Builder)) {
                return false;
            }
            final Builder builder = (Builder)o;
            return Objects.equals(optionsMap, builder.optionsMap);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(optionsMap);
        }
    }

    public static class OptionKey<T> {
        @Nonnull
        private final String optionName;
        @Nonnull
        private final Class<T> clazz;

        public OptionKey(@Nonnull final String optionName, @Nonnull final Class<T> clazz) {
            this.optionName = optionName;
            this.clazz = clazz;
        }

        @Nonnull
        public String getOptionName() {
            return optionName;
        }

        @Nonnull
        public Class<T> getClazz() {
            return clazz;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof OptionKey)) {
                return false;
            }
            final OptionKey<?> optionKey = (OptionKey<?>)o;
            return Objects.equals(optionName, optionKey.optionName) && clazz == optionKey.clazz;
        }

        @Override
        public int hashCode() {
            return Objects.hash(optionName);
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PVectorIndexScanOptions, VectorIndexScanOptions> {
        @Nonnull
        @Override
        public Class<PVectorIndexScanOptions> getProtoMessageClass() {
            return PVectorIndexScanOptions.class;
        }

        @Nonnull
        @Override
        public VectorIndexScanOptions fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PVectorIndexScanOptions vectorIndexScanOptionsProto) {
            return VectorIndexScanOptions.fromProto(vectorIndexScanOptionsProto);
        }
    }
}
