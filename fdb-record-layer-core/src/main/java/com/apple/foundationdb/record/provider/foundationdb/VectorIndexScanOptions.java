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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.planprotos.PVectorIndexScanOptions;
import com.apple.foundationdb.record.planprotos.PVectorIndexScanOptions.POptionEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorOptionKey;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class VectorIndexScanOptions implements PlanHashable, PlanSerializable {
    /** Engine-agnostic: whether the search returns the stored vectors alongside primary keys. */
    public static final VectorOptionKey<Boolean> VECTOR_RETURN_VECTORS =
            VectorOptionKey.ofBoolean("vectorReturnVectors", "hnswReturnVectors");
    /** HNSW-only: the size of the dynamic candidate list ({@code ef}) during the search. */
    public static final VectorOptionKey<Integer> HNSW_EF_SEARCH =
            VectorOptionKey.ofInteger("hnswEfSearch");
    /** Guardiann-only: the in-flight candidate pool size relative to {@code k}. */
    public static final VectorOptionKey<Double> GUARDIANN_CANDIDATE_POOL_FACTOR =
            VectorOptionKey.ofDouble("guardiannCandidatePoolFactor");
    /** Guardiann-only: the maximum number of cluster centroids to probe. */
    public static final VectorOptionKey<Integer> GUARDIANN_SEARCH_MAX_CLUSTERS =
            VectorOptionKey.ofInteger("guardiannSearchMaxClusters");
    /** Guardiann-only: the number of nearest clusters retained before distance-ratio pruning may drop any. */
    public static final VectorOptionKey<Integer> GUARDIANN_SEARCH_MIN_CLUSTERS_BEFORE_PRUNING =
            VectorOptionKey.ofInteger("guardiannSearchMinClustersBeforePruning");
    /** Guardiann-only: clusters beyond this multiple of the nearest centroid's distance are pruned. */
    public static final VectorOptionKey<Double> GUARDIANN_SEARCH_DISTANCE_RATIO_CUTOFF =
            VectorOptionKey.ofDouble("guardiannSearchDistanceRatioCutoff");
    /** Guardiann-only: the ring-search exploration factor for the centroid HNSW walk. */
    public static final VectorOptionKey<Integer> GUARDIANN_CENTROID_EF_RING_SEARCH =
            VectorOptionKey.ofInteger("guardiannCentroidEfRingSearch");
    /** Guardiann-only: the outward-search exploration factor for the centroid HNSW walk. */
    public static final VectorOptionKey<Integer> GUARDIANN_CENTROID_EF_OUTWARD_SEARCH =
            VectorOptionKey.ofInteger("guardiannCentroidEfOutwardSearch");
    /** Guardiann-only: the executor parallelism for the fan-out metadata/reference reads a search issues. */
    public static final VectorOptionKey<Integer> GUARDIANN_SEARCH_CONCURRENCY =
            VectorOptionKey.ofInteger("guardiannSearchConcurrency");

    private static final VectorIndexScanOptions EMPTY = new VectorIndexScanOptions(ImmutableMap.of());

    @Nonnull
    private static final ImmutableList<VectorOptionKey<?>> ALL_KEYS =
            ImmutableList.of(VECTOR_RETURN_VECTORS, HNSW_EF_SEARCH, GUARDIANN_CANDIDATE_POOL_FACTOR,
                    GUARDIANN_SEARCH_MAX_CLUSTERS, GUARDIANN_SEARCH_MIN_CLUSTERS_BEFORE_PRUNING,
                    GUARDIANN_SEARCH_DISTANCE_RATIO_CUTOFF, GUARDIANN_CENTROID_EF_RING_SEARCH,
                    GUARDIANN_CENTROID_EF_OUTWARD_SEARCH, GUARDIANN_SEARCH_CONCURRENCY);

    // Maps every wire name — canonical and legacy alias alike — to its canonical key, so a value serialized under a
    // legacy name (e.g. "hnswReturnVectors") deserializes to the same key as one written under the current name.
    @Nonnull
    private static final Map<String /* wireName */, VectorOptionKey<?>> optionsNameMap = buildNameMap();

    @Nonnull
    private static Map<String, VectorOptionKey<?>> buildNameMap() {
        final ImmutableMap.Builder<String, VectorOptionKey<?>> builder = ImmutableMap.builder();
        for (final VectorOptionKey<?> key : ALL_KEYS) {
            for (final String name : key.allNames()) {
                builder.put(name, key);
            }
        }
        return builder.build();
    }

    @Nonnull
    private final Map<VectorOptionKey<?>, Object> optionsMap;

    private VectorIndexScanOptions(@Nonnull final Map<VectorOptionKey<?>, Object> optionsMap) {
        this.optionsMap = optionsMap;
    }

    public boolean containsOption(@Nonnull final VectorOptionKey<?> key) {
        return optionsMap.containsKey(key);
    }

    @Nullable
    public <T> T getOption(@Nonnull final VectorOptionKey<T> key) {
        return key.getType().cast(optionsMap.get(key));
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
        for (final Map.Entry<VectorOptionKey<?>, Object> entry : optionsMap.entrySet()) {
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
        final Map<VectorOptionKey<?>, Object> optionsMap =
                Maps.newHashMapWithExpectedSize(vectorIndexScanOptionsProto.getOptionEntriesCount());
        for (int i = 0; i < vectorIndexScanOptionsProto.getOptionEntriesCount(); i ++) {
            final POptionEntry optionEntryProto = vectorIndexScanOptionsProto.getOptionEntries(i);
            final VectorOptionKey<?> key = Objects.requireNonNull(optionsNameMap.get(optionEntryProto.getKey()));
            if (optionsMap.containsKey(key)) {
                // Two wire names (e.g. a current name and its legacy alias) resolved to the same option; reject rather
                // than silently keeping one.
                throw new RecordCoreException("vector index scan options set the same option under more than one name")
                        .addLogInfo(LogMessageKeys.INDEX_OPTION, key.getCanonicalName());
            }
            optionsMap.put(key, LiteralKeyExpression.fromProtoValue(optionEntryProto.getValue()));
        }
        return new VectorIndexScanOptions(optionsMap);
    }

    public static class Builder {
        @Nonnull
        private final Map<VectorOptionKey<?>, Object> optionsMap;

        public Builder() {
            this(ImmutableMap.of());
        }

        public Builder(@Nonnull final Map<VectorOptionKey<?>, Object> optionsMap) {
            // creating an ordinary hashmap here since it is not null-averse
            this.optionsMap = Maps.newHashMap(optionsMap);
        }

        @Nonnull
        public <T> Builder putOption(@Nonnull final VectorOptionKey<T> key, T value) {
            optionsMap.put(key, value);
            return this;
        }

        @Nonnull
        public <T> Builder removeOption(@Nonnull final VectorOptionKey<T> key) {
            optionsMap.remove(key);
            return this;
        }

        @Nonnull
        public VectorIndexScanOptions build() {
            if (optionsMap.isEmpty()) {
                return EMPTY;
            }
            return new VectorIndexScanOptions(Collections.unmodifiableMap(Maps.newHashMap(optionsMap)));
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
