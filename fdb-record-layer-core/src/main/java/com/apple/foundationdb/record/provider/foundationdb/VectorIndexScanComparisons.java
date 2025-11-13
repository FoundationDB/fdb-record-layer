/*
 * MultidimensionalIndexScanComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.planprotos.PVectorIndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons.DistanceRankValueComparison;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * {@link ScanComparisons} for use in a multidimensional index scan.
 */
@API(API.Status.UNSTABLE)
public class VectorIndexScanComparisons implements IndexScanParameters {
    @Nonnull
    private final ScanComparisons prefixScanComparisons;
    @Nonnull
    private final DistanceRankValueComparison distanceRankValueComparison;
    @Nonnull
    private final ScanComparisons suffixScanComparisons;

    public VectorIndexScanComparisons(@Nonnull final ScanComparisons prefixScanComparisons,
                                      @Nonnull final DistanceRankValueComparison distanceRankValueComparison,
                                      @Nonnull final ScanComparisons suffixKeyComparisonRanges) {
        this.prefixScanComparisons = prefixScanComparisons;
        this.distanceRankValueComparison = distanceRankValueComparison;
        this.suffixScanComparisons = suffixKeyComparisonRanges;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_VALUE;
    }

    @Nonnull
    public ScanComparisons getPrefixScanComparisons() {
        return prefixScanComparisons;
    }

    @Nonnull
    public DistanceRankValueComparison getDistanceRankValueComparison() {
        return distanceRankValueComparison;
    }

    @Nonnull
    public ScanComparisons getSuffixScanComparisons() {
        return suffixScanComparisons;
    }

    @Nonnull
    @Override
    public VectorIndexScanBounds bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Index index,
                                      @Nonnull final EvaluationContext context) {
        return new VectorIndexScanBounds(prefixScanComparisons.toTupleRange(store, context),
                distanceRankValueComparison.getType(), distanceRankValueComparison.getVector(store, context),
                distanceRankValueComparison.getLimit(store, context), suffixScanComparisons.toTupleRange(store, context));
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, prefixScanComparisons, distanceRankValueComparison,
                suffixScanComparisons);
    }

    @Override
    public boolean isUnique(@Nonnull Index index) {
        return prefixScanComparisons.isEquality() && prefixScanComparisons.size() == index.getColumnSize();
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain() {
        @Nullable var tupleRange = prefixScanComparisons.toTupleRangeWithoutContext();
        final var prefix = tupleRange == null
                           ? prefixScanComparisons.explain().getExplainTokens()
                           : new ExplainTokens().addToString(tupleRange);

        ExplainTokens distanceRank;
        try {
            @Nullable var vector = distanceRankValueComparison.getVector(null, null);
            int limit = distanceRankValueComparison.getLimit(null, null);
            distanceRank =
                    new ExplainTokens().addNested(vector == null
                                                  ? new ExplainTokens().addKeyword("null")
                                                  : new ExplainTokens().addToString(vector));
            distanceRank.addKeyword(distanceRankValueComparison.getType().name()).addWhitespace().addToString(limit);
        } catch (final Comparisons.EvaluationContextRequiredException e) {
            distanceRank =
                    new ExplainTokens().addNested(distanceRankValueComparison.explain().getExplainTokens());
        }

        tupleRange = suffixScanComparisons.toTupleRangeWithoutContext();
        final var suffix = tupleRange == null
                           ? suffixScanComparisons.explain().getExplainTokens()
                           : new ExplainTokens().addToString(tupleRange);

        return ExplainTokensWithPrecedence.of(prefix.addOptionalWhitespace().addToString(":{").addOptionalWhitespace()
                .addNested(distanceRank).addOptionalWhitespace().addToString("}:").addOptionalWhitespace().addNested(suffix));
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        @Nullable TupleRange tupleRange = prefixScanComparisons.toTupleRangeWithoutContext();
        if (tupleRange != null) {
            detailsBuilder.add("prefix: " + tupleRange.getLowEndpoint().toString(false) + "{{plow}}, {{phigh}}" + tupleRange.getHighEndpoint().toString(true));
            attributeMapBuilder.put("plow", Attribute.gml(tupleRange.getLow() == null ? "-∞" : tupleRange.getLow().toString()));
            attributeMapBuilder.put("phigh", Attribute.gml(tupleRange.getHigh() == null ? "∞" : tupleRange.getHigh().toString()));
        } else {
            detailsBuilder.add("prefix comparisons: {{pcomparisons}}");
            attributeMapBuilder.put("pcomparisons", Attribute.gml(prefixScanComparisons.toString()));
        }

        try {
            @Nullable var vector = distanceRankValueComparison.getVector(null, null);
            int limit = distanceRankValueComparison.getLimit(null, null);
            detailsBuilder.add("distanceRank: {{vector}} {{type}} {{limit}}");
            attributeMapBuilder.put("vector", Attribute.gml(String.valueOf(vector)));
            attributeMapBuilder.put("type", Attribute.gml(distanceRankValueComparison.getType()));
            attributeMapBuilder.put("limit", Attribute.gml(limit));
        } catch (final Comparisons.EvaluationContextRequiredException e) {
            detailsBuilder.add("distanceRank: {{comparison}}");
            attributeMapBuilder.put("comparison", Attribute.gml(distanceRankValueComparison));
        }

        tupleRange = suffixScanComparisons.toTupleRangeWithoutContext();
        if (tupleRange != null) {
            detailsBuilder.add("suffix: " + tupleRange.getLowEndpoint().toString(false) + "{{slow}}, {{shigh}}" + tupleRange.getHighEndpoint().toString(true));
            attributeMapBuilder.put("slow", Attribute.gml(tupleRange.getLow() == null ? "-∞" : tupleRange.getLow().toString()));
            attributeMapBuilder.put("shigh", Attribute.gml(tupleRange.getHigh() == null ? "∞" : tupleRange.getHigh().toString()));
        } else {
            detailsBuilder.add("suffix comparisons: {{scomparisons}}");
            attributeMapBuilder.put("scomparisons", Attribute.gml(suffixScanComparisons.toString()));
        }
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> correlatedToBuilder = ImmutableSet.builder();
        correlatedToBuilder.addAll(prefixScanComparisons.getCorrelatedTo());
        correlatedToBuilder.addAll(distanceRankValueComparison.getCorrelatedTo());
        correlatedToBuilder.addAll(suffixScanComparisons.getCorrelatedTo());
        return correlatedToBuilder.build();
    }

    @Nonnull
    @Override
    public IndexScanParameters rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap), false);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final VectorIndexScanComparisons that = (VectorIndexScanComparisons)other;

        if (!prefixScanComparisons.semanticEquals(that.prefixScanComparisons, aliasMap)) {
            return false;
        }

        if (!distanceRankValueComparison.semanticEquals(that.distanceRankValueComparison, aliasMap)) {
            return false;
        }
        return suffixScanComparisons.semanticEquals(that.suffixScanComparisons, aliasMap);
    }

    @Override
    public int semanticHashCode() {
        int hashCode = prefixScanComparisons.semanticHashCode();
        hashCode = 31 * hashCode + distanceRankValueComparison.semanticHashCode();
        return 31 * hashCode + suffixScanComparisons.semanticHashCode();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     final boolean shouldSimplifyValues) {
        final ScanComparisons translatedPrefixScanComparisons =
                prefixScanComparisons.translateCorrelations(translationMap, shouldSimplifyValues);

        final DistanceRankValueComparison translatedDistanceRankValueComparison =
                distanceRankValueComparison.translateCorrelations(translationMap, shouldSimplifyValues);

        final ScanComparisons translatedSuffixKeyScanComparisons =
                suffixScanComparisons.translateCorrelations(translationMap, shouldSimplifyValues);

        if (translatedPrefixScanComparisons != prefixScanComparisons ||
                translatedDistanceRankValueComparison != distanceRankValueComparison ||
                translatedSuffixKeyScanComparisons != suffixScanComparisons) {
            return withComparisons(translatedPrefixScanComparisons, translatedDistanceRankValueComparison,
                    translatedSuffixKeyScanComparisons);
        }
        return this;
    }

    @Nonnull
    protected VectorIndexScanComparisons withComparisons(@Nonnull final ScanComparisons prefixScanComparisons,
                                                         @Nonnull final DistanceRankValueComparison distanceRankValueComparison,
                                                         @Nonnull final ScanComparisons suffixKeyScanComparisons) {
        return new VectorIndexScanComparisons(prefixScanComparisons, distanceRankValueComparison,
                suffixKeyScanComparisons);
    }

    @Override
    public String toString() {
        return "BY_VALUE(VECTOR):" + prefixScanComparisons + ":" + distanceRankValueComparison + ":" + suffixScanComparisons;
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public PVectorIndexScanComparisons toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PVectorIndexScanComparisons.Builder builder = PVectorIndexScanComparisons.newBuilder();
        builder.setPrefixScanComparisons(prefixScanComparisons.toProto(serializationContext));
        builder.setDistanceRankValueComparison(distanceRankValueComparison.toProto(serializationContext));
        builder.setSuffixScanComparisons(suffixScanComparisons.toProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder().setVectorIndexScanComparisons(toProto(serializationContext)).build();
    }

    @Nonnull
    public static VectorIndexScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PVectorIndexScanComparisons vectorIndexScanComparisonsProto) {
        return new VectorIndexScanComparisons(ScanComparisons.fromProto(serializationContext,
                Objects.requireNonNull(vectorIndexScanComparisonsProto.getPrefixScanComparisons())),
                Objects.requireNonNull(DistanceRankValueComparison.fromProto(serializationContext, vectorIndexScanComparisonsProto.getDistanceRankValueComparison())),
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(vectorIndexScanComparisonsProto.getSuffixScanComparisons())));
    }

    @Nonnull
    public static VectorIndexScanComparisons byValue(@Nullable ScanComparisons prefixScanComparisons,
                                                     @Nonnull final DistanceRankValueComparison distanceRankValueComparison,
                                                     @Nullable ScanComparisons suffixKeyScanComparisons) {
        if (prefixScanComparisons == null) {
            prefixScanComparisons = ScanComparisons.EMPTY;
        }

        if (suffixKeyScanComparisons == null) {
            suffixKeyScanComparisons = ScanComparisons.EMPTY;
        }

        return new VectorIndexScanComparisons(prefixScanComparisons, distanceRankValueComparison, suffixKeyScanComparisons);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PVectorIndexScanComparisons, VectorIndexScanComparisons> {
        @Nonnull
        @Override
        public Class<PVectorIndexScanComparisons> getProtoMessageClass() {
            return PVectorIndexScanComparisons.class;
        }

        @Nonnull
        @Override
        public VectorIndexScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                    @Nonnull final PVectorIndexScanComparisons vectorIndexScanComparisonsProto) {
            return VectorIndexScanComparisons.fromProto(serializationContext, vectorIndexScanComparisonsProto);
        }
    }
}
