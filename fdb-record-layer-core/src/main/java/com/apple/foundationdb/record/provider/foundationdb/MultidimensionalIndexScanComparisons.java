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
import com.apple.foundationdb.record.planprotos.PMultidimensionalIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds.Hypercube;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link ScanComparisons} for use in a multidimensional index scan.
 */
@API(API.Status.MAINTAINED)
public class MultidimensionalIndexScanComparisons implements IndexScanParameters {
    @Nonnull
    private final ScanComparisons prefixScanComparisons;
    @Nonnull
    private final List<ScanComparisons> dimensionsScanComparisons;
    @Nonnull
    private final ScanComparisons suffixScanComparisons;

    public MultidimensionalIndexScanComparisons(@Nonnull final ScanComparisons prefixScanComparisons,
                                                @Nonnull final List<ScanComparisons> dimensionsScanComparisons,
                                                @Nonnull final ScanComparisons suffixKeyComparisonRanges) {
        this.prefixScanComparisons = prefixScanComparisons;
        this.dimensionsScanComparisons = dimensionsScanComparisons;
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
    public List<ScanComparisons> getDimensionsScanComparisons() {
        return dimensionsScanComparisons;
    }

    @Nonnull
    public ScanComparisons getSuffixScanComparisons() {
        return suffixScanComparisons;
    }

    @Nonnull
    @Override
    public MultidimensionalIndexScanBounds bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Index index,
                                                @Nonnull final EvaluationContext context) {
        final ImmutableList.Builder<TupleRange> dimensionsTupleRangeBuilder = ImmutableList.builder();
        for (final ScanComparisons dimensionScanComparison : dimensionsScanComparisons) {
            dimensionsTupleRangeBuilder.add(dimensionScanComparison.toTupleRange(store, context));
        }
        final Hypercube hypercube = new Hypercube(dimensionsTupleRangeBuilder.build());
        return new MultidimensionalIndexScanBounds(prefixScanComparisons.toTupleRange(store, context),
                hypercube, suffixScanComparisons.toTupleRange(store, context));
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, prefixScanComparisons, dimensionsScanComparisons,
                suffixScanComparisons);
    }

    @Override
    public boolean isUnique(@Nonnull Index index) {
        return prefixScanComparisons.isEquality() && prefixScanComparisons.size() == index.getColumnSize();
    }

    @Nonnull
    @Override
    public String getScanDetails() {
        @Nullable TupleRange tupleRange = prefixScanComparisons.toTupleRangeWithoutContext();
        final String prefix = tupleRange == null ? prefixScanComparisons.toString() : tupleRange.toString();

        final String dimensions =
                dimensionsScanComparisons.stream()
                        .map(dimensionScanComparisons -> {
                            @Nullable final TupleRange dimensionTupleRange = dimensionScanComparisons.toTupleRangeWithoutContext();
                            return dimensionTupleRange == null ? dimensionScanComparisons.toString() : dimensionTupleRange.toString();
                        })
                        .collect(Collectors.joining(","));

        tupleRange = suffixScanComparisons.toTupleRangeWithoutContext();
        final String suffix = tupleRange == null ? suffixScanComparisons.toString() : tupleRange.toString();

        return prefix + ":{" + dimensions + "}:" + suffix;
    }

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

        for (int d = 0; d < dimensionsScanComparisons.size(); d++) {
            final ScanComparisons dimensionScanComparisons = dimensionsScanComparisons.get(d);
            tupleRange = dimensionScanComparisons.toTupleRangeWithoutContext();
            if (tupleRange != null) {
                detailsBuilder.add("dim" + d + ": " + tupleRange.getLowEndpoint().toString(false) + "{{dlow" + d + "}}, {{dhigh" + d + "}}" + tupleRange.getHighEndpoint().toString(true));
                attributeMapBuilder.put("dlow" + d, Attribute.gml(tupleRange.getLow() == null ? "-∞" : tupleRange.getLow().toString()));
                attributeMapBuilder.put("dhigh" + d, Attribute.gml(tupleRange.getHigh() == null ? "∞" : tupleRange.getHigh().toString()));
            } else {
                detailsBuilder.add("dim" + d + " comparisons: " + "{{dcomparisons" + d + "}}");
                attributeMapBuilder.put("dcomparisons" + d, Attribute.gml(dimensionScanComparisons.toString()));
            }
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
        correlatedToBuilder.addAll(dimensionsScanComparisons.stream()
                .flatMap(dimensionScanComparison -> dimensionScanComparison.getCorrelatedTo().stream()).iterator());
        correlatedToBuilder.addAll(suffixScanComparisons.getCorrelatedTo());
        return correlatedToBuilder.build();
    }

    @Nonnull
    @Override
    public IndexScanParameters rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
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

        final MultidimensionalIndexScanComparisons that = (MultidimensionalIndexScanComparisons)other;

        if (!prefixScanComparisons.semanticEquals(that.prefixScanComparisons, aliasMap)) {
            return false;
        }
        if (dimensionsScanComparisons.size() != that.dimensionsScanComparisons.size()) {
            return false;
        }
        for (int i = 0; i < dimensionsScanComparisons.size(); i++) {
            final ScanComparisons dimensionScanComparison = dimensionsScanComparisons.get(i);
            final ScanComparisons otherDimensionScanComparison = that.dimensionsScanComparisons.get(i);
            if (!dimensionScanComparison.semanticEquals(otherDimensionScanComparison, aliasMap)) {
                return false;
            }
        }
        return suffixScanComparisons.semanticEquals(that.suffixScanComparisons, aliasMap);
    }

    @Override
    public int semanticHashCode() {
        int hashCode = prefixScanComparisons.semanticHashCode();
        for (final ScanComparisons dimensionScanComparison : dimensionsScanComparisons) {
            hashCode = 31 * hashCode + dimensionScanComparison.semanticHashCode();
        }
        return 31 * hashCode + suffixScanComparisons.semanticHashCode();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap) {
        final ScanComparisons translatedPrefixScanComparisons = prefixScanComparisons.translateCorrelations(translationMap);

        final ImmutableList.Builder<ScanComparisons> translatedDimensionScanComparisonBuilder = ImmutableList.builder();
        boolean isSameDimensionsScanComparisons = true;
        for (final ScanComparisons dimensionScanComparisons : dimensionsScanComparisons) {
            final ScanComparisons translatedDimensionScanComparison = dimensionScanComparisons.translateCorrelations(translationMap);
            if (translatedDimensionScanComparison != dimensionScanComparisons) {
                isSameDimensionsScanComparisons = false;
            }
            translatedDimensionScanComparisonBuilder.add(translatedDimensionScanComparison);
        }

        final ScanComparisons translatedSuffixKeyScanComparisons = suffixScanComparisons.translateCorrelations(translationMap);

        if (translatedPrefixScanComparisons != prefixScanComparisons || !isSameDimensionsScanComparisons ||
                translatedSuffixKeyScanComparisons != suffixScanComparisons) {
            return withComparisons(translatedPrefixScanComparisons, translatedDimensionScanComparisonBuilder.build(),
                    translatedSuffixKeyScanComparisons);
        }
        return this;
    }

    @Nonnull
    protected MultidimensionalIndexScanComparisons withComparisons(@Nonnull final ScanComparisons prefixScanComparisons,
                                                                   @Nonnull final List<ScanComparisons> dimensionsComparisonRanges,
                                                                   @Nonnull final ScanComparisons suffixKeyScanComparisons) {
        return new MultidimensionalIndexScanComparisons(prefixScanComparisons, dimensionsComparisonRanges,
                suffixKeyScanComparisons);
    }

    @Override
    public String toString() {
        return "BY_VALUE(MD):" + prefixScanComparisons + ":" + dimensionsScanComparisons + ":" + suffixScanComparisons;
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
    public PMultidimensionalIndexScanComparisons toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PMultidimensionalIndexScanComparisons.Builder builder = PMultidimensionalIndexScanComparisons.newBuilder();
        builder.setPrefixScanComparisons(prefixScanComparisons.toProto(serializationContext));
        for (final ScanComparisons dimensionsScanComparison : dimensionsScanComparisons) {
            builder.addDimensionsScanComparisons(dimensionsScanComparison.toProto(serializationContext));
        }
        builder.setSuffixScanComparisons(suffixScanComparisons.toProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder().setMultidimensionalIndexScanComparisons(toProto(serializationContext)).build();
    }

    @Nonnull
    public static MultidimensionalIndexScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                                 @Nonnull final PMultidimensionalIndexScanComparisons multidimensionalIndexScanComparisonsProto) {
        final ImmutableList.Builder<ScanComparisons> dimensionScanComparisonsBuilder = ImmutableList.builder();
        for (int i = 0; i < multidimensionalIndexScanComparisonsProto.getDimensionsScanComparisonsCount(); i ++) {
            dimensionScanComparisonsBuilder.add(ScanComparisons.fromProto(serializationContext,
                    multidimensionalIndexScanComparisonsProto.getDimensionsScanComparisons(i)));
        }
        return new MultidimensionalIndexScanComparisons(ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(multidimensionalIndexScanComparisonsProto.getPrefixScanComparisons())),
                dimensionScanComparisonsBuilder.build(),
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(multidimensionalIndexScanComparisonsProto.getSuffixScanComparisons())));
    }

    @Nonnull
    public static MultidimensionalIndexScanComparisons byValue(@Nullable ScanComparisons prefixScanComparisons,
                                                               @Nonnull final List<ScanComparisons> dimensionsComparisonRanges,
                                                               @Nullable ScanComparisons suffixKeyScanComparisons) {
        if (prefixScanComparisons == null) {
            prefixScanComparisons = ScanComparisons.EMPTY;
        }

        if (suffixKeyScanComparisons == null) {
            suffixKeyScanComparisons = ScanComparisons.EMPTY;
        }

        return new MultidimensionalIndexScanComparisons(prefixScanComparisons, dimensionsComparisonRanges, suffixKeyScanComparisons);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PMultidimensionalIndexScanComparisons, MultidimensionalIndexScanComparisons> {
        @Nonnull
        @Override
        public Class<PMultidimensionalIndexScanComparisons> getProtoMessageClass() {
            return PMultidimensionalIndexScanComparisons.class;
        }

        @Nonnull
        @Override
        public MultidimensionalIndexScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                              @Nonnull final PMultidimensionalIndexScanComparisons multidimensionalIndexScanComparisonsProto) {
            return MultidimensionalIndexScanComparisons.fromProto(serializationContext, multidimensionalIndexScanComparisonsProto);
        }
    }
}
