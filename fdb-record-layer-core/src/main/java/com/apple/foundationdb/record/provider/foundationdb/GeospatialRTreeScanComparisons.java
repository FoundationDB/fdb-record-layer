/*
 * GeospatialRTreeScanComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PGeospatialRTreeScanComparisons;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.indexes.GeospatialRTreeIndexHelper;
import com.apple.foundationdb.record.query.expressions.DoubleValueOrParameter;
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
 * {@link IndexScanParameters} for a within-distance ({@link IndexScanType#BY_DISTANCE}) scan of a geospatial R-tree
 * index. Holds the prefix (grouping) comparisons, the center latitude/longitude and radius (each a literal or a query
 * parameter), and the suffix comparisons; {@link #bind} resolves them against the evaluation context into a
 * {@link GeospatialRTreeScanBounds}.
 *
 * @see GeospatialRTreeScanBounds
 */
@API(API.Status.EXPERIMENTAL)
public class GeospatialRTreeScanComparisons implements IndexScanParameters {
    @Nonnull
    private final ScanComparisons prefixScanComparisons;
    @Nonnull
    private final DoubleValueOrParameter centerLatitude;
    @Nonnull
    private final DoubleValueOrParameter centerLongitude;
    @Nonnull
    private final DoubleValueOrParameter radiusMeters;
    @Nonnull
    private final ScanComparisons suffixScanComparisons;

    public GeospatialRTreeScanComparisons(@Nonnull final ScanComparisons prefixScanComparisons,
                                          @Nonnull final DoubleValueOrParameter centerLatitude,
                                          @Nonnull final DoubleValueOrParameter centerLongitude,
                                          @Nonnull final DoubleValueOrParameter radiusMeters,
                                          @Nonnull final ScanComparisons suffixScanComparisons) {
        this.prefixScanComparisons = prefixScanComparisons;
        this.centerLatitude = centerLatitude;
        this.centerLongitude = centerLongitude;
        this.radiusMeters = radiusMeters;
        this.suffixScanComparisons = suffixScanComparisons;
    }

    /**
     * Build scan comparisons for a within-distance scan.
     * @param prefixScanComparisons comparisons on the grouping columns, or {@code null} for none
     * @param centerLatitude center latitude in degrees
     * @param centerLongitude center longitude in degrees
     * @param radiusMeters great-circle radius in meters
     * @param suffixScanComparisons comparisons on the key suffix, or {@code null} for none
     * @return the scan comparisons
     */
    @Nonnull
    public static GeospatialRTreeScanComparisons byCenterAndRadius(@Nullable ScanComparisons prefixScanComparisons,
                                                                   @Nonnull final DoubleValueOrParameter centerLatitude,
                                                                   @Nonnull final DoubleValueOrParameter centerLongitude,
                                                                   @Nonnull final DoubleValueOrParameter radiusMeters,
                                                                   @Nullable ScanComparisons suffixScanComparisons) {
        return new GeospatialRTreeScanComparisons(
                prefixScanComparisons == null ? ScanComparisons.EMPTY : prefixScanComparisons,
                centerLatitude, centerLongitude, radiusMeters,
                suffixScanComparisons == null ? ScanComparisons.EMPTY : suffixScanComparisons);
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_DISTANCE;
    }

    @Nonnull
    public ScanComparisons getPrefixScanComparisons() {
        return prefixScanComparisons;
    }

    @Nonnull
    public ScanComparisons getSuffixScanComparisons() {
        return suffixScanComparisons;
    }

    @Nonnull
    @Override
    public GeospatialRTreeScanBounds bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Index index,
                                          @Nonnull final EvaluationContext context) {
        final Double latitude = centerLatitude.getValue(context);
        final Double longitude = centerLongitude.getValue(context);
        final Double radius = radiusMeters.getValue(context);
        if (latitude == null || longitude == null || radius == null) {
            throw new RecordCoreException("geospatial scan requires non-null center and radius");
        }
        final long scale = GeospatialRTreeIndexHelper.getScale(index);
        return GeospatialRTreeScanBounds.withinDistance(prefixScanComparisons.toTupleRange(store, context),
                latitude, longitude, radius, scale, suffixScanComparisons.toTupleRange(store, context));
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, prefixScanComparisons, centerLatitude, centerLongitude, radiusMeters,
                suffixScanComparisons);
    }

    @Override
    public boolean isUnique(@Nonnull Index index) {
        // A within-distance scan can always match more than one point.
        return false;
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain() {
        @Nullable var tupleRange = prefixScanComparisons.toTupleRangeWithoutContext();
        final var prefix = tupleRange == null
                           ? prefixScanComparisons.explain().getExplainTokens()
                           : new ExplainTokens().addToString(tupleRange);

        tupleRange = suffixScanComparisons.toTupleRangeWithoutContext();
        final var suffix = tupleRange == null
                           ? suffixScanComparisons.explain().getExplainTokens()
                           : new ExplainTokens().addToString(tupleRange);

        return ExplainTokensWithPrecedence.of(prefix.addOptionalWhitespace().addToString(":{WITHIN ")
                .addToString(radiusMeters + " OF (" + centerLatitude + ", " + centerLongitude + ")")
                .addToString("}:").addOptionalWhitespace().addNested(suffix));
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder,
                                       @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        @Nullable final TupleRange prefixTupleRange = prefixScanComparisons.toTupleRangeWithoutContext();
        if (prefixTupleRange != null) {
            detailsBuilder.add("prefix: " + prefixTupleRange.getLowEndpoint().toString(false) + "{{plow}}, {{phigh}}" + prefixTupleRange.getHighEndpoint().toString(true));
            attributeMapBuilder.put("plow", Attribute.gml(prefixTupleRange.getLow() == null ? "-∞" : prefixTupleRange.getLow().toString()));
            attributeMapBuilder.put("phigh", Attribute.gml(prefixTupleRange.getHigh() == null ? "∞" : prefixTupleRange.getHigh().toString()));
        } else {
            detailsBuilder.add("prefix comparisons: {{pcomparisons}}");
            attributeMapBuilder.put("pcomparisons", Attribute.gml(prefixScanComparisons.toString()));
        }

        detailsBuilder.add("center latitude: {{centerLatitude}}");
        attributeMapBuilder.put("centerLatitude", Attribute.gml(centerLatitude.toString()));
        detailsBuilder.add("center longitude: {{centerLongitude}}");
        attributeMapBuilder.put("centerLongitude", Attribute.gml(centerLongitude.toString()));
        detailsBuilder.add("radius (meters): {{radiusMeters}}");
        attributeMapBuilder.put("radiusMeters", Attribute.gml(radiusMeters.toString()));

        @Nullable final TupleRange suffixTupleRange = suffixScanComparisons.toTupleRangeWithoutContext();
        if (suffixTupleRange != null) {
            detailsBuilder.add("suffix: " + suffixTupleRange.getLowEndpoint().toString(false) + "{{slow}}, {{shigh}}" + suffixTupleRange.getHighEndpoint().toString(true));
            attributeMapBuilder.put("slow", Attribute.gml(suffixTupleRange.getLow() == null ? "-∞" : suffixTupleRange.getLow().toString()));
            attributeMapBuilder.put("shigh", Attribute.gml(suffixTupleRange.getHigh() == null ? "∞" : suffixTupleRange.getHigh().toString()));
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
        final GeospatialRTreeScanComparisons that = (GeospatialRTreeScanComparisons)other;
        return prefixScanComparisons.semanticEquals(that.prefixScanComparisons, aliasMap) &&
               centerLatitude.equals(that.centerLatitude) &&
               centerLongitude.equals(that.centerLongitude) &&
               radiusMeters.equals(that.radiusMeters) &&
               suffixScanComparisons.semanticEquals(that.suffixScanComparisons, aliasMap);
    }

    @Override
    public int semanticHashCode() {
        int hashCode = prefixScanComparisons.semanticHashCode();
        hashCode = 31 * hashCode + centerLatitude.hashCode();
        hashCode = 31 * hashCode + centerLongitude.hashCode();
        hashCode = 31 * hashCode + radiusMeters.hashCode();
        return 31 * hashCode + suffixScanComparisons.semanticHashCode();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     final boolean shouldSimplifyValues) {
        final ScanComparisons translatedPrefixScanComparisons =
                prefixScanComparisons.translateCorrelations(translationMap, shouldSimplifyValues);
        final ScanComparisons translatedSuffixScanComparisons =
                suffixScanComparisons.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedPrefixScanComparisons != prefixScanComparisons ||
                translatedSuffixScanComparisons != suffixScanComparisons) {
            return new GeospatialRTreeScanComparisons(translatedPrefixScanComparisons, centerLatitude, centerLongitude,
                    radiusMeters, translatedSuffixScanComparisons);
        }
        return this;
    }

    @Override
    public String toString() {
        return "BY_DISTANCE:" + prefixScanComparisons + ":WITHIN " + radiusMeters + " OF (" + centerLatitude + ", " +
               centerLongitude + "):" + suffixScanComparisons;
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
    public PGeospatialRTreeScanComparisons toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PGeospatialRTreeScanComparisons.newBuilder()
                .setPrefixScanComparisons(prefixScanComparisons.toProto(serializationContext))
                .setCenterLatitude(centerLatitude.toProto(serializationContext))
                .setCenterLongitude(centerLongitude.toProto(serializationContext))
                .setRadiusMeters(radiusMeters.toProto(serializationContext))
                .setSuffixScanComparisons(suffixScanComparisons.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder().setGeospatialRtreeScanComparisons(toProto(serializationContext)).build();
    }

    @Nonnull
    public static GeospatialRTreeScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                           @Nonnull final PGeospatialRTreeScanComparisons proto) {
        return new GeospatialRTreeScanComparisons(
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(proto.getPrefixScanComparisons())),
                DoubleValueOrParameter.fromProto(serializationContext, Objects.requireNonNull(proto.getCenterLatitude())),
                DoubleValueOrParameter.fromProto(serializationContext, Objects.requireNonNull(proto.getCenterLongitude())),
                DoubleValueOrParameter.fromProto(serializationContext, Objects.requireNonNull(proto.getRadiusMeters())),
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(proto.getSuffixScanComparisons())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PGeospatialRTreeScanComparisons, GeospatialRTreeScanComparisons> {
        @Nonnull
        @Override
        public Class<PGeospatialRTreeScanComparisons> getProtoMessageClass() {
            return PGeospatialRTreeScanComparisons.class;
        }

        @Nonnull
        @Override
        public GeospatialRTreeScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PGeospatialRTreeScanComparisons proto) {
            return GeospatialRTreeScanComparisons.fromProto(serializationContext, proto);
        }
    }
}
