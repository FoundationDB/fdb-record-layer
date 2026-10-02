/*
 * GeospatialWithinDistanceComponent.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Query filter selecting records whose (latitude, longitude) lie within a great-circle distance (in meters) of a
 * center. The coordinates are supplied as a two-column {@link KeyExpression} (latitude then longitude) so the filter
 * can be matched against a {@link com.apple.foundationdb.record.metadata.IndexTypes#GEOSPATIAL_RTREE} index whose
 * coordinate columns are that same expression.
 *
 * <p>
 * Distance is measured with the haversine formula (see {@link GeospatialRTreeScanBounds#haversineMeters}), so this
 * filter is exactly what such an index evaluates — distinct from the planar {@code GeoPointWithinDistanceComponent} in
 * {@code fdb-record-layer-spatial}, whose distance is in coordinate units.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class GeospatialWithinDistanceComponent implements ComponentWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Geospatial-Within-Distance-Component");

    @Nonnull
    private final DoubleValueOrParameter centerLatitude;
    @Nonnull
    private final DoubleValueOrParameter centerLongitude;
    @Nonnull
    private final DoubleValueOrParameter radiusMeters;
    @Nonnull
    private final KeyExpression coordinatesExpression;

    /**
     * Create a within-distance filter over a two-column coordinate expression.
     * @param centerLatitude center latitude in degrees
     * @param centerLongitude center longitude in degrees
     * @param radiusMeters great-circle radius in meters
     * @param coordinatesExpression a two-column expression yielding latitude then longitude
     */
    public GeospatialWithinDistanceComponent(@Nonnull final DoubleValueOrParameter centerLatitude,
                                             @Nonnull final DoubleValueOrParameter centerLongitude,
                                             @Nonnull final DoubleValueOrParameter radiusMeters,
                                             @Nonnull final KeyExpression coordinatesExpression) {
        this.centerLatitude = centerLatitude;
        this.centerLongitude = centerLongitude;
        this.radiusMeters = radiusMeters;
        this.coordinatesExpression = coordinatesExpression;
    }

    @Nonnull
    public DoubleValueOrParameter getCenterLatitude() {
        return centerLatitude;
    }

    @Nonnull
    public DoubleValueOrParameter getCenterLongitude() {
        return centerLongitude;
    }

    @Nonnull
    public DoubleValueOrParameter getRadiusMeters() {
        return radiusMeters;
    }

    @Nonnull
    public KeyExpression getCoordinatesExpression() {
        return coordinatesExpression;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> rec, @Nullable Message message) {
        if (rec == null) {
            return null;
        }
        final Double radiusValue = radiusMeters.getValue(context);
        final Double centerLatitudeValue = centerLatitude.getValue(context);
        final Double centerLongitudeValue = centerLongitude.getValue(context);
        if (radiusValue == null || centerLatitudeValue == null || centerLongitudeValue == null) {
            return null;
        }
        final Key.Evaluated evaluated = coordinatesExpression.evaluateSingleton(rec);
        final Object latitudeObject = evaluated.getObject(0);
        final Object longitudeObject = evaluated.getObject(1);
        if (latitudeObject == null || longitudeObject == null) {
            return null;
        }
        final double distance = GeospatialRTreeScanBounds.haversineMeters(centerLatitudeValue, centerLongitudeValue,
                ((Number)latitudeObject).doubleValue(), ((Number)longitudeObject).doubleValue());
        return distance <= radiusValue;
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        coordinatesExpression.validate(descriptor);
        if (coordinatesExpression.getColumnSize() != 2) {
            throw new Query.InvalidExpressionException("coordinates expression must have exactly two columns");
        }
    }

    @Nonnull
    @Override
    public GraphExpansion expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                                 @Nonnull final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 @Nonnull final List<String> fieldNamePrefix) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.objectsPlanHash(mode, centerLatitude, centerLongitude, radiusMeters, coordinatesExpression);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, centerLatitude, centerLongitude, radiusMeters, coordinatesExpression);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return coordinatesExpression + " WITHIN " + radiusMeters + " METERS OF (" + centerLatitude + ", " + centerLongitude + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeospatialWithinDistanceComponent that = (GeospatialWithinDistanceComponent)o;
        return centerLatitude.equals(that.centerLatitude) &&
               centerLongitude.equals(that.centerLongitude) &&
               radiusMeters.equals(that.radiusMeters) &&
               coordinatesExpression.equals(that.coordinatesExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(centerLatitude, centerLongitude, radiusMeters, coordinatesExpression);
    }
}
