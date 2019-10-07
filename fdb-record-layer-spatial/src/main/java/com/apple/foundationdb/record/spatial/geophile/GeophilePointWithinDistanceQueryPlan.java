/*
 * GeoPointWithinDistanceQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.spatial.geophile;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.spatial.common.DoubleValueOrParameter;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.index.RecordWithSpatialObject;
import com.geophile.z.spatialobject.d2.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Query spatial index for points (latitude, longitude) within a given distance (inclusive) of a given center.
 */
@API(API.Status.EXPERIMENTAL)
public class GeophilePointWithinDistanceQueryPlan extends GeophileSpatialObjectQueryPlan {
    @Nonnull
    private final DoubleValueOrParameter centerLatitude;
    @Nonnull
    private final DoubleValueOrParameter centerLongitude;
    @Nonnull
    private final DoubleValueOrParameter distance;
    private final boolean covering;

    public GeophilePointWithinDistanceQueryPlan(@Nonnull DoubleValueOrParameter centerLatitude, @Nonnull DoubleValueOrParameter centerLongitude,
                                                @Nonnull DoubleValueOrParameter distance,
                                                @Nonnull String indexName, @Nonnull ScanComparisons prefixComparisons, boolean covering) {
        super(indexName, prefixComparisons);
        this.centerLatitude = centerLatitude;
        this.centerLongitude = centerLongitude;
        this.distance = distance;
        this.covering = covering;
    }

    @Nullable
    @Override
    protected SpatialObject getSpatialObject(@Nonnull EvaluationContext context) {
        Double distanceValue = distance.getValue(context);
        Double centerLatitudeValue = centerLatitude.getValue(context);
        Double centerLongitudeValue = centerLongitude.getValue(context);
        if (distanceValue == null || centerLatitudeValue == null || centerLongitudeValue == null) {
            return null;
        }
        // The desired circle's bounding box. So there will be additional false positives at the corners.
        return GeophileBoxLatLon.newBox(centerLatitudeValue - distanceValue, centerLatitudeValue + distanceValue,
                                centerLongitudeValue - distanceValue, centerLongitudeValue + distanceValue);
    }

    @Nullable
    @Override
    protected SpatialJoin.Filter<RecordWithSpatialObject, GeophileRecordImpl> getFilter(@Nonnull EvaluationContext context) {
        if (covering) {
            Double distanceValue = distance.getValue(context);
            Double centerLatitudeValue = centerLatitude.getValue(context);
            Double centerLongitudeValue = centerLongitude.getValue(context);
            if (distanceValue == null || centerLatitudeValue == null || centerLongitudeValue == null) {
                return null;
            }
            final GeometryFactory geometryFactory = new GeometryFactory();
            final Geometry center = geometryFactory.createPoint(new Coordinate(centerLatitudeValue, centerLongitudeValue));
            return (spatialObject, record) -> {
                Point point = (Point)record.spatialObject();
                Geometry geometry = geometryFactory.createPoint(new Coordinate(point.x(), point.y()));
                return geometry.isWithinDistance(center, distanceValue);
            };
        } else {
            return null;
        }
    }

    @Override
    protected BiFunction<IndexEntry, Tuple, GeophileRecordImpl> getRecordFunction() {
        if (covering) {
            return GeophileCoveringPointRecord::new;
        } else {
            return GeophileRecordImpl::new;
        }
    }

    @Override
    public int planHash() {
        return PlanHashable.objectsPlanHash(getIndexName(), getPrefixComparisons(), centerLatitude, centerLongitude, distance, covering);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        GeophilePointWithinDistanceQueryPlan that = (GeophilePointWithinDistanceQueryPlan)o;
        return covering == that.covering &&
               centerLatitude.equals(that.centerLatitude) &&
               centerLongitude.equals(that.centerLongitude) &&
               distance.equals(that.distance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), centerLatitude, centerLongitude, distance, covering);
    }
}
