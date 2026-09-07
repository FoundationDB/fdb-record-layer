/*
 * GeoPointWithinDistanceComponent.java
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

package com.apple.foundationdb.record.spatial.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.ComponentWithNoChildren;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Query filter for points (latitude, longitude) within a given distance of a given center.
 */
@API(API.Status.EXPERIMENTAL)
public class GeoPointWithinDistanceComponent implements ComponentWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Geo-Point-Within-Distance-Component");

    private final DoubleValueOrParameter centerLatitude;
    private final DoubleValueOrParameter centerLongitude;
    private final DoubleValueOrParameter distance;
    private final String latitudeFieldName;
    private final String longitudeFieldName;
    private final GeometryFactory geometryFactory = new GeometryFactory();

    public GeoPointWithinDistanceComponent(DoubleValueOrParameter centerLatitude, DoubleValueOrParameter centerLongitude,
                                           DoubleValueOrParameter distance,
                                           String latitudeFieldName, String longitudeFieldName) {
        this.centerLatitude = centerLatitude;
        this.centerLongitude = centerLongitude;
        this.distance = distance;
        this.latitudeFieldName = latitudeFieldName;
        this.longitudeFieldName = longitudeFieldName;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean evalMessage(FDBRecordStoreBase<M> store, EvaluationContext context, @Nullable FDBRecord<M> rec, @Nullable Message message) {
        Double distanceValue = distance.getValue(context);
        Double centerLatitudeValue = centerLatitude.getValue(context);
        Double centerLongitudeValue = centerLongitude.getValue(context);
        if (distanceValue == null || centerLatitudeValue == null || centerLongitudeValue == null) {
            return null;
        }
        Double pointLatitudeValue = getCoordinateField(message, latitudeFieldName);
        Double pointLongitudeValue = getCoordinateField(message, longitudeFieldName);
        if (pointLatitudeValue == null || pointLongitudeValue == null) {
            return null;
        }
        Geometry center = geometryFactory.createPoint(new Coordinate(centerLatitudeValue, centerLongitudeValue));
        Geometry point = geometryFactory.createPoint(new Coordinate(pointLatitudeValue, pointLongitudeValue));
        return point.isWithinDistance(center, distanceValue);
    }

    @Nullable
    private Double getCoordinateField(@Nullable Message message, String fieldName) {
        if (message == null) {
            return null;
        }
        final Descriptors.FieldDescriptor field = message.getDescriptorForType().findFieldByName(fieldName);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldName);
        }
        return (Double)message.getField(field);
    }

    @Override
    public void validate(Descriptors.Descriptor descriptor) {
        validateCoordinateField(descriptor, latitudeFieldName);
        validateCoordinateField(descriptor, longitudeFieldName);
    }

    private void validateCoordinateField(Descriptors.Descriptor descriptor, String fieldName) {
        Descriptors.FieldDescriptor field = descriptor.findFieldByName(fieldName);
        if (field == null) {
            throw new Query.InvalidExpressionException("Missing field " + fieldName);
        }
        if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.DOUBLE) {
            throw new Query.InvalidExpressionException("Required double field for " + fieldName);
        }
        if (field.isRepeated()) {
            throw new Query.InvalidExpressionException("Required scalar field, but got repeated field " + fieldName);
        }
    }

    @Override
    public GraphExpansion expand(final Quantifier.ForEach baseQuantifier,
                                 final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 final List<String> fieldNamePrefix) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.objectsPlanHash(mode, centerLatitude, centerLongitude, distance, latitudeFieldName, longitudeFieldName);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, centerLatitude, centerLongitude, distance, latitudeFieldName, longitudeFieldName);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "(" + latitudeFieldName + "," + longitudeFieldName + ") WITHIN " + distance + " OF (" + centerLatitude + "," + centerLongitude + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeoPointWithinDistanceComponent that = (GeoPointWithinDistanceComponent)o;
        return centerLatitude.equals(that.centerLatitude) &&
               centerLongitude.equals(that.centerLongitude) &&
               distance.equals(that.distance) &&
               latitudeFieldName.equals(that.latitudeFieldName) &&
               longitudeFieldName.equals(that.longitudeFieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(centerLatitude, centerLongitude, distance, latitudeFieldName, longitudeFieldName);
    }
}
