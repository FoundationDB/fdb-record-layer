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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.spatial.common.DoubleValueOrParameter;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.index.RecordWithSpatialObject;
import com.geophile.z.spatialobject.d2.Point;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Query spatial index for points (latitude, longitude) within a given distance (inclusive) of a given center.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings({"squid:S1206", "squid:S2160", "PMD.OverrideBothEqualsAndHashcode"})
public class GeophilePointWithinDistanceQueryPlan extends GeophileSpatialObjectQueryPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Geophile-Point-Within-Distance-Query-Plan");

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

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Nonnull
    @Override
    public RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords getFetchIndexRecords() {
        return RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        // TODO: Is this right?
        switch (mode.getKind()) {
            case LEGACY:
                return PlanHashable.objectsPlanHash(mode, getIndexName(), getPrefixComparisons(), centerLatitude, centerLongitude, distance, covering);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getIndexName(), getPrefixComparisons(), covering);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (!super.equalsWithoutChildren(otherExpression, equivalencesMap)) {
            return false;
        }
        GeophilePointWithinDistanceQueryPlan that = (GeophilePointWithinDistanceQueryPlan)otherExpression;
        return covering == that.covering &&
               centerLatitude.equals(that.centerLatitude) &&
               centerLongitude.equals(that.centerLongitude) &&
               distance.equals(that.distance);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(super.hashCodeWithoutChildren(), centerLatitude, centerLongitude, distance, covering);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return createIndexPlannerGraph(this,
                covering ? NodeInfo.COVERING_SPATIAL_INDEX_SCAN_OPERATOR : NodeInfo.SPATIAL_INDEX_SCAN_OPERATOR,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    @Nonnull
    @Override
    public PlannerGraph createIndexPlannerGraph(@Nonnull final RecordQueryPlan identity,
                                                @Nonnull final NodeInfo nodeInfo,
                                                @Nonnull final List<String> additionalDetails,
                                                @Nonnull final Map<String, Attribute> additionalAttributeMap) {
        final ImmutableList.Builder<String> detailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> attributeMapBuilder = ImmutableMap.builder();

        detailsBuilder
                .addAll(additionalDetails);
        attributeMapBuilder
                .putAll(additionalAttributeMap);

        detailsBuilder.add("center latitude: {{centerLatitude}}");
        attributeMapBuilder.put("centerLatitude", Attribute.gml(centerLatitude));

        detailsBuilder.add("center longitude: {{centerLongitude}}");
        attributeMapBuilder.put("centerLongitude", Attribute.gml(centerLongitude));

        detailsBuilder.add("distance: {{distance}}");
        attributeMapBuilder.put("distance", Attribute.gml(distance));

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(identity,
                        nodeInfo,
                        detailsBuilder.build(),
                        attributeMapBuilder.build()),
                ImmutableList.of(
                        PlannerGraph.fromNodeAndChildGraphs(
                                new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA, new Type.Relation(new Type.Any()), ImmutableList.copyOf(getUsedIndexes())),
                                ImmutableList.of())));
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }
}
