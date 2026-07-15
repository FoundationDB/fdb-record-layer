/*
 * GeospatialRTreeIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsGeoProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.expressions.DoubleValueOrParameter;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for {@link GeospatialRTreeIndexMaintainer} against a live FDB: records are indexed, then a
 * within-distance ({@link com.apple.foundationdb.record.IndexScanType#BY_DISTANCE}) scan retrieves them. Covers exact
 * circular-distance filtering (corner exclusion), per-grouping R-tree isolation, deletion, and antimeridian wraparound.
 */
@Tag(Tags.RequiresFDB)
class GeospatialRTreeIndexTest extends FDBRecordStoreQueryTestBase {
    private static final String UNGROUPED_INDEX = "cityLocation";
    private static final String GROUPED_INDEX = "cityLocationByCountry";
    // A small R-tree fan-out forces a multi-level tree from a modest record count, exercising MBR-based node pruning.
    private static final ImmutableMap<String, String> SMALL_NODE_OPTIONS =
            ImmutableMap.of(IndexOptions.RTREE_MIN_M, "2", IndexOptions.RTREE_MAX_M, "4");

    private static final RecordMetaDataHook UNGROUPED_HOOK = metaData ->
            metaData.addIndex("City", new Index(UNGROUPED_INDEX,
                    field("location").nest(concat(field("latitude"), field("longitude"))),
                    IndexTypes.GEOSPATIAL_RTREE, SMALL_NODE_OPTIONS));

    private static final RecordMetaDataHook GROUPED_HOOK = metaData ->
            metaData.addIndex("City", new Index(GROUPED_INDEX,
                    field("location").nest(concat(field("latitude"), field("longitude"))).groupBy(field("country")),
                    IndexTypes.GEOSPATIAL_RTREE, SMALL_NODE_OPTIONS));

    private void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        openAnyRecordStore(TestRecordsGeoProto.getDescriptor(), context, hook);
    }

    private static TestRecordsGeoProto.City city(final int id, final String country,
                                                 final double latitude, final double longitude) {
        final TestRecordsGeoProto.City.Builder builder =
                TestRecordsGeoProto.City.newBuilder().setGeoNameId(id).setCountry(country);
        builder.getLocationBuilder().setLatitude(latitude).setLongitude(longitude);
        return builder.build();
    }

    private void saveCities(@Nonnull final RecordMetaDataHook hook,
                            @Nonnull final List<TestRecordsGeoProto.City> cities) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (final TestRecordsGeoProto.City c : cities) {
                recordStore.saveRecord(c);
            }
            commit(context);
        }
    }

    private Set<Long> scanIds(@Nonnull final RecordMetaDataHook hook, @Nonnull final String indexName,
                              @Nonnull final TupleRange prefixRange,
                              final double centerLatitude, final double centerLongitude,
                              final double radiusMeters) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            final long scale = GeospatialRTreeIndexHelper.getScale(index);
            final GeospatialRTreeScanBounds bounds = GeospatialRTreeScanBounds.withinDistance(prefixRange,
                    centerLatitude, centerLongitude, radiusMeters, scale);
            final List<IndexEntry> entries = context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR,
                    recordStore.scanIndex(index, bounds, null, ScanProperties.FORWARD_SCAN).asList());
            return entries.stream()
                    .map(entry -> entry.getPrimaryKey().getLong(0))
                    .collect(Collectors.toSet());
        }
    }

    @Test
    void withinDistanceReturnsPointsInsideRadiusExcludingCorners() throws Exception {
        final ImmutableList.Builder<TestRecordsGeoProto.City> cities = ImmutableList.builder();
        cities.add(city(1, "X", 0.0, 0.0));      // center, inside
        cities.add(city(2, "X", 0.5, 0.0));      // ~55 km, inside
        cities.add(city(3, "X", 0.0, 0.5));      // ~55 km, inside
        cities.add(city(4, "X", 0.7, 0.7));      // ~110 km: inside the bounding box, outside the 100 km circle
        cities.add(city(5, "X", 5.0, 5.0));      // far outside
        // Bulk far-away points so the R-tree becomes multi-level and node pruning is exercised.
        for (int i = 10; i < 25; i++) {
            cities.add(city(i, "X", i, i));
        }
        saveCities(UNGROUPED_HOOK, cities.build());

        final Set<Long> ids = scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, 0.0, 0.0, 100_000.0);
        assertThat(ids).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    void groupingIsolatesRTreesPerGroup() throws Exception {
        saveCities(GROUPED_HOOK, ImmutableList.of(
                city(1, "A", 0.0, 0.0),
                city(2, "A", 0.3, 0.0),      // ~33 km, inside, group A
                city(3, "B", 0.0, 0.0),      // identical location, but group B
                city(4, "B", 10.0, 10.0)));  // far, group B

        final Set<Long> groupA = scanIds(GROUPED_HOOK, GROUPED_INDEX,
                TupleRange.betweenInclusive(Tuple.from("A"), Tuple.from("A")), 0.0, 0.0, 100_000.0);
        // The co-located group-B city (id 3) must not appear when scanning group A.
        assertThat(groupA).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void deleteRemovesPointFromIndex() throws Exception {
        saveCities(UNGROUPED_HOOK, ImmutableList.of(
                city(1, "X", 0.0, 0.0),
                city(2, "X", 0.5, 0.0)));

        assertThat(scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, 0.0, 0.0, 100_000.0))
                .containsExactlyInAnyOrder(1L, 2L);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, UNGROUPED_HOOK);
            recordStore.deleteRecord(Tuple.from(1L));
            commit(context);
        }

        assertThat(scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, 0.0, 0.0, 100_000.0))
                .containsExactlyInAnyOrder(2L);
    }

    @Test
    void withinDistanceWrapsAroundAntimeridian() throws Exception {
        saveCities(UNGROUPED_HOOK, ImmutableList.of(
                city(300, "X", 0.0, 179.95),    // just west of +180
                city(301, "X", 0.0, -179.95),   // just east of -180
                city(302, "X", 0.0, 0.0)));      // far from the antimeridian

        // A circle centered on the antimeridian must reach points on both sides of the ±180 seam.
        final Set<Long> ids = scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, 0.0, 180.0, 100_000.0);
        assertThat(ids).containsExactlyInAnyOrder(300L, 301L);
    }

    @Test
    void plannedIndexScanBindsAndExecutesWithinDistance() throws Exception {
        saveCities(UNGROUPED_HOOK, ImmutableList.of(
                city(1, "X", 0.0, 0.0),
                city(2, "X", 0.5, 0.0),
                city(3, "X", 0.7, 0.7),      // in the bounding box corner but outside the circle
                city(4, "X", 5.0, 5.0)));     // far outside

        // The center and radius are query parameters, so binding resolves them from the evaluation context.
        final RecordQueryPlan plan = new RecordQueryIndexPlan(UNGROUPED_INDEX,
                GeospatialRTreeScanComparisons.byCenterAndRadius(ScanComparisons.EMPTY,
                        DoubleValueOrParameter.parameter("centerLat"),
                        DoubleValueOrParameter.parameter("centerLon"),
                        DoubleValueOrParameter.parameter("radius"),
                        ScanComparisons.EMPTY),
                false);
        final EvaluationContext evaluationContext = EvaluationContext.forBindings(Bindings.newBuilder()
                .set("centerLat", 0.0)
                .set("centerLon", 0.0)
                .set("radius", 100_000.0)
                .build());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, UNGROUPED_HOOK);
            final RecordCursor<QueryResult> cursor =
                    plan.executePlan(recordStore, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE);
            final List<Long> ids = context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR,
                    cursor.map(queryResult -> Objects.requireNonNull(queryResult.getQueriedRecord())
                            .getPrimaryKey().getLong(0)).asList());
            assertThat(ids).containsExactlyInAnyOrder(1L, 2L);
            commit(context);
        }
    }
}
