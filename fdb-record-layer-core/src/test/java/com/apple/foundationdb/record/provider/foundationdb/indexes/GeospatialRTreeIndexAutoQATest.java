/*
 * GeospatialRTreeIndexAutoQATest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsGeoProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional edge-case tests for {@link GeospatialRTreeIndexMaintainer}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Tag(Tags.RequiresFDB)
class GeospatialRTreeIndexAutoQATest extends FDBRecordStoreQueryTestBase {
    private static final String UNGROUPED_INDEX = "cityLocation";
    private static final String GROUPED_INDEX = "cityLocationByCountry";
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

    private static TestRecordsGeoProto.City cityWithoutLocation(final int id, final String country) {
        return TestRecordsGeoProto.City.newBuilder().setGeoNameId(id).setCountry(country).build();
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
    void withinDistanceWithNullLocationRecordInMultiLevelTreeSkipsNullPoint() throws Exception {
        // BUG AQA-13: GeospatialRTreeScanBounds.overlapsMbrApproximately calls longValue() on a null MBR coordinate, so a
        // distance scan throws NPE once the tree has internal nodes and holds a record without a location. Spec:
        // GeospatialRTreeIndexMaintainer.encodePoint javadoc, "A null coordinate is preserved as null".
        // A record without a location is indexed with null coordinates; scanning a multi-level tree must skip it.
        final ImmutableList.Builder<TestRecordsGeoProto.City> cities = ImmutableList.builder();
        cities.add(city(1, "X", 0.0, 0.0));
        cities.add(cityWithoutLocation(2, "X"));
        // Exceeding the maximum fan-out of 4 splits the root, so the scan checks node MBRs.
        for (int i = 10; i < 13; i++) {
            cities.add(city(i, "X", i, i));
        }
        saveCities(UNGROUPED_HOOK, cities.build());

        assertThat(scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, 0.0, 0.0, 100_000.0))
                .containsExactlyInAnyOrder(1L);
    }

    @Test
    void updateLocationMovesPointInIndex() throws Exception {
        // Re-saving a record with a new location removes the old point and inserts the new one.
        final ImmutableList.Builder<TestRecordsGeoProto.City> cities = ImmutableList.builder();
        cities.add(city(1, "X", 0.0, 0.0));
        for (int i = 10; i < 25; i++) {
            cities.add(city(i, "X", i, i));
        }
        saveCities(UNGROUPED_HOOK, cities.build());
        saveCities(UNGROUPED_HOOK, ImmutableList.of(city(1, "X", -40.0, 100.0)));

        assertThat(scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, 0.0, 0.0, 100_000.0)).isEmpty();
        assertThat(scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, -40.0, 100.0, 1_000.0))
                .containsExactlyInAnyOrder(1L);
    }

    @Test
    void withinDistanceZeroRadiusMatchesPointInCenterCell() throws Exception {
        // BUG AQA-14: with radius 0, containsPosition compares the quantized stored point to the raw center, so a point in
        // the center cell is dropped when the center is not on the precision grid. Spec: GeospatialRTreeScanBounds
        // withinDistance javadoc, "non-positive values match only the exact center cell".
        // Javadoc: "non-positive values match only the exact center cell"; a point stored at the center qualifies.
        final double latitude = 0.12345678; // more fractional digits than the default precision of 7
        saveCities(UNGROUPED_HOOK, ImmutableList.of(city(1, "X", latitude, 0.0), city(2, "X", 1.0, 1.0)));

        assertThat(scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, latitude, 0.0, 0.0))
                .containsExactlyInAnyOrder(1L);
    }

    @Test
    void withinDistanceAroundPoleCoversAllMeridians() throws Exception {
        // A circle reaching the north pole must find nearby points on the far side of the pole.
        final ImmutableList.Builder<TestRecordsGeoProto.City> cities = ImmutableList.builder();
        cities.add(city(1, "X", 89.5, 0.0));     // center
        cities.add(city(2, "X", 89.8, 180.0));   // ~78 km over the pole, inside
        cities.add(city(3, "X", 89.7, -90.0));   // ~60 km, inside
        cities.add(city(4, "X", 88.0, 0.0));     // ~167 km, outside
        for (int i = 10; i < 25; i++) {
            cities.add(city(i, "X", i, i));
        }
        saveCities(UNGROUPED_HOOK, cities.build());

        assertThat(scanIds(UNGROUPED_HOOK, UNGROUPED_INDEX, TupleRange.ALL, 89.5, 0.0, 100_000.0))
                .containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    void groupedScanAcrossGroupsResumesFromContinuationWithLimitOne() throws Exception {
        // Resuming a multi-group scan one entry at a time returns every match exactly once.
        final ImmutableList.Builder<TestRecordsGeoProto.City> cities = ImmutableList.builder();
        int id = 1;
        for (final String country : ImmutableList.of("A", "B", "C")) {
            cities.add(city(id++, country, 0.0, 0.0));
            cities.add(city(id++, country, 0.3, 0.0));
            cities.add(city(id++, country, 5.0, 5.0)); // outside
        }
        saveCities(GROUPED_HOOK, cities.build());

        final ScanProperties limitOne = new ScanProperties(ExecuteProperties.newBuilder()
                .setReturnedRowLimit(1).build());
        final List<Long> ids = new ArrayList<>();
        byte[] continuation = null;
        boolean exhausted = false;
        for (int iteration = 0; iteration < 20 && !exhausted; iteration++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, GROUPED_HOOK);
                final Index index = recordStore.getRecordMetaData().getIndex(GROUPED_INDEX);
                final GeospatialRTreeScanBounds bounds = GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL,
                        0.0, 0.0, 100_000.0, GeospatialRTreeIndexHelper.getScale(index));
                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, bounds, continuation, limitOne)) {
                    final RecordCursorResult<IndexEntry> result =
                            context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, cursor.onNext());
                    if (result.hasNext()) {
                        ids.add(result.get().getPrimaryKey().getLong(0));
                        continuation = result.getContinuation().toBytes();
                    } else {
                        assertThat(result.getNoNextReason().isSourceExhausted()).isTrue();
                        exhausted = true;
                    }
                }
            }
        }

        assertThat(exhausted).isTrue();
        assertThat(ids).containsExactlyInAnyOrder(1L, 2L, 4L, 5L, 7L, 8L);
    }
}
