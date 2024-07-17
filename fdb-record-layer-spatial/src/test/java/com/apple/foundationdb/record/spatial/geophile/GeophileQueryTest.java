/*
 * FDBSpatialQueryTest.java
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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.spatial.common.DoubleValueOrParameter;
import com.apple.foundationdb.record.spatial.common.GeoPointWithinDistanceComponent;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Throwables;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for Geophile indexes.
 */
@Tag(Tags.RequiresFDB)
public class GeophileQueryTest extends FDBRecordStoreQueryTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeophileQueryTest.class);

    protected void openRecordStore(FDBRecordContext context, RecordMetaDataHook hook) throws Exception {
        openAnyRecordStore(TestRecordsGeoProto.getDescriptor(), context, hook);
    }

    protected static KeyExpression LOCATION_LAT_LONG = field("location")
            .nest(concatenateFields("latitude", "longitude"));
    protected static final Index CITY_LOCATION_INDEX = new Index("City$location",
            function(GeophileSpatialFunctionNames.GEOPHILE_POINT_Z, LOCATION_LAT_LONG),
            GeophileIndexTypes.SPATIAL_GEOPHILE);
    protected static final Index CITY_LOCATION_COVERING_INDEX = new Index("City$location",
            keyWithValue(concat(function(GeophileSpatialFunctionNames.GEOPHILE_POINT_Z, LOCATION_LAT_LONG), LOCATION_LAT_LONG), 1),
            GeophileIndexTypes.SPATIAL_GEOPHILE);
    protected static final Index COUNTRY_SHAPE_INDEX = new Index("Country$shape",
            function(GeophileSpatialFunctionNames.GEOPHILE_JSON_Z, concat(field("shape"), value(true))),
            GeophileIndexTypes.SPATIAL_GEOPHILE);

    protected void loadCities(RecordMetaDataHook hook, int minPopulation) throws Exception {
        FDBRecordContext context = openContext();
        int total = 0;
        try {
            openRecordStore(context, hook);
            try (FileInputStream file = new FileInputStream(".out/geonames/cities15000.txt")) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    String[] split = line.split("\t");
                    if (Integer.parseInt(split[14]) < minPopulation) {
                        continue;
                    }
                    TestRecordsGeoProto.City.Builder cityBuilder = TestRecordsGeoProto.City.newBuilder()
                            .setGeoNameId(Integer.parseInt(split[0]))
                            .setName(split[1])
                            .setNameAscii(split[2])
                            .setCountry(split[8]);
                    cityBuilder.getLocationBuilder()
                            .setLatitude(Double.parseDouble(split[4]))
                            .setLongitude(Double.parseDouble(split[5]));
                    recordStore.saveRecord(cityBuilder.build());
                    count++;
                    if (count > 100) {
                        commit(context);
                        context.close();
                        total += count;
                        count = 0;
                        context = openContext();
                        recordStore = recordStore.asBuilder().setContext(context).open();
                    }
                }
                commit(context);
                total += count;
            }
        } finally {
            context.close();
        }
        LOGGER.info(KeyValueLogMessage.of("Loaded cities", "count", total));
    }

    protected void loadCountries(RecordMetaDataHook hook, int minPopulation) throws Exception {
        int total = 0;
        try (FileInputStream file = new FileInputStream(".out/geonames/countryInfo.txt"); FDBRecordContext context = openContext()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            openRecordStore(context, hook);
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                try {
                    String[] split = line.split("\t");
                    if (Integer.parseInt(split[7]) < minPopulation) {
                        continue;
                    }
                    TestRecordsGeoProto.Country.Builder countryBuilder = TestRecordsGeoProto.Country.newBuilder()
                            .setGeoNameId(Integer.parseInt(split[16]))
                            .setName(split[4])
                            .setCode(split[0]);
                    recordStore.saveRecord(countryBuilder.build());
                    total++;
                } catch (Exception ex) {
                    LOGGER.error("loadCountries(): Failed to parse line " + line, ex);
                }
            }
            commit(context);
        }
        LOGGER.info(KeyValueLogMessage.of("Loaded countries", "count", total));
    }

    protected void loadCountryShapes(RecordMetaDataHook hook) throws Exception {
        FDBRecordContext context = openContext();
        try {
            openRecordStore(context, hook);
            try (FileInputStream file = new FileInputStream(".out/geonames/shapes_all_low.txt")) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
                String line = reader.readLine();
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    try {
                        String[] split = line.split("\t");
                        FDBStoredRecord<Message> country = recordStore.loadRecord(Tuple.from(Integer.parseInt(split[0])));
                        if (country == null) {
                            LOGGER.warn(KeyValueLogMessage.of("country not found", "country", split[0]));
                            continue;
                        }
                        TestRecordsGeoProto.Country.Builder countryBuilder = TestRecordsGeoProto.Country.newBuilder()
                                .mergeFrom(country.getRecord())
                                .setShape(split[1]);
                        recordStore.saveRecord(countryBuilder.build());
                        count++;
                        if (count > 100) {
                            commit(context);
                            context.close();
                            context = openContext();
                            recordStore = recordStore.asBuilder().setContext(context).open();
                            count = 0;
                        }
                    } catch (Exception ex) {
                        LOGGER.error("loadCountryShapes(): Failed to parse line " + line, ex);
                    }
                }
                commit(context);
            }
        } finally {
            context.close();
        }
    }

    @Nonnull
    protected RecordQueryPlan distanceFilter(double distance, RecordQueryPlan input) {
        return new RecordQueryFilterPlan(input,
                Query.field("location").matches(new GeoPointWithinDistanceComponent(
                        DoubleValueOrParameter.parameter("center_latitude"),
                        DoubleValueOrParameter.parameter("center_longitude"),
                        DoubleValueOrParameter.value(distance),
                        "latitude", "longitude")));
    }

    @Nonnull
    protected RecordQueryPlan distanceFilterScan(double distance) {
        return distanceFilter(distance,
                new RecordQueryTypeFilterPlan(
                        new RecordQueryScanPlan(ScanComparisons.EMPTY, false),
                        Collections.singleton("City")));
    }

    @Nonnull
    protected RecordQueryPlan distanceSpatialQuery(double distance, boolean covering) {
        RecordQueryPlan spatialQuery = new GeophilePointWithinDistanceQueryPlan(
                        DoubleValueOrParameter.parameter("center_latitude"),
                        DoubleValueOrParameter.parameter("center_longitude"),
                        DoubleValueOrParameter.value(distance),
                        "City$location", ScanComparisons.EMPTY, covering);
        if (covering) {
            return spatialQuery;
        } else {
            return distanceFilter(distance, spatialQuery);
        }
    }

    @Nonnull
    protected EvaluationContext bindCenter(int cityId) {
        Bindings.Builder bindings = Bindings.newBuilder();
        FDBStoredRecord<Message> city = recordStore.loadRecord(Tuple.from(cityId));
        if (city == null) {
            LOGGER.warn(KeyValueLogMessage.of("city not found", "city", cityId));
        } else {
            TestRecordsGeoProto.City.Builder cityBuilder = TestRecordsGeoProto.City.newBuilder()
                    .mergeFrom(city.getRecord());
            bindings.set("center_latitude", cityBuilder.getLocation().getLatitude());
            bindings.set("center_longitude", cityBuilder.getLocation().getLongitude());
        }
        return EvaluationContext.forBindings(bindings.build());
    }

    @Test
    @Tag(Tags.Slow)
    public void testDistance() throws Exception {
        final RecordMetaDataHook hook = md -> {
            md.addIndex("City", CITY_LOCATION_COVERING_INDEX);
        };

        loadCities(hook, 0);

        final int centerId = 5391959;
        final double distance = 1;
        final int scanLimit = 5000;

        final RecordQueryPlan scanPlan = distanceFilterScan(distance);
        final Set<Integer> scanResults = new HashSet<>();
        byte [] continuation = null;
        do {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                EvaluationContext joinContext = bindCenter(centerId);
                ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setScannedRecordsLimit(scanLimit).build();
                RecordCursor<FDBQueriedRecord<Message>> recordCursor = scanPlan.execute(recordStore, joinContext, continuation, executeProperties);
                recordCursor.forEach(city -> {
                    TestRecordsGeoProto.City.Builder cityBuilder = TestRecordsGeoProto.City.newBuilder()
                            .mergeFrom(city.getRecord());
                    LOGGER.debug(KeyValueLogMessage.of("Scan found", "geo_name_id", cityBuilder.getGeoNameId(), "city", cityBuilder.getName()));
                    scanResults.add(cityBuilder.getGeoNameId());
                }).join();
                continuation = recordCursor.getNext().getContinuation().toBytes();
                commit(context);
            }
        } while (continuation != null);

        final RecordQueryPlan indexPlan = distanceSpatialQuery(distance, false);
        final Set<Integer> indexResults = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            RecordCursor<FDBQueriedRecord<Message>> recordCursor = indexPlan.execute(recordStore, bindCenter(centerId));
            recordCursor.forEach(city -> {
                TestRecordsGeoProto.City.Builder cityBuilder = TestRecordsGeoProto.City.newBuilder()
                        .mergeFrom(city.getRecord());
                LOGGER.debug(KeyValueLogMessage.of("Index found", "geo_name_id", cityBuilder.getGeoNameId(), "city", cityBuilder.getName()));
                indexResults.add(cityBuilder.getGeoNameId());
            }).join();
            int given = timer.getCount(FDBStoreTimer.Counts.QUERY_FILTER_GIVEN);
            int passed = timer.getCount(FDBStoreTimer.Counts.QUERY_FILTER_PASSED);
            int discarded = timer.getCount(FDBStoreTimer.Counts.QUERY_DISCARDED);
            assertThat("Should have passed more than discarded", passed, greaterThan(discarded));
            commit(context);
        }

        final RecordQueryPlan coveringPlan = distanceSpatialQuery(distance, true);
        final Set<Integer> coveringResults = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            RecordCursor<FDBQueriedRecord<Message>> recordCursor = indexPlan.execute(recordStore, bindCenter(centerId));
            recordCursor.forEach(city -> {
                TestRecordsGeoProto.City.Builder cityBuilder = TestRecordsGeoProto.City.newBuilder()
                        .mergeFrom(city.getRecord());
                LOGGER.debug(KeyValueLogMessage.of("Covering found", "geo_name_id", cityBuilder.getGeoNameId(), "city", cityBuilder.getName()));
                coveringResults.add(cityBuilder.getGeoNameId());
            }).join();
            commit(context);
        }

        assertEquals(scanResults, indexResults);
        assertEquals(scanResults, coveringResults);
    }

    @Test
    @Tag(Tags.Slow)
    // The shape data is really still too detailed for this join to execute in a reasonable amount of time and without
    // many false matches. Also many of the geometries appear to be invalid.
    // So really this just shows that join results aren't wildly off.
    public void testJoin() throws Exception {
        final RecordMetaDataHook hook = md -> {
            md.setSplitLongRecords(true);
            md.addIndex("City", CITY_LOCATION_INDEX);
            md.addIndex("Country", COUNTRY_SHAPE_INDEX);
        };

        loadCities(hook, 500000);
        loadCountries(hook, 500000);
        loadCountryShapes(hook);

        final GeophileSpatialIndexJoinPlan plan = new GeophileSpatialIndexJoinPlan("City$location", ScanComparisons.EMPTY,
                                                                   "Country$shape", ScanComparisons.EMPTY);

        int[] stats = new int[4];
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            RecordCursor<Pair<FDBIndexedRecord<Message>, FDBIndexedRecord<Message>>> recordCursor = plan.execute(recordStore, EvaluationContext.EMPTY);
            final GeometryFactory geometryFactory = new GeometryFactory();
            final GeoJsonReader geoJsonReader = new GeoJsonReader(geometryFactory);
            recordCursor.forEach(pair -> {
                TestRecordsGeoProto.City.Builder cityBuilder = TestRecordsGeoProto.City.newBuilder()
                        .mergeFrom(pair.getLeft().getRecord());
                TestRecordsGeoProto.Country.Builder countryBuilder = TestRecordsGeoProto.Country.newBuilder()
                        .mergeFrom(pair.getRight().getRecord());
                Point cityLocation = geometryFactory.createPoint(new Coordinate(cityBuilder.getLocation().getLatitude(), cityBuilder.getLocation().getLongitude()));
                Geometry countryShape;
                try {
                    countryShape = GeophileSpatial.swapLatLong(geoJsonReader.read(countryBuilder.getShape()));
                } catch (ParseException ex) {
                    throw new RuntimeException(ex);
                }
                // A real join would need to do this because of Z-order join false positives.
                boolean contained;
                try {
                    contained = countryShape.contains(cityLocation);
                } catch (TopologyException ex) {
                    stats[3]++;
                    return;
                }
                if (!contained) {
                    stats[2]++;
                } else if (!countryBuilder.getCode().equals(cityBuilder.getCountry())) {
                    LOGGER.warn(KeyValueLogMessage.of("Code does not match",
                            "country_code", countryBuilder.getCode(),
                            "country_name", countryBuilder.getName(),
                            "city_country", cityBuilder.getCountry(),
                            "city", cityBuilder.getName()));
                    stats[1]++;
                } else {
                    LOGGER.debug(KeyValueLogMessage.of("join result",
                            "country_name", countryBuilder.getName(),
                            "city", cityBuilder.getName()));
                    stats[0]++;
                }
            }).join();
            commit(context);
        } catch (CompletionException ex) {
            assertThat(Throwables.getRootCause(ex), allOf(Matchers.instanceOf(FDBException.class), hasProperty("code", equalTo(1007))));  // transaction_too_old
        } finally {
            LOGGER.info(KeyValueLogMessage.of("testJoin stats",
                    "match", stats[0],
                    "no_match", stats[1],
                    "no_overlap", stats[2],
                    "invalid_geometry", stats[3]));
        }
    }

    @Test
    public void testNulls() throws Exception {
        final RecordMetaDataHook hook = md -> {
            md.addIndex("City", CITY_LOCATION_INDEX);
            md.addIndex("Country", COUNTRY_SHAPE_INDEX);
        };

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            TestRecordsGeoProto.City.Builder cityBuilder = TestRecordsGeoProto.City.newBuilder()
                    .setGeoNameId(-1);
            recordStore.saveRecord(cityBuilder.build());
            TestRecordsGeoProto.Country.Builder countryBuilder = TestRecordsGeoProto.Country.newBuilder()
                    .setGeoNameId(-2);
            recordStore.saveRecord(countryBuilder.build());
        }
    }

}
