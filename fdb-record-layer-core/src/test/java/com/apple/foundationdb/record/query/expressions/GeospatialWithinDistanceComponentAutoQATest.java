/*
 * GeospatialWithinDistanceComponentAutoQATest.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.TestRecordsGeoProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class GeospatialWithinDistanceComponentAutoQATest extends FDBRecordStoreQueryTestBase {
    private static final String INDEX_NAME = "cityLocation";
    private static final RecordMetaDataHook INDEX_HOOK = metaData ->
            metaData.addIndex("City", new Index(INDEX_NAME, coordinates(), IndexTypes.GEOSPATIAL_RTREE));

    @Test
    void nestedComponentEvaluatesAgainstNestedMessage() throws Exception {
        // BUG AQA-16: GeospatialWithinDistanceComponent.evalMessage evaluates its coordinates against the top-level record
        // instead of the nested message. Spec: QueryComponent.evalMessage javadoc, "If the key expression is meaningful
        // against a subrecord, it should evaluate against the message."
        // Nested under field("location"), the coordinates expression is relative to the Location message, so it must be
        // evaluated against that message rather than the top-level City record.
        saveCities(NO_HOOK, ImmutableList.of(city(1, 0.0, 0.0)));
        final GeospatialWithinDistanceComponent inner = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(0.0), DoubleValueOrParameter.value(0.0),
                DoubleValueOrParameter.value(1000.0), concat(field("latitude"), field("longitude")));
        final QueryComponent filter = Query.field("location").matches(inner);
        filter.validate(TestRecordsGeoProto.City.getDescriptor());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, NO_HOOK);
            final FDBStoredRecord<Message> rec = Objects.requireNonNull(recordStore.loadRecord(Tuple.from(1L)));
            assertThat(filter.eval(recordStore, EvaluationContext.EMPTY, rec)).isTrue();
        }
    }

    @Test
    void parameterizedFilterAgreesWithIndexScan() throws Exception {
        // The same parameterized filter must select the same records whether evaluated per record or via the index.
        final List<TestRecordsGeoProto.City> cities = ImmutableList.of(
                city(1, 10.0, 20.0), city(2, 10.5, 20.0), city(3, 10.7, 20.7), city(4, -10.0, -20.0));
        saveCities(INDEX_HOOK, cities);
        final GeospatialWithinDistanceComponent filter = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.parameter("lat"), DoubleValueOrParameter.parameter("lon"),
                DoubleValueOrParameter.parameter("radius"), coordinates());
        final EvaluationContext bindings = EvaluationContext.forBindings(Bindings.newBuilder()
                .set("lat", 10.0).set("lon", 20.0).set("radius", 100_000.0).build());

        final List<Long> indexed;
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, INDEX_HOOK);
            final RecordQueryPlan plan = planQuery(RecordQuery.newBuilder()
                    .setRecordType("City").setFilter(filter).build());
            assertThat(plan.getUsedIndexes()).contains(INDEX_NAME);
            indexed = executeIds(context, plan, bindings);
        }
        final List<Long> evaluated = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, INDEX_HOOK);
            for (final TestRecordsGeoProto.City c : cities) {
                final FDBStoredRecord<Message> rec =
                        Objects.requireNonNull(recordStore.loadRecord(Tuple.from((long)c.getGeoNameId())));
                if (Boolean.TRUE.equals(filter.eval(recordStore, bindings, rec))) {
                    evaluated.add((long)c.getGeoNameId());
                }
            }
        }
        assertThat(indexed).containsExactlyInAnyOrder(1L, 2L);
        assertThat(evaluated).containsExactlyInAnyOrderElementsOf(indexed);
    }

    @Test
    void planHashDistinguishesSwappedCenterAndParameters() {
        // Swapping the center latitude and longitude, or replacing a value by a parameter, changes the plan hash;
        // equal parameterized components hash equally.
        final GeospatialWithinDistanceComponent original = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(1.0), DoubleValueOrParameter.value(2.0),
                DoubleValueOrParameter.value(3.0), coordinates());
        final GeospatialWithinDistanceComponent swapped = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(2.0), DoubleValueOrParameter.value(1.0),
                DoubleValueOrParameter.value(3.0), coordinates());
        final GeospatialWithinDistanceComponent parameterized = parameterized("lat");
        for (final PlanHashable.PlanHashMode mode : ImmutableList.of(PlanHashable.CURRENT_LEGACY, PlanHashable.CURRENT_FOR_CONTINUATION)) {
            assertThat(swapped.planHash(mode)).isNotEqualTo(original.planHash(mode));
            assertThat(parameterized.planHash(mode)).isNotEqualTo(original.planHash(mode));
            assertThat(parameterized("lat").planHash(mode)).isEqualTo(parameterized.planHash(mode));
            assertThat(parameterized("other").planHash(mode)).isNotEqualTo(parameterized.planHash(mode));
        }
        assertThat(parameterized("lat")).isEqualTo(parameterized).hasSameHashCodeAs(parameterized);
        assertThat(parameterized.toString()).contains("WITHIN 3.0 METERS OF ($lat, 2.0)");
    }

    @Test
    void missingLocationDoesNotMatch() throws Exception {
        // A record without a location is neither inside nor outside the circle, so a scan with the filter skips it,
        // and NOT of the filter skips it too.
        final TestRecordsGeoProto.City noLocation = TestRecordsGeoProto.City.newBuilder().setGeoNameId(3).build();
        saveCities(NO_HOOK, ImmutableList.of(city(1, 0.0, 0.0), city(2, 5.0, 5.0), noLocation));
        final GeospatialWithinDistanceComponent filter = new GeospatialWithinDistanceComponent(
                DoubleValueOrParameter.value(0.0), DoubleValueOrParameter.value(0.0),
                DoubleValueOrParameter.value(1000.0), coordinates());
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, NO_HOOK);
            assertThat(executeIds(context, planQuery(RecordQuery.newBuilder()
                    .setRecordType("City").setFilter(filter).build()), EvaluationContext.EMPTY))
                    .containsExactly(1L);
            assertThat(executeIds(context, planQuery(RecordQuery.newBuilder()
                    .setRecordType("City").setFilter(Query.not(filter)).build()), EvaluationContext.EMPTY))
                    .containsExactly(2L);
        }
    }

    @Nonnull
    private static GeospatialWithinDistanceComponent parameterized(@Nonnull final String latitudeParameter) {
        return new GeospatialWithinDistanceComponent(DoubleValueOrParameter.parameter(latitudeParameter),
                DoubleValueOrParameter.value(2.0), DoubleValueOrParameter.value(3.0), coordinates());
    }

    @Nonnull
    private static KeyExpression coordinates() {
        return field("location").nest(concat(field("latitude"), field("longitude")));
    }

    @Nonnull
    private static TestRecordsGeoProto.City city(final int id, final double latitude, final double longitude) {
        final TestRecordsGeoProto.City.Builder builder = TestRecordsGeoProto.City.newBuilder().setGeoNameId(id);
        builder.getLocationBuilder().setLatitude(latitude).setLongitude(longitude);
        return builder.build();
    }

    private void openRecordStore(@Nonnull final FDBRecordContext context, @Nonnull final RecordMetaDataHook hook) throws Exception {
        openAnyRecordStore(TestRecordsGeoProto.getDescriptor(), context, hook);
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

    @Nonnull
    private List<Long> executeIds(@Nonnull final FDBRecordContext context, @Nonnull final RecordQueryPlan plan,
                                  @Nonnull final EvaluationContext evaluationContext) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR,
                plan.executePlan(recordStore, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE)
                        .map(result -> Objects.requireNonNull(result.getQueriedRecord()).getPrimaryKey().getLong(0))
                        .asList());
    }
}
