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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.TestRecordsGeoProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
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
