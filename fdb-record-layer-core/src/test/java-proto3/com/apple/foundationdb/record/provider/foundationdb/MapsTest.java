/*
 * MapsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.TestRecordsMapsProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test {@code map} fields.
 *
 * Maps are just a surface phenomenon in the generated code; the underlying representation is still a repeated message with
 * {@code key} and {@code value} fields.
 */
@Tag(Tags.RequiresFDB)
public class MapsTest extends FDBRecordStoreTestBase {

    @Test
    public void simple() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsMapsProto.getDescriptor(), context);

            TestRecordsMapsProto.StringToString.Builder recBuilder = TestRecordsMapsProto.StringToString.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.putMapValue("hello", "world");
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsMapsProto.getDescriptor(), context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecordsMapsProto.StringToString.Builder myrec1 = TestRecordsMapsProto.StringToString.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals("world",
                    myrec1.getMapValueOrThrow("hello"));
            commit(context);
        }
    }

    @Test
    public void indexed() throws Exception {
        final RecordMetaDataHook hook = md -> {
            md.addIndex("StringToInt", "mapKeyValue", Key.Expressions.mapKeyValues("map_value"));
        };

        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsMapsProto.getDescriptor(), context, hook);

            TestRecordsMapsProto.StringToInt.Builder recBuilder = TestRecordsMapsProto.StringToInt.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.putMapValue("num", 1);
            recBuilder.putMapValue("other", 2);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setRecNo(2);
            recBuilder.clearMapValue();
            recBuilder.putMapValue("num", 2);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }

        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("StringToInt")
                .setFilter(Query.field("map_value").mapMatches(k -> k.equalsValue("num"), v -> v.greaterThan(1)))
                .build();

        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsMapsProto.getDescriptor(), context, hook);
            RecordQueryPlan plan = planner.plan(query);
            List<Tuple> results = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).asList().join();
            assertEquals(Collections.singletonList(Tuple.from(2)), results);
            MatcherAssert.assertThat(plan, PlanMatchers.primaryKeyDistinct(PlanMatchers.indexScan(Matchers.allOf(
                    indexName("mapKeyValue"),
                    PlanMatchers.bounds(PlanMatchers.hasTupleString("([num, 1],[num]]"))))));
            commit(context);
        }
    }

}
