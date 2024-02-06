/*
 * FDBQueryCompatibilityTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.BoundRecordQuery;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
class FDBQueryCompatibilityTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that a complex query with an appropriate multi-field index uses the index.
     */
    @Test
    void testQueryCompatibilityDifferentStructure() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        BoundRecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord1")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("num_value_2").equalsParameter("p3")))
                .buildAndBind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", 3)
                                .set("p3", 0)
                                .build());

        BoundRecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("num_value_2").equalsParameter("p3")))
                .buildAndBind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", 3)
                                .set("p3", 0)
                                .build());

        assertNotEquals(query.hashCode(), query2.hashCode());
        assertNotEquals(query, query2);
    }

    @Test
    void testQueryCompatibilityUniqueUsages() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        BoundRecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("num_value_2").equalsParameter("p3")))
                .buildAndBind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", 3)
                                .set("p3", 0)
                                .build());

        BoundRecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("num_value_2").equalsParameter("p3")))
                .buildAndBind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", 3)
                                .set("p3", 0)
                                .build());

        assertEquals(query.hashCode(), query2.hashCode());
        assertEquals(query2, query);
    }

    @Test
    void testQueryCompatibilityDifferentParameters() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        BoundRecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("num_value_2").equalsParameter("p3")))
                .buildAndBind(recordStore);

        BoundRecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("pa"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("num_value_2").equalsParameter("p3")))
                .buildAndBind(recordStore);

        assertNotEquals(query.hashCode(), query2.hashCode());
        assertNotEquals(query2, query);
    }

    @Test
    void testQueryCompatibilityNestedStructure() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("nested").oneOfThem()
                                .matches(Query.and(
                                        Query.field("str_value_indexed").equalsParameter("p3"),
                                        Query.field("num_value_3_indexed").equalsParameter("p4")))))
                .build();
        BoundRecordQuery boundQuery = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", 3)
                                .set("p3", "even")
                                .set("p4", 4)
                                .build());

        BoundRecordQuery boundQuery2 = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "odd")
                                .set("p2", 3)
                                .set("p3", "odd")
                                .set("p4", 4)
                                .build());

        assertEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertEquals(boundQuery2, boundQuery);

        boundQuery2 = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "odd")
                                .set("p2", 3)
                                .set("p3", "even")
                                .set("p4", 4)
                                .build());

        assertEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertEquals(boundQuery2, boundQuery);
    }

    @Test
    void testQueryCompatibilityNestedStructure2() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("num_value_3_indexed").equalsParameter("p2"),
                        Query.field("nested").oneOfThem()
                                .matches(Query.and(
                                        Query.field("str_value_indexed").equalsParameter("p3"),
                                        Query.field("str_value_indexed").equalsParameter("p4"),
                                        Query.field("num_value_3_indexed").equalsParameter("p5")))))
                .build();

        BoundRecordQuery boundQuery = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", 3)
                                .set("p3", "even")
                                .set("p4", "even")
                                .set("p5", 4)
                                .build());

        BoundRecordQuery boundQuery2 = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "whatever")
                                .set("p2", 3)
                                .set("p3", "odd")
                                .set("p4", "odd")
                                .set("p5", 4)
                                .build());

        assertEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertEquals(boundQuery2, boundQuery);

        boundQuery2 = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "whatever")
                                .set("p2", 3)
                                .set("p3", "odd")
                                .set("p4", "even")
                                .set("p5", 4)
                                .build());

        assertNotEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertNotEquals(boundQuery2, boundQuery);
    }

    @Test
    void testQueryCompatibilityConstraintSimple() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);

        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("str_value_indexed").equalsParameter("p2"),
                        Query.field("num_value_3_indexed").equalsParameter("p3"),
                        Query.field("num_value_2").equalsParameter("p4")))
                .build();

        BoundRecordQuery boundQuery =
                query.bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", "even")
                                .set("p3", 3)
                                .set("p4", 0)
                                .build());

        BoundRecordQuery boundQuery2 =
                query.bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", "even")
                                .set("p3", 3)
                                .set("p4", 0)
                                .build());

        assertEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertEquals(boundQuery2, boundQuery);

        boundQuery2 =
                query.bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", "odd")
                                .set("p3", 3)
                                .set("p4", 0)
                                .build());

        assertNotEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertNotEquals(boundQuery2, boundQuery);

        boundQuery2 =
                query.bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "odd")
                                .set("p2", "odd")
                                .set("p3", 3)
                                .set("p4", 0)
                                .build());

        assertEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertEquals(boundQuery2, boundQuery);
    }

    @Test
    void testQueryCompatibilityConstraintComplex() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("str_value_indexed").equalsParameter("p2"),
                        Query.field("str_value_indexed").equalsParameter("p3"),
                        Query.field("str_value_indexed").equalsParameter("p4"),
                        Query.field("str_value_indexed").equalsParameter("p5"),
                        Query.field("str_value_indexed").equalsParameter("p6"),
                        Query.field("str_value_indexed").equalsParameter("p7"),
                        Query.field("str_value_indexed").equalsParameter("p8"),
                        Query.field("num_value_3_indexed").equalsParameter("p9"),
                        Query.field("num_value_2").equalsParameter("p10")))
                .build();
        BoundRecordQuery boundQuery = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", "even")
                                .set("p3", "even")
                                .set("p4", "even")
                                .set("p5", "even")
                                .set("p6", "even")
                                .set("p7", "even")
                                .set("p8", "even")
                                .set("p9", 3)
                                .set("p10", 0)
                                .build());

        BoundRecordQuery boundQuery2 = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "even")
                                .set("p2", "even")
                                .set("p3", "even")
                                .set("p4", "odd")
                                .set("p5", "odd")
                                .set("p6", "even")
                                .set("p7", "even")
                                .set("p8", "even")
                                .set("p9", 3)
                                .set("p10", 0)
                                .build());

        assertNotEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertNotEquals(boundQuery2, boundQuery);

        boundQuery2 = query
                .bind(recordStore,
                        Bindings.newBuilder()
                                .set("p1", "odd")
                                .set("p2", "odd")
                                .set("p3", "odd")
                                .set("p4", "odd")
                                .set("p5", "odd")
                                .set("p6", "odd")
                                .set("p7", "odd")
                                .set("p8", "odd")
                                .set("p9", 3)
                                .set("p10", 0)
                                .build());

        assertEquals(boundQuery.hashCode(), boundQuery2.hashCode());
        assertEquals(boundQuery2, boundQuery);
    }

    @DualPlannerTest
    public void testParameterBindings() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        BoundRecordQuery boundQuery = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("p1"),
                        Query.field("str_value_indexed").equalsParameter("p2"),
                        Query.field("num_value_3_indexed").equalsParameter("p3"),
                        Query.field("num_value_2").equalsValue(0)))
                .buildAndBind(recordStore, Bindings.newBuilder()
                        .set("p1", "whatever")
                        .set("p2", "whatever")
                        .set("p3", "-10")
                        .build());

        final RecordQueryPlan plan = planQuery(boundQuery.getRecordQuery());
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final Bindings compatibleBindings =
                    Bindings.newBuilder()
                            .set("p1", "even")
                            .set("p2", "even")
                            .set("p3", 3)
                            .build();
            assertTrue(boundQuery.isCompatible(recordStore, compatibleBindings));

            final Bindings incompatibleBindings =
                    Bindings.newBuilder()
                            .set("p1", "even")
                            .set("p2", "odd")
                            .set("p3", 3)
                            .build();
            assertFalse(boundQuery.isCompatible(recordStore, incompatibleBindings));

            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, EvaluationContext.forBindings(compatibleBindings)).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertEquals(0, (myrec.getNumValue2() % 3));
                    assertEquals(3, (myrec.getNumValue3Indexed() % 5));
                    i++;
                }
            }
            assertEquals(3, i);
            TestHelpers.assertDiscardedNone(context);
        }
    }
}
