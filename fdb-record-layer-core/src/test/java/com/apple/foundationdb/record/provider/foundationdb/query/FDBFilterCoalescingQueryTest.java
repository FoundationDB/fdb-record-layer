/*
 * FDBFilterCoalescingQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.descendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the planner's ability to coalesce redundant and adjacent filters.
 */
@Tag(Tags.RequiresFDB)
public class FDBFilterCoalescingQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that the planner removes duplicate filters.
     * TODO We currently don't. Update this test when it gets implemented.
     * TODO: Some query plans include redundant filtering operations even when the index is a complete specification (https://github.com/FoundationDB/fdb-record-layer/issues/2)
     */
    @Test
    public void duplicateFilters() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "str_value_indexed", "num_value_3_indexed"));
        };
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        Query.field("num_value_3_indexed").equalsValue(3),
                        Query.field("num_value_3_indexed").equalsValue(3)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, descendant(indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("[[even, 3],[even, 3]]"))))));
        assertEquals(-109457345, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertTrue((myrec.getNumValue3Indexed() % 5) == 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * TODO The planner does not currently coalesce the overlapping filters (>= 3 and > 0).
     * TODO: Planner does not currently coalesce overlapping filters (https://github.com/FoundationDB/fdb-record-layer/issues/1)
     */
    @Test
    public void overlappingFilters() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "str_value_indexed", "num_value_3_indexed"));
        };
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("str"),
                        Query.field("num_value_3_indexed").greaterThanOrEquals(3),
                        Query.field("num_value_3_indexed").lessThanOrEquals(4),
                        Query.field("num_value_3_indexed").greaterThan(0)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("multi_index"),
                bounds(hasTupleString("[EQUALS $str, [GREATER_THAN_OR_EQUALS 3 && LESS_THAN_OR_EQUALS 4 && GREATER_THAN 0]]")))));
        assertEquals(-459970970, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            EvaluationContext boundContext = EvaluationContext.forBinding("str", "even");
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = plan.execute(recordStore, boundContext).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertThat(myrec.getNumValue3Indexed(), allOf(greaterThanOrEqualTo(3), lessThanOrEqualTo(4)));
                    i++;
                }
            }
            assertEquals(20, i);
            assertDiscardedNone(context);
        }
    }


}
