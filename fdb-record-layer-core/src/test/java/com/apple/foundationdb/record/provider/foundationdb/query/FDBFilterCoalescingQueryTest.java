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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.Collections2;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests of the planner's ability to coalesce redundant and adjacent filters.
 */
@Tag(Tags.RequiresFDB)
public class FDBFilterCoalescingQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Validate that a query for all values within a given range on an indexed field will be coalesced
     * into a single query on that field.
     */
    @DualPlannerTest
    public void simpleRangeCoalesce() throws Exception {
        complexQuerySetup(NO_HOOK);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("num_value_3_indexed").greaterThanOrEquals(0),
                        Query.field("num_value_3_indexed").lessThanOrEquals(1)
                ))
                .build();

        // Index(MySimpleRecord$num_value_3_indexed [[0],[1]])
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[0],[1]]")))));
        assertEquals(1869980849, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(876021686, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertThat(myrec.getNumValue3Indexed(), allOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(1)));
                    assertThat(myrec.getRecNo() % 5, allOf(greaterThanOrEqualTo(0L), lessThanOrEqualTo(1L)));
                    i++;
                }
            }
            assertEquals(40, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Validate that a query for all values within a given range of versions will be coalesced into a single scan.
     * This is similar to the {@link #simpleRangeCoalesce()} test above, but it is for version key expressions in
     * particular.
     */
    @Test
    public void versionRangeCoalesce() throws Exception {
        Index versionIndex = new Index("MySimpleRecord$version", VersionKeyExpression.VERSION, IndexTypes.VERSION);
        RecordMetaDataHook hook = metaData -> {
            metaData.setStoreRecordVersions(true);
            metaData.addIndex("MySimpleRecord", versionIndex);
        };

        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            long readVersion = context.getReadVersion();
            FDBRecordVersion lowBoundary = FDBRecordVersion.firstInDBVersion(0);
            FDBRecordVersion highBoundary = FDBRecordVersion.lastInDBVersion(readVersion);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(
                            Query.version().greaterThan(lowBoundary),
                            Query.version().lessThan(highBoundary)
                    ))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName(versionIndex.getName()), bounds(hasTupleString("([" + lowBoundary.toVersionstamp() + "],[" + highBoundary.toVersionstamp() + "])")))));
            // NOTE: the plan hash is not validated here as that can change between executions as it includes the current read version

            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                int i = 0;
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    FDBRecordVersion version = rec.getVersion();
                    assertNotNull(version);
                    assertThat(version, allOf(lessThan(highBoundary), greaterThan(lowBoundary)));
                    i++;
                }
                assertEquals(100, i);
                assertDiscardedNone(context);
            }
        }
    }

    /**
     * Verify that the planner removes duplicate filters.
     */
    @DualPlannerTest
    public void duplicateFilters() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "str_value_indexed", "num_value_3_indexed"));
        };
        complexQuerySetup(hook);
        QueryComponent filter = Query.field("num_value_3_indexed").equalsValue(3);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsValue("even"),
                        filter,
                        filter))
                .build();

        RecordQueryPlan plan = planner.plan(query);
        // Index(multi_index [[even, 3],[even, 3]]) -> [num_value_3_indexed: KEY[1], rec_no: KEY[2], str_value_indexed: KEY[0]])
        final Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("[[even, 3],[even, 3]]"))));
        if (planner instanceof RecordQueryPlanner) {
            // TODO: Update this test when it gets implemented for old planner.
            //  Some query plans include redundant filtering operations even when the index is a complete specification (https://github.com/FoundationDB/fdb-record-layer/issues/2)
            // Fetch(Covering(...) | num_value_3_indexed EQUALS 3)
            assertThat(plan, fetch(filter(filter, coveringIndexScan(matcher))));
            assertEquals(-766201402, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1632715349, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        } else {
            assertThat(plan, matcher);
            assertEquals(681699231, plan.planHash(PlanHashable.CURRENT_LEGACY));
            assertEquals(-1692140528, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    assertEquals(3, myrec.getNumValue3Indexed());
                    assertEquals(3, myrec.getRecNo() % 5);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * TODO The planner does not currently coalesce the overlapping filters (>= 3 and > 0) (Cascades, more or less, can perform predicate simplification).
     * TODO: Planner does not currently coalesce overlapping filters (https://github.com/FoundationDB/fdb-record-layer/issues/1)
     */
    @DualPlannerTest
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

        // Index(multi_index [EQUALS $str, [GREATER_THAN_OR_EQUALS 3 && GREATER_THAN 0 && LESS_THAN_OR_EQUALS 4]])
        RecordQueryPlan plan = planner.plan(query);
        List<String> bounds = Arrays.asList("GREATER_THAN_OR_EQUALS 3", "LESS_THAN_OR_EQUALS 4", "GREATER_THAN 0");
        Collection<List<String>> combinations = Collections2.permutations(bounds);
        assertThat(plan, indexScan(allOf(indexName("multi_index"),
                bounds(anyOf(combinations.stream()
                        .map(ls -> hasTupleString("[EQUALS $str, [" + String.join(" && ", ls) + "]]"))
                        .collect(Collectors.toList()))))));
        assertEquals(241654378, plan.planHash(PlanHashable.CURRENT_LEGACY));
        assertEquals(-81379784, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));

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
