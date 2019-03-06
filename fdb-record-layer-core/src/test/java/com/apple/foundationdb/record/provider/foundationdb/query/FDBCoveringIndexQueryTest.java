/*
 * FDBCoveringIndexQueryTest.java
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

import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.TestHelpers.RealAnythingMatcher.anything;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasNoDescendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests related to planning queries that can use covering indexes.
 */
@Tag(Tags.RequiresFDB)
public class FDBCoveringIndexQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that a covering index is used when possible.
     */
    @Test
    public void coveringSimple() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Collections.singletonList(field("num_value_unique")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), bounds(hasTupleString("([990],>"))))));
        assertEquals(-158312359, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(myrec.getNumValueUnique() > 990);
                    assertFalse(myrec.hasNumValue2());
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a covering index is used with a compatible sort on the query.
     */
    @Test
    public void coveringSortNoFilter() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setSort(field("num_value_3_indexed"))
                .setRequiredResults(Collections.singletonList(field("num_value_3_indexed")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(unbounded())))));
        assertEquals(413789395, plan.planHash());
    }

    /**
     * Verify that a covering index is not used when it does not include enough fields; a regular index is used instead.
     */
    @Test
    public void coveringSimpleInsufficient() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"), true)
                .setRequiredResults(Arrays.asList(
                        field("num_value_unique"),
                        field("num_value_3_indexed")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, hasNoDescendant(coveringIndexScan(anything())));
        assertTrue(plan.isReverse());
        assertEquals(-158312358, plan.planHash());
    }

    /**
     * Verify that some other index scan is used when there is no appropriate index for the returned fields.
     */
    @Test
    public void notCoveringRecordScan() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setRequiredResults(Collections.singletonList(field("num_value_3_indexed")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, hasNoDescendant(coveringIndexScan(anything())));
        assertEquals(324762954, plan.planHash());
    }

    /**
     * Verify that if the filter contains additional parameters not satisfiable by an otherwise
     * covering index that it is not used.
     */
    @Test
    public void notCoveringWithAdditionalFilter() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_3_indexed").equalsValue(1), Query.field("num_value_2").lessThan(2)))
                .setRequiredResults(Collections.singletonList(field("num_value_3_indexed")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, hasNoDescendant(coveringIndexScan(anything())));
        assertThat(plan, filter(equalTo(Query.field("num_value_2").lessThan(2)),
                indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[[1],[1]]"))))));
        assertEquals(-1408807323, plan.planHash());
    }

    /**
     * Verify that an index can be covering if more than one field is required and they are in the key.
     */
    @Test
    public void coveringMulti() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index("multi_index", "num_value_unique", "num_value_2"));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Arrays.asList(
                        field("num_value_unique"),
                        field("num_value_2")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("multi_index"), bounds(hasTupleString("([990],>"))))));
        assertEquals(291429560, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(myrec.getNumValueUnique() > 990);
                    assertTrue(myrec.getNumValue2() == (999 - i) % 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that an index can be covering if some of the required fields are in the value part of the index.
     */
    @Test
    public void coveringMultiValue() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index(
                    "multi_index_value",
                    field("num_value_unique"),
                    field("num_value_2"),
                    IndexTypes.VALUE,
                    IndexOptions.UNIQUE_OPTIONS));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .setSort(field("num_value_unique"))
                .setRequiredResults(Arrays.asList(
                        field("num_value_unique"),
                        field("num_value_2")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("multi_index_value"), bounds(hasTupleString("([990],>"))))));
        assertEquals(-782505942, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(myrec.getNumValueUnique() > 990);
                    assertTrue(myrec.getNumValue2() == (999 - i) % 3);
                    i++;
                }
            }
            assertEquals(10, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that an index can be covering for nested fields if the field is in the value of the index.
     */
    @Test
    public void coveringWithHeaderValue() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(field("rec_no")));
            metaData.addIndex("MyRecord", new Index("MyRecord$str_value", field("str_value"), field("header").nest(field("path")),
                    IndexTypes.VALUE, Collections.emptyMap()));
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("lion"))
                .setRequiredResults(Collections.singletonList(field("header").nest("path")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("MyRecord$str_value"), bounds(hasTupleString("[[lion],[lion]]"))))));
        assertEquals(-629018945, plan.planHash());
    }

    /**
     * Verify that an index can be covering for concatenated nested fields in the value of the index.
     */
    @Test
    public void coveringWithHeaderConcatenatedValue() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(field("rec_no")));
            metaData.addIndex("MyRecord", new Index("MyRecord$str_value", field("str_value"), field("header").nest(concatenateFields("path", "num")),
                    IndexTypes.VALUE, Collections.emptyMap()));
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("leopard"))
                .setRequiredResults(Collections.singletonList(field("header").nest(concatenateFields("num", "path"))))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("MyRecord$str_value"), bounds(hasTupleString("[[leopard],[leopard]]"))))));
        assertEquals(-568702564, plan.planHash());
    }

    /**
     * Verify that an index can be covering if the required fields are in the primary key.
     */
    @Test
    public void coveringWithHeader() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
            metaData.addIndex("MyRecord", "str_value");
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            saveHeaderRecord(1, "a", 0, "lynx");
            saveHeaderRecord(2, "a", 1, "bobcat");
            saveHeaderRecord(3, "a", 2, "panther");

            saveHeaderRecord(1, "b", 3, "jaguar");
            saveHeaderRecord(2, "b", 4, "leopard");
            saveHeaderRecord(3, "b", 5, "lion");
            saveHeaderRecord(4, "b", 6, "tiger");
            context.commit();
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("lion"))
                .setRequiredResults(Collections.singletonList(field("header").nest("rec_no")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("MyRecord$str_value"), bounds(hasTupleString("[[lion],[lion]]"))))));
        assertEquals(-629018945, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(3, myrec.getHeader().getRecNo());
                }
            }
            context.commit();
            assertDiscardedNone(context);
        }

        query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").startsWith("l"))
                .setRequiredResults(Arrays.asList(field("header").nest(concatenateFields("path", "rec_no")), field("str_value")))
                .build();
        plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("MyRecord$str_value"), bounds(hasTupleString("{[l],[l]}"))))));
        assertEquals(-1471907004, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            List<Pair<String, Long>> results = new ArrayList<>();
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertThat(myrec.getStrValue(), startsWith("l"));
                    assertThat(myrec.getHeader().hasPath(), is(true));
                    assertThat(myrec.getHeader().hasRecNo(), is(true));
                    results.add(Pair.of(myrec.getHeader().getPath(), myrec.getHeader().getRecNo()));
                }
            }
            assertEquals(Arrays.asList(Pair.of("b", 2L), Pair.of("b", 3L), Pair.of("a", 1L)), results);
            context.commit();
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that if given a concatenated required-results field that a covering index is returned.
     */
    @Test
    public void coveringConcatenatedFields() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", "MySimpleRecord$2+3", concatenateFields("num_value_2", "num_value_3_indexed"));
        };
        complexQuerySetup(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(Query.field("num_value_2").greaterThan(0), Query.field("num_value_2").lessThan(10)))
                .setRequiredResults(Collections.singletonList(concatenateFields("num_value_2", "num_value_3_indexed")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(indexScan(allOf(indexName("MySimpleRecord$2+3"), bounds(hasTupleString("([0],[10])"))))));
        assertEquals(-152048326, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertThat(myrec.getNumValue2(), greaterThan(0));
                    assertThat(myrec.getNumValue2(), lessThan(10));
                    assertThat(myrec.hasNumValue3Indexed(), is(true));
                }
            }
            commit(context);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that covering indexes are not used when the an outer "header" field is missing from the primary key,
     * even though the index has all of the fields that the query actually asks for.
     */
    @Test
    public void notCoveringWithRequiredFieldsNotAvailable() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    // Even though path is required, it isn't part of the primary key, so won't be in the index,
                    // so won't be covering.
                    .setPrimaryKey(field("header").nest(field("rec_no")));
            metaData.addIndex("MyRecord", "str_value");
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("str_value").equalsValue("lion"))
                .setRequiredResults(Collections.singletonList(field("header").nest("rec_no")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, hasNoDescendant(coveringIndexScan(anything())));
        assertEquals(-629018945, plan.planHash());
    }

    /**
     * Verify that selecting the group key and the aggregate function from a grouped aggregate index can be planned
     * by a covering aggregate index.
     */
    @Test
    public void queryCoveringAggregate() throws Exception {
        Index sumIndex = new Index("value3sum", field("num_value_3_indexed").groupBy(Key.Expressions.concatenateFields("str_value_indexed", "num_value_2")), IndexTypes.SUM);
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", sumIndex);
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int i = 0; i < 20; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValue2(i % 3);
                recBuilder.setNumValue3Indexed(i % 5);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .setRequiredResults(Arrays.asList(field("str_value_indexed"), field("num_value_2")))
                .build();
        // This is here since the main planner doesn't currently support planning aggregates, so it's basically a
        // separate "mini-planner".
        // TODO: Support aggregate planning in the main query planner (https://github.com/FoundationDB/fdb-record-layer/issues/14)
        RecordQueryPlan plan = ((RecordQueryPlanner) planner).planCoveringAggregateIndex(query, "value3sum");

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals("even", myrec.getStrValueIndexed());
                    int sum = 0;
                    for (int j = 0; j < 20; j += 2) {
                        if (j % 3 == myrec.getNumValue2()) {
                            sum += j % 5;
                        }
                    }
                    assertEquals(sum, rec.getIndexEntry().getValue().getLong(0));
                    i++;
                }
            }
            assertEquals(3, i);
            assertDiscardedNone(context);
        }
    }
}
