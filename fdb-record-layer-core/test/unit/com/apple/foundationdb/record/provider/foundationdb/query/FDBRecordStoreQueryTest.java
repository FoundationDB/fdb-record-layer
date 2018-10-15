/*
 * FDBRecordStoreQueryTest.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import com.apple.foundationdb.record.TestRecordsMultiProto;
import com.apple.foundationdb.record.TestRecordsTupleFieldsProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanComplexityException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.TestHelpers.RealAnythingMatcher.anything;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtMost;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.union;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A fixture for miscellaneous query tests that don't fit anywhere else.
 * Before adding a test to this class, consider adding it to another (possibly new) class in
 * {@link com.apple.foundationdb.record.provider.foundationdb.query}.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreQueryTest extends FDBRecordStoreQueryTestBase {
    @Test
    public void query() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(i + 1000);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();
        RecordQueryPlan plan = planner.plan(query);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            int i = 0;
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue((myrec.getNumValueUnique() % 2) == 0);
                    i++;
                }
            }
            assertEquals(50, i);
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that byte string queries work with indexes as expected, including with complex queries that should
     * generate plan unions.
     */
    @Test
    public void queryByteString() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);
            recordStore.deleteAllRecords();

            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 2)).setSecondary(byteString(0, 1, 2)).setUnique(byteString(0, 2))
                    .setName("foo").build());
            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 5)).setSecondary(byteString(0, 1, 3)).setUnique(byteString(1, 2))
                    .setName("box").build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("ByteStringRecord")
                    .setFilter(Query.field("secondary").equalsValue(byteString(0, 1, 3)))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(Matchers.allOf(indexName("ByteStringRecord$secondary"),
                    bounds(hasTupleString("[[[0, 1, 3]],[[0, 1, 3]]]")))));
            assertEquals(-1357153726, plan.planHash());
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                int count = 0;
                while (cursor.hasNext()) {
                    TestRecordsBytesProto.ByteStringRecord.Builder record = TestRecordsBytesProto.ByteStringRecord.newBuilder();
                    record.mergeFrom(cursor.next().getRecord());
                    assertEquals(byteString(0, 1, 3), record.getSecondary());
                    assertEquals("box", record.getName());
                    count++;
                }
                assertEquals(1, count);
                assertDiscardedNone(context);
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);
            clearStoreCounter(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("ByteStringRecord")
                    .setFilter(Query.or(
                            Query.and(Query.field("secondary").lessThanOrEquals(byteString(0, 1, 2)), Query.field("name").notNull()),
                            Query.field("secondary").greaterThanOrEquals(byteString(0, 1, 3))))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, union(
                    filter(equalTo(Query.field("name").notNull()),
                            indexScan(Matchers.allOf(indexName("ByteStringRecord$secondary"), bounds(hasTupleString("([null],[[0, 1, 2]]]"))))),
                    indexScan(Matchers.allOf(indexName("ByteStringRecord$secondary"), bounds(hasTupleString("[[[0, 1, 3]],>"))))));
            assertEquals(1352435039, plan.planHash());
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                int count = 0;
                while (cursor.hasNext()) {
                    TestRecordsBytesProto.ByteStringRecord.Builder record = TestRecordsBytesProto.ByteStringRecord.newBuilder();
                    record.mergeFrom(cursor.next().getRecord());

                    if (count == 0) {
                        assertEquals(byteString(0, 1, 2), record.getPkey());
                    } else {
                        assertEquals(byteString(0, 1, 5), record.getPkey());
                    }

                    count++;
                }
                assertEquals(2, count);
                assertDiscardedNone(context);
            }
        }
    }

    /**
     * Verify that simple queries execute properly with continuations.
     */
    @Test
    public void queryWithContinuation() throws Exception {
        setupSimpleRecordStore(null, (i, builder) -> {
            builder.setRecNo(i);
            builder.setNumValue2(i % 2);
            builder.setStrValueIndexed((i % 2 == 0) ? "even" : "odd");
        });

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, null);

            RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord").setAllowedIndexes(Collections.emptyList()).build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, typeFilter(contains("MySimpleRecord"), scan(bounds(unbounded()))));
            assertEquals(1623132305, plan.planHash());
            byte[] continuation = null;
            List<TestRecords1Proto.MySimpleRecord> retrieved = new ArrayList<>(100);
            while (true) {
                RecordCursor<TestRecords1Proto.MySimpleRecord> cursor =
                        plan.execute(evaluationContext, continuation, ExecuteProperties.newBuilder()
                                .setReturnedRowLimit(10)
                                .build())
                            .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build());
                List<TestRecords1Proto.MySimpleRecord> list = cursor.asList().get();
                assertEquals(Math.min(10, 100 - retrieved.size()), list.size());
                for (int i = 0; i < list.size(); i++) {
                    assertEquals(retrieved.size() + i, list.get(i).getRecNo());
                }
                assertDiscardedNone(context);
                retrieved.addAll(list);
                if (retrieved.size() > 100) {
                    fail("added more records than were present");
                }
                continuation = cursor.getContinuation();
                if (continuation == null) {
                    break;
                }
            }

            query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                    .setFilter(Query.field("str_value_indexed").equalsValue("odd"))
                    .build();
            plan = planner.plan(query);
            assertThat(plan, indexScan(Matchers.allOf(indexName("MySimpleRecord$str_value_indexed"),
                    bounds(hasTupleString("[[odd],[odd]]")))));
            assertEquals(-1917280682, plan.planHash());
            continuation = null;
            retrieved = new ArrayList<>(50);
            while (true) {
                RecordCursor<TestRecords1Proto.MySimpleRecord> cursor =
                        plan.execute(evaluationContext, continuation, ExecuteProperties.newBuilder()
                                .setReturnedRowLimit(5)
                                .build())
                            .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build());
                List<TestRecords1Proto.MySimpleRecord> list = cursor.asList().get();
                assertEquals(Math.min(5, 50 - retrieved.size()), list.size());
                for (int i = 0; i < list.size(); i++) {
                    assertEquals(2 * (retrieved.size() + i) + 1, list.get(i).getRecNo());
                }
                assertDiscardedNone(context);
                retrieved.addAll(list);
                if (retrieved.size() > 50) {
                    fail("added more records than met filter");
                }
                continuation = cursor.getContinuation();
                if (continuation == null) {
                    break;
                }
            }

            clearStoreCounter(context);
            query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                    .setFilter(Query.field("num_value_2").equalsValue(0))
                    .build();
            plan = planner.plan(query);
            assertThat(plan, filter(equalTo(query.getFilter()), typeFilter(anything(), scan(bounds(unbounded())))));
            assertEquals(913370491, plan.planHash());
            continuation = null;
            retrieved = new ArrayList<>(50);
            while (true) {
                RecordCursor<TestRecords1Proto.MySimpleRecord> cursor =
                        plan.execute(evaluationContext, continuation, ExecuteProperties.newBuilder()
                                .setReturnedRowLimit(15)
                                .build())
                                .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build());
                List<TestRecords1Proto.MySimpleRecord> list = cursor.asList().get();
                assertEquals(Math.min(15, 50 - retrieved.size()), list.size());
                for (int i = 0; i < list.size(); i++) {
                    assertEquals(2 * (retrieved.size() + i), list.get(i).getRecNo());
                }
                retrieved.addAll(list);
                if (retrieved.size() > 50) {
                    fail("added more records than met filter");
                }
                continuation = cursor.getContinuation();
                if (continuation == null) {
                    break;
                }
            }
            assertDiscardedAtMost(51, context);
        }
    }

    /**
     * Verify that simple queries execute properly with short time limits.
     */
    @Test
    public void queryWithShortTimeLimit() throws Exception {
        setupSimpleRecordStore(null, (i, builder) -> {
            builder.setRecNo(i);
            builder.setNumValue3Indexed(i / 10);
        });

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").equalsValue(5))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(1000)
                .setTimeLimit(1)
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .build();

        List<Long> list = new ArrayList<>();
        byte[] continuation = null;
        int count = 0;
        
        do {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, null);
                long timeLeft = context.getTransactionCreateTime() + executeProperties.getTimeLimit() - System.currentTimeMillis();
                if (timeLeft > 0) {
                    Thread.sleep(timeLeft);
                }
                count++;
                try (RecordCursor<Long> cursor =
                             plan.execute(evaluationContext, continuation, executeProperties)
                     .map(record -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(record.getRecord()).getRecNo())) {
                    cursor.forEach(list::add).join();
                    continuation = cursor.getContinuation();
                    if (continuation == null) {
                        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursor.getNoNextReason());
                    } else {
                        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursor.getNoNextReason());
                    }
                }
            }
        } while (continuation != null);

        assertEquals(LongStream.range(50, 60).mapToObj(Long::valueOf).collect(Collectors.toList()), list);
        assertEquals(11, count);
    }

    /**
     * Verify that index lookups work with parameterized queries.
     */
    @Test
    public void testParameterQuery1() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.and(
                        Query.field("str_value_indexed").equalsParameter("1"),
                        Query.field("num_value_2").equalsParameter("2")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(Matchers.allOf(
                indexName("multi_index"),
                bounds(hasTupleString("[EQUALS $1, EQUALS $2]")))));
        assertEquals(584809367, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int attempt = 1; attempt <= 2; attempt++) {
                String strValue;
                int numValue2;
                switch (attempt) {
                    case 1:
                        strValue = "even";
                        numValue2 = 1;
                        break;
                    case 2:
                    default:
                        strValue = "odd";
                        numValue2 = 2;
                        break;
                }
                Bindings.Builder bindings = Bindings.newBuilder();
                bindings.set("1", strValue);
                bindings.set("2", numValue2);
                evaluationContext = recordStore.createEvaluationContext(bindings.build());
                int i = 0;
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertEquals(strValue, myrec.getStrValueIndexed());
                        assertTrue((myrec.getNumValue2() % 3) == numValue2);
                        i++;
                    }
                }
                assertEquals(16, i);
            }
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that a record scan can implement a filter on the primary key.
     */
    @Test
    public void testPartialRecordScan() throws Exception {
        RecordMetaDataHook hook = complexPrimaryKeyHook();
        complexQuerySetup(hook);
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(
                        Query.field("str_value_indexed").equalsValue("even"))
                .setAllowedIndexes(Collections.emptyList())
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertTrue(plan.hasRecordScan(), "should use scan");
        assertFalse(plan.hasFullRecordScan(), "should not use full scan");
    }

    /**
     * Verify that enum field indexes are used.
     */
    @Test
    public void enumFields() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder type = metaData.getRecordType("MyShapeRecord");
            metaData.addIndex(type, new Index("size", field("size")));
            metaData.addIndex(type, new Index("color", field("color")));
            metaData.addIndex(type, new Index("shape", field("shape")));
        };
        setupEnumShapes(hook);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyShapeRecord")
                .setFilter(Query.field("color").equalsValue(TestRecordsEnumProto.MyShapeRecord.Color.RED))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(indexName("color")));
        assertFalse(plan.hasRecordScan(), "should not use record scan");
        assertEquals(1393755963, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openEnumRecordStore(context, hook);
            int i = 0;
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsEnumProto.MyShapeRecord.Builder shapeRec = TestRecordsEnumProto.MyShapeRecord.newBuilder();
                    shapeRec.mergeFrom(rec.getRecord());
                    assertEquals(TestRecordsEnumProto.MyShapeRecord.Color.RED, shapeRec.getColor());
                    i++;
                }
            }
            assertEquals(9, i);
            assertDiscardedNone(context);
        }
    }

    @Test
    public void nullQuery() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("yes");
            recBuilder.setNumValueUnique(1);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(2);
            recBuilder.setStrValueIndexed("no");
            recBuilder.setNumValueUnique(2);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(3);
            recBuilder.setNumValueUnique(3);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }

        {
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("str_value_indexed").notEquals("yes"))
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                int i = 0;
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertTrue(myrec.getNumValueUnique() != 3);
                        i++;
                    }
                }
                assertEquals(1, i);
                assertDiscardedAtMost(2, context);
            }
        }
        {
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("str_value_indexed").notNull())
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            try (FDBRecordContext context = openContext()) {
                clearStoreCounter(context);
                openSimpleRecordStore(context);
                int i = 0;
                try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = cursor.next();
                        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                        myrec.mergeFrom(rec.getRecord());
                        assertTrue(myrec.hasStrValueIndexed());
                        i++;
                    }
                }
                assertEquals(2, i);
                assertDiscardedNone(context);
            }
        }
    }

    /**
     * Verify that complex queries on multiple types with uncommon primary keys are implemented, but only with record
     * scans, type filters, and filters.
     */
    @Test
    public void testUncommonPrimaryKey() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openMultiRecordStore(context);
            recordStore.deleteAllRecords();

            Message record = TestRecordsMultiProto.MultiRecordOne.newBuilder()
                    .setId(1066L)
                    .setName("William the Conqueror")
                    .addElement("Hastings")
                    .addElement("Normandy")
                    .addElement("Canterbury")
                    .addElement("England")
                    .build();
            recordStore.saveRecord(record);

            record = TestRecordsMultiProto.MultiRecordOne.newBuilder()
                    .setId(948L)
                    .setName("Æthelred the Unræd")
                    .addElement("St. Paul's")
                    .addElement("Ælfgifu of York")
                    .addElement("Ælfthryth")
                    .build();
            recordStore.saveRecord(record);

            record = TestRecordsMultiProto.MultiRecordTwo.newBuilder()
                    .setEgo(1776L)
                    .setValue("George III")
                    .addElement("Hanover")
                    .addElement("Great Britain")
                    .addElement("Proclamation of 1763")
                    .build();
            recordStore.saveRecord(record);

            record = TestRecordsMultiProto.MultiRecordThree.newBuilder()
                    .setEgo(800L)
                    .setData("Charlemagne")
                    .addElement("Saxony")
                    .addElement("Francia")
                    .addElement("Rome")
                    .build();
            recordStore.saveRecord(record);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordTypes(Arrays.asList("MultiRecordTwo", "MultiRecordThree"))
                    .setFilter(Query.field("element").oneOfThem().greaterThan("A"))
                    .setRemoveDuplicates(true)
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, filter(equalTo(query.getFilter()),
                    typeFilter(containsInAnyOrder("MultiRecordTwo", "MultiRecordThree"), scan(bounds(unbounded())))));
            assertEquals(1808059644, plan.planHash());
            assertEquals(Arrays.asList(800L, 1776L),
                    plan.execute(evaluationContext)
                            .map(FDBQueriedRecord::getRecord)
                            .map(message -> message.getField(message.getDescriptorForType().findFieldByNumber(1)))
                            .asList().join());
            // TOOD add a performance test here, but doing it before refactoring would be a lot of extra work

            query = RecordQuery.newBuilder()
                    .setRecordTypes(Arrays.asList("MultiRecordOne", "MultiRecordTwo"))
                    .setFilter(Query.field("element").oneOfThem().greaterThan("A"))
                    .setRemoveDuplicates(true)
                    .build();
            plan = planner.plan(query);
            assertThat(plan, filter(equalTo(query.getFilter()),
                    typeFilter(containsInAnyOrder("MultiRecordOne", "MultiRecordTwo"), scan(bounds(unbounded())))));
            assertEquals(-663593392, plan.planHash());
            assertEquals(Arrays.asList(948L, 1066L, 1776L),
                    plan.execute(evaluationContext)
                            .map(FDBQueriedRecord::getRecord)
                            .map(message -> message.getField(message.getDescriptorForType().findFieldByNumber(1)))
                            .asList().join());
            // TOOD add a performance test here, but doing it before refactoring would be a lot of extra work
        }
    }

    /**
     * Verify that null is excluded from an index scan.
     */
    @Test
    public void queryExcludeNull() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                if (i % 2 == 0) {
                    recBuilder.setNumValue3Indexed(i % 5);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").lessThan(2))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, indexScan(Matchers.allOf(indexName("MySimpleRecord$num_value_3_indexed"),
                bounds(hasTupleString("([null],[2])")))));
        assertEquals(-699045510, plan.planHash());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            int i = 0;
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = plan.execute(evaluationContext)) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(myrec.hasNumValue3Indexed() && myrec.getNumValue3Indexed() < 2);
                    i++;
                }
            }
            assertEquals(20, i);
            assertDiscardedNone(context);
        }
    }

    @Test
    public void queryComplexityLimit() throws Exception {
        RecordMetaDataHook hook = complexQuerySetupHook();
        complexQuerySetup(hook);

        List<QueryComponent> clauses = Lists.newArrayList(Query.field("num_value_unique").greaterThanOrEquals(-1));

        for (int i = 0; i < RecordQueryPlanner.DEFAULT_COMPLEXITY_THRESHOLD + 10; i++) {
            clauses.add(Query.field("num_value_unique").greaterThanOrEquals(i));
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.or(clauses))
                .build();

        assertThrows(RecordQueryPlanComplexityException.class, () -> {
            planner.plan(query);
        });
    }

    @Test
    public void uuidPrimaryKey() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final List<UUID> uuids = setupTupleFields(context);

            FDBStoredRecord<Message> rec3 = recordStore.loadRecord(Tuple.from(uuids.get(3)));
            TestRecordsTupleFieldsProto.MyFieldsRecord.Builder myrec3 = TestRecordsTupleFieldsProto.MyFieldsRecord.newBuilder();
            myrec3.mergeFrom(rec3.getRecord());
            assertEquals("s3", TupleFieldsHelper.fromProto(myrec3.getFstring()));

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MyFieldsRecord")
                    .setFilter(Query.field("uuid").lessThan(uuids.get(3)))
                    .setSort(field("uuid"))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, scan(bounds(hasTupleString(String.format("([null],[%s])", uuids.get(3))))));
            assertEquals(uuids.subList(0, 3), plan.execute(evaluationContext).map(r -> r.getPrimaryKey().getUUID(0)).asList().join());
        }
    }

    @Test
    public void nullableInt32() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final List<UUID> uuids = setupTupleFields(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MyFieldsRecord")
                    .setFilter(Query.field("fint32").isNull())
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(Matchers.allOf(indexName("MyFieldsRecord$fint32"),
                    bounds(hasTupleString("[[null],[null]]")))));
            assertEquals(uuids.subList(3, 4), plan.execute(evaluationContext).map(r -> r.getPrimaryKey().getUUID(0)).asList().join());
        }
    }
}
