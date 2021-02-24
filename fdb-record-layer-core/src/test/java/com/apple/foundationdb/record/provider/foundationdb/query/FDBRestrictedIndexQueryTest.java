/*
 * FDBRestrictedIndexQueryTest.java
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

import com.apple.foundationdb.async.RangeSet;
import com.apple.foundationdb.record.AggregateFunctionNotSupportedException;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.descendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasNoDescendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests related to planning/execution with restricted (disabled, write-only, read-only, prohibited, etc.) indexes.
 */
@Tag(Tags.RequiresFDB)
public class FDBRestrictedIndexQueryTest extends FDBRecordStoreQueryTestBase {
    /**
     * Verify that plans do not use write-only indexes.
     * Verify that re-marking the index as readable makes the planner use the index again.
     * TODO: Abstract out common code in queryWithWriteOnly, queryWithDisabled, queryAggregateWithWriteOnly and queryAggregateWithDisabled (https://github.com/FoundationDB/fdb-record-layer/issues/4)
     */
    @DualPlannerTest
    public void queryWithWriteOnly() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_3_indexed").greaterThanOrEquals(5))
                .build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();
            recordStore.markIndexWriteOnly("MySimpleRecord$num_value_3_indexed").join();

            recordStore.saveRecord(
                    TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setRecNo(1066)
                            .setNumValue3Indexed(6)
                            .build()
            );
            recordStore.saveRecord(
                    TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setRecNo(1766)
                            .setNumValue3Indexed(4)
                            .build()
            );

            RecordQueryPlanner planner = new RecordQueryPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), recordStore.getTimer());
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, hasNoDescendant(indexScan(indexName(containsString("num_value_3_indexed")))));
            assertEquals(-625770219, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(2115232442, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(703683667, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

            List<TestRecords1Proto.MySimpleRecord> results = recordStore.executeQuery(plan)
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .asList().get();
            assertEquals(1, results.size());
            assertEquals(1066, results.get(0).getRecNo());
            assertEquals(6, results.get(0).getNumValue3Indexed());
            TestHelpers.assertDiscardedExactly(1, context);

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.uncheckedMarkIndexReadable("MySimpleRecord$num_value_3_indexed").join();

            clearStoreCounter(context);

            // Override state to read the write-only index.
            RecordQueryPlanner planner = new RecordQueryPlanner(
                    recordStore.getRecordMetaData(), new RecordStoreState(null, null), recordStore.getTimer());
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"),
                    bounds(hasTupleString("[[5],>")))));
            assertEquals(1008857208, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-482062757, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1290401822, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

            List<TestRecords1Proto.MySimpleRecord> results = recordStore.executeQuery(plan)
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                    .asList().get();
            assertEquals(1, results.size());
            assertEquals(1066, results.get(0).getRecNo());
            assertEquals(6, results.get(0).getNumValue3Indexed());
            TestHelpers.assertDiscardedNone(context);
        }
    }

    /**
     * Verify that the planner does not use disabled indexes.
     * Verify that re-enabling the index makes the planner use it again.
     * TODO: Abstract out common code in queryWithWriteOnly, queryWithDisabled, queryAggregateWithWriteOnly and queryAggregateWithDisabled (https://github.com/FoundationDB/fdb-record-layer/issues/4)
     */
    @DualPlannerTest
    public void queryWithDisabled() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.markIndexDisabled("MySimpleRecord$str_value_indexed").get();
            commit(context);
        }

        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066L)
                .setStrValueIndexed("not_actually_indexed")
                .build();

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("not_actually_indexed"))
                .build();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.saveRecord(record);
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, hasNoDescendant(indexScan(indexName(containsString("str_value_indexed")))));
            if (planner instanceof RecordQueryPlanner) {
                assertEquals(423324477, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            // TODO: Issue https://github.com/FoundationDB/fdb-record-layer/issues/1074
            // assertEquals(1148834070, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            } else {
                assertEquals(-1489463374, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            }

            List<Long> keys = recordStore.executeQuery(plan)
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).getRecNo()).asList().get();
            assertEquals(Collections.singletonList(1066L), keys);

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.uncheckedMarkIndexReadable("MySimpleRecord$str_value_indexed").get();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"),
                    bounds(hasTupleString("[[not_actually_indexed],[not_actually_indexed]]")))));
            assertEquals(-1270285984, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1303175523, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1939367966, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

            List<Long> keys = recordStore.executeQuery(plan)
                    .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).getRecNo()).asList().get();
            assertEquals(Collections.emptyList(), keys);

            commit(context);
        }
    }

    /**
     * Verify that write-only aggregate indexes are not used by the planner.
     * Verify that re-allowing reads to those indexes allows the planner to use them.
     * TODO: Abstract out common code in queryWithWriteOnly, queryWithDisabled, queryAggregateWithWriteOnly and queryAggregateWithDisabled (https://github.com/FoundationDB/fdb-record-layer/issues/4)
     */
    @Test
    public void queryAggregateWithWriteOnly() throws Exception {
        Index sumIndex = new Index("value3sum", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        Index maxIndex = new Index("value3max", field("num_value_3_indexed").ungrouped(), IndexTypes.MAX_EVER_TUPLE);
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", sumIndex);
            metaData.addIndex("MySimpleRecord", maxIndex);
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();
            recordStore.markIndexWriteOnly("value3sum").join();
            recordStore.markIndexWriteOnly("value3max").join();

            RangeSet rangeSet = new RangeSet(recordStore.indexRangeSubspace(sumIndex));
            rangeSet.insertRange(context.ensureActive(), Tuple.from(1000).pack(), Tuple.from(1500).pack(), true).get();

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1066).setNumValue3Indexed(42);
            recordStore.saveRecord(recBuilder.build());
            recBuilder.clear().setRecNo(1776).setNumValue3Indexed(100);
            recordStore.saveRecord(recBuilder.build());

            assertThrowsAggregateFunctionNotSupported(() ->
                            recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                                    new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName()),
                                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get(),
                    "value3sum.sum(Field { 'num_value_3_indexed' None} group 1)");

            assertThrowsAggregateFunctionNotSupported(() ->
                            recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                                    new IndexAggregateFunction(FunctionNames.MAX_EVER, maxIndex.getRootExpression(), maxIndex.getName()),
                                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get(),
                    "value3max.max_ever(Field { 'num_value_3_indexed' None} group 1)");

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.uncheckedMarkIndexReadable("value3sum").join();
            recordStore.uncheckedMarkIndexReadable("value3max").join();

            // Unsafe: made readable without building indexes, which is why sum gets wrong answer.
            assertEquals(42L, recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get().getLong(0));
            assertEquals(100L, recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.MAX_EVER, maxIndex.getRootExpression(), maxIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get().getLong(0));

            recordStore.rebuildAllIndexes().get();
            assertEquals(142L, recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get().getLong(0));
            assertEquals(100L, recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.MAX_EVER, maxIndex.getRootExpression(), maxIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get().getLong(0));
        }
    }

    /**
     * Verify that disabled aggregate indexes are not used by the planner.
     * Verify that re-enabling those indexes allows the planner to use them.
     * TODO: Abstract out common code in queryWithWriteOnly, queryWithDisabled, queryAggregateWithWriteOnly and queryAggregateWithDisabled (https://github.com/FoundationDB/fdb-record-layer/issues/4)
     */
    @Test
    public void queryAggregateWithDisabled() throws Exception {
        Index sumIndex = new Index("value3sum", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        Index maxIndex = new Index("value3max", field("num_value_3_indexed").ungrouped(), IndexTypes.MAX_EVER_TUPLE);
        RecordMetaDataHook hook = metaData -> {
            metaData.addIndex("MySimpleRecord", sumIndex);
            metaData.addIndex("MySimpleRecord", maxIndex);
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();
            recordStore.markIndexDisabled("value3sum").join();
            recordStore.markIndexDisabled("value3max").join();

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1066).setNumValue3Indexed(42);
            recordStore.saveRecord(recBuilder.build());
            recBuilder.clear().setRecNo(1776).setNumValue3Indexed(100);
            recordStore.saveRecord(recBuilder.build());

            assertThrowsAggregateFunctionNotSupported(() ->
                            recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                                    new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName()),
                                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get(),
                    "value3sum.sum(Field { 'num_value_3_indexed' None} group 1)");

            assertThrowsAggregateFunctionNotSupported(() ->
                            recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                                    new IndexAggregateFunction(FunctionNames.MAX_EVER, maxIndex.getRootExpression(), maxIndex.getName()),
                                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get(),
                    "value3max.max_ever(Field { 'num_value_3_indexed' None} group 1)");

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.uncheckedMarkIndexReadable("value3sum").join();
            recordStore.uncheckedMarkIndexReadable("value3max").join();

            // Unsafe: made readable without building indexes, which is why sum gets wrong answer.
            assertEquals(0L, recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get().getLong(0));
            assertNull(recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.MAX_EVER, maxIndex.getRootExpression(), maxIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get());

            recordStore.rebuildAllIndexes().get();
            assertEquals(142L, recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get().getLong(0));
            assertEquals(100L, recordStore.evaluateAggregateFunction(Collections.singletonList("MySimpleRecord"),
                    new IndexAggregateFunction(FunctionNames.MAX_EVER, maxIndex.getRootExpression(), maxIndex.getName()),
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).get().getLong(0));
        }
    }

    public static void assertThrowsAggregateFunctionNotSupported(Executable executable, String aggregateFunction) {
        final AggregateFunctionNotSupportedException e = assertThrows(AggregateFunctionNotSupportedException.class, executable);
        assertEquals("Aggregate function requires appropriate index", e.getMessage());
        assertEquals(aggregateFunction, e.getLogInfo().get(LogMessageKeys.FUNCTION.toString()).toString());
    }

    /**
     * Verify that queries do not use prohibited indexes.
     */
    @DualPlannerTest
    public void queryAllowedIndexes() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
            metaData.addIndex("MySimpleRecord", new Index("limited_str_value_index", field("str_value_indexed"),
                    Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.NOT_ALLOWED_FOR_QUERY_OPTIONS));
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();

            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recBuilder.setNumValueUnique(123);
            recordStore.saveRecord(recBuilder.build());

            recBuilder.setRecNo(2);
            recBuilder.setStrValueIndexed("xyz");
            recBuilder.setNumValueUnique(987);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("abc"))
                .build();
        RecordQueryPlan plan1 = planner.plan(query1);
        assertThat("should not use prohibited index", plan1, hasNoDescendant(indexScan("limited_str_value_index")));
        assertTrue(plan1.hasFullRecordScan(), "should use full record scan");
        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-223683738, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        // TODO: Issue https://github.com/FoundationDB/fdb-record-layer/issues/1074
        // assertEquals(1148834070, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-2136471589, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan1)) {
                FDBQueriedRecord<Message> rec = cursor.getNext().get();
                TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("abc", myrec.getStrValueIndexed());
                assertFalse(cursor.getNext().hasNext());
            }
            TestHelpers.assertDiscardedExactly(1, context);
            clearStoreCounter(context);
        }

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("abc"))
                .setAllowedIndex("limited_str_value_index")
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertThat("explicitly use prohibited index", plan2, descendant(indexScan("limited_str_value_index")));
        assertFalse(plan2.hasRecordScan(), "should not use record scan");
        assertEquals(-1573180774, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1694647451, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1751707953, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan2)) {
                FDBQueriedRecord<Message> rec = cursor.getNext().get();
                TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("abc", myrec.getStrValueIndexed());
                assertFalse(cursor.getNext().hasNext());
            }
            TestHelpers.assertDiscardedNone(context);
        }
    }

    /**
     * Verify that queries can override prohibited indexes explicitly.
     */
    @DualPlannerTest
    public void queryAllowedUniversalIndex() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.addUniversalIndex(
                    new Index("universal_num_value_2", field("num_value_2"),
                            Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.NOT_ALLOWED_FOR_QUERY_OPTIONS));
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();
            commit(context);
        }

        RecordQuery query1 = RecordQuery.newBuilder()
                .setFilter(Query.field("num_value_2").equalsValue(123))
                .build();
        RecordQueryPlan plan1 = planner.plan(query1);
        assertThat("should not use prohibited index", plan1, hasNoDescendant(indexScan("universal_num_value_2")));
        assertTrue(plan1.hasFullRecordScan(), "should use full record scan");
        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-709761689, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        // TODO: Issue https://github.com/FoundationDB/fdb-record-layer/issues/1074
        // assertEquals(-1366919407, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(1427197808, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        }

        RecordQuery query2 = RecordQuery.newBuilder()
                .setFilter(Query.field("num_value_2").equalsValue(123))
                .setAllowedIndex("universal_num_value_2")
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertThat("explicitly use prohibited index", plan2, descendant(indexScan("universal_num_value_2")));
        assertFalse(plan2.hasRecordScan(), "should not use record scan");
        assertEquals(-1692774119, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(-2103432626, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(-1953894133, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }

    /**
     * Verify that the query planner uses the specified {@link com.apple.foundationdb.record.query.IndexQueryabilityFilter}.
     * If both allowed indexes and a queryability filter are set, verify that the planner uses the allowed indexes.
     */
    @DualPlannerTest
    public void indexQueryabilityFilter() {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
            metaData.addIndex("MySimpleRecord", new Index("limited_str_value_index", field("str_value_indexed"),
                    Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.NOT_ALLOWED_FOR_QUERY_OPTIONS));
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteAllRecords();
            commit(context);
        }

        RecordQuery query1 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("abc"))
                .build();
        RecordQueryPlan plan1 = planner.plan(query1);
        assertThat("should not use prohibited index", plan1, hasNoDescendant(indexScan("limited_str_value_index")));
        assertTrue(plan1.hasFullRecordScan(), "should use full record scan");
        if (planner instanceof RecordQueryPlanner) {
            assertEquals(-223683738, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        // TODO: Issue https://github.com/FoundationDB/fdb-record-layer/issues/1074
        // assertEquals(-1148834070, plan1.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
        } else {
            assertEquals(-2136471589, plan1.planHash(PlanHashable.PlanHashKind.LEGACY));
        }

        RecordQuery query2 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("abc"))
                .setIndexQueryabilityFilter(index -> true)
                .build();
        RecordQueryPlan plan2 = planner.plan(query2);
        assertThat("explicitly use any index", plan2, descendant(indexScan("limited_str_value_index")));
        assertFalse(plan2.hasRecordScan(), "should not use record scan");
        assertEquals(-1573180774, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1694647451, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1751707953, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));

        RecordQuery query3 = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("abc"))
                .setIndexQueryabilityFilter(index -> false)
                .setAllowedIndex("limited_str_value_index")
                .build();
        RecordQueryPlan plan3 = planner.plan(query3);
        assertThat("should use allowed index despite index queryability filter", plan3, descendant(indexScan("limited_str_value_index")));
        assertFalse(plan3.hasRecordScan(), "should not use record scan");
        assertEquals(-1573180774, plan2.planHash(PlanHashable.PlanHashKind.LEGACY));
        assertEquals(1694647451, plan2.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        assertEquals(1751707953, plan2.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
    }
}
