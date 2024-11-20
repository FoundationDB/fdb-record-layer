/*
 * SyntheticRecordPlannerJoinIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.query.plan.ScanComparisons.range;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.coveringIndexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.indexPlanOf;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.scanComparisons;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SyntheticRecordPlanner} regarding joining indexes.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerJoinIndexTest extends AbstractSyntheticRecordPlannerTest {
    @Test
    void joinIndex() {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("Simple_Other");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");
        metaDataBuilder.addIndex(joined, new Index("simple.str_value_other.num_value_3", concat(field("simple").nest("str_value"), field("other").nest("num_value_3"))));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < i; j++) {
                    TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                    simple.setRecNo(100 * i + j).setOtherRecNo(1000 + i);
                    simple.setStrValue((i + j) % 2 == 0 ? "even" : "odd");
                    recordStore.saveRecord(simple.build());
                }
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                other.setNumValue3(i);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            final Index index = recordStore.getRecordMetaData().getIndex("simple.str_value_other.num_value_3");
            final TupleRange range = new ScanComparisons.Builder()
                    .addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "even"))
                    .addInequalityComparison(new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1))
                    .build()
                    .toTupleRange();

            List<Tuple> expected1 = Arrays.asList(Tuple.from("even", 2, -1, Tuple.from(200), Tuple.from(1002)));
            List<Tuple> results1 = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected1, results1);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(201));
            TestRecordsJoinIndexProto.MySimpleRecord.Builder recordBuilder = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder().mergeFrom(record.getRecord());
            recordBuilder.setStrValue("even");
            recordStore.saveRecord(recordBuilder.build());

            List<Tuple> expected2 = Arrays.asList(Tuple.from("even", 2, -1, Tuple.from(200), Tuple.from(1002)), Tuple.from("even", 2, -1, Tuple.from(201), Tuple.from(1002)));
            List<Tuple> results2 = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected2, results2);

            recordStore.deleteRecord(Tuple.from(1002));

            List<Tuple> expected3 = Arrays.asList();
            List<Tuple> results3 = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected3, results3);
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            final QueryPlanner planner = setupPlanner(recordStore, null);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("Simple_Other")
                    .setFilter(Query.field("simple").matches(Query.field("str_value").equalsValue("even")))
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            Assertions.assertTrue(
                    indexPlan().where(RecordQueryPlanMatchers.indexName("simple.str_value_other.num_value_3"))
                            .and(scanComparisons(range("[[even],[even]]"))).matches(plan));
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                int count = 0;
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> record = Objects.requireNonNull(cursor.next());
                    Message message = record.getRecord();
                    count++;
                    Descriptors.Descriptor descriptor = message.getDescriptorForType();
                    Message simpleRecord = (Message)message.getField(descriptor.findFieldByName("simple"));
                    Descriptors.Descriptor simpleDescriptor = simpleRecord.getDescriptorForType();
                    Assertions.assertEquals(200L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
                    Assertions.assertEquals(1002L, simpleRecord.getField(simpleDescriptor.findFieldByName("other_rec_no")));
                    Message otherRecord = (Message)message.getField(descriptor.findFieldByName("other"));
                    Descriptors.Descriptor otherDescriptor = otherRecord.getDescriptorForType();
                    Assertions.assertEquals(1002L, otherRecord.getField(otherDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals(2, otherRecord.getField(otherDescriptor.findFieldByName("num_value_3")));
                }
                Assertions.assertEquals(1, count);
            }

            query = RecordQuery.newBuilder()
                    .setRecordType("Simple_Other")
                    .setFilter(Query.field("simple").matches(Query.field("str_value").equalsValue("even")))
                    .setRequiredResults(ImmutableList.of(field("simple").nest("str_value"), field("other").nest("num_value_3")))
                    .build();
            plan = planner.plan(query);
            Assertions.assertTrue(
                    coveringIndexPlan()
                            .where(indexPlanOf(indexPlan().where(RecordQueryPlanMatchers.indexName("simple.str_value_other.num_value_3"))
                                    .and(scanComparisons(range("[[even],[even]]"))))).matches(plan));
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                int count = 0;
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> record = Objects.requireNonNull(cursor.next());
                    Message message = record.getRecord();
                    count++;
                    Descriptors.Descriptor descriptor = message.getDescriptorForType();
                    Message simpleRecord = (Message)message.getField(descriptor.findFieldByName("simple"));
                    Descriptors.Descriptor simpleDescriptor = simpleRecord.getDescriptorForType();
                    Assertions.assertEquals(200L, simpleRecord.getField(simpleDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals("even", simpleRecord.getField(simpleDescriptor.findFieldByName("str_value")));
                    Message otherRecord = (Message)message.getField(descriptor.findFieldByName("other"));
                    Descriptors.Descriptor otherDescriptor = otherRecord.getDescriptorForType();
                    Assertions.assertEquals(1002L, otherRecord.getField(otherDescriptor.findFieldByName("rec_no")));
                    Assertions.assertEquals(2, otherRecord.getField(otherDescriptor.findFieldByName("num_value_3")));
                }
                Assertions.assertEquals(1, count);
            }
        }
    }

    @Test
    void buildJoinIndex() {
        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("Simple_Other");
        joined.addConstituent("simple", "MySimpleRecord");
        joined.addConstituent("other", "MyOtherRecord");
        joined.addJoin("simple", "other_rec_no", "other", "rec_no");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            for (int i = 0; i < 3; i++) {
                TestRecordsJoinIndexProto.MySimpleRecord.Builder simple = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder();
                simple.setRecNo(i).setOtherRecNo(1000 + i);
                simple.setNumValue2(i * 2);
                recordStore.saveRecord(simple.build());
                TestRecordsJoinIndexProto.MyOtherRecord.Builder other = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder();
                other.setRecNo(1000 + i);
                other.setNumValue3(i * 3);
                recordStore.saveRecord(other.build());
            }

            context.commit();
        }

        final Index joinedIndex = new Index("simple.num_value_2_other.num_value_3", concat(field("simple").nest("num_value_2"), field("other").nest("num_value_3")));
        metaDataBuilder.addIndex(joined, joinedIndex);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            recordStore.rebuildIndex(joinedIndex).join();
            final TupleRange range = new ScanComparisons.Builder()
                    .addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 2))
                    .addEqualityComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 3))
                    .build()
                    .toTupleRange();

            List<Tuple> expected1 = Arrays.asList(Tuple.from(2, 3, -1, Tuple.from(1), Tuple.from(1001)));
            List<Tuple> results1 = recordStore.scanIndex(joinedIndex, IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join();
            assertEquals(expected1, results1);
        }
    }

    @Test
    void aggregateJoinIndex() {
        final KeyExpression pkey = concat(recordType(), field("uuid"));
        metaDataBuilder.getRecordType("Customer").setPrimaryKey(pkey);
        metaDataBuilder.getRecordType("Order").setPrimaryKey(pkey);
        metaDataBuilder.getRecordType("Item").setPrimaryKey(pkey);
        metaDataBuilder.addIndex("Customer", "name");
        metaDataBuilder.addIndex("Order", "order_no");
        metaDataBuilder.addIndex("Order", "customer_uuid");
        metaDataBuilder.addIndex("Item", "order_uuid");
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("COI");
        joined.addConstituent("c", "Customer");
        joined.addConstituent("o", "Order");
        joined.addConstituent("i", "Item");
        joined.addJoin("o", "customer_uuid", "c", "uuid");
        joined.addJoin("i", "order_uuid", "o", "uuid");
        metaDataBuilder.addIndex(joined, new Index("total_price_by_city", field("i").nest("total_price").groupBy(field("c").nest("city")), IndexTypes.SUM));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setName("Jones").setCity("Boston");
            recordStore.saveRecord(c.build());
            c.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setName("Smith").setCity("New York");
            recordStore.saveRecord(c.build());
            c.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setName("Lee").setCity("Boston");
            recordStore.saveRecord(c.build());

            context.commit();
        }

        final RecordQuery findByName = RecordQuery.newBuilder().setRecordType("Customer").setFilter(Query.field("name").equalsParameter("name")).build();
        final RecordQuery findByOrderNo = RecordQuery.newBuilder().setRecordType("Order").setFilter(Query.field("order_no").equalsParameter("order_no")).build();
        final Index index = metaDataBuilder.getRecordMetaData().getIndex("total_price_by_city");
        final IndexAggregateFunction sumByCity = new IndexAggregateFunction(FunctionNames.SUM, index.getRootExpression(), index.getName());
        final List<String> coi = Collections.singletonList("COI");

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Jones")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1001).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(123).setQuantity(100).setTotalPrice(200).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(456).setQuantity(10).setTotalPrice(1000).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Smith")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1002).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(789).setQuantity(20).setTotalPrice(200).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Lee")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1003).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(123).setQuantity(150).setTotalPrice(300).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1500), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Lee")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setOrderNo(1004).setCustomerUuid(c.getUuid());
            recordStore.saveRecord(o.build());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(456).setQuantity(1).setTotalPrice(100).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1600), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Order.Builder o = TestRecordsJoinIndexProto.Order.newBuilder();
            o.mergeFrom(recordStore.planQuery(findByOrderNo).execute(recordStore, EvaluationContext.forBinding("order_no", 1003)).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());

            TestRecordsJoinIndexProto.Item.Builder i = TestRecordsJoinIndexProto.Item.newBuilder();
            i.setUuid(TupleFieldsHelper.toProto(UUID.randomUUID())).setItemNo(789).setQuantity(10).setTotalPrice(100).setOrderUuid(o.getUuid());
            recordStore.saveRecord(i.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1700), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            TestRecordsJoinIndexProto.Customer.Builder c = TestRecordsJoinIndexProto.Customer.newBuilder();
            c.mergeFrom(recordStore.planQuery(findByName).execute(recordStore, EvaluationContext.forBinding("name", "Lee")).first().join().orElseThrow(() -> new RuntimeException("not found")).getRecord());
            c.setCity("San Francisco");
            recordStore.saveRecord(c.build());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            assertEquals(Tuple.from(1200), recordStore.evaluateAggregateFunction(coi, sumByCity, Key.Evaluated.scalar("Boston"), IsolationLevel.SERIALIZABLE).join());

            Map<Tuple, Tuple> expected = ImmutableMap.of(Tuple.from("Boston"), Tuple.from(1200), Tuple.from("New York"), Tuple.from(200), Tuple.from("San Francisco"), Tuple.from(500));
            Map<Tuple, Tuple> results = recordStore.scanIndex(index, IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList().join().stream().collect(Collectors.toMap(IndexEntry::getKey, IndexEntry::getValue));
            assertEquals(expected, results);
        }
    }
}
