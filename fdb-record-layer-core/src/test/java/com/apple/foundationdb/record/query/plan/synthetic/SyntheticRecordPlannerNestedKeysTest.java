/*
 * SyntheticRecordPlannerNestedKeysTest.java
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.IntWrappingFunction;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SyntheticRecordPlanner} regarding joins on nested keys.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerNestedKeysTest extends AbstractSyntheticRecordPlannerTest {
    @Test
    void joinOnNestedKeysWithDifferentTypes() throws Exception {
        metaDataBuilder.getRecordType("CustomerWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("int_rec_id")));
        metaDataBuilder.getRecordType("OrderWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("MultiNestedFieldJoin");
        joined.addConstituent("order", "OrderWithHeader");
        joined.addConstituent("cust", "CustomerWithHeader");

        joined.addJoin("order",
                field("___header").nest("z_key"),
                "cust",
                field("___header").nest("z_key")
        );
        joined.addJoin("order",
                field("custRef").nest("string_value"),
                "cust",
                function(IntWrappingFunction.NAME, field("___header").nest("int_rec_id"))
        );

        metaDataBuilder.addIndex(joined, new Index("joinNestedConcat", concat(
                field("cust").nest("name"),
                field("order").nest("order_no")
        )));

        // Add index on custRef field to facilitate finding join partners of customer records
        metaDataBuilder.addIndex("OrderWithHeader", new Index("order$custRef", concat(
                field("___header").nest("z_key"),
                field("custRef").nest("string_value")
        )));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();
            final Index joinIndex = recordStore.getRecordMetaData().getIndex("joinNestedConcat");

            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);
            JoinedRecordType joinedRecordType = (JoinedRecordType)recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
            assertConstituentPlansMatch(planner, joinedRecordType, Map.of(
                    "order",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("CustomerWithHeader"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS wrap_int^-1($_j2)]")))))),
                    "cust",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("order$custRef"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]"))))))
            ));

            TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setIntRecId(1L))
                    .setName("Scott")
                    .setCity("Toronto")
                    .build();
            recordStore.saveRecord(customer);

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("23"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1"))
                    .build();
            recordStore.saveRecord(order);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Scott", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(10L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Update the customer name
            TestRecordsJoinIndexProto.CustomerWithHeader customerWithNewName = customer.toBuilder()
                    .setName("Alec")
                    .build();
            recordStore.saveRecord(customerWithNewName);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(10L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Update the order number
            TestRecordsJoinIndexProto.OrderWithHeader orderWithNewNumber = order.toBuilder()
                    .setOrderNo(42)
                    .build();
            recordStore.saveRecord(orderWithNewNumber);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(42L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Insert an order with no associated customer
            TestRecordsJoinIndexProto.OrderWithHeader orderWithNoCustomer = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(2).setRecId("noCustomer"))
                    .setOrderNo(1066)
                    .setQuantity(9001)
                    .build();
            recordStore.saveRecord(orderWithNoCustomer);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(42L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }

            // Update no customer record with a string that cannot be decanonicalized
            TestRecordsJoinIndexProto.OrderWithHeader orderWithNoncanonicalCustomer = orderWithNoCustomer.toBuilder()
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("dangling_ref"))
                    .build();
            recordStore.saveRecord(orderWithNoncanonicalCustomer);

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> entries = cursor.asList().get();
                assertEquals(1, entries.size());
                final IndexEntry indexEntry = entries.get(0);
                assertEquals("Alec", indexEntry.getKey().getString(0), "Incorrect customer name");
                assertEquals(42L, indexEntry.getKey().getLong(1), "Incorrect order number");
            }
        }
    }

    @Test
    void joinOnListOfKeysWithDifferentTypes() throws Exception {
        metaDataBuilder.getRecordType("CustomerWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("int_rec_id")));
        metaDataBuilder.getRecordType("OrderWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("OrderCCJoin");
        joined.addConstituent("order", "OrderWithHeader");
        joined.addConstituent("cust", "CustomerWithHeader");
        joined.addJoin("order",
                field("___header").nest("z_key"),
                "cust",
                field("___header").nest("z_key")
        );
        joined.addJoin("order",
                field("cc", KeyExpression.FanType.FanOut).nest("string_value"),
                "cust",
                function(IntWrappingFunction.NAME, field("___header").nest("int_rec_id"))
        );

        // Add an index on the cc field so that the join planner can use the index to resolve join pairs
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("OrderWithHeader"),
                new Index("OrderWithHeader$cc", concat(field("___header").nest("z_key"), field("cc", KeyExpression.FanType.FanOut).nest("string_value"))));
        // Add a join index listing all of the CC'd customer names for a given order number
        metaDataBuilder.addIndex(joined, new Index("OrderCCNames",
                concat(field("order").nest(field("___header").nest("z_key")), field("order").nest("order_no"), field("cust").nest("name"))));

        List<TestRecordsJoinIndexProto.CustomerWithHeader> customers = IntStream.range(0, 10)
                .mapToObj(i -> TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                        .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1L).setIntRecId(i).build())
                        .setName("Customer " + i)
                        .build()
                )
                .collect(Collectors.toList());
        List<TestRecordsJoinIndexProto.OrderWithHeader> orders = IntStream.range(0, customers.size())
                .mapToObj(i -> TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                        .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1L).setRecId("order_" + i))
                        .setOrderNo(1000 + i)
                        .setQuantity(100)
                        .addAllCc(IntStream.range(0, i).mapToObj(refId -> TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:" + refId).build()).collect(Collectors.toList()))
                        .build())
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);
            JoinedRecordType joinedRecordType = (JoinedRecordType)recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
            assertConstituentPlansMatch(planner, joinedRecordType, Map.of(
                    "order",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.inComparand(PlanMatchers.hasTypelessString("wrap_int^-1($_j2)"),
                                    PlanMatchers.typeFilter(Matchers.contains("CustomerWithHeader"),
                                            PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $__in_int_rec_id__0]"))))))),
                    "cust",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.primaryKeyDistinct(
                                    PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("OrderWithHeader$cc"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]"))))))))
            );

            for (int i = 0; i < customers.size(); i++) {
                recordStore.saveRecord(customers.get(i));
                recordStore.saveRecord(orders.get(orders.size() - i - 1));
            }

            final Index joinIndex = recordStore.getRecordMetaData().getIndex("OrderCCNames");
            for (TestRecordsJoinIndexProto.OrderWithHeader order : orders) {
                final Set<String> ccIds = order.getCcList().stream()
                        .map(TestRecordsJoinIndexProto.Ref::getStringValue)
                        .collect(Collectors.toSet());
                final List<String> customerNames = customers.stream()
                        .filter(customer -> ccIds.contains("i:" + customer.getHeader().getIntRecId()))
                        .map(TestRecordsJoinIndexProto.CustomerWithHeader::getName)
                        .collect(Collectors.toList());
                try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(
                        joinIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(order.getHeader().getZKey(), order.getOrderNo())), null, ScanProperties.FORWARD_SCAN)) {

                    final List<String> foundNames = cursor
                            .map(IndexEntry::getKey)
                            .map(key -> key.getString(2))
                            .asList()
                            .get();

                    assertEquals(customerNames, foundNames);
                }
            }
        }
    }

    @Test
    void joinOnMultipleNestedKeys() throws Exception {
        metaDataBuilder.getRecordType("CustomerWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));
        metaDataBuilder.getRecordType("OrderWithHeader").setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("MultiNestedFieldJoin");
        joined.addConstituent("order", "OrderWithHeader");
        joined.addConstituent("cust", "CustomerWithHeader");
        joined.addJoin("order", field("___header").nest("z_key"),
                "cust", field("___header").nest("z_key"));
        joined.addJoin("order", field("custRef").nest("string_value"),
                "cust", field("___header").nest("rec_id"));

        metaDataBuilder.addIndex(joined, new Index("joinNestedConcat", concat(
                field("cust").nest("name"),
                field("order").nest("order_no")
        )));
        metaDataBuilder.addIndex("OrderWithHeader", "order$custRef", concat(field("___header").nest("z_key"), field("custRef").nest("string_value")));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            final SyntheticRecordPlanner planner = new SyntheticRecordPlanner(recordStore);
            JoinedRecordType joinedRecordType = (JoinedRecordType)recordStore.getRecordMetaData().getSyntheticRecordType(joined.getName());
            assertConstituentPlansMatch(planner, joinedRecordType, Map.of(
                    "order",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.typeFilter(Matchers.contains("CustomerWithHeader"),
                                    PlanMatchers.scan(PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]")))))),
                    "cust",
                    SyntheticPlanMatchers.joinedRecord(List.of(
                            PlanMatchers.indexScan(Matchers.allOf(PlanMatchers.indexName("order$custRef"), PlanMatchers.bounds(PlanMatchers.hasTupleString("[EQUALS $_j1, EQUALS $_j2]"))))))
            ));

            TestRecordsJoinIndexProto.CustomerWithHeader.Builder custBuilder = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder();
            custBuilder.getHeaderBuilder().setZKey(1).setRecId("1");
            custBuilder.setName("Scott Fines");
            custBuilder.setCity("Toronto");

            recordStore.saveRecord(custBuilder.build());

            TestRecordsJoinIndexProto.OrderWithHeader.Builder orderBuilder = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder();
            orderBuilder.getHeaderBuilder().setZKey(1).setRecId("23");
            orderBuilder.setOrderNo(10).setQuantity(23);
            orderBuilder.getCustRefBuilder().setStringValue("1");

            recordStore.saveRecord(orderBuilder.build());

            //now check that we can scan them back out again
            Index joinIdex = recordStore.getRecordMetaData().getIndex("joinNestedConcat");
            List<IndexEntry> entries = recordStore.scanIndex(joinIdex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(1, entries.size());
            final IndexEntry indexEntry = entries.get(0);
            assertEquals("Scott Fines", indexEntry.getKey().getString(0), "Incorrect customer name");
            assertEquals(10L, indexEntry.getKey().getLong(1), "Incorrect order number");
        }
    }
}
