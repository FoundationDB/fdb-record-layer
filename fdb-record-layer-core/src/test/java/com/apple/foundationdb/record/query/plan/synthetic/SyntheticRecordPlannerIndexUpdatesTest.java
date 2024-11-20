/*
 * SyntheticRecordPlannerIndexUpdatesTest.java
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
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.SyntheticRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.IntWrappingFunction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SyntheticRecordPlanner} regarding index updates and synthetic index behaviors.
 */
@Tag(Tags.RequiresFDB)
@API(API.Status.EXPERIMENTAL)
public class SyntheticRecordPlannerIndexUpdatesTest extends AbstractSyntheticRecordPlannerTest {
    private String addJoinedIndexToMetaData() {
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

        final Index index = new Index("joinNestedConcat", concat(
                field("cust").nest("name"),
                field("order").nest("order_no")
        ));
        metaDataBuilder.addIndex(joined, index);

        // Add index on custRef field to facilitate finding join partners of customer records
        metaDataBuilder.addIndex("OrderWithHeader", new Index("order$custRef", concat(
                field("___header").nest("z_key"),
                field("custRef").nest("string_value")
        )));
        return index.getName();
    }

    @Test
    void indexUpdatesOnSameSyntheticTypeSharePlans() {
        String index1Name = addJoinedIndexToMetaData();
        SyntheticRecordTypeBuilder<?> syntheticType = metaDataBuilder.getSyntheticRecordType("MultiNestedFieldJoin");
        String index2Name = "quantityByCity";
        metaDataBuilder.addIndex(syntheticType, new Index(index2Name, field("order").nest("quantity").groupBy(field("cust").nest("city")), IndexTypes.SUM));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();
            timer.reset();

            TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder()
                            .setZKey(42)
                            .setIntRecId(1066L)
                    )
                    .setName("Scott")
                    .setCity("Toronto")
                    .build();
            recordStore.saveRecord(customer);
            // Two indexes should be updated, but only one synthetic type plan should be needed
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));
            timer.reset();

            TestRecordsJoinIndexProto.OrderWithHeader order1 = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder()
                            .setZKey(customer.getHeader().getZKey())
                            .setRecId("id2")
                    )
                    .setOrderNo(101)
                    .setQuantity(10)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1066"))
                    .build();
            recordStore.saveRecord(order1);
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));
            timer.reset();

            TestRecordsJoinIndexProto.OrderWithHeader order2 = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder()
                            .setZKey(customer.getHeader().getZKey())
                            .setRecId("id3")
                    )
                    .setOrderNo(102)
                    .setQuantity(15)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1066"))
                    .build();
            recordStore.saveRecord(order2);
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));
            timer.reset();

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            final Index index1 = recordStore.getRecordMetaData().getIndex(index1Name);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index1, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> index2Entries = cursor.asList().join();
                assertThat(index2Entries, hasSize(2));

                IndexEntry entry1 = index2Entries.get(0);
                assertEquals(index1, entry1.getIndex());
                assertEquals(Tuple.from("Scott", 101L, -1L, Tuple.from(42L, "id2"), Tuple.from(42L, 1066L)), entry1.getKey());
                assertEquals(TupleHelpers.EMPTY, entry1.getValue());

                IndexEntry entry2 = index2Entries.get(1);
                assertEquals(index1, entry2.getIndex());
                assertEquals(Tuple.from("Scott", 102L, -1L, Tuple.from(42L, "id3"), Tuple.from(42L, 1066L)), entry2.getKey());
                assertEquals(TupleHelpers.EMPTY, entry2.getValue());
            }

            final Index index2 = recordStore.getRecordMetaData().getIndex(index2Name);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index2, IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                List<IndexEntry> index2Entries = cursor.asList().join();
                assertThat(index2Entries, hasSize(1));
                IndexEntry entry = index2Entries.get(0);
                assertEquals(index2, entry.getIndex());
                assertEquals(Tuple.from("Toronto"), entry.getKey());
                assertEquals(Tuple.from(25L), entry.getValue());
            }

            context.commit();
        }
    }

    @Test
    void wontUpdateSyntheticTypeIfUnderlyingIndexesAreDisabled() throws Exception {
        /*
         * If the underlying indexes that are to be updated are not writable, then the synthetic update
         * should not happen. This test verifies that by faking out the maintainer with a maintainer
         * that always fails when you attempt to update it. If the error is thrown, then we haven't
         * got it right.
         */
        String indexName = addJoinedIndexToMetaData();

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();
            final Index joinIndex = recordStore.getRecordMetaData().getIndex(indexName);
            recordStore.markIndexDisabled(joinIndex).get();
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            timer.reset();
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

            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));
            context.commit();
        }


        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            //force-mark the index as readable
            recordStore.uncheckedMarkIndexReadable(indexName).get();

            //now verify that no records exist in that index
            Index joinIndex = recordStore.getRecordMetaData().getIndex("joinNestedConcat");
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                Assertions.assertEquals(0, cursor.getCount().get(), "Wrote records to a disabled index!");
            }
        }
    }

    @Test
    void updateOnlyNonDisabledSyntheticTypes() {
        // Create two synthetic record types, each with one index
        final JoinedRecordTypeBuilder joined1 = metaDataBuilder.addJoinedRecordType("FirstJoinedType");
        joined1.addConstituent("simple", "MySimpleRecord");
        joined1.addConstituent("other", "MyOtherRecord");
        joined1.addJoin("simple", "other_rec_no", "other", "rec_no");
        final Index joined1Index = new Index("joined1_index", concat(field("simple").nest("str_value"), field("other").nest("num_value_3")));
        metaDataBuilder.addIndex(joined1, joined1Index);

        final JoinedRecordTypeBuilder joined2 = metaDataBuilder.addJoinedRecordType("SecondJoinedType");
        joined2.addConstituent("simple", "MySimpleRecord");
        joined2.addConstituent("other", "MyOtherRecord");
        joined2.addJoin("simple", "other_rec_no", "other", "rec_no");
        final Index joined2Index = new Index("joined2_index", concat(field("other").nest("num_value_3"), field("simple").nest("str_value")));
        metaDataBuilder.addIndex(joined2, joined2Index);

        metaDataBuilder.addIndex("MySimpleRecord", "other_rec_no"); // To facilitate join lookups

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            TestRecordsJoinIndexProto.MySimpleRecord simpleRecord = TestRecordsJoinIndexProto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setOtherRecNo(1415L)
                    .setStrValue("foo")
                    .build();
            recordStore.saveRecord(simpleRecord);
            assertEquals(2L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            // Disable the index on joined2, but leave the index on joined1
            recordStore.markIndexDisabled(joined2Index).join();

            timer.reset();
            TestRecordsJoinIndexProto.MyOtherRecord otherRecord = TestRecordsJoinIndexProto.MyOtherRecord.newBuilder()
                    .setRecNo(1415L)
                    .setNumValue3(42)
                    .build();
            recordStore.saveRecord(otherRecord);
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            try (RecordCursor<IndexEntry> joined1Cursor = recordStore.scanIndex(joined1Index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                RecordCursorResult<IndexEntry> firstResult = joined1Cursor.getNext();
                assertTrue(firstResult.hasNext());
                IndexEntry entry = firstResult.get();
                assertEquals(joined1Index, entry.getIndex());
                assertEquals(Tuple.from("foo", 42L, -1L, Tuple.from(1066L), Tuple.from(1415L)), entry.getKey());
                assertEquals(Tuple.from(), entry.getValue());

                RecordCursorResult<IndexEntry> secondResult = joined1Cursor.getNext();
                assertFalse(secondResult.hasNext());
                assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, secondResult.getNoNextReason());
            }

            recordStore.uncheckedMarkIndexReadable(joined2Index.getName()).join();
            try (RecordCursor<IndexEntry> joined2Cursor = recordStore.scanIndex(joined2Index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                RecordCursorResult<IndexEntry> firstResult = joined2Cursor.getNext();
                assertFalse(firstResult.hasNext());
                assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, firstResult.getNoNextReason());
            }

            context.commit();
        }
    }

    @Test
    void deleteSyntheticIndexesWhenDisabled() throws Exception {
        String indexName = addJoinedIndexToMetaData();

        TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setIntRecId(1L))
                .setName("Scott")
                .setCity("Toronto")
                .build();

        Tuple pk;
        //write some data
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            pk = recordStore.saveRecord(customer).getPrimaryKey();

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("23"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1"))
                    .build();
            recordStore.saveRecord(order);

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            recordStore.markIndexDisabled(indexName).get();

            context.commit();
        }

        //update the record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            timer.reset();
            //changing the join key should mean deleting it from the synthetic index, unless the index is disabled
            recordStore.deleteRecord(pk);

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("33"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:2"))
                    .build();
            recordStore.saveRecord(order);

            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            context.commit();
        }

        //force-bring the index back to readable, and make sure that the old record is still there
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            //force-mark the index as readable
            recordStore.uncheckedMarkIndexReadable(indexName).get();

            Index joinIndex = recordStore.getRecordMetaData().getIndex(indexName);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                final RecordCursorResult<IndexEntry> result = cursor.getNext();
                Assertions.assertFalse(result.hasNext(), "Did not return element from index");
            }
        }
    }

    @Test
    void updateSyntheticIndexesWhenDisabled() throws Exception {
        String indexName = addJoinedIndexToMetaData();

        TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setIntRecId(1L))
                .setName("Scott")
                .setCity("Toronto")
                .build();

        //write some data
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            recordStore.saveRecord(customer);

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("23"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1"))
                    .build();
            recordStore.saveRecord(order);

            context.commit();
        }

        //disabling the index should force the index to be treated as empty
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            recordStore.markIndexDisabled(indexName).get();

            context.commit();
        }

        //update the record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            timer.reset();

            //changing the join key should mean deleting it from the synthetic index, unless the index is disabled
            recordStore.saveRecord(customer.toBuilder().setName("Bob").build());

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("33"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:2"))
                    .build();
            recordStore.saveRecord(order);

            assertEquals(0L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            context.commit();
        }

        //force-bring the index back to readable, and make sure that the old record is still there
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            //force-mark the index as readable
            recordStore.uncheckedMarkIndexReadable(indexName).get();

            Index joinIndex = recordStore.getRecordMetaData().getIndex(indexName);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                final RecordCursorResult<IndexEntry> result = cursor.getNext();
                Assertions.assertFalse(result.hasNext(), "Did not return element from index");
            }
        }
    }

    @Test
    void updateRecordWhenSyntheticIndexesIsWriteOnly() throws Exception {
        String indexName = addJoinedIndexToMetaData();

        TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setIntRecId(1L))
                .setName("Scott")
                .setCity("Toronto")
                .build();

        //write some data
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            recordStore.saveRecord(customer);

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("23"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1"))
                    .build();
            recordStore.saveRecord(order);

            context.commit();
        }

        // Mark the index as write only. The index should not be readable, but it should still get updated when records change
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            recordStore.markIndexWriteOnly(indexName).get();

            context.commit();
        }

        //update the record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            timer.reset();

            //changing the join key should mean deleting it from the synthetic index, unless the index is disabled
            recordStore.saveRecord(customer.toBuilder().setName("Bob").build());
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));
            timer.reset();

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("33"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:2"))
                    .build();
            recordStore.saveRecord(order);
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            context.commit();
        }

        //bring the index to readable, and the data should have been updated
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            recordStore.markIndexReadable(indexName).get();

            Index joinIndex = recordStore.getRecordMetaData().getIndex(indexName);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                final RecordCursorResult<IndexEntry> result = cursor.getNext();
                Assertions.assertTrue(result.hasNext());
                Assertions.assertEquals("Bob", result.get().getKey().getString(0));
            }
        }
    }

    @Test
    void insertRecordWhenSyntheticIndexIsWriteOnly() throws Exception {
        String indexName = addJoinedIndexToMetaData();

        //write some data
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

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

            context.commit();
        }

        //mark the index write only, then write some more data
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            recordStore.markIndexWriteOnly(indexName).get();

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            timer.reset();

            TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setIntRecId(2L))
                    .setName("Scott")
                    .setCity("Toronto")
                    .build();
            recordStore.saveRecord(customer);
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));
            timer.reset();

            TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("33"))
                    .setOrderNo(10)
                    .setQuantity(23)
                    .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:2"))
                    .build();
            recordStore.saveRecord(order);
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            context.commit();
        }

        //should be able to bring this back to readable, and read both rows
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            //force-mark the index as readable
            recordStore.markIndexReadable(indexName).get();

            Index joinIndex = recordStore.getRecordMetaData().getIndex(indexName);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                Assertions.assertEquals(2, cursor.getCount().get(), "Did not update a writable index");
            }
        }
    }

    @Test
    void deleteFromSyntheticIndexWhenIndexIsWriteOnly() throws Exception {
        String indexName = addJoinedIndexToMetaData();

        TestRecordsJoinIndexProto.OrderWithHeader order = TestRecordsJoinIndexProto.OrderWithHeader.newBuilder()
                .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setRecId("23"))
                .setOrderNo(10)
                .setQuantity(23)
                .setCustRef(TestRecordsJoinIndexProto.Ref.newBuilder().setStringValue("i:1"))
                .build();

        //write some data
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).create();

            TestRecordsJoinIndexProto.CustomerWithHeader customer = TestRecordsJoinIndexProto.CustomerWithHeader.newBuilder()
                    .setHeader(TestRecordsJoinIndexProto.Header.newBuilder().setZKey(1).setIntRecId(1L))
                    .setName("Scott")
                    .setCity("Toronto")
                    .build();
            recordStore.saveRecord(customer);

            recordStore.saveRecord(order);

            context.commit();
        }

        //mark the index write only, then write some more data
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();
            recordStore.markIndexWriteOnly(indexName).get();

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            Tuple pk;
            try (RecordCursor<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                final RecordCursorResult<FDBStoredRecord<Message>> next = cursor.getNext();
                Assertions.assertTrue(next.hasNext(), "Did not find a record in the store!");
                final FDBStoredRecord<Message> messageFDBStoredRecord = next.get();
                Assertions.assertNotNull(messageFDBStoredRecord);
                pk = messageFDBStoredRecord.getPrimaryKey();
            }

            timer.reset();
            recordStore.deleteRecord(pk);
            assertEquals(1L, timer.getCount(FDBStoreTimer.Counts.PLAN_SYNTHETIC_TYPE));

            context.commit();
        }

        //should be able to bring this back to readable, but will read zero rows
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore recordStore = recordStoreBuilder.setContext(context).open();

            //force-mark the index as readable
            recordStore.markIndexReadable(indexName).get();

            Index joinIndex = recordStore.getRecordMetaData().getIndex(indexName);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(joinIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
                Assertions.assertEquals(0, cursor.getCount().get(), "Did not update a writable index");
            }
        }
    }
}
