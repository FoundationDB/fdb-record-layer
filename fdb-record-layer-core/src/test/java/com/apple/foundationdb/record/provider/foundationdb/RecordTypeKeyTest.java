/*
 * RecordTypeKeyTest.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.TestHelpers.assertThrows;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for record type key in primary keys.
 */
@Tag(Tags.RequiresFDB)
public class RecordTypeKeyTest extends FDBRecordStoreTestBase {

    @Test
    public void testExplicitKeys() throws Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        final RecordTypeBuilder t1 = metaDataBuilder.getRecordType("MySimpleRecord");
        final RecordTypeBuilder t2 = metaDataBuilder.getRecordType("MyOtherRecord");
        final KeyExpression pkey = concat(recordType(), field("rec_no"));
        t1.setPrimaryKey(pkey);
        t1.setRecordTypeKey("t1");
        t2.setPrimaryKey(pkey);
        RecordMetaData metaData = metaDataBuilder.getRecordMetaData();
        assertEquals("t1", metaData.getRecordType("MySimpleRecord").getExplicitRecordTypeKey());
        assertNull(metaData.getRecordType("MyOtherRecord").getExplicitRecordTypeKey());

        metaDataBuilder = new RecordMetaDataBuilder(metaData.toProto(), false);
        metaData = metaDataBuilder.getRecordMetaData();
        assertEquals("t1", metaData.getRecordType("MySimpleRecord").getExplicitRecordTypeKey());
        assertNull(metaData.getRecordType("MyOtherRecord").getExplicitRecordTypeKey());
    }

    @Test
    public void testIllegalKey() throws Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        final RecordTypeBuilder t1 = metaDataBuilder.getRecordType("MySimpleRecord");
        assertThrows(MetaDataException.class, () -> {
            t1.setRecordTypeKey(this);
            return null;
        });
    }

    @Test
    public void testDuplicateRecordTypeKeys() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder t1 = metaData.getRecordType("MySimpleRecord");
            final RecordTypeBuilder t2 = metaData.getRecordType("MyOtherRecord");
            final KeyExpression pkey = concat(recordType(), field("rec_no"));
            t1.setRecordTypeKey("same");
            t1.setPrimaryKey(pkey);
            t2.setRecordTypeKey("same");
            t2.setPrimaryKey(pkey);
        };
        assertThrows(MetaDataException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, hook);
            }
            return null;
        });
    }

    @Test
    public void testOverlappingRecordTypeKeys() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder t1 = metaData.getRecordType("MySimpleRecord");
            final RecordTypeBuilder t2 = metaData.getRecordType("MyOtherRecord");
            final KeyExpression pkey = concat(recordType(), field("rec_no"));
            t1.setPrimaryKey(pkey);
            t2.setRecordTypeKey(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER);
            t2.setPrimaryKey(pkey);
        };
        assertThrows(MetaDataException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, hook);
            }
            return null;
        });
    }

    static final RecordMetaDataHook BASIC_HOOK = metaData -> {
        final RecordTypeBuilder t1 = metaData.getRecordType("MySimpleRecord");
        final RecordTypeBuilder t2 = metaData.getRecordType("MyOtherRecord");
        final KeyExpression pkey = concat(recordType(), field("rec_no"));
        t1.setPrimaryKey(pkey);
        t2.setPrimaryKey(pkey);
        metaData.removeIndex(COUNT_INDEX.getName());
        metaData.removeIndex(COUNT_UPDATES_INDEX.getName());
        metaData.addUniversalIndex(new Index("countByRecordType", GroupingKeyExpression.of(empty(), recordType()), IndexTypes.COUNT));
    };

    @Test
    public void testWriteRead() throws Exception {
        List<FDBStoredRecord<Message>> recs = saveSomeRecords(BASIC_HOOK);

        // Primary key encodes record type.
        assertEquals(Tuple.from(1, 123), recs.get(0).getPrimaryKey());
        assertEquals(Tuple.from(1, 456), recs.get(1).getPrimaryKey());
        assertEquals(Tuple.from(2, 123), recs.get(2).getPrimaryKey());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);

            // Retrieve by record id.
            for (FDBStoredRecord<Message> rec : recs) {
                assertEquals(rec, recordStore.loadRecord(rec.getPrimaryKey()));
            }

            // Index entries properly rendezvous with record.
            assertEquals(recs.subList(0, 1), recordStore.executeQuery(RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord").setFilter(Query.field("str_value_indexed").equalsValue("abc")).build())
                    .map(FDBQueriedRecord::getStoredRecord).asList().join());
        }
    }

    @Test
    public void testScan() throws Exception {
        List<FDBStoredRecord<Message>> recs = saveSomeRecords(BASIC_HOOK);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            assertEquals(recs.subList(0, 2), recordStore.executeQuery(query)
                    .map(FDBQueriedRecord::getStoredRecord).asList().join());
            assertThat(plan, scan(bounds(hasTupleString("[IS MySimpleRecord]"))));
        }
    }

    @Test
    public void testIndexScan() throws Exception {
        // This means that some record types do not have a record type key, so an index scan will be better.
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder t1 = metaData.getRecordType("MySimpleRecord");
            final KeyExpression pkey = concat(recordType(), field("rec_no"));
            t1.setPrimaryKey(pkey);
            metaData.removeIndex(COUNT_INDEX.getName());
            metaData.removeIndex(COUNT_UPDATES_INDEX.getName());
        };

        List<FDBStoredRecord<Message>> recs = saveSomeRecords(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            assertEquals(recs.subList(0, 2), recordStore.executeQuery(query)
                    .map(FDBQueriedRecord::getStoredRecord).asList().join());
            assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(unbounded()))));
        }
    }

    @Test
    public void testSinglyBoundedScan() throws Exception {
        List<FDBStoredRecord<Message>> recs = saveSomeRecords(BASIC_HOOK);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("rec_no").lessThan(400L))
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            assertEquals(recs.subList(0, 1), recordStore.executeQuery(query)
                    .map(FDBQueriedRecord::getStoredRecord).asList().join());
            assertThat(plan, scan(bounds(hasTupleString("[IS MySimpleRecord, [LESS_THAN 400]]"))));
        }
    }

    @Test
    public void testDoublyBoundedScan() throws Exception {
        List<FDBStoredRecord<Message>> recs = saveSomeRecords(BASIC_HOOK);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(
                            Query.field("rec_no").greaterThan(200L),
                            Query.field("rec_no").lessThan(500L)))
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            assertEquals(recs.subList(1, 2), recordStore.executeQuery(query)
                    .map(FDBQueriedRecord::getStoredRecord).asList().join());
            assertThat(plan, scan(bounds(hasTupleString("[IS MySimpleRecord, [GREATER_THAN 200 && LESS_THAN 500]]"))));
        }
    }

    @Test
    public void testSingleton() throws Exception {
        final RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder t1 = metaData.getRecordType("MySimpleRecord");
            final RecordTypeBuilder t2 = metaData.getRecordType("MyOtherRecord");
            t1.setPrimaryKey(concat(recordType(), field("rec_no")));
            t2.setPrimaryKey(recordType());
            metaData.removeIndex(COUNT_INDEX.getName());
            metaData.removeIndex(COUNT_UPDATES_INDEX.getName());
            metaData.addUniversalIndex(new Index("countByRecordType", GroupingKeyExpression.of(empty(), recordType()), IndexTypes.COUNT));
        };

        List<FDBStoredRecord<Message>> recs = saveSomeRecords(hook);

        assertEquals(Tuple.from(1, 123), recs.get(0).getPrimaryKey());
        assertEquals(Tuple.from(1, 456), recs.get(1).getPrimaryKey());
        assertEquals(Tuple.from(2), recs.get(2).getPrimaryKey());

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (FDBStoredRecord<Message> rec : recs) {
                assertEquals(rec, recordStore.loadRecord(rec.getPrimaryKey()));
            }

            TestRecords1Proto.MyOtherRecord.Builder rec2Builder = TestRecords1Proto.MyOtherRecord.newBuilder();
            rec2Builder.setRecNo(-1);
            recs.set(2, recordStore.saveRecord(rec2Builder.build()));

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MyOtherRecord")
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            assertEquals(recs.subList(2, 3), recordStore.executeQuery(query)
                    .map(FDBQueriedRecord::getStoredRecord).asList().join());
            assertThat(plan, scan(bounds(hasTupleString("[IS MyOtherRecord]"))));
        }
    }

    @Test
    public void testDeleteType() throws Exception {
        List<FDBStoredRecord<Message>> recs = saveSomeRecords(BASIC_HOOK);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);

            assertEquals(3, recordStore.getSnapshotRecordCount().join().intValue());
            assertEquals(2, recordStore.getSnapshotRecordCountForRecordType("MySimpleRecord").join().intValue());

            recordStore.deleteRecordsWhere("MySimpleRecord", null);

            assertEquals(1, recordStore.getSnapshotRecordCount().join().intValue());
            assertEquals(0, recordStore.getSnapshotRecordCountForRecordType("MySimpleRecord").join().intValue());

            assertEquals(recs.subList(2, 3), recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join());
            assertEquals(0, recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_3_indexed"),
                    IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).getCount().join().intValue());
        }
    }

    @Test
    public void testDeletePartial() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder t1 = metaData.getRecordType("MySimpleRecord");
            final RecordTypeBuilder t2 = metaData.getRecordType("MyOtherRecord");
            t1.setPrimaryKey(concat(recordType(), field("str_value_indexed"), field("rec_no")));
            metaData.removeIndex(COUNT_INDEX.getName());
            metaData.removeIndex(COUNT_UPDATES_INDEX.getName());
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
            metaData.removeIndex("MySimpleRecord$num_value_3_indexed");
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex(t1, new Index("str_num_3", concatenateFields("str_value_indexed", "num_value_3_indexed")));
            t2.setPrimaryKey(concat(recordType(), field("rec_no")));
        };
        List<FDBStoredRecord<Message>> recs = saveSomeRecords(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.deleteRecordsWhere("MySimpleRecord", Query.field("str_value_indexed").equalsValue("abc"));

            assertEquals(recs.subList(1, 3), recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join());
            assertEquals(Collections.singletonList(Tuple.from("xyz", 2, 1, 456)),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("str_num_3"),
                    IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
        }
    }

    @Test
    public void testBuildIndexIndexThreshold() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();
            context.commit();
        }

        // Lots of records, but not lots of the ones that need to be scanned.
        saveManyRecords(BASIC_HOOK, 10, 500);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);

            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            assertEquals(10, recordStore.getSnapshotRecordCountForRecordType("MySimpleRecord").join().intValue());
            assertEquals(500, recordStore.getSnapshotRecordCountForRecordType("MyOtherRecord").join().intValue());
        }

        RecordMetaDataHook hook = metaData -> {
            BASIC_HOOK.apply(metaData);
            metaData.addIndex("MySimpleRecord", "newIndex", "num_value_2");
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
 
            timer.reset();

            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            assertTrue(recordStore.isIndexReadable("newIndex"));

            assertEquals(10, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals(10, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

            assertEquals(IntStream.range(0, 10).mapToObj(i -> Tuple.from(i, 1, i)).collect(Collectors.toList()),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("newIndex"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
            context.commit();
        }
    }

    @Test
    public void testOnlineIndexBuilder() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();
            context.commit();
        }

        saveManyRecords(BASIC_HOOK, 250, 250);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, BASIC_HOOK);

            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            assertEquals(250, recordStore.getSnapshotRecordCountForRecordType("MySimpleRecord").join().intValue());
            assertEquals(250, recordStore.getSnapshotRecordCountForRecordType("MyOtherRecord").join().intValue());
        }

        RecordMetaDataHook hook = metaData -> {
            BASIC_HOOK.apply(metaData);
            metaData.addIndex("MySimpleRecord", "newIndex", "num_value_2");
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            assertTrue(recordStore.isIndexWriteOnly("newIndex"));

            timer.reset();

            // Build in this transaction.
            try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
                indexBuilder.rebuildIndex(recordStore);
            }
            recordStore.markIndexReadable("newIndex").join();

            assertEquals(250, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals(250, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

            assertEquals(IntStream.range(0, 250).mapToObj(i -> Tuple.from(i, 1, i)).collect(Collectors.toList()),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("newIndex"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            assertTrue(recordStore.isIndexReadable("newIndex"));

            recordStore.clearAndMarkIndexWriteOnly("newIndex").join();
            context.commit();
        }

        timer.reset();

        // Build in multiple transactions.
        try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
            indexBuilder.buildIndex();
        }

        assertThat(timer.getCount(FDBStoreTimer.Events.COMMIT), Matchers.greaterThanOrEqualTo(3));
        assertEquals(250, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(250, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertEquals(IntStream.range(0, 250).mapToObj(i -> Tuple.from(i, 1, i)).collect(Collectors.toList()),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("newIndex"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
        }
    }

    private List<FDBStoredRecord<Message>> saveSomeRecords(@Nonnull RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            final List<FDBStoredRecord<Message>> recs = new ArrayList<>();

            TestRecords1Proto.MySimpleRecord.Builder rec1Builder = TestRecords1Proto.MySimpleRecord.newBuilder();
            rec1Builder.setRecNo(123);
            rec1Builder.setStrValueIndexed("abc");
            rec1Builder.setNumValue3Indexed(1);
            recs.add(recordStore.saveRecord(rec1Builder.build()));

            rec1Builder.setRecNo(456);
            rec1Builder.setStrValueIndexed("xyz");
            rec1Builder.setNumValue3Indexed(2);
            recs.add(recordStore.saveRecord(rec1Builder.build()));

            TestRecords1Proto.MyOtherRecord.Builder rec2Builder = TestRecords1Proto.MyOtherRecord.newBuilder();
            rec2Builder.setRecNo(123);
            rec2Builder.setNumValue3Indexed(2);
            recs.add(recordStore.saveRecord(rec2Builder.build()));

            context.commit();
            for (int i = 0; i < recs.size(); i++) {
                recs.set(i, recs.get(i).withCommittedVersion(context.getVersionStamp()));
            }
            return recs;
        }
    }

    private void saveManyRecords(@Nonnull RecordMetaDataHook hook, int count1, int count2) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < count1; i++) {
                TestRecords1Proto.MySimpleRecord.Builder rec1Builder = TestRecords1Proto.MySimpleRecord.newBuilder();
                rec1Builder.setRecNo(i);
                rec1Builder.setNumValue2(i);
                recordStore.saveRecord(rec1Builder.build());
            }

            for (int i = 0; i < count2; i++) {
                TestRecords1Proto.MyOtherRecord.Builder rec2Builder = TestRecords1Proto.MyOtherRecord.newBuilder();
                rec2Builder.setRecNo(i);
                recordStore.saveRecord(rec2Builder.build());
            }

            context.commit();
        }
    }

}
