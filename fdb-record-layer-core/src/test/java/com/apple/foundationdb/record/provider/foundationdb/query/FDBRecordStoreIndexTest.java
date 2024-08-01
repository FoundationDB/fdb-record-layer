/*
 * FDBRecordStoreIndexTest.java
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

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedAtLeast;
import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that define new indexes, build them, and scan them directly (without the planner).
 * TODO this probably shouldn't depend on the query test infrastructure.
 */
@Tag(Tags.RequiresFDB)
@Disabled
public class FDBRecordStoreIndexTest extends FDBRecordStoreQueryTestBase {

    private void uncheckedOpenNestedRecordStore(FDBRecordContext context) throws Exception {
        uncheckedOpenNestedRecordStore(context, NO_HOOK);
    }

    private void uncheckedOpenNestedRecordStore(FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        uncheckedOpenRecordStore(context, nestedMetaData(hook));
    }

    /**
     * Verify that building a universal index works.
     */
    @Test
    public void buildNewUniversalIndex() throws Exception {
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenNestedRecordStore(context);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();

            for (int i = 0; i < 10; i++) {
                recordStore.saveRecord(TestRecords4Proto.RestaurantReviewer.newBuilder()
                        .setId(i).setName("r " + i % 10).build());
                recordStore.saveRecord(TestRecords4Proto.RestaurantRecord.newBuilder()
                        .setRestNo(100 + i).setName("r " + i % 10).build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setFilter(Query.field("name").equalsValue("r 8"))
                .build();
        final Function<Message, Object> getIds = r -> {
            final Descriptors.Descriptor descriptor = r.getDescriptorForType();
            return r.getField(Objects.equals(descriptor.getName(), "RestaurantRecord") ?
                              descriptor.findFieldByNumber(TestRecords4Proto.RestaurantRecord.REST_NO_FIELD_NUMBER) :
                              descriptor.findFieldByNumber(TestRecords4Proto.RestaurantReviewer.ID_FIELD_NUMBER));
        };
        assertEquals(Arrays.asList(8L, 108L),
                fetchResultValues(planQuery(query), this::uncheckedOpenNestedRecordStore, getIds));

        // Adds an index for that query.
        RecordMetaDataHook hook = metaData -> metaData.addUniversalIndex(new Index("new_index", "name"));

        Opener openWithNewIndex = context -> uncheckedOpenNestedRecordStore(context, hook);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenNestedRecordStore(context, hook);

            for (int i = 10; i < 20; i++) {
                recordStore.saveRecord(TestRecords4Proto.RestaurantReviewer.newBuilder()
                        .setId(i).setName("r " + i % 10).build());
                recordStore.saveRecord(TestRecords4Proto.RestaurantRecord.newBuilder()
                        .setRestNo(100 + i).setName("r " + i % 10).build());
            }
            commit(context);
        }

        // Only sees new record added after index.
        assertEquals(Arrays.asList(18L, 118L),
                fetchResultValues(planQuery(query), openWithNewIndex, getIds));

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenNestedRecordStore(context, hook);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.NONE).join();
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX_FEW_RECORDS), "should build new index");
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX), "should build new index");
            commit(context);
        }

        // Now see both records from indexed query.
        assertEquals(Arrays.asList(8L, 18L, 108L, 118L),
                fetchResultValues(planQuery(query), openWithNewIndex, getIds, TestHelpers::assertDiscardedNone));
    }

    /**
     * Verify that building an index works for a single-type index.
     */
    @Test
    public void buildNewIndex() throws Exception {
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();

            for (int i = 0; i < 10; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue2(i % 10);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_2").equalsValue(6))
                .build();
        assertEquals(Arrays.asList(6L),
                fetchResultValues(planQuery(query),
                        TestRecords1Proto.MySimpleRecord.REC_NO_FIELD_NUMBER,
                        this::uncheckedOpenSimpleRecordStore,
                        context -> assertDiscardedAtLeast(9, context)));

        // Adds an index for that query.
        RecordMetaDataHook hook = metaData -> metaData.addIndex("MySimpleRecord", "new_index", "num_value_2");

        Opener openWithNewIndex = context -> uncheckedOpenSimpleRecordStore(context, hook);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, hook);

            for (int i = 10; i < 20; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setNumValue2(i % 10);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        // Only sees new record added after index.
        assertEquals(Arrays.asList(16L),
                fetchResultValues(planQuery(query),
                        TestRecords1Proto.MySimpleRecord.REC_NO_FIELD_NUMBER,
                        openWithNewIndex,
                        TestHelpers::assertDiscardedNone));

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, hook);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.NONE).join();
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX_FEW_RECORDS), "should build new index");
            assertEquals(1, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX), "should build new index");
            commit(context);
        }

        // Now see both records from indexed query.
        assertEquals(Arrays.asList(6L, 16L),
                fetchResultValues(planQuery(query),
                        TestRecords1Proto.MySimpleRecord.REC_NO_FIELD_NUMBER,
                        openWithNewIndex,
                        TestHelpers::assertDiscardedNone));

    }

    /**
     * Verify that explicit (i.e. bypassing the planner) index scans work .
     */
    @Test
    public void scanIndexWithValue() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.removeIndex("MySimpleRecord$num_value_unique");
            metaData.addIndex("MySimpleRecord", new Index(
                    "multi_index_value",
                    Key.Expressions.field("num_value_unique"),
                    Key.Expressions.field("num_value_2"),
                    IndexTypes.VALUE,
                    IndexOptions.UNIQUE_OPTIONS));
        };
        complexQuerySetup(hook);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            int i = 0;
            try (RecordCursorIterator<IndexEntry> cursor = recordStore.scanIndex(
                    recordStore.getRecordMetaData().getIndex("multi_index_value"),
                    IndexScanType.BY_VALUE,
                    new TupleRange(Tuple.from(900L), Tuple.from(950L), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE),
                    null, ScanProperties.FORWARD_SCAN).asIterator()) {
                while (cursor.hasNext()) {
                    IndexEntry tuples = cursor.next();
                    Tuple key = tuples.getKey();
                    Tuple value = tuples.getValue();
                    assertEquals(2, key.size());
                    assertEquals(1, value.size());
                    assertTrue(key.getLong(0) >= 900);
                    assertTrue(key.getLong(0) <= 950);
                    assertTrue(value.getLong(0) == (999 - i) % 3);
                    i++;
                }
            }
            assertEquals(50, i);
            assertDiscardedNone(context);
        }
    }

}
