/*
 * FDBTypedRecordStoreTest.java
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

import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.TypedRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBTypedRecordStore}.
 */
@Tag(Tags.RequiresFDB)
public class FDBTypedRecordStoreTest {
    @RegisterExtension
    @Order(0)
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    @Order(1)
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    FDBDatabase fdb;
    KeySpacePath path;
    FDBTypedRecordStore<TestRecords1Proto.MySimpleRecord> recordStore;

    static final FDBTypedRecordStore.Builder<TestRecords1Proto.MySimpleRecord> BUILDER =
            FDBTypedRecordStore.newBuilder(
                    TestRecords1Proto.getDescriptor(),
                    TestRecords1Proto.RecordTypeUnion.getDescriptor().findFieldByNumber(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER),
                    TestRecords1Proto.RecordTypeUnion::newBuilder,
                    TestRecords1Proto.RecordTypeUnion::hasMySimpleRecord,
                    TestRecords1Proto.RecordTypeUnion::getMySimpleRecord,
                    TestRecords1Proto.RecordTypeUnion.Builder::setMySimpleRecord);

    static final RecordSerializer<TestRecords1Proto.MyOtherRecord> OTHER_SERIALIZER =
            new TypedRecordSerializer<>(
                    TestRecords1Proto.RecordTypeUnion.getDescriptor().findFieldByNumber(TestRecords1Proto.RecordTypeUnion._MYOTHERRECORD_FIELD_NUMBER),
                    TestRecords1Proto.RecordTypeUnion::newBuilder,
                    TestRecords1Proto.RecordTypeUnion::hasMyOtherRecord,
                    TestRecords1Proto.RecordTypeUnion::getMyOtherRecord,
                    TestRecords1Proto.RecordTypeUnion.Builder::setMyOtherRecord);

    @BeforeEach
    void setUp() {
        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    private void openTypedRecordStore(FDBRecordContext context) {
        recordStore = BUILDER.copyBuilder()
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();
    }

    @Test
    void writeRead() {
        try (FDBRecordContext context = fdb.openContext()) {
            openTypedRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recBuilder.setNumValueUnique(123);
            recordStore.saveRecord(recBuilder.build());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openTypedRecordStore(context);
            TestRecords1Proto.MySimpleRecord myrec1 = recordStore.loadRecord(Tuple.from(1L)).getRecord();
            assertNotNull(myrec1);
            assertEquals("abc", myrec1.getStrValueIndexed());
            assertEquals(123, myrec1.getNumValueUnique());
            context.commit();
        }
    }

    @Test
    void query() {
        try (FDBRecordContext context = fdb.openContext()) {
            openTypedRecordStore(context);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(i + 1000);
                recordStore.saveRecord(recBuilder.build());
            }
            context.commit();
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();
        try (FDBRecordContext context = fdb.openContext()) {
            openTypedRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<TestRecords1Proto.MySimpleRecord>> cursor = recordStore.executeQuery(query).asIterator()) {
                while (cursor.hasNext()) {
                    TestRecords1Proto.MySimpleRecord myrec = cursor.next().getRecord();
                    assertTrue((myrec.getNumValueUnique() % 2) == 0);
                    i++;
                }
            }
            assertEquals(50, i);
        }
    }

    @Test
    void otherTypes() {
        try (FDBRecordContext context = fdb.openContext()) {
            openTypedRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(2);
            recBuilder.setNumValue3Indexed(456);
            recordStore.saveRecord(recBuilder.build());

            FDBTypedRecordStore<TestRecords1Proto.MyOtherRecord> otherStore = recordStore.getTypedRecordStore(OTHER_SERIALIZER);
            TestRecords1Proto.MyOtherRecord.Builder otherBuilder = TestRecords1Proto.MyOtherRecord.newBuilder();
            otherBuilder.setRecNo(3);
            otherBuilder.setNumValue3Indexed(789);
            otherStore.saveRecord(otherBuilder.build());

            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openTypedRecordStore(context);

            TestRecords1Proto.MySimpleRecord myrec2 = recordStore.loadRecord(Tuple.from(2L)).getRecord();
            assertNotNull(myrec2);
            assertEquals(456, myrec2.getNumValue3Indexed());

            FDBTypedRecordStore<TestRecords1Proto.MyOtherRecord> otherStore = recordStore.getTypedRecordStore(OTHER_SERIALIZER);
            TestRecords1Proto.MyOtherRecord otherrec3 = otherStore.loadRecord(Tuple.from(3L)).getRecord();
            assertEquals(789, otherrec3.getNumValue3Indexed());

            FDBRecordStore untypedStore = recordStore.getUntypedRecordStore();

            assertEquals(myrec2, untypedStore.loadRecord(Tuple.from(2L)).getRecord());
            assertEquals(otherrec3, untypedStore.loadRecord(Tuple.from(3L)).getRecord());

            context.commit();
        }
    }

}
