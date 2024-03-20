/*
 * OneOfTest.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsOneOfProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.MessageBuilderRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test use of {@code oneof} to define the union message.
 */
@Tag(Tags.RequiresFDB)
public class OneOfTest {
    @RegisterExtension
    static final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    private FDBDatabase fdb;
    private KeySpacePath path;

    @BeforeEach
    void setUp() {
        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RAW_DATA);
    }

    private RecordMetaDataBuilder metaData() {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsOneOfProto.getDescriptor());
        final KeyExpression pkey = Key.Expressions.field("rec_no");
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(pkey);
        metaData.getRecordType("MyOtherRecord").setPrimaryKey(pkey);
        metaData.addIndex("MySimpleRecord", "str_value_indexed");
        return metaData;
    }

    @Test
    public void dynamic() {
        final FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData())
                .setKeySpacePath(path);

        try (FDBRecordContext context = fdb.openContext()) {
            final FDBRecordStore recordStore = storeBuilder.setContext(context).create();
            TestRecordsOneOfProto.MySimpleRecord.Builder recBuilder = TestRecordsOneOfProto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recordStore.saveRecord(recBuilder.build());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            final FDBRecordStore recordStore = storeBuilder.setContext(context).open();
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecordsOneOfProto.MySimpleRecord.Builder myrec1 = TestRecordsOneOfProto.MySimpleRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals("abc", myrec1.getStrValueIndexed());
            context.commit();
        }
    }

    @Test
    public void builder() {
        final FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData())
                .setKeySpacePath(path)
                .setSerializer(new MessageBuilderRecordSerializer(TestRecordsOneOfProto.RecordTypeUnion::newBuilder));

        try (FDBRecordContext context = fdb.openContext()) {
            final FDBRecordStore recordStore = storeBuilder.setContext(context).create();
            TestRecordsOneOfProto.MySimpleRecord.Builder recBuilder = TestRecordsOneOfProto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recordStore.saveRecord(recBuilder.build());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            final FDBRecordStore recordStore = storeBuilder.setContext(context).open();
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecordsOneOfProto.MySimpleRecord myrec1 = (TestRecordsOneOfProto.MySimpleRecord)rec1.getRecord();
            assertEquals("abc", myrec1.getStrValueIndexed());
            context.commit();
        }
    }

    @Test
    public void typed() {
        final FDBTypedRecordStore.Builder<TestRecordsOneOfProto.MySimpleRecord> storeBuilder = FDBTypedRecordStore.newBuilder(
                        TestRecordsOneOfProto.RecordTypeUnion.getDescriptor().findFieldByNumber(TestRecordsOneOfProto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER),
                        TestRecordsOneOfProto.RecordTypeUnion::newBuilder,
                        TestRecordsOneOfProto.RecordTypeUnion::hasMySimpleRecord,
                        TestRecordsOneOfProto.RecordTypeUnion::getMySimpleRecord,
                        TestRecordsOneOfProto.RecordTypeUnion.Builder::setMySimpleRecord)
                        .setMetaDataProvider(metaData())
                        .setKeySpacePath(path);

        try (FDBRecordContext context = fdb.openContext()) {
            final FDBTypedRecordStore<TestRecordsOneOfProto.MySimpleRecord> recordStore = storeBuilder.setContext(context).create();
            TestRecordsOneOfProto.MySimpleRecord.Builder recBuilder = TestRecordsOneOfProto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recordStore.saveRecord(recBuilder.build());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            final FDBTypedRecordStore<TestRecordsOneOfProto.MySimpleRecord> recordStore = storeBuilder.setContext(context).open();
            FDBStoredRecord<TestRecordsOneOfProto.MySimpleRecord> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecordsOneOfProto.MySimpleRecord myrec1 = rec1.getRecord();
            assertEquals("abc", myrec1.getStrValueIndexed());
            context.commit();
        }
    }

}
