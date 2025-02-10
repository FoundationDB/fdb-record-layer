/*
 * FDBRecordStoreCRUDTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TestRecordsUuidProto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Basic CRUD operation tests on {@link FDBRecordStore}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
@Tag("Quicky")
public class FDBRecordStoreCrudTest extends FDBRecordStoreTestBase {

    @Test
    public void writeRead() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .setStrValueIndexed("abc")
                    .setNumValueUnique(123)
                    .build();
            recordStore.saveRecord(rec);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecords1Proto.MySimpleRecord.Builder myrec1 = TestRecords1Proto.MySimpleRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(123, myrec1.getNumValueUnique());
            commit(context);
        }
    }

    @Test
    public void writeCheckExists() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .setStrValueIndexed("abc")
                    .setNumValueUnique(123)
                    .build();
            recordStore.saveRecord(rec);
            assertThat(recordStore.recordExists(Tuple.from(1L)), is(true));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.recordExists(Tuple.from(1L)), is(true));
            assertThat(recordStore.recordExists(Tuple.from(2L)), is(false));
            commit(context);
        }
    }

    @Test
    public void writeCheckExistsConcurrently() throws Exception {
        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            openSimpleRecordStore(context1);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            openSimpleRecordStore(context2);
            assertThat(recordStore.recordExists(Tuple.from(1066L)), is(false));
            TestRecords1Proto.MySimpleRecord rec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1415L)
                    .build();
            recordStore.saveRecord(rec2);

            commit(context1);
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context2::commit);
        }
        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            openSimpleRecordStore(context1);
            recordStore.deleteRecord(Tuple.from(1066L));

            openSimpleRecordStore(context2);
            assertThat(recordStore.recordExists(Tuple.from(1066L), IsolationLevel.SNAPSHOT), is(true));
            TestRecords1Proto.MySimpleRecord rec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1415L)
                    .build();
            recordStore.saveRecord(rec2);

            commit(context1);
            commit(context2);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.recordExists(Tuple.from(1066L)), is(false));
            assertThat(recordStore.recordExists(Tuple.from(1415L)), is(true));
            commit(context);
        }
    }

    @Test
    public void writeByteString() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);

            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 2)).setSecondary(byteString(0, 1, 2)).setName("foo").build());
            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 3)).setSecondary(byteString(0, 1, 3)).setName("foo").build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(byteString(0, 1, 2).toByteArray()));
            assertNotNull(rec1);
            TestRecordsBytesProto.ByteStringRecord.Builder myrec1 = TestRecordsBytesProto.ByteStringRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(byteString(0, 1, 2), myrec1.getPkey());
            assertEquals("foo", myrec1.getName());
            commit(context);
        }
    }

    @Test
    public void writeUuid() {
        UUID uuid1 = UUID.fromString("710730ce-d9fd-417a-bb6e-27bcfefe3d4d");
        UUID uuid2 = UUID.fromString("03b9221a-e61b-4bee-8c47-34e1248ed273");

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, RecordMetaData.build(TestRecordsUuidProto.getDescriptor()));
            recordStore.saveRecord(TestRecordsUuidProto.UuidRecord.newBuilder()
                    .setSecondary(TupleFieldsHelper.toProto(UUID.randomUUID())).setPkey(TupleFieldsHelper.toProto(uuid1)).setName("foo").build());
            recordStore.saveRecord(TestRecordsUuidProto.UuidRecord.newBuilder()
                    .setSecondary(TupleFieldsHelper.toProto(UUID.randomUUID())).setPkey(TupleFieldsHelper.toProto(uuid2)).setName("foo").build());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, RecordMetaData.build(TestRecordsUuidProto.getDescriptor()));
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(uuid1));
            assertNotNull(rec1);
            TestRecordsUuidProto.UuidRecord.Builder myrec1 = TestRecordsUuidProto.UuidRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(uuid1, TupleFieldsHelper.fromProto(myrec1.getPkey()));
            assertEquals("foo", myrec1.getName());
            commit(context);
        }
    }

    @Test
    public void writeNotUnionType() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            assertThrows(MetaDataException.class, () -> {
                TestRecordsWithUnionProto.NotInUnion.Builder recBuilder = TestRecordsWithUnionProto.NotInUnion.newBuilder();
                recBuilder.setNumValueUnique(3);
                recBuilder.setStrValueIndexed("boxes");
                recordStore.saveRecord(recBuilder.build());
                commit(context);
            });
        }
    }

    @Test
    public void readPreloaded() throws Exception {
        byte[] versionstamp;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
            versionstamp = context.getVersionStamp();
            assertNotNull(versionstamp);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context
            context.ensureActive().cancel(); // ensure no more I/O done through the transaction
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(record);
            assertSame(TestRecords1Proto.MySimpleRecord.getDescriptor(), record.getRecordType().getDescriptor());
            assertEquals(1066L, record.getRecord().getField(TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByNumber(TestRecords1Proto.MySimpleRecord.REC_NO_FIELD_NUMBER)));
            assertEquals(FDBRecordVersion.complete(versionstamp, 0), record.getVersion());

            FDBExceptions.FDBStoreException e = assertThrows(FDBExceptions.FDBStoreException.class, context::commit);
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(FDBException.class));
            FDBException fdbE = (FDBException)e.getCause();
            assertEquals(FDBError.TRANSACTION_CANCELLED.code(), fdbE.getCode());
        }
    }

    @Test
    public void readMissingPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            // 4488 does not exist
            recordStore.preloadRecordAsync(Tuple.from(4488L)).get();  // ensure loaded in context
            context.ensureActive().cancel(); // ensure no more I/O done through the transaction

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(4488L));
            assertNull(record);

            FDBExceptions.FDBStoreException e = assertThrows(FDBExceptions.FDBStoreException.class, context::commit);
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(FDBException.class));
            FDBException fdbE = (FDBException)e.getCause();
            assertEquals(FDBError.TRANSACTION_CANCELLED.code(), fdbE.getCode());
        }
    }

    @Test
    public void readYourWritesPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();
            context.ensureActive().cancel(); // ensure no more I/O done through the transaction
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(record);
            assertSame(TestRecords1Proto.MySimpleRecord.getDescriptor(), record.getRecordType().getDescriptor());
            assertEquals(rec.toByteString(), record.getRecord().toByteString());
            assertEquals(FDBRecordVersion.incomplete(0), record.getVersion());

            FDBExceptions.FDBStoreException e = assertThrows(FDBExceptions.FDBStoreException.class, context::commit);
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(FDBException.class));
            FDBException fdbE = (FDBException)e.getCause();
            assertEquals(FDBError.TRANSACTION_CANCELLED.code(), fdbE.getCode());
        }
    }

    @Test
    public void deletePreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context
            recordStore.deleteRecord(Tuple.from(1066L));
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNull(record);
        }
    }

    @Test
    public void deleteAllPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context
            recordStore.deleteAllRecords();
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNull(record);
        }
    }

    @Test
    public void saveOverPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setStrValueIndexed("first_value")
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setStrValueIndexed("second_value")
                    .build();
            recordStore.saveRecord(rec);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(record);
            assertSame(TestRecords1Proto.MySimpleRecord.getDescriptor(), record.getRecordType().getDescriptor());
            assertEquals(1066L, record.getRecord().getField(TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByNumber(TestRecords1Proto.MySimpleRecord.REC_NO_FIELD_NUMBER)));
            assertEquals("second_value", record.getRecord().getField(TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByNumber(TestRecords1Proto.MySimpleRecord.STR_VALUE_INDEXED_FIELD_NUMBER)));
            assertEquals(FDBRecordVersion.incomplete(0), record.getVersion());
        }
    }

    @Test
    public void preloadNonExisting() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            // Make sure pre-loading a non-existing record doesn't fail
            recordStore.preloadRecordAsync(Tuple.from(1L, 2L, 3L, 4L));
        }
    }

    @Test
    public void delete() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recBuilder.setNumValueUnique(123);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteRecord(Tuple.from(1L));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNull(rec1);
            commit(context);
        }
    }

}
