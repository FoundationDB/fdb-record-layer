/*
 * FDBRecordStoreTest.java
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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestNoIndexesProto;
import com.apple.foundationdb.record.TestNoUnionProto;
import com.apple.foundationdb.record.TestRecords1EvolvedAgainProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TestRecordsImportProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Events.DELETE_INDEX_ENTRY;
import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Events.SAVE_INDEX_ENTRY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Basic tests for {@link FDBRecordStore}.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreTest extends FDBRecordStoreTestBase {
    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStoreTest.class);

    private void openLongRecordStore(FDBRecordContext context) throws Exception {
        createOrOpenRecordStore(context, RecordMetaData.build(TestRecords2Proto.getDescriptor()));
    }

    @SuppressWarnings("deprecation")
    private KeyExpression openRecordWithHeaderPrimaryKey(FDBRecordContext context, boolean useCountIndex) throws Exception {
        final KeyExpression groupExpr = field("header").nest("rec_no");
        openRecordWithHeader(context, metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("rec_no", "path")));
            metaData.addIndex("MyRecord", "MyRecord$str_value", concat(groupExpr, field("str_value")));
            if (useCountIndex) {
                metaData.addUniversalIndex(new Index("MyRecord$count", new GroupingKeyExpression(groupExpr, 0), IndexTypes.COUNT));
            } else {
                metaData.setRecordCountKey(groupExpr);
            }
        });
        return groupExpr;
    }

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
    public void writeUniqueByteString() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);

            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 2)).setSecondary(byteString(0, 1, 2)).setUnique(byteString(0, 2))
                    .setName("foo").build());
            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 5)).setSecondary(byteString(0, 1, 3)).setUnique(byteString(0, 2))
                    .setName("box").build());
            assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
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
    public void asyncUniqueInserts() throws Exception {
        List<TestRecords1Proto.MySimpleRecord> records = new ArrayList<>();
        Random r = new Random(0xdeadc0deL);
        for (int i = 0; i < 100; i++) {
            records.add(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(r.nextLong())
                    .setNumValueUnique(r.nextInt())
                    .build()
            );
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            CompletableFuture<?>[] futures = new CompletableFuture<?>[records.size()];
            for (int i = 0; i < records.size(); i++) {
                futures[i] = recordStore.saveRecordAsync(records.get(i));
            }

            CompletableFuture.allOf(futures).get();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (TestRecords1Proto.MySimpleRecord record : records) {
                assertEquals(record.toString(), recordStore.loadRecord(Tuple.from(record.getRecNo())).getRecord().toString());
            }
        }
    }

    @Test
    public void asyncNotUniqueInserts() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            CompletableFuture<?>[] futures = new CompletableFuture<?>[2];
            futures[0] = recordStore.saveRecordAsync(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValueUnique(42)
                    .build()
            );
            futures[1] = recordStore.saveRecordAsync(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1776L)
                    .setNumValueUnique(42)
                    .build()
            );

            CompletableFuture.allOf(futures).get();
            assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
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
            assertEquals(1025, fdbE.getCode());  // transaction_cancelled
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
            assertEquals(1025, fdbE.getCode());  // transaction_cancelled
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
    public void longRecords() throws Exception {
        Random rand = new Random();
        byte[] bytes;
        bytes = new byte[10000];
        rand.nextBytes(bytes);
        ByteString bytes1 = ByteString.copyFrom(bytes);
        bytes = new byte[250000];
        rand.nextBytes(bytes);
        ByteString bytes2 = ByteString.copyFrom(bytes);
        bytes = new byte[1000];
        rand.nextBytes(bytes);
        ByteString bytes3 = ByteString.copyFrom(bytes);

        try (FDBRecordContext context = openContext()) {
            openLongRecordStore(context);

            TestRecords2Proto.MyLongRecord.Builder recBuilder = TestRecords2Proto.MyLongRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setBytesValue(bytes1);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = TestRecords2Proto.MyLongRecord.newBuilder();
            recBuilder.setRecNo(2);
            recBuilder.setBytesValue(bytes2);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = TestRecords2Proto.MyLongRecord.newBuilder();
            recBuilder.setRecNo(3);
            recBuilder.setBytesValue(bytes3);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openLongRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecords2Proto.MyLongRecord.Builder myrec1 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(bytes1, myrec1.getBytesValue());
            FDBStoredRecord<Message> rec2 = recordStore.loadRecord(Tuple.from(2L));
            assertNotNull(rec2);
            TestRecords2Proto.MyLongRecord.Builder myrec2 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec2.mergeFrom(rec2.getRecord());
            assertEquals(bytes2, myrec2.getBytesValue());
            FDBStoredRecord<Message> rec3 = recordStore.loadRecord(Tuple.from(3L));
            assertNotNull(rec3);
            TestRecords2Proto.MyLongRecord.Builder myrec3 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec3.mergeFrom(rec3.getRecord());
            assertEquals(bytes3, myrec3.getBytesValue());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openLongRecordStore(context);
            RecordCursorIterator<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> rec1 = cursor.next();
            TestRecords2Proto.MyLongRecord.Builder myrec1 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(bytes1, myrec1.getBytesValue());
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> rec2 = cursor.next();
            TestRecords2Proto.MyLongRecord.Builder myrec2 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec2.mergeFrom(rec2.getRecord());
            assertEquals(bytes2, myrec2.getBytesValue());
            FDBStoredRecord<Message> rec3 = cursor.next();
            TestRecords2Proto.MyLongRecord.Builder myrec3 = TestRecords2Proto.MyLongRecord.newBuilder();
            myrec3.mergeFrom(rec3.getRecord());
            assertEquals(bytes3, myrec3.getBytesValue());
            assertFalse(cursor.hasNext());
            commit(context);
        }
    }

    @Test
    public void scanContinuations() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            for (int i = 0; i < 100; i++) {
                recBuilder.setRecNo(i);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (Boolean builtInLimit : new Boolean[] { Boolean.TRUE, Boolean.FALSE }) {
                for (int limit = 1; limit <= 5; limit++) {
                    int i = 0;
                    byte[] continuation = null;
                    do {
                        try (RecordCursorIterator<FDBStoredRecord<Message>> cursor = scanContinuationsCursor(continuation, limit, false, builtInLimit).asIterator()) {
                            while (cursor.hasNext()) {
                                assertEquals(i, cursor.next().getPrimaryKey().getLong(0));
                                i++;
                            }
                            continuation = cursor.getContinuation();
                        }
                    } while (continuation != null);
                    assertEquals(100, i);
                    do {
                        try (RecordCursorIterator<FDBStoredRecord<Message>> cursor = scanContinuationsCursor(continuation, limit, true, builtInLimit).asIterator()) {
                            while (cursor.hasNext()) {
                                i--;
                                assertEquals(i, cursor.next().getPrimaryKey().getLong(0));
                            }
                            continuation = cursor.getContinuation();
                        }
                    } while (continuation != null);
                    assertEquals(0, i);
                }
            }
            commit(context);
        }
    }

    private RecordCursor<FDBStoredRecord<Message>> scanContinuationsCursor(byte[] continuation, int limit, boolean reverse, boolean builtInLimit) {
        if (builtInLimit) {
            return recordStore.scanRecords(continuation, new ScanProperties(ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(limit)
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build(), reverse));
        } else {
            // Using a separate limit cursor will mean calling getContinuation on the inner cursor in the middle of its stream.
            return recordStore.scanRecords(continuation, new ScanProperties(ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(Integer.MAX_VALUE)
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build(), reverse)).limitRowsTo(limit);
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

    @Test
    public void testCountRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            saveSimpleRecord2("a", 1);

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            assertEquals(1, (int)recordStore.countRecords(
                    null, null, EndpointType.TREE_START, EndpointType.TREE_END).join());
            assertEquals(1, (int)recordStore.countRecords(
                    Tuple.from("a"), Tuple.from("a"),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE).join());

            saveSimpleRecord2("b", 1);
            saveSimpleRecord2("c", 1);

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            assertEquals(3, (int)recordStore.countRecords(
                    null, null, EndpointType.TREE_START, EndpointType.TREE_END).join());
            assertEquals(1, (int)recordStore.countRecords(
                    Tuple.from("a"), Tuple.from("a"),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE).join());
            assertEquals(2, (int)recordStore.countRecords(
                    Tuple.from("a"), Tuple.from("c"),
                    EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE).join());
        }

        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            assertEquals(3, (int)recordStore.countRecords(
                    null, null, EndpointType.TREE_START, EndpointType.TREE_END, null,
                    new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(IsolationLevel.SNAPSHOT).build())).join());

            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder()
                    .setStrValueIndexed("xz")
                    .setEtag(1)
                    .build());
            try (FDBRecordContext context2 = openContext()) {
                FDBRecordStore recordStore2 = openNewUnionRecordStore(context2);

                recordStore2.loadRecord(Tuple.from("xz"));
                recordStore2.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder()
                        .setStrValueIndexed("ab")
                        .setEtag(1)
                        .build());
                context2.commit();
            }
            context.commit();
        }

    }

    @Test
    @SuppressWarnings("deprecation")
    public void testUpdateRecordCounts() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("path", "num", "rec_no")));
            builder.setRecordCountKey(field("header").nest(concat(field("path"), field("num"))));
            createOrOpenRecordStore(context, builder.getRecordMetaData());

            saveHeaderRecord(1, "/FirstPath", 0, "johnny");
            saveHeaderRecord(2, "/FirstPath", 0, "apple");
            saveHeaderRecord(3, "/LastPath", 2016, "seed");
            saveHeaderRecord(3, "/LastPath", 2017, "seed");

            saveHeaderRecord(4, "/SecondPath", 0, "cloud");
            saveHeaderRecord(5, "/SecondPath", 0, "apple");
            saveHeaderRecord(6, "/SecondPath", 0, "seed");
            saveHeaderRecord(7, "/SecondPath", 0, "johnny");

            assertEquals(8L, recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get().size());

            assertEquals(8L, recordStore.getSnapshotRecordCount().get().intValue());

            // Delete 2 records
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("/FirstPath")));

            assertEquals(6L, recordStore.getSnapshotRecordCount().get().intValue());

            // Delete 4 records
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("/SecondPath")));

            assertEquals(2L, recordStore.getSnapshotRecordCount().get().intValue());

            // Delete a single record
            recordStore.deleteRecordsWhere(Query.field("header").matches(
                    Query.and(
                            Query.field("path").equalsValue("/LastPath"),
                            Query.field("num").equalsValue(2016))));


            assertEquals(1L, recordStore.getSnapshotRecordCount().get().intValue());
            
            // Delete a single record
            recordStore.deleteRecordsWhere(Query.field("header").matches(
                    Query.and(
                            Query.field("path").equalsValue("/LastPath"),
                            Query.field("num").equalsValue(2017))));

            assertEquals(0L, recordStore.getSnapshotRecordCount().get().intValue());

            context.commit();
        }
    }

    @Test
    public void testDeleteWhereCountIndex() throws Exception {
        testDeleteWhere(true);
    }

    @Test
    public void testDeleteWhere() throws Exception {
        testDeleteWhere(false);
    }

    private void testDeleteWhere(boolean useCountIndex) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeaderPrimaryKey(context, useCountIndex);

            saveHeaderRecord(1, "a", 0, "lynx");
            saveHeaderRecord(1, "b", 1, "bobcat");
            saveHeaderRecord(1, "c", 2, "panther");

            saveHeaderRecord(2, "a", 3, "jaguar");
            saveHeaderRecord(2, "b", 4, "leopard");
            saveHeaderRecord(2, "c", 5, "lion");
            saveHeaderRecord(2, "d", 6, "tiger");
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final KeyExpression groupExpr = openRecordWithHeaderPrimaryKey(context, useCountIndex);

            assertEquals(3, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("rec_no").equalsValue(1)));
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            final KeyExpression groupExpr = openRecordWithHeaderPrimaryKey(context, useCountIndex);

            assertEquals(0, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            int expectedNum = 3;
            for (FDBStoredRecord<Message> storedRecord : recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join()) {
                TestRecordsWithHeaderProto.MyRecord record = parseMyRecord(storedRecord.getRecord());
                assertEquals(2, record.getHeader().getRecNo());
                assertEquals(expectedNum++, record.getHeader().getNum());
            }
            assertEquals(7, expectedNum);
            expectedNum = 3;
            for (FDBIndexedRecord<Message> indexedRecord : recordStore.scanIndexRecords("MyRecord$str_value").asList().join()) {
                TestRecordsWithHeaderProto.MyRecord record = parseMyRecord(indexedRecord.getRecord());
                assertEquals(2, record.getHeader().getRecNo());
                assertEquals(expectedNum++, record.getHeader().getNum());
            }
            assertEquals(7, expectedNum);
            context.commit();
        }
    }

    @Test
    public void testDeleteWhereGroupedCount() throws Exception {
        KeyExpression groupExpr = concat(
                field("header").nest(field("rec_no")),
                field("header").nest(field("path")));
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(concat(
                            field("header").nest(field("rec_no")),
                            field("header").nest(field("path")),
                            field("header").nest(field("num"))));

            metaData.addUniversalIndex(new Index("MyRecord$groupedCount", new GroupingKeyExpression(groupExpr, 0), IndexTypes.COUNT));
            metaData.addUniversalIndex(new Index("MyRecord$groupedUpdateCount", new GroupingKeyExpression(groupExpr, 0), IndexTypes.COUNT_UPDATES));
        };
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            saveHeaderRecord(1, "a", 0, "lynx");
            saveHeaderRecord(1, "a", 1, "bobcat");
            saveHeaderRecord(1, "b", 2, "panther");

            saveHeaderRecord(2, "a", 3, "jaguar");
            saveHeaderRecord(2, "b", 4, "leopard");
            saveHeaderRecord(2, "c", 5, "lion");
            saveHeaderRecord(2, "d", 6, "tiger");
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            // Number of records where first component of primary key is 1
            assertEquals(3, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            // Number of updates to such records
            assertEquals(3, recordStore.getSnapshotRecordUpdateCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            recordStore.deleteRecordsWhere(Query.and(
                    Query.field("header").matches(Query.field("rec_no").equalsValue(1)),
                    Query.field("header").matches(Query.field("path").equalsValue("a"))));

            assertEquals(1, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            // Deleting by group prefix resets the update counter for the group(s)
            assertEquals(1, recordStore.getSnapshotRecordUpdateCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            context.commit();
        }
    }    

    @Test
    public void testDeleteWhereMissingPrimaryKey() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeaderPrimaryKey(context, false);
            assertThrows(Query.InvalidExpressionException.class, () ->
                    recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue(1))));
        }
    }

    @Test
    public void testDeleteWhereMissingIndex() throws Exception {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord")
                .setPrimaryKey(field("header").nest(concatenateFields("rec_no", "path")));
            builder.addIndex("MyRecord", "MyRecord$str_value", concat(field("header").nest("path"),
                    field("str_value")));
            RecordMetaData metaData = builder.getRecordMetaData();
            createOrOpenRecordStore(context, metaData);
            assertThrows(Query.InvalidExpressionException.class, () ->
                    recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("rec_no").equalsValue(1))));
        }
    }

    @Test
    public void testOverlappingPrimaryKey() throws Exception {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord")
                .setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
            builder.addIndex("MyRecord", "MyRecord$path_str", concat(field("header").nest("path"),
                    field("str_value")));
            RecordMetaData metaData = builder.getRecordMetaData();
            createOrOpenRecordStore(context, metaData);

            TestRecordsWithHeaderProto.MyRecord.Builder recBuilder = TestRecordsWithHeaderProto.MyRecord.newBuilder();
            TestRecordsWithHeaderProto.HeaderRecord.Builder headerBuilder = recBuilder.getHeaderBuilder();
            headerBuilder.setPath("aaa");
            headerBuilder.setRecNo(1);
            recBuilder.setStrValue("hello");
            recordStore.saveRecord(recBuilder.build());

            headerBuilder.setPath("aaa");
            headerBuilder.setRecNo(2);
            recBuilder.setStrValue("goodbye");
            recordStore.saveRecord(recBuilder.build());

            headerBuilder.setPath("zzz");
            headerBuilder.setRecNo(3);
            recBuilder.setStrValue("end");
            recordStore.saveRecord(recBuilder.build());

            List<List<Object>> rows = new ArrayList<>();
            Index index = metaData.getIndex("MyRecord$path_str");
            ScanComparisons comparisons = ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "aaa"));
            TupleRange range = comparisons.toTupleRange();
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range,
                                                                            null, ScanProperties.FORWARD_SCAN)) {
                cursor.forEach(row -> rows.add(row.getKey().getItems())).join();
            }
            assertEquals(Arrays.asList(Arrays.asList("aaa", "goodbye", 2L),
                                       Arrays.asList("aaa", "hello", 1L)),
                         rows);
        }
    }

    @Test
    public void testIndexKeyTooLarge() throws Exception {
        assertThrows(FDBExceptions.FDBStoreKeySizeException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);

                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(1);
                recBuilder.setStrValueIndexed(Strings.repeat("x", 10000));
                recordStore.saveRecord(recBuilder.build());
                fail("exception should have been thrown before commit");
                commit(context);
            }
        });
    }

    @Test
    public void testIndexValueTooLarge() throws Exception {
        assertThrows(FDBExceptions.FDBStoreValueSizeException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, md -> {
                    md.setSplitLongRecords(true);
                    md.removeIndex("MySimpleRecord$str_value_indexed");
                    md.addIndex("MySimpleRecord", new Index(
                            "valueIndex",
                            field("num_value_2"),
                            field("str_value_indexed"),
                            IndexTypes.VALUE,
                            Collections.emptyMap()));
                });

                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(1);
                recBuilder.setStrValueIndexed(Strings.repeat("x", 100000));
                recordStore.saveRecord(recBuilder.build());
                fail("exception should have been thrown before commit");
                commit(context);
            }
        });
    }

    @Test
    public void testStoredRecordSizeIsPlausible() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();

            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed(Strings.repeat("x", 10));
            FDBStoredRecord<Message> rec1 = recordStore.saveRecord(recBuilder.build());
            assertEquals(1, rec1.getKeyCount(), "small record should only need one key-value pair");
            assertThat("small record should only need few key bytes", rec1.getKeySize(), allOf(greaterThan(5), lessThan(100)));
            assertThat("small record should only need few value bytes", rec1.getValueSize(), allOf(greaterThan(10), lessThan(100)));

            recBuilder.setRecNo(2);
            recBuilder.setStrValueIndexed(Strings.repeat("x", 100000));
            FDBStoredRecord<Message> rec2 = recordStore.saveRecord(recBuilder.build());
            assertThat("large record should only need several key-value pairs", rec2.getKeyCount(), allOf(greaterThan(1), lessThan(10)));
            assertThat("large record should only need few key bytes", rec2.getKeySize(), allOf(greaterThan(10), lessThan(100)));
            assertThat("large record should only need many value bytes", rec2.getValueSize(), allOf(greaterThan(100000), lessThan(101000)));

            FDBStoredRecord<Message> rec1x = recordStore.loadRecord(rec1.getPrimaryKey());
            assertEquals(rec1.getKeyCount(), rec1x.getKeyCount(), "small record loaded key count should match");
            assertEquals(rec1.getKeySize(), rec1x.getKeySize(), "small record loaded key size should match");
            assertEquals(rec1.getValueSize(), rec1x.getValueSize(), "small record loaded value size should match");

            FDBStoredRecord<Message> rec2x = recordStore.loadRecord(rec2.getPrimaryKey());
            assertEquals(rec2.getKeyCount(), rec2x.getKeyCount(), "large record loaded key count should match");
            assertEquals(rec2.getKeySize(), rec2x.getKeySize(), "large record loaded key size should match");
            assertEquals(rec2.getValueSize(), rec2x.getValueSize(), "large record loaded value size should match");

            commit(context);
        }
    }

    @Test
    public void testStoredRecordSizeIsConsistent() throws Exception {
        final RecordMetaDataHook hook = md -> {
            md.setSplitLongRecords(true);
        };

        Set<FDBStoredRecord<Message>> saved;
        try (FDBRecordContext context = openContext()) {
            List<FDBStoredRecord<Message>> saving = new ArrayList<>();
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 10; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recBuilder.setStrValueIndexed(Strings.repeat("z", i * 10));
                saving.add(recordStore.saveRecord(recBuilder.build()));
            }
            commit(context);

            final byte[] commitVersionstamp = context.getVersionStamp();
            assertNotNull(commitVersionstamp);
            saved = saving.stream().map(rec -> rec.withCommittedVersion(context.getVersionStamp())).collect(Collectors.toSet());
        }

        Set<FDBStoredRecord<Message>> scanned = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).forEach(scanned::add).join();
            commit(context);
        }

        Set<FDBStoredRecord<Message>> indexed = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed").forEach(i -> indexed.add(i.getStoredRecord())).join();
            commit(context);
        }

        assertEquals(saved, scanned);
        assertEquals(saved, indexed);

    }

    @Test
    public void testSplitContinuation() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            commit(context);
        }

        final String bigValue = Strings.repeat("X", SplitHelper.SPLIT_RECORD_SIZE + 10);
        final String smallValue = Strings.repeat("Y", 5);

        final List<FDBStoredRecord<Message>> createdRecords = new ArrayList<>();
        createdRecords.add(saveAndSplitSimpleRecord(1L, smallValue, 1));
        createdRecords.add(saveAndSplitSimpleRecord(2L, smallValue, 2));
        createdRecords.add(saveAndSplitSimpleRecord(3L, bigValue, 3));
        createdRecords.add(saveAndSplitSimpleRecord(4L, smallValue, 4));
        createdRecords.add(saveAndSplitSimpleRecord(5L, bigValue, 5));
        createdRecords.add(saveAndSplitSimpleRecord(6L, bigValue, 6));
        createdRecords.add(saveAndSplitSimpleRecord(7L, smallValue, 7));
        createdRecords.add(saveAndSplitSimpleRecord(8L, smallValue, 8));
        createdRecords.add(saveAndSplitSimpleRecord(9L, smallValue, 9));

        // Scan one record at a time using continuations
        final List<FDBStoredRecord<Message>> scannedRecords = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            RecordCursorIterator<FDBStoredRecord<Message>> messageCursor = recordStore.scanRecords(null,
                    new ScanProperties(ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build()))
                    .asIterator();
            while (messageCursor.hasNext()) {
                scannedRecords.add(messageCursor.next());
                messageCursor = recordStore.scanRecords(messageCursor.getContinuation(), new ScanProperties(
                        ExecuteProperties.newBuilder()
                                .setReturnedRowLimit(1)
                                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                        .build())).asIterator();
            }
            commit(context);
        }
        assertEquals(createdRecords, scannedRecords);
    }

    @Test
    public void testSaveRecordWithDifferentSplits() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            commit(context);
        }
        
        final long recno = 1;
        final String shortString = Strings.repeat("x", 10);
        final String mediumString = Strings.repeat("y", SplitHelper.SPLIT_RECORD_SIZE + 10);
        final String longString = Strings.repeat("y", SplitHelper.SPLIT_RECORD_SIZE * 2 + 10);

        // unsplit
        saveAndCheckSplitSimpleRecord(recno, shortString, 123);
        // ... -> split
        saveAndCheckSplitSimpleRecord(recno, mediumString, 456);
        // ... -> longer split
        saveAndCheckSplitSimpleRecord(recno, longString, 789);
        // ... -> shorter split
        saveAndCheckSplitSimpleRecord(recno, mediumString, 456);
        // ... -> unsplit
        saveAndCheckSplitSimpleRecord(recno, shortString, 123);
        // ... -> deleted
        deleteAndCheckSplitSimpleRecord(recno);

        // long split
        saveAndCheckSplitSimpleRecord(recno, longString, 789);
        // ... -> unsplit
        saveAndCheckSplitSimpleRecord(recno, shortString, 123);
        // ... -> long split
        saveAndCheckSplitSimpleRecord(recno, longString, 789);
        // ... -> deleted
        deleteAndCheckSplitSimpleRecord(recno);

        // Check corruption with no split
        saveAndCheckCorruptSplitSimpleRecord(recno, shortString, 1066);
        //    "       "     with a short split
        saveAndCheckCorruptSplitSimpleRecord(recno, mediumString, 1415);
        //    "       "     with a long split
        saveAndCheckCorruptSplitSimpleRecord(recno, longString, 1066);
        // and delete
        deleteAndCheckSplitSimpleRecord(recno);
    }

    private void saveAndCheckSplitSimpleRecord(long recno, String strValue, int numValue) throws Exception {
        FDBStoredRecord<Message> savedRecord = saveAndSplitSimpleRecord(recno, strValue, numValue);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            FDBStoredRecord<Message> loadedRecord = recordStore.loadRecord(Tuple.from(recno));
            assertEquals(savedRecord, loadedRecord);

            List<FDBStoredRecord<Message>> scannedRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(Collections.singletonList(savedRecord), scannedRecords);

            List<FDBStoredRecord<Message>> scanOneRecord = recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())).asList().join();
            assertEquals(Collections.singletonList(savedRecord), scanOneRecord);

            List<FDBStoredRecord<Message>> reversedScannedRecords = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().join();
            assertEquals(Collections.singletonList(savedRecord), reversedScannedRecords);

            List<FDBStoredRecord<Message>> reversedScannedOneRecord = recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build(), true)).asList().join();
            assertEquals(Collections.singletonList(savedRecord), reversedScannedOneRecord);

            commit(context);
        }
    }

    private void runAndCheckSplitException(TestHelpers.DangerousRunnable runnable, String msgPrefix, String failureMessage) {
        try {
            runnable.run();
            fail(failureMessage);
        } catch (Exception e) {
            RuntimeException runE = FDBExceptions.wrapException(e);
            if (runE instanceof RecordCoreException) {
                assertThat(runE.getMessage(), startsWith(msgPrefix));
            } else {
                throw runE;
            }
        }
    }

    private void saveAndCheckCorruptSplitSimpleRecord(long recno, String strValue, int numValue) throws Exception {
        FDBStoredRecord<Message> savedRecord = saveAndSplitSimpleRecord(recno, strValue, numValue);
        if (!savedRecord.isSplit()) {
            return;
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            // Corrupt the data by removing the first key.
            byte[] key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, recno, SplitHelper.START_SPLIT_RECORD));
            context.ensureActive().clear(key);

            runAndCheckSplitException(() -> recordStore.loadRecord(Tuple.from(recno)),
                                      "Found split record without start", "Loaded split record missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get(),
                                      "Found split record without start", "Scanned split records missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())).asList().get(),
                                      "Found split record without start", "Scanned one split record missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().get(),
                                      "Found split record without start", "Scanned split records in reverse missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build(), true)).asList().get(),
                                      "Found split record without start", "Scanned one split record in reverse missing start key");

            // Redo the scans with non-corrupt elements on either side of the corrupted split record.
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recno - 1).build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recno + 1).build());


            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get(),
                                      "Found split record without start", "Scanned split records missing start key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().get(),
                                      "Found split record without start", "Scanned split records in reverse missing start key");

            // DO NOT COMMIT
        }
        if (savedRecord.getKeyCount() <= 2) {
            return;
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            // Corrupt the data by removing a middle key.
            byte[] key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, recno, SplitHelper.START_SPLIT_RECORD + 1));
            context.ensureActive().clear(key);

            runAndCheckSplitException(() -> recordStore.loadRecord(Tuple.from(recno)),
                                      "Split record segments out of order", "Loaded split record missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get(),
                                      "Split record segments out of order", "Scanned split records missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build())).asList().get(),
                                      "Split record segments out of order", "Scanned one split record missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().get(),
                                      "Split record segments out of order", "Scanned split records in reverse missing middle key");
            runAndCheckSplitException(() -> recordStore.scanRecords(null, new ScanProperties(
                    ExecuteProperties.newBuilder()
                            .setReturnedRowLimit(1)
                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                            .build(), true)).asList().get(),
                                      "Split record segments out of order", "Scanned one split record in reverse missing middle key");

            // DO NOT COMMIT
        }
    }

    private void deleteAndCheckSplitSimpleRecord(long recno) throws Exception {
        FDBStoredRecord<Message> savedRecord;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            recordStore.deleteRecord(Tuple.from(recno));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            FDBStoredRecord<Message> loadedRecord = recordStore.loadRecord(Tuple.from(recno));
            assertNull(loadedRecord);

            List<FDBStoredRecord<Message>> scannedRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(Collections.emptyList(), scannedRecords);

            List<FDBStoredRecord<Message>> reverseScannedRecords = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().join();
            assertEquals(Collections.emptyList(), reverseScannedRecords);

            commit(context);
        }
    }

    @Test
    public void testStoreTimersIncrement() throws Exception {
        final int recordCount = 10;
        final int recordKeyCount = 2 * recordCount;
        final int minRecordKeyBytes = 8;
        final int minTotalRecordKeyBytes = minRecordKeyBytes * recordKeyCount;
        final int minRecordValueBytes = 15;
        final int minTotalRecordValueBytes = recordCount * minRecordValueBytes;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < recordCount; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recordStore.saveRecord(recBuilder.build());
            }

            assertThat(timer.getCount(FDBStoreTimer.Events.SAVE_RECORD), equalTo(recordCount));
            assertThat(timer.getCount(FDBStoreTimer.Events.SAVE_INDEX_ENTRY), equalTo(recordCount * 3));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY), equalTo(recordKeyCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY), equalTo(recordCount * 3));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES), greaterThan(minTotalRecordKeyBytes * 3));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES), equalTo(0));
            assertThat(timer.getCount(RecordSerializer.Events.SERIALIZE_PROTOBUF_RECORD), equalTo(recordCount));


            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            
            recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().join();

            assertThat(timer.getCount(FDBStoreTimer.Events.SCAN_RECORDS), equalTo( recordCount + 1));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY), equalTo(recordKeyCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));
            assertThat(timer.getCount(RecordSerializer.Events.DESERIALIZE_PROTOBUF_RECORD), equalTo(recordCount));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed").getCount().join();

            assertThat(timer.getCount(FDBStoreTimer.Events.SCAN_INDEX_KEYS), equalTo(recordCount + 1));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY), equalTo(recordKeyCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_INDEX_KEY), equalTo(recordCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES), equalTo(0));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (int i = 0; i < recordCount; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recBuilder.setStrValueIndexed("abcxyz");
                recordStore.saveRecord(recBuilder.build());
            }
            assertThat(timer.getCount(FDBStoreTimer.Counts.REPLACE_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < recordCount; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recordStore.saveRecord(recBuilder.build());
            }
            for (int i = 0; i < recordCount; i++) {
                recordStore.deleteRecord(Tuple.from(i + 1));
            }
            assertThat(timer.getCount(FDBStoreTimer.Events.SAVE_RECORD), equalTo(recordCount));
            assertEquals(timer.getCount(FDBStoreTimer.Events.SAVE_RECORD), timer.getCount(FDBStoreTimer.Events.DELETE_RECORD));
            assertEquals(timer.getCount(FDBStoreTimer.Events.SAVE_INDEX_ENTRY), timer.getCount(FDBStoreTimer.Events.DELETE_INDEX_ENTRY));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY), timer.getCount(FDBStoreTimer.Counts.DELETE_RECORD_KEY));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY_BYTES), timer.getCount(FDBStoreTimer.Counts.DELETE_RECORD_KEY_BYTES));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_VALUE_BYTES), timer.getCount(FDBStoreTimer.Counts.DELETE_RECORD_VALUE_BYTES));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY), timer.getCount(FDBStoreTimer.Counts.DELETE_INDEX_KEY));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES), timer.getCount(FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES));

            commit(context);
        }
    }

    @Test
    public void updateUnchanged() throws Exception {
        TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
        recBuilder.setRecNo(1);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            recBuilder.setStrValueIndexed("abc");
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recBuilder.setStrValueIndexed("xyz");
            recordStore.saveRecord(recBuilder.build());
            assertEquals(2, timer.getCount(DELETE_INDEX_ENTRY) + timer.getCount(SAVE_INDEX_ENTRY), "should update one index");
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(0, timer.getCount(DELETE_INDEX_ENTRY) + timer.getCount(SAVE_INDEX_ENTRY), "should not update any index");
            commit(context);
        }
    }

    @SuppressWarnings("deprecation")
    private static RecordMetaDataHook countKeyHook(KeyExpression key, boolean useIndex, int indexVersion) {
        if (useIndex) {
            return md -> {
                md.removeIndex(COUNT_INDEX.getName());
                Index index = new Index("record_count", new GroupingKeyExpression(key, 0), IndexTypes.COUNT);
                index.setLastModifiedVersion(indexVersion);
                md.addUniversalIndex(index);
            };
        } else {
            return md -> md.setRecordCountKey(key);
        }
    }

    private static RecordMetaDataHook countUpdatesKeyHook(KeyExpression key, int indexVersion) {
        return md -> {
            md.removeIndex(COUNT_UPDATES_INDEX.getName());
            Index index = new Index("record_update_count", new GroupingKeyExpression(key, 0), IndexTypes.COUNT_UPDATES);
            index.setLastModifiedVersion(indexVersion);
            md.addUniversalIndex(index);
        };
    }

    @Test
    public void countRecordsIndex() throws Exception {
        countRecords(true);
    }

    @Test
    public void countRecords() throws Exception {
        countRecords(false);
    }

    private void countRecords(boolean useIndex) throws Exception {
        final RecordMetaDataHook hook = countKeyHook(EmptyKeyExpression.EMPTY, useIndex, 0);
        HashMap<Integer, Integer> expectedCountBuckets = new HashMap<>();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(0, recordStore.getSnapshotRecordCount().join().longValue());

            for (int i = 0; i < 100; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(100, recordStore.getSnapshotRecordCount().join().longValue());
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 5; i++) {
                int recNo = i * 10;
                recordStore.deleteRecord(Tuple.from(recNo));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(95, recordStore.getSnapshotRecordCount().join().longValue());
            commit(context);
        }
    }

    private TestRecords1Proto.MySimpleRecord makeRecord(long recordNo, int numValue2, int numValue3Indexed) {
        TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
        recBuilder.setRecNo(recordNo);
        recBuilder.setNumValue2(numValue2);
        recBuilder.setNumValue3Indexed(numValue3Indexed);
        return recBuilder.build();
    }

    private void checkRecordUpdateCounts(HashMap<Integer, Integer> expectedCounts,
                                         RecordMetaDataHook hook,
                                         KeyExpression key) throws Exception {
        int sum = expectedCounts.values().stream().mapToInt(Number::intValue).sum();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertEquals(sum, recordStore.getSnapshotRecordUpdateCount().join().longValue());

            expectedCounts.forEach((bucketNum, expected) ->
                    assertEquals(expectedCounts.get(bucketNum).longValue(),
                            recordStore.getSnapshotRecordUpdateCount(key, Key.Evaluated.scalar(bucketNum)).join().longValue()));
        }
    }

    @Test
    public void countRecordUpdates() throws Exception {
        final KeyExpression key = field("num_value_3_indexed");
        final RecordMetaDataHook hook = countUpdatesKeyHook(key, 0);
        HashMap<Integer, Integer> expectedCountBuckets = new HashMap<>();

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(0, recordStore.getSnapshotRecordUpdateCount().join().longValue());

            // Create 100 records
            for (int i = 0; i < 100; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
                expectedCountBuckets.put(numBucket, expectedCountBuckets.getOrDefault(numBucket, 0) + 1);
            }
            commit(context);
        }

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Delete 5 records, this shouldn't change the counts
            for (int i = 95; i < 100; i++) {
                recordStore.deleteRecord(Tuple.from(i));
            }
            commit(context);
        }

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Update 10 records
            for (int i = 0; i < 10; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
                expectedCountBuckets.put(numBucket, expectedCountBuckets.getOrDefault(numBucket, 0) + 1);
            }
            commit(context);
        }

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            // Update and create (or re-create) some records
            for (int i = 90; i < 110; i++) {
                int numBucket = i % 5;
                recordStore.saveRecord(makeRecord(i, 0, numBucket));
                expectedCountBuckets.put(numBucket, expectedCountBuckets.getOrDefault(numBucket, 0) + 1);
            }
            // Delete 5 records
            for (int i = 20; i < 25; i++) {
                recordStore.deleteRecord(Tuple.from(i));
            }
            commit(context);
        }

        checkRecordUpdateCounts(expectedCountBuckets, hook, key);
    }

    @Test
    public void countRecordsKeyedIndex() throws Exception {
        countRecordsKeyed(true);
    }

    @Test
    public void countRecordsKeyed() throws Exception {
        countRecordsKeyed(false);
    }

    private void countRecordsKeyed(boolean useIndex) throws Exception {
        final KeyExpression key = field("num_value_3_indexed");
        final RecordMetaDataHook hook = countKeyHook(key, useIndex, 0);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 100; i++) {
                recordStore.saveRecord(makeRecord(i, 0, i % 5));
            }
            commit(context);
        }
        
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertEquals(100, recordStore.getSnapshotRecordCount().join().longValue());
            assertEquals(20, recordStore.getSnapshotRecordCount(key, Key.Evaluated.scalar(1)).join().longValue());
            commit(context);
        }
    }

    @Test
    public void recountAndClearRecordsIndex() throws Exception {
        recountAndClearRecords(true);
    }

    @Test
    public void recountAndClearRecords() throws Exception {
        recountAndClearRecords(false);
    }

    // Get a new metadata version every time we change the count key definition.
    static class CountMetaDataHook implements RecordMetaDataHook {
        int metaDataVersion = 100;
        RecordMetaDataHook baseHook = null;

        @Override
        public void apply(RecordMetaDataBuilder metaData) {
            if (baseHook != null) {
                baseHook.apply(metaData);
            }
            metaData.setVersion(metaDataVersion);
        }
    }

    private void recountAndClearRecords(boolean useIndex) throws Exception {
        final CountMetaDataHook countMetaDataHook = new CountMetaDataHook();
        countMetaDataHook.baseHook = metaData -> metaData.removeIndex(COUNT_INDEX.getName());

        final int startingPoint = 7890;
        final int value1 = 12345;
        final int value2 = 54321;
        final int value3 = 24567;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore.deleteAllRecords();
            // Simulate the state the store would be in if this were done before counting was added.
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.INFO_ADDED_FORMAT_VERSION).build();
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).join();

            for (int i = 0; i < 90; i++) {
                recordStore.saveRecord(makeRecord(i + startingPoint, value1, i % 5));
            }
            commit(context);
        }

        KeyExpression key3 = field("num_value_3_indexed");
        countMetaDataHook.metaDataVersion++;
        countMetaDataHook.baseHook = countKeyHook(key3, useIndex, countMetaDataHook.metaDataVersion);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.RECORD_COUNT_ADDED_FORMAT_VERSION).build();

            for (int i = 90; i < 100; i++) {
                recordStore.saveRecord(makeRecord(i + startingPoint, value2, i % 5));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.RECORD_COUNT_ADDED_FORMAT_VERSION).build();

            assertEquals(10, recordStore.getSnapshotRecordCount().join().longValue(), "should only see new records");
            commit(context);
        }

        // Need to allow immediate rebuild of new count index.
        final FDBRecordStoreBase.UserVersionChecker alwaysEnabled = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(Integer.valueOf(1));
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.READABLE;
            }
        };

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore.checkVersion(alwaysEnabled, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join(); // Index is rebuilt here automatically in useIndex case

            assertEquals(100, recordStore.getSnapshotRecordCount().join().longValue(), "should see all records");
            assertEquals(20, recordStore.getSnapshotRecordCount(key3, Key.Evaluated.scalar(2)).join().longValue());
            commit(context);
        }

        KeyExpression key2 = field("num_value_2");
        countMetaDataHook.metaDataVersion++;
        countMetaDataHook.baseHook = countKeyHook(key2, useIndex, countMetaDataHook.metaDataVersion);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).join();

            if (useIndex) {
                // Need to manually rebuild index in this case.
                Index index = recordStore.getRecordMetaData().getIndex("record_count");
                recordStore.rebuildIndex(index).get();
                assertThat(recordStore.isIndexReadable(index), is(true));
            }

            assertEquals(100, recordStore.getSnapshotRecordCount().join().longValue(), "should see all records");

            for (int i = 0; i < 32; i++) {
                recordStore.saveRecord(makeRecord(i + startingPoint + 1000, value3, 0));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            assertEquals(90, recordStore.getSnapshotRecordCount(key2, Key.Evaluated.scalar(value1)).join().longValue());
            assertEquals(10, recordStore.getSnapshotRecordCount(key2, Key.Evaluated.scalar(value2)).join().longValue());
            assertEquals(32, recordStore.getSnapshotRecordCount(key2, Key.Evaluated.scalar(value3)).join().longValue());
        }

        KeyExpression pkey = field("rec_no");
        countMetaDataHook.metaDataVersion++;
        countMetaDataHook.baseHook = countKeyHook(pkey, useIndex, countMetaDataHook.metaDataVersion);

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, countMetaDataHook);
            recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.NONE).join();

            if (useIndex) {
                // Need to manually rebuild index in this case.
                Index index = recordStore.getRecordMetaData().getIndex("record_count");
                recordStore.rebuildIndex(index).get();
                assertThat(recordStore.isIndexReadable(index), is(true));
            }

            assertEquals(132, recordStore.getSnapshotRecordCount().join().longValue());
            for (int i = 0; i < 100; i++) {
                assertEquals(1, recordStore.getSnapshotRecordCount(pkey, Key.Evaluated.scalar(i + startingPoint)).join().longValue(), "Incorrect when i is " + i);
            }
        }
    }

    @Test
    public void addCountIndex() throws Exception {
        RecordMetaDataHook removeCountHook = metaData -> {
            metaData.removeIndex(COUNT_INDEX.getName());
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, removeCountHook);

            for (int i = 0; i < 10; i++) {
                recordStore.saveRecord(makeRecord(i, 1066, i % 5));
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, removeCountHook);
            recordStore.getSnapshotRecordCount().get();
            fail("evaluated count without index or key");
        } catch (RecordCoreException e) {
            assertThat(e.getMessage(), containsString("requires appropriate index"));
        }

        RecordMetaDataHook hook = countKeyHook(Key.Expressions.field("num_value_3_indexed"), true, 10);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            Index countIndex = recordStore.getRecordMetaData().getIndex("record_count");
            assertThat(recordStore.getRecordStoreState().isWriteOnly(countIndex), is(true));
            recordStore.getSnapshotRecordCount().get();
            fail("evaluated count with write-only index");
        } catch (RecordCoreException e) {
            assertThat(e.getMessage(), containsString("requires appropriate index"));
        }

        // Build the index
        try (OnlineIndexer onlineIndexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "record_count")) {
            onlineIndexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            Index countIndex = recordStore.getRecordMetaData().getIndex("record_count");
            assertThat(recordStore.getRecordStoreState().isWriteOnly(countIndex), is(false));
            assertEquals(10L, recordStore.getSnapshotRecordCount().get().longValue());
        }
    }

    @Test
    public void typeChange() throws Exception {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, RecordMetaData.build(TestRecords7Proto.getDescriptor()));

            TestRecords7Proto.MyRecord1.Builder rec1Builder = TestRecords7Proto.MyRecord1.newBuilder();
            rec1Builder.setRecNo(1);
            rec1Builder.setStrValue("one");
            recordStore.saveRecord(rec1Builder.build());

            assertEquals(1L, recordStore.scanIndexRecords("MyRecord1$str_value").getCount().get().longValue(), "should have one record in index");

            TestRecords7Proto.MyRecord2.Builder rec2Builder = TestRecords7Proto.MyRecord2.newBuilder();
            rec2Builder.setRecNo(1); // Same primary key
            rec2Builder.setStrValue("two");
            recordStore.saveRecord(rec2Builder.build());

            FDBStoredRecord<Message> rec2 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec2);
            TestRecords7Proto.MyRecord2.Builder myrec = TestRecords7Proto.MyRecord2.newBuilder();
            myrec.mergeFrom(rec2.getRecord());
            assertEquals("two", myrec.getStrValue(), "should load second record");

            assertEquals(0L, recordStore.scanIndexRecords("MyRecord1$str_value").getCount().get().longValue(), "should have no records in index");
        }
    }

    @Test
    public void open() throws Exception {
        // This tests the functionality of "open", so doesn't use the same method of opening
        // the record store that other methods within this class use.
        Object[] metaDataPathObjects = new Object[]{"record-test", "unit", "metadataStore"};
        KeySpacePath metaDataPath;
        Subspace expectedSubspace;
        Subspace metaDataSubspace;
        try (FDBRecordContext context = fdb.openContext()) {
            metaDataPath = TestKeySpace.getKeyspacePath(metaDataPathObjects);
            expectedSubspace = path.toSubspace(context);
            metaDataSubspace = metaDataPath.toSubspace(context);
            context.ensureActive().clear(Range.startsWith(metaDataSubspace.pack()));
            context.commit();
        }

        Index newIndex = new Index("newIndex", concatenateFields("str_value_indexed", "num_value_3_indexed"));
        Index newIndex2 = new Index("newIndex2", concatenateFields("str_value_indexed", "rec_no"));
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                                                    .setRecNo(1066L)
                                                    .setNumValue2(42)
                                                    .setStrValueIndexed("value")
                                                    .setNumValue3Indexed(1729)
                                                    .build();

        // Test open without a MetaDataStore

        try (FDBRecordContext context = fdb.openContext()) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataProvider(metaDataBuilder).createOrOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(metaDataBuilder.getVersion(), recordStore.getRecordMetaData().getVersion());
            final int version = metaDataBuilder.getVersion();

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This stops the index build.

            final RecordMetaData staleMetaData = metaDataBuilder.getRecordMetaData();
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertEquals(Collections.singleton(newIndex2.getName()), recordStore.getRecordStoreState().getWriteOnlyIndexNames());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            final FDBRecordStore.Builder staleBuilder = recordStore.asBuilder().setMetaDataProvider(staleMetaData);
            TestHelpers.assertThrows(RecordStoreStaleMetaDataVersionException.class, staleBuilder::createOrOpen,
                    LogMessageKeys.LOCAL_VERSION.toString(), version + 1,
                    LogMessageKeys.STORED_VERSION.toString(), version + 2);
        }

        // Test open with a MetaDataStore

        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, null);

            FDBRecordStore.newBuilder().setMetaDataStore(metaDataStore).setContext(context).setKeySpacePath(path)
                    .createOrOpenAsync().handle((store, e) -> {
                        assertNull(store);
                        assertNotNull(e);
                        assertThat(e, instanceOf(CompletionException.class));
                        Throwable cause = e.getCause();
                        assertNotNull(cause);
                        assertThat(cause, instanceOf(FDBMetaDataStore.MissingMetaDataException.class));
                        return null;
                    }).join();

            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            RecordMetaData origMetaData = metaDataBuilder.getRecordMetaData();
            final int version = origMetaData.getVersion();

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .createOrOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
            recordStore.saveRecord(record);

            final FDBMetaDataStore staleMetaDataStore = metaDataStore;

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            metaDataStore.saveRecordMetaData(metaDataBuilder.getRecordMetaData());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace).setMetaDataStore(metaDataStore).open();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertEquals(recordStore.getRecordStoreState(), recordStore.getRecordStoreState());
            assertEquals(Collections.singleton(newIndex2.getName()), recordStore.getRecordStoreState().getWriteOnlyIndexNames());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            // The stale meta-data store uses the cached meta-data, hence the stale version exception
            FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(staleMetaDataStore);
            TestHelpers.assertThrows(RecordStoreStaleMetaDataVersionException.class, storeBuilder::createOrOpen,
                    LogMessageKeys.LOCAL_VERSION.toString(), version + 1,
                    LogMessageKeys.STORED_VERSION.toString(), version + 2);
        }

        // Test uncheckedOpen without a MetaDataStore

        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataProvider(metaDataBuilder).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(metaDataBuilder.getVersion(), recordStore.getRecordMetaData().getVersion());
            final int version = metaDataBuilder.getVersion();

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            recordStore = recordStore.asBuilder().setMetaDataProvider(metaDataBuilder).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This would stop the build if this ran checkVersion.

            final RecordMetaData staleMetaData = metaDataBuilder.getRecordMetaData();
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataProvider(metaDataBuilder).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            recordStore = recordStore.asBuilder().setMetaDataProvider(staleMetaData).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
        }

        // Test uncheckedOpen with a MetaDataStore

        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, null);

            FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).uncheckedOpenAsync().handle((store, e) -> {
                        assertNull(store);
                        assertNotNull(e);
                        assertThat(e, instanceOf(CompletionException.class));
                        Throwable cause = e.getCause();
                        assertNotNull(cause);
                        assertThat(cause, instanceOf(FDBMetaDataStore.MissingMetaDataException.class));
                        return null;
                    }).join();

            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            RecordMetaData origMetaData = metaDataBuilder.getRecordMetaData();
            int version = origMetaData.getVersion();

            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            metaDataBuilder.addIndex("MySimpleRecord", newIndex);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());

            recordStore.saveRecord(record); // This would stop the build if this used checkVersion

            final FDBMetaDataStore staleMetaDataStore = metaDataStore;

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            metaDataBuilder.addIndex("MySimpleRecord", newIndex2);
            metaDataStore.saveAndSetCurrent(metaDataBuilder.getRecordMetaData().toProto()).join();
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path).setMetaDataStore(metaDataStore).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());

            // The stale meta-data store uses the cached meta-data, hence the old version in the final assert
            recordStore = FDBRecordStore.newBuilder().setContext(context).setSubspace(expectedSubspace)
                    .setMetaDataStore(staleMetaDataStore).uncheckedOpen();
            assertEquals(expectedSubspace, recordStore.getSubspace());
            assertTrue(recordStore.getRecordStoreState().allIndexesReadable());
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
        }
    }

    /**
     * Validate that if the store header changes then an open record store in another transaction is failed with
     * a conflict.
     */
    @Test
    public void conflictWithHeaderChange() throws Exception {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");
        RecordMetaData metaData2 = metaDataBuilder.getRecordMetaData();
        assertThat(metaData1.getVersion(), lessThan(metaData2.getVersion()));

        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context)
                    .setMetaDataProvider(metaData1)
                    .create();
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context);
        }

        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            FDBRecordStore recordStore1 = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context1)
                    .setMetaDataProvider(metaData1)
                    .open();
            assertEquals(metaData1.getVersion(), recordStore1.getRecordStoreState().getStoreHeader().getMetaDataversion());

            FDBRecordStore recordStore2 = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context2)
                    .setMetaDataProvider(metaData2)
                    .open();
            assertEquals(metaData2.getVersion(), recordStore2.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context2);

            // Add a write to the first record store to make sure that the conflict are actually checked.
            recordStore1.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(1415)
                    .build());
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context1::commit);
        }

        try (FDBRecordContext context = openContext()) {
            assertThrows(RecordStoreStaleMetaDataVersionException.class, () -> FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context)
                    .setMetaDataProvider(metaData1)
                    .open());

            FDBRecordStore recordStore = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setContext(context)
                    .setMetaDataProvider(metaData2)
                    .open();
            assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            commit(context);
        }
    }

    @Nonnull
    private FDBMetaDataStore createMetaDataStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath metaDataPath, @Nonnull Subspace metaDataSubspace, @Nullable Descriptors.FileDescriptor localFileDescriptor) {
        FDBMetaDataStore metaDataStore = new FDBMetaDataStore(context, metaDataPath);
        metaDataStore.setMaintainHistory(false);
        assertEquals(metaDataSubspace, metaDataStore.getSubspace());
        metaDataStore.setDependencies(new Descriptors.FileDescriptor[]{RecordMetaDataOptionsProto.getDescriptor()});
        metaDataStore.setLocalFileDescriptor(localFileDescriptor);
        return metaDataStore;
    }

    @Test
    public void testCommittedVersion() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final long readVersion = context.ensureActive().getReadVersion().get();

            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
            assertThat(context.getCommittedVersion(), greaterThan(readVersion));
        }
    }

    @Test
    public void testCommittedVersionReadOnly() throws Exception {
        try (FDBRecordContext context = openContext()) {
            commit(context);
            assertThat(context.getCommittedVersion(), equalTo(-1L));
        }
    }

    @Test
    public void testVersionStamp() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
            assertNotNull(context.getVersionStamp());

            long committedVersion = context.getCommittedVersion();
            assertThat(committedVersion, greaterThan(0L));
            assertEquals(committedVersion, ByteBuffer.wrap(context.getVersionStamp()).getLong());
            // TODO: When have version stamp operations in tuples, etc. check them.
        }
    }

    @Test
    public void testVersionStampReadOnly() throws Exception {
        try (FDBRecordContext context = openContext()) {
            commit(context);
            assertNull(context.getVersionStamp());
        }
    }

    @Test
    public void testCancelWhileCommitVersionStamp() throws Exception {
        FDBRecordContext context = openContext();
        openSimpleRecordStore(context);
        recordStore.addConflictForSubspace(true); // so that we are not a read-only transaction
        CompletableFuture<Void> commitFuture = context.commitAsync();
        context.close();
        try {
            commitFuture.get();
        } catch (ExecutionException e) {
            // Ignore. Only waiting to make sure it's completed.
        }

        // Depending on who wins the race, we might hit either assert. However, the behavior
        // should match the result of getCommittedVersion.
        boolean shouldFail;
        long committedVersion;
        try {
            committedVersion = context.getCommittedVersion();
            assertThat(committedVersion, greaterThan(0L));
            shouldFail = false;
        } catch (RecordCoreStorageException e) {
            committedVersion = -1L;
            shouldFail = true;
        }
        try {
            byte[] versionStamp = context.getVersionStamp();
            assertThat(shouldFail, is(false));
            assertNotNull(versionStamp);
            assertEquals(committedVersion, ByteBuffer.wrap(versionStamp).getLong());
        } catch (RecordCoreStorageException e) {
            assertEquals("Transaction has not been committed yet.", e.getMessage());
            assertThat(shouldFail, is(true));
        }
    }

    @Test
    public void testSubspaceWriteConflict() throws Exception {
        // Double check that it works to have two contexts on same space without conflict.
        FDBRecordContext context1 = openContext();
        uncheckedOpenSimpleRecordStore(context1);
        FDBRecordStore recordStore1 = recordStore;
        try (FDBRecordContext context2 = openContext()) {
            uncheckedOpenSimpleRecordStore(context2);
            commit(context2);
        }
        commit(context1);

        // Again with conflict.
        FDBRecordContext context3 = openContext();
        uncheckedOpenSimpleRecordStore(context3);
        FDBRecordStore recordStore3 = recordStore;
        recordStore3.loadRecord(Tuple.from(0L)); // Need to read something as write-only transactions never conflict
        recordStore3.addConflictForSubspace(true);
        try (FDBRecordContext context4 = openContext()) {
            uncheckedOpenSimpleRecordStore(context4);
            recordStore.addConflictForSubspace(true);
            commit(context4);
        }
        try {
            commit(context3);
            fail("should have gotten failure");
        } catch (FDBExceptions.FDBStoreRetriableException ex) {
            assertTrue(ex.getCause() instanceof FDBException);
            assertThat(((FDBException)ex.getCause()).getCode(), equalTo(1020)); // not_committed
        }
    }

    @Test
    public void testSubspaceReadWriteConflict() throws Exception {
        // Double check that it works to have two contexts on same space writing different records without conflict.
        FDBRecordContext context1 = openContext();
        uncheckedOpenSimpleRecordStore(context1);
        FDBRecordStore recordStore1 = recordStore;
        recordStore1.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1).build());
        try (FDBRecordContext context2 = openContext()) {
            uncheckedOpenSimpleRecordStore(context2);
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(2).build());
            commit(context2);
        }
        commit(context1);

        // Again with requested conflict.
        FDBRecordContext context3 = openContext();
        uncheckedOpenSimpleRecordStore(context3);
        FDBRecordStore recordStore3 = recordStore;
        recordStore3.addConflictForSubspace(false);
        recordStore3.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(3).build());
        try (FDBRecordContext context4 = openContext()) {
            uncheckedOpenSimpleRecordStore(context4);
            recordStore.addConflictForSubspace(false);
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(4).build());
            commit(context4);
        }
        try {
            commit(context3);
            fail("should have gotten failure");
        } catch (FDBExceptions.FDBStoreRetriableException ex) {
            assertTrue(ex.getCause() instanceof FDBException);
            assertThat(((FDBException)ex.getCause()).getCode(), equalTo(1020)); // not_committed
        }
    }

    @Test
    public void testFormatVersionUpgrade() throws Exception {
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION - 1).create();
            assertEquals(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION - 1, recordStore.getFormatVersion());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION).open();
            assertEquals(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION, recordStore.getFormatVersion());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION - 1).open();
            assertEquals(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION, recordStore.getFormatVersion());
            commit(context);
        }
    }

    @Test
    public void testUserVersionMonotonic() throws Exception {
        final FDBRecordStoreBase.UserVersionChecker userVersion1 = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(101);
            }
        };
        final FDBRecordStoreBase.UserVersionChecker userVersion2 = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(102);
            }
        };

        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                .setContext(context).setKeySpacePath(path).setMetaDataProvider(builder).setUserVersionChecker(userVersion1)
                .create();
            assertEquals(101, recordStore.getUserVersion());
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(builder).setUserVersionChecker(userVersion2)
                    .open();
            assertEquals(102, recordStore.getUserVersion());
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(builder).setUserVersionChecker(userVersion1);
            RecordCoreException ex = assertThrows(RecordCoreException.class, () -> {
                storeBuilder.open();
            });
            assertThat(ex.getMessage(), containsString("Stale user version"));
        }
    }

    static class SwitchingProvider implements RecordMetaDataProvider, FDBRecordStoreBase.UserVersionChecker {
        private boolean needOld = false;
        private final Integer defaultVersion;
        private final RecordMetaData metaData1;
        private final RecordMetaData metaData2;

        SwitchingProvider(int defaultVersion, RecordMetaData metaData1, RecordMetaData metaData2) {
            this.defaultVersion = defaultVersion;
            this.metaData1 = metaData1;
            this.metaData2 = metaData2;
        }

        @Nonnull
        @Override
        public RecordMetaData getRecordMetaData() {
            return needOld ? metaData1 : metaData2;
        }

        @Override
        public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
            if (oldUserVersion < 0) {
                return CompletableFuture.completedFuture(defaultVersion);
            }
            if (oldUserVersion == 101) {
                needOld = true;
            }
            return CompletableFuture.completedFuture(oldUserVersion);
        }
    }

    @Test
    public void testUserVersionDeterminesMetaData() throws Exception {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        builder.setVersion(101);
        final RecordMetaData metaData1 = builder.getRecordMetaData();
        builder.setVersion(102);
        final RecordMetaData metaData2 = builder.getRecordMetaData();
        final SwitchingProvider oldProvider = new SwitchingProvider(101, metaData1, metaData1);
        final SwitchingProvider newProvider = new SwitchingProvider(102, metaData1, metaData2);

        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(oldProvider).setUserVersionChecker(oldProvider)
                    .create();
            assertEquals(101, recordStore.getUserVersion());
            assertEquals(metaData1, recordStore.getRecordMetaData());
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(newProvider).setUserVersionChecker(newProvider)
                    .open();
            assertEquals(101, recordStore.getUserVersion());
            assertEquals(metaData1, recordStore.getRecordMetaData());
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            context.commit();
        }

        final SwitchingProvider newProvider2 = new SwitchingProvider(102, metaData1, metaData2);
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, path);
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(newProvider2).setUserVersionChecker(newProvider2)
                    .create();
            assertEquals(102, recordStore.getUserVersion());
            assertEquals(metaData2, recordStore.getRecordMetaData());
            assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            context.commit();
        }
    }

    @Test
    public void unsplitCompatibility() throws Exception {
        TestRecords1Proto.MySimpleRecord rec1 = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1415L).build();
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context);
            // Write a record using the old format
            recordStore = recordStore.asBuilder()
                    .setFormatVersion(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION - 1)
                    .create();
            assertEquals(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION - 1, recordStore.getFormatVersion());
            recordStore.saveRecord(rec1);
            final byte[] rec1Key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L));
            FDBStoredRecord<Message> readRec1 = recordStore.loadRecord(Tuple.from(1415L));
            assertNotNull(readRec1);
            assertFalse(readRec1.isSplit());
            assertEquals(1, readRec1.getKeyCount());
            assertEquals(rec1Key.length, readRec1.getKeySize());
            assertEquals(Tuple.from(1415L), readRec1.getPrimaryKey());
            assertEquals(rec1, readRec1.getRecord());

            // Upgrade the format version to use new format
            recordStore = recordStore.asBuilder()
                    .setFormatVersion(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION)
                    .open();
            assertEquals(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION, recordStore.getFormatVersion());
            Message rec2 = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build();
            recordStore.saveRecord(rec2);

            // Read old-record that was written using old format.
            readRec1 = recordStore.loadRecord(Tuple.from(1415L));
            assertNotNull(readRec1);
            assertFalse(readRec1.isSplit());
            assertEquals(1, readRec1.getKeyCount());
            assertEquals(rec1Key.length, readRec1.getKeySize());
            assertEquals(Tuple.from(1415L), readRec1.getPrimaryKey());
            assertEquals(rec1, readRec1.getRecord());

            // Ensure written using old format.
            final byte[] rec2Key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L));
            final byte[] rawRecord = context.ensureActive().get(recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L))).get();
            assertNotNull(rawRecord);
            assertEquals(rec2, TestRecords1Proto.RecordTypeUnion.parseFrom(rawRecord).getMySimpleRecord());

            // Ensure can still read using point-lookup.
            FDBStoredRecord<Message> readRec2 = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(readRec2);
            assertFalse(readRec2.isSplit());
            assertEquals(1, readRec2.getKeyCount());
            assertEquals(Tuple.from(1066L), readRec2.getPrimaryKey());
            assertEquals(rec2Key.length, readRec2.getKeySize());
            assertEquals(rec2, readRec2.getRecord());

            // Ensure can still read using range scan.
            List<FDBStoredRecord<Message>> recs = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(2, recs.size());

            assertFalse(recs.get(0).isSplit());
            assertEquals(1, recs.get(0).getKeyCount());
            assertEquals(rec2Key.length, recs.get(0).getKeySize());
            assertEquals(Tuple.from(1066L), recs.get(0).getPrimaryKey());
            assertEquals(rec2, recs.get(0).getRecord());

            assertFalse(recs.get(1).isSplit());
            assertEquals(rec1, recs.get(1).getRecord());
            assertEquals(1, recs.get(1).getKeyCount());
            assertEquals(rec1Key.length, recs.get(0).getKeySize());
            assertEquals(Tuple.from(1415L), recs.get(1).getPrimaryKey());
            assertEquals(rec1, recs.get(1).getRecord());

            // Ensure can still delete.
            recordStore.deleteRecord(Tuple.from(1066L));
            assertEquals(1, recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().get().intValue());
            recordStore.deleteRecord(Tuple.from(1415L));
            assertEquals(0, recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().get().intValue());

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            // Add an index so that we have to check the record count. (This automatic upgrade happens only
            // if we are already reading the record count anyway, which is why it happens here.)
            uncheckedOpenSimpleRecordStore(context, metaDataBuilder ->
                    metaDataBuilder.addUniversalIndex(new Index("global$newCount", FDBRecordStoreTestBase.COUNT_INDEX.getRootExpression(), IndexTypes.COUNT))
            );
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION).open();
            assertEquals(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION, recordStore.getFormatVersion());

            final byte[] rec1Key = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L, SplitHelper.UNSPLIT_RECORD));
            FDBStoredRecord<Message> writtenRec1 = recordStore.saveRecord(rec1);
            assertNotNull(writtenRec1);
            assertFalse(writtenRec1.isSplit());
            assertEquals(1, writtenRec1.getKeyCount());
            assertEquals(rec1Key.length, writtenRec1.getKeySize());
            assertEquals(Tuple.from(1415L), writtenRec1.getPrimaryKey());
            assertEquals(rec1, writtenRec1.getRecord());

            final byte[] rawRec1 = context.ensureActive().get(rec1Key).get();
            assertNotNull(rawRec1);
            assertEquals(writtenRec1.getValueSize(), rawRec1.length);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            // Read from the now upgraded store with a record store that sets its format version to be older to make sure
            // it correctly switches to the new one.
            // Use same meta-data as last test
            uncheckedOpenSimpleRecordStore(context, metaDataBuilder ->
                    metaDataBuilder.addUniversalIndex(new Index("global$newCount", FDBRecordStoreTestBase.COUNT_INDEX.getRootExpression(), IndexTypes.COUNT))
            );
            recordStore = recordStore.asBuilder().setFormatVersion(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION - 1).open();
            assertEquals(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION, recordStore.getFormatVersion());

            final byte[] rawKey1 = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L, SplitHelper.UNSPLIT_RECORD));
            FDBStoredRecord<Message> writtenRec1 = recordStore.loadRecord(Tuple.from(1415L));
            assertNotNull(writtenRec1);
            assertEquals(rec1, writtenRec1.getRecord());
            assertEquals(Tuple.from(1415L), writtenRec1.getPrimaryKey());
            assertEquals(rawKey1.length, writtenRec1.getKeySize());

            final TestRecords1Proto.MySimpleRecord rec2 = rec1.toBuilder().setRecNo(1623L).build();
            final byte[] rawKey2 = recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1623L, SplitHelper.UNSPLIT_RECORD));
            FDBStoredRecord<Message> writtenRec2 = recordStore.saveRecord(rec1.toBuilder().setRecNo(1623L).build());
            assertEquals(rec2, writtenRec2.getRecord());
            assertEquals(1, writtenRec2.getKeyCount());
            assertEquals(rawKey2.length, writtenRec2.getKeySize());
            assertEquals(Tuple.from(1623L), writtenRec2.getPrimaryKey());

            final byte[] rawRec2 = context.ensureActive().get(rawKey2).get();
            assertNotNull(rawRec2);
            assertEquals(writtenRec2.getValueSize(), rawRec2.length);

            commit(context);
        }
    }

    @Test
    public void unsplitToSplitUpgrade() throws Exception {
        TestRecords1Proto.MySimpleRecord rec1 = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1066L).build();
        TestRecords1Proto.MySimpleRecord rec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                                                    .setRecNo(1415L)
                                                    .setStrValueIndexed(Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE + 2))
                                                    .build();
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
                metaDataBuilder.setStoreRecordVersions(false);
            });
            assertTrue(recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS).get());
            assertFalse(recordStore.getRecordMetaData().isSplitLongRecords());

            FDBStoredRecord<Message> storedRecord = recordStore.saveRecord(rec1);
            assertNotNull(storedRecord);
            assertEquals(1, storedRecord.getKeyCount());
            assertEquals(recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L, SplitHelper.UNSPLIT_RECORD)).length,
                    storedRecord.getKeySize());
            assertEquals(Tuple.from(1066L), storedRecord.getPrimaryKey());
            assertFalse(storedRecord.isSplit());
            assertEquals(rec1, storedRecord.getRecord());

            try {
                recordStore.saveRecord(rec2);
            } catch (RecordCoreException e) {
                assertThat(e.getMessage(), startsWith("Record is too long"));
            }
            assertNull(recordStore.loadRecord(Tuple.from(1415L)));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, TEST_SPLIT_HOOK);
            assertTrue(recordStore.checkVersion(null, FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS).get());
            assertTrue(recordStore.getRecordMetaData().isSplitLongRecords());

            // Single key lookup should still work.
            final FDBStoredRecord<Message> storedRec1 = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(storedRec1);
            assertEquals(1, storedRec1.getKeyCount());
            assertEquals(recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1066L, SplitHelper.UNSPLIT_RECORD)).length,
                    storedRec1.getKeySize());
            assertEquals(Tuple.from(1066L), storedRec1.getPrimaryKey());
            assertFalse(storedRec1.isSplit());
            assertEquals(rec1, storedRec1.getRecord());

            // Scan should return only that record
            RecordCursorIterator<FDBStoredRecord<Message>> cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> scannedRec1 = cursor.next();
            assertEquals(storedRec1, scannedRec1);
            assertFalse(cursor.hasNext());
            cursor = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> scannedReverseRec1 = cursor.next();
            assertEquals(storedRec1, scannedReverseRec1);
            assertFalse(cursor.hasNext());

            // Save a split record
            final FDBStoredRecord<Message> storedRec2 = recordStore.saveRecord(rec2);
            assertNotNull(storedRec2);
            assertEquals(2, storedRec2.getKeyCount());
            assertEquals(Tuple.from(1415L), storedRec2.getPrimaryKey());
            assertEquals(
                    2 * recordStore.getSubspace().pack(Tuple.from(FDBRecordStore.RECORD_KEY, 1415L)).length
                            + Tuple.from(SplitHelper.START_SPLIT_RECORD).pack().length + Tuple.from(SplitHelper.START_SPLIT_RECORD + 1).pack().length,
                    storedRec2.getKeySize()
            );
            assertTrue(storedRec2.isSplit());
            assertEquals(rec2, storedRec2.getRecord());

            // Scan should now contain both records.
            cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec1 = cursor.next();
            assertEquals(storedRec1, scannedRec1);
            assertTrue(cursor.hasNext());
            FDBStoredRecord<Message> scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertFalse(cursor.hasNext());
            cursor = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertTrue(cursor.hasNext());
            scannedRec1 = cursor.next();
            assertEquals(storedRec1, scannedRec1);
            assertFalse(cursor.hasNext());

            // Delete the unsplit record.
            assertTrue(recordStore.deleteRecord(Tuple.from(1066L)));

            // Scan should now have just the second record
            cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertFalse(cursor.hasNext());
            cursor = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asIterator();
            assertTrue(cursor.hasNext());
            scannedRec2 = cursor.next();
            assertEquals(storedRec2, scannedRec2);
            assertFalse(cursor.hasNext());

            commit(context);
        }
    }

    @Test
    public void importedRecordType() throws Exception {
        final RecordMetaDataHook hook = md -> {
            md.addIndex("MySimpleRecord", "added_index", "num_value_2");
        };

        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsImportProto.getDescriptor(), context, hook);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recBuilder.setNumValueUnique(123);
            recBuilder.setNumValue2(456);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsImportProto.getDescriptor(), context, hook);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecords1Proto.MySimpleRecord.Builder myrec1 = TestRecords1Proto.MySimpleRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(123, myrec1.getNumValueUnique());
            assertEquals(Collections.singletonList(Tuple.from("abc", 1)),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
            assertEquals(Collections.singletonList(Tuple.from(456, 1)),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("added_index"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
            commit(context);
        }
    }


    @Test
    public void storeExistenceChecks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(metaData);
            assertThrows(RecordStoreDoesNotExistException.class, storeBuilder::open);
            recordStore = storeBuilder.uncheckedOpen();
            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.setRecNo(1);
            recordStore.insertRecord(simple.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.Builder storeBuilder = recordStore.asBuilder()
                    .setContext(context);
            assertThrows(RecordStoreNoInfoAndNotEmptyException.class, storeBuilder::createOrOpen);
            recordStore = storeBuilder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.Builder storeBuilder = recordStore.asBuilder()
                    .setContext(context);
            assertThrows(RecordStoreAlreadyExistsException.class, storeBuilder::create);
            recordStore = storeBuilder.open();
            assertNotNull(recordStore.loadRecord(Tuple.from(1)));
            commit(context);
        }
    }

    @Test
    public void existenceChecks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.setRecNo(1);
            simple.setNumValue2(111);
            recordStore.insertRecord(simple.build());

            TestRecords1Proto.MyOtherRecord.Builder other = TestRecords1Proto.MyOtherRecord.newBuilder();
            other.setRecNo(2);
            other.setNumValue2(222);
            recordStore.insertRecord(other.build());

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.setRecNo(1);
            simple.setNumValue2(1111);
            assertThrows(RecordAlreadyExistsException.class, () -> recordStore.insertRecord(simple.build()));

            simple.setRecNo(3);
            simple.setNumValue2(3333);
            assertThrows(RecordDoesNotExistException.class, () -> recordStore.updateRecord(simple.build()));

            simple.setRecNo(2);
            simple.setNumValue2(2222);
            assertThrows(RecordTypeChangedException.class, () -> recordStore.updateRecord(simple.build()));

        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder simple = TestRecords1Proto.MySimpleRecord.newBuilder();
            simple.mergeFrom(recordStore.loadRecord(Tuple.from(1L)).getRecord());
            assertEquals(111, simple.getNumValue2());
            simple.setNumValue2(1111);
            recordStore.updateRecord(simple.build());

            simple.clear();
            simple.setRecNo(4);
            simple.setNumValue2(444);
            recordStore.insertRecord(simple.build());

            commit(context);
        }
    }

    @Test
    public void invalidMetaData() throws Exception {
        RecordMetaDataHook invalid = metaData -> {
            metaData.addIndex("MySimpleRecord", "no_such_field");
        };
        try (FDBRecordContext context = openContext()) {
            assertThrows(KeyExpression.InvalidExpressionException.class, () -> openSimpleRecordStore(context, invalid));
        }
    }

    @Test
    public void commitChecks() throws Exception {
        // Start check now; fails even if added.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(checkRec1Exists());

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .build();
            recordStore.saveRecord(rec);
            assertThrows(RecordDoesNotExistException.class, () -> commit(context));
        }
        // Deferred check; fails if not added.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(this::checkRec1Exists);

            assertThrows(RecordDoesNotExistException.class, () -> commit(context));
        }
        // Succeeds if added.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(this::checkRec1Exists);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .build();
            recordStore.saveRecord(rec);
            commit(context);
        }
        // Immediate succeeds too now.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(checkRec1Exists());
            commit(context);
        }
    }

    private CompletableFuture<Void> checkRec1Exists() {
        return recordStore.recordExistsAsync(Tuple.from(1)).thenAccept(exists -> {
            if (!exists) {
                throw new RecordDoesNotExistException("required record does not exist");
            }
        });
    }

    @Test
    public void testUpdateRecords() {
        KeySpacePath metaDataPath;
        Subspace metaDataSubspace;
        try (FDBRecordContext context = fdb.openContext()) {
            metaDataPath = TestKeySpace.getKeyspacePath(new Object[]{"record-test", "unit", "metadataStore"});
            metaDataSubspace = metaDataPath.toSubspace(context);
            context.ensureActive().clear(Range.startsWith(metaDataSubspace.pack()));
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            RecordMetaData origMetaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            final int version = origMetaData.getVersion();

            FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1Proto.getDescriptor());
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .createOrOpen();
            assertEquals(version, recordStore.getRecordMetaData().getVersion());

            TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .setStrValueIndexed("value")
                    .setNumValue3Indexed(1729)
                    .build();
            recordStore.saveRecord(record);

            // Update the records without a local descriptor. Storing an evolved record must fail.
            final TestRecords1EvolvedProto.MySimpleRecord evolvedRecord = TestRecords1EvolvedProto.MySimpleRecord.newBuilder()
                    .setRecNo(1067L)
                    .setNumValue2(43)
                    .setStrValueIndexed("evolved value")
                    .setNumValue3Indexed(1730)
                    .build();

            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, null);
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()); // Bumps the version
            final FDBRecordStore recordStoreWithNoLocalFileDescriptor = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(version + 1, recordStoreWithNoLocalFileDescriptor.getRecordMetaData().getVersion());
            MetaDataException e = assertThrows(MetaDataException.class, () -> recordStoreWithNoLocalFileDescriptor.saveRecord(evolvedRecord));
            assertEquals(e.getMessage(), "descriptor did not match record type");

            // Update the records with a local descriptor. Storing an evolved record must succeed this time.
            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1EvolvedProto.getDescriptor());
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()); // Bumps the version
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(version + 2, recordStore.getRecordMetaData().getVersion());
            recordStore.saveRecord(evolvedRecord);

            // Evolve the meta-data one more time and use it for local file descriptor. SaveRecord will succeed.
            final TestRecords1EvolvedAgainProto.MySimpleRecord evolvedAgainRecord = TestRecords1EvolvedAgainProto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .setStrValueIndexed("value")
                    .setNumValue3Indexed(1729)
                    .build();
            metaDataStore = createMetaDataStore(context, metaDataPath, metaDataSubspace, TestRecords1EvolvedAgainProto.getDescriptor());
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()); // Bumps the version
            recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore).setMetaDataProvider(origMetaData)
                    .open();
            assertEquals(version + 3, recordStore.getRecordMetaData().getVersion());
            recordStore.saveRecord(evolvedAgainRecord);
        }
    }

    @Nonnull
    private FDBMetaDataStore openMetaDataStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath metaDataPath, boolean clear) {
        FDBMetaDataStore metaDataStore = new FDBMetaDataStore(context, metaDataPath);
        metaDataStore.setDependencies(new Descriptors.FileDescriptor[] {
                RecordMetaDataOptionsProto.getDescriptor()
        });
        metaDataStore.setMaintainHistory(false);
        if (clear) {
            context.ensureActive().clear(Range.startsWith(metaDataPath.toSubspace(context).pack()));
        }
        return metaDataStore;
    }

    @Test
    public void testNoUnion() {
        final KeySpacePath metaDataPath = TestKeySpace.getKeyspacePath(new Object[]{"record-test", "unit", "metadataStore"});
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, true);
            metaDataStore.saveRecordMetaData(TestNoUnionProto.getDescriptor());
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            version = metaDataStore.getRecordMetaData().getVersion();
        }

        // Store a MySimpleRecord record.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore)
                    .createOrOpen();
            assertEquals(version, recordStore.getRecordMetaData().getVersion());
            Descriptors.Descriptor mySimpleRecordDescriptor = recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getDescriptor();
            final Descriptors.FieldDescriptor recNo = mySimpleRecordDescriptor.findFieldByName("rec_no");
            final Descriptors.FieldDescriptor numValue2 = mySimpleRecordDescriptor.findFieldByName("num_value_2");
            final Descriptors.FieldDescriptor strValueIndexed = mySimpleRecordDescriptor.findFieldByName("str_value_indexed");
            final Descriptors.FieldDescriptor numValue3Indexed = mySimpleRecordDescriptor.findFieldByName("num_value_3_indexed");
            Message.Builder messageBuilder = DynamicMessage.newBuilder(mySimpleRecordDescriptor);
            messageBuilder.setField(recNo, 1066L);
            messageBuilder.setField(numValue2, 42);
            messageBuilder.setField(strValueIndexed, "value");
            messageBuilder.setField(numValue3Indexed, 1729);
            recordStore.saveRecord(messageBuilder.build());
        }

        // Store another MySimpleRecord record with local file descriptor.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            metaDataStore.setLocalFileDescriptor(TestNoUnionProto.getDescriptor());
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore)
                    .createOrOpen();
            assertEquals(version, recordStore.getRecordMetaData().getVersion());
            TestNoUnionProto.MySimpleRecord record = TestNoUnionProto.MySimpleRecord.newBuilder()
                    .setRecNo(1067L)
                    .setNumValue2(43)
                    .setStrValueIndexed("value2")
                    .setNumValue3Indexed(1730)
                    .build();
            recordStore.saveRecord(record);
        }

        // Add a record type.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version , metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            metaDataStore.mutateMetaData(metaDataProto -> MetaDataProtoEditor.addRecordType(metaDataProto, newRecordType, Key.Expressions.field("rec_no")));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getRecordType("MyNewRecord").getSinceVersion().intValue());
            context.commit();
        }

        // Store a MyNewRecord record.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore)
                    .createOrOpen();
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
            Descriptors.Descriptor myNewRecordDescriptor = recordStore.getRecordMetaData().getRecordType("MyNewRecord").getDescriptor();
            final Descriptors.FieldDescriptor recNo = myNewRecordDescriptor.findFieldByName("rec_no");
            Message.Builder messageBuilder = DynamicMessage.newBuilder(myNewRecordDescriptor);
            messageBuilder.setField(recNo, 2345);
            recordStore.saveRecord(messageBuilder.build());
        }
    }

    @Test
    public void metaDataVersionZero() throws Exception {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestNoIndexesProto.getDescriptor());
        metaData.setVersion(0);

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData);

        final FDBRecordStoreBase.UserVersionChecker newStore = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                assertEquals(-1, oldUserVersion);
                assertEquals(-1, oldMetaDataVersion);
                return CompletableFuture.completedFuture(0);
            }
        };

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).setUserVersionChecker(newStore).create();
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().hasMetaDataversion());
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().hasUserVersion());
            commit(context);
        }

        final FDBRecordStoreBase.UserVersionChecker oldStore = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                assertEquals(0, oldUserVersion);
                assertEquals(0, oldMetaDataVersion);
                return CompletableFuture.completedFuture(0);
            }
        };

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).setUserVersionChecker(oldStore).open();
            commit(context);
        }
    }

    @Test
    public void updateLastUpdatedTime() throws Exception {
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");

        long firstUpdateTime;
        int metaDataVersion;
        try (FDBRecordContext context = openContext()) {
            final long beforeOpenTime = System.currentTimeMillis();
            openSimpleRecordStore(context);
            metaDataVersion = recordStore.getRecordMetaData().getVersion();
            firstUpdateTime = recordStore.getRecordStoreState().getStoreHeader().getLastUpdateTime();
            assertThat(firstUpdateTime, greaterThanOrEqualTo(beforeOpenTime));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            final long beforeOpenTime = System.currentTimeMillis();
            assumeTrue(firstUpdateTime < beforeOpenTime, "time has not advanced since first update");
            openSimpleRecordStore(context, hook);
            assertEquals(metaDataVersion + 1, recordStore.getRecordMetaData().getVersion());
            long secondUpdateTime = recordStore.getRecordStoreState().getStoreHeader().getLastUpdateTime();
            assertThat(secondUpdateTime, greaterThan(firstUpdateTime));
            commit(context);
        }
    }

    /**
     * Test mode for {@link #clearOmitUnsplitRecordSuffix(ClearOmitUnsplitRecordSuffixMode)}.
     */
    public enum ClearOmitUnsplitRecordSuffixMode {
        EMPTY, SAVE, DELETE
    }

    @EnumSource(ClearOmitUnsplitRecordSuffixMode.class)
    @ParameterizedTest(name = "clearOmitUnsplitRecordSuffix [mode = {0}]")
    public void clearOmitUnsplitRecordSuffix(ClearOmitUnsplitRecordSuffixMode mode) throws Exception {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData)
                .setFormatVersion(FDBRecordStore.FORMAT_CONTROL_FORMAT_VERSION);

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).create();
            if (mode != ClearOmitUnsplitRecordSuffixMode.EMPTY) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1L)
                        .setStrValueIndexed("abc")
                        .build();
                recordStore.saveRecord(record);
            }
            commit(context);
        }

        if (mode == ClearOmitUnsplitRecordSuffixMode.DELETE) {
            try (FDBRecordContext context = openContext()) {
                recordStore = builder.setContext(context).open();
                recordStore.deleteRecord(Tuple.from(1L));
                commit(context);
            }
        }

        metaData.addIndex("MySimpleRecord", "num_value_2");
        builder.setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).open();
            assertEquals(mode == ClearOmitUnsplitRecordSuffixMode.SAVE,
                    recordStore.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());
        }
    }

    @Test
    public void clearOmitUnsplitRecordSuffixTyped() throws Exception {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        final KeyExpression pkey = concat(Key.Expressions.recordType(), field("rec_no"));
        metaData.getRecordType("MySimpleRecord").setPrimaryKey(pkey);
        metaData.getRecordType("MyOtherRecord").setPrimaryKey(pkey);

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData)
                .setFormatVersion(FDBRecordStore.FORMAT_CONTROL_FORMAT_VERSION);

        final FDBStoredRecord<Message> saved;
        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).create();
            TestRecords1Proto.MyOtherRecord record = TestRecords1Proto.MyOtherRecord.newBuilder()
                        .setRecNo(1L)
                        .build();
            saved = recordStore.saveRecord(record);
            commit(context);
        }

        metaData.addIndex("MySimpleRecord", "num_value_2");
        builder.setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).open();
            FDBStoredRecord<Message> loaded = recordStore.loadRecord(saved.getPrimaryKey());
            assertNotNull(loaded);
            assertEquals(saved.getRecord(), loaded.getRecord());
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());
        }
    }

    @Test
    public void clearOmitUnsplitRecordSuffixOverlapping() throws Exception {
        final RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());

        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setKeySpacePath(path)
                .setMetaDataProvider(metaData)
                .setFormatVersion(FDBRecordStore.FORMAT_CONTROL_FORMAT_VERSION);

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).create();
            commit(context);
        }

        FDBRecordContext context1 = openContext();
        FDBRecordStore recordStore = builder.setContext(context1).open();
        TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1L)
                .setStrValueIndexed("abc")
                .build();
        FDBStoredRecord<Message> saved = recordStore.saveRecord(record);

        metaData.addIndex("MySimpleRecord", "num_value_2");
        builder.setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);

        // If we did build, we'd create a read confict on the new index.
        // We want to test that a conflict comes from the clearing itself.
        final FDBRecordStoreBase.UserVersionChecker dontBuild = new FDBRecordStoreBase.UserVersionChecker() {
            @Override
            public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
                return CompletableFuture.completedFuture(0);
            }

            @Override
            public IndexState needRebuildIndex(Index index, long recordCount, boolean indexOnNewRecordTypes) {
                return IndexState.DISABLED;
            }
        };
        builder.setUserVersionChecker(dontBuild);

        FDBRecordContext context2 = openContext();
        FDBRecordStore recordStore2 = builder.setContext(context2).open();
        assertFalse(recordStore2.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());

        commit(context1);
        assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(context2));

        try (FDBRecordContext context = openContext()) {
            recordStore = builder.setContext(context).open();
            FDBStoredRecord<Message> loaded = recordStore.loadRecord(saved.getPrimaryKey());
            assertNotNull(loaded);
            assertEquals(saved.getRecord(), loaded.getRecord());
            assertTrue(recordStore.getRecordStoreState().getStoreHeader().getOmitUnsplitRecordSuffix());
        }
    }
}
