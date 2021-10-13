/*
 * FDBRecordStoreUniquenessTest.java
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

import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of uniqueness checks.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreUniqueIndexTest extends FDBRecordStoreTestBase {

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

    /**
     * Validate the behavior when a unique index is added on existing data that already has duplicates on the same data.
     * The new index should not be built, as that is impossible to do without breaking the constraints of the index, but
     * it also shouldn't fail the store opening or commit, as otherwise, the store would never be able to be opened.
     *
     * @throws Exception from store opening code
     */
    @Test
    public void buildUniqueInCheckVersion() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            // create a uniqueness violation on a field without a unique index
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1412L)
                    .setNumValue2(42)
                    .build());

            commit(context);
        }

        final Index uniqueIndex = new Index("unique_num_value_2_index", Key.Expressions.field("num_value_2"),
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        assertTrue(uniqueIndex.isUnique());
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", uniqueIndex));

            assertFalse(recordStore.isIndexReadable(uniqueIndex), "index with uniqueness violations should not be readable after being added to the meta-data");
            final List<RecordIndexUniquenessViolation> uniquenessViolations = recordStore.scanUniquenessViolations(uniqueIndex).asList().get();
            assertThat(uniquenessViolations, not(empty()));
            for (RecordIndexUniquenessViolation uniquenessViolation : uniquenessViolations) {
                assertThat(uniquenessViolation.getPrimaryKey(), either(equalTo(Tuple.from(1066L))).or(equalTo(Tuple.from(1412L))));
            }

            commit(context);
        }
    }

    @Test
    public void uniquenessChecks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            Index index1 = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            Index index2 = recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_unique");

            AtomicBoolean check1Run = new AtomicBoolean(false);
            CompletableFuture<Void> check1 = MoreAsyncUtil.delayedFuture(1L, TimeUnit.MILLISECONDS)
                    .thenRun(() -> check1Run.set(true));
            recordStore.addIndexUniquenessCommitCheck(index1, check1);

            CompletableFuture<Void> check2 = new CompletableFuture<>();
            RecordCoreException err = new RecordCoreException("unable to run check");
            check2.completeExceptionally(err);
            recordStore.addIndexUniquenessCommitCheck(index2, check2);

            // Checks For index 1 should complete successfully and mark the "check1Run" boolean as completed. It
            // should not throw an error from check 2 completing exceptionally.
            recordStore.whenAllIndexUniquenessCommitChecks(index1).get();
            assertTrue(check1Run.get(), "check1 should have marked check1Run as having completed");

            // For index 2, the error should be caught when the uniqueness checks are waited on
            ExecutionException thrownExecutionException = assertThrows(ExecutionException.class, () -> recordStore.whenAllIndexUniquenessCommitChecks(index2).get());
            assertSame(err, thrownExecutionException.getCause());

            // The error from the "uniqueness check" should block the transaction from committing
            RecordCoreException thrownRecordCoreException = assertThrows(RecordCoreException.class, context::commit);
            assertSame(err, thrownRecordCoreException);
        }
    }
}
