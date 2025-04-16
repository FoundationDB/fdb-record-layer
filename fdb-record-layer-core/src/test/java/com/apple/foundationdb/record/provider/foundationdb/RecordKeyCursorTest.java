/*
 * RecordValidationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

// TODO: Add omitUnsplitRecordSuffix, remove formatVersion

public class RecordKeyCursorTest extends FDBRecordStoreTestBase {
    @ParameterizedTest(name = "testIterateRecordsNoIssue [splitLongRecords = {0}]")
    @BooleanSource()
    public void testIterateRecordsNoIssue(boolean splitLongRecords) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        saveRecords(splitLongRecords, hook, 100);

        // Scan records
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            actualKeys = scanKeys(context, null, false);
            // TODO: limit manager, Skip, limit etc.
            context.commit();
        }
        List<Tuple> expectedKeys = IntStream.range(1, 101).boxed().map(Tuple::from).collect(Collectors.toList());
        assertEquals(expectedKeys, actualKeys);
    }

    @ParameterizedTest(name = "testIterateRecordsNoIssueWithContinuation [splitLongRecords = {0}]")
    @BooleanSource()
    public void testIterateRecordsNoIssueWithContinuation(boolean splitLongRecords) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        saveRecords(splitLongRecords, hook, 100);

        // Scan records
        final List<Tuple> actualKeys;
        final List<FDBRawRecord> actualKeysOld;
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(19) // TODO: It seems we're only getting 10 results back with limit of 19
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .build();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            actualKeys = scanKeys(context, new ScanProperties(executeProperties), true);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            actualKeysOld = scanKeysOld(context, new ScanProperties(executeProperties), true);
            context.commit();
        }
        List<Tuple> expectedKeys = IntStream.range(1, 101).boxed().map(Tuple::from).collect(Collectors.toList());
        assertEquals(expectedKeys, actualKeys);
    }

    @ParameterizedTest(name = "testIterateRecordsMissingRecord [splitLongRecords = {0}]")
    @BooleanSource
    public void testIterateRecordsMissingRecord(boolean splitLongRecords) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, hook, 100);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            // Note that the PK start with 1, so the location is one-off when removed
            recordStore.deleteRecord(result.get(20).getPrimaryKey());
            recordStore.deleteRecord(result.get(21).getPrimaryKey());
            recordStore.deleteRecord(result.get(22).getPrimaryKey());
            recordStore.deleteRecord(result.get(23).getPrimaryKey());
            commit(context);
        }
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            actualKeys = scanKeys(context, null, false);
            context.commit();
        }
        List<Tuple> expectedKeys = IntStream.range(1, 101).filter(i -> !Set.of(21, 22, 23, 24).contains(i)).boxed().map(Tuple::from).collect(Collectors.toList());
        assertEquals(expectedKeys, actualKeys);
    }

    @ParameterizedTest(name = "testIterateRecordsMissingSplit [splitNumber = {0}]")
    @CsvSource({"0", "1", "2", "3"})
    public void testIterateRecordsMissingSplit(int splitNumber) throws Exception {
        boolean splitLongRecords = true;

        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> savedRecords = saveRecords(splitLongRecords, hook, 100);
        // Delete a split
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            // If operating on the short record, #0 is the only split
            // If operating on the long record, splits can be 1,2,3
            // Use splitNumber to decide which record to operate on.
            // Record #1 in the saved records is a short record, #17 is a long (split) record)
            int recordNumber = (splitNumber == 0) ? 1 : 17;
            final Tuple pkAndSplit = savedRecords.get(recordNumber).getPrimaryKey().add(splitNumber);
            byte[] split = recordStore.recordsSubspace().pack(pkAndSplit);
            recordStore.ensureContextActive().clear(split);
            // Also delete a split from the first record in the list TODO
            commit(context);
        }

        // Scan records
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            actualKeys = scanKeys(context, null, false);
            context.commit();
        }
        List<Tuple> expectedKeys = IntStream.range(1, 101).boxed().map(Tuple::from).collect(Collectors.toList());
        assertEquals(expectedKeys, actualKeys);
    }

    @Test
    public void testIterateRecordsMissingVersion() throws Exception {
        boolean splitLongRecords = true;

        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> records = saveRecords(splitLongRecords, hook, 100);
        // Delete the versions for the first 20 records
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int i = 0; i < 20; i++) {
                Tuple pkAndVersion = records.get(i).getPrimaryKey().add(-1);
                byte[] versionKey = recordStore.recordsSubspace().pack(pkAndVersion);
                recordStore.ensureContextActive().clear(versionKey);
            }
            commit(context);
        }

        // Scan records
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            actualKeys = scanKeys(context, null, true);
            context.commit();
        }
        List<Tuple> expectedKeys = IntStream.range(1, 101).boxed().map(Tuple::from).collect(Collectors.toList());
        assertEquals(expectedKeys, actualKeys);
    }

    @Nonnull
    private static RecordMetaDataHook getRecordMetaDataHook(final boolean splitLongRecords) {
        return metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
    }

    @Nonnull
    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, final RecordMetaDataHook hook, int numRecords) throws Exception {
        List<FDBStoredRecord<Message>> result = new ArrayList<>(numRecords);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            for (int i = 1; i <= numRecords; i++) {
                final String someText = (splitLongRecords && ((i % 17) == 0)) ? Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE * 2 + 2) : "some text (short)";
                final TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setStrValueIndexed(someText)
                        .setNumValue3Indexed(1415 + i * 7)
                        .build();
                result.add(recordStore.saveRecord(record));
            }
            commit(context);
        }
        return result;
    }

    private List<Tuple> scanKeys(final FDBRecordContext context, @Nullable ScanProperties scanProperties, boolean withContinuations) throws InterruptedException, ExecutionException {
        final Subspace recordsSubspace = recordStore.recordsSubspace();
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        if (scanProperties == null) {
            scanProperties = ScanProperties.FORWARD_SCAN;
        }
        RecordKeyCursor recordKeyCursor = new RecordKeyCursor(context, recordsSubspace, TupleRange.allOf(null), sizeInfo, scanProperties);
        if (!withContinuations) {
            return recordKeyCursor.asList().get();
        } else {
            boolean done = false;
            List<Tuple> result = new ArrayList<>();
            while (!done) {
                RecordCursorResult<Tuple> currentRecord = recordKeyCursor.getNext();
                while (currentRecord.hasNext()) {
                    result.add(currentRecord.get());
                    currentRecord = recordKeyCursor.getNext();
                }
                RecordCursorContinuation continuation = currentRecord.getContinuation();
                if (continuation.isEnd()) {
                    done = true;
                } else {
                    recordKeyCursor = new RecordKeyCursor(context, recordsSubspace, TupleRange.allOf(null), continuation.toBytes(), new SplitHelper.SizeInfo(), scanProperties, new CursorLimitManager(scanProperties));
                }
            }
            return result;
        }
    }

    private List<FDBRawRecord> scanKeysOld(final FDBRecordContext context, @Nullable ScanProperties scanProperties, boolean withContinuations) throws InterruptedException, ExecutionException {
        final Subspace recordsSubspace = recordStore.recordsSubspace();
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        if (scanProperties == null) {
            scanProperties = ScanProperties.FORWARD_SCAN;
        }
        KeyValueCursor inner = KeyValueCursor.Builder
                .withSubspace(recordsSubspace)
                .setContext(context)
                .setContinuation(null)
                .setRange(TupleRange.allOf(null))
                .setScanProperties(scanProperties)
                .build();
        SplitHelper.KeyValueUnsplitter recordKeyCursor = new SplitHelper.KeyValueUnsplitter(context, recordsSubspace, inner, false, sizeInfo, scanProperties);
        if (!withContinuations) {
            return recordKeyCursor.asList().get();
        } else {
            boolean done = false;
            List<FDBRawRecord> result = new ArrayList<>();
            while (!done) {
                RecordCursorResult<FDBRawRecord> currentRecord = recordKeyCursor.getNext();
                while (currentRecord.hasNext()) {
                    result.add(currentRecord.get());
                    currentRecord = recordKeyCursor.getNext();
                }
                RecordCursorContinuation continuation = currentRecord.getContinuation();
                if (continuation.isEnd()) {
                    done = true;
                } else {
                    inner = KeyValueCursor.Builder
                            .withSubspace(recordsSubspace)
                            .setContext(context)
                            .setContinuation(continuation.toBytes())
                            .setRange(TupleRange.allOf(null))
                            .setScanProperties(scanProperties)
                            .build();
                    recordKeyCursor = new SplitHelper.KeyValueUnsplitter(context, recordsSubspace, inner, false, sizeInfo, scanProperties);
                }
            }
            return result;
        }
    }
}
