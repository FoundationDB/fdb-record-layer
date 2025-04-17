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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordKeyCursorTest extends FDBRecordStoreTestBase {
    public enum UseContinuations { NONE, CONTINUATIONS, BYTE_LIMIT }

    public static Stream<Arguments> splitContinuationVersion() {
        return Stream.of(true, false)
                .flatMap(
                        split -> Arrays.stream(UseContinuations.values())
                                .flatMap(useContinuations -> Stream.of(3, 6, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                                        .map(version -> Arguments.of(split, useContinuations, version))));
    }

    @ParameterizedTest(name = "testIterateRecordsNoIssue [splitLongRecords = {0}, useContinuations = {1}, formatVersion = {2}]")
    @MethodSource("splitContinuationVersion")
    void testIterateRecordsNoIssue(boolean splitLongRecords, UseContinuations useContinuations, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            saveRecords(store, splitLongRecords, hook, 100);
            commit(context);
        }
        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            actualKeys = scanKeys(store, scanProperties, useContinuations != UseContinuations.NONE);
            commit(context);
        }
        List<Tuple> expectedKeys = IntStream.range(1, 101).boxed().map(Tuple::from).collect(Collectors.toList());
        assertEquals(expectedKeys, actualKeys);
    }

    @ParameterizedTest(name = "testIterateRecordsMissingRecord [splitLongRecords = {0}], useContinuations = {1}")
    @MethodSource("splitContinuationVersion")
    public void testIterateRecordsMissingRecord(boolean splitLongRecords, UseContinuations useContinuations, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            result = saveRecords(store, splitLongRecords, hook, 100);
            commit(context);
        }
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Note that the PK start with 1, so the location is one-off when removed
            store.deleteRecord(result.get(20).getPrimaryKey());
            store.deleteRecord(result.get(21).getPrimaryKey());
            store.deleteRecord(result.get(22).getPrimaryKey());
            store.deleteRecord(result.get(23).getPrimaryKey());
            commit(context);
        }
        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            actualKeys = scanKeys(store, scanProperties, useContinuations != UseContinuations.NONE);
            commit(context);
        }
        List<Tuple> expectedKeys = IntStream.range(1, 101).filter(i -> !Set.of(21, 22, 23, 24).contains(i)).boxed().map(Tuple::from).collect(Collectors.toList());
        assertEquals(expectedKeys, actualKeys);
    }

    public static Stream<Arguments> splitNumberContinuationsFormat() {
        return Stream.of(0, 1, 2, 3)
                .flatMap(
                        splitNumber -> Stream.of(UseContinuations.NONE, UseContinuations.CONTINUATIONS, UseContinuations.BYTE_LIMIT)
                                .flatMap(useContinuastions -> Stream.of(3, 6, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                                        .map(formatVersion -> Arguments.of(splitNumber, useContinuastions, formatVersion))));
    }

    @ParameterizedTest(name = "testIterateRecordsMissingSplit [splitNumber = {0}, useContinuations = {1}, formatVersion = {2}]")
    @MethodSource("splitNumberContinuationsFormat")
    public void testIterateRecordsMissingSplit(int splitNumber, UseContinuations useContinuations, int formatVersion) throws Exception {
        boolean splitLongRecords = true;

        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> savedRecords;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            savedRecords = saveRecords(store, splitLongRecords, hook, 100);
            commit(context);
        }
        // Delete a split
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // If operating on the short record, #0 is the only split
            // If operating on the long record, splits can be 1,2,3
            // Use splitNumber to decide which record to operate on.
            // Record #1 in the saved records is a short record, #17 is a long (split) record)
            int recordNumber = (splitNumber == 0) ? 1 : 17; // TODO ensure the record numbers are right
            final Tuple pkAndSplit = savedRecords.get(recordNumber).getPrimaryKey().add(splitNumber);
            byte[] split = store.recordsSubspace().pack(pkAndSplit);
            store.ensureContextActive().clear(split);
            // Also delete a split from the first record in the list TODO
            commit(context);
        }

        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            actualKeys = scanKeys(store, scanProperties, useContinuations != UseContinuations.NONE);
            commit(context);
        }
        List<Tuple> expectedKeys;
        // When format version is 3 and the record is a short record, deleting the only split will make the record disappear
        if ((splitNumber == 0) && (formatVersion == 3)) {
            expectedKeys = IntStream.range(1, 101).filter(i -> i != 2).boxed().map(Tuple::from).collect(Collectors.toList());
        } else {
            expectedKeys = IntStream.range(1, 101).boxed().map(Tuple::from).collect(Collectors.toList());
        }
        assertEquals(expectedKeys, actualKeys);
    }

    public static Stream<Arguments> splitAndContinuation() {
        return Stream.of(true, false)
                .flatMap(
                        split -> Arrays.stream(UseContinuations.values())
                                .map(useContinuations -> Arguments.of(split, useContinuations)));
    }

    @ParameterizedTest(name = "testIterateRecordsMissingVersion [splitLongRecords = {0}, useContinuations = {1}]")
    @MethodSource("splitAndContinuation")
    void testIterateRecordsMissingVersion(boolean splitLongRecords, UseContinuations useContinuations) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> savedRecords;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
            savedRecords = saveRecords(store, splitLongRecords, hook, 100);
            commit(context);
        }
        // Delete the versions for the first 20 records
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
            for (int i = 0; i < 20; i++) {
                Tuple pkAndVersion = savedRecords.get(i).getPrimaryKey().add(-1);
                byte[] versionKey = store.recordsSubspace().pack(pkAndVersion);
                store.ensureContextActive().clear(versionKey);
            }
            commit(context);
        }

        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
            actualKeys = scanKeys(store, scanProperties, useContinuations != UseContinuations.NONE);
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
    private List<FDBStoredRecord<Message>> saveRecords(FDBRecordStore store, final boolean splitLongRecords, final RecordMetaDataHook hook, int numRecords) throws Exception {
        List<FDBStoredRecord<Message>> result = new ArrayList<>(numRecords);
        for (int i = 1; i <= numRecords; i++) {
            final String someText = (splitLongRecords && ((i % 17) == 0)) ? Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE * 2 + 2) : "some text (short)";
            final TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(i)
                    .setStrValueIndexed(someText)
                    .setNumValue3Indexed(1415 + i * 7)
                    .build();
            result.add(store.saveRecord(record));
        }
        return result;
    }

    @Nullable
    private static ScanProperties getScanProperties(final UseContinuations useContinuations) {
        ExecuteProperties executeProperties;
        switch (useContinuations) {
            case NONE:
                return null;
            case CONTINUATIONS:
                executeProperties = ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(19) // TODO: It seems we're only getting 10 results back with limit of 19
                        .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                        .build();
                return new ScanProperties(executeProperties);
            case BYTE_LIMIT:
                executeProperties = ExecuteProperties.newBuilder()
                        .setScannedBytesLimit(2000)
                        .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                        .build();
                return new ScanProperties(executeProperties);
            default:
                throw new IllegalArgumentException("Unknown continuation");
        }
    }

    private List<Tuple> scanKeys(@Nonnull FDBRecordStore store, @Nullable ScanProperties scanProperties, boolean withContinuations) throws InterruptedException, ExecutionException {
        if (scanProperties == null) {
            scanProperties = ScanProperties.FORWARD_SCAN;
        }
        RecordCursor<Tuple> recordKeyCursor = store.scanRecordKeys(TupleRange.allOf(null), null, scanProperties);
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
                    recordKeyCursor = store.scanRecordKeys(TupleRange.allOf(null), continuation.toBytes(), scanProperties);
                }
            }
            return result;
        }
    }

    private List<FDBStoredRecord<Message>> scanKeysOld(@Nonnull FDBRecordStore store, @Nullable ScanProperties scanProperties, boolean withContinuations) throws InterruptedException, ExecutionException {
        if (scanProperties == null) {
            scanProperties = ScanProperties.FORWARD_SCAN;
        }
        RecordCursor<FDBStoredRecord<Message>> recordKeyCursor = store.scanRecords(TupleRange.allOf(null), null, scanProperties);
        if (!withContinuations) {
            return recordKeyCursor.asList().get();
        } else {
            boolean done = false;
            List<FDBStoredRecord<Message>> result = new ArrayList<>();
            while (!done) {
                RecordCursorResult<FDBStoredRecord<Message>> currentRecord = recordKeyCursor.getNext();
                while (currentRecord.hasNext()) {
                    result.add(currentRecord.get());
                    currentRecord = recordKeyCursor.getNext();
                }
                RecordCursorContinuation continuation = currentRecord.getContinuation();
                if (continuation.isEnd()) {
                    done = true;
                } else {
                    recordKeyCursor = store.scanRecords(TupleRange.allOf(null), continuation.toBytes(), scanProperties);
                }
            }
            return result;
        }
    }
}
