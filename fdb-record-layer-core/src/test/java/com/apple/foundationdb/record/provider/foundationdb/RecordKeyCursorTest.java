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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

// TODO: Add omitUnsplitRecordSuffix, remove formatVersion

public class RecordKeyCursorTest extends FDBRecordStoreTestBase {
    @ParameterizedTest(name = "testIterateRecordsNoIssue [splitLongRecords = {0}]")
    @BooleanSource()
    public void testIterateRecordsNoIssue(boolean splitLongRecords) throws Exception {
//    @Test
//    public void testIterateRecordsNoIssue() throws Exception {
//        boolean splitLongRecords = true;

        final RecordMetaDataHook hook = metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
        final List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, hook);

        // Scan records
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final Subspace recordsSubspace = recordStore.recordsSubspace();
            final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
            RecordCursor<KeyValue> inner = KeyValueCursor.Builder
                    .withSubspace(recordsSubspace)
                    .setContext(context)
                    .setContinuation(null)
                    .setRange(TupleRange.allOf(null))
                    .setScanProperties(ScanProperties.FORWARD_SCAN)
                    .build();
            final RecordKeyCursor recordKeyCursor = new RecordKeyCursor(context, recordsSubspace, inner, false, sizeInfo, ScanProperties.FORWARD_SCAN);

            final List<Tuple> keys = recordKeyCursor.asList().get();
            // TODO: limit manager, Skip, limit etc.

            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsMissingRecord [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionAndSplitArgs")
    public void testValidateRecordsMissingRecord(int formatVersion, boolean splitLongRecords) throws Exception {
        final RecordMetaDataHook hook = metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, hook);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.deleteRecord(result.get(0).getPrimaryKey());
            commit(context);
        }
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            validateRecord(result.get(0), FDBRecordStoreBase.RecordValidationOptions.RECORD_EXISTS);
            validateRecord(result.get(1), null);
            context.commit();
        }
    }

    @Nonnull
    public static Stream<Arguments> formatVersionAndSplitNumber() {
        return formatVersions()
                .flatMap(formatVersion -> Stream.of(
                        Arguments.of(formatVersion, 0),
                        Arguments.of(formatVersion, 1),
                        Arguments.of(formatVersion, 2),
                        Arguments.of(formatVersion, 3)));
    }

    @ParameterizedTest(name = "testValidateRecordsMissingSplit [formatVersion = {0}, splitNumber = {1}]")
    @MethodSource("formatVersionAndSplitNumber")
    public void testValidateRecordsMissingSplit(int formatVersion, int splitNumber) throws Exception {
        boolean splitLongRecords = true;
        // unsplit (short) records have split #0; split records splits start at #1
        // Use this number to decide which record to operate on
        int recordNumber = (splitNumber == 0) ? 0 : 1;

        final RecordMetaDataHook hook = metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, hook);
        // Delete a split
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            // If operating on the short record, #0 is the only split
            // If operating on the long record, splits can be 1,2,3
            final Tuple pkAndSplit = result.get(recordNumber).getPrimaryKey().add(splitNumber);
            byte[] split = recordStore.recordsSubspace().pack(pkAndSplit);
            recordStore.ensureContextActive().clear(split);
            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            validateRecord(result.get(recordNumber), FDBRecordStoreBase.RecordValidationOptions.VALID_VALUE);
            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsMissingVersion [formatVersion = {0}]")
    @MethodSource("formatVersions")
    public void testValidateRecordsMissingVersion(int formatVersion) throws Exception {
        boolean splitLongRecords = true;

        final RecordMetaDataHook hook = metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, hook);
        // Delete the versions
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            Tuple pkAndVersion = result.get(0).getPrimaryKey().add(-1);
            byte[] versionKey = recordStore.recordsSubspace().pack(pkAndVersion);
            recordStore.ensureContextActive().clear(versionKey);

            pkAndVersion = result.get(1).getPrimaryKey().add(-1);
            versionKey = recordStore.recordsSubspace().pack(pkAndVersion);
            recordStore.ensureContextActive().clear(versionKey);
            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            validateRecord(result.get(0), FDBRecordStoreBase.RecordValidationOptions.VALID_VERSION);
            validateRecord(result.get(1), FDBRecordStoreBase.RecordValidationOptions.VALID_VERSION);
            commit(context);
        }
    }

    private void validateRecord(final FDBStoredRecord<Message> rec, final FDBRecordStoreBase.RecordValidationOptions validationOption) throws Exception {
        EnumSet<FDBRecordStoreBase.RecordValidationOptions> recordValidationOptions;
        recordValidationOptions = recordStore.validateRecordAsync(rec.getPrimaryKey(), EnumSet.allOf(FDBRecordStoreBase.RecordValidationOptions.class), false).get();
        assertNotNull(recordValidationOptions);
        EnumSet<FDBRecordStoreBase.RecordValidationOptions> expected = (validationOption == null) ?
                                                                       EnumSet.noneOf(FDBRecordStoreBase.RecordValidationOptions.class) :
                                                                       EnumSet.of(validationOption);
        assertEquals(expected, recordValidationOptions);

        recordValidationOptions = recordStore.validateRecordAsync(rec.getPrimaryKey(), EnumSet.noneOf(FDBRecordStoreBase.RecordValidationOptions.class), false).get();
        assertNotNull(recordValidationOptions);
        assertEquals(EnumSet.noneOf(FDBRecordStoreBase.RecordValidationOptions.class), recordValidationOptions);
    }

    @Nonnull
    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, final RecordMetaDataHook hook) throws Exception {
        final TestRecords1Proto.MySimpleRecord record1 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1L)
                .setStrValueIndexed("foo")
                .setNumValue3Indexed(1066)
                .build();
        final String someText = splitLongRecords ? Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE * 2 + 2) : "some text (short)";
        final TestRecords1Proto.MySimpleRecord record2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(2L)
                .setStrValueIndexed(someText)
                .setNumValue3Indexed(1415)
                .build();
        // Save the two records
        final FDBStoredRecord<Message> savedRecord1;
        final FDBStoredRecord<Message> savedRecord2;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, hook);
            // Save with various formats
            final FDBRecordStore.Builder storeBuilder = recordStore.asBuilder();
            final FDBRecordStore store = storeBuilder.create();
            savedRecord1 = store.saveRecord(record1);
            savedRecord2 = store.saveRecord(record2);
            commit(context);
        }
        return List.of(savedRecord1, savedRecord2);
    }
}
