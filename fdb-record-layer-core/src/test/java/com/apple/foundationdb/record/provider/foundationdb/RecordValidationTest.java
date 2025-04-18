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

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RecordValidationTest extends FDBRecordStoreTestBase {
    @Nonnull
    public static Stream<Integer> formatVersions() {
        return Stream.of(
                FDBRecordStore.RECORD_COUNT_KEY_ADDED_FORMAT_VERSION, // 3
                FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION, // 6
                FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
    }

    public static Stream<Arguments> splitAndVersion() {
        return Stream.of(true, false)
                .flatMap(
                        split -> formatVersions()
                                .map(version -> Arguments.of(split, version)));
    }

    @ParameterizedTest(name = "testValidateRecordsNoIssue [splitLongRecords = {0}], formatVersion = {1}")
    @MethodSource("splitAndVersion")
    public void testValidateRecordsNoIssue(boolean splitLongRecords, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecord(store, result.get(0), null);
            validateRecord(store, result.get(1), null);
            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsMissingRecord [splitLongRecords = {0}], formatVersion = {1}")
    @MethodSource("splitAndVersion")
    public void testValidateRecordsMissingRecord(boolean splitLongRecords, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            store.deleteRecord(result.get(0).getPrimaryKey());
            commit(context);
        }
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecord(store, result.get(0), FDBRecordStoreBase.RecordValidationOptions.RECORD_EXISTS);
            validateRecord(store, result.get(1), null);
            context.commit();
        }
    }

    @Nonnull
    public static Stream<Arguments> splitNumberAndFormatVersion() {
        return Stream.of(0, 1, 2)
                .flatMap(split -> formatVersions()
                        .map(version -> Arguments.of(split, version)));
    }

    @ParameterizedTest(name = "testValidateRecordsMissingSplit [splitNumber = {0}, formatVersion = {1}]")
    @MethodSource("splitNumberAndFormatVersion")
    public void testValidateRecordsMissingSplit(int splitNumber, int formatVersion) throws Exception {
        boolean splitLongRecords = true;
        // unsplit (short) records have split #0; split records splits start at #1
        // Use this number to decide which record to operate on
        int recordNumber = (splitNumber == 0) ? 0 : 1;

        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a split
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // If operating on the short record, #0 is the only split
            // If operating on the long record, splits can be 1,2,3
            final Tuple pkAndSplit = result.get(recordNumber).getPrimaryKey().add(splitNumber);
            byte[] split = store.recordsSubspace().pack(pkAndSplit);
            store.ensureContextActive().clear(split);
            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            final FDBRecordStoreBase.RecordValidationOptions expected;
            // When format version is 3 and the record is a short record, deleting the only split will make the record disappear
            if ((splitNumber == 0) && (formatVersion == 3)) {
                expected = FDBRecordStoreBase.RecordValidationOptions.RECORD_EXISTS;
            } else {
                expected = FDBRecordStoreBase.RecordValidationOptions.VALID_VALUE;
            }
            validateRecord(store, result.get(recordNumber), expected);
            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsDeserialize [formatVersion = {0}]")
    @MethodSource("formatVersions")
    public void testValidateRecordsDeserialize(int formatVersion) throws Exception {
        boolean splitLongRecords = true;
        // Remove the last split from the record (#3) so that it fails to deserialize
        int recordNumber = 1;

        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete split #3
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            final Tuple pkAndSplit = result.get(recordNumber).getPrimaryKey().add(3);
            byte[] split = store.recordsSubspace().pack(pkAndSplit);
            store.ensureContextActive().clear(split);
            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecord(store, result.get(recordNumber), FDBRecordStoreBase.RecordValidationOptions.DESERIALIZABLE);
            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsMissingVersion [splitLongRecords = {0}]")
    @BooleanSource
    public void testValidateRecordsMissingVersion(boolean splitLongRecords) throws Exception {
        final RecordMetaDataHook hook = getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION, hook);
        // Delete the versions
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
            Tuple pkAndVersion = result.get(0).getPrimaryKey().add(-1);
            byte[] versionKey = store.recordsSubspace().pack(pkAndVersion);
            store.ensureContextActive().clear(versionKey);

            pkAndVersion = result.get(1).getPrimaryKey().add(-1);
            versionKey = store.recordsSubspace().pack(pkAndVersion);
            store.ensureContextActive().clear(versionKey);
            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
            validateRecord(store, result.get(0), FDBRecordStoreBase.RecordValidationOptions.VALID_VERSION);
            validateRecord(store, result.get(1), FDBRecordStoreBase.RecordValidationOptions.VALID_VERSION);
            commit(context);
        }
    }

    @Nonnull
    private static RecordMetaDataHook getRecordMetaDataHook(final boolean splitLongRecords) {
        return metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
    }

    private void validateRecord(FDBRecordStore store, final FDBStoredRecord<Message> rec, final FDBRecordStoreBase.RecordValidationOptions validationOption) throws Exception {
        EnumSet<FDBRecordStoreBase.RecordValidationOptions> recordValidationOptions;
        recordValidationOptions = store.validateRecordAsync(rec.getPrimaryKey(), EnumSet.allOf(FDBRecordStoreBase.RecordValidationOptions.class), false).get();
        assertNotNull(recordValidationOptions);
        EnumSet<FDBRecordStoreBase.RecordValidationOptions> expected = (validationOption == null) ?
                                                                       EnumSet.noneOf(FDBRecordStoreBase.RecordValidationOptions.class) :
                                                                       EnumSet.of(validationOption);
        assertEquals(expected, recordValidationOptions);

        recordValidationOptions = store.validateRecordAsync(rec.getPrimaryKey(), EnumSet.noneOf(FDBRecordStoreBase.RecordValidationOptions.class), false).get();
        assertNotNull(recordValidationOptions);
        assertEquals(EnumSet.noneOf(FDBRecordStoreBase.RecordValidationOptions.class), recordValidationOptions);
    }


    @Nonnull
    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, final int formatVersion, final RecordMetaDataHook hook) throws Exception {
        List<FDBStoredRecord<Message>> result;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
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

            savedRecord1 = store.saveRecord(record1);
            savedRecord2 = store.saveRecord(record2);

            result = List.of(savedRecord1, savedRecord2);
            commit(context);
        }
        return result;
    }
}
