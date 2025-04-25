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

package com.apple.foundationdb.record.provider.foundationdb.recordvalidation;

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordValidationTest extends FDBRecordStoreTestBase {
    // Repeated here for the benefit of MethodSource
    private static Stream<Integer> formatVersions() {
        return ValidationTestUtils.formatVersions();
    }

    public static Stream<Arguments> splitAndVersion() {
        return Stream.of(true, false)
                .flatMap(split -> formatVersions()
                        .map(version -> Arguments.of(split, version)));
    }

    @ParameterizedTest(name = "testValidateRecordsNoIssue [splitLongRecords = {0}, formatVersion = {1}]")
    @MethodSource("splitAndVersion")
    void testValidateRecordsNoIssue(boolean splitLongRecords, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecordValue(store, result.get(0).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(0).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordValue(store, result.get(1).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(1).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsMissingRecord [splitLongRecords = {0}, formatVersion = {1}]")
    @MethodSource("splitAndVersion")
    void testValidateRecordsMissingRecord(boolean splitLongRecords, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            store.deleteRecord(result.get(0).getPrimaryKey());
            store.deleteRecord(result.get(1).getPrimaryKey());
            commit(context);
        }
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecordValue(store, result.get(0).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(0).getPrimaryKey(), RecordVersionValidator.CODE_RECORD_MISSING_ERROR);
            validateRecordValue(store, result.get(1).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(1).getPrimaryKey(), RecordVersionValidator.CODE_RECORD_MISSING_ERROR);
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
    void testValidateRecordsMissingSplit(int splitNumber, int formatVersion) throws Exception {
        boolean splitLongRecords = true;
        // unsplit (short) records have split #0; split records splits start at #1
        // Use this number to decide which record to operate on
        int recordNumber = (splitNumber == 0) ? 0 : 1;

        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a split
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // If operating on the short record, #0 is the only split
            // If operating on the long record, splits can be 1,2,3
            // Only using 1 and 2 in this test, 3 will cause a deserialization error (tested in testValidateRecordsDeserialize)
            byte[] split = ValidationTestUtils.getSplitKey(store, result.get(recordNumber).getPrimaryKey(), splitNumber);
            store.ensureContextActive().clear(split);
            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            final String expected;
            // When format version is 3 and the record is a short record, deleting the only split will make the record disappear
            if ((splitNumber == 0) && (formatVersion == 3)) {
                validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordValidationResult.CODE_VALID);
                validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordVersionValidator.CODE_RECORD_MISSING_ERROR);
            } else {
                validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordValueValidator.CODE_SPLIT_ERROR);
                final Exception exception = assertThrows(Exception.class, () -> validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordValidationResult.CODE_VALID));
                assertTrue((exception.getCause() instanceof SplitHelper.FoundSplitWithoutStartException) ||
                        (exception.getCause() instanceof SplitHelper.FoundSplitOutOfOrderException));
            }
            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsDeserialize [formatVersion = {0}]")
    @MethodSource("formatVersions")
    void testValidateRecordsDeserialize(int formatVersion) throws Exception {
        boolean splitLongRecords = true;
        // Remove the last split from the record (#3) so that it fails to deserialize
        int recordNumber = 1;

        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete split #3
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            byte[] split = ValidationTestUtils.getSplitKey(store, result.get(recordNumber).getPrimaryKey(), 3);
            store.ensureContextActive().clear(split);
            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordValueValidator.CODE_DESERIALIZE_ERROR);
            final Exception exception = assertThrows(Exception.class, () -> validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordValidationResult.CODE_VALID));
            assertTrue(exception.getCause() instanceof RecordDeserializationException);
            context.commit();
        }
    }

    @ParameterizedTest(name = "testValidateRecordsMissingVersion [splitLongRecords = {0}, formatVersion = {1}]")
    @MethodSource("splitAndVersion")
    void testValidateRecordsMissingVersion(boolean splitLongRecords, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete the versions
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            byte[] versionKey = ValidationTestUtils.getSplitKey(store, result.get(0).getPrimaryKey(), -1);
            store.ensureContextActive().clear(versionKey);

            versionKey = ValidationTestUtils.getSplitKey(store, result.get(1).getPrimaryKey(), -1);
            store.ensureContextActive().clear(versionKey);
            commit(context);
        }

        // For format version 3, the version is stored elsewhere so deleting it from the split makes no difference
        String expectedResult;
        if (formatVersion == 3) {
            expectedResult = RecordValidationResult.CODE_VALID;
        } else {
            expectedResult = RecordVersionValidator.CODE_VERSION_MISSING_ERROR;
        }
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecordValue(store, result.get(0).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(0).getPrimaryKey(), expectedResult);
            validateRecordValue(store, result.get(1).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(1).getPrimaryKey(), expectedResult);
            commit(context);
        }
    }

    @ParameterizedTest(name = "testValidateRecordsNoVersionStored [splitLongRecords = {0}, formatVersion = {1}]")
    @MethodSource("splitAndVersion")
    void testValidateRecordsNoVersionStored(boolean splitLongRecords, int formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, false);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            validateRecordValue(store, result.get(0).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(0).getPrimaryKey(), RecordVersionValidator.CODE_VERSION_MISSING_ERROR);
            validateRecordValue(store, result.get(1).getPrimaryKey(), RecordValidationResult.CODE_VALID);
            validateRecordVersion(store, result.get(1).getPrimaryKey(), RecordVersionValidator.CODE_VERSION_MISSING_ERROR);
            commit(context);
        }
    }

    // list of arguments for version and a bitset that has all the combinations of 4 bits set (except all unset)
    private static Stream<Arguments> versionAndBitset() {
        return formatVersions().flatMap(
                version -> ValidationTestUtils.splitsToRemove()
                        .map(bitset -> Arguments.of(version, bitset)));
    }

    /**
     * A test that runs through all the combinations of 4-bits and erases a split for every bit that is set.
     * This simulated all the combinations of splits that can go missing for a record with 3 splits
     * (version, splits 1-3).
     * Many of the test cases were also covered by a specific test elsewhere, this is kind of a "catch-all" test that
     * enhances coverage.
     *
     * @param formatVersion the version format
     * @param splitsToRemove the splits to remove
     */
    @ParameterizedTest(name = "testValidateRecordCombinationSplitMissing [formatVersion = {0}, splitsToRemove = {1}]")
    @MethodSource("versionAndBitset")
    void testValidateRecordCombinationSplitMissing(int formatVersion, BitSet splitsToRemove) throws Exception {
        // for formatVersion 3 we don't have a version split, so removing it by itself does nothing
        Assumptions.assumeFalse((formatVersion == 3) && (splitsToRemove.equals(ValidationTestUtils.toBitSet(0b0001))));

        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Delete the splits
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Delete all the splits that have a bit set
            splitsToRemove.stream().forEach(bit -> {
                // bit #0 is the version (-1)
                // bits #1 - #3 are the split numbers (no split #0 for a split record)
                int split = (bit == 0) ? -1 : bit;
                byte[] key = ValidationTestUtils.getSplitKey(store, result.get(1).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
            });
            commit(context);
        }

        // Validate by primary key
        // We should see at least one validation fail for each split combination
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordValidator valueValidator = new RecordValueValidator(store);
            final RecordValidationResult valueValidatorResult = valueValidator.validateRecordAsync(result.get(1).getPrimaryKey()).get();
            if (valueValidatorResult.isValid()) {
                // ensure there is a version issue instead
                RecordValidator versionValidator = new RecordVersionValidator(store);
                final RecordValidationResult versionValidatorResult = versionValidator.validateRecordAsync(result.get(1).getPrimaryKey()).get();
                assertFalse(versionValidatorResult.isValid());
            }

            commit(context);
        }
    }

    /**
     * A test that corrupts one of the splits of the recordsand ensures it is not deserializable.
     * @param formatVersion the version format
     */
    @ParameterizedTest(name = "testValidateRecordCorruptSplit [formatVersion = {0}]")
    @MethodSource("formatVersions")
    void testValidateRecordCorruptSplit(int formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Mess up the splits
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // corrupt the two records
            byte[] key = ValidationTestUtils.getSplitKey(store, result.get(0).getPrimaryKey(), 0);
            final byte[] value = new byte[] {1, 2, 3, 4, 5};
            store.ensureContextActive().set(key, value);

            key = ValidationTestUtils.getSplitKey(store, result.get(1).getPrimaryKey(), 1);
            store.ensureContextActive().set(key, value);

            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordValidator valueValidator = new RecordValueValidator(store);
            RecordValidationResult valueValidatorResult = valueValidator.validateRecordAsync(result.get(0).getPrimaryKey()).get();
            assertFalse(valueValidatorResult.isValid());
            assertEquals(RecordValueValidator.CODE_DESERIALIZE_ERROR, valueValidatorResult.getErrorCode());

            valueValidatorResult = valueValidator.validateRecordAsync(result.get(1).getPrimaryKey()).get();
            assertFalse(valueValidatorResult.isValid());
            assertEquals(RecordValueValidator.CODE_DESERIALIZE_ERROR, valueValidatorResult.getErrorCode());

            commit(context);
        }
    }

    /**
     * A test that corrupts the version of the records and ensures it is handled.
     *
     * @param formatVersion the version format
     */
    @ParameterizedTest(name = "testValidateRecordCorruptVersion [formatVersion = {0}]")
    @MethodSource("formatVersions")
    void testValidateRecordCorruptVersion(int formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Mess up the splits
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // corrupt the two records
            byte[] key = ValidationTestUtils.getSplitKey(store, result.get(0).getPrimaryKey(), -1);
            final byte[] value = new byte[] {1, 2, 3, 4, 5};
            store.ensureContextActive().set(key, value);

            key = ValidationTestUtils.getSplitKey(store, result.get(1).getPrimaryKey(), -1);
            store.ensureContextActive().set(key, value);

            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordValidator valueValidator = new RecordValueValidator(store);
            // Currently, we don't handle corrupt versions.
            Exception ex = assertThrows(ExecutionException.class, () -> valueValidator.validateRecordAsync(result.get(0).getPrimaryKey()).get());
            assertTrue(ex.getCause() instanceof UnknownValidationException);
            ex = assertThrows(ExecutionException.class, () -> valueValidator.validateRecordAsync(result.get(1).getPrimaryKey()).get());
            assertTrue(ex.getCause() instanceof UnknownValidationException);

            commit(context);
        }
    }

    private static void validateRecordVersion(final FDBRecordStore store, final Tuple primaryKey, final @Nonnull String expectedVersionValidationCode) throws InterruptedException, ExecutionException {
        RecordValidator versionValidator = new RecordVersionValidator(store);
        final RecordValidationResult versionValidatorResult = versionValidator.validateRecordAsync(primaryKey).get();
        if (expectedVersionValidationCode.equals(RecordValidationResult.CODE_VALID)) {
            assertTrue(versionValidatorResult.isValid());
        } else {
            assertFalse(versionValidatorResult.isValid());
        }
        assertEquals(expectedVersionValidationCode, versionValidatorResult.getErrorCode());
    }

    private static void validateRecordValue(final FDBRecordStore store, final Tuple primaryKey, final @Nonnull String expectedValueValidationCode) throws InterruptedException, ExecutionException {
        RecordValidator valueValidator = new RecordValueValidator(store);
        final RecordValidationResult valueValidatorResult = valueValidator.validateRecordAsync(primaryKey).get();
        if (expectedValueValidationCode.equals(RecordValidationResult.CODE_VALID)) {
            assertTrue(valueValidatorResult.isValid());
        } else {
            assertFalse(valueValidatorResult.isValid());
        }
        assertEquals(expectedValueValidationCode, valueValidatorResult.getErrorCode());
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
