/*
 * RecordValidatorTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.ParameterizedTestUtils;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test for the {@link RecordValidator} implementations.
 * This test creates some corruption in records and then validates that the validators flag them accordingly.
 * Because the validators make assumptions about the structure of the record, splits and versions, this test monitors
 * the available format versions and forces re-evaluation when a new format version is added.
 */
public class RecordValidatorTest extends FDBRecordStoreTestBase {
    // Repeated here for the benefit of MethodSource
    private static Stream<FormatVersion> formatVersions() {
        return ValidationTestUtils.formatVersions();
    }

    public static Stream<Arguments> splitAndVersions() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("split"),
                formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"));
    }

    /**
     * This test class (and the implementations of the validators) make assumptions about the structure of the records.
     * This test method tries to ensure that these assumptions are validated with the new format version added.
     * Then, update the version below to the latest.
     */
    @Test
    void monitorFormatVersion() {
        assertEquals(FormatVersion.FULL_STORE_LOCK, FormatVersion.getMaximumSupportedVersion(),
                "New format version found. Please review the validators to ensure they still catch corruptions");
    }

    @ParameterizedTest
    @MethodSource("splitAndVersions")
    void testValidateRecordsNoIssue(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            result.forEach(rec -> {
                validateRecordValue(store, rec.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                validateRecordVersion(store, rec.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
            });
            context.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("splitAndVersions")
    void testValidateRecordsMissingRecord(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            result.forEach(rec -> {
                store.deleteRecord(rec.getPrimaryKey());
            });
            commit(context);
        }
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            result.forEach(rec -> {
                validateRecordValue(store, rec.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                if (storeVersions) {
                    validateRecordVersion(store, rec.getPrimaryKey(), RecordRepairResult.CODE_RECORD_MISSING_ERROR, RecordRepairResult.REPAIR_NOT_NEEDED);
                } else {
                    // Check skipped completely when versions are not stored
                    validateRecordVersion(store, rec.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                }
            });
            context.commit();
        }
    }

    @Nonnull
    public static Stream<Arguments> splitNumberAndFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(0, 1, 2),
                formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"));
    }

    @ParameterizedTest
    @MethodSource("splitNumberAndFormatVersion")
    void testValidateRecordsMissingSplit(int splitNumber, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        boolean splitLongRecords = true;
        // unsplit (short) records have split #0; split records splits start at #1
        // Use this number to decide which record to operate on
        // Split 0 means a short record (#0)
        // Splits 1,2 mean a long record (#1)
        int recordNumber = (splitNumber == 0) ? 0 : 1;

        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
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
            if ((splitNumber == 0) && ( !ValidationTestUtils.versionStoredWithRecord(formatVersion) || !storeVersions)) {
                // When format version is below 6 and the record is a short record, deleting the only split will make the record disappear
                validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                if (storeVersions) {
                    validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_RECORD_MISSING_ERROR, RecordRepairResult.REPAIR_NOT_NEEDED);
                } else {
                    // In this case we're not validating at all
                    validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                }
            } else {
                validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_SPLIT_ERROR, null);
                if (storeVersions) {
                    final Exception exception = assertThrows(Exception.class, () -> validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_VALID, null));
                    assertTrue((exception.getCause() instanceof SplitHelper.FoundSplitWithoutStartException) ||
                            (exception.getCause() instanceof SplitHelper.FoundSplitOutOfOrderException));
                }
                // Now with repair, the record will be deleted
                validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_SPLIT_ERROR, RecordRepairResult.REPAIR_RECORD_DELETED);
                if (storeVersions) {
                    validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_RECORD_MISSING_ERROR, RecordRepairResult.REPAIR_NOT_NEEDED);
                }
                // Record deleted, validate again to make sure
                validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_VALID, null);
                if (storeVersions) {
                    validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_RECORD_MISSING_ERROR, RecordRepairResult.REPAIR_NOT_NEEDED);
                }
            }
            context.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("formatVersions")
    void testValidateRecordsDeserialize(FormatVersion formatVersion) throws Exception {
        boolean splitLongRecords = true;
        // Remove the last split from the record (#3) so that it fails to deserialize
        int recordNumber = 1;

        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, true);
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
            validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_DESERIALIZE_ERROR, null);
            final Exception exception = assertThrows(Exception.class, () -> validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_VALID, null));
            assertTrue(exception.getCause() instanceof RecordDeserializationException);
            validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_DESERIALIZE_ERROR, RecordRepairResult.REPAIR_RECORD_DELETED);
            validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_RECORD_MISSING_ERROR, RecordRepairResult.REPAIR_NOT_NEEDED);
            assertTrue(exception.getCause() instanceof RecordDeserializationException);
            // Record deleted, validate again to make sure
            validateRecordValue(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_VALID, null);
            validateRecordVersion(store, result.get(recordNumber).getPrimaryKey(), RecordRepairResult.CODE_RECORD_MISSING_ERROR, RecordRepairResult.REPAIR_NOT_NEEDED);
            context.commit();
        }
    }

    @ParameterizedTest
    @MethodSource("splitAndVersions")
    void testValidateRecordsMissingVersion(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
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

        // For format version below 6, the version is stored elsewhere so deleting it from the split makes no difference
        String expectedResult;
        String expectedRepair;
        if (!ValidationTestUtils.versionStoredWithRecord(formatVersion) || !storeVersions) {
            expectedResult = RecordRepairResult.CODE_VALID;
            expectedRepair = RecordRepairResult.REPAIR_NOT_NEEDED;
        } else {
            expectedResult = RecordRepairResult.CODE_VERSION_MISSING_ERROR;
            expectedRepair = RecordRepairResult.REPAIR_VERSION_CREATED;
        }
        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            result.forEach(rec -> {
                validateRecordValue(store, rec.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                validateRecordVersion(store, rec.getPrimaryKey(), expectedResult, expectedRepair);
            });
            commit(context);
        }
    }

    // list of arguments for version and a bitset that has all the combinations of 4 bits set (except all unset)
    private static Stream<Arguments> versionAndBitset() {
        return ParameterizedTestUtils.cartesianProduct(
                formatVersions(),
                ValidationTestUtils.splitsToRemove());
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
    @ParameterizedTest
    @MethodSource("versionAndBitset")
    void testValidateRecordCombinationSplitMissing(FormatVersion formatVersion, BitSet splitsToRemove) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Delete the splits
        final FDBStoredRecord<Message> longRecord = result.get(1);
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Delete all the splits that have a bit set
            splitsToRemove.stream().forEach(bit -> {
                // bit #0 is the version (-1)
                // bits #1 - #3 are the split numbers (no split #0 for a split record)
                int split = (bit == 0) ? -1 : bit;
                byte[] key = ValidationTestUtils.getSplitKey(store, longRecord.getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
            });
            commit(context);
        }

        // validate
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);

            if (ValidationTestUtils.recordWillRemainValid(3, splitsToRemove, formatVersion)) {
                validateRecordValue(store, longRecord.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                validateRecordVersion(store, longRecord.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
            } else if (ValidationTestUtils.recordWillDisappear(3, splitsToRemove, formatVersion)) {
                validateRecordValue(store, longRecord.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                validateRecordVersion(store, longRecord.getPrimaryKey(), RecordRepairResult.CODE_RECORD_MISSING_ERROR, RecordRepairResult.REPAIR_NOT_NEEDED);
            } else if (ValidationTestUtils.recordWillHaveVersionMissing(3, splitsToRemove, formatVersion)) {
                validateRecordValue(store, longRecord.getPrimaryKey(), RecordRepairResult.CODE_VALID, RecordRepairResult.REPAIR_NOT_NEEDED);
                validateRecordVersion(store, longRecord.getPrimaryKey(), RecordRepairResult.CODE_VERSION_MISSING_ERROR, RecordRepairResult.REPAIR_VERSION_CREATED);
            } else {
                RecordValidator valueValidator = new RecordValueValidator(store);
                // Some other validation error for the value (missing split or deserialization error
                RecordRepairResult validationResult = valueValidator.validateRecordAsync(longRecord.getPrimaryKey()).join();
                assertFalse(validationResult.isValid());
                validationResult = valueValidator.repairRecordAsync(validationResult).join();
                assertTrue(validationResult.isRepaired());
                assertEquals(RecordRepairResult.REPAIR_RECORD_DELETED, validationResult.getRepairCode());
            }
            commit(context);
        }
    }

    /**
     * A test that corrupts one of the splits of the records and ensures it is not deserializable.
     * @param formatVersion the version format
     */
    @ParameterizedTest(name = "testValidateRecordCorruptSplit [formatVersion = {0}]")
    @MethodSource("formatVersions")
    void testValidateRecordCorruptSplit(FormatVersion formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Mess up the splits
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // corrupt the two records
            final FDBStoredRecord<Message> shortRecord = result.get(0);
            final FDBStoredRecord<Message> longRecord = result.get(1);

            byte[] key = ValidationTestUtils.getSplitKey(store, shortRecord.getPrimaryKey(), 0);
            final byte[] value = new byte[] {1, 2, 3, 4, 5};
            store.ensureContextActive().set(key, value);

            key = ValidationTestUtils.getSplitKey(store, longRecord.getPrimaryKey(), 1);
            store.ensureContextActive().set(key, value);

            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            result.forEach(rec -> {
                validateRecordValue(store, rec.getPrimaryKey(), RecordRepairResult.CODE_DESERIALIZE_ERROR, RecordRepairResult.REPAIR_RECORD_DELETED);
            });
            commit(context);
        }
    }

    /**
     * A test that corrupts the version of the records and ensures it is handled.
     *
     * @param formatVersion the version format
     */
    @ParameterizedTest
    @MethodSource("formatVersions")
    void testValidateRecordCorruptVersion(FormatVersion formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Mess up the splits
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // corrupt the two records
            final FDBStoredRecord<Message> shortRecord = result.get(0);
            final FDBStoredRecord<Message> longRecord = result.get(1);

            byte[] key = ValidationTestUtils.getSplitKey(store, shortRecord.getPrimaryKey(), -1);
            final byte[] value = new byte[] {1, 2, 3, 4, 5};
            store.ensureContextActive().set(key, value);

            key = ValidationTestUtils.getSplitKey(store, longRecord.getPrimaryKey(), -1);
            store.ensureContextActive().set(key, value);

            commit(context);
        }

        // Validate by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordValidator valueValidator = new RecordValueValidator(store);
            // Currently, we don't handle corrupt versions.
            result.forEach(rec -> {
                Exception ex = assertThrows(ExecutionException.class, () -> valueValidator.validateRecordAsync(rec.getPrimaryKey()).get());
                assertTrue(ex.getCause() instanceof UnknownValidationException);
            });

            commit(context);
        }
    }

    private void validateRecordVersion(final FDBRecordStore store, final Tuple primaryKey, final @Nonnull String expectedValidationCode, final String expectedRepairCode) {
        validate(expectedValidationCode, expectedRepairCode, new RecordVersionValidator(store), primaryKey);
    }

    private void validateRecordValue(final FDBRecordStore store, final Tuple primaryKey, final @Nonnull String expectedValidationCode, final String expectedRepairCode) {
        validate(expectedValidationCode, expectedRepairCode, new RecordValueValidator(store), primaryKey);
    }

    private void validate(String expectedValidationCode, @Nullable String expectedRepairCode, RecordValidator validator, Tuple primaryKey) {
        RecordRepairResult actualResult = validator.validateRecordAsync(primaryKey).join();

        assertEquals(primaryKey, actualResult.getPrimaryKey());
        if (expectedValidationCode.equals(RecordRepairResult.CODE_VALID)) {
            assertTrue(actualResult.isValid());
        } else {
            assertFalse(actualResult.isValid());
        }
        assertEquals(expectedValidationCode, actualResult.getErrorCode());

        if (expectedRepairCode != null) {
            RecordRepairResult repairResult = validator.repairRecordAsync(actualResult).join();
            assertTrue(repairResult.isRepaired());
            assertEquals(expectedRepairCode, repairResult.getRepairCode());
        }
    }

    @Nonnull
    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaDataHook hook) throws Exception {
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
