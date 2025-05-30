/*
 * RecordRepairRunnerTest.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.ParameterizedTestUtils;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Test the store's {@link RecordRepairRunner} implementation.
 * End to end test for the entire record validation process.
 */
public class RecordValidateOnlyTest extends FDBRecordStoreTestBase {
    public static Stream<Arguments> splitFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("splitLongRecords"),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"),
                Arrays.stream(RecordRepairRunner.ValidationKind.values()));
    }

    @ParameterizedTest()
    @MethodSource("splitFormatVersion")
    void testValidateRecordsNoIssue(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepairRunner.ValidationKind validationKind) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        saveRecords(splitLongRecords, formatVersion, hook);

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
        RecordValidationStatsResult repairStats = runner.runValidationStats(storeBuilder, validationKind);
        List<RecordValidationResult> repairResults = runner.runValidationAndRepair(storeBuilder, validationKind, false);

        // Verify records: If we are saving versions - all is OK.
        // If we're not saving versions, they will be flagged as missing.
        if (storeVersions || validationKind.equals(RecordRepairRunner.ValidationKind.RECORD_VALUE)) {
            Assertions.assertThat(repairStats.getStats()).isEmpty();
            Assertions.assertThat(repairResults).hasSize(0);
        } else {
            Assertions.assertThat(repairStats.getStats()).hasSize(1);
            Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 50);

            Assertions.assertThat(repairResults).hasSize(50);
            Assertions.assertThat(repairResults).allMatch(result ->
                    (!result.isValid()) && result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR)
            );
        }
    }

    @ParameterizedTest()
    @MethodSource("splitFormatVersion")
    void testValidateRecordsMissingRecord(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepairRunner.ValidationKind validationKind) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> records = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Note that the primary keys start with 1, so the location is one-off when removed
            store.deleteRecord(records.get(ValidationTestUtils.RECORD_INDEX_WITH_NO_SPLITS).getPrimaryKey());
            store.deleteRecord(records.get(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey());
            store.deleteRecord(records.get(21).getPrimaryKey());
            store.deleteRecord(records.get(22).getPrimaryKey());
            store.deleteRecord(records.get(44).getPrimaryKey());
            commit(context);
        }

        RecordValidationStatsResult repairStats;
        List<RecordValidationResult> repairResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
            repairStats = runner.runValidationStats(store.asBuilder(), validationKind);
            repairResults = runner.runValidationAndRepair(store.asBuilder(), validationKind, false);
        }

        // Verify records: The missing records are gone, so won't be flagged, leaving only 45 records around.
        if (storeVersions || validationKind.equals(RecordRepairRunner.ValidationKind.RECORD_VALUE)) {
            Assertions.assertThat(repairStats.getStats()).isEmpty();
            Assertions.assertThat(repairResults).hasSize(0);
        } else {
            Assertions.assertThat(repairStats.getStats()).hasSize(1);
            Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 45);

            Assertions.assertThat(repairResults).hasSize(45);
            Assertions.assertThat(repairResults).allMatch(result ->
                    (!result.isValid()) && result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR)
            );
        }
    }

    public static Stream<Arguments> splitNumberFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(0, 1, 2, 3),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"),
                Arrays.stream(RecordRepairRunner.ValidationKind.values()));
    }

    @ParameterizedTest
    @MethodSource("splitNumberFormatVersion")
    void testValidateMissingSplit(int splitNumber, FormatVersion formatVersion, boolean storeVersions, RecordRepairRunner.ValidationKind validationKind) throws Exception {
        boolean splitLongRecords = true;

        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> savedRecords = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a split
        int recordIndex = (splitNumber == 0) ? ValidationTestUtils.RECORD_INDEX_WITH_NO_SPLITS : ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS;
        final Tuple primaryKey = savedRecords.get(recordIndex).getPrimaryKey();
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // If operating on the short record, #0 is the only split
            // If operating on the long record, splits can be 1,2,3
            // Use splitNumber to decide which record to operate on.
            // Record #1 in the saved records is a short record, #33 is a long (split) record
            byte[] split = ValidationTestUtils.getSplitKey(store, primaryKey, splitNumber);
            store.ensureContextActive().clear(split);
            commit(context);
        }

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
        RecordValidationStatsResult repairStats = runner.runValidationStats(storeBuilder, validationKind);
        List<RecordValidationResult> repairResults = runner.runValidationAndRepair(storeBuilder, validationKind, false);

        if (splitNumber == 0) {
            if (storeVersions) {
                if (ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
                    // record split gone but version remains
                    Assertions.assertThat(repairStats.getStats()).hasSize(1);
                    Assertions.assertThat(repairStats.getStats()).containsEntry(RecordValueValidator.CODE_SPLIT_ERROR, 1);
                    Assertions.assertThat(repairResults).hasSize(1);
                    Assertions.assertThat(repairResults.get(0)).isEqualTo(RecordValidationResult.invalid(primaryKey, RecordValueValidator.CODE_SPLIT_ERROR, "any"));
                } else {
                    // record split gone and version elsewhere - record looks gone
                    Assertions.assertThat(repairStats.getStats()).isEmpty();
                    Assertions.assertThat(repairResults).isEmpty();
                }
            } else {
                if (validationKind.equals(RecordRepairRunner.ValidationKind.RECORD_VALUE)) {
                    // not storing and not checking versions
                    Assertions.assertThat(repairStats.getStats()).isEmpty();
                    Assertions.assertThat(repairResults).isEmpty();
                } else {
                    // not storing but checking version (one record considered gone)
                    Assertions.assertThat(repairStats.getStats()).hasSize(1);
                    Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 49);

                    Assertions.assertThat(repairResults).hasSize(49);
                    Assertions.assertThat(repairResults).allMatch(result ->
                            (!result.isValid()) && result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR)
                    );
                }
            }
        } else {
            final String expectedError = (splitNumber == 3) ? RecordValueValidator.CODE_DESERIALIZE_ERROR : RecordValueValidator.CODE_SPLIT_ERROR;
            if (storeVersions) {
                // record split missing
                Assertions.assertThat(repairStats.getStats()).hasSize(1);
                Assertions.assertThat(repairStats.getStats()).containsEntry(expectedError, 1);
                Assertions.assertThat(repairResults).hasSize(1);
                Assertions.assertThat(repairResults.get(0)).isEqualTo(RecordValidationResult.invalid(primaryKey, expectedError, "any"));
            } else {
                if (validationKind.equals(RecordRepairRunner.ValidationKind.RECORD_VALUE)) {
                    // not storing and not checking versions - one split missing
                    Assertions.assertThat(repairStats.getStats()).hasSize(1);
                    Assertions.assertThat(repairStats.getStats()).containsEntry(expectedError, 1);
                    Assertions.assertThat(repairResults).hasSize(1);
                    Assertions.assertThat(repairResults.get(0)).isEqualTo(RecordValidationResult.invalid(primaryKey, expectedError, "any"));
                } else {
                    // not storing but checking version (one record with split missing)
                    Assertions.assertThat(repairStats.getStats()).hasSize(2);
                    Assertions.assertThat(repairStats.getStats()).containsEntry(expectedError, 1);
                    Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 49);

                    Assertions.assertThat(repairResults).hasSize(50);
                    Assertions.assertThat(repairResults).allMatch(result ->
                            result.equals(RecordValidationResult.invalid(primaryKey, expectedError, "Blah")) ||
                                    (!result.isValid() && result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR))
                    );
                }
            }
        }
    }

    @MethodSource("splitFormatVersion")
    @ParameterizedTest
    void testValidateRecordsMissingVersion(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepairRunner.ValidationKind validationKind) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> savedRecords = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete the versions for the first 20 records
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            for (int i = 0; i < 20; i++) {
                byte[] versionKey = ValidationTestUtils.getSplitKey(store, savedRecords.get(i).getPrimaryKey(), -1);
                store.ensureContextActive().clear(versionKey);
            }
            commit(context);
        }

        RecordValidationStatsResult repairStats;
        List<RecordValidationResult> repairResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
            repairStats = runner.runValidationStats(store.asBuilder(), validationKind);
            repairResults = runner.runValidationAndRepair(store.asBuilder(), validationKind, false);
        }

        if (validationKind.equals(RecordRepairRunner.ValidationKind.RECORD_VALUE)) {
            // not validating versions
            Assertions.assertThat(repairStats.getStats()).isEmpty();
            Assertions.assertThat(repairResults).isEmpty();
        } else {
            if (!storeVersions) {
                // checking but not storing versions
                Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 50);
                Assertions.assertThat(repairResults).allMatch(result -> result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR));
                Assertions.assertThat(repairResults.stream().map(RecordValidationResult::getPrimaryKey).collect(Collectors.toList()))
                        .isEqualTo(IntStream.range(1, 51).boxed().map(Tuple::from).collect(Collectors.toList()));
            } else {
                if (!ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
                    // versions stored elsewhere - none deleted
                    Assertions.assertThat(repairStats.getStats()).isEmpty();
                    Assertions.assertThat(repairResults).isEmpty();
                } else {
                    // versions stored with records, 20 are deleted
                    Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 20);
                    Assertions.assertThat(repairResults).allMatch(result -> result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR));
                    Assertions.assertThat(repairResults.stream().map(RecordValidationResult::getPrimaryKey).collect(Collectors.toList()))
                            .isEqualTo(IntStream.range(1, 21).boxed().map(Tuple::from).collect(Collectors.toList()));
                }
            }
        }
    }

    public static Stream<Arguments> formatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ValidationTestUtils.formatVersions(),
                Arrays.stream(RecordRepairRunner.ValidationKind.values()));
    }

    @MethodSource("formatVersion")
    @ParameterizedTest
    void testValidateRecordsCorruptRecord(FormatVersion formatVersion, RecordRepairRunner.ValidationKind validationKind) throws Exception {
        boolean splitLongRecords = true;
        boolean storeVersions = true;
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> savedRecords = saveRecords(splitLongRecords, formatVersion, hook);
        final Tuple primaryKey = savedRecords.get(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey();
        // corrupt the value of the record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            byte[] key = ValidationTestUtils.getSplitKey(store, primaryKey, 1);
            final byte[] value = new byte[] {1, 2, 3, 4, 5};
            store.ensureContextActive().set(key, value);
            commit(context);
        }

        RecordValidationStatsResult repairStats;
        List<RecordValidationResult> repairResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
            repairStats = runner.runValidationStats(store.asBuilder(), validationKind);
            repairResults = runner.runValidationAndRepair(store.asBuilder(), validationKind, false);
        }

        Assertions.assertThat(repairStats.getStats()).hasSize(1);
        Assertions.assertThat(repairStats.getStats()).containsEntry(RecordValueValidator.CODE_DESERIALIZE_ERROR, 1);
        Assertions.assertThat(repairResults).hasSize(1);
        Assertions.assertThat(repairResults.get(0)).isEqualTo(RecordValidationResult.invalid(primaryKey, RecordValueValidator.CODE_DESERIALIZE_ERROR, "any"));
    }

    @MethodSource("formatVersion")
    @ParameterizedTest
    void testValidateRecordsCorruptVersion(FormatVersion formatVersion, RecordRepairRunner.ValidationKind validationKind) throws Exception {
        boolean splitLongRecords = true;
        boolean storeVersions = true;
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> savedRecords = saveRecords(splitLongRecords, formatVersion, hook);
        // corrupt the value of the version
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            byte[] key = ValidationTestUtils.getSplitKey(store, savedRecords.get(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey(), -1);
            final byte[] value = new byte[] {1, 2, 3, 4, 5};
            store.ensureContextActive().set(key, value);
            commit(context);
        }

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }

        RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
        // We don't currently support detecting this king of error
        Assertions.assertThatThrownBy(() -> runner.runValidationStats(storeBuilder, validationKind)).hasCauseInstanceOf(UnknownValidationException.class);
        Assertions.assertThatThrownBy(() -> runner.runValidationAndRepair(storeBuilder, validationKind, false)).hasCauseInstanceOf(UnknownValidationException.class);
    }

    // list of arguments for version and a bitset that has all the combinations of 4 bits set (except all unset)
    private static Stream<Arguments> versionAndBitset() {
        return ParameterizedTestUtils.cartesianProduct(
                ValidationTestUtils.formatVersions(),
                ValidationTestUtils.splitsToRemove());
    }

    /**
     * Don't store any versions but verify versions, so there would be many results (all records missing versions).
     * Validate the max number of results.
     * @param maxResultSize the max result size to return
     */
    @ParameterizedTest
    @CsvSource({"-1", "0", "1", "10", "100"})
    void testValidateMaxResultsReturned(int maxResultSize) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, false);
        final FormatVersion maximumSupportedVersion = FormatVersion.getMaximumSupportedVersion();
        saveRecords(true, maximumSupportedVersion, hook);

        RecordRepairRunner.ValidationKind validationKind = RecordRepairRunner.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            storeBuilder = store.asBuilder();
        }

        int expectedResultSize;
        switch (maxResultSize) {
            case -1:
            case 0:
            case 100:
                expectedResultSize = 50;
                break;
            default:
                expectedResultSize = maxResultSize;
                break;
        }

        RecordRepairRunner runner = RecordRepairRunner.builder(fdb)
                .withMaxResultsReturned(maxResultSize)
                .build();
        RecordValidationStatsResult repairStats = runner.runValidationStats(storeBuilder, validationKind);
        List<RecordValidationResult> repairResults = runner.runValidationAndRepair(storeBuilder, validationKind, false);

        Assertions.assertThat(repairStats.getStats()).hasSize(1);
        Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 50);
        Assertions.assertThat(repairResults).hasSize(expectedResultSize);
    }

    /**
     * Allow only a few scans per sec.
     * Validate the total length of time the validation takes.
     */
    @Test
    void testValidateMaxScansPerSec() throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, true);
        final FormatVersion maximumSupportedVersion = FormatVersion.getMaximumSupportedVersion();
        final int totalRecords = 200;
        final int maxRecordScannedPerSec = 100;
        saveRecords(1, totalRecords, true, maximumSupportedVersion, simpleMetaData(hook));

        RecordRepairRunner.ValidationKind validationKind = RecordRepairRunner.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepairRunner runner = RecordRepairRunner.builder(fdb)
                // 200 records at 100 records / sec should average out to 2 seconds (actual scanning time is minimal)
                .withMaxRecordScannedPerSec(maxRecordScannedPerSec)
                // have transaction as short as we can since the per-sec calculation only kicks in when transaction is done
                .withTransactionTimeQuotaMillis(1)
                .build();

        long start = System.currentTimeMillis();
        RecordValidationStatsResult repairStats = runner.runValidationStats(storeBuilder, validationKind);
        long mid = System.currentTimeMillis();
        List<RecordValidationResult> repairResults = runner.runValidationAndRepair(storeBuilder, validationKind, false);
        long end = System.currentTimeMillis();

        // Total time to run the scan. Add some slack (300 ms) to allow for test to be less flaky
        Assertions.assertThat(mid - start).isGreaterThan(totalRecords / maxRecordScannedPerSec * 1000 - 300);
        Assertions.assertThat(end - mid).isGreaterThan(totalRecords / maxRecordScannedPerSec * 1000 - 300);
    }


    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaDataHook hook) throws Exception {
        return saveRecords(1, 50, splitLongRecords, formatVersion, simpleMetaData(hook));
    }

    private List<FDBStoredRecord<Message>> saveRecords(int initialId, int totalRecords, final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaData metaData) throws Exception {
        List<FDBStoredRecord<Message>> result;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = createOrOpenRecordStore(context, metaData, path, formatVersion);
            result = ValidationTestUtils.saveRecords(store, initialId, totalRecords, splitLongRecords);
            commit(context);
        }
        return result;
    }
}
