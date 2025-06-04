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
 * Test the store's {@link RecordRepair} implementation.
 * End-to-end test for the entire record validation process.
 * This test does validation only and stats (allowRepair = false).
 */
public class RecordValidateOnlyTest extends FDBRecordStoreTestBase {
    private static final int NUM_RECORDS = 50;

    public static Stream<Arguments> splitFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("splitLongRecords"),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"),
                Arrays.stream(RecordRepair.ValidationKind.values()));
    }

    @ParameterizedTest()
    @MethodSource("splitFormatVersion")
    void testValidateRecordsNoIssue(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepair.ValidationKind validationKind) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        saveRecords(splitLongRecords, formatVersion, hook);

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }
        final RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withValidationKind(validationKind);
        try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {
            RepairStatsResults repairStats = statsRunner.run().join();
            RepairValidationResults repairResults = repairRunner.run().join();

            ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
            // Verify records: all is OK.
            // If we are storing versions, they will all be there
            // If we are not storing versions, verifying them is a no-op, so none will be flagged
            ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS);
            ValidationTestUtils.assertInvalidResults(repairResults.getInvalidResults(), 0, null);
        }
    }

    @ParameterizedTest()
    @MethodSource("splitFormatVersion")
    void testValidateRecordsMissingRecord(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepair.ValidationKind validationKind) throws Exception {
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

        RepairStatsResults repairStats;
        RepairValidationResults repairResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepair.Builder builder = RecordRepair.builder(fdb, store.asBuilder()).withValidationKind(validationKind);
            try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                    RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {
                repairStats = statsRunner.run().join();
                repairResults = repairRunner.run().join();
            }
        }

        ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS - 5);
        // Verify records: The missing records are gone, so won't be flagged, leaving only 45 records around.
        // If we are storing versions, they will all be there
        // If we are not storing versions, verifying them is a no-op, so none will be flagged
        ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS - 5);
        ValidationTestUtils.assertInvalidResults(repairResults.getInvalidResults(), 0, null);
    }

    public static Stream<Arguments> splitNumberFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(0, 1, 2, 3),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"),
                Arrays.stream(RecordRepair.ValidationKind.values()));
    }

    @ParameterizedTest
    @MethodSource("splitNumberFormatVersion")
    void testValidateMissingSplit(int splitNumber, FormatVersion formatVersion, boolean storeVersions, RecordRepair.ValidationKind validationKind) throws Exception {
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
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withValidationKind(validationKind);
        try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {
            RepairStatsResults repairStats = statsRunner.run().join();
            RepairValidationResults repairResults = repairRunner.run().join();
            List<RecordRepairResult> invalidResults = repairResults.getInvalidResults();

            if (splitNumber == 0) {
                if (storeVersions) {
                    if (ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
                        // record split gone but version remains
                        ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS - 1, 1, RecordRepairResult.CODE_SPLIT_ERROR);
                        ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
                        ValidationTestUtils.assertInvalidResults(
                                invalidResults,
                                1,
                                result -> result.equals(RecordRepairResult.invalid(primaryKey, RecordRepairResult.CODE_SPLIT_ERROR, "any")));
                    } else {
                        // record split gone and version elsewhere - record looks gone
                        ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS - 1);
                        ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS - 1);
                        ValidationTestUtils.assertInvalidResults(invalidResults, 0, null);
                    }
                } else {
                    // Not storing versions - record looks gone
                    ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS - 1);
                    ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS - 1);
                    ValidationTestUtils.assertInvalidResults(invalidResults, 0, null);
                }
            } else {
                final String expectedError = (splitNumber == 3) ? RecordRepairResult.CODE_DESERIALIZE_ERROR : RecordRepairResult.CODE_SPLIT_ERROR;
                // record split missing - there will always be some split remaining, so the record will be flagged
                ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS - 1, 1, expectedError);
                ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
                ValidationTestUtils.assertInvalidResults(
                        invalidResults,
                        1,
                        result -> result.equals(RecordRepairResult
                                .invalid(primaryKey, expectedError, "any")));
            }
        }
    }

    @MethodSource("splitFormatVersion")
    @ParameterizedTest
    void testValidateRecordsMissingVersion(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepair.ValidationKind validationKind) throws Exception {
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

        RepairStatsResults repairStats;
        RepairValidationResults repairResults;
        List<RecordRepairResult> invalidResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepair.Builder builder = RecordRepair.builder(fdb, store.asBuilder()).withValidationKind(validationKind);
            try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                    RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {
                repairStats = statsRunner.run().join();
                repairResults = repairRunner.run().join();
                invalidResults = repairResults.getInvalidResults();
            }
        }

        ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
        if (!storeVersions ||
                !ValidationTestUtils.versionStoredWithRecord(formatVersion) ||
                validationKind.equals(RecordRepair.ValidationKind.RECORD_VALUE)) {
            // if there are no versions or they are stored elsewhere, all looks OK
            ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS);
            ValidationTestUtils.assertInvalidResults(invalidResults, 0, null);
        } else {
            // versions stored with records, 20 are deleted
            ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS - 20, 20, RecordRepairResult.CODE_VERSION_MISSING_ERROR);
            ValidationTestUtils.assertInvalidResults(invalidResults, 20, result -> result.getErrorCode().equals(RecordRepairResult.CODE_VERSION_MISSING_ERROR));
            Assertions.assertThat(invalidResults.stream().map(RecordRepairResult::getPrimaryKey).collect(Collectors.toList()))
                    .isEqualTo(IntStream.range(1, 21).boxed().map(Tuple::from).collect(Collectors.toList()));
        }
    }

    public static Stream<Arguments> formatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ValidationTestUtils.formatVersions(),
                Arrays.stream(RecordRepair.ValidationKind.values()));
    }

    @MethodSource("formatVersion")
    @ParameterizedTest
    void testValidateRecordsCorruptRecord(FormatVersion formatVersion, RecordRepair.ValidationKind validationKind) throws Exception {
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

        RepairStatsResults repairStats;
        RepairValidationResults repairResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepair.Builder builder = RecordRepair.builder(fdb, store.asBuilder()).withValidationKind(validationKind);
            try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                    RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {
                repairStats = statsRunner.run().join();
                repairResults = repairRunner.run().join();
            }
        }

        ValidationTestUtils.assertRepairStats(repairStats, NUM_RECORDS - 1, 1, RecordRepairResult.CODE_DESERIALIZE_ERROR);
        ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
        ValidationTestUtils.assertInvalidResults(
                repairResults.getInvalidResults(),
                1,
                result -> result.equals(RecordRepairResult.invalid(primaryKey, RecordRepairResult.CODE_DESERIALIZE_ERROR, "any")));
    }

    /**
     * This test causes an exception to be thrown by the validation process. In order to make sure this exception is handled
     * correctly, we set the number of retries to 0 and assert that the total records scanned and detected is correct.
     */
    @MethodSource("formatVersion")
    @ParameterizedTest
    void testValidateRecordsCorruptVersion(FormatVersion formatVersion, RecordRepair.ValidationKind validationKind) throws Exception {
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

        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withNumOfRetries(0).withValidationKind(validationKind);
        // We don't currently support detecting this king of error
        try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {
            RepairStatsResults repairStats = statsRunner.run().join();
            RepairValidationResults repairResults = repairRunner.run().join();
            // Iteration stopped short of the full scan
            Assertions.assertThat(repairStats.getExceptionCaught().getCause()).isInstanceOf(UnknownValidationException.class);
            Assertions.assertThat(repairStats.getStats()).containsEntry(RecordRepairResult.CODE_VALID, ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS);
            Assertions.assertThat(repairStats.getStats()).hasSize(1);
            Assertions.assertThat(repairResults.isComplete()).isFalse();
            Assertions.assertThat(repairResults.getCaughtException().getCause()).isInstanceOf(UnknownValidationException.class);
            Assertions.assertThat(repairResults.getValidResultCount()).isEqualTo(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS);
            Assertions.assertThat(repairResults.getInvalidResults()).isEmpty();
        }
    }

    /**
     * Delete all versions so there would be many results (all records missing versions).
     * Validate the max number of results.
     * @param maxResultSize the max result size to return
     */
    @ParameterizedTest
    @CsvSource({"-1", "0", "1", "10", "100"})
    void testValidateMaxResultsReturned(int maxResultSize) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, true);
        final FormatVersion maximumSupportedVersion = FormatVersion.getMaximumSupportedVersion();
        final List<FDBStoredRecord<Message>> savedRecords = saveRecords(true, maximumSupportedVersion, hook);

        // Delete the versions for the all records
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            savedRecords.forEach(record -> {
                byte[] versionKey = ValidationTestUtils.getSplitKey(store, record.getPrimaryKey(), -1);
                store.ensureContextActive().clear(versionKey);
            });
            commit(context);
        }

        RecordRepair.ValidationKind validationKind = RecordRepair.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            storeBuilder = store.asBuilder();
        }

        int expectedResultSize;
        boolean isComplete;
        switch (maxResultSize) {
            case -1:
            case 0:
            case 100:
                expectedResultSize = NUM_RECORDS;
                isComplete = true;
                break;
            default:
                expectedResultSize = maxResultSize;
                isComplete = false;
                break;
        }

        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder)
                .withMaxResultsReturned(maxResultSize)
                .withValidationKind(validationKind);
        try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {

            RepairStatsResults repairStats = statsRunner.run().join();
            RepairValidationResults repairResults = repairRunner.run().join();

            ValidationTestUtils.assertRepairStats(repairStats, 0, NUM_RECORDS, RecordRepairResult.CODE_VERSION_MISSING_ERROR);
            Assertions.assertThat(repairResults.isComplete()).isEqualTo(isComplete);
            Assertions.assertThat(repairResults.getValidResultCount() + repairResults.getInvalidResults().size()).isEqualTo(expectedResultSize);
            Assertions.assertThat(repairResults.getCaughtException()).isNull();
            ValidationTestUtils.assertInvalidResults(repairResults.getInvalidResults(), expectedResultSize, result -> result.getErrorCode().equals(RecordRepairResult.CODE_VERSION_MISSING_ERROR));
        }
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

        RecordRepair.ValidationKind validationKind = RecordRepair.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder)
                // 200 records at 100 records / sec should average out to 2 seconds (actual scanning time is minimal)
                .withMaxRecordScannedPerSec(maxRecordScannedPerSec)
                // have transaction as short as we can since the per-sec calculation only kicks in when transaction is done
                .withTransactionTimeQuotaMillis(1)
                .withValidationKind(validationKind);

        try (RecordRepairStatsRunner statsRunner = builder.buildStatsRunner();
                RecordRepairValidateRunner repairRunner = builder.buildRepairRunner(false)) {
            long start = System.currentTimeMillis();
            statsRunner.run().join();
            long mid = System.currentTimeMillis();
            repairRunner.run().join();
            long end = System.currentTimeMillis();

            // Total time to run the scan. Add some slack (300 ms) to allow for test to be less flaky
            Assertions.assertThat(mid - start).isGreaterThan(1000 * totalRecords / maxRecordScannedPerSec - 300);
            Assertions.assertThat(end - mid).isGreaterThan(1000 * totalRecords / maxRecordScannedPerSec - 300);
        }
    }

    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaDataHook hook) throws Exception {
        return saveRecords(1, NUM_RECORDS, splitLongRecords, formatVersion, simpleMetaData(hook));
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
