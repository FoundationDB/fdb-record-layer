/*
 * RecordValidateAndRepairTest.java
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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.ParameterizedTestUtils;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Test the store's {@link RecordRepair} implementation.
 * End-to-end test for the entire record validation and repair process.
 * This is very close in implementation to {@link RecordValidateOnlyTest}, except that the "repair" is turned on
 * and more assertions and validations are added. It feals as though the two tests should remain separate to keep the
 * code readable and maintainable.
 */
public class RecordValidateAndRepairTest extends FDBRecordStoreTestBase {
    private static final int NUM_RECORDS = 50;

    public static Stream<Arguments> splitFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("splitLongRecords"),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"),
                Arrays.stream(RecordRepair.ValidationKind.values()));
    }

    @MethodSource("splitFormatVersion")
    @ParameterizedTest
    void testValidateRecordsNoIssue(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepair.ValidationKind validationKind) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        saveRecords(splitLongRecords, formatVersion, hook);

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withValidationKind(validationKind);
        // Run validation and repair
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            RepairValidationResults repairResults = runner.run().join();
            ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
            // Verify records: all is OK.
            // If we are storing versions, they will all be there
            // If we are not storing versions, verifying them is a no-op, so none will be flagged
            ValidationTestUtils.assertInvalidResults(repairResults.getInvalidResults(), 0, null);
        }

        validateNormalScan(hook, formatVersion, NUM_RECORDS, storeVersions);
    }

    public static Stream<Arguments> splitNumberFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(0, 1, 2, 3),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"),
                Arrays.stream(RecordRepair.ValidationKind.values()));
    }

    @MethodSource("splitNumberFormatVersion")
    @ParameterizedTest
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
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            // Run validation and repair
            RepairValidationResults repairResults = runner.run().join();
            List<RecordRepairResult> invalidResults = repairResults.getInvalidResults();

            if (splitNumber == 0) {
                if (storeVersions) {
                    if (ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
                        // record split gone but version remains
                        ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
                        ValidationTestUtils.assertInvalidResults(
                                invalidResults,
                                1,
                                result -> result.equals(RecordRepairResult
                                        .invalid(primaryKey, RecordRepairResult.CODE_SPLIT_ERROR, "any")
                                        .withRepair(RecordRepairResult.REPAIR_RECORD_DELETED)));
                    } else {
                        // record split gone and version elsewhere - record looks gone
                        ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS - 1);
                        ValidationTestUtils.assertInvalidResults(invalidResults, 0, null);
                    }
                } else {
                    // Not storing versions - record looks gone
                    ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS - 1);
                    ValidationTestUtils.assertInvalidResults(invalidResults, 0, null);
                }
            } else {
                final String expectedError = (splitNumber == 3) ? RecordRepairResult.CODE_DESERIALIZE_ERROR : RecordRepairResult.CODE_SPLIT_ERROR;
                // record split missing - there will always be some split remaining, so the record will be flagged
                ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
                ValidationTestUtils.assertInvalidResults(
                        invalidResults,
                        1,
                        result -> result.equals(RecordRepairResult
                                .invalid(primaryKey, expectedError, "any")
                                .withRepair(RecordRepairResult.REPAIR_RECORD_DELETED)));
            }
        }

        // Run validation again, no repair
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(false)) {
            RepairValidationResults repairResults = runner.run().join();
            // Everything was "repaired"
            ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS - 1);
            ValidationTestUtils.assertInvalidResults(repairResults.getInvalidResults(), 0, null);
        }

        // Load the records again to  make sure they are all there
        validateNormalScan(hook, formatVersion, NUM_RECORDS - 1, storeVersions);
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

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }

        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withValidationKind(validationKind);
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            // Run validate and repair
            RepairValidationResults repairResults = runner.run().join();
            List<RecordRepairResult> invalidResults = repairResults.getInvalidResults();

            ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
            if (!storeVersions ||
                    !ValidationTestUtils.versionStoredWithRecord(formatVersion) ||
                    validationKind.equals(RecordRepair.ValidationKind.RECORD_VALUE)) {
                // if there are no versions or they are stored elsewhere, all looks OK
                ValidationTestUtils.assertInvalidResults(invalidResults, 0, null);
            } else {
                // versions stored with records, 20 are deleted
                ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
                ValidationTestUtils.assertInvalidResults(
                        invalidResults,
                        20,
                        result -> result.isRepaired() && result.getRepairCode().equals(RecordRepairResult.REPAIR_VERSION_CREATED));
                Assertions.assertThat(invalidResults.stream().map(RecordRepairResult::getPrimaryKey).collect(Collectors.toList()))
                        .isEqualTo(IntStream.range(1, 21).boxed().map(Tuple::from).collect(Collectors.toList()));
            }
        }

        // Run validation again
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(false)) {
            RepairValidationResults repairResults = runner.run().join();
            // Everything was "repaired"
            ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
            ValidationTestUtils.assertInvalidResults(repairResults.getInvalidResults(), 0, null);
        }

        // Load the records again to make sure they are all there
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            final List<FDBStoredRecord<Message>> records = store.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get();
            if (!storeVersions) {
                // no versions stored - no repair done
                records.forEach(rec -> Assertions.assertThat(rec.getVersion()).isNull());
            } else if (!ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
                // Versions were not deleted - they are all still there
                records.forEach(rec -> Assertions.assertThat(rec.getVersion()).isNotNull());
            } else if (validationKind.equals(RecordRepair.ValidationKind.RECORD_VALUE_AND_VERSION)) {
                // Repair was run
                records.forEach(rec -> Assertions.assertThat(rec.getVersion()).isNotNull());
            } else {
                // Versions deleted but no repair - they are still missing
                Assertions.assertThat(records.stream().filter(rec -> rec.getVersion() == null).count()).isEqualTo(20);
            }
            Assertions.assertThat(records).hasSize(NUM_RECORDS);
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
        // corrupt the value of the record
        final Tuple primaryKey = savedRecords.get(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey();
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            byte[] key = ValidationTestUtils.getSplitKey(store, primaryKey, 1);
            final byte[] value = new byte[] {1, 2, 3, 4, 5};
            store.ensureContextActive().set(key, value);
            commit(context);
        }

        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withValidationKind(validationKind);
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            // Run validate and repair
            RepairValidationResults repairResults = runner.run().join();

            ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS);
            ValidationTestUtils.assertInvalidResults(
                    repairResults.getInvalidResults(),
                    1,
                    result -> result.equals(RecordRepairResult.invalid(primaryKey, RecordRepairResult.CODE_DESERIALIZE_ERROR, "any")
                            .withRepair(RecordRepairResult.REPAIR_RECORD_DELETED)));
        }

        // Run validation again
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(false)) {
            RepairValidationResults repairResults = runner.run().join();

            ValidationTestUtils.assertCompleteResults(repairResults, NUM_RECORDS - 1);
            ValidationTestUtils.assertInvalidResults(repairResults.getInvalidResults(), 0, null);
        }

        // Load the records again to make sure they are all there
        validateNormalScan(hook, formatVersion, NUM_RECORDS - 1, storeVersions);
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
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            // This means that the repair process stops mid-way, as soon as we hit the corrupt record. The last transaction will not be committed.
            RepairValidationResults repairResults = runner.run().join();
            Assertions.assertThat(repairResults.isComplete()).isFalse();
            Assertions.assertThat(repairResults.getCaughtException().getCause()).isInstanceOfAny(UnknownValidationException.class);
            Assertions.assertThat(repairResults.getValidResultCount()).isEqualTo(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS);
            Assertions.assertThat(repairResults.getInvalidResults()).isEmpty();
        }
    }

    // list of arguments for version and a bitset that has all the combinations of 4 bits set (except all unset)
    private static Stream<Arguments> versionAndBitset() {
        return ParameterizedTestUtils.cartesianProduct(
                ValidationTestUtils.formatVersions(),
                ValidationTestUtils.splitsToRemove());
    }

    /**
     * A test that runs through all the combinations of 4-bits and erases a split for every bit that is set.
     * This simulated all the combinations of splits that can go missing for a record with 3 splits
     * (version, splits 1-3).
     *
     * @param formatVersion the version format
     * @param splitsToRemove the splits to remove
     */
    @MethodSource("versionAndBitset")
    @ParameterizedTest
    void testValidateRecordCombinationSplitMissing(FormatVersion formatVersion, BitSet splitsToRemove) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Delete the splits for two of the long records
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Delete all the splits that have a bit set
            splitsToRemove.stream().forEach(bit -> {
                // bit #0 is the version (-1)
                // bits #1 - #3 are the split numbers (no split #0 for a split record)
                int split = (bit == 0) ? -1 : bit;
                byte[] key = ValidationTestUtils.getSplitKey(store, result.get(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
                key = ValidationTestUtils.getSplitKey(store, result.get(ValidationTestUtils.RECORD_INDEX_WITH_TWO_SPLITS).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
            });
            commit(context);
        }

        RecordRepair.ValidationKind validationKind = RecordRepair.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withValidationKind(validationKind);
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            // Run validate and repair
            RepairValidationResults repairResults = runner.run().join();

            int validResults = NUM_RECORDS;
            if (ValidationTestUtils.recordWillDisappear(2, splitsToRemove, formatVersion)) {
                validResults--;
            }
            if (ValidationTestUtils.recordWillDisappear(3, splitsToRemove, formatVersion)) {
                validResults--;
            }
            ValidationTestUtils.assertCompleteResults(repairResults, validResults);

            Map<Integer, RecordRepairResult> validationResultMap = repairResults.getInvalidResults().stream()
                    .collect(Collectors.toMap(res -> (int)res.getPrimaryKey().getLong(0), res -> res));

            // Assert that both records are either gone or are valid or flagged as corrupt
            Assertions.assertThat(
                            ValidationTestUtils.recordWillDisappear(2, splitsToRemove, formatVersion) ||
                                    ValidationTestUtils.recordWillRemainValid(2, splitsToRemove, formatVersion) ||
                                    validationResultMap.containsKey(ValidationTestUtils.RECORD_ID_WITH_TWO_SPLITS))
                    .isTrue();

            Assertions.assertThat(
                            ValidationTestUtils.recordWillDisappear(3, splitsToRemove, formatVersion) ||
                                    ValidationTestUtils.recordWillRemainValid(3, splitsToRemove, formatVersion) ||
                                    validationResultMap.containsKey(ValidationTestUtils.RECORD_ID_WITH_THREE_SPLITS))
                    .isTrue();
        }

        // Load the records again to  make sure they are all there
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            final List<FDBStoredRecord<Message>> records = store.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get();
            // Ensure there are 48 records plus unaffected ones (the corrupt ones are gone)
            // Also present are records which only have their versions removed
            int validRecords = NUM_RECORDS - 2;
            if (ValidationTestUtils.recordWillRemainValid(2, splitsToRemove, formatVersion) || ValidationTestUtils.recordWillHaveVersionMissing(2, splitsToRemove, formatVersion)) {
                validRecords++;
            }
            if (ValidationTestUtils.recordWillRemainValid(3, splitsToRemove, formatVersion) || ValidationTestUtils.recordWillHaveVersionMissing(3, splitsToRemove, formatVersion)) {
                validRecords++;
            }
            Assertions.assertThat(records).hasSize(validRecords);
        }
    }

    /**
     * Allow only a few deletes per sec.
     * Validate the total length of time the validation takes.
     * We corrupt 1/2 of the records and verify that only the number of deletes impacts the total time.
     */
    @Test
    void testValidateMaxDeletesPerSec() throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, true);
        final FormatVersion maximumSupportedVersion = FormatVersion.getMaximumSupportedVersion();
        final List<FDBStoredRecord<Message>> savedRecords = saveRecords(1, 400, false, maximumSupportedVersion, simpleMetaData(hook));

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            savedRecords.stream().forEach(rec -> {
                // Corrupt 1/2 of the records (with even PK)
                if (rec.getPrimaryKey().getLong(0) % 2 == 0) {
                    // Delete all #0 splits from the records
                    byte[] split = ValidationTestUtils.getSplitKey(store, rec.getPrimaryKey(), 0);
                    store.ensureContextActive().clear(split);
                }
            });
            commit(context);
        }

        RecordRepair.ValidationKind validationKind = RecordRepair.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder)
                // 200 records at 100 records / sec should average out to 2 seconds (actual scanning time is minimal)
                .withMaxRecordDeletesPerSec(100)
                // Have to break to multiple transactions for the per-sec calculation to take place
                .withMaxRecordDeletesPerTransaction(10)
                .withValidationKind(validationKind);

        long start = System.currentTimeMillis();
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            runner.run().join();
        }
        long end = System.currentTimeMillis();

        Assertions.assertThat(end - start).isGreaterThan(2000).isLessThan(3000);

        // There should be 200 records left
        validateNormalScan(hook, maximumSupportedVersion, 200, true);
    }

    /**
     * Allow a few transactions to commit but then introduce an exception that will stop the iteration.
     */
    @Test
    void testValidateSomeTransactionsCommitted() throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, true);
        final FormatVersion maximumSupportedVersion = FormatVersion.getMaximumSupportedVersion();
        final List<FDBStoredRecord<Message>> savedRecords = saveRecords(true, maximumSupportedVersion, hook);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            savedRecords.stream().forEach(rec -> {
                if (rec.getPrimaryKey().getLong(0) == ValidationTestUtils.RECORD_ID_WITH_THREE_SPLITS) {
                    // Corrupt version for record #33, so that the iteration will fail at that point
                    byte[] key = ValidationTestUtils.getSplitKey(store, rec.getPrimaryKey(), -1);
                    final byte[] value = new byte[] {1, 2, 3, 4, 5};
                    store.ensureContextActive().set(key, value);
                } else {
                    // Delete all #0 #1 splits from the other records
                    byte[] split = ValidationTestUtils.getSplitKey(store, rec.getPrimaryKey(), 0);
                    store.ensureContextActive().clear(split);
                    split = ValidationTestUtils.getSplitKey(store, rec.getPrimaryKey(), 1);
                    store.ensureContextActive().clear(split);
                }
            });
            commit(context);
        }

        RecordRepair.ValidationKind validationKind = RecordRepair.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder)
                // break into smaller transactions
                .withMaxRecordDeletesPerTransaction(10)
                .withNumOfRetries(0) // so that we can reason about the record count
                .withValidationKind(validationKind);
        try (RecordRepairValidateRunner runner = builder.buildRepairRunner(true)) {
            RepairValidationResults repairResults = runner.run().join();
            Assertions.assertThat(repairResults.isComplete()).isFalse();
            Assertions.assertThat(repairResults.getCaughtException()).hasCauseInstanceOf(UnknownValidationException.class);
            Assertions.assertThat(repairResults.getValidResultCount()).isZero();
            // We still count the uncommitted deletes though
            Assertions.assertThat(repairResults.getInvalidResults()).hasSize(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS);
        }

        // We should not be able to load the records
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            Assertions.assertThatThrownBy(() -> store.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get()).hasCauseInstanceOf(SplitHelper.FoundSplitWithoutStartException.class);
        }
    }

    @Test
    void testRunnerUnusableOnceClosed() throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, true);
        final FormatVersion maximumSupportedVersion = FormatVersion.getMaximumSupportedVersion();
        saveRecords(true, maximumSupportedVersion, hook);

        RecordRepair.ValidationKind validationKind = RecordRepair.ValidationKind.RECORD_VALUE_AND_VERSION;
        FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, maximumSupportedVersion);
            storeBuilder = store.asBuilder();
        }
        RecordRepair.Builder builder = RecordRepair.builder(fdb, storeBuilder).withValidationKind(validationKind);

        RecordRepairValidateRunner runner = null;
        try {
            runner = builder.buildRepairRunner(true);
        } finally {
            runner.close();
        }
        RepairValidationResults results = runner.run().join();
        Assertions.assertThat(results.isComplete()).isFalse();
        Assertions.assertThat(results.getCaughtException()).isInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
        Assertions.assertThat(results.getValidResultCount()).isZero();
        Assertions.assertThat(results.getInvalidResults()).isEmpty();
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


    private void validateNormalScan(final RecordMetaDataHook hook, final FormatVersion formatVersion, final int numRecords, Boolean hasVersion) throws Exception {
        // Load the records again to make sure they are all there
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            final List<FDBStoredRecord<Message>> records = store.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList().get();
            Assertions.assertThat(records).hasSize(numRecords);
            if (hasVersion != null) {
                if (hasVersion) {
                    records.forEach(rec -> Assertions.assertThat(rec.getVersion()).isNotNull());
                } else {
                    records.forEach(rec -> Assertions.assertThat(rec.getVersion()).isNull());
                }
            }
        }
    }
}
