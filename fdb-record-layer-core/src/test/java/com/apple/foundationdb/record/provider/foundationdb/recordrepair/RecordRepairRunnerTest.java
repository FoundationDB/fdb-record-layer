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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.ParameterizedTestUtils;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Test the store's {@link FDBRecordStore#scanRecordKeys(byte[], ScanProperties)} implementation.
 * Heavily parameterized test that tries to create various record corruption issues and scans to ensure that the
 * expected
 * keys can still be picked up by the scan operation.
 */
public class RecordRepairRunnerTest extends FDBRecordStoreTestBase {
    private static final int LONG_RECORD_SPACING = 17;
    private static final int RECORD_INDEX_WITH_NO_SPLITS = 1;
    private static final int RECORD_ID_WITH_NO_SPLITS = RECORD_INDEX_WITH_NO_SPLITS + 1;
    private static final int RECORD_INDEX_WITH_TWO_SPLITS = 16;
    private static final int RECORD_ID_WITH_TWO_SPLITS = RECORD_INDEX_WITH_TWO_SPLITS + 1;
    private static final int RECORD_INDEX_WITH_THREE_SPLITS = 33;
    private static final int RECORD_ID_WITH_THREE_SPLITS = RECORD_INDEX_WITH_THREE_SPLITS + 1;

    public static Stream<Arguments> splitFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("splitLongRecords"),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"),
                Arrays.stream(RecordRepairRunner.ValidationKind.values()));
    }

    // TODO: Test with failures
    // TODO: maybe test with different limits?
    // TODO: tests with delete limits (per sec/per transaction)
    // TODO: Run outside transactions

    @ParameterizedTest()
    @MethodSource("splitFormatVersion")
    void testValidateRecordsNoIssue(boolean splitLongRecords, FormatVersion formatVersion, boolean storeVersions, RecordRepairRunner.ValidationKind validationKind) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        saveRecords(splitLongRecords, formatVersion, hook);

        RecordValidationStatsResult repairStats;
        List<RecordValidationResult> repairResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);

            RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
            repairStats = runner.runValidationStats(store.asBuilder(), validationKind);
            repairResults = runner.runValidationAndRepair(store.asBuilder(), validationKind, false);
        }

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
            store.deleteRecord(records.get(RECORD_INDEX_WITH_NO_SPLITS).getPrimaryKey());
            store.deleteRecord(records.get(RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey());
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
                Stream.of(0, 1, 2), // todo: Add 3rd split
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
        int recordIndex = (splitNumber == 0) ? RECORD_INDEX_WITH_NO_SPLITS : RECORD_INDEX_WITH_THREE_SPLITS;
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
        RecordValidationStatsResult repairStats;
        List<RecordValidationResult> repairResults;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
            repairStats = runner.runValidationStats(store.asBuilder(), validationKind);
            repairResults = runner.runValidationAndRepair(store.asBuilder(), validationKind, false);
        }

        // When format version is below 6 and the record is a short record, deleting the only split will make the record disappear
        // When format version is 6 or 10, and we're not saving version, the same
        if ((splitNumber == 0) && (!ValidationTestUtils.versionStoredWithRecord(formatVersion) || !storeVersions)) {
            if (storeVersions || validationKind.equals(RecordRepairRunner.ValidationKind.RECORD_VALUE)) {
                Assertions.assertThat(repairStats.getStats()).isEmpty();
                Assertions.assertThat(repairResults).hasSize(0);
            } else {
                Assertions.assertThat(repairStats.getStats()).hasSize(1);
                Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 49);

                Assertions.assertThat(repairResults).hasSize(49);
                Assertions.assertThat(repairResults).allMatch(result ->
                        (!result.isValid()) && result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR)
                );
            }
        } else {
            if (storeVersions || validationKind.equals(RecordRepairRunner.ValidationKind.RECORD_VALUE)) {
                Assertions.assertThat(repairStats.getStats()).hasSize(1);
                Assertions.assertThat(repairStats.getStats()).containsEntry(RecordValueValidator.CODE_SPLIT_ERROR, 1);
                Assertions.assertThat(repairResults).hasSize(1);
                Assertions.assertThat(repairResults.get(0)).isEqualTo(RecordValidationResult.invalid(primaryKey, RecordValueValidator.CODE_SPLIT_ERROR, "Blah"));
            } else {
                Assertions.assertThat(repairStats.getStats()).hasSize(2);
                Assertions.assertThat(repairStats.getStats()).containsEntry(RecordValueValidator.CODE_SPLIT_ERROR, 1);
                Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 49);

                Assertions.assertThat(repairResults).hasSize(50);
                Assertions.assertThat(repairResults).allMatch(result ->
                        result.equals(RecordValidationResult.invalid(primaryKey, RecordValueValidator.CODE_SPLIT_ERROR, "Blah")) ||
                                (!result.isValid() && result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR))
                );
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
            Assertions.assertThat(repairStats.getStats()).isEmpty();
            Assertions.assertThat(repairResults).isEmpty();
        } else {
            if (!ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
                if (!storeVersions) {
                    Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 50);
                    Assertions.assertThat(repairResults).allMatch(result -> result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR));
                    Assertions.assertThat(repairResults.stream().map(RecordValidationResult::getPrimaryKey).collect(Collectors.toList()))
                            .isEqualTo(IntStream.range(1, 51).boxed().map(Tuple::from).collect(Collectors.toList()));
                } else {
                    Assertions.assertThat(repairStats.getStats()).isEmpty();
                    Assertions.assertThat(repairResults).isEmpty();
                }
            } else if (!storeVersions) {
                Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 50);
                Assertions.assertThat(repairResults).allMatch(result -> result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR));
                Assertions.assertThat(repairResults.stream().map(RecordValidationResult::getPrimaryKey).collect(Collectors.toList()))
                        .isEqualTo(IntStream.range(1, 51).boxed().map(Tuple::from).collect(Collectors.toList()));
            } else {
                Assertions.assertThat(repairStats.getStats()).containsEntry(RecordVersionValidator.CODE_VERSION_MISSING_ERROR, 20);
                Assertions.assertThat(repairResults).allMatch(result -> result.getErrorCode().equals(RecordVersionValidator.CODE_VERSION_MISSING_ERROR));
                Assertions.assertThat(repairResults.stream().map(RecordValidationResult::getPrimaryKey).collect(Collectors.toList()))
                        .isEqualTo(IntStream.range(1, 21).boxed().map(Tuple::from).collect(Collectors.toList()));
            }
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
    @ParameterizedTest
    @MethodSource("versionAndBitset")
    void testValidateRecordCombinationSplitMissing(FormatVersion formatVersion, BitSet splitsToRemove) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(true, true);
        List<FDBStoredRecord<Message>> result = saveRecords(true, formatVersion, hook);
        // Delete the splits
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Delete all the splits that have a bit set
            splitsToRemove.stream().forEach(bit -> {
                // bit #0 is the version (-1)
                // bits #1 - #3 are the split numbers (no split #0 for a split record)
                int split = (bit == 0) ? -1 : bit;
                byte[] key = ValidationTestUtils.getSplitKey(store, result.get(RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
                key = ValidationTestUtils.getSplitKey(store, result.get(RECORD_INDEX_WITH_TWO_SPLITS).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
            });
            commit(context);
        }

        RecordValidationStatsResult repairStats;
        List<RecordValidationResult> repairResults;
        RecordRepairRunner.ValidationKind validationType = RecordRepairRunner.ValidationKind.RECORD_VALUE_AND_VERSION;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            RecordRepairRunner runner = RecordRepairRunner.builder(fdb).build();
            repairStats = runner.runValidationStats(store.asBuilder(), validationType);
            repairResults = runner.runValidationAndRepair(store.asBuilder(), validationType, false);
        }

        final List<Integer> actualKeys = repairResults.stream().map(res -> (int)res.getPrimaryKey().getLong(0)).collect(Collectors.toList());
        // The cases where the record will go missing altogether
        Set<Integer> keysExpectedToDisappear = new HashSet<>();
        if (recordWillDisappear(2, splitsToRemove, formatVersion)) {
            keysExpectedToDisappear.add(RECORD_ID_WITH_TWO_SPLITS);
        }
        if (recordWillDisappear(3, splitsToRemove, formatVersion)) {
            keysExpectedToDisappear.add(RECORD_ID_WITH_THREE_SPLITS);
        }

        // Assert that both keys are either gone or are flagged as corrupt or valid
        Assertions.assertThat(
                        recordWillDisappear(2, splitsToRemove, formatVersion) ||
                                recordRemainsValid(2, splitsToRemove, formatVersion) ||
                                actualKeys.contains(RECORD_ID_WITH_TWO_SPLITS))
                .isTrue();

        Assertions.assertThat(
                        recordWillDisappear(3, splitsToRemove, formatVersion) ||
                                recordRemainsValid(3, splitsToRemove, formatVersion) ||
                                actualKeys.contains(RECORD_ID_WITH_THREE_SPLITS))
                .isTrue();

        // Assert that both keys are either gone or are captured in stats
        // Assertions.assertThat(repairStats.getStats().values().stream().mapToInt(i -> i).sum() + keysExpectedToDisappear.size()).isEqualTo(2);
    }

    private boolean recordWillDisappear(int numOfSplits, BitSet splitsToRemove, FormatVersion formatVersion) {
        final BitSet allThreeSplits = ValidationTestUtils.toBitSet(0b1111);
        final BitSet allThreeSplitsWithoutVersion = ValidationTestUtils.toBitSet(0b1110);
        final BitSet allTwoSplits = ValidationTestUtils.toBitSet(0b0111);
        final BitSet allTwoSplitsWithoutVersion = ValidationTestUtils.toBitSet(0b0110);
        final boolean storingVersion = ValidationTestUtils.versionStoredWithRecord(formatVersion);
        switch (numOfSplits) {
            case 3:
                return (splitsToRemove.equals(allThreeSplits) ||
                                (!storingVersion && splitsToRemove.equals(allThreeSplitsWithoutVersion)));
            case 2:
                return (splitsToRemove.equals(allThreeSplits) || splitsToRemove.equals(allTwoSplits) ||
                                (!storingVersion &&
                                         (splitsToRemove.equals(allThreeSplitsWithoutVersion) || splitsToRemove.equals(allTwoSplitsWithoutVersion))));
            default:
                throw new IllegalArgumentException("Non supported number of splits");
        }
    }

    private boolean recordRemainsValid(int numOfSplits, BitSet splitsToRemove, FormatVersion formatVersion) {

        final BitSet thirdSplitOnly = ValidationTestUtils.toBitSet(0b1000);
        final BitSet thirdSplitAndVerion = ValidationTestUtils.toBitSet(0b1001);
        final BitSet versionSplitOnly = ValidationTestUtils.toBitSet(0b0001);
        final boolean storingVersion = ValidationTestUtils.versionStoredWithRecord(formatVersion);
        if ((numOfSplits == 2) && thirdSplitOnly.equals(splitsToRemove)) {
            // removing non-existent 3rd split
            return true;
        }
        if ((numOfSplits == 2) && thirdSplitAndVerion.equals(splitsToRemove) && !ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
            // version stored elsewhere and we remove version and non-existent split
            return true;
        }
        if (versionSplitOnly.equals(splitsToRemove) && !ValidationTestUtils.versionStoredWithRecord(formatVersion)) {
            // version stored elsewhere and we only remove the version
            return true;
        }
        return false;
    }

    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaDataHook hook) throws Exception {
        return saveRecords(1, 50, splitLongRecords, formatVersion, hook);
    }

    private List<FDBStoredRecord<Message>> saveRecords(int initialId, int totalRecords, final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaDataHook hook) throws Exception {
        return saveRecords(initialId, totalRecords, splitLongRecords, formatVersion, simpleMetaData(hook));
    }

    private List<FDBStoredRecord<Message>> saveRecords(int initialId, int totalRecords, final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaData metaData) throws Exception {
        List<FDBStoredRecord<Message>> result;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = createOrOpenRecordStore(context, metaData, path, formatVersion);
            List<FDBStoredRecord<Message>> result1 = new ArrayList<>(totalRecords);
            for (int i = initialId; i < initialId + totalRecords; i++) {
                final String someText = Strings.repeat("x", recordTextSize(splitLongRecords, i));
                final TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setStrValueIndexed(someText)
                        .setNumValue3Indexed(1415 + i * 7)
                        .build();
                result1.add(store.saveRecord(record));
            }
            result = result1;
            commit(context);
        }
        return result;
    }

    private int recordTextSize(boolean splitLongRecords, int recordId) {
        // Every 17th record is long. The number of splits increases with the record ID
        if (splitLongRecords && ((recordId % LONG_RECORD_SPACING) == 0)) {
            final int sizeInSplits = recordId / LONG_RECORD_SPACING;
            return SplitHelper.SPLIT_RECORD_SIZE * sizeInSplits + 2;
        } else {
            return 10;
        }
    }

    @Nonnull
    private static List<Tuple> getExpectedPrimaryKeys() {
        return getExpectedPrimaryKeys(i -> true);
    }

    @Nonnull
    private static List<Tuple> getExpectedPrimaryKeys(@Nonnull IntPredicate filter) {
        return IntStream.range(1, 51).filter(filter).boxed().map(Tuple::from).collect(Collectors.toList());
    }
}
