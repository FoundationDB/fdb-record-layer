/*
 * ValidationTestUtils.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ValidationTestUtils {
    private static final int LONG_RECORD_SPACING = 17;
    // A few constants for records that were saved with saveRandomRecords() below
    public static final int RECORD_INDEX_WITH_NO_SPLITS = 1;
    public static final int RECORD_ID_WITH_NO_SPLITS = RECORD_INDEX_WITH_NO_SPLITS + 1;
    public static final int RECORD_INDEX_WITH_TWO_SPLITS = 16;
    public static final int RECORD_ID_WITH_TWO_SPLITS = RECORD_INDEX_WITH_TWO_SPLITS + 1;
    public static final int RECORD_INDEX_WITH_THREE_SPLITS = 33;
    public static final int RECORD_ID_WITH_THREE_SPLITS = RECORD_INDEX_WITH_THREE_SPLITS + 1;


    @Nonnull
    public static Stream<FormatVersion> formatVersions() {
        return Stream.of(
                FormatVersion.RECORD_COUNT_KEY_ADDED, // 3
                FormatVersion.SAVE_UNSPLIT_WITH_SUFFIX, // 5
                FormatVersion.SAVE_VERSION_WITH_RECORD, // 6
                FormatVersion.getMaximumSupportedVersion());
    }

    public static BitSet toBitSet(final long l) {
        return BitSet.valueOf(new long[] {l});
    }

    /**
     * Create a stream of bitsets that represent splits combinations.
     * The bitsets represent combinations of splits to be removed by the test:
     * bit 0 is the version (Split #-1)
     * bits 1-3 are the 1st (#1), 2nd (#2) or 3rd (#3) splits of a long (split) record
     * @return a stream of splits combinations
     */
    @Nonnull
    public static Stream<BitSet> splitsToRemove() {
        return LongStream.range(1, 16).mapToObj(l -> toBitSet(l));
    }

    /**
     * Return TRUE if a record matching the given parameters will disappear if the given splits are removed.
     * This is useful to know since a gone record will not show up in the validation results.
     * A record will disappear if all of its splits  and version are deleted, or if we are not storing version with the
     * record and all of its data is deleted.
     * @param numOfSplits number of splits for the record (2 or 3)
     * @param splitsToRemove the bitset of splits to be removed
     * @param formatVersion the format version for the store
     * @return TRUE if the record will be gone if the splits are removed
     */
    public static boolean recordWillDisappear(int numOfSplits, BitSet splitsToRemove, FormatVersion formatVersion) {
        final BitSet allThreeSplits = toBitSet(0b1111);
        final BitSet allThreeSplitsWithoutVersion = toBitSet(0b1110);
        final BitSet allTwoSplits = toBitSet(0b0111);
        final BitSet allTwoSplitsWithoutVersion = toBitSet(0b0110);
        final boolean storingVersion = versionStoredWithRecord(formatVersion);
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

    /**
     * Return TRUE if removing the given splits will leave the record in valid state.
     * The record will remain valid if we are removing non-existing splits or a version when the version is stored elsewhere.
     * @param numOfSplits number of splits for the record (2 or 3)
     * @param splitsToRemove the bitset of splits to be removed
     * @param formatVersion the format version for the store
     * @return TRUE if the record will be valid if the splits are removed
     */
    public static boolean recordWillRemainValid(int numOfSplits, BitSet splitsToRemove, FormatVersion formatVersion) {
        final BitSet thirdSplitOnly = toBitSet(0b1000);
        final BitSet thirdSplitAndVerion = toBitSet(0b1001);
        final BitSet versionSplitOnly = toBitSet(0b0001);
        if ((numOfSplits == 2) && thirdSplitOnly.equals(splitsToRemove)) {
            // removing non-existent 3rd split
            return true;
        }
        if ((numOfSplits == 2) && thirdSplitAndVerion.equals(splitsToRemove) && !versionStoredWithRecord(formatVersion)) {
            // version stored elsewhere and we remove version and non-existent split
            return true;
        }
        if (versionSplitOnly.equals(splitsToRemove) && !versionStoredWithRecord(formatVersion)) {
            // version stored elsewhere and we only remove the version
            return true;
        }
        return false;
    }

    /**
     * Return TRUE if removing the given splits will leave the record without a version but otherwise valid.
     * @param numOfSplits number of splits for the record (2 or 3)
     * @param splitsToRemove the bitset of splits to be removed
     * @param formatVersion the format version for the store
     * @return TRUE if the record will be valid but without a version
     */
    public static boolean recordWillHaveVersionMissing(int numOfSplits, BitSet splitsToRemove, FormatVersion formatVersion) {
        final BitSet thirdSplitAndVersion = toBitSet(0b1001);
        final BitSet versionSplitOnly = toBitSet(0b0001);
        if (!versionStoredWithRecord(formatVersion)) {
            return false;
        }

        if ((numOfSplits == 2) && (versionSplitOnly.equals(splitsToRemove) || thirdSplitAndVersion.equals(splitsToRemove))) {
            // removing version or version and non-existent split
            return true;
        }
        if ((numOfSplits == 3) && versionSplitOnly.equals(splitsToRemove)) {
            // remove version and no split
            return true;
        }
        return false;
    }

    @Nonnull
    public static FDBRecordStoreTestBase.RecordMetaDataHook getRecordMetaDataHook(final boolean splitLongRecords) {
        return getRecordMetaDataHook(splitLongRecords, true);
    }

    @Nonnull
    public static FDBRecordStoreTestBase.RecordMetaDataHook getRecordMetaDataHook(final boolean splitLongRecords, final boolean storeVersions) {
        return metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            metaData.setStoreRecordVersions(storeVersions);
            // index cannot be used with large fields
            metaData.removeIndex("MySimpleRecord$str_value_indexed");
        };
    }

    public static byte[] getSplitKey(FDBRecordStore store, Tuple primaryKey, int splitNumber) {
        final Tuple pkAndSplit = primaryKey.add(splitNumber);
        return store.recordsSubspace().pack(pkAndSplit);
    }

    public static boolean versionStoredWithRecord(final FormatVersion formatVersion) {
        return formatVersion.isAtLeast(FormatVersion.SAVE_VERSION_WITH_RECORD);
    }


    public static List<FDBStoredRecord<Message>> saveRecords(FDBRecordStore store, boolean splitLongRecords) throws Exception {
        return saveRecords(store, 1, 50, splitLongRecords);
    }

    public static List<FDBStoredRecord<Message>> saveRecords(FDBRecordStore store, int initialId, int totalRecords, final boolean splitLongRecords) throws Exception {
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
        return result1;
    }

    public static void assertInvalidResults(List<RecordRepairResult> invalidResults, int expectedResults, @Nullable Predicate<RecordRepairResult> predicate) {
        Assertions.assertThat(invalidResults)
                .hasSize(expectedResults)
                .allMatch(predicate);
    }

    public static void assertNoInvalidResults(List<RecordRepairResult> invalidResults) {
        Assertions.assertThat(invalidResults).isEmpty();
    }

    public static void assertCompleteResults(final RepairValidationResults repairResults, int numRecords) {
        Assertions.assertThat(repairResults.isComplete()).isTrue();
        Assertions.assertThat(repairResults.getCaughtException()).isNull();
        Assertions.assertThat(repairResults.getInvalidResults().size() + repairResults.getValidResultCount()).isEqualTo(numRecords);
    }

    public static void assertRepairStats(RepairStatsResults stats, int numValidResults) {
        assertRepairStats(stats, numValidResults, 0, null, 0, null);
    }

    public static void assertRepairStats(RepairStatsResults stats, int numValidResults, int numResults1, String codeResults1) {
        assertRepairStats(stats, numValidResults, numResults1, codeResults1, 0, null);
    }

    public static void assertRepairStats(RepairStatsResults stats, int numValidResults, int numResults1, String codeResults1, int numResults2, String codeResults2) {
        int numEntries = 0;
        if (numValidResults > 0) {
            Assertions.assertThat(stats.getStats()).containsEntry(RecordRepairResult.CODE_VALID, numValidResults);
            numEntries++;
        }
        if (numResults1 > 0) {
            Assertions.assertThat(stats.getStats()).containsEntry(codeResults1, numResults1);
            numEntries++;
        }
        if (numResults2 > 0) {
            Assertions.assertThat(stats.getStats()).containsEntry(codeResults2, numResults2);
            numEntries++;
        }
        Assertions.assertThat(stats.getExceptionCaught()).isNull();
        Assertions.assertThat(stats.getStats()).hasSize(numEntries);
    }

    private static int recordTextSize(boolean splitLongRecords, int recordId) {
        // Every 17th record is long. The number of splits increases with the record ID
        if (splitLongRecords && ((recordId % LONG_RECORD_SPACING) == 0)) {
            final int sizeInSplits = recordId / LONG_RECORD_SPACING;
            return SplitHelper.SPLIT_RECORD_SIZE * sizeInSplits + 2;
        } else {
            return 10;
        }
    }
}
