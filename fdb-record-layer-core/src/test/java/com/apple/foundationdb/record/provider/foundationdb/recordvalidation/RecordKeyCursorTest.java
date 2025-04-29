/*
 * RecordKeyCursorTest.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
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
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordKeyCursorTest extends FDBRecordStoreTestBase {
    public enum UseContinuations { NONE, CONTINUATIONS, BYTE_LIMIT }

    public static Stream<Arguments> splitContinuationVersion() {
        return Stream.of(true, false)
                .flatMap(split -> Arrays.stream(UseContinuations.values())
                        .flatMap(useContinuations -> ValidationTestUtils.formatVersions()
                                .flatMap(formatVersion -> Stream.of(true, false)
                                        .map(storeVersions -> Arguments.of(split, useContinuations, formatVersion, storeVersions)))));
    }

    @ParameterizedTest(name = "testIterateRecordsNoIssue [splitLongRecords = {0}, useContinuations = {1}, formatVersion = {2}], storeVersions = {3}")
    @MethodSource("splitContinuationVersion")
    void testIterateRecordsNoIssue(boolean splitLongRecords, UseContinuations useContinuations, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        saveRecords(splitLongRecords, formatVersion, hook);
        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        List<Tuple> expectedKeys = getExpectedPrimaryKeys();
        assertEquals(expectedKeys, actualKeys);
    }

    @ParameterizedTest(name = "testIterateRecordsMissingRecord [splitLongRecords = {0}, useContinuations = {1}, formatVersion = {2}, storeVersions = {3}")
    @MethodSource("splitContinuationVersion")
    void testIterateRecordsMissingRecord(boolean splitLongRecords, UseContinuations useContinuations, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Note that the primary keys start with 1, so the location is one-off when removed
            store.deleteRecord(result.get(16).getPrimaryKey());
            store.deleteRecord(result.get(21).getPrimaryKey());
            store.deleteRecord(result.get(22).getPrimaryKey());
            store.deleteRecord(result.get(70).getPrimaryKey());
            commit(context);
        }
        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        List<Tuple> expectedKeys = getExpectedPrimaryKeys(i -> !Set.of(17, 22, 23, 71).contains(i));
        assertEquals(expectedKeys, actualKeys);
    }

    public static Stream<Arguments> splitNumberContinuationsVersion() {
        return Stream.of(0, 1, 2, 3)
                .flatMap(splitNumber -> Stream.of(UseContinuations.values())
                        .flatMap(useContinuations -> ValidationTestUtils.formatVersions()
                                .flatMap(formatVersion -> Stream.of(true, false)
                                        .map(storeVersions -> Arguments.of(splitNumber, useContinuations, formatVersion, storeVersions)))));
    }

    @ParameterizedTest(name = "testIterateRecordsMissingSplit [splitNumber = {0}, useContinuations = {1}, formatVersion = {2}, storeVersions = {3}]")
    @MethodSource("splitNumberContinuationsVersion")
    void testIterateRecordsMissingSplit(int splitNumber, UseContinuations useContinuations, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        boolean splitLongRecords = true;

        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> savedRecords = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a split
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // If operating on the short record, #0 is the only split
            // If operating on the long record, splits can be 1,2,3
            // Use splitNumber to decide which record to operate on.
            // Record #1 in the saved records is a short record, #16 is a long (split) record
            int recordNumber = (splitNumber == 0) ? 1 : 16;
            byte[] split = ValidationTestUtils.getSplitKey(store, savedRecords.get(recordNumber).getPrimaryKey(), splitNumber);
            store.ensureContextActive().clear(split);
            commit(context);
        }

        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        List<Tuple> expectedKeys;
        // When format version is 3 and the record is a short record, deleting the only split will make the record disappear
        // When format version is 6 or 10, and we're not saving version, the same
        if ((splitNumber == 0) && ((formatVersion == FormatVersion.RECORD_COUNT_KEY_ADDED) || !storeVersions)) {
            expectedKeys = getExpectedPrimaryKeys(i -> i != 2);
        } else {
            expectedKeys = getExpectedPrimaryKeys();
        }
        assertEquals(expectedKeys, actualKeys);
    }

    public static Stream<Arguments> splitContinuationFormatVersion() {
        return Stream.of(true, false)
                .flatMap(split -> Arrays.stream(UseContinuations.values())
                        .flatMap(useContinuations -> ValidationTestUtils.formatVersions()
                                .map(formatVersion -> Arguments.of(split, useContinuations, formatVersion))));
    }

    @ParameterizedTest(name = "testIterateRecordsMissingVersion [splitLongRecords = {0}, useContinuations = {1}, formatVersion = {2}]")
    @MethodSource("splitContinuationFormatVersion")
    void testIterateRecordsMissingVersion(boolean splitLongRecords, UseContinuations useContinuations, FormatVersion formatVersion) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, true);
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

        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        List<Tuple> expectedKeys = getExpectedPrimaryKeys();
        assertEquals(expectedKeys, actualKeys);
    }

    /**
     * A test that has a store with mixed versions: Some record with and some without.
     *
     * @param splitLongRecords whether to split long versions
     * @param useContinuations whether to use continuations
     * @param formatVersion what format version to use
     */
    @ParameterizedTest(name = "testIterateRecordsMixedVersions [splitLongRecords = {0}, useContinuations = {1}, formatVersion = {2}]")
    @MethodSource("splitContinuationFormatVersion")
    void testIterateRecordsMixedVersions(boolean splitLongRecords, UseContinuations useContinuations, FormatVersion formatVersion) throws Exception {
        // This test changes the metadata so needs special attention to the metadata version
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, false);
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(globalCountIndex());
        metaDataBuilder.addUniversalIndex(globalCountUpdatesIndex());
        hook.apply(metaDataBuilder);

        // save 50 records with no version
        saveRecords(1, 50, splitLongRecords, formatVersion, metaDataBuilder.build());
        // now save 50 with versions
        metaDataBuilder.setStoreRecordVersions(true);
        saveRecords(51, 50, splitLongRecords, formatVersion, metaDataBuilder.build());

        // Scan records
        final List<Tuple> actualKeys;
        ScanProperties scanProperties = getScanProperties(useContinuations);
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = createOrOpenRecordStore(context, metaDataBuilder.build(), path, formatVersion);
            actualKeys = scanKeys(store, scanProperties, useContinuations != UseContinuations.NONE);
        }
        List<Tuple> expectedKeys = getExpectedPrimaryKeys();
        assertEquals(expectedKeys, actualKeys);
    }

    // list of arguments for version and a bitset that has all the combinations of 4 bits set (except all unset)
    private static Stream<Arguments> continuationVersionAndBitset() {
        return Arrays.stream(UseContinuations.values())
                .flatMap(useContinuations -> ValidationTestUtils.formatVersions()
                        .flatMap(version -> ValidationTestUtils.splitsToRemove()
                                .map(bitset -> Arguments.of(useContinuations, version, bitset))));
    }

    /**
     * A test that runs through all the combinations of 4-bits and erases a split for every bit that is set.
     * This simulated all the combinations of splits that can go missing for a record with 3 splits
     * (version, splits 1-3).
     *
     * @param useContinuations whether to use continuations for the iteration
     * @param formatVersion the version format
     * @param splitsToRemove the splits to remove
     */
    @ParameterizedTest(name = "testIterateRecordCombinationSplitMissing [useContinuations = {0}, formatVersion = {1}, splitsToRemove = {2}]")
    @MethodSource("continuationVersionAndBitset")
    void testIterateRecordCombinationSplitMissing(UseContinuations useContinuations, FormatVersion formatVersion, BitSet splitsToRemove) throws Exception {
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
                // record #16 is a long record
                byte[] key = ValidationTestUtils.getSplitKey(store, result.get(16).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
            });
            commit(context);
        }

        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        // The cases where the record will go missing altogether
        List<Tuple> expectedKeys;
        if (splitsToRemove.equals(ValidationTestUtils.toBitSet(0b1111)) ||
                ((formatVersion == FormatVersion.RECORD_COUNT_KEY_ADDED) && splitsToRemove.equals(ValidationTestUtils.toBitSet(0b1110)))) {
            expectedKeys = getExpectedPrimaryKeys(i -> (i != 17));
        } else {
            expectedKeys = getExpectedPrimaryKeys();
        }
        assertEquals(expectedKeys, actualKeys);
    }

    @Nullable
    private static ScanProperties getScanProperties(final UseContinuations useContinuations) {
        ExecuteProperties executeProperties;
        switch (useContinuations) {
            case NONE:
                return null;
            case CONTINUATIONS:
                executeProperties = ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(19)
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

    private List<Tuple> scanKeys(final UseContinuations useContinuations, FormatVersion formatVersion, final RecordMetaDataHook hook, final ScanProperties scanProperties) throws Exception {
        final List<Tuple> actualKeys;
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            actualKeys = scanKeys(store, scanProperties, useContinuations != UseContinuations.NONE);
            context.commit();
        }
        return actualKeys;
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
            boolean foundContinuation = false;
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
                    assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, currentRecord.getNoNextReason());
                } else {
                    recordKeyCursor = store.scanRecordKeys(TupleRange.allOf(null), continuation.toBytes(), scanProperties.with(executeProperties -> executeProperties.resetState()));
                    foundContinuation = true;
                    assertTrue(currentRecord.getNoNextReason().isLimitReached());
                }
            }
            assertTrue(foundContinuation, "Expected continuations but all records returned in first try");
            return result;
        }
    }

    private List<FDBStoredRecord<Message>> saveRecords(final boolean splitLongRecords, FormatVersion formatVersion, final RecordMetaDataHook hook) throws Exception {
        return saveRecords(1, 100, splitLongRecords, formatVersion, hook);
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
                final String someText = (splitLongRecords && ((i % 17) == 0)) ? Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE * 2 + 2) : "some text (short)";
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

    @Nonnull
    private static List<Tuple> getExpectedPrimaryKeys() {
        return getExpectedPrimaryKeys(i -> true);
    }

    @Nonnull
    private static List<Tuple> getExpectedPrimaryKeys(@Nonnull IntPredicate filter) {
        return IntStream.range(1, 101).filter(filter).boxed().map(Tuple::from).collect(Collectors.toList());
    }
}
