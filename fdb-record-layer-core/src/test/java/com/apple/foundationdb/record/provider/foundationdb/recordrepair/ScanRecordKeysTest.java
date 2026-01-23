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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.ParameterizedTestUtils;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the store's {@link FDBRecordStore#scanRecordKeys(byte[], ScanProperties)} implementation.
 * Heavily parameterized test that tries to create various record corruption issues and scans to ensure that the expected
 * keys can still be picked up by the scan operation.
 */
public class ScanRecordKeysTest extends FDBRecordStoreTestBase {
    private static final int ROW_LIMIT = 19;
    private static final int BYTES_LIMIT = 2000;

    public enum UseContinuations { NONE, CONTINUATIONS, BYTE_LIMIT }

    /**
     * This test class (and the implementations of the key scanner) make assumptions about the structure of the records.
     * This test method tries to ensure that these assumptions are validated with the new format version added.
     * Then, update the version below to the latest.
     */
    @Test
    void monitorFormatVersion() {
        assertEquals(FormatVersion.INCARNATION, FormatVersion.getMaximumSupportedVersion(),
                "New format version found. Please review the key scanner to ensure they still catch corruptions");
    }

    public static Stream<Arguments> splitContinuationVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("splitLongRecords"),
                Arrays.stream(UseContinuations.values()),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions"));
    }

    @ParameterizedTest
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

    @ParameterizedTest
    @MethodSource("splitContinuationVersion")
    void testIterateRecordsMissingRecord(boolean splitLongRecords, UseContinuations useContinuations, FormatVersion formatVersion, boolean storeVersions) throws Exception {
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, storeVersions);
        List<FDBStoredRecord<Message>> result = saveRecords(splitLongRecords, formatVersion, hook);
        // Delete a record
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            // Note that the primary keys start with 1, so the location is one-off when removed
            store.deleteRecord(result.get(ValidationTestUtils.RECORD_INDEX_WITH_NO_SPLITS).getPrimaryKey());
            store.deleteRecord(result.get(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey());
            store.deleteRecord(result.get(21).getPrimaryKey());
            store.deleteRecord(result.get(22).getPrimaryKey());
            store.deleteRecord(result.get(44).getPrimaryKey());
            commit(context);
        }
        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        List<Tuple> expectedKeys = getExpectedPrimaryKeys(i -> !Set.of(ValidationTestUtils.RECORD_ID_WITH_NO_SPLITS, ValidationTestUtils.RECORD_ID_WITH_THREE_SPLITS, 22, 23, 45).contains(i));
        assertEquals(expectedKeys, actualKeys);
    }

    public static Stream<Arguments> splitNumberContinuationsVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                Stream.of(0, 1, 2, 3),
                Arrays.stream(UseContinuations.values()),
                ValidationTestUtils.formatVersions(),
                ParameterizedTestUtils.booleans("storeVersions")
        );
    }

    @ParameterizedTest
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
            // Record #1 in the saved records is a short record, #33 is a long (split) record
            int recordIndex = (splitNumber == 0) ? ValidationTestUtils.RECORD_INDEX_WITH_NO_SPLITS : ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS;
            byte[] split = ValidationTestUtils.getSplitKey(store, savedRecords.get(recordIndex).getPrimaryKey(), splitNumber);
            store.ensureContextActive().clear(split);
            commit(context);
        }

        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        List<Tuple> expectedKeys;
        // When format version is below 6 and the record is a short record, deleting the only split will make the record disappear
        // When format version is 6 or 10, and we're not saving version, the same
        if ((splitNumber == 0) && (!ValidationTestUtils.versionStoredWithRecord(formatVersion) || !storeVersions)) {
            expectedKeys = getExpectedPrimaryKeys(i -> i != ValidationTestUtils.RECORD_ID_WITH_NO_SPLITS);
        } else {
            expectedKeys = getExpectedPrimaryKeys();
        }
        assertEquals(expectedKeys, actualKeys);

        // Verify that the corruption actually made the records unreadable for the normal operation
        if ((splitNumber > 0) || (ValidationTestUtils.versionStoredWithRecord(formatVersion) && storeVersions)) {
            assertRecordsCorrupted(formatVersion, hook);
        }
    }

    public static Stream<Arguments> splitContinuationFormatVersion() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("splitLongVersions"),
                Arrays.stream(UseContinuations.values()),
                ValidationTestUtils.formatVersions()
        );
    }

    @ParameterizedTest
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
    @ParameterizedTest
    @MethodSource("splitContinuationFormatVersion")
    void testIterateRecordsMixedVersions(boolean splitLongRecords, UseContinuations useContinuations, FormatVersion formatVersion) throws Exception {
        // This test changes the metadata so needs special attention to the metadata version
        final RecordMetaDataHook hook = ValidationTestUtils.getRecordMetaDataHook(splitLongRecords, false);
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addUniversalIndex(globalCountIndex());
        metaDataBuilder.addUniversalIndex(globalCountUpdatesIndex());
        hook.apply(metaDataBuilder);

        // save 25 records with no version
        saveRecords(1, 25, splitLongRecords, formatVersion, metaDataBuilder.build());
        // now save 25 with versions
        metaDataBuilder.setStoreRecordVersions(true);
        saveRecords(26, 25, splitLongRecords, formatVersion, metaDataBuilder.build());

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
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(UseContinuations.values()),
                ValidationTestUtils.formatVersions(),
                ValidationTestUtils.splitsToRemove()
        );
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
    @ParameterizedTest
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
                byte[] key = ValidationTestUtils.getSplitKey(store, result.get(ValidationTestUtils.RECORD_INDEX_WITH_THREE_SPLITS).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
                key = ValidationTestUtils.getSplitKey(store, result.get(ValidationTestUtils.RECORD_INDEX_WITH_TWO_SPLITS).getPrimaryKey(), split);
                store.ensureContextActive().clear(key);
            });
            commit(context);
        }

        // Scan records
        ScanProperties scanProperties = getScanProperties(useContinuations);
        final List<Tuple> actualKeys = scanKeys(useContinuations, formatVersion, hook, scanProperties);
        // The cases where the record will go missing altogether
        Set<Integer> keysExpectedToDisappear = new HashSet<>();
        if (ValidationTestUtils.recordWillDisappear(2, splitsToRemove, formatVersion)) {
            keysExpectedToDisappear.add(ValidationTestUtils.RECORD_ID_WITH_TWO_SPLITS);
        }
        if (ValidationTestUtils.recordWillDisappear(3, splitsToRemove, formatVersion)) {
            keysExpectedToDisappear.add(ValidationTestUtils.RECORD_ID_WITH_THREE_SPLITS);
        }

        List<Tuple> expectedKeys = getExpectedPrimaryKeys(i -> !keysExpectedToDisappear.contains(i));
        assertEquals(expectedKeys, actualKeys);

        // Verify that the corruption actually made the records unreadable for the normal operation
        // Only in cases where the corruption actually makes the record corrupt.
        if ( !(keysExpectedToDisappear.size() < 2) && splitsToRemove.equals(ValidationTestUtils.toBitSet(0b0110))) {
            assertRecordsCorrupted(formatVersion, hook);
        }
    }

    @Nullable
    private static ScanProperties getScanProperties(final UseContinuations useContinuations) {
        ExecuteProperties executeProperties;
        switch (useContinuations) {
            case NONE:
                return null;
            case CONTINUATIONS:
                executeProperties = ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(ROW_LIMIT)
                        .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                        .build();
                return new ScanProperties(executeProperties);
            case BYTE_LIMIT:
                executeProperties = ExecuteProperties.newBuilder()
                        .setScannedBytesLimit(BYTES_LIMIT)
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
        RecordCursor<Tuple> recordKeyCursor = store.scanRecordKeys(null, scanProperties);
        if (!withContinuations) {
            return recordKeyCursor.asList().get();
        } else {
            boolean done = false;
            boolean foundContinuation = false;
            List<Tuple> result = new ArrayList<>();
            while (!done) {
                RecordCursorResult<Tuple> currentRecord = recordKeyCursor.getNext();
                List<Tuple> temp = new ArrayList<>();
                while (currentRecord.hasNext()) {
                    temp.add(currentRecord.get());
                    currentRecord = recordKeyCursor.getNext();
                }
                assertTrue((temp.size() == ROW_LIMIT) ||
                        (RecordCursor.NoNextReason.BYTE_LIMIT_REACHED.equals(currentRecord.getNoNextReason())) ||
                        (RecordCursor.NoNextReason.SOURCE_EXHAUSTED.equals(currentRecord.getNoNextReason())));
                result.addAll(temp);
                RecordCursorContinuation continuation = currentRecord.getContinuation();
                if (continuation.isEnd()) {
                    done = true;
                    assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, currentRecord.getNoNextReason());
                } else {
                    recordKeyCursor = store.scanRecordKeys(continuation.toBytes(), scanProperties.with(executeProperties -> executeProperties.resetState()));
                    foundContinuation = true;
                    assertTrue(currentRecord.getNoNextReason().isLimitReached());
                }
            }
            assertTrue(foundContinuation, "Expected continuations but all records returned in first try");
            return result;
        }
    }

    private void assertRecordsCorrupted(final FormatVersion formatVersion, final RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openSimpleRecordStore(context, hook, formatVersion);
            final ExecutionException exception = assertThrows(ExecutionException.class, () -> store.scanRecords(TupleRange.allOf(null), null, ScanProperties.FORWARD_SCAN).asList().get());
            assertInstanceOf(RecordCoreException.class, exception.getCause());
        }
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

    @Nonnull
    private static List<Tuple> getExpectedPrimaryKeys() {
        return getExpectedPrimaryKeys(i -> true);
    }

    @Nonnull
    private static List<Tuple> getExpectedPrimaryKeys(@Nonnull IntPredicate filter) {
        return IntStream.range(1, 51).filter(filter).boxed().map(Tuple::from).collect(Collectors.toList());
    }
}
