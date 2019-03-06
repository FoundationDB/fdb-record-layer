/*
 * SplitHelperTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for checking the validity of the "split helper" utility class that handles breaking
 * records across key-value pairs and putting them back together again.
 */
@Tag(Tags.RequiresFDB)
public class SplitHelperTest extends FDBRecordStoreTestBase {

    // From the traditional nursery rhyme
    private static final byte[] HUMPTY_DUMPTY =
            ("Humpty Dumpty sat on a wall,\n"
            + "Humpty Dumpty had a great fall\n"
            + "All the king's horses and all the king's men\n"
            + "Couldn't put Humpty Dumpty together again.\n").getBytes(Charsets.UTF_8);

    private static final int MEDIUM_COPIES = 5;
    private static final int MEDIUM_LEGNTH = HUMPTY_DUMPTY.length * MEDIUM_COPIES;
    private static final int LONG_LENGTH = HUMPTY_DUMPTY.length * 1_000; // requires 1 split
    private static final int VERY_LONG_LENGTH = HUMPTY_DUMPTY.length * 2_000; // requires 2 splits
    private static final byte[] SHORT_STRING = HUMPTY_DUMPTY;
    private static final byte[] MEDIUM_STRING;
    private static final byte[] LONG_STRING;
    private static final byte[] VERY_LONG_STRING;

    private Subspace subspace;

    static {
        ByteBuffer mediumBuffer = ByteBuffer.allocate(MEDIUM_LEGNTH);
        for (int i = 0; i < MEDIUM_COPIES; i++) {
            mediumBuffer.put(HUMPTY_DUMPTY);
        }
        MEDIUM_STRING = mediumBuffer.array();
        ByteBuffer longBuffer = ByteBuffer.allocate(LONG_LENGTH);
        while (longBuffer.position() < LONG_LENGTH - HUMPTY_DUMPTY.length) {
            longBuffer.put(HUMPTY_DUMPTY);
        }
        LONG_STRING = longBuffer.array();
        ByteBuffer veryLongBuffer = ByteBuffer.allocate(VERY_LONG_LENGTH);
        while (veryLongBuffer.position() < VERY_LONG_LENGTH - HUMPTY_DUMPTY.length) {
            veryLongBuffer.put(HUMPTY_DUMPTY);
        }
        VERY_LONG_STRING = veryLongBuffer.array();
    }

    @BeforeEach
    public void setSubspace() {
        try (FDBRecordContext context = openContext()) {
            subspace = path.toSubspace(context);
        }
    }

    @Nonnull
    public static Stream<Arguments> splitAndSuffixArgs() {
        // Arguments: splitLongRecords, omitUnsplitSuffix
        // Note that "true", "true" is not valid
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false)
        );
    }

    @Nonnull
    public static Stream<Arguments> limitsAndReverseArgs() {
        List<Integer> limits = Arrays.asList(1, 2, 7, Integer.MAX_VALUE);
        return limits.stream()
                .flatMap(returnLimit -> limits.stream()
                        .flatMap(readLimit -> Stream.of(Arguments.of(returnLimit, readLimit, false), Arguments.of(returnLimit, readLimit, true))));
    }

    private <E extends Throwable> SplitHelper.SizeInfo saveUnsuccessfully(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
                                                                          @Nullable FDBRecordVersion version, boolean splitLongRecords, boolean omitUnsplitSuffix,
                                                                          @Nullable FDBStoredSizes previousSizeInfo,
                                                                          @Nonnull Class<E> errClazz, @Nonnull String errMessage) {
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        E e = assertThrows(errClazz,
                () -> SplitHelper.saveWithSplit(context, subspace, key, serialized, version, splitLongRecords, omitUnsplitSuffix, previousSizeInfo != null, previousSizeInfo, sizeInfo));
        assertThat(e.getMessage(), containsString(errMessage));

        assertEquals(0, sizeInfo.getKeyCount());
        assertEquals(0, sizeInfo.getKeySize());
        assertEquals(0, sizeInfo.getValueSize());
        assertThat(sizeInfo.isVersionedInline(), is(false));

        int count = KeyValueCursor.Builder.withSubspace(subspace.subspace(key))
                .setContext(context)
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build()
                .getCount()
                .join();
        assertEquals(0, previousSizeInfo == null ? 0 : previousSizeInfo.getKeyCount());

        return sizeInfo;
    }

    private SplitHelper.SizeInfo saveSuccessfully(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
                                                  @Nullable FDBRecordVersion version, boolean splitLongRecords, boolean omitUnsplitSuffix,
                                                  @Nullable FDBStoredSizes previousSizeInfo) {
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        SplitHelper.saveWithSplit(context, subspace, key, serialized, version, splitLongRecords, omitUnsplitSuffix, previousSizeInfo != null, previousSizeInfo, sizeInfo);

        int dataKeyCount = (serialized.length - 1) / SplitHelper.SPLIT_RECORD_SIZE + 1;
        boolean isSplit = dataKeyCount > 1;
        int keyCount = dataKeyCount;
        if (version != null) {
            keyCount += 1;
        }
        int keySize = (subspace.pack().length + key.pack().length) * keyCount;
        assertEquals(isSplit, sizeInfo.isSplit());
        assertEquals(keyCount, sizeInfo.getKeyCount());
        if (!omitUnsplitSuffix || splitLongRecords) {
            // Add in the the counters the split points.
            if (!isSplit) {
                keySize += 1; // As 0 requires 1 byte when Tuple packed
            } else {
                keySize += dataKeyCount * 2; // As each split point is two bytes when tuple packed
            }
        }
        if (version != null) {
            keySize += 2;
        }
        int valueSize = serialized.length + (version != null ? 1 + FDBRecordVersion.VERSION_LENGTH : 0);
        assertEquals(keySize, sizeInfo.getKeySize());
        assertEquals(valueSize, sizeInfo.getValueSize());
        assertEquals(version != null, sizeInfo.isVersionedInline());

        final Subspace keySubspace = subspace.subspace(key);
        RecordCursorIterator<KeyValue> kvCursor = KeyValueCursor.Builder.withSubspace(keySubspace)
                .setContext(context)
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build()
                .asIterator();
        List<Long> indexes = new ArrayList<>(keyCount);
        byte[] versionBytes = null;
        byte[] valueBytes = null;
        while (kvCursor.hasNext()) {
            KeyValue kv = kvCursor.next();
            Tuple suffix = keySubspace.unpack(kv.getKey());
            if (omitUnsplitSuffix) {
                assertThat(suffix.isEmpty(), is(true));
                valueBytes = kv.getValue();
            } else {
                Long index = suffix.getLong(0);
                indexes.add(index);
                if (index == SplitHelper.RECORD_VERSION) {
                    versionBytes = kv.getValue();
                } else {
                    if (valueBytes == null) {
                        valueBytes = kv.getValue();
                    } else {
                        valueBytes = ByteArrayUtil.join(valueBytes, kv.getValue());
                    }
                }
            }
        }
        List<Long> expectedIndexes;
        if (omitUnsplitSuffix) {
            expectedIndexes = Collections.emptyList();
        } else {
            expectedIndexes = new ArrayList<>(keyCount);
            if (version != null && version.isComplete()) {
                expectedIndexes.add(SplitHelper.RECORD_VERSION);
            }
            if (!isSplit) {
                expectedIndexes.add(SplitHelper.UNSPLIT_RECORD);
            } else {
                LongStream.range(SplitHelper.START_SPLIT_RECORD, SplitHelper.START_SPLIT_RECORD + dataKeyCount)
                        .forEach(expectedIndexes::add);
            }
        }
        assertEquals(expectedIndexes, indexes);

        assertNotNull(valueBytes);
        assertArrayEquals(serialized, valueBytes);

        if (version != null) {
            if (!version.isComplete()) {
                assertNull(versionBytes);
            } else {
                assertNotNull(versionBytes);
                assertEquals(version, FDBRecordVersion.fromVersionstamp(Tuple.fromBytes(versionBytes).getVersionstamp(0)));
            }
        } else {
            assertNull(versionBytes);
        }

        return sizeInfo;
    }

    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
                                               @Nullable FDBRecordVersion version, boolean splitLongRecords, boolean omitUnsplitSuffix,
                                               @Nullable FDBStoredSizes previousSizeInfo) {
        if (omitUnsplitSuffix && version != null) {
            return saveUnsuccessfully(context, key, serialized, version, false, omitUnsplitSuffix, previousSizeInfo,
                    RecordCoreArgumentException.class, "Cannot include version");
        } else if (!splitLongRecords && serialized.length > SplitHelper.SPLIT_RECORD_SIZE) {
            return saveUnsuccessfully(context, key, serialized, version, false, omitUnsplitSuffix, previousSizeInfo,
                    RecordCoreException.class, "Record is too long");
        } else {
            return saveSuccessfully(context, key, serialized, version, splitLongRecords, omitUnsplitSuffix, previousSizeInfo);
        }
    }

    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
                                               @Nullable FDBRecordVersion version, boolean splitLongRecords, boolean omitUnsplitSuffix) {
        return saveWithSplit(context, key, serialized, version, splitLongRecords, omitUnsplitSuffix, null);
    }

    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized, boolean splitLongRecords, boolean omitUnsplitSuffix,
                                               @Nullable FDBStoredSizes previousSizeInfo) {
        return saveWithSplit(context, key, serialized, null, splitLongRecords, omitUnsplitSuffix, previousSizeInfo);
    }

    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized, boolean splitLongRecords, boolean omitUnsplitSuffix) {
        return saveWithSplit(context, key, serialized, null, splitLongRecords, omitUnsplitSuffix);
    }

    @MethodSource("splitAndSuffixArgs")
    @ParameterizedTest(name = "saveWithSplit [splitLongRecords = {0}, omitUnsplitSuffix = {1}]")
    public void saveWithSplit(boolean splitLongRecords, boolean omitUnsplitSuffix) throws Exception {
        try (FDBRecordContext context = openContext()) {
            // No version
            FDBStoredSizes sizes1 = saveWithSplit(context, Tuple.from(1066L), SHORT_STRING, splitLongRecords, omitUnsplitSuffix);
            FDBStoredSizes sizes2 = saveWithSplit(context, Tuple.from(1415L), LONG_STRING, splitLongRecords, omitUnsplitSuffix);
            FDBStoredSizes sizes3 = saveWithSplit(context, Tuple.from(1776L), VERY_LONG_STRING, splitLongRecords, omitUnsplitSuffix);

            // Save over some things using the previous split points
            if (splitLongRecords) {
                saveWithSplit(context, Tuple.from(1066L), VERY_LONG_STRING, true, omitUnsplitSuffix, sizes1);
                saveWithSplit(context, Tuple.from(1776), LONG_STRING, true, omitUnsplitSuffix, sizes3);
            }
            saveWithSplit(context, Tuple.from(1415L), SHORT_STRING, splitLongRecords, omitUnsplitSuffix, sizes2);

            commit(context);
        }
    }

    @MethodSource("splitAndSuffixArgs")
    @ParameterizedTest(name = "saveWithSplitAndIncompleteVersions [splitLongRecords = {0}, omitUnsplitSuffix = {1}]")
    public void saveWithSplitAndIncompleteVersions(boolean splitLongRecords, boolean omitUnsplitSuffix) throws Exception {
        final byte[] versionstamp;
        final byte[] globalVersion = "whiteroses".getBytes(Charsets.US_ASCII);
        try (FDBRecordContext context = openContext()) {
            // With incomplete version
            saveWithSplit(context, Tuple.from(962L), SHORT_STRING, FDBRecordVersion.incomplete(context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);
            saveWithSplit(context, Tuple.from(967L), LONG_STRING, FDBRecordVersion.incomplete(context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);
            saveWithSplit(context, Tuple.from(996L), VERY_LONG_STRING, FDBRecordVersion.incomplete(context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);

            commit(context);
            versionstamp = context.getVersionStamp();
            if (!omitUnsplitSuffix) {
                assertNotNull(versionstamp);
            } else {
                assertNull(versionstamp);
            }
        }
        if (!omitUnsplitSuffix) {
            try (FDBRecordContext context = openContext()) {
                List<Pair<Tuple, FDBRecordVersion>> keys = Arrays.asList(
                        Pair.of(Tuple.from(962L), FDBRecordVersion.complete(versionstamp, 0)),
                        Pair.of(Tuple.from(967L), FDBRecordVersion.complete(versionstamp, 1)),
                        Pair.of(Tuple.from(996L), FDBRecordVersion.complete(versionstamp, 2))
                );

                for (int i = 0; i < keys.size(); i++) {
                    Tuple key = keys.get(i).getLeft();
                    FDBRecordVersion version = keys.get(i).getRight();
                    byte[] versionBytes = context.ensureActive().get(subspace.pack(key.add(SplitHelper.RECORD_VERSION))).join();
                    if (i % 3 == 0 || splitLongRecords) {
                        assertNotNull(versionBytes);
                        FDBRecordVersion deserializedVersion = SplitHelper.unpackVersion(versionBytes);
                        assertEquals(version, deserializedVersion);
                    } else {
                        assertNull(versionBytes);
                    }
                }
            }
        }
    }

    @MethodSource("splitAndSuffixArgs")
    @ParameterizedTest(name = "saveWithSplitAndCompleteVersion [splitLongRecords = {0}, omitUnsplitSuffix = {1}]")
    public void saveWithSplitAndCompleteVersions(boolean splitLongRecords, boolean omitUnsplitSuffix) throws Exception {
        try (FDBRecordContext context = openContext()) {
            // With complete version
            byte[] globalVersion = "karlgrosse".getBytes(Charsets.US_ASCII);
            saveWithSplit(context, Tuple.from(800L), SHORT_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);
            saveWithSplit(context, Tuple.from(813L), LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);
            saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);

            // Save over the records *without* using the previous size info
            saveWithSplit(context, Tuple.from(800L), SHORT_STRING, splitLongRecords, omitUnsplitSuffix);
            saveWithSplit(context, Tuple.from(813L), LONG_STRING, splitLongRecords, omitUnsplitSuffix);
            saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, splitLongRecords, omitUnsplitSuffix);

            FDBStoredSizes sizes4 = saveWithSplit(context, Tuple.from(800L), SHORT_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);
            FDBStoredSizes sizes5 = saveWithSplit(context, Tuple.from(813L), LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);
            FDBStoredSizes sizes6 = saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), splitLongRecords, omitUnsplitSuffix);

            // Save over the records *with* using the previous size info
            saveWithSplit(context, Tuple.from(800L), SHORT_STRING, splitLongRecords, omitUnsplitSuffix, sizes4);
            saveWithSplit(context, Tuple.from(813L), LONG_STRING, splitLongRecords, omitUnsplitSuffix, sizes5);
            saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, splitLongRecords, omitUnsplitSuffix, sizes6);

            commit(context);
        }
    }

    @Nonnull
    private FDBStoredSizes writeDummyRecord(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nullable FDBRecordVersion version, int splits, boolean omitUnsplitSuffix) {
        final Transaction tr = context.ensureActive();
        SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        if (version != null) {
            assertThat(omitUnsplitSuffix, is(false));
            sizeInfo.setVersionedInline(true);
            byte[] keyBytes = subspace.pack(key.add(SplitHelper.RECORD_VERSION));
            byte[] valueBytes = SplitHelper.packVersion(version);
            tr.set(keyBytes, valueBytes);
            sizeInfo.add(keyBytes, valueBytes);
        }
        if (splits == 1) {
            if (omitUnsplitSuffix) {
                byte[] keyBytes = subspace.pack(key);
                sizeInfo.add(keyBytes, SHORT_STRING);
                tr.set(keyBytes, SHORT_STRING);
            } else {
                byte[] keyBytes = subspace.pack(key.add(SplitHelper.UNSPLIT_RECORD));
                sizeInfo.add(keyBytes, SHORT_STRING);
                tr.set(keyBytes, SHORT_STRING);
            }
            sizeInfo.setSplit(false);
        } else {
            for (int i = 0; i < splits; i++) {
                byte[] keyBytes = subspace.pack(key.add(SplitHelper.START_SPLIT_RECORD + i));
                sizeInfo.add(keyBytes, SHORT_STRING);
                tr.set(keyBytes, SHORT_STRING);
            }
            sizeInfo.setSplit(true);
        }
        return sizeInfo;
    }

    @Nonnull
    private FDBStoredSizes writeDummyRecord(@Nonnull FDBRecordContext context, @Nonnull Tuple key, int splits, boolean omitUnsplitSuffix) {
        return writeDummyRecord(context, key, null, splits, omitUnsplitSuffix);
    }

    @Nonnull
    private FDBStoredSizes writeDummyRecord(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nonnull FDBRecordVersion version, int splits) {
        return writeDummyRecord(context, key, version, splits, false);
    }

    private void deleteSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key,
                             boolean splitLongRecords, boolean omitUnsplitSuffix,
                             @Nullable FDBStoredSizes sizeInfo) {
        SplitHelper.deleteSplit(context, subspace, key, splitLongRecords, omitUnsplitSuffix, sizeInfo != null, sizeInfo);
        int count = KeyValueCursor.Builder.withSubspace(subspace.subspace(key))
                .setContext(context)
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build()
                .getCount()
                .join();
        assertEquals(0, count);
    }

    @MethodSource("splitAndSuffixArgs")
    @ParameterizedTest(name = "deleteWithSplit [splitLongRecords = {0}, omitUnsplitSuffix = {1}]")
    public void deleteWithSplit(boolean splitLongRecords, boolean omitUnsplitSuffix) throws Exception {
        try (FDBRecordContext context = openContext()) {
            // Delete unsplit with the size info
            FDBStoredSizes sizes1 = writeDummyRecord(context, Tuple.from(-660L), 1, omitUnsplitSuffix);
            deleteSplit(context, Tuple.from(-660L), splitLongRecords, omitUnsplitSuffix, sizes1);

            // Delete unsplit without the size info
            writeDummyRecord(context, Tuple.from(-581L), 1, omitUnsplitSuffix);
            deleteSplit(context, Tuple.from(-581L), splitLongRecords, omitUnsplitSuffix, null);

            if (splitLongRecords) {
                // Delete split with the size info
                FDBStoredSizes sizes3 = writeDummyRecord(context, Tuple.from(-549L), 5, omitUnsplitSuffix);
                deleteSplit(context, Tuple.from(-549L), true, omitUnsplitSuffix, sizes3);

                // Delete split without the size info
                writeDummyRecord(context, Tuple.from(-510L), 5, omitUnsplitSuffix);
                deleteSplit(context, Tuple.from(-510L), true, omitUnsplitSuffix, null);
            }

            commit(context);
        }
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "deleteWithSplitAndVersion [splitLongRecords = {0}]")
    public void deleteWithSplitAndVersion(TestHelpers.BooleanEnum splitLongRecordsEnum) throws Exception {
        final boolean splitLongRecords = splitLongRecordsEnum.toBoolean();
        final byte[] globalVersion = "chrysan_th".getBytes(Charsets.US_ASCII);
        try (FDBRecordContext context = openContext()) {
            // Delete unsplit with size info
            FDBStoredSizes sizes1 = writeDummyRecord(context, Tuple.from(-475L), FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), 1);
            deleteSplit(context, Tuple.from(-475L), splitLongRecords, false, sizes1);

            // Delete unsplit without size info
            writeDummyRecord(context, Tuple.from(-392L), FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), 1);
            deleteSplit(context, Tuple.from(-392L), splitLongRecords, false, null);

            // Delete split with size info
            FDBStoredSizes sizes3 = writeDummyRecord(context, Tuple.from(-475L), FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), 5);
            deleteSplit(context, Tuple.from(-475L), splitLongRecords, false, sizes3);

            // Delete split without size info
            writeDummyRecord(context, Tuple.from(-475L), FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), 5);
            deleteSplit(context, Tuple.from(-475L), splitLongRecords, false, null);
        }
    }

    @FunctionalInterface
    private interface LoadRecordFunction {
        FDBRawRecord load(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nullable FDBStoredSizes sizes, @Nullable byte[] expectedContents, @Nullable FDBRecordVersion version);

        default FDBRawRecord load(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nullable FDBStoredSizes sizes, @Nullable byte[] expectedContents) {
            return load(context, key, sizes, expectedContents, null);
        }
    }

    private void loadSingleRecords(boolean splitLongRecords, boolean omitUnsplitSuffix, @Nonnull LoadRecordFunction loadRecordFunction) throws Exception {
        final byte[] globalVersion = "-hastings-".getBytes(Charsets.US_ASCII);
        try (FDBRecordContext context = openContext()) {
            // No record
            loadRecordFunction.load(context, Tuple.from(1042L), null, null);

            // One unsplit record
            FDBStoredSizes sizes1 = writeDummyRecord(context, Tuple.from(1066L), 1, omitUnsplitSuffix);
            assertThat(sizes1.isSplit(), is(false));
            loadRecordFunction.load(context, Tuple.from(1066L), sizes1, HUMPTY_DUMPTY);

            if (!omitUnsplitSuffix) {
                // One record with version
                FDBRecordVersion version2 = FDBRecordVersion.complete(globalVersion, context.claimLocalVersion());
                FDBStoredSizes sizes2 = writeDummyRecord(context, Tuple.from(1087L), version2, 1);
                assertThat(sizes2.isVersionedInline(), is(true));
                loadRecordFunction.load(context, Tuple.from(1087L), sizes2, HUMPTY_DUMPTY, version2);

                // One version but missing record
                FDBRecordVersion version3 = FDBRecordVersion.complete(globalVersion, context.claimLocalVersion());
                writeDummyRecord(context, Tuple.from(1100L), version3, 1);
                context.ensureActive().clear(subspace.pack(Tuple.from(1100L, SplitHelper.UNSPLIT_RECORD)));
                assertThrows(SplitHelper.FoundSplitWithoutStartException.class,
                        () -> loadRecordFunction.load(context, Tuple.from(1100L), null, null, version3));
            }

            if (splitLongRecords) {
                // One split record
                FDBStoredSizes sizes4 = writeDummyRecord(context, Tuple.from(1135L), MEDIUM_COPIES, false);
                assertEquals(MEDIUM_COPIES, sizes4.getKeyCount());
                loadRecordFunction.load(context, Tuple.from(1135L), sizes4, MEDIUM_STRING);

                // One split record but then delete the last split point (no way to distinguish this from just inserting one fewer split)
                writeDummyRecord(context, Tuple.from(1135L), MEDIUM_COPIES + 1, false);
                context.ensureActive().clear(subspace.pack(Tuple.from(1135L, SplitHelper.START_SPLIT_RECORD + MEDIUM_COPIES)));
                loadRecordFunction.load(context, Tuple.from(1135L), sizes4, MEDIUM_STRING);

                // One split record then delete the first split point
                writeDummyRecord(context, Tuple.from(1189L), MEDIUM_COPIES, false);
                context.ensureActive().clear(subspace.pack(Tuple.from(1189L, SplitHelper.START_SPLIT_RECORD)));
                assertThrows(SplitHelper.FoundSplitWithoutStartException.class,
                        () -> loadRecordFunction.load(context, Tuple.from(1189L), null, null));

                // One split record then delete the a middle split point
                writeDummyRecord(context, Tuple.from(1199L), MEDIUM_COPIES, false);
                context.ensureActive().clear(subspace.pack(Tuple.from(1199L, SplitHelper.START_SPLIT_RECORD + 2)));
                RecordCoreException err7 = assertThrows(RecordCoreException.class,
                        () -> loadRecordFunction.load(context, Tuple.from(1199L), null, null));
                assertThat(err7.getMessage(), containsString("Split record segments out of order"));

                // One split record then add an extra key in the middle
                writeDummyRecord(context, Tuple.from(1216L), MEDIUM_COPIES, false);
                context.ensureActive().set(subspace.pack(Tuple.from(1216L, SplitHelper.START_SPLIT_RECORD + 2, 0L)), HUMPTY_DUMPTY);
                RecordCoreException err8 = assertThrows(RecordCoreException.class,
                        () -> loadRecordFunction.load(context, Tuple.from(1216L), null, null));
                assertThat(err8.getMessage(), anyOf(
                        containsString("Expected only a single key extension"),
                        containsString("Split record segments out of order")
                ));

                // One split record with version then delete the first split point
                FDBRecordVersion version9 = FDBRecordVersion.complete(globalVersion, context.claimLocalVersion());
                writeDummyRecord(context, Tuple.from(1272L), version9, MEDIUM_COPIES);
                context.ensureActive().clear(subspace.pack(Tuple.from(1272L, SplitHelper.START_SPLIT_RECORD)));
                assertThrows(SplitHelper.FoundSplitWithoutStartException.class,
                        () -> loadRecordFunction.load(context, Tuple.from(1272L), null, null, version9));
            }

            commit(context);
        }
    }

    @Nullable
    private FDBRawRecord loadWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, boolean splitLongRecords, boolean omitUnsplitSuffix,
                                       @Nullable FDBStoredSizes expectedSizes, @Nullable byte[] expectedContents, @Nullable FDBRecordVersion expectedVersion) {
        final ReadTransaction tr = context.ensureActive();
        SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        FDBRawRecord rawRecord;
        try {
            rawRecord = SplitHelper.loadWithSplit(tr, context, subspace, key, splitLongRecords, omitUnsplitSuffix, sizeInfo).get();
        } catch (InterruptedException | ExecutionException e) {
            throw FDBExceptions.wrapException(e);
        }

        if (expectedSizes == null || expectedContents == null) {
            assertNull(rawRecord);
        } else {
            assertNotNull(rawRecord);
            assertArrayEquals(expectedContents, rawRecord.getRawRecord());
            int valueSize = expectedContents.length;
            if (expectedVersion != null) {
                valueSize += 1 + FDBRecordVersion.VERSION_LENGTH;
            }
            assertEquals(valueSize, rawRecord.getValueSize());
            if (!splitLongRecords) {
                assertThat(rawRecord.isSplit(), is(false));
            }
            if (omitUnsplitSuffix) {
                assertThat(rawRecord.isVersionedInline(), is(false));
            }
            boolean isSplit = rawRecord.getKeyCount() - (expectedVersion != null ? 1 : 0) != 1;
            assertEquals(isSplit, rawRecord.isSplit());
            assertEquals(key, rawRecord.getPrimaryKey());
            if (expectedVersion != null) {
                assertThat(rawRecord.isVersionedInline(), is(true));
                assertEquals(expectedVersion, rawRecord.getVersion());
            } else {
                assertThat(rawRecord.isVersionedInline(), is(false));
                assertNull(rawRecord.getVersion());
            }

            // Verify that the expected sizes are the same as the ones retrieved
            assertEquals(expectedSizes.getKeyCount(), rawRecord.getKeyCount());
            assertEquals(expectedSizes.getKeySize(), rawRecord.getKeySize());
            assertEquals(expectedSizes.getValueSize(), rawRecord.getValueSize());
            assertEquals(expectedSizes.isSplit(), rawRecord.isSplit());
            assertEquals(expectedSizes.isVersionedInline(), rawRecord.isVersionedInline());

            // Verify using sizeInfo and using the raw record get the same size information
            assertEquals(rawRecord.getKeyCount(), sizeInfo.getKeyCount());
            assertEquals(rawRecord.getKeySize(), sizeInfo.getKeySize());
            assertEquals(rawRecord.getValueSize(), sizeInfo.getValueSize());
            assertEquals(rawRecord.isSplit(), sizeInfo.isSplit());
            assertEquals(rawRecord.isVersionedInline(), sizeInfo.isVersionedInline());
        }

        return rawRecord;
    }

    @Nullable
    private FDBRawRecord loadWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, boolean splitLongRecords, boolean omitUnsplitSuffix,
                                       @Nullable FDBStoredSizes expectedSizes, @Nullable byte[] expectedContents) throws InterruptedException {
        return loadWithSplit(context, key, splitLongRecords, omitUnsplitSuffix, expectedSizes, expectedContents, null);
    }

    @MethodSource("splitAndSuffixArgs")
    @ParameterizedTest(name = "deleteWithSplitAndVersion [splitLongRecords = {0}, omitUnsplitSuffix = {1}]")
    public void loadWithSplit(boolean splitLongRecords, boolean omitUnsplitSuffix) throws Exception {
        loadSingleRecords(splitLongRecords, omitUnsplitSuffix,
                (context, key, expectedSizes, expectedContents, version) -> loadWithSplit(context, key, splitLongRecords, omitUnsplitSuffix, expectedSizes, expectedContents, version));

        if (splitLongRecords) {
            try (FDBRecordContext context = openContext()) {
                // Unsplit record followed by some unsplit stuff
                // This particular error is caught by the single key unsplitter but not the mulit-key one
                writeDummyRecord(context, Tuple.from(1307L), 1, false);
                writeDummyRecord(context, Tuple.from(1307L), MEDIUM_COPIES, false);
                RecordCoreException err = assertThrows(RecordCoreException.class,
                        () -> loadWithSplit(context, Tuple.from(1307L), true, false, null, null));
                assertThat(err.getMessage(), containsString("Unsplit value followed by split"));

                commit(context);
            }
        }
    }

    private FDBRawRecord scanSingleRecord(@Nonnull FDBRecordContext context, boolean reverse, @Nonnull Tuple key, @Nullable FDBStoredSizes expectedSizes, @Nullable byte[] expectedContents, @Nullable FDBRecordVersion version) {
        final ScanProperties scanProperties = reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN;
        KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                .setContext(context)
                .setRange(TupleRange.allOf(key))
                .setScanProperties(scanProperties)
                .build();
        SplitHelper.KeyValueUnsplitter kvUnsplitter = new SplitHelper.KeyValueUnsplitter(context, subspace, kvCursor, false, null, scanProperties);

        RecordCursorResult<FDBRawRecord> result = kvUnsplitter.getNext();
        if (expectedSizes == null || expectedContents == null) {
            assertThat(result.hasNext(), is(false));
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
            return null;
        } else {
            assertThat(result.hasNext(), is(true));
            final FDBRawRecord rawRecord = result.get();
            result = kvUnsplitter.getNext();
            assertThat(result.hasNext(), is(false));
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());

            assertNotNull(rawRecord);
            assertEquals(key, rawRecord.getPrimaryKey());
            assertArrayEquals(expectedContents, rawRecord.getRawRecord());
            assertEquals(expectedSizes.getKeyCount(), rawRecord.getKeyCount());
            assertEquals(expectedSizes.getKeySize(), rawRecord.getKeySize());
            assertEquals(expectedSizes.getValueSize(), rawRecord.getValueSize());
            boolean isSplit = rawRecord.getKeyCount() - (rawRecord.isVersionedInline() ? 1 : 0) != 1;
            assertEquals(rawRecord.getKeyCount() - (rawRecord.isVersionedInline() ? 1 : 0) != 1, expectedSizes.isSplit());
            assertEquals(version != null, expectedSizes.isVersionedInline());

            return rawRecord;
        }
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "scan [reverse = {0}]")
    public void scanSingleRecords(final TestHelpers.BooleanEnum reverseEnum) throws Exception {
        final boolean reverse = reverseEnum.toBoolean();
        loadSingleRecords(true, false,
                (context, key, expectedSizes, expectedContents, version) -> scanSingleRecord(context, reverse, key, expectedSizes, expectedContents, version));
    }

    private List<FDBRawRecord> writeDummyRecords() throws Exception {
        final byte[] globalVersion = "_cushions_".getBytes(Charsets.US_ASCII);
        final List<FDBRawRecord> rawRecords = new ArrayList<>();
        // Generate primary keys using a generalization of the Fibonacci formula: https://oeis.org/A247698
        long currKey = 2308L;
        long nextKey = 4261L;

        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 50; i++) {
                FDBRecordVersion version = (i % 2 == 0) ? FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()) : null;
                byte[] rawBytes = (i % 4 < 2) ? SHORT_STRING : MEDIUM_STRING;
                Tuple key = Tuple.from(currKey);
                FDBStoredSizes sizes = writeDummyRecord(context, key, version, (i % 4 < 2) ? 1 : MEDIUM_COPIES, false);
                rawRecords.add(new FDBRawRecord(key, rawBytes, version, sizes));

                long temp = currKey + nextKey;
                currKey = nextKey;
                nextKey = temp;
            }

            commit(context);
        }

        return rawRecords;
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "scanMultipleRecords [reverse = {0}]")
    public void scanMultipleRecords(final TestHelpers.BooleanEnum reverseEnum) throws Exception {
        final boolean reverse = reverseEnum.toBoolean();
        final ScanProperties scanProperties = reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN;
        List<FDBRawRecord> rawRecords = writeDummyRecords();

        try (FDBRecordContext context = openContext()) {
            KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setScanProperties(scanProperties)
                    .build();
            List<FDBRawRecord> readRecords = new SplitHelper.KeyValueUnsplitter(context, subspace, kvCursor, false, null, scanProperties)
                    .asList().get();
            if (reverse) {
                readRecords = Lists.reverse(readRecords);
            }
            assertEquals(rawRecords.size(), readRecords.size());
            for (int i = 0; i < rawRecords.size(); i++) {
                assertEquals(rawRecords.get(i), readRecords.get(i));
            }
            assertEquals(rawRecords, readRecords);

            commit(context);
        }
    }

    @MethodSource("limitsAndReverseArgs")
    @ParameterizedTest(name = "scanContinuations [returnLimit = {0}, readLimit = {1}, reverse = {2}]")
    public void scanContinuations(final int returnLimit, final int readLimit, final boolean reverse) throws Exception {
        List<FDBRawRecord> rawRecords = writeDummyRecords();
        if (reverse) {
            rawRecords = Lists.reverse(rawRecords);
        }
        final Iterator<FDBRawRecord> expectedRecordIterator = rawRecords.iterator();

        try (FDBRecordContext context = openContext()) {
            byte[] continuation = null;

            do {
                final ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(returnLimit)
                        .setScannedRecordsLimit(readLimit)
                        .build();
                ScanProperties scanProperties = new ScanProperties(executeProperties, reverse);
                RecordCursor<KeyValue> kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                        .setContext(context)
                        .setRange(TupleRange.ALL)
                        .setScanProperties(scanProperties.with(ExecuteProperties::clearRowAndTimeLimits).with(ExecuteProperties::clearState))
                        .setContinuation(continuation)
                        .build();
                RecordCursorIterator<FDBRawRecord> recordCursor = new SplitHelper.KeyValueUnsplitter(context, subspace, kvCursor, false, null, scanProperties.with(ExecuteProperties::clearReturnedRowLimit))
                        .limitRowsTo(returnLimit)
                        .asIterator();

                int retrieved = 0;
                int rowsScanned = 0;
                while (recordCursor.hasNext()) {
                    assertThat(retrieved, lessThan(returnLimit));
                    assertThat(rowsScanned, lessThanOrEqualTo(readLimit));

                    FDBRawRecord nextRecord = recordCursor.next();
                    assertNotNull(nextRecord);
                    assertThat(expectedRecordIterator.hasNext(), is(true));
                    FDBRawRecord expectedRecord = expectedRecordIterator.next();
                    assertEquals(expectedRecord, nextRecord);

                    rowsScanned += nextRecord.getKeyCount();
                    retrieved += 1;
                }

                if (retrieved > 0) {
                    continuation = recordCursor.getContinuation();
                    if (retrieved >= returnLimit) {
                        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, recordCursor.getNoNextReason());
                        assertNotNull(continuation);
                    } else if (rowsScanned > readLimit) {
                        assertEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, recordCursor.getNoNextReason());
                        assertNotNull(continuation);
                    } else if (rowsScanned < readLimit) {
                        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, recordCursor.getNoNextReason());
                    } else {
                        // If we read exactly as many records as is allowed by the read record limit, then
                        // this probably means that we hit SCAN_LIMIT_REACHED, but it's also possible to
                        // hit SOURCE_EXHAUSTED if we hit the record read limit at exactly the same time
                        // as we needed to do another speculative read to determine if a split record
                        // continues or not.
                        assertEquals(readLimit, rowsScanned);
                        assertThat(recordCursor.getNoNextReason(), isOneOf(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED , RecordCursor.NoNextReason.SOURCE_EXHAUSTED));
                        if (!recordCursor.getNoNextReason().isSourceExhausted()) {
                            assertNotNull(recordCursor.getContinuation());
                        }
                    }
                } else {
                    assertNull(recordCursor.getContinuation());
                    continuation = null;
                }
            } while (continuation != null);

            commit(context);
        }
    }
}
