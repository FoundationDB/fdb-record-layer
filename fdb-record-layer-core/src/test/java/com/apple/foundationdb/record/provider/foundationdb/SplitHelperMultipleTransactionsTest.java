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
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FDBRecordStoreProperties;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreInternalException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.test.BooleanSource;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.Tags;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.shadow.com.univocity.parsers.common.ArgumentUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for checking the validity of the "split helper" utility class that handles breaking
 * records across key-value pairs and putting them back together again.
 */
@Tag(Tags.RequiresFDB)
public class SplitHelperMultipleTransactionsTest extends FDBRecordStoreTestBase {

    // From the traditional nursery rhyme
    private static final byte[] HUMPTY_DUMPTY =
            ("Humpty Dumpty sat on a wall,\n"
                     + "Humpty Dumpty had a great fall\n"
                     + "All the king's horses and all the king's men\n"
                     + "Couldn't put Humpty Dumpty together again.\n").getBytes(StandardCharsets.UTF_8);

    private static final int MEDIUM_COPIES = 5;
    private static final int MEDIUM_LEGNTH = HUMPTY_DUMPTY.length * MEDIUM_COPIES;
    private static final int LONG_LENGTH = HUMPTY_DUMPTY.length * 1_000; // requires 1 split
    private static final int VERY_LONG_LENGTH = HUMPTY_DUMPTY.length * 2_000; // requires 2 splits
    private static final byte[] SHORT_STRING = HUMPTY_DUMPTY;
    private static final byte[] MEDIUM_STRING;
    private static final byte[] LONG_STRING;
    private static final byte[] VERY_LONG_STRING;

    private Subspace subspace;
    private SplitHelperTestConfig testConfig = SplitHelperTestConfig.getDefault();

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

    static class SplitHelperTestConfig {
        private final boolean splitLongRecords;
        private final boolean omitUnsplitSuffix;
        private final boolean unrollRecordDeletes;
        private final boolean loadViaGets;
        private final boolean isDryRun;
        private final boolean useVersionInKey;

        public SplitHelperTestConfig(boolean splitLongRecords, boolean omitUnsplitSuffix, boolean unrollRecordDeletes,
                                     boolean loadViaGets, boolean isDryRun, boolean useVersionInKey) {
            this.splitLongRecords = splitLongRecords;
            this.omitUnsplitSuffix = omitUnsplitSuffix;
            this.unrollRecordDeletes = unrollRecordDeletes;
            this.loadViaGets = loadViaGets;
            this.isDryRun = isDryRun;
            this.useVersionInKey = useVersionInKey;
        }

        public SplitKeyValueHelper keyHelper(int localVersion) {
            if (useVersionInKey) {
                return new VersioningSplitKeyValueHelper(Versionstamp.incomplete(localVersion));
            } else {
                return DefaultSplitKeyValueHelper.INSTANCE;
            }
        }

        @Nonnull
        public RecordLayerPropertyStorage.Builder setProps(@Nonnull RecordLayerPropertyStorage.Builder props) {
            return props
                    .addProp(FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES, unrollRecordDeletes)
                    .addProp(FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS, loadViaGets);
        }

        public boolean hasSplitPoints() {
            return splitLongRecords || !omitUnsplitSuffix;
        }

        @Override
        public String toString() {
            return "SplitHelperTestConfig{" +
                    "splitLongRecords=" + splitLongRecords +
                    ", omitUnsplitSuffix=" + omitUnsplitSuffix +
                    ", unrollRecordDeletes=" + unrollRecordDeletes +
                    ", loadViaGets=" + loadViaGets +
                    ", isDryRun=" + isDryRun +
                    ", useVersionInKey=" + useVersionInKey +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SplitHelperTestConfig that = (SplitHelperTestConfig)o;
            return splitLongRecords == that.splitLongRecords && omitUnsplitSuffix == that.omitUnsplitSuffix &&
                    unrollRecordDeletes == that.unrollRecordDeletes && loadViaGets == that.loadViaGets &&
                    isDryRun == that.isDryRun && useVersionInKey == that.useVersionInKey;
        }

        @Override
        public int hashCode() {
            return Objects.hash(splitLongRecords, omitUnsplitSuffix, unrollRecordDeletes, loadViaGets, useVersionInKey);
        }

        public static Stream<SplitHelperTestConfig> allValidConfigs() {
            // Note that splitLongRecords="true" && omitUnsplitSuffix="true" is not valid
            // Note that useVersionInKey="true" && isDryRun="true" is not valid (versionstamp never completes without commit)
            return Stream.of(false, true).flatMap(useVersionInKey ->
                    Stream.of(false, true).flatMap(splitLongRecords ->
                            (splitLongRecords ? Stream.of(false) : Stream.of(false, true)).flatMap(omitUnsplitSuffix ->
                                    Stream.of(false, true).flatMap(unrollRecordDeletes ->
                                            Stream.of(false, true).flatMap(loadViaGets ->
                                                    Stream.of(false, true)
                                                            .filter(isDryRun -> !useVersionInKey || !isDryRun)
                                                            .map(isDryRun ->
                                                                    new SplitHelperTestConfig(splitLongRecords, omitUnsplitSuffix, unrollRecordDeletes, loadViaGets, isDryRun, useVersionInKey)))))));
        }

        public static SplitHelperTestConfig getDefault() {
            return new SplitHelperTestConfig(true, false,
                    FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES.getDefaultValue(),
                    FDBRecordStoreProperties.LOAD_RECORDS_VIA_GETS.getDefaultValue(), false, false);
        }
    }

    public static Stream<Arguments> testConfigs() {
        return SplitHelperTestConfig.allValidConfigs().map(Arguments::of);
    }

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return testConfig.setProps(super.addDefaultProps(props));
    }

    private <E extends Throwable> SplitHelper.SizeInfo saveUnsuccessfully(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
                                                                          @Nullable FDBRecordVersion version,
                                                                          @Nonnull SplitHelperTestConfig testConfig,
                                                                          @Nullable FDBStoredSizes previousSizeInfo,
                                                                          @Nonnull Class<E> errClazz, @Nonnull String errMessage) {
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        E e = assertThrows(errClazz,
                () -> SplitHelper.saveWithSplit(context, subspace, key, serialized, version, testConfig.splitLongRecords, testConfig.omitUnsplitSuffix,
                        testConfig.keyHelper(0), previousSizeInfo != null, previousSizeInfo, sizeInfo));
        assertThat(e.getMessage(), containsString(errMessage));

        assertEquals(0, sizeInfo.getKeyCount());
        assertEquals(0, sizeInfo.getKeySize());
        assertEquals(0, sizeInfo.getValueSize());
        assertThat(sizeInfo.isVersionedInline(), is(false));

        assertEquals(0, previousSizeInfo == null ? 0 : previousSizeInfo.getKeyCount());

        return sizeInfo;
    }

    private SplitHelper.SizeInfo saveOnly(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
                                          @Nullable FDBRecordVersion version,
                                          @Nonnull SplitHelperTestConfig testConfig,
                                          @Nullable FDBStoredSizes previousSizeInfo,
                                          int localVersion) {
        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        SplitHelper.saveWithSplit(context, subspace, key, serialized, version, testConfig.splitLongRecords, testConfig.omitUnsplitSuffix,
                testConfig.keyHelper(localVersion), previousSizeInfo != null, previousSizeInfo, sizeInfo);
        int dataKeyCount = (serialized.length - 1) / SplitHelper.SPLIT_RECORD_SIZE + 1;
        boolean isSplit = dataKeyCount > 1;
        int keyCount = dataKeyCount;
        if (version != null) {
            keyCount += 1;
        }
        assertEquals(isSplit, sizeInfo.isSplit());
        assertEquals(keyCount, sizeInfo.getKeyCount());
        int valueSize = serialized.length + (version != null ? 1 + FDBRecordVersion.VERSION_LENGTH : 0);
        assertEquals(valueSize, sizeInfo.getValueSize());
        assertEquals(version != null, sizeInfo.isVersionedInline());
        if (!testConfig.useVersionInKey) {
            // Key size can only be asserted when the key is fully known at write time (not the case for versioning keys,
            // since the versionstamp in the key is not yet resolved until after commit)
            int keySize = (subspace.pack().length + key.pack().length) * keyCount;
            if (testConfig.hasSplitPoints()) {
                // Add in the counters the split points.
                if (!isSplit) {
                    keySize += 1; // As 0 requires 1 byte when Tuple packed
                } else {
                    keySize += dataKeyCount * 2; // As each split point is two bytes when tuple packed
                }
            }
            if (version != null) {
                keySize += 2;
            }
            assertEquals(keySize, sizeInfo.getKeySize());
        }
        return sizeInfo;
    }

    private Tuple toCompleteKey(Tuple key, byte[] versionStamp, int localVersion, boolean versionInKey) {
        if (versionInKey) {
            return Tuple.from(Versionstamp.complete(versionStamp, localVersion)).addAll(key);
        } else {
            return key;
        }
    }

    private void verifySuccessfullySaved(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
                                         @Nullable FDBRecordVersion version,
                                         @Nonnull SplitHelperTestConfig testConfig) {
        // Similar to the calculation in saveOnly
        int dataKeyCount = (serialized.length - 1) / SplitHelper.SPLIT_RECORD_SIZE + 1;
        boolean isSplit = dataKeyCount > 1;
        int keyCount = dataKeyCount;
        if (version != null) {
            keyCount += 1;
        }
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
            if (testConfig.omitUnsplitSuffix) {
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
        if (testConfig.omitUnsplitSuffix) {
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
    }

//    private SplitHelper.SizeInfo saveSuccessfully(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
//                                                  @Nullable FDBRecordVersion version,
//                                                  @Nonnull SplitHelperTestConfig testConfig,
//                                                  @Nullable FDBStoredSizes previousSizeInfo) {
//        // localVersion=0: single-transaction callers always use useVersionInKey=false, so the value doesn't matter
//        SplitHelper.SizeInfo sizeInfo = saveOnly(context, key, serialized, version, testConfig, previousSizeInfo, 0);
//        verifySuccessfullySaved(context, key, serialized, version, testConfig);
//        return sizeInfo;
//    }

//    private SplitHelper.SizeInfo dryRunSetSizeInfo(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
//                                                   @Nullable FDBRecordVersion version,
//                                                   @Nonnull SplitHelperTestConfig testConfig,
//                                                   @Nullable FDBStoredSizes previousSizeInfo) {
//        final SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
//        SplitHelper.dryRunSaveWithSplitOnlySetSizeInfo(subspace, key, serialized, version, testConfig.splitLongRecords, testConfig.omitUnsplitSuffix, sizeInfo);
//
//        int dataKeyCount = (serialized.length - 1) / SplitHelper.SPLIT_RECORD_SIZE + 1;
//        boolean isSplit = dataKeyCount > 1;
//        int keyCount = dataKeyCount;
//        if (version != null) {
//            keyCount += 1;
//        }
//        int keySize = (subspace.pack().length + key.pack().length) * keyCount;
//        assertEquals(isSplit, sizeInfo.isSplit());
//        assertEquals(keyCount, sizeInfo.getKeyCount());
//        if (testConfig.hasSplitPoints()) {
//            // Add in the the counters the split points.
//            if (!isSplit) {
//                keySize += 1; // As 0 requires 1 byte when Tuple packed
//            } else {
//                keySize += dataKeyCount * 2; // As each split point is two bytes when tuple packed
//            }
//        }
//        if (version != null) {
//            keySize += 2;
//        }
//        int valueSize = serialized.length + (version != null ? 1 + FDBRecordVersion.VERSION_LENGTH : 0);
//        assertEquals(keySize, sizeInfo.getKeySize());
//        assertEquals(valueSize, sizeInfo.getValueSize());
//        assertEquals(version != null, sizeInfo.isVersionedInline());
//        // assert nothing is written
//        int count = KeyValueCursor.Builder.withSubspace(subspace.subspace(key))
//                .setContext(context)
//                .setScanProperties(ScanProperties.FORWARD_SCAN)
//                .build()
//                .getCount()
//                .join();
//        assertEquals(0, previousSizeInfo == null ? count : previousSizeInfo.getKeyCount() + count);
//        sizeInfo.reset();
//        return sizeInfo;
//    }

//    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
//                                               @Nullable FDBRecordVersion version,
//                                               @Nonnull SplitHelperTestConfig testConfig,
//                                               @Nullable FDBStoredSizes previousSizeInfo) {
//        if (testConfig.omitUnsplitSuffix && version != null) {
//            return saveUnsuccessfully(context, key, serialized, version, testConfig, previousSizeInfo,
//                    RecordCoreArgumentException.class, "Cannot include version");
//        } else if (!testConfig.splitLongRecords && serialized.length > SplitHelper.SPLIT_RECORD_SIZE) {
//            return saveUnsuccessfully(context, key, serialized, version, testConfig, previousSizeInfo,
//                    RecordCoreException.class, "Record is too long");
//        } else if (testConfig.isDryRun) {
//            return dryRunSetSizeInfo(context, key, serialized, version, testConfig, previousSizeInfo);
//        } else {
//            return saveSuccessfully(context, key, serialized, version, testConfig, previousSizeInfo);
//        }
//    }

//    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized,
//                                               @Nullable FDBRecordVersion version, @Nonnull SplitHelperTestConfig testConfig) {
//        return saveWithSplit(context, key, serialized, version, testConfig, null);
//    }
//
//    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized, @Nonnull SplitHelperTestConfig testConfig,
//                                               @Nullable FDBStoredSizes previousSizeInfo) {
//        return saveWithSplit(context, key, serialized, null, testConfig, previousSizeInfo);
//    }
//
//    private SplitHelper.SizeInfo saveWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, byte[] serialized, @Nonnull SplitHelperTestConfig testConfig) {
//        return saveWithSplit(context, key, serialized, null, testConfig);
//    }

//    @MethodSource("testConfigs")
//    @ParameterizedTest(name = "saveWithSplit[{0}]")
//    public void saveWithSplit(@Nonnull SplitHelperTestConfig testConfig) {
//        this.testConfig = testConfig;
//        // For versionInKey we need multiple transactions - one to save the key and one to read the timestamp from it
//        Assumptions.assumeFalse(testConfig.useVersionInKey);
//        try (FDBRecordContext context = openContext()) {
//            // No version
//            FDBStoredSizes sizes1 = saveWithSplit(context, Tuple.from(1066L), SHORT_STRING, testConfig);
//            FDBStoredSizes sizes2 = saveWithSplit(context, Tuple.from(1415L), LONG_STRING, testConfig);
//            FDBStoredSizes sizes3 = saveWithSplit(context, Tuple.from(1776L), VERY_LONG_STRING, testConfig);
//
//            // Save over some things using the previous split points
//            if (testConfig.splitLongRecords) {
//                saveWithSplit(context, Tuple.from(1066L), VERY_LONG_STRING, testConfig, sizes1);
//                saveWithSplit(context, Tuple.from(1776), LONG_STRING, testConfig, sizes3);
//            }
//            saveWithSplit(context, Tuple.from(1415L), SHORT_STRING, testConfig, sizes2);
//
//            commit(context);
//        }
//    }

    @MethodSource("testConfigs")
    @ParameterizedTest(name = "saveWithSplitMultipleTransactions[{0}]")
    public void saveWithSplitMultipleTransactions(@Nonnull SplitHelperTestConfig testConfig) {
        // dry run does not support transactions
        Assumptions.assumeFalse(testConfig.isDryRun);
        this.testConfig = testConfig;

        final Tuple key1 = Tuple.from(1066L);
        final Tuple key2 = Tuple.from(1415L);
        final Tuple key3 = Tuple.from(1776L);
        final int localVersion1;
        final int localVersion2;
        final int localVersion3;
        final SplitHelper.SizeInfo sizes1;
        final SplitHelper.SizeInfo sizes2;
        final SplitHelper.SizeInfo sizes3;
        byte[] globalVersionstamp;
        try (FDBRecordContext context = openContext()) {
            // save with no version and no previousSizeInfo
            localVersion1 = context.claimLocalVersion();
            sizes1 = saveWithSplitForMultipleTransactions(context, key1, SHORT_STRING, null, testConfig, null, localVersion1);
            localVersion2 = context.claimLocalVersion();
            sizes2 = saveWithSplitForMultipleTransactions(context, key2, LONG_STRING, null, testConfig, null, localVersion2);
            localVersion3 = context.claimLocalVersion();
            sizes3 = saveWithSplitForMultipleTransactions(context, key3, VERY_LONG_STRING, null, testConfig, null, localVersion3);

            commit(context);
            globalVersionstamp = context.getVersionStamp();
        }

        final Tuple verifyKey1 = toCompleteKey(key1, globalVersionstamp, localVersion1, testConfig.useVersionInKey);
        final Tuple verifyKey2 = toCompleteKey(key2, globalVersionstamp, localVersion2, testConfig.useVersionInKey);
        final Tuple verifyKey3 = toCompleteKey(key3, globalVersionstamp, localVersion3, testConfig.useVersionInKey);
        try (FDBRecordContext context = openContext()) {
            verifySuccessfullySaved(context, verifyKey1, SHORT_STRING, null, testConfig);
            if (testConfig.splitLongRecords) {
                verifySuccessfullySaved(context, verifyKey2, LONG_STRING, null, testConfig);
                verifySuccessfullySaved(context, verifyKey3, VERY_LONG_STRING, null, testConfig);
            }
        }

        int localVersion4 = 0;
        int localVersion5 = 0;
        int localVersion6 = 0;
        try (FDBRecordContext context = openContext()) {
            // Save over some things using the previous split points
            if (testConfig.splitLongRecords) {
                localVersion4 = context.claimLocalVersion();
                saveWithSplitForMultipleTransactions(context, key1, VERY_LONG_STRING, null, testConfig, sizes1, localVersion4);
                localVersion5 = context.claimLocalVersion();
                saveWithSplitForMultipleTransactions(context, key3, LONG_STRING, null, testConfig, sizes3, localVersion5);
            }
            localVersion6 = context.claimLocalVersion();
            saveWithSplitForMultipleTransactions(context, key2, SHORT_STRING, null, testConfig, sizes2, localVersion6);
            commit(context);
            globalVersionstamp = context.getVersionStamp();
        }
        Tuple verifyKey4 = toCompleteKey(key1, globalVersionstamp, localVersion4, testConfig.useVersionInKey);
        Tuple verifyKey5 = toCompleteKey(key3, globalVersionstamp, localVersion5, testConfig.useVersionInKey);
        Tuple verifyKey6 = toCompleteKey(key2, globalVersionstamp, localVersion6, testConfig.useVersionInKey);
        try (FDBRecordContext context = openContext()) {
            if (testConfig.splitLongRecords) {
                verifySuccessfullySaved(context, verifyKey4, VERY_LONG_STRING, null, testConfig);
                verifySuccessfullySaved(context, verifyKey5, LONG_STRING, null, testConfig);
            }
            verifySuccessfullySaved(context, verifyKey6, SHORT_STRING, null, testConfig);
        }
    }

    private SplitHelper.SizeInfo saveWithSplitForMultipleTransactions(@Nonnull FDBRecordContext context, @Nonnull Tuple key,
                                                                      byte[] serialized,
                                                                      @Nullable FDBRecordVersion version,
                                                                      @Nonnull SplitHelperTestConfig testConfig,
                                                                      @Nullable FDBStoredSizes previousSizeInfo,
                                                                      int localVersion) {
        if (testConfig.omitUnsplitSuffix && version != null) {
            return saveUnsuccessfully(context, key, serialized, version, testConfig, previousSizeInfo,
                    RecordCoreArgumentException.class, "Cannot include version");
        } else if (!testConfig.splitLongRecords && serialized.length > SplitHelper.SPLIT_RECORD_SIZE) {
            return saveUnsuccessfully(context, key, serialized, version, testConfig, previousSizeInfo,
                    RecordCoreException.class, "Record is too long");
        } else {
            return saveOnly(context, key, serialized, version, testConfig, previousSizeInfo, localVersion);
        }
    }
    // TODO
//    @MethodSource("testConfigs")
//    @ParameterizedTest(name = "saveWithSplitAndCompleteVersion[{0}]")
//    public void saveWithSplitAndCompleteVersions(SplitHelperTestConfig testConfig) {
//        this.testConfig = testConfig;
//        Assumptions.assumeFalse(testConfig.useVersionInKey);
//        try (FDBRecordContext context = openContext()) {
//            byte[] globalVersion = "karlgrosse".getBytes(StandardCharsets.US_ASCII);
//            saveWithSplit(context, Tuple.from(800L), SHORT_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), testConfig);
//            saveWithSplit(context, Tuple.from(813L), LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), testConfig);
//            saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), testConfig);
//
//            // Save over the records *without* using the previous size info
//            saveWithSplit(context, Tuple.from(800L), SHORT_STRING, testConfig);
//            saveWithSplit(context, Tuple.from(813L), LONG_STRING, testConfig);
//            saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, testConfig);
//
//            FDBStoredSizes sizes4 = saveWithSplit(context, Tuple.from(800L), SHORT_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), testConfig);
//            FDBStoredSizes sizes5 = saveWithSplit(context, Tuple.from(813L), LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), testConfig);
//            FDBStoredSizes sizes6 = saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()), testConfig);
//
//            // Save over the records *with* using the previous size info
//            saveWithSplit(context, Tuple.from(800L), SHORT_STRING, testConfig, sizes4);
//            saveWithSplit(context, Tuple.from(813L), LONG_STRING, testConfig, sizes5);
//            saveWithSplit(context, Tuple.from(823L), VERY_LONG_STRING, testConfig, sizes6);
//
//            commit(context);
//        }
//    }

    @Nonnull
    private FDBStoredSizes writeDummyRecord(@Nonnull FDBRecordContext context, @Nonnull Tuple key,
                                            @Nullable FDBRecordVersion version, int splits,
                                            boolean omitUnsplitSuffix, boolean useVersionInKey, int localVersion) {
        SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        if (version != null) {
            assertThat(omitUnsplitSuffix, is(false));
            assertThat(useVersionInKey, is(false));
            sizeInfo.setVersionedInline(true);
            Tuple keyTuple = key.add(SplitHelper.RECORD_VERSION);
            byte[] valueBytes = SplitHelper.packVersion(version);
            // Note that this will not mutate the version in the value
            writeDummyKV(context, keyTuple, valueBytes, sizeInfo, useVersionInKey, localVersion);
        }
        if (splits == 1) {
            if (omitUnsplitSuffix) {
                Tuple keyTuple = key;
                byte[] valueBytes = SHORT_STRING;
                writeDummyKV(context, keyTuple, valueBytes, sizeInfo, useVersionInKey, localVersion);
            } else {
                Tuple keyTuple = key.add(SplitHelper.UNSPLIT_RECORD);
                byte[] valueBytes = SHORT_STRING;
                writeDummyKV(context, keyTuple, valueBytes, sizeInfo, useVersionInKey, localVersion);
            }
            sizeInfo.setSplit(false);
        } else {
            for (int i = 0; i < splits; i++) {
                Tuple keyTuple = key.add(SplitHelper.START_SPLIT_RECORD + i);
                byte[] valueBytes = SHORT_STRING;
                writeDummyKV(context, keyTuple, valueBytes, sizeInfo, useVersionInKey, localVersion);
            }
            sizeInfo.setSplit(true);
        }
        return sizeInfo;
    }

    @Nonnull
    private FDBStoredSizes writeDummyRecord(@Nonnull FDBRecordContext context, @Nonnull Tuple key, int splits, boolean omitUnsplitSuffix) {
        return writeDummyRecord(context, key, null, splits, omitUnsplitSuffix, false, 0);
    }

    @Nonnull
    private FDBStoredSizes writeDummyRecord(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nonnull FDBRecordVersion version, int splits) {
        return writeDummyRecord(context, key, version, splits, false, false, 0);
    }

    private void writeDummyKV(@Nonnull FDBRecordContext context, @Nonnull Tuple keyTuple,
                              byte[] valueBytes, @Nullable SplitHelper.SizeInfo sizeInfo, boolean useVersionInKey, int localVersion) {
        byte[] keyBytes;
        // Mimic the work done in both SplitKeyValueHelper
        if (useVersionInKey) {
            Tuple versionedKeyTuple = Tuple.from(Versionstamp.incomplete(localVersion)).addAll(keyTuple);
            keyBytes = subspace.packWithVersionstamp(versionedKeyTuple);
            context.addVersionMutation(MutationType.SET_VERSIONSTAMPED_KEY, keyBytes, valueBytes);
        } else {
            keyBytes = subspace.pack(keyTuple);
            context.ensureActive().set(keyBytes, valueBytes);
        }
        if (sizeInfo != null) {
            sizeInfo.add(keyBytes, valueBytes);
        }
    }

    private void deleteSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key,
                             @Nonnull SplitHelperTestConfig testConfig,
                             @Nullable FDBStoredSizes sizeInfo) {
        SplitHelper.deleteSplit(context, subspace, key, testConfig.splitLongRecords, testConfig.omitUnsplitSuffix, sizeInfo != null, sizeInfo);
        int count = KeyValueCursor.Builder.withSubspace(subspace.subspace(key))
                .setContext(context)
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build()
                .getCount()
                .join();
        assertEquals(0, count);
    }

    @MethodSource("testConfigs")
    @ParameterizedTest(name = "deleteWithSplitMultipleTransactions[{0}]")
    public void deleteWithSplitMultipleTransactions(@Nonnull SplitHelperTestConfig testConfig) {
        this.testConfig = testConfig;

        final Tuple key1 = Tuple.from(-660L);
        final Tuple key2 = Tuple.from(-581L);
        final Tuple key3 = Tuple.from(-549L);
        final Tuple key4 = Tuple.from(-510L);

        // tx1: write records
        int localVersion1;
        int localVersion2;
        int localVersion3 = 0;
        int localVersion4 = 0;
        final FDBStoredSizes sizes1;
        final FDBStoredSizes sizes2;
        FDBStoredSizes sizes3 = null;
        FDBStoredSizes sizes4 = null;
        byte[] globalVersionStamp;
        try (FDBRecordContext context = openContext()) {
            localVersion1 = context.claimLocalVersion();
            sizes1 = writeDummyRecord(context, key1, null, 1, testConfig.omitUnsplitSuffix, testConfig.useVersionInKey, localVersion1);
            localVersion2 = context.claimLocalVersion();
            sizes2 = writeDummyRecord(context, key2, null, 1, testConfig.omitUnsplitSuffix, testConfig.useVersionInKey, localVersion2);
            if (testConfig.splitLongRecords) {
                localVersion3 = context.claimLocalVersion();
                sizes3 = writeDummyRecord(context, key3, null, 5, testConfig.omitUnsplitSuffix, testConfig.useVersionInKey, localVersion3);
                localVersion4 = context.claimLocalVersion();
                sizes4 = writeDummyRecord(context, key4, null, 5, testConfig.omitUnsplitSuffix, testConfig.useVersionInKey, localVersion4);
            }
            commit(context);
            globalVersionStamp = context.getVersionStamp();
        }

        final Tuple deleteKey1 = toCompleteKey(key1, globalVersionStamp, localVersion1, testConfig.useVersionInKey);
        final Tuple deleteKey2 = toCompleteKey(key2, globalVersionStamp, localVersion2, testConfig.useVersionInKey);
        final Tuple deleteKey3 = toCompleteKey(key3, globalVersionStamp, localVersion3, testConfig.useVersionInKey);
        final Tuple deleteKey4 = toCompleteKey(key4, globalVersionStamp, localVersion4, testConfig.useVersionInKey);

        // tx2: delete records
        try (FDBRecordContext context = openContext()) {
            deleteSplit(context, deleteKey1, testConfig, sizes1);
            deleteSplit(context, deleteKey2, testConfig, sizes2);
            if (testConfig.splitLongRecords) {
                deleteSplit(context, deleteKey3, testConfig, sizes3);
                deleteSplit(context, deleteKey4, testConfig, sizes4);
            }
            commit(context);
        }
    }
//
//    static Stream<Arguments> deleteWithSplitAndVersion() {
//        return Stream.of(false, true).flatMap(splitLongRecords ->
//                Stream.of(false, true).map(unrollSingleRecordDeletes ->
//                        Arguments.of(splitLongRecords, unrollSingleRecordDeletes)));
//    }

    @FunctionalInterface
    private interface LoadRecordFunction {
        FDBRawRecord load(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nullable FDBStoredSizes sizes, @Nullable byte[] expectedContents, @Nullable FDBRecordVersion version);

        // TODO: Remoe this implementation
        default FDBRawRecord load(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nullable FDBStoredSizes sizes, @Nullable byte[] expectedContents) {
            return load(context, key, sizes, expectedContents, null);
        }
    }

    private void loadSingleRecordsMultipleTransactions(SplitHelperTestConfig testConfig, @Nonnull LoadRecordFunction loadRecordFunction) {
        final Tuple key1 = Tuple.from(1042L);
        final Tuple key2 = Tuple.from(1066L);
        final Tuple key3 = Tuple.from(1087L);
        final Tuple key4 = Tuple.from(1100L);
        final Tuple key5 = Tuple.from(1135L);
        final Tuple key6 = Tuple.from(1189L);
        final Tuple key7 = Tuple.from(1199L);
        final Tuple key8 = Tuple.from(1216L);
        final Tuple key9 = Tuple.from(1272L);
        int localVersion2;
        int localVersion3 = 0;
        int localVersion4 = 0;
        int localVersion5 = 0;
        int localVersion6 = 0;
        int localVersion7 = 0;
        int localVersion8 = 0;
        int localVersion9 = 0;
        FDBRecordVersion version3 = null;
        FDBRecordVersion version4 = null;
        FDBRecordVersion version9 = null;
        FDBStoredSizes sizes2;
        FDBStoredSizes sizes3 = null;
        FDBStoredSizes sizes5 = null;
        final byte[] valueGlobalVersion = "-hastings-".getBytes(StandardCharsets.US_ASCII);
        byte[] keyGlobalVersion;
        try (FDBRecordContext context = openContext()) {
            // One unsplit record
            localVersion2 = context.claimLocalVersion();
            sizes2 = writeDummyRecord(context, key2, null, 1, testConfig.omitUnsplitSuffix, testConfig.useVersionInKey, localVersion2);
            assertThat(sizes2.isSplit(), is(false));

            if ((!testConfig.omitUnsplitSuffix) && (!testConfig.useVersionInKey)) {
                // One record with version
                localVersion3 = context.claimLocalVersion();
                version3 = FDBRecordVersion.complete(valueGlobalVersion, localVersion3);
                sizes3 = writeDummyRecord(context, key3, version3, 1);
                assertThat(sizes3.isVersionedInline(), is(true));

                // One version but missing record
                localVersion4 = context.claimLocalVersion();
                version4 = FDBRecordVersion.complete(valueGlobalVersion, localVersion4);
                writeDummyRecord(context, key4, version4, 1);
                context.ensureActive().clear(subspace.pack(key4.add(SplitHelper.UNSPLIT_RECORD)));
            }

            if (testConfig.splitLongRecords) {
                // One split record
                localVersion5 = context.claimLocalVersion();
                sizes5 = writeDummyRecord(context, key5, null, MEDIUM_COPIES, false, testConfig.useVersionInKey, localVersion5);
                assertEquals(MEDIUM_COPIES, sizes5.getKeyCount());

                // One split record but then delete the last split point (no way to distinguish this from just inserting one fewer split)
                writeDummyRecord(context, key5, MEDIUM_COPIES + 1, false);

                // One split record then delete the first split point
                localVersion6 = context.claimLocalVersion();
                writeDummyRecord(context, key6, null, MEDIUM_COPIES, false, testConfig.useVersionInKey, localVersion6);

                // One split record then delete the middle split point
                localVersion7 = context.claimLocalVersion();
                writeDummyRecord(context, key7, null, MEDIUM_COPIES, false, testConfig.useVersionInKey, localVersion7);

                // One split record then add an extra key in the middle
                localVersion8 = context.claimLocalVersion();
                writeDummyRecord(context, key8, null, MEDIUM_COPIES, false, testConfig.useVersionInKey, localVersion8);
                writeDummyKV(context, key8.add(SplitHelper.START_SPLIT_RECORD + 2).add( 0L), HUMPTY_DUMPTY, null, testConfig.useVersionInKey, localVersion8);

                // One split record with version then delete the first split point
                if (!testConfig.useVersionInKey) {
                    localVersion9 = context.claimLocalVersion();
                    version9 = FDBRecordVersion.complete(valueGlobalVersion, context.claimLocalVersion());
                    writeDummyRecord(context, key9, version9, MEDIUM_COPIES);
                }
            }

            commit(context);
            keyGlobalVersion = context.getVersionStamp();
        }

        // transaction 2 - delete any items needing deleting
        try (FDBRecordContext context = openContext()) {
            // One split record but then delete the last split point (no way to distinguish this from just inserting one fewer split)
            if (testConfig.splitLongRecords) {
                final Tuple deleteKey5 = toCompleteKey(key5.add(SplitHelper.START_SPLIT_RECORD + MEDIUM_COPIES), keyGlobalVersion, localVersion5, testConfig.useVersionInKey);
                context.ensureActive().clear(subspace.pack(deleteKey5));
                // One split record then delete the first split point
                final Tuple deleteKey6 = toCompleteKey(key6.add(SplitHelper.START_SPLIT_RECORD), keyGlobalVersion, localVersion6, testConfig.useVersionInKey);
                context.ensureActive().clear(subspace.pack(deleteKey6));
                // One split record then delete the middle split point
                final Tuple deleteKey7 = toCompleteKey(key7.add(SplitHelper.START_SPLIT_RECORD + 2), keyGlobalVersion, localVersion7, testConfig.useVersionInKey);
                context.ensureActive().clear(subspace.pack(deleteKey7));
                if (!testConfig.useVersionInKey) {
                    // One split record with version then delete the first split point
                    final Tuple DeleteKey9 = toCompleteKey(key9.add(SplitHelper.START_SPLIT_RECORD), keyGlobalVersion, localVersion9, testConfig.useVersionInKey);
                    context.ensureActive().clear(subspace.pack(DeleteKey9));
                }
            }
            commit(context);
        }

        final Tuple completeKey1 = toCompleteKey(key1, keyGlobalVersion, 0, testConfig.useVersionInKey);
        final Tuple completeKey2 = toCompleteKey(key2, keyGlobalVersion, localVersion2, testConfig.useVersionInKey);
        final Tuple completeKey3 = toCompleteKey(key3, keyGlobalVersion, localVersion3, testConfig.useVersionInKey);
        final Tuple completeKey4 = toCompleteKey(key4, keyGlobalVersion, localVersion4, testConfig.useVersionInKey);
        final Tuple completeKey5 = toCompleteKey(key5, keyGlobalVersion, localVersion5, testConfig.useVersionInKey);
        final Tuple completeKey6 = toCompleteKey(key6, keyGlobalVersion, localVersion6, testConfig.useVersionInKey);
        final Tuple completeKey7 = toCompleteKey(key7, keyGlobalVersion, localVersion7, testConfig.useVersionInKey);
        final Tuple completeKey8 = toCompleteKey(key8, keyGlobalVersion, localVersion8, testConfig.useVersionInKey);
        final Tuple completeKey9 = toCompleteKey(key9, keyGlobalVersion, localVersion9, testConfig.useVersionInKey);

        // transaction 3 - verify
        try (FDBRecordContext context = openContext()) {
            // No record
            loadRecordFunction.load(context, completeKey1, null, null);
            // One unsplit record
            loadRecordFunction.load(context, completeKey2, sizes2, HUMPTY_DUMPTY);
            if ((!testConfig.omitUnsplitSuffix) && (!testConfig.useVersionInKey)) {
                // One record with version
                loadRecordFunction.load(context, completeKey3, sizes3, HUMPTY_DUMPTY, version3);
                // One version but missing record
                final FDBRecordVersion v4 = version4;
                assertThrows(SplitHelper.FoundSplitWithoutStartException.class,
                        () -> loadRecordFunction.load(context, completeKey4, null, null, v4));
            }
            if (testConfig.splitLongRecords) {
                // One split record
                // One split record but then delete the last split point (no way to distinguish this from just inserting one fewer split)
                loadRecordFunction.load(context, completeKey5, sizes5, MEDIUM_STRING);
                // One split record then delete the first split point
                if (testConfig.loadViaGets) {
                    loadRecordFunction.load(context, completeKey6, null, null);
                } else {
                    assertThrows(SplitHelper.FoundSplitWithoutStartException.class,
                            () -> loadRecordFunction.load(context, completeKey6, null, null));
                }
                // One split record then delete the middle split point
                RecordCoreException err7 = assertThrows(RecordCoreException.class,
                        () -> loadRecordFunction.load(context, completeKey7, null, null));
                assertThat(err7.getMessage(), containsString("Split record segments out of order"));
                // One split record then add an extra key in the middle
                RecordCoreException err8 = assertThrows(RecordCoreException.class,
                        () -> loadRecordFunction.load(context, completeKey8, null, null));
                assertThat(err8.getMessage(), anyOf(
                        containsString("Expected only a single key extension"),
                        containsString("Split record segments out of order")
                ));
                // One split record with version then delete the first split point
                if (!testConfig.useVersionInKey) {
                    final FDBRecordVersion v9 = version9;
                    assertThrows(SplitHelper.FoundSplitWithoutStartException.class,
                            () -> loadRecordFunction.load(context, completeKey9, null, null, v9));
                }
            }
        }
    }

    @Nullable
    private FDBRawRecord loadWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, @Nonnull SplitHelperTestConfig testConfig,
                                       @Nullable FDBStoredSizes expectedSizes, @Nullable byte[] expectedContents, @Nullable FDBRecordVersion expectedVersion) {
        final ReadTransaction tr = context.ensureActive();
        SplitHelper.SizeInfo sizeInfo = new SplitHelper.SizeInfo();
        FDBRawRecord rawRecord;
        try {
            rawRecord = SplitHelper.loadWithSplit(tr, context, subspace, key, testConfig.splitLongRecords, testConfig.omitUnsplitSuffix, sizeInfo).get();
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
            if (!testConfig.splitLongRecords) {
                assertThat(rawRecord.isSplit(), is(false));
            }
            if (testConfig.omitUnsplitSuffix) {
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
            assertEquals(expectedSizes.getValueSize(), rawRecord.getValueSize());
            assertEquals(expectedSizes.isSplit(), rawRecord.isSplit());
            assertEquals(expectedSizes.isVersionedInline(), rawRecord.isVersionedInline());

            // Verify using sizeInfo and using the raw record get the same size information
            assertEquals(rawRecord.getKeyCount(), sizeInfo.getKeyCount());
            assertEquals(rawRecord.getValueSize(), sizeInfo.getValueSize());
            assertEquals(rawRecord.isSplit(), sizeInfo.isSplit());
            assertEquals(rawRecord.isVersionedInline(), sizeInfo.isVersionedInline());

            // Do not attempt to compare key sizes if the keys contain incomplete version stamps
            if (!testConfig.useVersionInKey) {
                assertEquals(expectedSizes.getKeySize(), rawRecord.getKeySize());
                assertEquals(rawRecord.getKeySize(), sizeInfo.getKeySize());
            }
        }

        return rawRecord;
    }

    @Nullable
    private FDBRawRecord loadWithSplit(@Nonnull FDBRecordContext context, @Nonnull Tuple key, SplitHelperTestConfig testConfig,
                                       @Nullable FDBStoredSizes expectedSizes, @Nullable byte[] expectedContents) {
        return loadWithSplit(context, key, testConfig, expectedSizes, expectedContents, null);
    }

    @MethodSource("testConfigs")
    @ParameterizedTest(name = "loadWithSplitMultipleTransactions[{0}]")
    public void loadWithSplitMultipleTransactions(SplitHelperTestConfig testConfig) {
        Assumptions.assumeFalse(testConfig.isDryRun);
        this.testConfig = testConfig;
        loadSingleRecordsMultipleTransactions(testConfig,
                (context, key, expectedSizes, expectedContents, version) ->
                        loadWithSplit(context, key, testConfig, expectedSizes, expectedContents, version));

        if (testConfig.splitLongRecords) {
            final Tuple key = Tuple.from(1307L);
            final int localVersion;
            final byte[] globalVersion;
            // Unsplit record followed by some unsplit stuff
            // This particular error is caught by the single key unsplitter but not the mulit-key one
            try (FDBRecordContext context = openContext()) {
                localVersion = context.claimLocalVersion();
                writeDummyRecord(context, key, null,  1, false, testConfig.useVersionInKey, localVersion);
                writeDummyRecord(context, key, null, MEDIUM_COPIES, false, testConfig.useVersionInKey, localVersion);

                commit(context);
                globalVersion = context.getVersionStamp();
            }
            Tuple completeKey = toCompleteKey(key, globalVersion, localVersion, testConfig.useVersionInKey);
            try (FDBRecordContext context = openContext()) {
                RecordCoreException err = assertThrows(RecordCoreException.class,
                        () -> loadWithSplit(context, completeKey, testConfig, null, null));
                assertThat(err.getMessage(), containsString("Unsplit value followed by split"));
            }
        }
    }

    private FDBRawRecord scanSingleRecord(@Nonnull FDBRecordContext context, boolean reverse,
                                          @Nonnull Tuple key, @Nullable FDBStoredSizes expectedSizes,
                                          @Nullable byte[] expectedContents, @Nullable FDBRecordVersion version,
                                          boolean useVersionInKey) {
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
            assertEquals(expectedSizes.getValueSize(), rawRecord.getValueSize());
            boolean isSplit = rawRecord.getKeyCount() - (rawRecord.isVersionedInline() ? 1 : 0) != 1;
            assertEquals(rawRecord.getKeyCount() - (rawRecord.isVersionedInline() ? 1 : 0) != 1, expectedSizes.isSplit());
            assertEquals(version != null, expectedSizes.isVersionedInline());
            if (!useVersionInKey) {
                assertEquals(expectedSizes.getKeySize(), rawRecord.getKeySize());
            }
            return rawRecord;
        }
    }

    @ParameterizedTest(name = "scan[reverse = {0}, useVersionInKey = {1}]")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    public void scanSingleRecordsMultipleTransactions(boolean reverse, boolean useVersionInKey) {
        loadSingleRecordsMultipleTransactions(new SplitHelperTestConfig(true, false, FDBRecordStoreProperties.UNROLL_SINGLE_RECORD_DELETES.getDefaultValue(), false, false, useVersionInKey),
                (context, key, expectedSizes, expectedContents, version) ->
                        scanSingleRecord(context, reverse, key, expectedSizes, expectedContents, version, useVersionInKey));
    }

    private List<FDBRawRecord> writeDummyRecords() {
        final byte[] globalVersion = "_cushions_".getBytes(StandardCharsets.US_ASCII);
        final List<FDBRawRecord> rawRecords = new ArrayList<>();
        // Generate primary keys using a generalization of the Fibonacci formula: https://oeis.org/A247698
        long currKey = 2308L;
        long nextKey = 4261L;

        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 50; i++) {
                FDBRecordVersion version = (i % 2 == 0) ? FDBRecordVersion.complete(globalVersion, context.claimLocalVersion()) : null;
                byte[] rawBytes = (i % 4 < 2) ? SHORT_STRING : MEDIUM_STRING;
                Tuple key = Tuple.from(currKey);
                FDBStoredSizes sizes = writeDummyRecord(context, key, version, (i % 4 < 2) ? 1 : MEDIUM_COPIES, false, false, 0);
                rawRecords.add(new FDBRawRecord(key, rawBytes, version, sizes));

                long temp = currKey + nextKey;
                currKey = nextKey;
                nextKey = temp;
            }

            commit(context);
        }

        return rawRecords;
    }

    private List<FDBRawRecord> writeDummyRecordsMultipleTransactions(boolean useVersionInKey) {
        final byte[] valueVersion = "_cushions_".getBytes(StandardCharsets.US_ASCII);
        // Generate primary keys using a generalization of the Fibonacci formula: https://oeis.org/A247698
        long currKey = 2308L;
        long nextKey = 4261L;

        final int numRecords = 50;
        final Tuple[] keys = new Tuple[numRecords];
        final byte[][] rawBytesArr = new byte[numRecords][];
        final FDBRecordVersion[] versions = new FDBRecordVersion[numRecords];
        final int[] localVersions = new int[numRecords];
        final FDBStoredSizes[] sizes = new FDBStoredSizes[numRecords];

        final byte[] globalVersionStamp;
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < numRecords; i++) {
                keys[i] = Tuple.from(currKey);
                rawBytesArr[i] = (i % 4 < 2) ? SHORT_STRING : MEDIUM_STRING;
                versions[i] = (!useVersionInKey && (i % 2 == 0)) ? FDBRecordVersion.complete(valueVersion, context.claimLocalVersion()) : null;
                localVersions[i] = useVersionInKey ? context.claimLocalVersion() : 0;
                sizes[i] = writeDummyRecord(context, keys[i], versions[i], (i % 4 < 2) ? 1 : MEDIUM_COPIES, false, useVersionInKey, localVersions[i]);

                long temp = currKey + nextKey;
                currKey = nextKey;
                nextKey = temp;
            }
            commit(context);
            globalVersionStamp = context.getVersionStamp();
        }

        // Produce the raw records
        final List<FDBRawRecord> rawRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Tuple expectedKey = toCompleteKey(keys[i], globalVersionStamp, localVersions[i], useVersionInKey);
            rawRecords.add(new FDBRawRecord(expectedKey, rawBytesArr[i], versions[i], sizes[i]));
        }

        return rawRecords;
    }

    @ParameterizedTest(name = "scanMultipleRecords[reverse = {0}]")
    @BooleanSource
    public void scanMultipleRecords(boolean reverse) {
        final ScanProperties scanProperties = reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN;
        List<FDBRawRecord> rawRecords = writeDummyRecords();

        try (FDBRecordContext context = openContext()) {
            KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setScanProperties(scanProperties)
                    .build();
            List<FDBRawRecord> readRecords = new SplitHelper.KeyValueUnsplitter(context, subspace, kvCursor, false, null, scanProperties)
                    .asList().join();
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

    @ParameterizedTest(name = "scanMultipleRecordsMultipleTransactions[reverse = {0}, useVersionInKey = {1}]")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void scanMultipleRecordsMultipleTransactions(boolean reverse, boolean useVersionInKey) {
        final ScanProperties scanProperties = reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN;
        List<FDBRawRecord> rawRecords = writeDummyRecordsMultipleTransactions(useVersionInKey);

        try (FDBRecordContext context = openContext()) {
            KeyValueCursor kvCursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(TupleRange.ALL)
                    .setScanProperties(scanProperties)
                    .build();
            List<FDBRawRecord> readRecords = new SplitHelper.KeyValueUnsplitter(context, subspace, kvCursor, false, null, scanProperties)
                    .asList().join();
            if (reverse) {
                readRecords = Lists.reverse(readRecords);
            }
            assertEquals(rawRecords.size(), readRecords.size());
            for (int i = 0; i < rawRecords.size(); i++) {
                if (useVersionInKey) {
                    assertEqualsNoKeySize(rawRecords.get(i), readRecords.get(i));
                } else {
                    assertEquals(rawRecords.get(i), readRecords.get(i));
                }
            }

            commit(context);
        }
    }

    private void assertEqualsNoKeySize(final FDBRawRecord expected, final FDBRawRecord actual) {
        assertEquals(expected.getPrimaryKey(), actual.getPrimaryKey());
        assertArrayEquals(expected.getRawRecord(), actual.getRawRecord());
        assertEquals(expected.getVersion(), actual.getVersion());
        assertEquals(expected.getKeyCount(), actual.getKeyCount());
        assertEquals(expected.getValueSize(), actual.getValueSize());
        assertEquals(expected.isSplit(), actual.isSplit());
        assertEquals(expected.isVersionedInline(), actual.isVersionedInline());
    }

    @MethodSource("limitsAndReverseArgs")
    @ParameterizedTest(name = "scanContinuations [returnLimit = {0}, readLimit = {1}, reverse = {2}]")
    public void scanContinuations(final int returnLimit, final int readLimit, final boolean reverse) {
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
                        assertThat(recordCursor.getNoNextReason(), is(oneOf(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, RecordCursor.NoNextReason.SOURCE_EXHAUSTED)));
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

    @Nonnull
    public static Stream<Arguments> limitsAndReverseArgs() {
        List<Integer> limits = Arrays.asList(1, 2, 7, Integer.MAX_VALUE);
        return limits.stream()
                .flatMap(returnLimit -> limits.stream()
                        .flatMap(readLimit -> Stream.of(Arguments.of(returnLimit, readLimit, false), Arguments.of(returnLimit, readLimit, true))));
    }

    @Nonnull
    public static Stream<Arguments> limitsReverseVersionArgs() {
        List<Integer> limits = List.of(1, 2, 7, Integer.MAX_VALUE);
        return ParameterizedTestUtils.cartesianProduct(
                limits.stream(),
                limits.stream(),
                ParameterizedTestUtils.booleans("reverse"),
                ParameterizedTestUtils.booleans("useVersionInKey"));
    }

    @MethodSource("limitsReverseVersionArgs")
    @ParameterizedTest
    void scanContinuationsMultipleTransactions(final int returnLimit, final int readLimit, final boolean reverse, boolean useVersionInKey) {
        List<FDBRawRecord> rawRecords = writeDummyRecordsMultipleTransactions(useVersionInKey);
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
                    if (useVersionInKey) {
                        assertEqualsNoKeySize(expectedRecord, nextRecord);
                    } else {
                        assertEquals(expectedRecord, nextRecord);
                    }

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
                        assertThat(recordCursor.getNoNextReason(), is(oneOf(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, RecordCursor.NoNextReason.SOURCE_EXHAUSTED)));
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

    /**
     * When two saveWithSplit calls use the same incomplete versionstamp (same localVersion, same key) within
     * one transaction, we may get a failure or data corruption. The localVersionCache (map by key) may contain
     * previous
     * values from an identical key (same versionstamp/localversion/PK) but different splits and may not collide
     * directly
     * with the previous values. This test shows the case where there is a collision since the split numbers are the
     * same.
     */
    @Test
    void saveWithSplitVersionInKeyOverwriteInTransaction() {
        final Tuple key = Tuple.from(1066L);
        final int localVersion;
        try (FDBRecordContext context = openContext()) {
            localVersion = context.claimLocalVersion();
            // First write: VERY_LONG_STRING requires multiple splits
            final VersioningSplitKeyValueHelper splitKeyHelper = new VersioningSplitKeyValueHelper(Versionstamp.incomplete(localVersion));
            SplitHelper.saveWithSplit(context, subspace, key, VERY_LONG_STRING, null,
                    true, false,
                    splitKeyHelper,
                    false, null, null);

            // Second write: LONG_STRING — same localVersion, same key, shorter value (fewer splits)
            final RecordCoreInternalException ex = assertThrows(RecordCoreInternalException.class, () -> SplitHelper.saveWithSplit(context, subspace, key, LONG_STRING, null,
                    true, false,
                    splitKeyHelper,
                    false, null, null));
            assertTrue(ex.getMessage().contains("Key with version overwritten"));
        }
    }
}
