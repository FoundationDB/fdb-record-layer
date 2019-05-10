/*
 * VersionIndexTest.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase.RecordMetaDataHook;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@code VERSION} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class VersionIndexTest extends FDBTestBase {
    private static final byte VERSIONSTAMP_CODE = Tuple.from(Versionstamp.complete(new byte[10])).pack()[0];

    private RecordMetaData metaData;
    private RecordQueryPlanner planner;
    private FDBRecordStore recordStore;
    private int formatVersion;
    private boolean splitLongRecords;
    private FDBDatabase fdb;
    private Subspace subspace;

    @BeforeEach
    public void setUp() {
        if (fdb == null) {
            fdb = FDBDatabaseFactory.instance().getDatabase();
        }
        if (subspace == null) {
            subspace = fdb.run(context -> TestKeySpace.getKeyspacePath("record-test", "unit", "indexTest", "version").toSubspace(context));
        }
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        formatVersion = FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION;
        splitLongRecords = false;
    }

    private RecordMetaDataHook simpleVersionHook = metaDataBuilder -> {
        metaDataBuilder.setSplitLongRecords(splitLongRecords);
        metaDataBuilder.addUniversalIndex(new Index("globalCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("MySimpleRecord$num2-version", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION));
        metaDataBuilder.addUniversalIndex(
                new Index("globalVersion", VersionKeyExpression.VERSION, IndexTypes.VERSION));
    };

    private RecordMetaDataHook repeatedVersionHook = metaDataBuilder -> {
        metaDataBuilder.setSplitLongRecords(splitLongRecords);
        metaDataBuilder.addIndex("MySimpleRecord", new Index("MySimpleRecord$repeater-version", concat(field("repeater", KeyExpression.FanType.FanOut), VersionKeyExpression.VERSION), IndexTypes.VERSION));
    };

    private RecordMetaDataHook repeatedAndCompoundVersionHook = metaDataBuilder -> {
        metaDataBuilder.setSplitLongRecords(splitLongRecords);
        metaDataBuilder.addIndex("MySimpleRecord", new Index("MySimpleRecord$repeater-version", concat(field("repeater", KeyExpression.FanType.FanOut), VersionKeyExpression.VERSION), IndexTypes.VERSION));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("MySimpleRecord$num2-version", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION));
    };

    // Provide a combination of format versions relevant to versionstamps along with
    // information as to whether large records are split
    private static Stream<Arguments> formatVersionArguments() {
        return Stream.of(
                FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION - 1,
                FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION,
                FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION,
                FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION
        ).flatMap(formatVersion -> Stream.of(Arguments.of(formatVersion, true), Arguments.of(formatVersion, false)));
    }

    /**
     * A function that returns the version after a certain point and a fixed number before that.
     */
    public static class MaybeVersionFunctionKeyExpression extends FunctionKeyExpression {
        private static final List<Key.Evaluated> FIRST_VERSION_EVALUATED = Collections.singletonList(Key.Evaluated.scalar(FDBRecordVersion.MIN_VERSION));

        protected MaybeVersionFunctionKeyExpression(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Override
        public int getMinArguments() {
            return 2;
        }

        @Override
        public int getMaxArguments() {
            return 2;
        }

        @Nonnull
        @Override
        public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record,
                                                                        @Nullable Message message,
                                                                        @Nonnull Key.Evaluated arguments) {
            long id = arguments.getLong(0);
            if (id < 1066L) {
                // Prior to 1066, we might as well be at the beginning of time.
                return FIRST_VERSION_EVALUATED;
            } else {
                Object version = arguments.getObject(1);
                return Collections.singletonList(Key.Evaluated.scalar(version));
            }
        }

        @Override
        public boolean createsDuplicates() {
            return false;
        }

        @Override
        public int getColumnSize() {
            return 1;
        }

        @Override
        public int versionColumns() {
            return 1;
        }
    }

    /**
     * A factory for {@link MaybeVersionFunctionKeyExpression}.
     */
    @AutoService(FunctionKeyExpression.Factory.class)
    public static class MaybeVersionFunctionFactory implements FunctionKeyExpression.Factory {
        @Nonnull
        @Override
        public List<FunctionKeyExpression.Builder> getBuilders() {
            return Collections.singletonList(new FunctionKeyExpression.BiFunctionBuilder("maybeVersion", MaybeVersionFunctionKeyExpression::new));
        }
    }

    private RecordMetaDataHook functionVersionHook = metaDataBuilder -> {
        metaDataBuilder.addIndex("MySimpleRecord", new Index("MySimpleRecord$maybeVersion", function("maybeVersion", concat(field("num_value_2"), VersionKeyExpression.VERSION)), IndexTypes.VERSION));
    };

    private FDBRecordContext openContext(@Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        hook.apply(metaDataBuilder);

        FDBRecordContext context = fdb.openContext();
        recordStore = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaDataBuilder)
                .setContext(context)
                .setSubspace(subspace)
                .setFormatVersion(formatVersion)
                .createOrOpen();
        metaData = recordStore.getRecordMetaData();
        planner = new RecordQueryPlanner(metaData, recordStore.getRecordStoreState());

        return context;
    }

    @ParameterizedTest(name = "saveLoadWithVersion [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void saveLoadWithVersion(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;
        MySimpleRecord record1 = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(42).build();
        TestRecords1Proto.MyOtherRecord record2 = TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(1776L).setNumValue2(1729).build();

        byte[] versionstamp;

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            recordStore.saveRecord(record1);
            recordStore.saveRecord(record2);
            context.commit();

            versionstamp = context.getVersionStamp();
            assertEquals(2, context.claimLocalVersion());
        }

        FDBRecordVersion version1;
        FDBRecordVersion version2;

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            FDBStoredRecord<Message> stored1 = recordStore.loadRecord(Tuple.from(1066L));
            assertThat(stored1.hasVersion(), is(true));
            version1 = stored1.getVersion();
            assertNotNull(version1);
            assertThat(version1.isComplete(), is(true));
            assertArrayEquals(versionstamp, version1.getGlobalVersion());
            assertEquals(0, version1.getLocalVersion());

            FDBStoredRecord<Message> stored2 = recordStore.loadRecord(Tuple.from(1776L));
            assertThat(stored2.hasVersion(), is(true));
            version2 = stored2.getVersion();
            assertNotNull(version2);
            assertThat(version2.isComplete(), is(true));
            assertArrayEquals(versionstamp, version2.getGlobalVersion());
            assertEquals(1, version2.getLocalVersion());
        }

        // Saving them again should change the versions.
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            recordStore.saveRecord(record1);
            recordStore.saveRecord(record2);
            context.commit();

            versionstamp = context.getVersionStamp();
            assertEquals(2, context.claimLocalVersion());
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            FDBStoredRecord<Message> stored1 = recordStore.loadRecord(Tuple.from(1066L));
            assertThat(stored1.hasVersion(), is(true));
            FDBRecordVersion version1Prime = stored1.getVersion();
            assertNotNull(version1Prime);
            assertThat(version1Prime.isComplete(), is(true));
            assertEquals(0, version1Prime.getLocalVersion());
            assertArrayEquals(versionstamp, version1Prime.getGlobalVersion());
            assertThat(Arrays.equals(version1.getGlobalVersion(), version1Prime.getGlobalVersion()), is(false));
            assertNotEquals(version1, version1Prime);

            FDBStoredRecord<Message> stored2 = recordStore.loadRecord(Tuple.from(1776L));
            assertThat(stored1.hasVersion(), is(true));
            FDBRecordVersion version2Prime = stored2.getVersion();
            assertNotNull(version2Prime);
            assertThat(version2Prime.isComplete(), is(true));
            assertEquals(1, version2Prime.getLocalVersion());
            assertArrayEquals(versionstamp, version2Prime.getGlobalVersion());
            assertThat(Arrays.equals(version2.getGlobalVersion(), version2Prime.getGlobalVersion()), is(false));
            assertNotEquals(version2, version2Prime);
        }

        // Saving them again with an explicit version should keep the version the same.
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            recordStore.saveRecord(record1, version1);
            recordStore.saveRecord(record2, version2);
            context.commit();

            versionstamp = context.getVersionStamp();
            assertEquals(0, context.claimLocalVersion());
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            FDBStoredRecord<Message> stored1 = recordStore.loadRecord(Tuple.from(1066L));
            assertThat(stored1.hasVersion(), is(true));
            FDBRecordVersion version1Prime = stored1.getVersion();
            assertNotNull(version1Prime);
            assertThat(version1 == version1Prime, is(false));
            assertThat(version1Prime.isComplete(), is(true));
            assertEquals(0, version1Prime.getLocalVersion());
            assertThat(Arrays.equals(versionstamp, version1Prime.getGlobalVersion()), is(false));
            assertArrayEquals(version1.getGlobalVersion(), version1Prime.getGlobalVersion());
            assertEquals(version1, version1Prime);

            FDBStoredRecord<Message> stored2 = recordStore.loadRecord(Tuple.from(1776L));
            assertThat(stored2.hasVersion(), is(true));
            FDBRecordVersion version2Prime = stored2.getVersion();
            assertNotNull(version2Prime);
            assertThat(version2 == version2Prime, is(false));
            assertThat(version2Prime.isComplete(), is(true));
            assertEquals(1, version2Prime.getLocalVersion());
            assertThat(Arrays.equals(versionstamp, version2Prime.getGlobalVersion()), is(false));
            assertArrayEquals(version2.getGlobalVersion(), version2Prime.getGlobalVersion());
            assertEquals(version2, version2Prime);
        }

        // Saving new records with an explicit NO_VERSION behavior, should set the new records version to null
        MySimpleRecord record3 = MySimpleRecord.newBuilder().setRecNo(3066L).setNumValue2(42).build();
        TestRecords1Proto.MyOtherRecord record4 = TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(4776L).setNumValue2(1729).build();

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            assertThrows(RecordCoreException.class, () -> {
                recordStore.saveRecord(record3, version2, FDBRecordStoreBase.VersionstampSaveBehavior.NO_VERSION);
                fail("Save record with NO_VERSION behavior should throw an exception if the supplied version isn't null");
            });
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            recordStore.saveRecord(record3, null, FDBRecordStoreBase.VersionstampSaveBehavior.NO_VERSION);
            recordStore.saveRecord(record4, null, FDBRecordStoreBase.VersionstampSaveBehavior.NO_VERSION);
            context.commit();
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            FDBStoredRecord<Message> stored3 = recordStore.loadRecord(Tuple.from(3066L));
            assertThat(stored3.hasVersion(), is(false));

            FDBStoredRecord<Message> stored4 = recordStore.loadRecord(Tuple.from(4776L));
            assertThat(stored4.hasVersion(), is(false));
        }

        // Saving them again with an explicit WITH_VERSION behavior, should set them a version (identical to the
        // default behavior in this case).
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            recordStore.saveRecord(record3, null, FDBRecordStoreBase.VersionstampSaveBehavior.WITH_VERSION);
            recordStore.saveRecord(record4, null, FDBRecordStoreBase.VersionstampSaveBehavior.WITH_VERSION);
            context.commit();

            versionstamp = context.getVersionStamp();
            assertEquals(2, context.claimLocalVersion());
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            FDBStoredRecord<Message> stored3 = recordStore.loadRecord(Tuple.from(3066L));
            assertThat(stored3.hasVersion(), is(true));
            FDBRecordVersion version3Prime = stored3.getVersion();
            assertNotNull(version3Prime);
            assertThat(version3Prime.isComplete(), is(true));
            assertEquals(0, version3Prime.getLocalVersion());
            assertArrayEquals(versionstamp, version3Prime.getGlobalVersion());

            FDBStoredRecord<Message> stored4 = recordStore.loadRecord(Tuple.from(4776L));
            assertThat(stored4.hasVersion(), is(true));
            FDBRecordVersion version4Prime = stored4.getVersion();
            assertNotNull(version4Prime);
            assertThat(version4Prime.isComplete(), is(true));
            assertEquals(1, version4Prime.getLocalVersion());
            assertArrayEquals(versionstamp, version4Prime.getGlobalVersion());
        }
    }

    @ParameterizedTest(name = "saveLoadWithFunctionVersion [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void saveLoadWithFunctionVersion(int testFormatVersion, boolean testSplitLongRecords) throws ExecutionException, InterruptedException {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        MySimpleRecord recordCommitWithDummy = MySimpleRecord.newBuilder()
                .setRecNo(43L)
                .setNumValue2(43)
                .build();
        MySimpleRecord recordManualWithDummy = MySimpleRecord.newBuilder()
                .setRecNo(871L)
                .setNumValue2(871)
                .build();
        MySimpleRecord recordCommitWithoutDummy = MySimpleRecord.newBuilder()
                .setRecNo(1415L)
                .setNumValue2(1415)
                .build();
        MySimpleRecord recordManualWithoutDummy = MySimpleRecord.newBuilder()
                .setRecNo(1707L)
                .setNumValue2(1707)
                .build();

        FDBRecordVersion manualVersion;
        byte[] versionstamp;
        try (FDBRecordContext context = openContext(functionVersionHook)) {
            long readVersion = context.ensureActive().getReadVersion().get();
            manualVersion = FDBRecordVersion.firstInDBVersion(readVersion);
            FDBStoredRecord<Message> storedCommitWithDummy =  recordStore.saveRecord(recordCommitWithDummy);
            assertEquals(FDBRecordVersion.incomplete(0), storedCommitWithDummy.getVersion());
            FDBStoredRecord<Message> storedManualWithDummy = recordStore.saveRecord(recordManualWithDummy, manualVersion);
            assertEquals(manualVersion, storedManualWithDummy.getVersion());
            FDBStoredRecord<Message> storedCommitWithoutDummy = recordStore.saveRecord(recordCommitWithoutDummy);
            assertEquals(FDBRecordVersion.incomplete(1), storedCommitWithoutDummy.getVersion());
            FDBStoredRecord<Message> storedManualWithoutDummy = recordStore.saveRecord(recordManualWithoutDummy, manualVersion);
            assertEquals(manualVersion, storedManualWithoutDummy.getVersion());
            context.commit();
            versionstamp = context.getVersionStamp();
            assertNotNull(versionstamp);
        }

        try (FDBRecordContext context = openContext(functionVersionHook)) {
            // Scan the functional index
            List<Tuple> indexKeys = recordStore.scanIndex(metaData.getIndex("MySimpleRecord$maybeVersion"), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .map(IndexEntry::getKey).asList().get();
            List<Tuple> expectedKeys = Arrays.asList(
                    Tuple.from(FDBRecordVersion.MIN_VERSION.toVersionstamp(), 43L),
                    Tuple.from(FDBRecordVersion.MIN_VERSION.toVersionstamp(), 871L),
                    Tuple.from(manualVersion.toVersionstamp(), 1707L),
                    Tuple.from(Versionstamp.complete(versionstamp, 1), 1415L)
            );
            assertEquals(expectedKeys, indexKeys);
        }
    }

    @ParameterizedTest(name = "enableRecordVersionsAfterTheFact [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void enableRecordVersionsAfterTheFact(int testFormatVersion, boolean testSplitLongRecords) throws ExecutionException, InterruptedException {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        MySimpleRecord record1 = MySimpleRecord.newBuilder()
                .setRecNo(871L)
                .setNumValue2(871)
                .build();
        MySimpleRecord record2 = MySimpleRecord.newBuilder()
                .setRecNo(1415L)
                .setNumValue2(1415)
                .build();
        MySimpleRecord record3 = MySimpleRecord.newBuilder()
                .setRecNo(3415L)
                .setNumValue2(3415)
                .build();
        Index globalCountIndex = new Index("globalCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        try (FDBRecordContext context = openContext(metaDataBuilder -> {
            metaDataBuilder.addUniversalIndex(globalCountIndex);
            metaDataBuilder.setStoreRecordVersions(false);
        })) {
            assertThat(metaData.isStoreRecordVersions(), is(false));
            recordStore.saveRecord(record1);
            recordStore.saveRecord(record2);
            recordStore.saveRecord(record3, null, FDBRecordStoreBase.VersionstampSaveBehavior.WITH_VERSION);
            context.commit();
        }
        try (FDBRecordContext context = openContext(metaDataBuilder -> {
            metaDataBuilder.addUniversalIndex(globalCountIndex);
            metaDataBuilder.setStoreRecordVersions(false);
            metaDataBuilder.setStoreRecordVersions(true);
            functionVersionHook.apply(metaDataBuilder);
        })) {
            assertThat(metaData.isStoreRecordVersions(), is(true));
            FDBStoredRecord<Message> storedRecord1 = recordStore.loadRecord(Tuple.from(871L));
            assertNotNull(storedRecord1);
            assertEquals(record1, storedRecord1.getRecord());
            assertThat(storedRecord1.hasVersion(), is(false));
            FDBStoredRecord<Message> storedRecord2 = recordStore.loadRecord(Tuple.from(1415L));
            assertNotNull(storedRecord2);
            assertEquals(record2, storedRecord2.getRecord());
            assertThat(storedRecord2.hasVersion(), is(false));
            FDBStoredRecord<Message> storedRecord3 = recordStore.loadRecord(Tuple.from(3415L));
            assertNotNull(storedRecord3);
            assertEquals(record3, storedRecord3.getRecord());
            assertThat(storedRecord3.hasVersion(), is(true));

            RecordCursor<IndexEntry> cursor =  recordStore.scanIndex(metaData.getIndex("MySimpleRecord$maybeVersion"), IndexScanType.BY_VALUE,
                    TupleRange.ALL, null, ScanProperties.FORWARD_SCAN);
            assertEquals(Arrays.asList(
                    Tuple.from(null, 1415L),
                    Tuple.from(FDBRecordVersion.MIN_VERSION.toVersionstamp(), 871L),
                    Tuple.from(storedRecord3.getVersion().toVersionstamp(), 3415L)),
                    cursor.map(IndexEntry::getKey).asList().get());
        }
    }

    @ParameterizedTest(name = "removeWithVersion [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    public void removeWithVersion(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        MySimpleRecord record = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(42).build();

        FDBStoredRecord<Message> storedRecord;
        byte[] versionstamp;

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            storedRecord = recordStore.saveRecord(record);
            context.commit();

            versionstamp = context.getVersionStamp();
            assertEquals(1, context.claimLocalVersion());
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            RecordFunction<FDBRecordVersion> function = Query.version().getFunction();
            FDBRecordVersion version = recordStore.evaluateRecordFunction(function, storedRecord).join();
            assertNotNull(version);
            assertArrayEquals(versionstamp, version.getGlobalVersion());
            assertEquals(0, version.getLocalVersion());
            Optional<FDBRecordVersion> versionOptional = recordStore.loadRecordVersion(storedRecord.getPrimaryKey());
            assertThat(versionOptional.isPresent(), is(true));
            assertEquals(version, versionOptional.get());

            // Remove record saved within previous transaction
            boolean present = recordStore.deleteRecord(Tuple.from(1066L));
            assertThat(present, is(true));
            version = recordStore.evaluateRecordFunction(function, storedRecord).join();
            assertNull(version);
            versionOptional = recordStore.loadRecordVersion(Tuple.from(1066L));
            assertThat(versionOptional.isPresent(), is(false));

            present = recordStore.deleteRecord(Tuple.from(1066L));
            assertThat(present, is(false));
            version = recordStore.evaluateRecordFunction(function, storedRecord).join();
            assertNull(version);
            versionOptional = recordStore.loadRecordVersion(Tuple.from(1066L));
            assertThat(versionOptional.isPresent(), is(false));

            // Save a new record and verify version removed with it after it is deleted
            MySimpleRecord record2 = record.toBuilder().setRecNo(1415L).build();
            FDBStoredRecord<Message> storedRecord2 = recordStore.saveRecord(record2);
            assertThat(storedRecord2.hasVersion(), is(true));
            assertThat(storedRecord2.getVersion().isComplete(), is(false));
            version = recordStore.evaluateRecordFunction(function, storedRecord2).join();
            assertNotNull(version);
            assertEquals(storedRecord2.getVersion(), version);
            versionOptional = recordStore.loadRecordVersion(Tuple.from(1415L));
            assertThat(versionOptional.isPresent(), is(true));
            assertEquals(version, versionOptional.get());

            present = recordStore.deleteRecord(Tuple.from(1415L));
            assertThat(present, is(true));
            version = recordStore.evaluateRecordFunction(function, storedRecord2).join();
            assertNull(version);
            versionOptional = recordStore.loadRecordVersion(Tuple.from(1415L));
            assertThat(versionOptional.isPresent(), is(false));

            context.commit();
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            // Verify that the version added in the second record wasn't actually committed during the
            // pre-commit hook that writes all the versioned keys and values
            Optional<FDBRecordVersion> versionOptional = recordStore.loadRecordVersion(Tuple.from(1415L));
            assertThat(versionOptional.isPresent(), is(false));
            context.commit();
        }
    }

    @ParameterizedTest(name = "saveLoadWithRepeatedVersion [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void scanWithIncompleteVersion(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        MySimpleRecord record1 = MySimpleRecord.newBuilder().setRecNo(1066L).build();
        MySimpleRecord record2 = MySimpleRecord.newBuilder().setRecNo(1415L).build();
        byte[] globalVersion;
        List<FDBStoredRecord<Message>> savedRecords;

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            FDBStoredRecord<Message> storedRecord1 = recordStore.saveRecord(record1);
            FDBStoredRecord<Message> storedRecord2 = recordStore.saveRecord(record2);

            List<FDBStoredRecord<Message>> scannedRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(Arrays.asList(storedRecord1, storedRecord2), scannedRecords);

            scannedRecords = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().join();
            assertEquals(Arrays.asList(storedRecord2, storedRecord1), scannedRecords);

            context.commit();

            globalVersion = context.getVersionStamp();
            assertNotNull(globalVersion);

            assertNotNull(storedRecord1.getVersion());
            assertNotNull(storedRecord2.getVersion());
            savedRecords = Arrays.asList(
                    storedRecord1.withVersion(FDBRecordVersion.complete(globalVersion, storedRecord1.getVersion().getLocalVersion())),
                    storedRecord2.withVersion(FDBRecordVersion.complete(globalVersion, storedRecord2.getVersion().getLocalVersion()))
            );
        }
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            List<FDBStoredRecord<Message>> scannedRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join();
            assertEquals(savedRecords, scannedRecords);

            scannedRecords = recordStore.scanRecords(null, ScanProperties.REVERSE_SCAN).asList().join();
            assertEquals(Lists.reverse(savedRecords), scannedRecords);
            context.commit();
        }
    }

    @ParameterizedTest(name = "saveLoadWithRepeatedVersion [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void saveLoadWithRepeatedVersion(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        MySimpleRecord simpleRecord1 = MySimpleRecord.newBuilder().setRecNo(1066L).addRepeater(1).addRepeater(2).addRepeater(3).build();
        MySimpleRecord simpleRecord2 = MySimpleRecord.newBuilder().setRecNo(1729L).addRepeater(1).build();
        MySimpleRecord simpleRecord3 = MySimpleRecord.newBuilder().setRecNo(1776L).build();

        byte[] versionstamp;

        try (FDBRecordContext context = openContext(repeatedVersionHook)) {
            recordStore.saveRecord(simpleRecord1);
            recordStore.saveRecord(simpleRecord2);
            recordStore.saveRecord(simpleRecord3);

            context.commit();
            versionstamp = context.getVersionStamp();
            assertEquals(3, context.claimLocalVersion());
        }

        try (FDBRecordContext context = openContext(repeatedVersionHook)) {
            FDBStoredRecord<Message> stored1 = recordStore.loadRecord(Tuple.from(1066L));
            assertThat(stored1.hasVersion(), is(true));
            FDBRecordVersion version1 = stored1.getVersion();
            assertNotNull(version1);
            assertArrayEquals(versionstamp, version1.getGlobalVersion());
            assertEquals(0, version1.getLocalVersion());

            FDBStoredRecord<Message> stored2 = recordStore.loadRecord(Tuple.from(1729L));
            assertThat(stored2.hasVersion(), is(true));
            FDBRecordVersion version2 = stored2.getVersion();
            assertNotNull(version2);
            assertArrayEquals(versionstamp, version2.getGlobalVersion());
            assertEquals(1, version2.getLocalVersion());

            FDBStoredRecord<Message> stored3 = recordStore.loadRecord(Tuple.from(1776L));
            assertThat(stored3.hasVersion(), is(true));
            FDBRecordVersion version3 = stored3.getVersion();
            assertNotNull(version3);
            assertArrayEquals(versionstamp, version3.getGlobalVersion());
            assertEquals(2, version3.getLocalVersion());
        }
    }

    @ParameterizedTest(name = "saveLoadWithRepeatedAndCompoundVersion [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void saveLoadWithRepeatedAndCompoundVersion(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        MySimpleRecord simpleRecord1 = MySimpleRecord.newBuilder().setRecNo(1066L).addRepeater(1).addRepeater(2).addRepeater(3).build();
        MySimpleRecord simpleRecord2 = MySimpleRecord.newBuilder().setRecNo(1729L).addRepeater(1).build();
        MySimpleRecord simpleRecord3 = MySimpleRecord.newBuilder().setRecNo(1776L).build();

        byte[] versionstamp;

        try (FDBRecordContext context = openContext(repeatedAndCompoundVersionHook)) {
            recordStore.saveRecord(simpleRecord1);
            recordStore.saveRecord(simpleRecord2);
            recordStore.saveRecord(simpleRecord3);

            context.commit();
            versionstamp = context.getVersionStamp();
            assertEquals(3, context.claimLocalVersion());
        }

        try (FDBRecordContext context = openContext(repeatedAndCompoundVersionHook)) {
            FDBStoredRecord<Message> stored1 = recordStore.loadRecord(Tuple.from(1066L));
            assertThat(stored1.hasVersion(), is(true));
            FDBRecordVersion version1 = stored1.getVersion();
            assertNotNull(version1);
            assertArrayEquals(versionstamp, version1.getGlobalVersion());
            assertEquals(0, version1.getLocalVersion());

            FDBStoredRecord<Message> stored2 = recordStore.loadRecord(Tuple.from(1729L));
            assertThat(stored2.hasVersion(), is(true));
            FDBRecordVersion version2 = stored2.getVersion();
            assertNotNull(version2);
            assertArrayEquals(versionstamp, version2.getGlobalVersion());
            assertEquals(1, version2.getLocalVersion());

            FDBStoredRecord<Message> stored3 = recordStore.loadRecord(Tuple.from(1776L));
            assertThat(stored3.hasVersion(), is(true));
            FDBRecordVersion version3 = stored3.getVersion();
            assertNotNull(version3);
            assertArrayEquals(versionstamp, version3.getGlobalVersion());
            assertEquals(2, version3.getLocalVersion());
        }
    }

    @ParameterizedTest(name = "updateWithinContext [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void updateWithinContext(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        testSplitLongRecords = testSplitLongRecords;

        MySimpleRecord record1 = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(42).setNumValue3Indexed(1).build();
        MySimpleRecord record2 = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(42).setNumValue3Indexed(2).build();
        MySimpleRecord record3 = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(43).setNumValue3Indexed(2).build();

        MySimpleRecord record4 = MySimpleRecord.newBuilder().setRecNo(1776L).setNumValue2(42).setNumValue3Indexed(1).build();

        byte[] versionstamp;

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            FDBRecordVersion version = FDBRecordVersion.incomplete(context.claimLocalVersion());
            FDBStoredRecord<?> stored1 = recordStore.saveRecord(record1, version);
            assertThat(stored1.hasVersion(), is(true));
            assertEquals(stored1.getVersion().getLocalVersion(), 0);
            assertThat(stored1.getVersion().isComplete(), is(false));

            FDBStoredRecord<?> stored1a = recordStore.saveRecord(record1, version); // Save same again. Should be idempotent.
            assertThat(stored1a.hasVersion(), is(true));
            assertEquals(stored1a.getVersion().getLocalVersion(), 0);
            assertThat(stored1a.getVersion().isComplete(), is(false));
            assertEquals(stored1, stored1a);

            FDBStoredRecord<?> stored2 = recordStore.saveRecord(record2, version); // Save record. Shouldn't update version information.
            assertThat(stored1.hasVersion(), is(true));
            assertEquals(stored2.getVersion().getLocalVersion(), 0);
            assertThat(stored2.getVersion().isComplete(), is(false));
            assertEquals(stored1.getPrimaryKey(), stored2.getPrimaryKey());
            assertEquals(stored1.getVersion(), stored2.getVersion());

            FDBStoredRecord<?> stored3 = recordStore.saveRecord(record3, version); // Save record. Shouldn't update version information
            assertThat(stored3.hasVersion(), is(true));
            assertEquals(stored3.getVersion().getLocalVersion(), 0);
            assertThat(stored3.getVersion().isComplete(), is(false));
            assertEquals(stored1.getPrimaryKey(), stored3.getPrimaryKey());
            assertEquals(stored1.getVersion(), stored3.getVersion());

            FDBStoredRecord<?> stored4 = recordStore.saveRecord(record4); // New record.
            assertThat(stored4.hasVersion(), is(true));
            assertEquals(stored4.getVersion().getLocalVersion(), 1);
            assertThat(stored4.getVersion().isComplete(), is(false));

            FDBStoredRecord<?> stored4a = recordStore.saveRecord(record4); // Same record. New version.
            assertThat(stored4a.hasVersion(), is(true));
            assertEquals(stored4a.getVersion().getLocalVersion(), 2);
            assertThat(stored4a.getVersion().isComplete(), is(false));

            context.commit();
            versionstamp = context.getVersionStamp();
            assertEquals(3, context.claimLocalVersion());
        }

        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            Optional<FDBRecordVersion> storedVersionOptional = recordStore.loadRecordVersion(Tuple.from(1066L));
            assertThat(storedVersionOptional.isPresent(), is(true));
            assertEquals(FDBRecordVersion.complete(versionstamp, 0), storedVersionOptional.get());

            Optional<FDBRecordVersion> storedVersionOptional2 = recordStore.loadRecordVersion(Tuple.from(1776L));
            assertThat(storedVersionOptional2.isPresent(), is(true));
            assertEquals(FDBRecordVersion.complete(versionstamp, 2), storedVersionOptional2.get());

            // Verify that there are only two entries in the index.
            assertEquals(
                    Arrays.asList(
                        Tuple.from(FDBRecordVersion.complete(versionstamp, 0).toVersionstamp(), 1066L),
                        Tuple.from(FDBRecordVersion.complete(versionstamp, 2).toVersionstamp(), 1776L)),
                    recordStore.scanIndex(metaData.getIndex("globalVersion"), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .map(IndexEntry::getKey).asList().join()
            );
            assertEquals(
                    Arrays.asList(
                        Tuple.from(42, FDBRecordVersion.complete(versionstamp, 2).toVersionstamp(), 1776L),
                        Tuple.from(43, FDBRecordVersion.complete(versionstamp, 0).toVersionstamp(), 1066L)),
                    recordStore.scanIndex(metaData.getIndex("MySimpleRecord$num2-version"), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                            .map(IndexEntry::getKey).asList().join()
            );
        }
    }

    @ParameterizedTest(name = "queryOnVersion [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void queryOnVersion(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        List<MySimpleRecord> simpleRecords = IntStream.range(0, 30)
                .mapToObj(id -> MySimpleRecord.newBuilder().setRecNo(id * 2).setNumValue2(id % 2).setNumValue3Indexed(id % 3).build())
                .collect(Collectors.toList());
        List<TestRecords1Proto.MyOtherRecord> otherRecords = IntStream.range(0, 30)
                .mapToObj(id -> TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(id * 2 + 1).setNumValue2(id % 2).setNumValue3Indexed(id % 3).build())
                .collect(Collectors.toList());

        Iterator<MySimpleRecord> simpleIterator = simpleRecords.iterator();
        Iterator<TestRecords1Proto.MyOtherRecord> otherIterator = otherRecords.iterator();

        while (simpleIterator.hasNext()) {
            try (FDBRecordContext context = openContext(simpleVersionHook)) {
                int done = 0;
                while (simpleIterator.hasNext() && done != 5) {
                    recordStore.saveRecord(simpleIterator.next());
                    recordStore.saveRecord(otherIterator.next());
                    done += 1;
                }
                context.commit();
            }
        }

        // Query all records.
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            List<Long> expectedKeys = Stream.concat(
                        simpleRecords.stream().map(MySimpleRecord::getRecNo),
                        otherRecords.stream().map(TestRecords1Proto.MyOtherRecord::getRecNo))
                    .sorted()
                    .collect(Collectors.toList());

            FDBRecordVersion last = null;
            List<Long> receivedKeys = new ArrayList<>();
            int totalSeen = 0;

            while (true) {
                RecordQueryPlan plan;

                if (last == null) {
                    RecordQuery query = RecordQuery.newBuilder().setSort(VersionKeyExpression.VERSION).build();
                    plan = planner.plan(query);
                    assertEquals("Index(globalVersion <,>)", plan.toString());
                } else {
                    RecordQuery query = RecordQuery.newBuilder()
                            .setFilter(Query.version().greaterThan(last))
                            .setSort(VersionKeyExpression.VERSION)
                            .build();
                    plan = planner.plan(query);
                    assertEquals("Index(globalVersion ([" + last.toVersionstamp() + "],>)", plan.toString());
                }

                RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asIterator();
                boolean hasAny = false;
                while (cursor.hasNext()) {
                    hasAny = true;
                    FDBQueriedRecord<Message> record = cursor.next();
                    assertThat(record.hasVersion(), is(true));
                    if (last != null) {
                        assertThat(last, lessThan(record.getVersion()));
                    }
                    last = record.getVersion();

                    receivedKeys.add(field("rec_no").evaluateSingleton(record.getStoredRecord()).toTuple().getLong(0));
                    totalSeen += 1;
                }

                if (!hasAny) {
                    break;
                }
            }

            assertEquals(simpleRecords.size() + otherRecords.size(), totalSeen);
            assertEquals(expectedKeys, receivedKeys);
        }

        // Query MySimpleRecord based on value.
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            List<Long> expectedKeys = simpleRecords.stream().filter(rec -> rec.getNumValue2() == 0)
                    .map(MySimpleRecord::getRecNo).collect(Collectors.toList());

            List<Long> receivedKeys = new ArrayList<>();
            FDBRecordVersion last = null;
            int totalSeen = 0;

            while (true) {
                RecordCursorIterator<? extends FDBRecord<Message>> cursor;

                if (last == null) {
                    RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                            .setFilter(Query.field("num_value_2").equalsValue(0))
                            .setSort(VersionKeyExpression.VERSION)
                            .build();
                    RecordQueryPlan plan = planner.plan(query);
                    assertEquals("Index(MySimpleRecord$num2-version [[0],[0]])", plan.toString());
                    cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(3).build())
                            .asIterator();
                } else {
                    RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                            .setFilter(Query.and(Query.field("num_value_2").equalsValue(0), Query.version().greaterThan(last)))
                            .setSort(VersionKeyExpression.VERSION)
                            .build();
                    RecordQueryPlan plan = planner.plan(query);
                    assertEquals("Index(MySimpleRecord$num2-version ([0, " + last.toVersionstamp() + "],[0]])", plan.toString());
                    cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(3).build())
                            .asIterator();
                }

                boolean hasAny = false;
                while (cursor.hasNext()) {
                    hasAny = true;
                    FDBRecord<Message> record = cursor.next();
                    MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().mergeFrom(record.getRecord()).build();
                    assertEquals(0, simpleRecord.getNumValue2());
                    assertThat(record.hasVersion(), is(true));
                    if (last != null) {
                        assertThat(last, lessThan(record.getVersion()));
                    }
                    last = record.getVersion();

                    receivedKeys.add(simpleRecord.getRecNo());
                    totalSeen += 1;
                }

                if (!hasAny) {
                    break;
                }
            }

            assertEquals((simpleRecords.size() + 1) / 2, totalSeen);
            assertEquals(expectedKeys, receivedKeys);
        }

        // Query that requires also filtering
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            List<Long> expectedKeys = simpleRecords.stream().filter(rec -> rec.getNumValue2() == 0 && rec.getNumValue3Indexed() == 0)
                    .map(MySimpleRecord::getRecNo).collect(Collectors.toList());

            List<Long> receivedKeys = new ArrayList<>();
            FDBRecordVersion last = null;
            int totalSeen = 0;

            while (true) {
                RecordCursorIterator<? extends FDBRecord<Message>> cursor;

                if (last == null) {
                    RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                            .setFilter(Query.and(Query.field("num_value_2").equalsValue(0), Query.field("num_value_3_indexed").equalsValue(0)))
                            .setSort(VersionKeyExpression.VERSION)
                            .build();
                    RecordQueryPlan plan = planner.plan(query);
                    assertEquals("Index(MySimpleRecord$num2-version [[0],[0]]) | num_value_3_indexed EQUALS 0", plan.toString());
                    cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(2).build())
                            .asIterator();
                } else {
                    RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                            .setFilter(Query.and(
                                    Query.field("num_value_2").equalsValue(0),
                                    Query.field("num_value_3_indexed").equalsValue(0),
                                    Query.version().greaterThan(last)
                            ))
                            .setSort(VersionKeyExpression.VERSION)
                            .build();
                    RecordQueryPlan plan = planner.plan(query);
                    assertEquals("Index(MySimpleRecord$num2-version ([0, " + last.toVersionstamp() + "],[0]]) | num_value_3_indexed EQUALS 0", plan.toString());
                    cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(2).build())
                            .asIterator();
                }

                boolean hasAny = false;
                while (cursor.hasNext()) {
                    hasAny = true;
                    FDBRecord<Message> record = cursor.next();
                    MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().mergeFrom(record.getRecord()).build();
                    assertEquals(0, simpleRecord.getNumValue2());
                    assertThat(record.hasVersion(), is(true));
                    if (last != null) {
                        assertThat(last, lessThan(record.getVersion()));
                    }
                    last = record.getVersion();

                    receivedKeys.add(simpleRecord.getRecNo());
                    totalSeen += 1;
                }

                if (!hasAny) {
                    break;
                }
            }

            assertEquals(simpleRecords.size() / 6, totalSeen);
            assertEquals(expectedKeys, receivedKeys);
        }

        // Query that can't be satisfied with an index scan.
        try (FDBRecordContext context = openContext(simpleVersionHook)) {
            // Preliminary query to get a read version.
            RecordQuery prelimQuery = RecordQuery.newBuilder()
                    .setSort(VersionKeyExpression.VERSION)
                    .build();
            RecordQueryPlan prelimPlan = planner.plan(prelimQuery);
            FDBRecordVersion chosenVersion = recordStore.executeQuery(prelimPlan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(10).build()).asList().thenApply(list -> list.get(list.size() - 1).getVersion()).join();

            RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                    .setFilter(Query.version().greaterThan(chosenVersion))
                    .setSort(field("num_value_3_indexed"))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertEquals("Index(MySimpleRecord$num_value_3_indexed <,>) | version GREATER_THAN " + chosenVersion.toString(), plan.toString());
            List<FDBQueriedRecord<Message>> records = recordStore.executeQuery(plan).asList().join();

            int last = -1;
            for (FDBQueriedRecord<Message> record : records) {
                MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().mergeFrom(record.getRecord()).build();
                assertThat(last, lessThanOrEqualTo(simpleRecord.getNumValue3Indexed()));
                assertThat(record.hasVersion(), is(true));
                assertThat(chosenVersion, lessThan(record.getVersion()));

                last = simpleRecord.getNumValue3Indexed();
            }

            assertEquals(simpleRecords.size() - 5, records.size());
        }
    }

    @ParameterizedTest(name = "queryOnRepeatedVersions [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void queryOnRepeatedVersion(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        List<MySimpleRecord> simpleRecords = IntStream.range(0, 30)
                .mapToObj(id -> {
                    MySimpleRecord.Builder builder = MySimpleRecord.newBuilder().setRecNo(id * 2).setNumValue2(id % 2).setNumValue3Indexed(id % 3);
                    for (int i = 0; i < id % 3; i++) {
                        builder.addRepeater(i);
                    }
                    return builder.build();
                })
                .collect(Collectors.toList());

        Iterator<MySimpleRecord> simpleIterator = simpleRecords.iterator();

        while (simpleIterator.hasNext()) {
            try (FDBRecordContext context = openContext(repeatedVersionHook)) {
                int done = 0;
                while (simpleIterator.hasNext() && done != 5) {
                    recordStore.saveRecord(simpleIterator.next());
                    done += 1;
                }
                context.commit();
            }
        }

        try (FDBRecordContext context = openContext(repeatedVersionHook)) {
            List<Long> expectedKeys =  simpleRecords.stream().filter(rec -> rec.getRepeaterList().contains(1))
                    .map(MySimpleRecord::getRecNo).collect(Collectors.toList());

            FDBRecordVersion last = null;
            List<Long> receivedKeys = new ArrayList<>();
            int totalSeen = 0;

            while (true) {
                RecordQueryPlan plan;

                if (last == null) {
                    RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                            .setFilter(Query.field("repeater").oneOfThem().equalsValue(1))
                            .setSort(VersionKeyExpression.VERSION)
                            .setRemoveDuplicates(false)
                            .build();
                    plan = planner.plan(query);
                    assertEquals("Index(MySimpleRecord$repeater-version [[1],[1]])", plan.toString());
                } else {
                    RecordQuery query = RecordQuery.newBuilder().setRecordType("MySimpleRecord")
                            .setFilter(Query.and(Query.field("repeater").oneOfThem().equalsValue(1), Query.version().greaterThan(last)))
                            .setSort(VersionKeyExpression.VERSION)
                            .setRemoveDuplicates(false)
                            .build();
                    plan = planner.plan(query);
                    assertEquals("Index(MySimpleRecord$repeater-version ([1, " + last.toVersionstamp() + "],[1]])", plan.toString());
                }

                RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore
                        .executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(4).build())
                        .asIterator();
                boolean hasAny = false;
                while (cursor.hasNext()) {
                    hasAny = true;
                    FDBQueriedRecord<Message> record = cursor.next();
                    assertThat(record.hasVersion(), is(true));
                    if (last != null) {
                        assertThat(last, lessThan(record.getVersion()));
                    }
                    last = record.getVersion();

                    MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().mergeFrom(record.getRecord()).build();
                    assertThat(simpleRecord.getRepeaterList(), hasItem(1));

                    receivedKeys.add(field("rec_no").evaluateSingleton(record.getStoredRecord()).toTuple().getLong(0));
                    totalSeen += 1;
                }

                if (!hasAny) {
                    break;
                }
            }

            assertEquals(simpleRecords.size() / 3, totalSeen);
            assertEquals(expectedKeys, receivedKeys);
        }
    }

    @ParameterizedTest(name = "withMetaDataRebuilds [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void withMetaDataRebuilds(int testFormatVersion, boolean testSplitLongRecords) {
        formatVersion = testFormatVersion;
        splitLongRecords = testSplitLongRecords;

        RecordMetaDataHook firstHook = metaDataBuilder -> {
            metaDataBuilder.setSplitLongRecords(splitLongRecords);
            metaDataBuilder.addUniversalIndex(new Index("globalCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
            metaDataBuilder.addUniversalIndex(new Index("globalVersion", VersionKeyExpression.VERSION, IndexTypes.VERSION));
        };
        RecordMetaDataHook secondHook = metaDataBuilder -> {
            firstHook.apply(metaDataBuilder);
            metaDataBuilder.removeIndex("globalVersion");
            metaDataBuilder.setStoreRecordVersions(false);
        };
        RecordMetaDataHook thirdHook = metaDataBuilder -> {
            secondHook.apply(metaDataBuilder);
            metaDataBuilder.setStoreRecordVersions(true);
            metaDataBuilder.addUniversalIndex(new Index("globalVersion2", VersionKeyExpression.VERSION, IndexTypes.VERSION));
        };

        MySimpleRecord record1 = MySimpleRecord.newBuilder().setRecNo(1066L).build();
        FDBRecordVersion version1;
        MySimpleRecord record2 = MySimpleRecord.newBuilder().setRecNo(1776L).build();
        MySimpleRecord record3 = MySimpleRecord.newBuilder().setRecNo(1955L).build();
        FDBRecordVersion version3;

        RecordQuery query = RecordQuery.newBuilder().setSort(VersionKeyExpression.VERSION).build();

        // First with version on.
        try (FDBRecordContext context = openContext(firstHook)) {
            FDBStoredRecord<?> storedRecord = recordStore.saveRecord(record1);
            assertThat(storedRecord.hasVersion(), is(true));
            context.commit();
            version1 = FDBRecordVersion.complete(context.getVersionStamp(), storedRecord.getVersion().getLocalVersion());
        }
        try (FDBRecordContext context = openContext(firstHook)) {
            FDBStoredRecord<?> loadedRecord = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(loadedRecord);
            assertThat(loadedRecord.hasVersion(), is(true));
            assertEquals(version1, loadedRecord.getVersion());

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(indexName("globalVersion")));
            List<FDBQueriedRecord<Message>> records = recordStore.executeQuery(plan).asList().join();
            assertEquals(1, records.size());
            FDBQueriedRecord<Message> queriedRecord = records.get(0);
            assertEquals(Tuple.from(1066L), queriedRecord.getPrimaryKey());
            assertThat(queriedRecord.hasVersion(), is(true));
            assertEquals(version1, queriedRecord.getVersion());
        }

        // Now with version off.
        try (FDBRecordContext context = openContext(secondHook)) {
            FDBStoredRecord<?> storedRecord = recordStore.saveRecord(record2);
            assertThat(storedRecord.hasVersion(), is(false));
            context.commit();
        }
        try (FDBRecordContext context = openContext(secondHook)) {
            FDBStoredRecord<?> loadedRecord1 = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(loadedRecord1);
            assertThat(loadedRecord1.hasVersion(), is(testFormatVersion >= FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION));
            FDBStoredRecord<?> loadedRecord2 = recordStore.loadRecord(Tuple.from(1776L));
            assertNotNull(loadedRecord2);
            assertThat(loadedRecord2.hasVersion(), is(false));

            assertThrows(RecordCoreException.class, () -> {
                RecordQueryPlan plan = planner.plan(query);
                fail("Came up with plan " + plan.toString() + " when it should be impossible");
            });
        }

        // Now with version back on.
        try (FDBRecordContext context = openContext(thirdHook)) {
            FDBStoredRecord<?> storedRecord = recordStore.saveRecord(record3);
            assertThat(storedRecord.hasVersion(), is(true));
            context.commit();
            version3 = FDBRecordVersion.complete(context.getVersionStamp(), storedRecord.getVersion().getLocalVersion());
        }
        try (FDBRecordContext context = openContext(thirdHook)) {
            FDBStoredRecord<?> loadedRecord1 = recordStore.loadRecord(Tuple.from(1066L));
            assertThat(loadedRecord1.hasVersion(), is(testFormatVersion >= FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION));
            FDBStoredRecord<?> loadedRecord2 = recordStore.loadRecord(Tuple.from(1776L));
            assertThat(loadedRecord2.hasVersion(), is(false));
            FDBStoredRecord<?> loadedRecord3 = recordStore.loadRecord(Tuple.from(1955L));
            assertThat(loadedRecord3.hasVersion(), is(true));
            assertEquals(version3, loadedRecord3.getVersion());

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(indexName("globalVersion2")));
            List<FDBQueriedRecord<Message>> records = recordStore.executeQuery(plan).asList().join();
            assertEquals(3, records.size());

            if (testFormatVersion < FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION) {
                FDBQueriedRecord<Message> queriedRecord1 = records.get(0);
                assertEquals(Tuple.from(1066L), queriedRecord1.getPrimaryKey());
                assertThat(queriedRecord1.hasVersion(), is(false));

                FDBQueriedRecord<Message> queriedRecord2 = records.get(1);
                assertEquals(Tuple.from(1776L), queriedRecord2.getPrimaryKey());
                assertThat(queriedRecord2.hasVersion(), is(false));
            } else {
                FDBQueriedRecord<Message> queriedRecord1 = records.get(0);
                assertEquals(Tuple.from(1776L), queriedRecord1.getPrimaryKey());
                assertThat(queriedRecord1.hasVersion(), is(false));

                FDBQueriedRecord<Message> queriedRecord2 = records.get(1);
                assertEquals(Tuple.from(1066L), queriedRecord2.getPrimaryKey());
                assertThat(queriedRecord2.hasVersion(), is(true));
                assertEquals(version1, queriedRecord2.getVersion());
            }

            FDBQueriedRecord<Message> queriedRecord3 = records.get(2);
            assertEquals(Tuple.from(1955L), queriedRecord3.getPrimaryKey());
            assertThat(queriedRecord3.hasVersion(), is(true));
            assertEquals(version3, queriedRecord3.getVersion());
        }
    }

    @Test
    public void invalidIndexes() {
        List<KeyExpression> expressions = Arrays.asList(
                field("num_value_2"),
                field("num_value_2").groupBy(field("num_value_3_indexed")),
                field("num_value_2").groupBy(VersionKeyExpression.VERSION),
                concat(field("num_value_2"), VersionKeyExpression.VERSION).groupBy(field("num_value_3_indexed")),
                concat(VersionKeyExpression.VERSION, field("num_value_2")).groupBy(field("num_value_3_indexed")),
                concat(VersionKeyExpression.VERSION, VersionKeyExpression.VERSION)
        );

        for (KeyExpression expression : expressions) {
            assertThrows(KeyExpression.InvalidExpressionException.class, () -> {
                Index index = new Index("test_index", expression, IndexTypes.VERSION);
                RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
                metaDataBuilder.addIndex("MySimpleRecord", index);
                metaDataBuilder.getRecordMetaData();
            });
        }

        assertThrows(MetaDataException.class, () -> {
            Index index = new Index("test_index", VersionKeyExpression.VERSION, EmptyKeyExpression.EMPTY, IndexTypes.VERSION, IndexOptions.UNIQUE_OPTIONS);
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
            metaDataBuilder.addIndex("MySimpleRecord", index);
            metaDataBuilder.getRecordMetaData();
        });

        assertThrows(MetaDataException.class, () -> {
            Index index = new Index("global_version", VersionKeyExpression.VERSION, IndexTypes.VERSION);
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords2Proto.getDescriptor());
            metaDataBuilder.addUniversalIndex(index);
            metaDataBuilder.getRecordMetaData();
        });
    }

    @ParameterizedTest(name = "upgradeFormatVersions [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionArguments")
    @SuppressWarnings("try")
    public void upgradeFormatVersions(int testFormatVersion, boolean splitLongRecords) {
        formatVersion = testFormatVersion;
        final RecordMetaDataHook hook = metaDataBuilder -> {
            simpleVersionHook.apply(metaDataBuilder);
            metaDataBuilder.setSplitLongRecords(splitLongRecords);
        };

        final List<Message> records = Arrays.asList(
                MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(1).build(),
                MySimpleRecord.newBuilder().setRecNo(1415L).setNumValue2(1).build(),
                MySimpleRecord.newBuilder().setRecNo(1776L).setNumValue2(2).build()
        );

        List<FDBStoredRecord<Message>> storedRecords;
        try (FDBRecordContext context = openContext(hook)) {
            List<FDBStoredRecord<Message>> storedRecordsWithIncompletes = records.stream().map(recordStore::saveRecord).collect(Collectors.toList());
            context.commit();
            byte[] globalVersion = context.getVersionStamp();
            storedRecords = storedRecordsWithIncompletes.stream()
                    .map(record -> record.withCommittedVersion(globalVersion))
                    .collect(Collectors.toList());
        }
        try (FDBRecordContext context = openContext(hook)) {
            if (testFormatVersion < FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION) {
                validateUsingOlderVersionFormat(storedRecords);
            } else {
                validateUsingNewerVersionFormat(storedRecords);
            }
        }

        // Update to the current format version
        formatVersion = FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION;

        if (testFormatVersion < FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION &&
                (splitLongRecords || testFormatVersion >= FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION)) {
            // After format version upgrade, each record should now store that it has the version inlined
            storedRecords = storedRecords.stream()
                    .map(record -> new FDBStoredRecord<>(record.getPrimaryKey(), record.getRecordType(), record.getRecord(),
                            record.getKeyCount() + 1, record.getKeySize() * 2 + 1, record.getValueSize() + 1 + FDBRecordVersion.VERSION_LENGTH,
                            record.isSplit(), true, record.getVersion()))
                    .collect(Collectors.toList());
        }

        try (FDBRecordContext context = openContext(hook)) {
            if (!splitLongRecords && testFormatVersion < FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION) {
                validateUsingOlderVersionFormat(storedRecords);
            } else {
                validateUsingNewerVersionFormat(storedRecords);
            }

            for (FDBStoredRecord<Message> storedRecord : storedRecords) {
                Optional<FDBRecordVersion> loadedVersion = recordStore.loadRecordVersion(storedRecord.getPrimaryKey());
                assertThat(loadedVersion.isPresent(), is(true));
                assertEquals(storedRecord.getVersion(), loadedVersion.get());

                RecordQuery query = RecordQuery.newBuilder()
                        .setFilter(Query.version().equalsValue(storedRecord.getVersion()))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                final String endpointString = "[" + storedRecord.getVersion().toVersionstamp(false).toString() + "]";
                assertThat(plan, indexScan(allOf(indexName("globalVersion"), bounds(hasTupleString("[" + endpointString + "," + endpointString + "]")))));
                List<FDBStoredRecord<Message>> queriedRecords = recordStore.executeQuery(plan).map(FDBQueriedRecord::getStoredRecord).asList().join();
                assertEquals(Collections.singletonList(storedRecord), queriedRecords);
            }

            assertThat(recordStore.deleteRecord(storedRecords.get(0).getPrimaryKey()), is(true));
            final List<FDBStoredRecord<Message>> fewerRecords = storedRecords.subList(1, storedRecords.size());
            if (!splitLongRecords && testFormatVersion < FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION) {
                validateUsingOlderVersionFormat(fewerRecords);
            } else {
                validateUsingNewerVersionFormat(fewerRecords);
            }

            recordStore.saveRecord(storedRecords.get(0).getRecord());
            if (!splitLongRecords && testFormatVersion < FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION) {
                validateUsingOlderVersionFormat(fewerRecords);
            } else {
                validateUsingNewerVersionFormat(fewerRecords);
            }

            // do not commit (so we can do a second upgrade)
        }

        final Index newValueIndex = new Index("MySimpleRecord$num2", field("num_value_2"), IndexTypes.VALUE);
        final Index newVersionIndex = new Index("MySimpleRecord$version-num2", concat(VersionKeyExpression.VERSION, field("num_value_2")), IndexTypes.VERSION);
        final RecordMetaDataHook hookWithNewIndexes = metaDataBuilder -> {
            hook.apply(metaDataBuilder);
            metaDataBuilder.addIndex("MySimpleRecord", newValueIndex);
            metaDataBuilder.addIndex("MySimpleRecord", newVersionIndex);
        };

        final List<FDBStoredRecord<Message>> newStoredRecords;
        try (FDBRecordContext context = openContext(hookWithNewIndexes)) {
            assertThat(recordStore.getRecordStoreState().isReadable(newValueIndex), is(true));
            boolean performedMigration = testFormatVersion < FDBRecordStore.SAVE_VERSION_WITH_RECORD_FORMAT_VERSION
                                         && (splitLongRecords || testFormatVersion >= FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION);
            assertThat(recordStore.getRecordStoreState().isReadable(newVersionIndex), is(!performedMigration));

            if (recordStore.getRecordStoreState().isReadable(newVersionIndex)) {
                // Validate versions are the same for all records in index and in primary store
                List<Pair<Tuple, FDBRecordVersion>> recordScannedValues = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                        .map(record -> Pair.of(record.getPrimaryKey(), record.getVersion()))
                        .asList()
                        .join();
                List<Pair<Tuple, FDBRecordVersion>> indexedScannedValues = recordStore.scanIndex(newVersionIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .map(indexEntry ->  Pair.of(TupleHelpers.subTuple(indexEntry.getKey(), 2, indexEntry.getKey().size()), FDBRecordVersion.fromVersionstamp(indexEntry.getKey().getVersionstamp(0), false)))
                        .asList()
                        .join();
                assertEquals(recordScannedValues, indexedScannedValues);
            }

            // Save record at newer version
            assertThat(recordStore.deleteRecord(storedRecords.get(0).getPrimaryKey()), is(true));
            FDBStoredRecord<Message> newRecord0 = recordStore.saveRecord(storedRecords.get(0).getRecord());
            FDBStoredRecord<Message> newRecord2 = recordStore.saveRecord(storedRecords.get(2).getRecord());

            assertEquals(newRecord0.getVersion(), recordStore.loadRecordVersion(newRecord0.getPrimaryKey()).get());
            assertEquals(newRecord2.getVersion(), recordStore.loadRecordVersion(newRecord2.getPrimaryKey()).get());

            context.commit();
            byte[] versionstamp = context.getVersionStamp();
            newStoredRecords = Arrays.asList(
                    newRecord0.withVersion(FDBRecordVersion.complete(versionstamp, 0)),
                    storedRecords.get(1),
                    newRecord2.withVersion(FDBRecordVersion.complete(versionstamp, 1))
            );
        }
        try (FDBRecordContext context = openContext(hookWithNewIndexes)) {
            if (!splitLongRecords && testFormatVersion < FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION) {
                validateUsingOlderVersionFormat(newStoredRecords);
            } else {
                validateUsingNewerVersionFormat(newStoredRecords);
            }
        }
    }

    private <M extends Message> void validateUsingOlderVersionFormat(@Nonnull List<FDBStoredRecord<M>> storedRecords) {
        // Make sure all of the records have versions in the old keyspace
        final Subspace legacyVersionSubspace = recordStore.getSubspace().subspace(Tuple.from(FDBRecordStore.RECORD_VERSION_KEY));
        RecordCursorIterator<Pair<Tuple, FDBRecordVersion>> versionKeyPairs = KeyValueCursor.Builder.withSubspace(legacyVersionSubspace)
                .setContext(recordStore.getRecordContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build()
                .map(kv -> Pair.of(legacyVersionSubspace.unpack(kv.getKey()), FDBRecordVersion.fromBytes(kv.getValue())))
                .asIterator();
        for (FDBStoredRecord<M> storedRecord : storedRecords) {
            assertThat(versionKeyPairs.hasNext(), is(true));
            Pair<Tuple, FDBRecordVersion> versionPair = versionKeyPairs.next();
            assertEquals(storedRecord.getPrimaryKey(), versionPair.getLeft());
            assertEquals(storedRecord.getVersion(), versionPair.getRight());
        }
        assertThat(versionKeyPairs.hasNext(), is(false));

        // Validate that no value in the record subspace begins with the type code for versionstamps
        final Subspace recordsSubspace = recordStore.recordsSubspace();
        KeyValueCursor.Builder.withSubspace(recordsSubspace)
                .setContext(recordStore.getRecordContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build()
                .forEach(kv -> assertNotEquals(kv.getValue()[0], VERSIONSTAMP_CODE))
                .join();
    }

    private <M extends Message> void validateUsingNewerVersionFormat(@Nonnull List<FDBStoredRecord<M>> storedRecords) {
        // Make sure the old keyspace doesn't have anything in it
        final Subspace legacyVersionSubspace = recordStore.getSubspace().subspace(Tuple.from(FDBRecordStore.RECORD_VERSION_KEY));
        KeyValueCursor legacyKvs = KeyValueCursor.Builder.withSubspace(legacyVersionSubspace)
                .setContext(recordStore.getRecordContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build();
        assertEquals(0, (int)legacyKvs.getCount().join());

        // Look for the versions within the primary keyspace
        final Subspace recordsSubspace = recordStore.recordsSubspace();
        RecordCursorIterator<Pair<Tuple, FDBRecordVersion>> versionKeyPairs = KeyValueCursor.Builder.withSubspace(recordsSubspace)
                .setContext(recordStore.getRecordContext())
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build()
                .map(kv -> Pair.of(recordsSubspace.unpack(kv.getKey()), kv.getValue()))
                .filter(tupleBytesPair -> tupleBytesPair.getLeft().getLong(tupleBytesPair.getLeft().size() - 1) == -1)
                .map(tupleBytesPair -> Pair.of(tupleBytesPair.getLeft().popBack(), FDBRecordVersion.fromVersionstamp(Tuple.fromBytes(tupleBytesPair.getRight()).getVersionstamp(0))))
                .asIterator();
        for (FDBStoredRecord<M> storedRecord : storedRecords) {
            assertThat(versionKeyPairs.hasNext(), is(true));
            Pair<Tuple, FDBRecordVersion> versionPair = versionKeyPairs.next();
            assertEquals(storedRecord.getPrimaryKey(), versionPair.getLeft());
            assertEquals(storedRecord.getVersion(), versionPair.getRight());
        }
        assertThat(versionKeyPairs.hasNext(), is(false));
    }
}
