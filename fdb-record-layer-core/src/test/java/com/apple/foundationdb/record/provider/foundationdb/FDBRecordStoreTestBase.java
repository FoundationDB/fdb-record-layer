/*
 * FDBRecordStoreTestBase.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TestRecordsMultiProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for tests for {@link FDBRecordStore}.
 */
public abstract class FDBRecordStoreTestBase extends FDBTestBase {
    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStoreTestBase.class);

    protected static final Object[] PATH_OBJECTS = new Object[]{"record-test", "unit", "recordStore"};

    protected FDBDatabase fdb;
    protected FDBRecordStore recordStore;
    protected FDBStoreTimer timer = new FDBStoreTimer();
    protected boolean useRewritePlanner = false;
    protected QueryPlanner planner;
    protected final KeySpacePath path;

    public FDBRecordStoreTestBase() {
        this(PATH_OBJECTS);
    }

    public FDBRecordStoreTestBase(Object[] path) {
        this.path = TestKeySpace.getKeyspacePath(path);
    }

    @Nonnull
    public static Stream<Arguments> formatVersionAndSplitArgs() {
        return Stream.of(FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION - 1, FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION, FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                .flatMap(formatVersion -> Stream.of(Arguments.of(formatVersion, false), Arguments.of(formatVersion, true)));
    }

    /**
     * Meta data setup hook, used for testing.
     */
    @FunctionalInterface
    public interface RecordMetaDataHook {
        void apply(RecordMetaDataBuilder metaData);
    }

    protected static final RecordMetaDataHook NO_HOOK = metadata -> {
    };
    protected static final Index COUNT_INDEX = new Index("globalRecordCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
    protected static final Index COUNT_UPDATES_INDEX = new Index("globalRecordUpdateCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT_UPDATES);

    @FunctionalInterface
    protected interface Opener {
        void open(FDBRecordContext context) throws Exception;
    }

    public FDBRecordContext openContext() {
        return openContext(RecordLayerPropertyStorage.newBuilder());
    }

    public FDBRecordContext openContext(@Nonnull final RecordLayerPropertyStorage.Builder propsBuilder) {
        final FDBRecordContextConfig config = contextConfig(propsBuilder).build();
        return fdb.openContext(config);
    }

    protected FDBRecordContextConfig.Builder contextConfig(@Nonnull final RecordLayerPropertyStorage.Builder propsBuilder) {
        return FDBRecordContextConfig.newBuilder()
                .setTimer(timer)
                .setMdcContext(ImmutableMap.of("uuid", UUID.randomUUID().toString()))
                .setTrackOpen(true)
                .setSaveOpenStackTrace(true)
                .setRecordContextProperties(propsBuilder.build());
    }

    @BeforeEach
    public void clearAndInitialize() {
        getFDB();
        fdb.run(timer, null, context -> {
            path.deleteAllData(context);
            return null;
        });
    }

    @AfterEach
    public void checkForOpenContexts() {
        int count = fdb.warnAndCloseOldTrackedOpenContexts(0);
        assertEquals(0, count, "should not have left any contexts open");
    }

    public void getFDB() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION) // set to max to test newest features (unsafe for real deployments)
                .setKeySpacePath(path)
                .setContext(context)
                .setMetaDataProvider(metaData);
    }

    protected void createOrOpenRecordStore(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        recordStore = getStoreBuilder(context, metaData).createOrOpen();
        setupPlanner(null);
    }

    protected void uncheckedOpenRecordStore(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        recordStore = getStoreBuilder(context, metaData).uncheckedOpen();
        setupPlanner(null);
    }

    public void setUseRewritePlanner(boolean useRewritePlanner) {
        this.useRewritePlanner = useRewritePlanner;
    }

    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
        if (useRewritePlanner) {
            planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
            if (Debugger.getDebugger() == null) {
                Debugger.setDebugger(new DebuggerWithSymbolTables());
            }
            Debugger.setup();
        } else {
            if (indexTypes == null) {
                indexTypes = PlannableIndexTypes.DEFAULT;
            }
            planner = new RecordQueryPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
    }

    public void commit(FDBRecordContext context) {
        try {
            context.commit();
            if (logger.isInfoEnabled()) {
                KeyValueLogMessage msg = KeyValueLogMessage.build("committing transaction");
                msg.addKeysAndValues(timer.getKeysAndValues());
                logger.info(msg.toString());
            }
        } finally {
            timer.reset();
        }
    }

    public static ByteString byteString(int... ints) {
        byte[] bytes = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            bytes[i] = (byte)ints[i];
        }
        return ByteString.copyFrom(bytes);
    }

    public void openSimpleRecordStore(FDBRecordContext context) throws Exception {
        openSimpleRecordStore(context, NO_HOOK);
    }

    public void openSimpleRecordStore(FDBRecordContext context, @Nullable RecordMetaDataHook hook) {
        createOrOpenRecordStore(context, simpleMetaData(hook));
    }

    public void openSimpleRecordStoreWithSingletonPipeline(FDBRecordContext context) {
        recordStore = FDBRecordStore.newBuilder()
                .setContext(context)
                .setKeySpacePath(path)
                .setMetaDataProvider(simpleMetaData(NO_HOOK))
                .setPipelineSizer(operation -> 1)
                .createOrOpen();
        setupPlanner(null);
    }

    public void uncheckedOpenSimpleRecordStore(FDBRecordContext context) throws Exception {
        uncheckedOpenSimpleRecordStore(context, NO_HOOK);
    }

    public void uncheckedOpenSimpleRecordStore(FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        uncheckedOpenRecordStore(context, simpleMetaData(hook));
    }

    protected RecordMetaData simpleMetaData(@Nullable RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData.addUniversalIndex(COUNT_INDEX);
        metaData.addUniversalIndex(COUNT_UPDATES_INDEX);
        if (hook != null) {
            hook.apply(metaData);
        }
        return metaData.getRecordMetaData();
    }

    public void openBytesRecordStore(FDBRecordContext context) throws Exception {
        createOrOpenRecordStore(context, RecordMetaData.build(TestRecordsBytesProto.getDescriptor()));
    }

    public void openRecordWithHeader(FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        if (hook != null) {
            hook.apply(metaData);
        }
        createOrOpenRecordStore(context, metaData.getRecordMetaData());
    }

    public void openUnionRecordStore(FDBRecordContext context) throws Exception {
        this.recordStore = openNewUnionRecordStore(context);
    }

    @Nonnull
    protected FDBRecordStore openNewUnionRecordStore(FDBRecordContext context) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsWithUnionProto.getDescriptor());
        metaDataBuilder.addUniversalIndex(
                new Index("versions", field("etag")));
        metaDataBuilder.addMultiTypeIndex(
                // partial_versions explicitly does not include MySimpleRecord3
                Arrays.asList(metaDataBuilder.getRecordType("MySimpleRecord"),
                        metaDataBuilder.getRecordType("MySimpleRecord2")),
                new Index("partial_versions", field("etag")));
        metaDataBuilder.addUniversalIndex(
                new Index("cross_versions", field("nested").nest("etag")));
        metaDataBuilder.addMultiTypeIndex(
                Arrays.asList(metaDataBuilder.getRecordType("MySimpleRecord"),
                        metaDataBuilder.getRecordType("MySimpleRecord2"),
                        metaDataBuilder.getRecordType("MySimpleRecord3")),
                new Index("partial_nested_versions", concat(field("nested").nest(field("etag")), field("etag"))));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
        return recordStore;
    }

    public void openAnyRecordStore(Descriptors.FileDescriptor fileDescriptor, FDBRecordContext context) throws Exception {
        openAnyRecordStore(fileDescriptor, context, NO_HOOK);
    }

    public void openAnyRecordStore(Descriptors.FileDescriptor fileDescriptor, FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(fileDescriptor);
        if (hook != null) {
            hook.apply(metaData);
        }
        createOrOpenRecordStore(context, metaData.getRecordMetaData());
    }

    public void openMultiRecordStore(FDBRecordContext context) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsMultiProto.getDescriptor());
        metaDataBuilder.addUniversalIndex(COUNT_INDEX);
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(metaDataBuilder.getRecordType("MultiRecordOne"), metaDataBuilder.getRecordType("MultiRecordTwo")),
                new Index("onetwo$element", field("element", FanType.FanOut)));
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    protected void saveSimpleRecord(long recNo, String strValue, int etag) {
        recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed(strValue)
                .setEtag(etag)
                .build());
    }

    protected void saveSimpleRecord2(String strValue, int etag) {
        recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder()
                .setStrValueIndexed(strValue)
                .setEtag(etag)
                .build());
    }

    protected void saveSimpleRecord3(String strValue, int etag) {
        recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord3.newBuilder()
                .setStrValueIndexed(strValue)
                .setEtag(etag)
                .build());
    }

    protected void saveHeaderRecord(long rec_no, String path, int num, String str) {
        TestRecordsWithHeaderProto.MyRecord.Builder recBuilder = TestRecordsWithHeaderProto.MyRecord.newBuilder();
        TestRecordsWithHeaderProto.HeaderRecord.Builder headerBuilder = recBuilder.getHeaderBuilder();
        headerBuilder.setRecNo(rec_no);
        headerBuilder.setPath(path);
        headerBuilder.setNum(num);
        recBuilder.setStrValue(str);
        recordStore.saveRecord(recBuilder.build());
    }

    protected TestRecordsWithHeaderProto.MyRecord parseMyRecord(Message message) {
        TestRecordsWithHeaderProto.MyRecord.Builder recBuilder = TestRecordsWithHeaderProto.MyRecord.newBuilder();
        recBuilder.mergeFrom(message);
        return recBuilder.build();
    }

    protected FDBStoredRecord<Message> saveAndSplitSimpleRecord(long recno, String strValue, int numValue) {
        FDBStoredRecord<Message> savedRecord;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);

            TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
            myrec.setRecNo(recno);
            myrec.setStrValueIndexed(strValue);
            myrec.setNumValueUnique(numValue);
            FDBStoredRecord<Message> savedRecordInitial = recordStore.saveRecord(myrec.build());
            commit(context);

            savedRecord = savedRecordInitial.withCommittedVersion(context.getVersionStamp());
        }
        assertEquals((strValue.length() + SplitHelper.SPLIT_RECORD_SIZE - 1) / SplitHelper.SPLIT_RECORD_SIZE, savedRecord.getKeyCount());
        return savedRecord;
    }

    protected static final RecordMetaDataHook TEST_SPLIT_HOOK = md -> {
        md.setSplitLongRecords(true);
        md.removeIndex("MySimpleRecord$str_value_indexed");
        md.setStoreRecordVersions(false);
    };
}
