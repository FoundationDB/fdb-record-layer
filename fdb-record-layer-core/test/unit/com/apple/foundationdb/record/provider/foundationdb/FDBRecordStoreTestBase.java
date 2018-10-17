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
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.temp.RewritePlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for tests for {@link FDBRecordStore}.
 */
public abstract class FDBRecordStoreTestBase {
    private static final Logger logger = LoggerFactory.getLogger(FDBRecordStoreTestBase.class);

    private static final Object[] PATH_OBJECTS = new Object[]{"record-test", "unit", "recordStore"};

    protected FDBDatabase fdb;
    protected FDBRecordStore recordStore;
    protected FDBStoreTimer timer = new FDBStoreTimer();
    protected boolean useRewritePlanner = false;
    protected QueryPlanner planner;
    protected FDBEvaluationContext<Message> evaluationContext;
    protected KeySpacePath path;

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
        FDBRecordContext context = fdb.openContext();
        context.setTimer(timer);
        return context;
    }

    @BeforeEach
    public void clearAndInitialize() {
        getFDB();
        fdb.run(timer, null, context -> {
            setKeySpacePath(context);
            Subspace subspace = new Subspace(path.toTuple());
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
    }

    public void getFDB() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
    }

    private void setKeySpacePath(FDBRecordContext context) {
        path = TestKeySpace.getKeyspacePath(context, PATH_OBJECTS);
    }

    protected void createRecordStore(FDBRecordContext context, RecordMetaData metaData) {
        recordStore = FDBRecordStore.newBuilder()
                .setContext(context).setKeySpacePath(path).setMetaDataProvider(metaData)
                .uncheckedOpen();
        recordStore.validateMetaData();
        setupPlanner(null);
        evaluationContext = recordStore.emptyEvaluationContext();
    }

    public void setUseRewritePlanner(boolean useRewritePlanner) {
        this.useRewritePlanner = useRewritePlanner;
    }

    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
        if (useRewritePlanner) {
            planner = new RewritePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        } else {
            if (indexTypes == null) {
                indexTypes = PlannableIndexTypes.DEFAULT;
            }
            planner = new RecordQueryPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
    }

    public void commit(FDBRecordContext context) throws Exception {
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

    public void openSimpleRecordStore(FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(TestRecords1Proto.getDescriptor());
        metaData.addIndex(null, COUNT_INDEX);
        metaData.addIndex(null, COUNT_UPDATES_INDEX);
        if (hook != null) {
            hook.apply(metaData);
        }
        createRecordStore(context, metaData.getRecordMetaData());
    }

    public void openBytesRecordStore(FDBRecordContext context) throws Exception {
        createRecordStore(context, RecordMetaData.build(TestRecordsBytesProto.getDescriptor()));
    }

    public void openRecordWithHeader(FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(TestRecordsWithHeaderProto.getDescriptor());
        if (hook != null) {
            hook.apply(metaData);
        }
        createRecordStore(context, metaData.getRecordMetaData());
    }

    public void openUnionRecordStore(FDBRecordContext context) throws Exception {
        this.recordStore = openNewUnionRecordStore(context);
    }

    @Nonnull
    protected FDBRecordStore openNewUnionRecordStore(FDBRecordContext context) {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecordsWithUnionProto.getDescriptor());
        metaDataBuilder.addIndex(null,
                new Index("versions", field("etag")));
        metaDataBuilder.addMultiTypeIndex(
                // partial_versions explicitly does not include MySimpleRecord3
                Arrays.asList(metaDataBuilder.getRecordType("MySimpleRecord"),
                        metaDataBuilder.getRecordType("MySimpleRecord2")),
                new Index("partial_versions", field("etag")));
        metaDataBuilder.addIndex(null,
                new Index("cross_versions", field("nested").nest("etag")));
        metaDataBuilder.addMultiTypeIndex(
                Arrays.asList(metaDataBuilder.getRecordType("MySimpleRecord"),
                        metaDataBuilder.getRecordType("MySimpleRecord2"),
                        metaDataBuilder.getRecordType("MySimpleRecord3")),
                new Index("partial_nested_versions", concat(field("nested").nest(field("etag")), field("etag"))));
        createRecordStore(context, metaDataBuilder.getRecordMetaData());
        return recordStore;
    }

    public void openAnyRecordStore(Descriptors.FileDescriptor fileDescriptor, FDBRecordContext context) throws Exception {
        openAnyRecordStore(fileDescriptor, context, NO_HOOK);
    }

    public void openAnyRecordStore(Descriptors.FileDescriptor fileDescriptor, FDBRecordContext context, @Nullable RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaData = new RecordMetaDataBuilder(fileDescriptor);
        if (hook != null) {
            hook.apply(metaData);
        }
        createRecordStore(context, metaData.getRecordMetaData());
    }

    public void openMultiRecordStore(FDBRecordContext context) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecordsMultiProto.getDescriptor());
        metaDataBuilder.addIndex(null, COUNT_INDEX);
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(metaDataBuilder.getRecordType("MultiRecordOne"), metaDataBuilder.getRecordType("MultiRecordTwo")),
                new Index("onetwo$element", field("element", FanType.FanOut)));
        createRecordStore(context, metaDataBuilder.getRecordMetaData());
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

    protected FDBStoredRecord<Message> saveAndSplitSimpleRecord(long recno, String strValue, int numValue) throws Exception {
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
