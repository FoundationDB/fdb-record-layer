/*
 * FDBRecordStoreIndexPrefetchTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

/**
 * Remote fetch test with a compound primary key.
 */
@Tag(Tags.RequiresFDB)
class RemoteFetchMultiColumnKeyTest extends RemoteFetchTestBase {
    protected static final RecordQuery STR_HELLO = RecordQuery.newBuilder()
            .setRecordType("MyRecord")
            .setFilter(Query.field("str_value").equalsValue("hello"))
            .build();

    protected static final RecordQuery REC_NO_1 = RecordQuery.newBuilder()
            .setRecordType("MyRecord")
            .setFilter(Query.and(Query.field("str_value").equalsValue("hello"), Query.field("header").matches(Query.field("rec_no").equalsValue(1L))))
            .build();

    protected static final RecordQuery PATH_AAA = RecordQuery.newBuilder()
            .setRecordType("MyRecord")
            .setFilter(Query.and(Query.field("str_value").equalsValue("hello"), Query.field("header").matches(Query.field("path").equalsValue("aaa"))))
            .build();

    /**
     * Test with an index that has none of the primary key components.
     * @param indexFetchMethod the fetch type
     */
    @ParameterizedTest(name = "testMultiColumnPrimaryKeyNoKeyInIndex(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    public void testMultiColumnPrimaryKeyNoKeyInIndex(IndexFetchMethod indexFetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        List<FDBQueriedRecord<Message>> records = executeQuery(indexFetchMethod, indexEntryReturnPolicy, recordMetadataStrValueIndex(), STR_HELLO);
        assertRecordStrIndex(records.get(0), "aaa", 1, "hello");
    }

    /**
     * Test with an index that has the second key component (rec_no) as part of the index.
     * @param indexFetchMethod the fetch type
     */
    @ParameterizedTest(name = "testMultiColumnPrimaryKeyRecnoInIndex(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    public void testMultiColumnPrimaryKeyRecnoInIndex(IndexFetchMethod indexFetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        List<FDBQueriedRecord<Message>> records = executeQuery(indexFetchMethod, indexEntryReturnPolicy, recordMetadataRecnoIndex(), REC_NO_1);
        assertRecordRecnoIndex(records.get(0), "aaa", 1, "hello");
    }

    /**
     * Test with an index that has the first part of the key (path) as part of the index.
     * @param indexFetchMethod the fetch type
     * @throws Exception in case of error
     */
    @ParameterizedTest(name = "testMultiColumnPrimaryKeyPathInIndex(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    public void testMultiColumnPrimaryKeyPathInIndex(IndexFetchMethod indexFetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        List<FDBQueriedRecord<Message>> records = executeQuery(indexFetchMethod, indexEntryReturnPolicy, recordMetadataPathIndex(), PATH_AAA);
        assertRecordPathIndex(records.get(0), "aaa", 1, "hello");
    }

    private List<FDBQueriedRecord<Message>> executeQuery(final IndexFetchMethod indexFetchMethod, final IndexEntryReturnPolicy indexEntryReturnPolicy,
                                                         final RecordMetaData metaData, final RecordQuery query) throws InterruptedException, ExecutionException {
        populateRecords(metaData);
        List<FDBQueriedRecord<Message>> records;
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            RecordQueryPlan plan = plan(query, indexFetchMethod, indexEntryReturnPolicy);
            records = recordStore.executeQuery(plan, null, ExecuteProperties.SERIAL_EXECUTE).asList().get();
            assertEquals(1, records.size());
        }
        return records;
    }

    private void populateRecords(final RecordMetaData metaData) {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);

            TestRecordsWithHeaderProto.MyRecord.Builder recBuilder = TestRecordsWithHeaderProto.MyRecord.newBuilder();
            TestRecordsWithHeaderProto.HeaderRecord.Builder headerBuilder = recBuilder.getHeaderBuilder();
            headerBuilder.setPath("aaa");
            headerBuilder.setRecNo(1);
            recBuilder.setStrValue("hello");
            recordStore.saveRecord(recBuilder.build());

            headerBuilder.setPath("aaa");
            headerBuilder.setRecNo(2);
            recBuilder.setStrValue("goodbye");
            recordStore.saveRecord(recBuilder.build());

            headerBuilder.setPath("zzz");
            headerBuilder.setRecNo(3);
            recBuilder.setStrValue("end");
            recordStore.saveRecord(recBuilder.build());

            context.commit();
        }
    }

    private RecordMetaData recordMetadataStrValueIndex() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        builder.getRecordType("MyRecord").setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
        builder.addIndex("MyRecord", "str_value_index", field("str_value"));
        return builder.getRecordMetaData();
    }

    private RecordMetaData recordMetadataRecnoIndex() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        builder.getRecordType("MyRecord").setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
        builder.addIndex("MyRecord", "recno_index", concat(field("str_value"), field("header").nest(field("rec_no"))));
        return builder.getRecordMetaData();
    }

    private RecordMetaData recordMetadataPathIndex() {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        builder.getRecordType("MyRecord").setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
        builder.addIndex("MyRecord", "path_index", concat(field("str_value"), field("header").nest(field("path"))));
        return builder.getRecordMetaData();
    }

    private void assertRecordStrIndex(final FDBQueriedRecord<Message> rec, final String path, final long rec_no, final String strValue) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo("str_value_index"));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(3));
        assertThat(indexElements.get(0), equalTo(strValue));
        assertThat(indexElements.get(1), equalTo(path));
        assertThat(indexElements.get(2), equalTo(rec_no));
        List<Object> indexPrimaryKey = indexEntry.getPrimaryKey().getItems();
        assertThat(indexPrimaryKey.size(), equalTo(2));
        assertThat(indexPrimaryKey.get(0), equalTo(path));
        assertThat(indexPrimaryKey.get(1), equalTo(rec_no));

        FDBStoredRecord<Message> storedRecord = rec.getStoredRecord();
        assertThat(storedRecord.getPrimaryKey().size(), equalTo(2));
        assertThat(storedRecord.getPrimaryKey().get(0), equalTo(path));
        assertThat(storedRecord.getPrimaryKey().get(1), equalTo(rec_no));
        assertThat(storedRecord.getRecordType().getName(), equalTo("MyRecord"));

        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
        assertThat(myrec.getHeader().getRecNo(), equalTo(rec_no));
        assertThat(myrec.getHeader().getPath(), equalTo(path));
        assertThat(myrec.getStrValue(), equalTo(strValue));
    }

    private void assertRecordRecnoIndex(final FDBQueriedRecord<Message> rec, final String path, final long rec_no, final String strValue) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo("recno_index"));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(3));
        assertThat(indexElements.get(0), equalTo(strValue));
        assertThat(indexElements.get(1), equalTo(rec_no));
        assertThat(indexElements.get(2), equalTo(path));
        List<Object> indexPrimaryKey = indexEntry.getPrimaryKey().getItems();
        assertThat(indexPrimaryKey.size(), equalTo(2));
        assertThat(indexPrimaryKey.get(0), equalTo(path));
        assertThat(indexPrimaryKey.get(1), equalTo(rec_no));

        FDBStoredRecord<Message> storedRecord = rec.getStoredRecord();
        assertThat(storedRecord.getPrimaryKey().size(), equalTo(2));
        assertThat(storedRecord.getPrimaryKey().get(0), equalTo(path));
        assertThat(storedRecord.getPrimaryKey().get(1), equalTo(rec_no));
        assertThat(storedRecord.getRecordType().getName(), equalTo("MyRecord"));

        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
        assertThat(myrec.getHeader().getRecNo(), equalTo(rec_no));
        assertThat(myrec.getHeader().getPath(), equalTo(path));
        assertThat(myrec.getStrValue(), equalTo(strValue));
    }

    private void assertRecordPathIndex(final FDBQueriedRecord<Message> rec, final String path, final long rec_no, final String strValue) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo("path_index"));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(3));
        assertThat(indexElements.get(0), equalTo(strValue));
        assertThat(indexElements.get(1), equalTo(path));
        assertThat(indexElements.get(2), equalTo(rec_no));
        List<Object> indexPrimaryKey = indexEntry.getPrimaryKey().getItems();
        assertThat(indexPrimaryKey.size(), equalTo(2));
        assertThat(indexPrimaryKey.get(0), equalTo(path));
        assertThat(indexPrimaryKey.get(1), equalTo(rec_no));

        FDBStoredRecord<Message> storedRecord = rec.getStoredRecord();
        assertThat(storedRecord.getPrimaryKey().size(), equalTo(2));
        assertThat(storedRecord.getPrimaryKey().get(0), equalTo(path));
        assertThat(storedRecord.getPrimaryKey().get(1), equalTo(rec_no));
        assertThat(storedRecord.getRecordType().getName(), equalTo("MyRecord"));

        TestRecordsWithHeaderProto.MyRecord.Builder myrec = TestRecordsWithHeaderProto.MyRecord.newBuilder();
        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
        assertThat(myrec.getHeader().getRecNo(), equalTo(rec_no));
        assertThat(myrec.getHeader().getPath(), equalTo(path));
        assertThat(myrec.getStrValue(), equalTo(strValue));
    }
}
