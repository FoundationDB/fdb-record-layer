/*
 * FDBRecordStoreIndexPrefetchOldVersionTest.java
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
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for RemoteFetch tests.
 */
@Tag(Tags.RequiresFDB)
public class RemoteFetchTestBase extends FDBRecordStoreQueryTestBase {

    protected static final RecordQuery NUM_VALUES_LARGER_THAN_990 = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(990))
            .build();

    protected static final RecordQuery NUM_VALUES_LARGER_THAN_1000_REVERSE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(1000))
            .setSort(Key.Expressions.field("num_value_unique"), true)
            .build();

    protected static final RecordQuery NUM_VALUES_LARGER_THAN_990_REVERSE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(990))
            .setSort(Key.Expressions.field("num_value_unique"), true)
            .build();

    protected static final RecordQuery STR_VALUE_EVEN = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("str_value_indexed").equalsValue("even"))
            .build();

    protected static final RecordQuery PRIMARY_KEY_EQUAL = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("rec_no").equalsValue(1L))
            .build();

    protected void assertRecord(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue,
                                final int numValue, final String indexName, Object indexedValue) {
        assertBaseRecord(rec, primaryKey, strValue, numValue, indexName, indexedValue);

        FDBRecordVersion version = rec.getStoredRecord().getVersion();
        assertThat(version.toBytes().length, equalTo(12));
        assertThat(version.toBytes()[11], equalTo((byte)primaryKey));
    }

    protected void assertRecord(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue,
                                final int numValue, final String indexName, Object indexedValue, final int localVersion) {
        assertBaseRecord(rec, primaryKey, strValue, numValue, indexName, indexedValue);

        FDBRecordVersion version = rec.getStoredRecord().getVersion();
        assertThat(version.getLocalVersion(), equalTo(localVersion));
    }

    private void assertBaseRecord(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue, final int numValue, final String indexName, final Object indexedValue) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo(indexName));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(2));
        assertThat(indexElements.get(0), equalTo(indexedValue));
        assertThat(indexElements.get(1), equalTo(primaryKey));
        List<Object> indexPrimaryKey = indexEntry.getPrimaryKey().getItems();
        assertThat(indexPrimaryKey.size(), equalTo(1));
        assertThat(indexPrimaryKey.get(0), equalTo(primaryKey));

        FDBStoredRecord<Message> storedRecord = rec.getStoredRecord();
        assertThat(storedRecord.getPrimaryKey().get(0), equalTo(primaryKey));
        assertThat(storedRecord.getRecordType().getName(), equalTo("MySimpleRecord"));

        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
        assertThat(myrec.getRecNo(), equalTo(primaryKey));
        assertThat(myrec.getStrValueIndexed(), equalTo(strValue));
        assertThat(myrec.getNumValueUnique(), equalTo(numValue));
    }

    @Nonnull
    protected RecordQueryPlan plan(final RecordQuery query, final RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) {
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setIndexFetchMethod(useIndexPrefetch)
                .build());
        return planner.plan(query);
    }

    protected byte[] executeAndVerifyData(RecordQueryPlan plan, int expectedRecords, BiConsumer<FDBQueriedRecord<Message>,
            Integer> recordVerifier, final RecordMetaDataHook metaDataHook) throws Exception {
        return executeAndVerifyData(plan, null, ExecuteProperties.SERIAL_EXECUTE, expectedRecords, recordVerifier, metaDataHook);
    }

    protected byte[] executeAndVerifyData(RecordQueryPlan plan, byte[] continuation, ExecuteProperties executeProperties,
                                          int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier, final RecordMetaDataHook metaDataHook) throws Exception {
        int count = 0;
        byte[] lastContinuation;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataHook);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> record = cursor.next();
                    recordVerifier.accept(record, count);
                    count++;
                }
                lastContinuation = cursor.getContinuation();
            }
        }
        assertThat(count, equalTo(expectedRecords));
        return lastContinuation;
    }
}
