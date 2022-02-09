/*
 * FDBRecordStoreQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A test for the Index Prefetch FDB API.
 */
@Tag(Tags.RequiresFDB)
class FDBRecordStoreIndexPrefetchTest extends FDBRecordStoreQueryTestBase {

    @Test
    void testIndexPrefetchWithMockData() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                // TODO: Need to test with reverse
                .build();
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH)
                .build());
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        int count = 0;
        try (FDBRecordContext context = openContext()) {
            context.ensureActive().options().setReadYourWritesDisable();
            openSimpleRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties)) {
                for (RecordCursorResult<FDBQueriedRecord<Message>> recordResult = cursor.getNext(); recordResult.hasNext(); recordResult = cursor.getNext()) {
                    assertRecordResult(recordResult, count, "MySimpleRecord$num_value_unique");
                    count++;
                }
            }
        }

        assertThat(count, equalTo(5));
    }

    @Test
    void indexPrefetchSimpleIndexTest() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("num_value_unique").greaterThan(990))
                .build();
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH)
                .build());
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        int count = 0;
        try (FDBRecordContext context = openContext()) {
            context.ensureActive().options().setReadYourWritesDisable();
            openSimpleRecordStore(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = Objects.requireNonNull(cursor.next());
                    assertRecord(rec, 9 - count, "MySimpleRecord$num_value_unique");
                    count++;
                }
            }
        }

        assertThat(count, equalTo(10));
    }

    @Test
    void indexPrefetchComplexIndexTest() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH)
                .build());
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        int count = 0;
        try (FDBRecordContext context = openContext()) {
            context.ensureActive().options().setReadYourWritesDisable();
            openSimpleRecordStore(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = Objects.requireNonNull(cursor.next());
                    assertRecord(rec, count * 2, "MySimpleRecord$str_value_indexed"); // we are filtering out all odd entries, so count*2 are the keys of the even ones
                    count++;
                }
            }
        }

        assertThat(count, equalTo(50));
    }

    @Test
    void indexPrefetchContinuationFails() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH)
                .build());
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setReturnedRowLimit(5)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        try (FDBRecordContext context = openContext()) {
            context.ensureActive().options().setReadYourWritesDisable();
            openSimpleRecordStore(context);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties).asIterator()) {
                // For now, there is no continuation support.
                assertThrows(RecordCoreException.class, () -> cursor.hasNext());
            }
        }
    }

    @Test
    void indexPrefetchByteLimitNoContinuation() throws Exception {
        complexQuerySetup(null);

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH)
                .build());
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .setScannedBytesLimit(100)
                .build();
        RecordQueryPlan plan = planner.plan(query);
        try (FDBRecordContext context = openContext()) {
            context.ensureActive().options().setReadYourWritesDisable();
            openSimpleRecordStore(context);
            byte[] continuation;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursorIterator = recordStore.executeQuery(plan, null, executeProperties).asIterator()) {
                // We only get one record before hitting the limit
                FDBQueriedRecord<Message> rec = Objects.requireNonNull(cursorIterator.next());
                assertRecord(rec, 0, "MySimpleRecord$str_value_indexed");
                assertThat(cursorIterator.hasNext(), equalTo(false));
                assertThat(cursorIterator.getNoNextReason(), equalTo(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED));

                continuation = cursorIterator.getContinuation();
            }
            // Execute the plan again with the continuation
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursorIterator = recordStore.executeQuery(plan, continuation, executeProperties).asIterator()) {
                // We only get one record before hitting the limit
                assertThat(cursorIterator.hasNext(), equalTo(false));
                assertThat(cursorIterator.getNoNextReason(), equalTo(RecordCursor.NoNextReason.SOURCE_EXHAUSTED));
            }
        }
    }

    private void assertRecordResult(final RecordCursorResult<FDBQueriedRecord<Message>> recResult, final int i, final String indexName) {
        assertContinuation(recResult.getContinuation(), i);
        assertRecord(recResult.get(), i, indexName);
    }

    private void assertContinuation(final RecordCursorContinuation continuation, final int i) {
        int x = 5;
    }

    private void assertRecord(final FDBQueriedRecord<Message> rec, final int i, final String indexName) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo(indexName));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(2));
        assertThat(indexElements.get(0), equalTo((long)(1000 - i)));
        assertThat(indexElements.get(1), equalTo((long)i));
        List<Object> primaryKey = indexEntry.getPrimaryKey().getItems();
        assertThat(primaryKey.size(), equalTo(1));
        assertThat(primaryKey.get(0), equalTo((long)i));

        FDBStoredRecord<Message> storedRecord = rec.getStoredRecord();
        assertThat(storedRecord.getPrimaryKey().get(0), equalTo((long)i));
        assertThat(storedRecord.getRecordType().getName(), equalTo("MySimpleRecord"));

        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
        assertThat(myrec.getRecNo(), equalTo((long)i));
        assertThat(myrec.getStrValueIndexed(), equalTo((i % 2 == 0) ? "even" : "odd"));
        assertThat(myrec.getNumValueUnique(), equalTo(1000 - i));

        FDBRecordVersion version = storedRecord.getVersion();
        assertThat(version.toBytes().length, equalTo(12));
        assertThat(version.toBytes()[11], equalTo((byte)i));
    }
}
