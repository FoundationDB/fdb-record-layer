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

import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.provider.Arguments;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Counts.REMOTE_FETCH;
import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Events.SCAN_REMOTE_FETCH_ENTRY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for RemoteFetch tests.
 */
@Tag(Tags.RequiresFDB)
public class RemoteFetchTestBase extends FDBRecordStoreQueryTestBase {

    protected static final RecordQuery NUM_VALUES_LARGER_THAN_990 = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(990))
            .build();

    protected static final RecordQuery NUM_VALUES_LARGER_EQUAL_0 = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThanOrEquals(0))
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

    protected static final RecordQuery NUM_VALUES_LARGER_EQUAL_0_REVERSE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThanOrEquals(0))
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

    public static Stream<Arguments> fetchMethodAndStreamMode() {
        return Stream.of(IndexFetchMethod.values())
                .flatMap(indexFetchMethod -> Stream.of(CursorStreamingMode.ITERATOR, CursorStreamingMode.LARGE, CursorStreamingMode.MEDIUM, CursorStreamingMode.SMALL)
                        .map(streamMode -> Arguments.of(indexFetchMethod, streamMode)));
    }

    protected void assertRecord(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue,
                                final int numValue, final String indexName, Object indexedValue) {
        assertBaseRecord(rec, primaryKey, strValue, numValue, indexName, indexedValue);

        FDBRecordVersion version = rec.getStoredRecord().getVersion();
        assertThat(version.toBytes().length, equalTo(12));
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
    protected RecordQueryPlan plan(final RecordQuery query, final IndexFetchMethod useIndexPrefetch) {
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setIndexFetchMethod(useIndexPrefetch)
                .build());
        return planQuery(query);
    }

    protected byte[] executeAndVerifyData(RecordQueryPlan plan, int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier,
                                          final RecordMetaDataHook metaDataHook) throws Exception {
        return executeAndVerifyData(plan, null, ExecuteProperties.SERIAL_EXECUTE, expectedRecords, recordVerifier, metaDataHook);
    }

    protected byte[] executeAndVerifyData(RecordQueryPlan plan, byte[] continuation, ExecuteProperties executeProperties,
                                          int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier, final RecordMetaDataHook metaDataHook) throws Exception {
        byte[] lastContinuation;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataHook);
            lastContinuation = executeAndVerifyData(context, plan, continuation, executeProperties, expectedRecords, recordVerifier);
        }
        return lastContinuation;
    }

    protected byte[] executeAndVerifyData(FDBRecordContext context, RecordQueryPlan plan, byte[] continuation, ExecuteProperties executeProperties,
                                          int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier) {
        byte[] lastContinuation;

        try (RecordCursorIterator<FDBQueriedRecord<Message>> iterator = recordStore.executeQuery(plan, continuation, executeProperties).asIterator()) {
            lastContinuation = verifyData(expectedRecords, recordVerifier, iterator);
        }

        return lastContinuation;
    }

    protected byte[] scanAndVerifyData(String indexName, IndexFetchMethod fetchMethod, IndexScanBounds scanBounds,
                                       final ScanProperties scanProperties, byte[] continuation,
                                       int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier, RecordMetaDataHook metaDataHook) {
        byte[] lastContinuation;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataHook);
            lastContinuation = scanAndVerifyData(context, indexName, fetchMethod, scanBounds, scanProperties, continuation, expectedRecords, recordVerifier);
        }
        return lastContinuation;
    }

    @Nullable
    protected byte[] scanAndVerifyData(FDBRecordContext context, String indexName, IndexFetchMethod fetchMethod,
                                       IndexScanBounds scanBounds, final ScanProperties scanProperties, final byte[] continuation,
                                       int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier) {
        byte[] lastContinuation;
        try (RecordCursorIterator<FDBQueriedRecord<Message>> iterator = recordStore.scanIndexRecords(
                        indexName, fetchMethod, scanBounds,
                        continuation, IndexOrphanBehavior.ERROR, scanProperties)
                .map(FDBQueriedRecord::indexed)
                .asIterator()) {
            lastContinuation = verifyData(expectedRecords, recordVerifier, iterator);
        }
        return lastContinuation;
    }

    @Nullable
    protected byte[] verifyData(final int expectedRecords,
                                final BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier,
                                final RecordCursorIterator<FDBQueriedRecord<Message>> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            FDBQueriedRecord<Message> record = iterator.next();
            recordVerifier.accept(record, count);
            count++;
        }
        assertThat(count, equalTo(expectedRecords));
        return iterator.getContinuation();
    }

    protected void assertCounters(final IndexFetchMethod useIndexPrefetch, final int expectedRemoteFetches, final int expectedRemoteFetchEntries) {
        if ((useIndexPrefetch != IndexFetchMethod.SCAN_AND_FETCH) &&
                (recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1))) {

            StoreTimer.Counter numRemoteFetches = recordStore.getTimer().getCounter(REMOTE_FETCH);
            StoreTimer.Counter numRemoteFetchEntries = recordStore.getTimer().getCounter(SCAN_REMOTE_FETCH_ENTRY);
            // Assert expected <= actual since there could be some other reads because of some set up code
            assertTrue(expectedRemoteFetches <= numRemoteFetches.getCount());
            assertTrue(expectedRemoteFetchEntries <= numRemoteFetchEntries.getCount());
        }
    }

    protected List<FDBQueriedRecord<Message>> executeToList(FDBRecordContext context, RecordQueryPlan plan, byte[] continuation, ExecuteProperties executeProperties) throws Exception {
        final List<FDBQueriedRecord<Message>> results;

        try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties)) {
            results = cursor.asList().get();
        }
        return results;
    }

    protected List<FDBQueriedRecord<Message>> scanToList(FDBRecordContext context, String indexName, IndexFetchMethod fetchMethod,
                                                         IndexScanBounds scanBounds, final ScanProperties scanProperties, final KeyExpression commonPrimaryKey,
                                                         final byte[] continuation) throws Exception {
        final List<FDBQueriedRecord<Message>> results;

        try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.scanIndexRecords(indexName, fetchMethod, scanBounds, continuation, IndexOrphanBehavior.ERROR, scanProperties)
                .map(FDBQueriedRecord::indexed)) {
            results = cursor.asList().get();
        }
        return results;
    }

    @Nonnull
    protected ExecuteProperties serializableWithStreamingMode(final CursorStreamingMode streamingMode) {
        final ExecuteProperties execProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .setState(ExecuteState.NO_LIMITS)
                .setDefaultCursorStreamingMode(streamingMode)
                .build();
        return execProperties;
    }
}
