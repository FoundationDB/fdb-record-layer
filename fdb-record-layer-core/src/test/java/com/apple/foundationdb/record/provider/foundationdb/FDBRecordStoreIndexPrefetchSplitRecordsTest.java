/*
 * FDBRecordStoreIndexPrefetchSplitRecordsTest.java
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

/**
 * A test for the Index Prefetch FDB API.
 */
@Tag(Tags.RequiresFDB)
class FDBRecordStoreIndexPrefetchSplitRecordsTest extends FDBRecordStoreQueryTestBase {

    private static final RecordQuery NUM_VALUES_LARGER_THAN_990 = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(990))
            .build();

    private static final RecordQuery NUM_VALUES_LARGER_THAN_1000_REVERSE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(1000))
            .setSort(Key.Expressions.field("num_value_unique"), true)
            .build();

    private static final RecordQuery NUM_VALUES_LARGER_THAN_990_REVERSE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").greaterThan(990))
            .setSort(Key.Expressions.field("num_value_unique"), true)
            .build();

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetup(simpleMetadataHook);
    }

    @ParameterizedTest(name = "indexPrefetchSplitRecordTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSplitRecordTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        saveLargeRecord(1, 200, 2000);
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);

        executeAndVerifyData(plan, 11, (rec, i) -> {
            if (i < 10) {
                int primaryKey = 9 - i;
                String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                int numValue = 1000 - primaryKey;
                assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
            } else {
                // this is the extra, split, record
                assertRecord(rec, 200, "even", 2000, "MySimpleRecord$num_value_unique", (long)2000, 0);
            }
        });
    }

    @ParameterizedTest(name = "indexPrefetchSplitRecordReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSplitRecordReverseTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        saveLargeRecord(1, 200, 2000);
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990_REVERSE, useIndexPrefetch);

        executeAndVerifyData(plan, 11, (rec, i) -> {
            if (i > 0) {
                int primaryKey = i - 1;
                String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                int numValue = 1000 - primaryKey;
                assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
            } else {
                // this is the extra, split, record
                assertRecord(rec, 200, "even", 2000, "MySimpleRecord$num_value_unique", (long)2000, 0);
            }
        });
    }

    @ParameterizedTest(name = "indexPrefetchManySplitRecordTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchManySplitRecordTest(RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) throws Exception {
        // TODO: This test actually runs the API in a way that returns results that are too large: Over 50MB
        // FDB will fix the issue to limit the bytes returned and then this test would need to adjust accordingly.
        int numTransactions = 10;
        int numRecordsPerTransaction = 10;
        for (int i = 0; i < numTransactions; i++) {
            int firstPrimaryKey = 200 + (i * numRecordsPerTransaction);
            int numValueUniqueOffset = 2000 - (i * numRecordsPerTransaction);
            saveLargeRecord(numRecordsPerTransaction, firstPrimaryKey, numValueUniqueOffset);
        }
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_1000_REVERSE, useIndexPrefetch);

        executeAndVerifyData(plan, numRecordsPerTransaction * numTransactions, (rec, i) -> {
            int primaryKey = 200 + i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 2000 - i;
            int localVersion = i % numRecordsPerTransaction;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, localVersion);
        });
    }

    private void saveLargeRecord(final int numRecords, final int firstPrimaryKey, final int numValueUniqueOffset) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            for (int i = 0; i < numRecords; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(firstPrimaryKey + i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(numValueUniqueOffset - i);
                recBuilder.setNumValue2(i % 3);
                recBuilder.setNumValue3Indexed(numValueUniqueOffset % 5);

                for (int j = 0; j < 100000; j++) {
                    recBuilder.addRepeater(j);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    private void assertRecord(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue, final int numValue, final String indexName, Object indexedValue, final int localVersion) {
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

        FDBRecordVersion version = storedRecord.getVersion();
        assertThat(version.getLocalVersion(), equalTo(localVersion));
    }

    @Nonnull
    private final RecordMetaDataHook simpleMetadataHook = metaDataBuilder -> {
        // UseSplitRecords can be set to different values to impact the way the store is opened
        metaDataBuilder.setSplitLongRecords(true);
    };

    @Nonnull
    private RecordQueryPlan plan(final RecordQuery query, final RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch) {
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setIndexFetchMethod(useIndexPrefetch)
                .build());
        return planner.plan(query);
    }

    private byte[] executeAndVerifyData(RecordQueryPlan plan, int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier) throws Exception {
        return executeAndVerifyData(plan, null, ExecuteProperties.SERIAL_EXECUTE, expectedRecords, recordVerifier);
    }

    private byte[] executeAndVerifyData(RecordQueryPlan plan, byte[] continuation, ExecuteProperties executeProperties,
                                        int expectedRecords, BiConsumer<FDBQueriedRecord<Message>, Integer> recordVerifier) throws Exception {
        int count = 0;
        byte[] lastContinuation;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
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
