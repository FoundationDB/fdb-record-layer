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
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A "test" that compares the performance of the Index Scan and Index Prefetch FDB APIs.
 */
@Tag(Tags.RequiresFDB)
class FDBIndexPrefetchPerformanceTest extends FDBRecordStoreQueryTestBase {

    @Test
    @Disabled
    void queryPerformance() throws Exception {
        int nRecords = 50;
        int nTimes = 100;

        // populate the DB
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (int i = 0; i < nRecords * 2; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(i + 1000);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();

        // Run regular index scan
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(RecordQueryPlannerConfiguration.IndexPrefetchUse.NONE)
                .build());
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder().build();
        RecordQueryPlan plan = planner.plan(query);

        long time = 0;
        time = executeQuery(plan, nTimes, executeProperties);
        System.out.println("Executed query " + nTimes + " times for " + nRecords * nTimes + " records using index scan in " + TimeUnit.NANOSECONDS.toMillis(time) + " millis");

        // Run IndexPrefetch plan
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setUseIndexPrefetch(RecordQueryPlannerConfiguration.IndexPrefetchUse.USE_INDEX_PREFETCH)
                .build());
        executeProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT)
                .build();
        plan = planner.plan(query);

        time = executeQuery(plan, nTimes, executeProperties);
        System.out.println("Executed query " + nTimes + " times for " + nRecords * nTimes + " records using index prefetch in " + TimeUnit.NANOSECONDS.toMillis(time) + " millis");
    }

    private long executeQuery(RecordQueryPlan plan, int times, final ExecuteProperties executeProperties) throws
                                                                                                          Exception {
        long start;
        long end;
        List<FDBQueriedRecord<Message>> results = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            context.ensureActive().options().setReadYourWritesDisable();
            openSimpleRecordStore(context);

            start = System.nanoTime();
            for (int i = 0; i < times; i++) {
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, executeProperties).asIterator()) {
                    while (cursor.hasNext()) {
                        FDBQueriedRecord<Message> rec = Objects.requireNonNull(cursor.next());
                        results.add(rec);
                    }
                }
            }
            end = System.nanoTime();
        }
        return end - start;
    }
}
