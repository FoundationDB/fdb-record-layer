/*
 * FDBRecordStoreIndexScanPreferenceTest.java
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

import com.apple.foundationdb.record.TestNoIndexesProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;

/**
 * Tests of {@link QueryPlanner.IndexScanPreference}.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreIndexScanPreferenceTest extends FDBRecordStoreQueryTestBase {

    @Test
    public void noIndexes() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestNoIndexesProto.getDescriptor(), context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .build();

        for (QueryPlanner.IndexScanPreference indexScanPreference : QueryPlanner.IndexScanPreference.values()) {
            planner.setIndexScanPreference(indexScanPreference);
            RecordQueryPlan plan = planQuery(query);
            assertThat(plan, scan(unbounded()));
        }
    }

    @Test
    public void regularIndexes() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .build();

        for (QueryPlanner.IndexScanPreference indexScanPreference : QueryPlanner.IndexScanPreference.values()) {
            planner.setIndexScanPreference(indexScanPreference);
            RecordQueryPlan plan = planQuery(query);
            assertThat(plan, indexScanPreference == QueryPlanner.IndexScanPreference.PREFER_INDEX ?
                             indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), unbounded())) :
                             typeFilter(contains("MySimpleRecord"), scan(unbounded())));
        }
    }

    @Test
    public void primaryKeyIndex() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, md -> {
                md.addIndex("MySimpleRecord", "pkey", "rec_no");
            });
        }

        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MySimpleRecord")
                .build();

        for (QueryPlanner.IndexScanPreference indexScanPreference : QueryPlanner.IndexScanPreference.values()) {
            planner.setIndexScanPreference(indexScanPreference);
            RecordQueryPlan plan = planQuery(query);
            assertThat(plan, indexScanPreference == QueryPlanner.IndexScanPreference.PREFER_SCAN ?
                             typeFilter(contains("MySimpleRecord"), scan(unbounded())) :
                             indexScan(allOf(indexName("pkey"), unbounded())));
        }
    }

}
