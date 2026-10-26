/*
 * TextScanPlannerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.textIndexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

/**
 * Tests of {@link TextScanPlanner} that need no database, since planning only needs the meta-data.
 */
class TextScanPlannerTest {
    private static final String INDEX_NAME = "MyRecord$header_path";

    private static RecordQueryPlanner planner() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
        builder.getRecordType("MyRecord").setPrimaryKey(field("header").nest("rec_no"));
        builder.addIndex("MyRecord", new Index(INDEX_NAME, field("header").nest("path"), IndexTypes.TEXT));
        return new RecordQueryPlanner(builder.getRecordMetaData(), new RecordStoreState(null, null));
    }

    @Test
    void plansTextPredicateOnSingularNestedFieldWithTextIndex() {
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.field("path").text().contains("foundation")))
                .build();

        final RecordQueryPlan plan = planner().plan(query);

        assertThat(plan, textIndexScan(indexName(INDEX_NAME)));
    }

    @Test
    void doesNotUseTextIndexForNonTextPredicateOnSingularNestedField() {
        // An equality comparison on the indexed field is not a text predicate, so the text index cannot satisfy it.
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("MyRecord")
                .setFilter(Query.field("header").matches(Query.field("path").equalsValue("foundation")))
                .build();

        final RecordQueryPlan plan = planner().plan(query);

        assertThat(plan, not(textIndexScan(indexName(INDEX_NAME))));
    }
}
