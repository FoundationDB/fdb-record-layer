/*
 * LuceneSyntheticPlannerTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.Sets;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.query;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.scanParams;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScanType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasToString;

/**
 * Tests about applying the Lucene index when used as part of a SyntheticRecord join.
 */
public class LuceneSyntheticPlannerTest extends FDBRecordStoreTestBase {

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook, boolean attemptWholeFilter) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsJoinIndexProto.getDescriptor());
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData()).createOrOpen();

        PlannableIndexTypes indexTypes = new PlannableIndexTypes(
                    Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                    Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                    Sets.newHashSet(IndexTypes.TEXT),
                    Sets.newHashSet(LuceneIndexTypes.LUCENE)
            );
        planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setPlanOtherAttemptWholeFilter(attemptWholeFilter)
                .build());
    }

    /**
     * Verify that Lucene index on Joined record gives covering scan.
     */
    @Test
    void canPlanQueryAgainstSyntheticLuceneType() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.getRecordType("CustomerWithHeader")
                        .setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));
                metaDataBuilder.getRecordType("OrderWithHeader")
                        .setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

                //set up the joined index
                final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("luceneJoinedIdx");
                joined.addConstituent("order", "OrderWithHeader");
                joined.addConstituent("cust", "CustomerWithHeader");
                joined.addJoin("order", field("___header").nest("z_key"),
                        "cust", field("___header").nest("z_key"));
                joined.addJoin("order", field("custRef").nest("string_value"),
                        "cust", field("___header").nest("rec_id"));

                metaDataBuilder.addIndex(joined, new Index("joinNestedConcat", concat(
                        field("cust").nest(function(LuceneFunctionNames.LUCENE_STORED, field("name"))),
                        field("order").nest(concat(function(LuceneFunctionNames.LUCENE_STORED, field("order_no")),
                                function(LuceneFunctionNames.LUCENE_TEXT, field("order_desc"))
                        ))
                ), LuceneIndexTypes.LUCENE));
                metaDataBuilder.addIndex("OrderWithHeader", "order$custRef", concat(field("___header").nest("z_key"), field("custRef").nest("string_value")));
            }, false);

            String luceneSearch = "order_order_desc: \"twelve pineapple\" and cust_name: \"steve\"";
            QueryComponent filter = new LuceneQueryComponent(luceneSearch, List.of("order", "cust"));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("luceneJoinedIdx")
                    .setFilter(filter)
                    .setRequiredResults(List.of(Key.Expressions.field("order").nest("order_no")))
                    .build();
            final RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = coveringIndexScan(indexScan(allOf(
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName("joinNestedConcat"),
                    scanParams(query(hasToString(luceneSearch)))
            )));
            assertThat(plan, matcher);
        }

    }
}
