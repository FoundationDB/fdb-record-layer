/*
 * FDBRecordStoreLimitTestBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.limits;

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Base class for tests of various scan limits.
 */
@Tag(Tags.RequiresFDB)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FDBRecordStoreLimitTestBase extends FDBRecordStoreTestBase {
    @BeforeAll
    public void init() {
        clearAndInitialize();
    }

    @BeforeEach
    public void setupSimpleRecordStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(i + 1000);
                recBuilder.setNumValue3Indexed(i % 3);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    private RecordQueryPlan indexPlanEquals(String indexName, Object value) {
        IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Arrays.asList(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value)),
                        Collections.emptySet()));
        return new RecordQueryIndexPlan(indexName, scan, false);
    }

    private KeyExpression primaryKey() {
        return field("rec_no");
    }

    public Stream<Arguments> plans(boolean fail) {
        RecordQueryPlan scanPlan = new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
        IndexScanParameters fullValueScan = IndexScanComparisons.byValue();
        RecordQueryPlan indexPlan = new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed",
                fullValueScan, false);
        QueryComponent filter = Query.field("str_value_indexed").equalsValue("odd");
        QueryComponent middleFilter = Query.and(
                Query.field("rec_no").greaterThan(24L),
                Query.field("rec_no").lessThan(60L));
        RecordQueryPlan firstChild = indexPlanEquals("MySimpleRecord$str_value_indexed", "even");
        RecordQueryPlan secondChild = indexPlanEquals("MySimpleRecord$num_value_3_indexed", 0);
        return Stream.of(
                Arguments.of("full record scan", fail, scanPlan),
                Arguments.of("simple index scan", fail, indexPlan),
                Arguments.of("reverse index scan", fail, new RecordQueryIndexPlan("MySimpleRecord$str_value_indexed", fullValueScan, true)),
                Arguments.of("filter on scan plan", fail, new RecordQueryFilterPlan(scanPlan, filter)),
                Arguments.of("filter on index plan", fail, new RecordQueryFilterPlan(indexPlan, filter)),
                Arguments.of("type filter on scan plan", fail, new RecordQueryTypeFilterPlan(scanPlan, Collections.singletonList("MySimpleRecord"))),
                Arguments.of("type filter on index plan", fail, new RecordQueryTypeFilterPlan(indexPlan, Collections.singletonList("MySimpleRecord"))),
                Arguments.of("disjoint union", fail, RecordQueryUnionPlan.from(
                        indexPlanEquals("MySimpleRecord$str_value_indexed", "odd"),
                        indexPlanEquals("MySimpleRecord$str_value_indexed", "even"),
                        primaryKey(), false)),
                Arguments.of("overlapping union", fail, RecordQueryUnionPlan.from(firstChild, secondChild, primaryKey(), false)),
                Arguments.of("overlapping union (swapped args)", fail, RecordQueryUnionPlan.from(secondChild, firstChild, primaryKey(), false)),
                Arguments.of("overlapping intersection", fail, RecordQueryIntersectionPlan.from(firstChild, secondChild, primaryKey())),
                Arguments.of("overlapping intersection", fail, RecordQueryIntersectionPlan.from(secondChild, firstChild, primaryKey())),
                Arguments.of("union with inner filter", fail, RecordQueryUnionPlan.from(
                        new RecordQueryFilterPlan(firstChild, middleFilter), secondChild, primaryKey(), false)),
                Arguments.of("union with two inner filters", fail, RecordQueryUnionPlan.from(
                        new RecordQueryFilterPlan(firstChild, middleFilter),
                        new RecordQueryFilterPlan(secondChild, Query.field("rec_no").lessThan(55L)),
                        primaryKey(), false)),
                Arguments.of("intersection with inner filter", fail, RecordQueryIntersectionPlan.from(
                        new RecordQueryFilterPlan(firstChild, middleFilter), secondChild, primaryKey())),
                Arguments.of("intersection with two inner filters", fail, RecordQueryIntersectionPlan.from(
                        new RecordQueryFilterPlan(firstChild, middleFilter),
                        new RecordQueryFilterPlan(secondChild, Query.field("rec_no").lessThan(55L)),
                        primaryKey())));
    }

    public Stream<Arguments> unorderedPlans(boolean fail) {
        return Stream.of(
                Arguments.of("unordered union", fail, RecordQueryUnorderedUnionPlan.from(indexPlanEquals("MySimpleRecord$str_value_indexed", "even"), indexPlanEquals("MySimpleRecord$num_value_3_indexed", 2)))
        );
    }
}
