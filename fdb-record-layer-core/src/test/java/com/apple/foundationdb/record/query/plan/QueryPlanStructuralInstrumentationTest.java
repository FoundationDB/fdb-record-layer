/*
 * QueryPlanStructuralInstrumentationTest.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for instrumentation related to query planning and structure of the generated plan.
 */
public class QueryPlanStructuralInstrumentationTest {
    private static int VALUE = 4;

    private RecordQueryPlan indexPlanEquals(String indexName, Object value) {
        IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Arrays.asList(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value)),
                        Collections.emptySet()));
        return new RecordQueryIndexPlan(indexName, scan, false);
    }

    private void assertNoIndexes(RecordQueryPlan plan) {
        assertUsesIndexes(plan, new ArrayList<>());
    }

    private void assertUsesIndexes(RecordQueryPlan plan, Collection<String> indexes) {
        Set<String> used = plan.getUsedIndexes();
        assertEquals(used.size(), indexes.size());
        assertTrue(used.containsAll(indexes));
    }

    @Test
    public void indexPlan() {
        final String indexName = "an_index";
        StoreTimer timer = new FDBStoreTimer();
        RecordQueryPlan plan = indexPlanEquals(indexName, VALUE);
        plan.logPlanStructure(timer);

        assertUsesIndexes(plan, Lists.newArrayList(indexName));
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_INDEX), 1);
    }

    @Test
    public void fullScan() {
        StoreTimer timer = new FDBStoreTimer();
        RecordQueryPlan plan = new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
        plan.logPlanStructure(timer);

        assertNoIndexes(plan);
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_SCAN), 1);
    }

    @Test
    public void in() {
        final String indexName = "a_field";
        final IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Arrays.asList(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "another_field")), Collections.emptySet()));
        final RecordQueryPlan plan = new RecordQueryInValuesJoinPlan(
                new RecordQueryIndexPlan(indexName, scan, false),
                "another_field",
                Bindings.Internal.IN,
                Arrays.asList(2, 4),
                false,
                false);
        assertUsesIndexes(plan, Lists.newArrayList(indexName));

        final StoreTimer timer = new FDBStoreTimer();
        plan.logPlanStructure(timer);
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_IN_VALUES), 1);
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_INDEX), 1);
    }

    @Test
    public void unionSameIndex() {
        final RecordQueryPlan plan = RecordQueryUnionPlan.from(
                indexPlanEquals("index_1", 2),
                indexPlanEquals("index_1", 4),
                EmptyKeyExpression.EMPTY, false);

        assertUsesIndexes(plan, Lists.newArrayList("index_1"));

        final StoreTimer timer = new FDBStoreTimer();
        plan.logPlanStructure(timer);
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_UNION), 1);
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_INDEX), 2);
    }

    @Test
    public void unionDifferentIndex() {
        final RecordQueryPlan plan = RecordQueryUnionPlan.from(
                indexPlanEquals("index_1", 2),
                indexPlanEquals("index_2", 4),
                EmptyKeyExpression.EMPTY, false);

        assertUsesIndexes(plan, Lists.newArrayList("index_1", "index_2"));

        final StoreTimer timer = new FDBStoreTimer();
        plan.logPlanStructure(timer);
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_UNION), 1);
        assertEquals(timer.getCount(FDBStoreTimer.Counts.PLAN_INDEX), 2);
    }

    @Test
    public void noIndexes() {
        final RecordQueryPlan plan = new RecordQueryLoadByKeysPlan("test");
        assertNoIndexes(plan);
    }

}
