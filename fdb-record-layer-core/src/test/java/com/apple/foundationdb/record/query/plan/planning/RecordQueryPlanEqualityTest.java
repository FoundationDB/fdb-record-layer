/*
 * RecordQueryPlanEqualityTest.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link Object#equals} of {@link RecordQueryPlan}.
 */
public class RecordQueryPlanEqualityTest {
    private RecordQueryPlan scanPlan() {
        return new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
    }

    private RecordQueryPlan indexPlanEquals(String indexName, Object value) {
        IndexScanParameters scan = IndexScanComparisons.byValue(new ScanComparisons(Arrays.asList(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value)),
                        Collections.emptySet()));
        return new RecordQueryIndexPlan(indexName, scan, false);
    }

    private RecordQueryPlan unionPlan(Object value1, Object value2) {
        return RecordQueryUnionPlan.from(
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", value1),
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", value2),
                Key.Expressions.field("num_value_3_indexed"), false);
    }

    private RecordQueryPlan distinctUnorderedUnionPlan(Object value1, Object value2) {
        return new RecordQueryUnorderedDistinctPlan(
                RecordQueryUnorderedUnionPlan.from(
                        indexPlanEquals("MySimpleRecord$num_value_3_indexed", value1),
                        indexPlanEquals("MySimpleRecord$num_value_3_indexed", value2)),
                Key.Expressions.field("a_field"));
    }

    private RecordQueryPlan intersectionPlan(Object value1, Object value2) {
        return RecordQueryIntersectionPlan.from(
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", value1),
                indexPlanEquals("MySimpleRecord$num_value_3_indexed", value2),
                Key.Expressions.field("num_value_3_indexed"));
    }

    private List<Supplier<RecordQueryPlan>> planSuppliers = Arrays.asList(
            // don't test RecordQueryCoveringIndexPlan since mocking it is complicated
            () -> new RecordQueryFilterPlan(scanPlan(), Query.field("a_field").lessThan(50)),
            () -> indexPlanEquals("testIndex", 4),
            () -> new RecordQueryInParameterJoinPlan(scanPlan(), "testField", Bindings.Internal.IN, "testField2", false, false),
            () -> intersectionPlan(2, "even"),
            () -> new RecordQueryInValuesJoinPlan(scanPlan(), "testField", Bindings.Internal.IN, Arrays.asList(1, 2, 3),
                    false, false),
            () -> new RecordQueryLoadByKeysPlan("param"),
            () -> scanPlan(),
            // don't test RecordQueryScoreForRankPlan since mocking it is complicated
            () -> new RecordQueryTypeFilterPlan(scanPlan(), Arrays.asList("MySimpleRecord")),
            () -> unionPlan(2, 4),
            () -> new RecordQueryUnorderedPrimaryKeyDistinctPlan(indexPlanEquals("testIndex", 2)),
            () -> new RecordQueryUnorderedDistinctPlan(indexPlanEquals("testIndex", 2),
                    Key.Expressions.field("a_field")),
            () -> new RecordQueryFilterPlan(scanPlan(), // complex comparison for deep equals() testing
                    Query.and(Query.not(
                            new FieldWithComparison("testField", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5))),
                            new FieldWithComparison("testField2", new Comparisons.ListComparison(Comparisons.Type.IN, Arrays.asList(1, 3)))))
    );

    /**
     * Runs the given function twice, to generate two RecordQueryPlans that _should_ be identical.
     * Then asserts that they are equal and have matching hash codes.
     */
    private void assertSimpleEquality(Supplier<RecordQueryPlan> supplier) {
        final RecordQueryPlan plan1 = supplier.get();
        final RecordQueryPlan plan2 = supplier.get();
        assertEquals(plan1.hashCode(), plan2.hashCode());
        assertEquals(plan1, plan2);
    }

    @Test
    public void testSimplePlanEquality() {
        for (Supplier<RecordQueryPlan> supplier : planSuppliers) {
            assertSimpleEquality(supplier);
        }
    }

    @Test
    public void differentPlanClassesNotEqual() {
        for (int i = 0; i < planSuppliers.size(); i++) {
            for (int j = i + 1; j < planSuppliers.size(); j++) {
                final RecordQueryPlan plan1 = planSuppliers.get(i).get();
                final RecordQueryPlan plan2 = planSuppliers.get(j).get();
                assertNotEquals(plan1, plan2);
            }
        }
    }

    @Test
    public void childOrderDoesNotMatter() {
        RecordQueryPlan plan1 = unionPlan(2, 4);
        RecordQueryPlan plan2 = unionPlan(4, 2);
        assertNotEquals(plan1, plan2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertTrue(plan1.semanticEquals(plan2));
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());

        plan1 = distinctUnorderedUnionPlan(2, 4);
        plan2 = distinctUnorderedUnionPlan(4, 2);
        assertNotEquals(plan1, plan2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertTrue(plan1.semanticEquals(plan2));

        plan1 = intersectionPlan(2, 4);
        plan2 = intersectionPlan(4, 2);
        assertNotEquals(plan1, plan2);
        assertNotEquals(plan1.hashCode(), plan2.hashCode());
        assertTrue(plan1.semanticEquals(plan2));
        assertEquals(plan1.semanticHashCode(), plan2.semanticHashCode());
    }
}
