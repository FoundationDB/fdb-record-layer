/*
 * ImplementIntersectionRuleTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImplementIntersectionRuleTest {
    @Nonnull
    private static final ImplementIntersectionRule rule = new ImplementIntersectionRule();
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(rule, PlannerPhase.PLANNING);

    /**
     * Tests that the rule implements an intersection of plain for-each legs without wrapping them.
     */
    @Test
    void testIntersection1() {
        final RecordQueryIntersectionOnValuesPlan plan = implement(false);

        for (final Quantifier quantifier : plan.getQuantifiers()) {
            assertInstanceOf(RecordQueryIndexPlan.class, onlyPlan(quantifier));
        }
    }

    /**
     * Tests that the rule wraps each null-on-empty leg in a {@link RecordQueryDefaultOnEmptyPlan}.
     */
    @Test
    void testIntersection2() {
        final RecordQueryIntersectionOnValuesPlan plan = implement(true);

        assertEquals(2, plan.getQuantifiers().size());
        for (final Quantifier quantifier : plan.getQuantifiers()) {
            final RecordQueryDefaultOnEmptyPlan wrapper =
                    assertInstanceOf(RecordQueryDefaultOnEmptyPlan.class, onlyPlan(quantifier));
            assertInstanceOf(RecordQueryIndexPlan.class, wrapper.getChild());
        }
    }

    @Nonnull
    private static RecordQueryIntersectionOnValuesPlan implement(final boolean nullOnEmpty) {
        final Quantifier leg1 = leg(nullOnEmpty);
        final Quantifier leg2 = leg(nullOnEmpty);
        final FieldValue comparisonValue =
                FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), RuleTestHelper.TYPE_S), "one");
        final LogicalIntersectionExpression intersection = LogicalIntersectionExpression.fromQuantifiers(
                List.of((Quantifier.ForEach)leg1, (Quantifier.ForEach)leg2),
                List.of(new ProvidedOrderingPart(comparisonValue, ProvidedSortOrder.ASCENDING)));

        final TestRuleExecution execution = testHelper.run(intersection, EvaluationContext.EMPTY);
        assertTrue(execution.hasYielded());
        final RecordQueryIntersectionOnValuesPlan plan =
                execution.getResultMemberWithClass(RecordQueryIntersectionOnValuesPlan.class);
        assertNotNull(plan);
        return plan;
    }

    @Nonnull
    private static Quantifier leg(final boolean nullOnEmpty) {
        final Reference reference = Reference.plannedOf(indexScan());
        return nullOnEmpty ? Quantifier.forEachWithNullOnEmpty(reference) : Quantifier.forEach(reference);
    }

    @Nonnull
    private static Object onlyPlan(@Nonnull final Quantifier quantifier) {
        return Iterables.getOnlyElement(quantifier.getRangesOver().getFinalExpressions());
    }

    @Nonnull
    private static RecordQueryIndexPlan indexScan() {
        return new RecordQueryIndexPlan("IndexA", null, IndexScanComparisons.byValue(), IndexFetchMethod.SCAN_AND_FETCH,
                FetchIndexRecords.PRIMARY_KEY, false, false, Optional.empty(), RuleTestHelper.TYPE_S,
                QueryPlanConstraint.noConstraint());
    }
}
