/*
 * TiebreakerTests.java
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FindExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInComparandJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of {@link Tiebreaker} implementations along.
 */
class TiebreakerImplementationTests {
    private static final Set<Class<? extends RelationalExpression>> interestingClasses = ImmutableSet.of();

    @Nonnull
    private static Quantifier.Physical physicalOf(@Nonnull RecordQueryPlan plan) {
        return Quantifier.physical(Reference.ofFinalExpression(PlannerStage.PLANNED, plan));
    }

    private static <T extends RelationalExpression> int compare(@Nonnull Tiebreaker<? super T> tiebreaker, @Nonnull T a, @Nonnull T b) {
        return tiebreaker.compare(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                FindExpressionVisitor.evaluate(interestingClasses, a), FindExpressionVisitor.evaluate(interestingClasses, b),
                a, b);
    }

    private static int signum(int intValue) {
        if (intValue < 0) {
            return -1;
        } else if (intValue > 0) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Assert that a {@link Tiebreaker} compares two values in an expected way.
     *
     * @param tiebreaker the {@link Tiebreaker} to use to compare expressions
     * @param a the left expression in a comparison
     * @param b the right expression in a comparison
     * @param comparison a value representing the expected signum of the comparison
     * @param <T> the type of expression consumed by the {@link Tiebreaker}
     */
    private static <T extends RelationalExpression> void assertCompares(@Nonnull Tiebreaker<? super T> tiebreaker, @Nonnull T a, @Nonnull T b, int comparison) {
        assertThat(signum(compare(tiebreaker, a, b)))
                .as("comparison of %s and %s should have same sign as %s", a, b, comparison)
                .isEqualTo(signum(comparison));

        // Reversing the comparison should always return the opposite of the original expected comparison
        assertThat(signum(compare(tiebreaker, b, a)))
                .as("comparison of %s and %s should have same sign as %s", b, a, -1 * comparison)
                .isEqualTo(-1 * signum(comparison));
    }

    /**
     * Assert that an item compares equal to itself with a given tiebreaker. With few exceptions (like the
     * {@link PickRightTiebreaker}), {@link Tiebreaker} implementations should generally consider expressions
     * to be equal to themselves.
     *
     * @param tiebreaker the {@link Tiebreaker} to use to compare expressions
     * @param t the expression to compare to itself
     * @param <T> the type of expression expected by the {@link Tiebreaker}
     */
    private static <T extends RelationalExpression> void assertReflexive(@Nonnull Tiebreaker<? super T> tiebreaker, @Nonnull T t) {
        assertThat(compare(tiebreaker, t, t))
                .isZero();
    }

    /**
     * Validate that the {@link PlanningCostModel#inOperatorTiebreaker()} compares values in the expected way.
     */
    @Test
    void inOperatorTest() {
        // Construct three plans that we commonly see with IN comparisons:
        //   1. An index scan with a single filter (executing the IN) on top
        //   2. An in-join over an index scan, with the predicate from the IN pushed into the scan
        //   3. An in-join over an index scan, but with the corresponding predicate executed as residual filter
        // This tiebreaker is designed to reject the third plan. The second plan is also better than the
        // first plan, but that is a different tiebreaker's job to distinguish.
        final Tiebreaker<RecordQueryPlan> inOperatorTiebreaker = PlanningCostModel.inOperatorTiebreaker();
        final Value arrayValue = ConstantObjectValue.of(Quantifier.constant(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.LONG, false)));
        final Comparisons.Comparison arrayComparison = new Comparisons.ValueComparison(Comparisons.Type.IN, arrayValue);

        // Create a plan where the entire index is scanned, and then we execute the IN as a residual filter
        final RecordQueryIndexPlan scanAllIndexPlan = new RecordQueryIndexPlan("blahIndex", new IndexScanComparisons(IndexScanType.BY_VALUE, ScanComparisons.EMPTY), false);
        final Quantifier.Physical scanAllQun = physicalOf(scanAllIndexPlan);
        final RecordQueryPredicatesFilterPlan filterScanWithInPlan = new RecordQueryPredicatesFilterPlan(scanAllQun, ImmutableList.of(scanAllQun.getFlowedObjectValue().withComparison(arrayComparison)));
        assertReflexive(inOperatorTiebreaker, filterScanWithInPlan);

        // Create an in plan where the in value is sarged into the index scan
        final String bindingName = "__corr_x";
        final CorrelationIdentifier bindingAlias = CorrelationIdentifier.of(Bindings.Internal.CORRELATION.identifier(bindingName));
        final Comparisons.ValueComparison singleValueComparison = new Comparisons.ValueComparison(Comparisons.Type.EQUALS, QuantifiedObjectValue.of(bindingAlias, Type.primitiveType(Type.TypeCode.LONG, false)));
        final RecordQueryIndexPlan scanSingleIndexPlan = new RecordQueryIndexPlan("blahIndex",
                new IndexScanComparisons(IndexScanType.BY_VALUE,
                        Objects.requireNonNull(ScanComparisons.from(singleValueComparison))),
                false);
        final RecordQueryInComparandJoinPlan inJoinSargedComparisonPlan = new RecordQueryInComparandJoinPlan(physicalOf(scanSingleIndexPlan), bindingName, Bindings.Internal.CORRELATION, arrayComparison, false, false);
        assertReflexive(inOperatorTiebreaker, inJoinSargedComparisonPlan);

        // Create an in plan where the in value is _not_ sarged into the index scan
        final RecordQueryPredicatesFilterPlan filterIndexForSingleValue = new RecordQueryPredicatesFilterPlan(scanAllQun, ImmutableList.of(scanAllQun.getFlowedObjectValue().withComparison(singleValueComparison)));
        final RecordQueryInComparandJoinPlan inJoinNonSargedPlan = new RecordQueryInComparandJoinPlan(physicalOf(filterIndexForSingleValue), bindingName, Bindings.Internal.CORRELATION, arrayComparison, false, false);
        assertReflexive(inOperatorTiebreaker, inJoinNonSargedPlan);

        // Compare each plan pairwise
        assertCompares(inOperatorTiebreaker, filterScanWithInPlan, inJoinNonSargedPlan, -1);
        assertCompares(inOperatorTiebreaker, filterScanWithInPlan, inJoinSargedComparisonPlan, 0);
        assertCompares(inOperatorTiebreaker, inJoinNonSargedPlan, inJoinSargedComparisonPlan, 1);
    }
}
