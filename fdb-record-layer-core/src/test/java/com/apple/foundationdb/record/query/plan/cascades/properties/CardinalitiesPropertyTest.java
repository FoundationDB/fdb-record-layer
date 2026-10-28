/*
 * CardinalitiesPropertyTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.OuterJoinExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinalities;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinality;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RangeValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMaxOneRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.cardinalities;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CardinalitiesProperty}.
 */
class CardinalitiesPropertyTest {

    @Nonnull
    private static Quantifier.ForEach rangeQuantifier(final long endExclusive) {
        final RangeValue rangeValue = (RangeValue) new RangeValue.RangeFn()
                .encapsulate(CallSiteArguments.ofPositional(LiteralValue.ofScalar(endExclusive)));
        final TableFunctionExpression tvf = new TableFunctionExpression(rangeValue);
        return Quantifier.forEach(Reference.initialOf(tvf));
    }

    @Nonnull
    private static Quantifier.ForEach unknownCardinalityQuantifier() {
        // `FullUnorderedScanExpression` yields `unknownMaxCardinality()`: min = 0, max = unknown.
        final FullUnorderedScanExpression scan = new FullUnorderedScanExpression(
                ImmutableSet.of("T"), Type.Record.fromFields(ImmutableList.of()), new AccessHints());
        return Quantifier.forEach(Reference.initialOf(scan));
    }

    // ---- `Cardinality.max()` ----

    @Test
    void cardinalityMaxReturnsLargerOfTwoKnownCardinalities() {
        final Cardinality c1 = Cardinality.ofCardinality(3L);
        final Cardinality c2 = Cardinality.ofCardinality(7L);

        assertThat(c1.max(c2).getCardinality()).isEqualTo(7L);
        assertThat(c2.max(c1).getCardinality()).isEqualTo(7L);
    }

    @Test
    void cardinalityMaxIsUnknownWhenEitherSideIsUnknown() {
        final Cardinality known = Cardinality.ofCardinality(5L);
        final Cardinality unknown = Cardinality.unknownCardinality();

        assertThat(known.max(unknown).isUnknown()).isTrue();
        assertThat(unknown.max(known).isUnknown()).isTrue();
        assertThat(unknown.max(unknown).isUnknown()).isTrue();
    }

    @Test
    void cardinalityMaxOfEqualValuesReturnsThatValue() {
        final Cardinality c1 = Cardinality.ofCardinality(4L);
        final Cardinality c2 = Cardinality.ofCardinality(4L);

        assertThat(c1.max(c2).getCardinality()).isEqualTo(4L);
    }

    // ---- `floor()` ----

    @Test
    void cardinalityFloorByCardinalityRaisesBelowMinimum() {
        final Cardinality value = Cardinality.ofCardinality(2L);
        final Cardinality minimum = Cardinality.ofCardinality(5L);

        assertThat(value.floor(minimum).getCardinality()).isEqualTo(5L);
    }

    @Test
    void cardinalityFloorByCardinalityKeepsValueAtOrAboveMinimum() {
        final Cardinality value = Cardinality.ofCardinality(7L);
        final Cardinality minimum = Cardinality.ofCardinality(5L);

        // `floor()` returns the same instance when no change is needed.
        assertThat(value.floor(minimum)).isSameAs(value);
    }

    @Test
    void cardinalityFloorByUnknownMinimumIsNoOp() {
        final Cardinality value = Cardinality.ofCardinality(3L);

        assertThat(value.floor(Cardinality.unknownCardinality())).isSameAs(value);
    }

    @Test
    void unknownCardinalityFloorByKnownMinimumStaysUnknown() {
        final Cardinality unknown = Cardinality.unknownCardinality();

        // `floor(long)` on an unknown cardinality returns itself unchanged; `floor(Cardinality)` delegates.
        assertThat(unknown.floor(Cardinality.ofCardinality(5L)).isUnknown()).isTrue();
    }

    @Test
    void cardinalitiesFloorByCardinalityRaisesBothBoundsBelowMinimum() {
        final Cardinalities original = new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(2L));
        final Cardinalities raised = original.floor(Cardinality.ofCardinality(5L));

        assertThat(raised.getMinCardinality().getCardinality()).isEqualTo(5L);
        assertThat(raised.getMaxCardinality().getCardinality()).isEqualTo(5L);
    }

    @Test
    void cardinalitiesFloorByCardinalityRaisesOnlyMinWhenMaxAlreadyAbove() {
        final Cardinalities original = new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(10L));
        final Cardinalities raised = original.floor(Cardinality.ofCardinality(3L));

        assertThat(raised.getMinCardinality().getCardinality()).isEqualTo(3L);
        assertThat(raised.getMaxCardinality().getCardinality()).isEqualTo(10L);
    }

    @Test
    void cardinalitiesFloorByCardinalityIsNoOpWhenMinimumIsUnknown() {
        final Cardinalities original = new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(2L));

        assertThat(original.floor(Cardinality.unknownCardinality())).isSameAs(original);
    }

    @Test
    void cardinalitiesFloorByCardinalityIsNoOpWhenAlreadyAtOrAboveMinimum() {
        final Cardinalities original = new Cardinalities(Cardinality.ofCardinality(5L), Cardinality.ofCardinality(10L));

        // The implementation returns `this` when neither bound needs to change.
        assertThat(original.floor(Cardinality.ofCardinality(5L))).isSameAs(original);
    }

    // ---- `intersect()` ----

    @Test
    void cardinalitiesIntersect() {
        final Cardinalities c0_5 = new Cardinalities(Cardinality.ofCardinality(0L), Cardinality.ofCardinality(5L));
        final Cardinalities c1_unknown = new Cardinalities(Cardinality.ofCardinality(1L), Cardinality.unknownCardinality());

        // The larger minimum and the smaller maximum win; an unknown bound is ignored in favor of the known one.
        final Cardinalities c = c0_5.intersect(c1_unknown);
        assertThat(c.getMinCardinality().getCardinality()).isEqualTo(1L);
        assertThat(c.getMaxCardinality().getCardinality()).isEqualTo(5L);
        assertThat(c1_unknown.intersect(c0_5)).isEqualTo(c);

        // `unknownCardinalities()` is the identity element.
        assertThat(c0_5.intersect(Cardinalities.unknownCardinalities())).isEqualTo(c0_5);
    }

    // ---- `evaluate(Iterable)` ----

    @Test
    void evaluateEquivalentExpressionsIntersectsTheirCardinalities() {
        // RANGE(1) → (1, 1); FullUnorderedScan → (0, unknown). The intersection takes the tighter bound of each.
        final ImmutableList<RelationalExpression> expressions = ImmutableList.of(
                unknownCardinalityQuantifier().getRangesOver().get(),
                rangeQuantifier(1L).getRangesOver().get());
        final Cardinalities c = cardinalities().evaluate(expressions);
        assertThat(c.getMinCardinality().getCardinality()).isEqualTo(1L);
        assertThat(c.getMaxCardinality().getCardinality()).isEqualTo(1L);

        // No expressions at all yield no information.
        assertThat(cardinalities().evaluate(ImmutableList.of())).isEqualTo(Cardinalities.unknownCardinalities());
    }

    // ---- `ceiling()` ----

    @Test
    void cardinalityCeiling() {
        final Cardinality value = Cardinality.ofCardinality(2L);
        final Cardinality unknown = Cardinality.unknownCardinality();

        assertThat(Cardinality.ofCardinality(5L).ceiling(2L).getCardinality()).isEqualTo(2L);
        // `ceiling()` returns the same instance when no change is needed.
        assertThat(value.ceiling(2L)).isSameAs(value);
        assertThat(value.ceiling(5L)).isSameAs(value);
        assertThat(unknown.ceiling(1L)).isSameAs(unknown);
    }

    // ---- `orElse()` ----

    /**
     * Tests that {@code orElse()} returns a known cardinality unchanged and replaces an unknown one.
     */
    @Test
    void cardinalityOrElse() {
        final Cardinality value = Cardinality.ofCardinality(2L);
        final Cardinality other = Cardinality.ofCardinality(5L);

        assertThat(value.orElse(other)).isSameAs(value);
        assertThat(Cardinality.unknownCardinality().orElse(other)).isSameAs(other);
    }

    // ---- `visitRecordQueryMaxOneRecordPlan()` ----

    /**
     * Tests that a {@link RecordQueryMaxOneRecordPlan} over an inner of unknown maximum cardinality has an upper bound of
     * 1, while keeping the lower bound of the inner.
     */
    @Test
    void maxOneRecordPlan1() {
        final Cardinalities result = cardinalities().evaluate(new RecordQueryMaxOneRecordPlan(
                Quantifier.physical(Reference.plannedOf(new RecordQueryScanPlan(ScanComparisons.EMPTY, false)))));

        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(0L);
        assertThat(result.getMaxCardinality().getCardinality()).isEqualTo(1L);
    }

    /**
     * Tests that a {@link RecordQueryMaxOneRecordPlan} over an inner of exactly one record has a cardinality of 1.
     */
    @Test
    void maxOneRecordPlan2() {
        final Quantifier.Physical scanQuantifier =
                Quantifier.physical(Reference.plannedOf(new RecordQueryScanPlan(ScanComparisons.EMPTY, false)));
        final RecordQueryFirstOrDefaultPlan firstOrDefaultPlan = new RecordQueryFirstOrDefaultPlan(scanQuantifier,
                new NullValue(scanQuantifier.getFlowedObjectType().nullable()));
        final Cardinalities result = cardinalities().evaluate(
                new RecordQueryMaxOneRecordPlan(Quantifier.physical(Reference.plannedOf(firstOrDefaultPlan))));

        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(1L);
        assertThat(result.getMaxCardinality().getCardinality()).isEqualTo(1L);
    }

    // ---- `visitOuterJoinExpression()` ----

    @Test
    void outerJoinPreservesPreservedSideMinimumByFlooringTimesProduct() {
        // The preserved side has cardinality (1, 1) (one row from RANGE(1)).
        // The null-supplying side is (0, unknown).
        // Naive product: (0, unknown).
        // After flooring by `preserved.min = 1`: (1, unknown).
        final Quantifier.ForEach preserved = rangeQuantifier(1L);
        final Quantifier.ForEach nullSupplying = unknownCardinalityQuantifier();
        final OuterJoinExpression outerJoin = new OuterJoinExpression(
                preserved, nullSupplying, ImmutableList.of(), preserved.getFlowedObjectValue());

        final Cardinalities result = cardinalities().evaluate(outerJoin);

        assertThat(result.getMinCardinality().isUnknown()).isFalse();
        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(1L);
        assertThat(result.getMaxCardinality().isUnknown()).isTrue();
    }

    @Test
    void outerJoinWithBothSidesKnownComputesProductCardinalities() {
        // Preserved side = RANGE(2) → (2, 2); null-supplying side = RANGE(3) → (3, 3).
        // Product: (6, 6); floor by 2 → (6, 6).
        final Quantifier.ForEach preserved = rangeQuantifier(2L);
        final Quantifier.ForEach nullSupplying = rangeQuantifier(3L);
        final OuterJoinExpression outerJoin = new OuterJoinExpression(
                preserved, nullSupplying, ImmutableList.of(), preserved.getFlowedObjectValue());

        final Cardinalities result = cardinalities().evaluate(outerJoin);

        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(6L);
        assertThat(result.getMaxCardinality().getCardinality()).isEqualTo(6L);
    }

    @Test
    void outerJoinPropagatesUnknownPreservedMinimumIntoResult() {
        // Preserved side = FullUnorderedScan → (0, unknown); null-supplying side = RANGE(3) → (3, 3).
        // Product: (0, unknown); floor by 0 → (0, unknown).
        final Quantifier.ForEach preserved = unknownCardinalityQuantifier();
        final Quantifier.ForEach nullSupplying = rangeQuantifier(3L);
        final OuterJoinExpression outerJoin = new OuterJoinExpression(
                preserved, nullSupplying, ImmutableList.of(), preserved.getFlowedObjectValue());

        final Cardinalities result = cardinalities().evaluate(outerJoin);

        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(0L);
        assertThat(result.getMaxCardinality().isUnknown()).isTrue();
    }

    @Test
    void visitorVisitOuterJoinExpressionDirectlyComputesProductFlooredByPreservedMin() {
        final Quantifier.ForEach preserved = rangeQuantifier(4L);
        final Quantifier.ForEach nullSupplying = rangeQuantifier(5L);
        final OuterJoinExpression outerJoin = new OuterJoinExpression(
                preserved, nullSupplying, ImmutableList.of(), preserved.getFlowedObjectValue());

        final Cardinalities result = new CardinalitiesProperty.CardinalitiesVisitor().visitOuterJoinExpression(outerJoin);

        // (4, 4) × (5, 5) = (20, 20); floor by preserved.min = 4 → (20, 20).
        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(20L);
        assertThat(result.getMaxCardinality().getCardinality()).isEqualTo(20L);
    }

    @Test
    void visitorVisitOuterJoinExpressionDirectlyHandlesUnknownNullSupplyingMax() {
        final Quantifier.ForEach preserved = rangeQuantifier(2L);
        final Quantifier.ForEach nullSupplying = unknownCardinalityQuantifier();
        final OuterJoinExpression outerJoin = new OuterJoinExpression(
                preserved, nullSupplying, ImmutableList.of(), preserved.getFlowedObjectValue());

        final Cardinalities result = new CardinalitiesProperty.CardinalitiesVisitor().visitOuterJoinExpression(outerJoin);

        // (2, 2) × (0, unknown) = (0, unknown); floor by preserved.min = 2 → (2, unknown).
        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(2L);
        assertThat(result.getMaxCardinality().isUnknown()).isTrue();
    }

    @Test
    void visitorVisitOuterJoinExpressionDirectlyHandlesZeroCardinalityPreserved() {
        // RANGE(0) yields zero rows: cardinality (0, 0). The floor step with preserved.min = 0 is a no-op.
        final Quantifier.ForEach preserved = rangeQuantifier(0L);
        final Quantifier.ForEach nullSupplying = rangeQuantifier(5L);
        final OuterJoinExpression outerJoin = new OuterJoinExpression(
                preserved, nullSupplying, ImmutableList.of(), preserved.getFlowedObjectValue());

        final Cardinalities result = new CardinalitiesProperty.CardinalitiesVisitor().visitOuterJoinExpression(outerJoin);

        // (0, 0) × (5, 5) = (0, 0); floor by preserved.min = 0 → (0, 0).
        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(0L);
        assertThat(result.getMaxCardinality().getCardinality()).isEqualTo(0L);
    }

    // ---- `AtMostOne` quantifiers ----

    /**
     * Tests that an existential quantifier contributes exactly one item to the cardinality of a {@code SELECT}, no
     * matter how many items the sub-graph it ranges over produces.
     */
    @Test
    void selectWithExistentialQuantifierFlowsExactlyOneItem() {
        final Quantifier.ForEach forEach = rangeQuantifier(3L);
        final Quantifier.Existential existential =
                Quantifier.existential(unknownCardinalityQuantifier().getRangesOver());
        final SelectExpression select = new SelectExpression(forEach.getFlowedObjectValue(),
                ImmutableList.of(forEach, existential), ImmutableList.of());

        final Cardinalities result = cardinalities().evaluate(select);

        // (3, 3) × (1, 1) = (3, 3), even though the existential ranges over a sub-graph with unknown cardinality.
        assertThat(result.getMinCardinality().getCardinality()).isEqualTo(3L);
        assertThat(result.getMaxCardinality().getCardinality()).isEqualTo(3L);
    }

    /**
     * Tests that, in contrast to an existential quantifier, a for-each quantifier over the same sub-graph leaves the
     * cardinality of a {@code SELECT} unknown.
     */
    @Test
    void selectWithForEachQuantifierOverUnknownCardinalityIsUnknown() {
        final Quantifier.ForEach forEach = rangeQuantifier(3L);
        final SelectExpression select = new SelectExpression(forEach.getFlowedObjectValue(),
                ImmutableList.of(forEach, unknownCardinalityQuantifier()), ImmutableList.of());

        final Cardinalities result = cardinalities().evaluate(select);

        assertThat(result.getMaxCardinality().isUnknown()).isTrue();
    }
}
