/*
 * QuantifierTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMaxOneRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link Quantifier}.
 */
class QuantifierTest {
    @Nonnull
    private final Memoizer memoizer = Memoizer.noMemoization(PlannerStage.PLANNED);
    @Nonnull
    private final RecordQueryScanPlan scanPlan = new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
    @Nonnull
    private final Reference scanReference = Reference.plannedOf(scanPlan);

    /**
     * Asserts that {@code wrapper} ranges over the scan plan via a single physical quantifier with the alias of
     * {@code quantifier}.
     */
    private void assertWrapsScanPlan(@Nonnull final RelationalExpression wrapper, @Nonnull final Quantifier quantifier) {
        final Quantifier inner = Iterables.getOnlyElement(wrapper.getQuantifiers());
        assertThat(inner).isInstanceOf(Quantifier.Physical.class);
        assertThat(inner.getAlias()).isEqualTo(quantifier.getAlias());
        assertThat(inner.getRangesOver().getFinalExpressions()).containsExactly(scanPlan);
    }

    /**
     * Tests that {@code applyGlue()} returns the given reference unchanged for a plain for-each quantifier.
     */
    @Test
    void testApplyGlue1() {
        assertThat(Quantifier.forEach(scanReference).applyGlue(memoizer, scanReference))
                .isSameAs(scanReference);
    }

    /**
     * Tests that {@code applyGlue()} wraps the given reference in a {@link RecordQueryDefaultOnEmptyPlan} with a
     * {@code NULL} default for a null-on-empty for-each quantifier.
     */
    @Test
    void testApplyGlue2() {
        final Quantifier.ForEach quantifier = Quantifier.forEachWithNullOnEmpty(scanReference);
        final Reference glued = quantifier.applyGlue(memoizer, scanReference);

        final RelationalExpression wrapper = Iterables.getOnlyElement(glued.getFinalExpressions());
        assertThat(wrapper).isInstanceOf(RecordQueryDefaultOnEmptyPlan.class);
        assertThat(((RecordQueryDefaultOnEmptyPlan)wrapper).getOnEmptyResultValue()).isInstanceOf(NullValue.class);
        assertWrapsScanPlan(wrapper, quantifier);
    }

    /**
     * Tests that {@code applyGlue()} wraps the given reference in a {@link RecordQueryFirstOrDefaultPlan} with a
     * {@code NULL} default for an existential quantifier.
     */
    @Test
    void testApplyGlue3() {
        final Quantifier.Existential quantifier = Quantifier.existential(scanReference);
        final Reference glued = quantifier.applyGlue(memoizer, scanReference);

        final RelationalExpression wrapper = Iterables.getOnlyElement(glued.getFinalExpressions());
        assertThat(wrapper).isInstanceOf(RecordQueryFirstOrDefaultPlan.class);
        assertThat(((RecordQueryFirstOrDefaultPlan)wrapper).getOnEmptyResultValue()).isInstanceOf(NullValue.class);
        assertWrapsScanPlan(wrapper, quantifier);
    }

    /**
     * Tests that the {@link Memoizer.ReferenceOfPlansBuilder} variant of {@code applyGlue()} returns the given builder
     * unchanged for a plain for-each quantifier, and wraps its reference for a null-on-empty one.
     */
    @Test
    void testApplyGlue4() {
        final Memoizer.ReferenceOfPlansBuilder builder = memoizer.memoizePlansBuilder(ImmutableList.of(scanPlan));
        assertThat(Quantifier.forEach(scanReference).applyGlue(memoizer, builder)).isSameAs(builder);

        final Quantifier.ForEach quantifier = Quantifier.forEachWithNullOnEmpty(scanReference);
        final Memoizer.ReferenceOfPlansBuilder glued = quantifier.applyGlue(memoizer, builder);
        final RelationalExpression wrapper = Iterables.getOnlyElement(glued.members());
        assertThat(wrapper).isInstanceOf(RecordQueryDefaultOnEmptyPlan.class);
        assertWrapsScanPlan(wrapper, quantifier);
    }

    /**
     * Tests that the {@link Memoizer.ReferenceOfPlansBuilder} variant of {@code applyGlue()} does not obtain the
     * reference of the builder for a quantifier that needs no glue, since doing so memoizes the plans of the builder.
     */
    @Test
    void testApplyGlue5() {
        final Memoizer.ReferenceOfPlansBuilder builder = new Memoizer.ReferenceOfPlansBuilder() {
            @Nonnull
            @Override
            public Reference reference() {
                throw new AssertionError("reference() must not be called");
            }

            @Nonnull
            @Override
            public Set<? extends RecordQueryPlan> members() {
                return ImmutableSet.of(scanPlan);
            }
        };
        assertThat(Quantifier.forEach(scanReference).applyGlue(memoizer, builder)).isSameAs(builder);
    }

    /**
     * Tests that both variants of {@code applyGlue()} return their input unchanged for a physical quantifier.
     */
    @Test
    void testApplyGlue6() {
        final Quantifier.Physical quantifier = Quantifier.physical(scanReference);
        assertThat(quantifier.applyGlue(memoizer, scanReference)).isSameAs(scanReference);

        final Memoizer.ReferenceOfPlansBuilder builder = memoizer.memoizePlansBuilder(ImmutableList.of(scanPlan));
        assertThat(quantifier.applyGlue(memoizer, builder)).isSameAs(builder);
    }

    /**
     * Tests that the glue plans of a null-on-empty for-each quantifier and of an existential quantifier report the
     * direction of their input, even if the glued reference holds more than one plan.
     */
    @Test
    void testGluePlanIsReverse1() {
        final Reference reverseReference = memoizer.memoizePlansBuilder(ImmutableList.of(
                new RecordQueryScanPlan(ImmutableSet.of("A"), new Type.Any(), null, ScanComparisons.EMPTY, true, false, Optional.empty()),
                new RecordQueryScanPlan(ImmutableSet.of("B"), new Type.Any(), null, ScanComparisons.EMPTY, true, false, Optional.empty()))).reference();
        assertThat(reverseReference.getFinalExpressions()).hasSize(2);

        final RecordQueryPlan defaultOnEmptyPlan = (RecordQueryPlan)Iterables.getOnlyElement(
                Quantifier.forEachWithNullOnEmpty(reverseReference).applyGlue(memoizer, reverseReference).getFinalExpressions());
        assertThat(defaultOnEmptyPlan).isInstanceOf(RecordQueryDefaultOnEmptyPlan.class);
        assertThat(defaultOnEmptyPlan.isReverse()).isTrue();

        final RecordQueryPlan firstOrDefaultPlan = (RecordQueryPlan)Iterables.getOnlyElement(
                Quantifier.existential(reverseReference).applyGlue(memoizer, reverseReference).getFinalExpressions());
        assertThat(firstOrDefaultPlan).isInstanceOf(RecordQueryFirstOrDefaultPlan.class);
        assertThat(firstOrDefaultPlan.isReverse()).isTrue();
    }

    /**
     * Tests that rebuilding an existential quantifier via {@code toBuilder()} preserves its alias unless a new one is
     * given.
     */
    @Test
    void testExistential1() {
        final Quantifier.Existential quantifier = Quantifier.existential(scanReference);

        assertThat(quantifier.toBuilder().build(scanReference))
                .isInstanceOfSatisfying(Quantifier.Existential.class,
                        rebuilt -> assertThat(rebuilt.getAlias()).isEqualTo(quantifier.getAlias()));

        final CorrelationIdentifier newAlias = CorrelationIdentifier.of("q");
        assertThat(quantifier.toBuilder().withAlias(newAlias).build(scanReference))
                .isInstanceOfSatisfying(Quantifier.Existential.class,
                        rebuilt -> assertThat(rebuilt.getAlias()).isEqualTo(newAlias));
    }

    /**
     * Tests that {@code overNewReference()} on an existential quantifier keeps it existential, and keeps its alias
     * unless a new one is given.
     */
    @Test
    void testExistential2() {
        final Quantifier.Existential quantifier = Quantifier.existential(scanReference);
        final Reference otherReference = Reference.plannedOf(new RecordQueryScanPlan(ScanComparisons.EMPTY, true));

        final Quantifier.Existential moved = quantifier.overNewReference(otherReference);
        assertThat(moved.getAlias()).isEqualTo(quantifier.getAlias());
        assertThat(moved.getRangesOver()).isSameAs(otherReference);

        final CorrelationIdentifier newAlias = CorrelationIdentifier.of("q");
        final Quantifier.Existential renamed = quantifier.overNewReference(otherReference, newAlias);
        assertThat(renamed.getAlias()).isEqualTo(newAlias);
        assertThat(renamed.getRangesOver()).isSameAs(otherReference);
    }

    /**
     * Tests that existential quantifiers over the same reference are semantically equal, but not equal to a for-each
     * quantifier over the same reference or to an existential quantifier over a different reference.
     */
    @Test
    void testExistential3() {
        final Quantifier.Existential quantifier = Quantifier.existential(scanReference);
        final Quantifier.Existential sameQuantifier = Quantifier.existential(scanReference);
        assertThat(quantifier).isEqualTo(sameQuantifier);
        assertThat(quantifier.semanticHashCode()).isEqualTo(sameQuantifier.semanticHashCode());

        final Quantifier.ForEach forEachQuantifier = Quantifier.forEach(scanReference);
        assertThat(quantifier).isNotEqualTo(forEachQuantifier);
        assertThat(forEachQuantifier).isNotEqualTo(quantifier);

        final Reference otherReference = Reference.plannedOf(new RecordQueryScanPlan(ScanComparisons.EMPTY, true));
        assertThat(quantifier).isNotEqualTo(Quantifier.existential(otherReference));
    }

    /**
     * Tests that an existential quantifier is an {@link Quantifier.AtMostOne}, which flows the nullable variant of the
     * inner type, whereas a for-each quantifier is not an {@link Quantifier.AtMostOne}.
     */
    @Test
    void testExistential4() {
        final Quantifier.Existential quantifier = Quantifier.existential(scanReference);
        assertThat(quantifier).isInstanceOf(Quantifier.AtMostOne.class);
        assertThat(Quantifier.forEach(scanReference)).isNotInstanceOf(Quantifier.AtMostOne.class);

        assertThat(quantifier.getFlowedObjectType().isNullable()).isTrue();
        assertThat(quantifier.getFlowedObjectType())
                .isEqualTo(Quantifier.forEach(scanReference).getFlowedObjectType().nullable());
    }

    /**
     * Tests that {@code applyGlue()} wraps the given reference in a {@link RecordQueryFirstOrDefaultPlan} that ranges
     * over a {@link RecordQueryMaxOneRecordPlan} for a scalar quantifier whose sub-graph is not provably single-row.
     */
    @Test
    void testApplyGlue7() {
        final Quantifier.Scalar quantifier = Quantifier.scalar(scanReference);
        final Reference glued = quantifier.applyGlue(memoizer, scanReference);

        final RelationalExpression wrapper = Iterables.getOnlyElement(glued.getFinalExpressions());
        assertThat(wrapper).isInstanceOf(RecordQueryFirstOrDefaultPlan.class);
        assertThat(((RecordQueryFirstOrDefaultPlan)wrapper).getOnEmptyResultValue()).isInstanceOf(NullValue.class);

        // The surrounding query graph refers to the wrapper, which keeps the alias of the scalar quantifier.
        final Quantifier inner = Iterables.getOnlyElement(wrapper.getQuantifiers());
        assertThat(inner.getAlias()).isEqualTo(quantifier.getAlias());
        final RelationalExpression check = Iterables.getOnlyElement(inner.getRangesOver().getFinalExpressions());
        assertThat(check).isInstanceOf(RecordQueryMaxOneRecordPlan.class);

        // The check ranges over the scan via a physical quantifier with a fresh alias.
        final Quantifier checked = Iterables.getOnlyElement(check.getQuantifiers());
        assertThat(checked).isInstanceOf(Quantifier.Physical.class);
        assertThat(checked.getAlias()).isNotEqualTo(quantifier.getAlias());
        assertThat(checked.getRangesOver().getFinalExpressions()).containsExactly(scanPlan);
    }

    /**
     * Tests that {@code applyGlue()} does not add a {@link RecordQueryMaxOneRecordPlan} for a scalar quantifier whose
     * sub-graph is provably single-row, as that check could never fire.
     */
    @Test
    void testApplyGlue8() {
        final RecordQueryMaxOneRecordPlan singleRowPlan =
                new RecordQueryMaxOneRecordPlan(Quantifier.physical(scanReference));
        final Reference singleRowReference = Reference.plannedOf(singleRowPlan);
        final Quantifier.Scalar quantifier = Quantifier.scalar(singleRowReference);
        final Reference glued = quantifier.applyGlue(memoizer, singleRowReference);

        final RelationalExpression wrapper = Iterables.getOnlyElement(glued.getFinalExpressions());
        assertThat(wrapper).isInstanceOf(RecordQueryFirstOrDefaultPlan.class);
        final Quantifier inner = Iterables.getOnlyElement(wrapper.getQuantifiers());
        assertThat(inner.getAlias()).isEqualTo(quantifier.getAlias());
        assertThat(inner.getRangesOver().getFinalExpressions()).containsExactly(singleRowPlan);
    }

    /**
     * Tests that rebuilding a scalar quantifier via {@code toBuilder()} and {@code overNewReference()} keeps it a
     * scalar quantifier, and keeps its alias unless a new one is given.
     */
    @Test
    void testScalar1() {
        final Quantifier.Scalar quantifier = Quantifier.scalar(scanReference);
        final CorrelationIdentifier newAlias = CorrelationIdentifier.of("q");
        final Reference otherReference = Reference.plannedOf(new RecordQueryScanPlan(ScanComparisons.EMPTY, true));

        assertThat(quantifier.toBuilder().build(scanReference))
                .isInstanceOfSatisfying(Quantifier.Scalar.class,
                        rebuilt -> assertThat(rebuilt.getAlias()).isEqualTo(quantifier.getAlias()));
        assertThat(quantifier.toBuilder().withAlias(newAlias).build(scanReference))
                .isInstanceOfSatisfying(Quantifier.Scalar.class,
                        rebuilt -> assertThat(rebuilt.getAlias()).isEqualTo(newAlias));

        final Quantifier.Scalar moved = quantifier.overNewReference(otherReference);
        assertThat(moved.getAlias()).isEqualTo(quantifier.getAlias());
        assertThat(moved.getRangesOver()).isSameAs(otherReference);
        final Quantifier.Scalar renamed = quantifier.overNewReference(otherReference, newAlias);
        assertThat(renamed.getAlias()).isEqualTo(newAlias);
        assertThat(renamed.getRangesOver()).isSameAs(otherReference);
    }

    /**
     * Tests that only a scalar quantifier requires its sub-graph to produce at most one record, that both kinds of
     * quantifiers are {@link Quantifier.AtMostOne}, and that they have different shorthands.
     */
    @Test
    void testScalar2() {
        final Quantifier.Scalar scalar = Quantifier.scalar(scanReference);
        final Quantifier.Existential existential = Quantifier.existential(scanReference);

        assertThat(scalar).isInstanceOf(Quantifier.AtMostOne.class);
        assertThat(scalar.requiresMaxOneRecord()).isTrue();
        assertThat(existential.requiresMaxOneRecord()).isFalse();
        assertThat(scalar.getShorthand()).isNotEqualTo(existential.getShorthand());
        assertThat(scalar.getFlowedObjectType().isNullable()).isTrue();
    }

    /**
     * Tests that a scalar quantifier is semantically equal to a scalar quantifier over the same reference, but not
     * to an existential or a for-each quantifier over the same reference, and not to a scalar quantifier over a
     * different reference.
     */
    @Test
    void testScalar3() {
        final Quantifier.Scalar quantifier = Quantifier.scalar(scanReference);
        final Quantifier.Scalar sameQuantifier = Quantifier.scalar(scanReference);
        assertThat(quantifier).isEqualTo(sameQuantifier);
        assertThat(quantifier.semanticHashCode()).isEqualTo(sameQuantifier.semanticHashCode());

        assertThat(quantifier).isNotEqualTo(Quantifier.existential(scanReference));
        assertThat(Quantifier.existential(scanReference)).isNotEqualTo(quantifier);
        assertThat(quantifier).isNotEqualTo(Quantifier.forEach(scanReference));

        final Reference otherReference = Reference.plannedOf(new RecordQueryScanPlan(ScanComparisons.EMPTY, true));
        assertThat(quantifier).isNotEqualTo(Quantifier.scalar(otherReference));
    }
}
