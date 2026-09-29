/*
 * QuantifiersTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link Quantifiers}.
 */
class QuantifiersTest {
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
        assertThat(Quantifiers.applyGlue(memoizer, Quantifier.forEach(scanReference), scanReference))
                .isSameAs(scanReference);
    }

    /**
     * Tests that {@code applyGlue()} wraps the given reference in a {@link RecordQueryDefaultOnEmptyPlan} with a
     * {@code NULL} default for a null-on-empty for-each quantifier.
     */
    @Test
    void testApplyGlue2() {
        final Quantifier.ForEach quantifier = Quantifier.forEachWithNullOnEmpty(scanReference);
        final Reference glued = Quantifiers.applyGlue(memoizer, quantifier, scanReference);

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
        final Reference glued = Quantifiers.applyGlue(memoizer, quantifier, scanReference);

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
        assertThat(Quantifiers.applyGlue(memoizer, Quantifier.forEach(scanReference), builder)).isSameAs(builder);

        final Quantifier.ForEach quantifier = Quantifier.forEachWithNullOnEmpty(scanReference);
        final Memoizer.ReferenceOfPlansBuilder glued = Quantifiers.applyGlue(memoizer, quantifier, builder);
        final RelationalExpression wrapper = Iterables.getOnlyElement(glued.members());
        assertThat(wrapper).isInstanceOf(RecordQueryDefaultOnEmptyPlan.class);
        assertWrapsScanPlan(wrapper, quantifier);
    }
}
