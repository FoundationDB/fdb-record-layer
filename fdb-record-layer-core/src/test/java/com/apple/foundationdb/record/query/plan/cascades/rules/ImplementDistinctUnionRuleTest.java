/*
 * ImplementDistinctUnionRuleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ImplementDistinctUnionRule#removeCommonEqualityBoundParts}.
 */
class ImplementDistinctUnionRuleTest {

    @Nonnull
    private static SetMultimap<Value, Binding> bindingMap(@Nonnull final Object... valueObjectPairs) {
        final var resultBindingMap = ImmutableSetMultimap.<Value, Binding>builder();
        int i;
        for (i = 0; i < valueObjectPairs.length;) {
            final var value = (Value)valueObjectPairs[i];
            if (valueObjectPairs[i + 1] instanceof Comparisons.Comparison) {
                resultBindingMap.put(value, Binding.fixed((Comparisons.Comparison)valueObjectPairs[i + 1]));
                i += 2;
            } else if (valueObjectPairs[i + 1] instanceof ProvidedSortOrder) {
                resultBindingMap.put(value, Binding.sorted((ProvidedSortOrder)valueObjectPairs[i + 1]));
                i += 2;
            } else {
                throw new IllegalArgumentException("unknown binding object");
            }
        }
        Verify.verify(i == valueObjectPairs.length);
        return resultBindingMap.build();
    }

    @Nonnull
    private static Comparisons.Comparison eq(int value) {
        return new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, value);
    }

    @Test
    void emptyList() {
        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(ImmutableList.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void singleOrdering() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");

        // Single ordering with a=5 fixed and b ascending
        final var ordering = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(b),
                false);

        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(ImmutableList.of(ordering));

        // Single ordering: a=5 is common (trivially), so it gets removed
        assertEquals(1, result.size());
        assertTrue(result.get(0).getFixedBindingMap().isEmpty());
        assertTrue(result.get(0).getOrderingSet().getSet().contains(b));
    }

    @Test
    void twoOrderingsSameFixedBinding() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        // Both orderings have a=5 fixed
        final var ordering1 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(b),
                false);
        final var ordering2 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), c, ProvidedSortOrder.DESCENDING),
                ImmutableList.of(c),
                false);

        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(ImmutableList.of(ordering1, ordering2));

        // a=5 is common, should be removed from both
        assertEquals(2, result.size());
        assertTrue(result.get(0).getFixedBindingMap().isEmpty());
        assertTrue(result.get(1).getFixedBindingMap().isEmpty());
        // directional parts should remain
        assertTrue(result.get(0).getBindingMap().containsKey(b));
        assertTrue(result.get(1).getBindingMap().containsKey(c));
    }

    @Test
    void twoOrderingsDifferentFixedBinding() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");

        // Same value (a) but different fixed bindings: a=5 vs a=6
        final var ordering1 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(b),
                false);
        final var ordering2 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(6), b, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(b),
                false);

        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(ImmutableList.of(ordering1, ordering2));

        // a=5 != a=6, nothing should be removed
        assertEquals(2, result.size());
        assertTrue(result.get(0).getFixedBindingMap().containsKey(a));
        assertTrue(result.get(1).getFixedBindingMap().containsKey(a));
    }

    @Test
    void twoOrderingsNoCommonFixed() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");

        // No fixed values at all, just directional
        final var ordering1 = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING, b, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, b),
                false);
        final var ordering2 = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING, b, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, b),
                false);

        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(ImmutableList.of(ordering1, ordering2));

        // Nothing fixed, nothing removed
        assertEquals(2, result.size());
        assertTrue(result.get(0).getFixedBindingMap().isEmpty());
        assertTrue(result.get(1).getFixedBindingMap().isEmpty());
        assertEquals(2, result.get(0).getOrderingSet().getSet().size());
    }

    @Test
    void multipleCommonFixedParts() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        // Both orderings have a=5 and b=10 fixed, c is directional
        final var ordering1 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, eq(10), c, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(c),
                false);
        final var ordering2 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, eq(10), c, ProvidedSortOrder.DESCENDING),
                ImmutableList.of(c),
                false);

        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(ImmutableList.of(ordering1, ordering2));

        // a=5 and b=10 are common, both removed
        assertEquals(2, result.size());
        assertTrue(result.get(0).getFixedBindingMap().isEmpty());
        assertTrue(result.get(1).getFixedBindingMap().isEmpty());
        assertTrue(result.get(0).getBindingMap().containsKey(c));
        assertTrue(result.get(1).getBindingMap().containsKey(c));
    }

    @Test
    void partialOverlap() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        // a=5 common in both, b is fixed in ordering1 but directional in ordering2
        final var ordering1 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, eq(10), c, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(c),
                false);
        final var ordering2 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, ProvidedSortOrder.ASCENDING, c, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(b, c),
                false);

        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(ImmutableList.of(ordering1, ordering2));

        // Only a=5 is common fixed, should be removed
        assertEquals(2, result.size());
        // ordering1: a removed, b=10 stays
        assertTrue(result.get(0).getFixedBindingMap().containsKey(b));
        assertFalse(result.get(0).getBindingMap().containsKey(a));
        // ordering2: a removed, b directional stays
        assertFalse(result.get(1).getFixedBindingMap().containsKey(b));
        assertTrue(result.get(1).getBindingMap().containsKey(b));
        assertFalse(result.get(1).getBindingMap().containsKey(a));
    }

    @Test
    void threeOrderingsCommonIntersection() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        // a=5 common across all three, b=10 only in first two
        final var ordering1 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, eq(10), c, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(c),
                false);
        final var ordering2 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, eq(10), c, ProvidedSortOrder.DESCENDING),
                ImmutableList.of(c),
                false);
        final var ordering3 = Ordering.ofOrderingSequence(
                bindingMap(a, eq(5), b, ProvidedSortOrder.ASCENDING, c, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(b, c),
                false);

        final var result = ImplementDistinctUnionRule.removeCommonEqualityBoundParts(
                ImmutableList.of(ordering1, ordering2, ordering3));

        // Only a=5 is common across all three
        assertEquals(3, result.size());
        for (final var ordering : result) {
            assertFalse(ordering.getBindingMap().containsKey(a));
        }
        // b=10 should still be present in ordering1 and ordering2
        assertTrue(result.get(0).getFixedBindingMap().containsKey(b));
        assertTrue(result.get(1).getFixedBindingMap().containsKey(b));
        // b should be directional in ordering3
        assertFalse(result.get(2).getFixedBindingMap().containsKey(b));
        assertTrue(result.get(2).getBindingMap().containsKey(b));
    }
}
