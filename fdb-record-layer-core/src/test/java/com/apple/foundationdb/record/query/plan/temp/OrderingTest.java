/*
 * OrderingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.combinatorics.PartialOrder;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderingTest {
    @Test
    void testOrdering() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));

        final var requestedOrdering = ImmutableList.of(a, b, c);

        final var providedOrdering =
                new Ordering(
                        ImmutableSetMultimap.of(b.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                        ImmutableList.of(a, c),
                        false);

        final var satisfyingOrderingOptional = providedOrdering.satisfiesRequestedOrdering(requestedOrdering, ImmutableSet.of());
        assertTrue(satisfyingOrderingOptional.isPresent());

        satisfyingOrderingOptional
                .ifPresentOrElse(satisfyingOrdering -> assertEquals(ImmutableList.of(a, b, c), satisfyingOrdering),
                        Assertions::fail);
    }

    @Test
    void testOrdering2() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));

        final var requestedOrdering = ImmutableList.of(a, b, c);

        final var providedOrdering =
                new Ordering(
                        ImmutableSetMultimap.of(a.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                                b.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                        ImmutableList.of(c),
                        false);

        final var satisfyingOrderingOptional = providedOrdering.satisfiesRequestedOrdering(requestedOrdering, ImmutableSet.of());

        satisfyingOrderingOptional
                .ifPresentOrElse(satisfyingOrdering -> assertEquals(ImmutableList.of(a, b, c), satisfyingOrdering),
                        Assertions::fail);
    }

    @Test
    void testMergeKeys() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));

        final var leftPartialOrder =
                PartialOrder.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(b, a));

        final var rightPartialOrder =
                PartialOrder.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, a));

        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(leftPartialOrder, rightPartialOrder);

        assertEquals(
                // note there is no b -> c here
                PartialOrder.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, a)),
                mergedPartialOrder);
    }

    @Test
    void testMergeKeys2() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));

        final var leftPartialOrder =
                PartialOrder.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var rightPartialOrder =
                PartialOrder.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(leftPartialOrder, rightPartialOrder);

        assertEquals(
                PartialOrder.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, b)),
                mergedPartialOrder);
    }

    @Test
    void testMergeKeys3() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));

        final var leftPartialOrder =
                PartialOrder.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var rightPartialOrder =
                PartialOrder.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(a, b, b, c));

        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(leftPartialOrder, rightPartialOrder);

        assertEquals(PartialOrder.empty(), mergedPartialOrder);
    }

    @Test
    void testMergePartialOrdersNAry() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));
        final var d = KeyPart.of(Key.Expressions.field("d"));
        final var e = KeyPart.of(Key.Expressions.field("e"));

        final var one =
                PartialOrder.of(ImmutableSet.of(a, b, c, d),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var two =
                PartialOrder.of(ImmutableSet.of(a, b, c, d),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var three =
                PartialOrder.of(ImmutableSet.of(a, b, c, d),
                        ImmutableSetMultimap.of(c, a, b, a));

        final var four =
                PartialOrder.of(ImmutableSet.of(a, b, c, d, e),
                        ImmutableSetMultimap.of(c, a, b, a));


        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(ImmutableList.of(one, two, three, four));

        assertEquals(
                PartialOrder.of(ImmutableSet.of(a, b, c, d), ImmutableSetMultimap.of(b, a, c, b)),
                mergedPartialOrder);
    }

    @Test
    void testCommonOrdering() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));
        final var d = KeyPart.of(Key.Expressions.field("d"));
        final var e = KeyPart.of(Key.Expressions.field("e"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(d.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(d.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var three = new Ordering(
                ImmutableSetMultimap.of(d.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var four = new Ordering(
                ImmutableSetMultimap.of(
                        d.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        e.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, c),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(
                                Optional.of(one),
                                Optional.of(two),
                                Optional.of(three),
                                Optional.of(four)),
                        requestedOrdering);

        commonOrderingKeysOptional
                .ifPresentOrElse(commonOrderingKeys -> assertEquals(ImmutableList.of(a, b, c, d), commonOrderingKeys),
                        Assertions::fail);
    }

    @Test
    void testCommonOrdering2() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));
        final var x = KeyPart.of(Key.Expressions.field("x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, c, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(
                                Optional.of(one),
                                Optional.of(two)),
                        requestedOrdering);

        commonOrderingKeysOptional
                .ifPresentOrElse(commonOrderingKeys -> assertEquals(ImmutableList.of(a, b, c, x), commonOrderingKeys),
                        Assertions::fail);
    }

    @Test
    void testCommonOrdering3() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));
        final var x = KeyPart.of(Key.Expressions.field("x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        final var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, c, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(
                                Optional.of(one),
                                Optional.of(two)),
                        requestedOrdering);

        commonOrderingKeysOptional
                .ifPresentOrElse(commonOrderingKeys -> assertEquals(ImmutableList.of(a, c, b, x), commonOrderingKeys),
                        Assertions::fail);
    }

    @Test
    void testCommonOrdering4() {
        final var a = KeyPart.of(Key.Expressions.field("a"));
        final var b = KeyPart.of(Key.Expressions.field("b"));
        final var c = KeyPart.of(Key.Expressions.field("c"));
        final var x = KeyPart.of(Key.Expressions.field("x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getNormalizedKeyExpression(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(
                                Optional.of(one),
                                Optional.of(two)),
                        requestedOrdering);

        assertFalse(commonOrderingKeysOptional.isPresent());
    }
}
