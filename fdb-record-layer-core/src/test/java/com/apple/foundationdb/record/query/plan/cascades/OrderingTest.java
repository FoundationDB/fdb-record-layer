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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.OrderingPart.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class OrderingTest {
    @Test
    void testOrdering() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));

        final var requestedOrdering = new RequestedOrdering(ImmutableList.of(a, b, c), RequestedOrdering.Distinctness.NOT_DISTINCT);

        final var providedOrdering =
                new Ordering(
                        ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                        ImmutableList.of(a, c),
                        false);

        final var satisfyingOrderings = ImmutableList.copyOf(providedOrdering.enumerateSatisfyingOrderings(requestedOrdering));
        assertEquals(1, satisfyingOrderings.size());

        final var satisfyingOrdering = satisfyingOrderings.get(0);
        assertEquals(ImmutableList.of(a, b, c), satisfyingOrdering);
    }

    @Test
    void testOrdering2() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));

        final var requestedOrdering = new RequestedOrdering(ImmutableList.of(a, b, c), RequestedOrdering.Distinctness.NOT_DISTINCT);

        final var providedOrdering =
                new Ordering(
                        ImmutableSetMultimap.of(a.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                                b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                        ImmutableList.of(c),
                        false);

        final var satisfyingOrderings = ImmutableList.copyOf(providedOrdering.enumerateSatisfyingOrderings(requestedOrdering));
        assertEquals(1, satisfyingOrderings.size());

        final var satisfyingOrdering = satisfyingOrderings.get(0);
        assertEquals(ImmutableList.of(a, b, c), satisfyingOrdering);
    }

    @Test
    void testMergeKeys() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));

        final var leftPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(b, a));

        final var rightPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, a));

        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(leftPartialOrder, rightPartialOrder);

        assertEquals(
                // note there is no b -> c here
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, a)),
                mergedPartialOrder);
    }

    @Test
    void testMergeKeys2() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));

        final var leftPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var rightPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(leftPartialOrder, rightPartialOrder);

        assertEquals(
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, b)),
                mergedPartialOrder);
    }

    @Test
    void testMergeKeys3() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));

        final var leftPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var rightPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(a, b, b, c));

        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(leftPartialOrder, rightPartialOrder);

        assertEquals(PartiallyOrderedSet.empty(), mergedPartialOrder);
    }

    @Test
    void testPullUp1() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var innerOrder = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a));
        final var rcv2 = rcvWrapper("a", "b", "c");
        final var qov = QuantifiedObjectValue.of(Quantifier.current(), rcv2.getResultType());
        final var ap = of(ValueTestHelpers.field(qov, "ap"));
        final var bp = of(ValueTestHelpers.field(qov, "bp"));
        final var cp = of(ValueTestHelpers.field(qov, "cp"));
        final var ordering = new Ordering(ImmutableSetMultimap.of(), innerOrder, false);
        final var result = ordering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());
        assertEquals(
                PartiallyOrderedSet.of(ImmutableSet.of(ap, bp, cp), ImmutableSetMultimap.of(bp, ap)),
                result.getOrderingSet());
    }

    @Test
    void testPullUp2() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var d = of(ValueTestHelpers.field(rcv, "d"));
        final var innerOrder = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, d, c));
        final var rcv2 = rcvWrapper("a", "b", "c");
        final var qov = QuantifiedObjectValue.of(Quantifier.current(), rcv2.getResultType());
        final var ap = of(ValueTestHelpers.field(qov, "ap"));
        final var bp = of(ValueTestHelpers.field(qov, "bp"));
        final var cp = of(ValueTestHelpers.field(qov, "cp"));
        final var ordering = new Ordering(ImmutableSetMultimap.of(), innerOrder, false);
        final var result = ordering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());
        assertEquals(
                PartiallyOrderedSet.of(ImmutableSet.of(ap, bp, cp), ImmutableSetMultimap.of(bp, ap)),
                result.getOrderingSet());
    }

    @Test
    void testPullUp3() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c")); // a <- b <- c
        final var innerOrder = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, b));
        final var rcv2 = rcvWrapper("b", "c");
        final var ordering = new Ordering(ImmutableSetMultimap.of(), innerOrder, false);
        final var result = ordering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());
        assertEquals(
                PartiallyOrderedSet.of(ImmutableSet.of(), ImmutableSetMultimap.of()),
                result.getOrderingSet());
    }

    @Test
    void testPullUp4() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var d = of(ValueTestHelpers.field(rcv, "d"));
        // a <- b <- c
        //   <- d
        final var innerOrder = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d), ImmutableSetMultimap.of(b, a, c, b, d, a));
        final var rcv2 = rcvWrapper("a", "d");
        final var qov = QuantifiedObjectValue.of(Quantifier.current(), rcv2.getResultType());
        final var ap = of(ValueTestHelpers.field(qov, "ap"));
        final var dp = of(ValueTestHelpers.field(qov, "dp"));
        final var ordering = new Ordering(ImmutableSetMultimap.of(), innerOrder, false);
        final var result = ordering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());
        assertEquals(
                PartiallyOrderedSet.of(ImmutableSet.of(ap, dp), ImmutableSetMultimap.of(dp, ap)),
                result.getOrderingSet());
    }

    @Test
    void testMergePartialOrdersNAry() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var d = of(ValueTestHelpers.field(rcv, "d"));
        final var e = of(ValueTestHelpers.field(rcv, "e"));

        final var one =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var two =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d),
                        ImmutableSetMultimap.of(c, b, b, a));

        final var three =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d),
                        ImmutableSetMultimap.of(c, a, b, a));

        final var four =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d, e),
                        ImmutableSetMultimap.of(c, a, b, a));


        final var mergedPartialOrder = Ordering.mergePartialOrderOfOrderings(ImmutableList.of(one, two, three, four));

        assertEquals(
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d), ImmutableSetMultimap.of(b, a, c, b)),
                mergedPartialOrder);
    }

    @Test
    void testCommonOrdering() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var d = of(ValueTestHelpers.field(rcv, "d"));
        final var e = of(ValueTestHelpers.field(rcv, "e"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(d.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(d.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var three = new Ordering(
                ImmutableSetMultimap.of(d.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var four = new Ordering(
                ImmutableSetMultimap.of(
                        d.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        e.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var mergedOrdering =
                Ordering.mergeOrderings(ImmutableList.of(one, two, three, four), Ordering::intersectEqualityBoundKeys, false);

        final var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, c),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var satisfyingOrderingsIterable = mergedOrdering.enumerateSatisfyingOrderings(requestedOrdering);
        final var onlySatisfyingOrdering =
                Iterables.getOnlyElement(satisfyingOrderingsIterable);
        assertEquals(ImmutableList.of(a, b, c, d), onlySatisfyingOrdering);
    }

    @Test
    void testCommonOrdering2() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var x = of(ValueTestHelpers.field(rcv, "x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        final var mergedOrdering =
                Ordering.mergeOrderings(ImmutableList.of(one, two), Ordering::intersectEqualityBoundKeys, false);

        var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, c, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var satisfyingOrderingsIterable = mergedOrdering.enumerateSatisfyingOrderings(requestedOrdering);
        final var onlySatisfyingOrdering =
                Iterables.getOnlyElement(satisfyingOrderingsIterable);

        assertEquals(ImmutableList.of(a, b, c, x), onlySatisfyingOrdering);
    }

    @Test
    void testCommonOrdering3() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var x = of(ValueTestHelpers.field(rcv, "x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        final var mergedOrdering =
                Ordering.mergeOrderings(ImmutableList.of(one, two), Ordering::intersectEqualityBoundKeys, false);

        final var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, c, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var satisfyingOrderingsIterable = mergedOrdering.enumerateSatisfyingOrderings(requestedOrdering);
        final var onlySatisfyingOrdering =
                Iterables.getOnlyElement(satisfyingOrderingsIterable);

        assertEquals(ImmutableList.of(a, c, b, x), onlySatisfyingOrdering);
    }

    @Test
    void testCommonOrdering4() {
        final var rcv = ValueTestHelpers.rcv();
        final var a = of(ValueTestHelpers.field(rcv, "a"));
        final var b = of(ValueTestHelpers.field(rcv, "b"));
        final var c = of(ValueTestHelpers.field(rcv, "c"));
        final var x = of(ValueTestHelpers.field(rcv, "x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        final var mergedOrdering =
                Ordering.mergeOrderings(ImmutableList.of(one, two), Ordering::intersectEqualityBoundKeys, false);

        var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        assertFalse(mergedOrdering.satisfies(requestedOrdering));
    }

    @Nonnull
    private static RecordConstructorValue rcvWrapper(@Nonnull final String... projection) {

        final var rcv = ValueTestHelpers.rcv();
        final List<Column<? extends Value>> columns = Arrays.stream(projection)
                .map(field -> FieldValue.ofFieldName(rcv, field))
                .map(field -> Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of(field.getLastFieldName().orElseThrow() + "p")), field))
                .collect(Collectors.toList());
        // create a bunch of aliases
        return RecordConstructorValue.ofColumns(columns);
    }
}
