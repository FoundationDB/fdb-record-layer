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
import com.apple.foundationdb.record.query.plan.cascades.Ordering.Binding;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class OrderingTest {
    @Test
    void testOrdering() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        final var requestedOrdering = new RequestedOrdering(requested(a, b, c), RequestedOrdering.Distinctness.NOT_DISTINCT);

        final var providedOrdering =
                Ordering.ofOrderingSequence(
                        bindingMap(a, ProvidedSortOrder.ASCENDING,
                                b, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                                c, ProvidedSortOrder.ASCENDING),
                        ImmutableList.of(a, c),
                        false);

        final var satisfyingOrderings =
                ImmutableList.copyOf(providedOrdering.enumerateCompatibleRequestedOrderings(requestedOrdering));
        assertEquals(1, satisfyingOrderings.size());

        final var satisfyingOrdering = satisfyingOrderings.get(0);
        assertEquals(requested(a, b, RequestedSortOrder.ANY, c), satisfyingOrdering);
    }

    @Test
    void testOrdering2() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        final var requestedOrdering = new RequestedOrdering(requested(a, b, c), RequestedOrdering.Distinctness.NOT_DISTINCT);

        final var providedOrdering =
                Ordering.ofOrderingSequence(
                        bindingMap(a, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                                b, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                                c, ProvidedSortOrder.ASCENDING),
                        ImmutableList.of(c),
                        false);

        final var satisfyingOrderings = ImmutableList.copyOf(providedOrdering.enumerateCompatibleRequestedOrderings(requestedOrdering));
        assertEquals(1, satisfyingOrderings.size());

        final var satisfyingOrdering = satisfyingOrderings.get(0);
        assertEquals(requested(a, RequestedSortOrder.ANY, b, RequestedSortOrder.ANY, c), satisfyingOrdering);
    }

    @Test
    void testMergeKeys() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        final var bindingMap =
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING);

        final var leftPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(b, a));
        final var leftOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        leftPartialOrder, false);

        final var rightPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, a));
        final var rightOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        rightPartialOrder, false);

        final var mergedOrdering =
                Ordering.merge(leftOrdering, rightOrdering, Ordering.UNION, false);

        final var expectedOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        // note there is no b -> c here
                        PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, a)),
                        false);

        assertEquals(expectedOrdering, mergedOrdering);
    }

    @Test
    void testMergeKeys2() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        final var bindingMap =
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING);

        final var leftPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));
        final var leftOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        leftPartialOrder, false);

        final var rightPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));
        final var rightOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        rightPartialOrder, false);

        final var mergedOrdering =
                Ordering.merge(leftOrdering, rightOrdering, Ordering.UNION, false);

        final var expectedOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, b)),
                        false);

        assertEquals(expectedOrdering, mergedOrdering);
    }

    @Test
    void testMergeKeys3() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");

        final var bindingMap =
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING);

        final var leftPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(c, b, b, a));
        final var leftOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        leftPartialOrder, false);

        final var rightPartialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        ImmutableSetMultimap.of(a, b, b, c));
        final var rightOrdering =
                Ordering.ofOrderingSet(bindingMap,
                        rightPartialOrder, false);

        final var mergedOrdering =
                Ordering.merge(leftOrdering, rightOrdering, Ordering.UNION, false);

        assertEquals(Ordering.empty(), mergedOrdering);
    }

    @Test
    void testPullUp1() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var innerOrderedSet = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a));
        final var innerOrdering =
                Ordering.ofOrderingSet(bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING), innerOrderedSet, false);

        final var rcv2 = select("a", "b", "c");
        final var pulledUpOrdering = innerOrdering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());

        final var qovCurrent = QuantifiedObjectValue.of(Quantifier.current(), rcv2.getResultType());
        final var ap = ValueTestHelpers.field(qovCurrent, "ap");
        final var bp = ValueTestHelpers.field(qovCurrent, "bp");
        final var cp = ValueTestHelpers.field(qovCurrent, "cp");

        final var expectedOrdering =
                Ordering.ofOrderingSet(bindingMap(ap, ProvidedSortOrder.ASCENDING,
                                bp, ProvidedSortOrder.ASCENDING,
                                cp, ProvidedSortOrder.ASCENDING),
                        PartiallyOrderedSet.of(ImmutableSet.of(ap, bp, cp), ImmutableSetMultimap.of(bp, ap)),
                        false);
        assertEquals(
                expectedOrdering,
                pulledUpOrdering);
    }

    @Test
    void testPullUp2() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var d = ValueTestHelpers.field(qov, "d");
        final var innerOrderedSet = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, d, c));
        final var innerOrdering =
                Ordering.ofOrderingSet(bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, ProvidedSortOrder.ASCENDING), innerOrderedSet, false);

        final var rcv2 = select("a", "b", "c");
        final var pulledUpOrdering = innerOrdering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());

        final var qovCurrent = QuantifiedObjectValue.of(Quantifier.current(), rcv2.getResultType());
        final var ap = ValueTestHelpers.field(qovCurrent, "ap");
        final var bp = ValueTestHelpers.field(qovCurrent, "bp");
        final var cp = ValueTestHelpers.field(qovCurrent, "cp");

        final var expectedOrdering =
                Ordering.ofOrderingSet(bindingMap(ap, ProvidedSortOrder.ASCENDING,
                                bp, ProvidedSortOrder.ASCENDING,
                                cp, ProvidedSortOrder.ASCENDING),
                        PartiallyOrderedSet.of(ImmutableSet.of(ap, bp, cp), ImmutableSetMultimap.of(bp, ap)),
                        false);

        assertEquals(expectedOrdering, pulledUpOrdering);
    }

    @Test
    void testPullUp3() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c"); // a <- b <- c
        final var innerOrderedSet = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c), ImmutableSetMultimap.of(b, a, c, b));
        final var innerOrdering =
                Ordering.ofOrderingSet(bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING), innerOrderedSet, false);

        final var rcv2 = select("b", "c");
        final var pulledUpOrdering = innerOrdering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());

        assertEquals(Ordering.empty(), pulledUpOrdering);
    }

    @Test
    void testPullUp4() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var d = ValueTestHelpers.field(qov, "d");
        // a <- b <- c
        //   <- d
        final var innerOrderedSet =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d), ImmutableSetMultimap.of(b, a, c, b, d, a));
        final var innerOrdering =
                Ordering.ofOrderingSet(bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, ProvidedSortOrder.ASCENDING), innerOrderedSet, false);

        final var rcv2 = select("a", "d");
        final var pulledUpOrdering = innerOrdering.pullUp(rcv2, AliasMap.emptyMap(), Set.of());

        final var qovCurrent = QuantifiedObjectValue.of(Quantifier.current(), rcv2.getResultType());
        final var ap = ValueTestHelpers.field(qovCurrent, "ap");
        final var dp = ValueTestHelpers.field(qovCurrent, "dp");
        final var expectedOrdering =
                Ordering.ofOrderingSet(bindingMap(ap, ProvidedSortOrder.ASCENDING,
                                dp, ProvidedSortOrder.ASCENDING),
                        PartiallyOrderedSet.of(ImmutableSet.of(ap, dp), ImmutableSetMultimap.of(dp, ap)),
                        false);

        assertEquals(expectedOrdering, pulledUpOrdering);
    }

    @Test
    void testMergePartialOrdersNAry() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var d = ValueTestHelpers.field(qov, "d");
        final var e = ValueTestHelpers.field(qov, "e");

        final var abcdBindingMap =
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, ProvidedSortOrder.ASCENDING);
        final var one =
                Ordering.ofOrderingSet(abcdBindingMap,
                        PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d),
                                ImmutableSetMultimap.of(c, b, b, a)), false);

        final var two =
                Ordering.ofOrderingSet(abcdBindingMap,
                        PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d),
                                ImmutableSetMultimap.of(c, b, b, a)), false);

        final var three =
                Ordering.ofOrderingSet(abcdBindingMap,
                        PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d),
                                ImmutableSetMultimap.of(c, a, b, a)), false);

        final var abcdeBindingMap =
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, ProvidedSortOrder.ASCENDING,
                        e, ProvidedSortOrder.ASCENDING);
        final var four =
                Ordering.ofOrderingSet(abcdeBindingMap,
                        PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d, e),
                                ImmutableSetMultimap.of(c, a, b, a)), false);

        final var mergedOrdering =
                Ordering.merge(ImmutableList.of(one, two, three, four),
                        Ordering.UNION, (left, right) -> false);

        final var expectedOrdering =
                Ordering.ofOrderingSet(abcdBindingMap,
                        PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d), ImmutableSetMultimap.of(b, a, c, b)), false);

        assertEquals(expectedOrdering, mergedOrdering);
    }

    @Test
    void testCommonOrdering() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var d = ValueTestHelpers.field(qov, "d");
        final var e = ValueTestHelpers.field(qov, "e");

        final var one = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var two = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var three = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var four = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, ProvidedSortOrder.ASCENDING,
                        d, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        e, new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, c),
                false);

        final var mergedOrdering =
                Ordering.merge(ImmutableList.of(one, two, three, four), Ordering.UNION,
                        (left, right) -> false);

        final var requestedOrdering = new RequestedOrdering(
                requested(a, b, c),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var satisfyingOrderingsIterable = mergedOrdering.enumerateCompatibleRequestedOrderings(requestedOrdering);
        final var onlySatisfyingOrdering =
                Iterables.getOnlyElement(satisfyingOrderingsIterable);
        assertEquals(requested(a, b, c, d, RequestedSortOrder.ANY), onlySatisfyingOrdering);
    }

    @Test
    void testCommonOrdering2() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var x = ValueTestHelpers.field(qov, "x");

        final var one = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        x, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, b, x),
                false);

        final var two = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        c, ProvidedSortOrder.ASCENDING,
                        x, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, c, x),
                false);


        final var mergedOrdering =
                Ordering.merge(ImmutableList.of(one, two), Ordering.UNION, (left, right) -> false);

        var requestedOrdering = new RequestedOrdering(
                requested(a, b, c, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var satisfyingOrderingsIterable = mergedOrdering.enumerateCompatibleRequestedOrderings(requestedOrdering);
        final var onlySatisfyingOrdering =
                Iterables.getOnlyElement(satisfyingOrderingsIterable);

        assertEquals(requested(a, b, c, x), onlySatisfyingOrdering);
    }

    @Test
    void testCommonOrdering3() {
        final var qov = ValueTestHelpers.rcv();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var x = ValueTestHelpers.field(qov, "x");

        final var one = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        x, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, b, x),
                false);

        final var two = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        c, ProvidedSortOrder.ASCENDING,
                        x, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, c, x),
                false);

        final var mergedOrdering =
                Ordering.merge(ImmutableList.of(one, two), Ordering.UNION, (left, right) -> false);

        final var requestedOrdering = new RequestedOrdering(
                requested(a, c, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var satisfyingOrderingsIterable = mergedOrdering.enumerateCompatibleRequestedOrderings(requestedOrdering);
        final var onlySatisfyingOrdering =
                Iterables.getOnlyElement(satisfyingOrderingsIterable);

        assertEquals(requested(a, c, b, x), onlySatisfyingOrdering);
    }

    @Test
    void testCommonOrdering4() {
        final var qov = ValueTestHelpers.qov();
        final var a = ValueTestHelpers.field(qov, "a");
        final var b = ValueTestHelpers.field(qov, "b");
        final var c = ValueTestHelpers.field(qov, "c");
        final var x = ValueTestHelpers.field(qov, "x");

        final var one = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, ProvidedSortOrder.ASCENDING,
                        c, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        x, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, b, x),
                false);

        final var two = Ordering.ofOrderingSequence(
                bindingMap(a, ProvidedSortOrder.ASCENDING,
                        b, new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                        c, ProvidedSortOrder.ASCENDING,
                        x, ProvidedSortOrder.ASCENDING),
                ImmutableList.of(a, c, x),
                false);

        final var mergedOrdering =
                Ordering.merge(ImmutableList.of(one, two), Ordering.UNION, (left, right) -> false);

        var requestedOrdering = new RequestedOrdering(
                requested(a, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        assertFalse(mergedOrdering.satisfies(requestedOrdering));
    }

    @Nonnull
    private static RecordConstructorValue select(@Nonnull final String... projection) {

        final var rcv = ValueTestHelpers.qov();
        final List<Column<? extends Value>> columns = Arrays.stream(projection)
                .map(field -> FieldValue.ofFieldName(rcv, field))
                .map(field -> Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of(field.getLastFieldName().orElseThrow() + "p")), field))
                .collect(Collectors.toList());
        // create a bunch of aliases
        return RecordConstructorValue.ofColumns(columns);
    }

    @Nonnull
    private static SetMultimap<Value, Binding> bindingMap(@Nonnull final Object... valueObjectPairs) {
        final var resultBindingMap = ImmutableSetMultimap.<Value, Binding>builder();
        int i;
        for (i = 0; i < valueObjectPairs.length;) {
            if (valueObjectPairs[i] instanceof Value) {
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
            } else {
                final var orderingPart = (OrderingPart<?>)valueObjectPairs[i];
                if (i + 1 < valueObjectPairs.length && valueObjectPairs[i + 1] instanceof Comparisons.Comparison) {
                    resultBindingMap.put(orderingPart.getValue(), Binding.fixed((Comparisons.Comparison)valueObjectPairs[i + 1]));
                    i += 2;
                } else {
                    resultBindingMap.put(orderingPart.getValue(), Binding.sorted((ProvidedSortOrder)orderingPart.getSortOrder()));
                    i += 1;
                }
            }
        }
        Verify.verify(i == valueObjectPairs.length);
        return resultBindingMap.build();
    }

    @Nonnull
    private static List<RequestedOrderingPart> requested(@Nonnull final Object... objects) {
        final var resultRequestedOrderingParts = ImmutableList.<RequestedOrderingPart>builder();
        int i;
        for (i = 0; i < objects.length;) {
            final var value = (Value)objects[i];
            final RequestedSortOrder requestedSortOrder;
            if (i + 1 < objects.length && objects[i + 1] instanceof RequestedSortOrder) {
                requestedSortOrder = (RequestedSortOrder)objects[i + 1];
                i += 2;
            } else {
                requestedSortOrder = RequestedSortOrder.ASCENDING;
                i += 1;
            }
            resultRequestedOrderingParts.add(new RequestedOrderingPart(value, requestedSortOrder));
        }
        Verify.verify(i == objects.length);

        return resultRequestedOrderingParts.build();
    }
}
