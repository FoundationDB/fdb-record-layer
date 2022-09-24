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

import com.apple.foundationdb.record.query.combinatorics.PartialOrder;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class OrderingTest {
    @Test
    void testOrdering() {
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));

        final var requestedOrdering = new RequestedOrdering(ImmutableList.of(a, b, c), RequestedOrdering.Distinctness.NOT_DISTINCT);

        final var providedOrdering =
                new Ordering(
                        ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                        ImmutableList.of(a, c),
                        false);

        final var satisfyingOrderings = ImmutableList.copyOf(providedOrdering.satisfiesRequestedOrdering(requestedOrdering));
        assertEquals(1, satisfyingOrderings.size());

        final var satisfyingOrdering = satisfyingOrderings.get(0);
        assertEquals(ImmutableList.of(a, b, c), satisfyingOrdering);
    }

    @Test
    void testOrdering2() {
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));

        final var requestedOrdering = new RequestedOrdering(ImmutableList.of(a, b, c), RequestedOrdering.Distinctness.NOT_DISTINCT);

        final var providedOrdering =
                new Ordering(
                        ImmutableSetMultimap.of(a.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL),
                                b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                        ImmutableList.of(c),
                        false);

        final var satisfyingOrderings = ImmutableList.copyOf(providedOrdering.satisfiesRequestedOrdering(requestedOrdering));
        assertEquals(1, satisfyingOrderings.size());

        final var satisfyingOrdering = satisfyingOrderings.get(0);
        assertEquals(ImmutableList.of(a, b, c), satisfyingOrdering);
    }

    @Test
    void testMergeKeys() {
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));

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
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));

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
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));

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
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));
        final var d = KeyPart.of(field("d"));
        final var e = KeyPart.of(field("e"));

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
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));
        final var d = KeyPart.of(field("d"));
        final var e = KeyPart.of(field("e"));

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

        final var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, c),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(one, two, three, four),
                        requestedOrdering);

        commonOrderingKeysOptional
                .ifPresentOrElse(commonOrderingKeys -> assertEquals(ImmutableList.of(a, b, c, d), commonOrderingKeys),
                        Assertions::fail);
    }

    @Test
    void testCommonOrdering2() {
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));
        final var x = KeyPart.of(field("x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, c, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(one, two),
                        requestedOrdering);

        commonOrderingKeysOptional
                .ifPresentOrElse(commonOrderingKeys -> assertEquals(ImmutableList.of(a, b, c, x), commonOrderingKeys),
                        Assertions::fail);
    }

    @Test
    void testCommonOrdering3() {
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));
        final var x = KeyPart.of(field("x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        final var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, c, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(one, two),
                        requestedOrdering);

        commonOrderingKeysOptional
                .ifPresentOrElse(commonOrderingKeys -> assertEquals(ImmutableList.of(a, c, b, x), commonOrderingKeys),
                        Assertions::fail);
    }

    @Test
    void testCommonOrdering4() {
        final var a = KeyPart.of(field("a"));
        final var b = KeyPart.of(field("b"));
        final var c = KeyPart.of(field("c"));
        final var x = KeyPart.of(field("x"));

        final var one = new Ordering(
                ImmutableSetMultimap.of(c.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, b, x),
                false);

        final var two = new Ordering(
                ImmutableSetMultimap.of(b.getValue(), new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                ImmutableList.of(a, c, x),
                false);

        var requestedOrdering = new RequestedOrdering(
                ImmutableList.of(a, b, x),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);

        final var commonOrderingKeysOptional =
                Ordering.commonOrderingKeys(
                        ImmutableList.of(one, two),
                        requestedOrdering);

        assertFalse(commonOrderingKeysOptional.isPresent());
    }

    @Nonnull
    private static Value field(@Nonnull final String fieldName) {
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of(fieldName)),
                                LiteralValue.ofScalar("fieldValue")));
        return FieldValue.ofFieldName(RecordConstructorValue.ofColumns(columns), fieldName);
    }
}
