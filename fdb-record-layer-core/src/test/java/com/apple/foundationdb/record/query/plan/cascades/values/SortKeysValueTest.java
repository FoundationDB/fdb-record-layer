/*
 * SortKeysValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SortKeysValue}.
 */
class SortKeysValueTest {
    @Nonnull
    private static final Type LONG_TYPE = Type.primitiveType(Type.TypeCode.LONG, false);

    /**
     * Tests that the sort key expressions are the children, in declared order, and that the value has no type of its
     * own.
     */
    @Test
    void sortKeyExpressionsAreTheChildren() {
        final Value first = new LiteralValue<>(LONG_TYPE, 1L);
        final Value second = new LiteralValue<>(LONG_TYPE, 2L);
        final SortKeysValue value = sortKeysValue(
                new SortKeysValue.SortKey(first, RequestedSortOrder.ASCENDING),
                new SortKeysValue.SortKey(second, RequestedSortOrder.DESCENDING));

        assertThat(ImmutableList.<Value>copyOf(value.getChildren())).containsExactly(first, second);
        assertThat(value.getResultType()).isEqualTo(new Type.Any());
    }

    /**
     * Tests that an empty clause is rejected. An aggregate without an in-call {@code ORDER BY} clause holds no sort
     * keys value at all (rather than an empty one).
     */
    @Test
    void constructorRejectsEmptySortKeys() {
        assertThatThrownBy(() -> new SortKeysValue(ImmutableList.of()))
                .isInstanceOf(VerifyException.class);
    }

    /**
     * Tests that a sort key without a definite direction is rejected, as the in-call clause always resolves to one.
     */
    @Test
    void sortKeyRejectsAnySortOrder() {
        assertThatThrownBy(() -> new SortKeysValue.SortKey(new LiteralValue<>(LONG_TYPE, 1L), RequestedSortOrder.ANY))
                .isInstanceOf(VerifyException.class);
    }

    /**
     * Tests that the value cannot be evaluated. It only ever carries a requirement to the planner.
     */
    @Test
    void evalThrows() {
        final SortKeysValue value =
                sortKeysValue(new SortKeysValue.SortKey(new LiteralValue<>(LONG_TYPE, 1L),
                        RequestedSortOrder.ASCENDING));

        assertThatThrownBy(() -> value.eval(null, EvaluationContext.empty()))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Tests that two values differing only in the sort order of a sort key are neither equal nor plan-hash equally.
     */
    @Test
    void equalsAndHashCodeAccountForSortOrder() {
        final Value sortKey = new LiteralValue<>(LONG_TYPE, 1L);
        final SortKeysValue ascending =
                sortKeysValue(new SortKeysValue.SortKey(sortKey, RequestedSortOrder.ASCENDING));
        final SortKeysValue descending =
                sortKeysValue(new SortKeysValue.SortKey(sortKey, RequestedSortOrder.DESCENDING));

        assertThat(ascending).isNotEqualTo(descending);
        assertThat(ascending).isEqualTo(sortKeysValue(new SortKeysValue.SortKey(sortKey,
                RequestedSortOrder.ASCENDING)));
        assertThat(ascending.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(descending.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    /**
     * Tests that the value survives a round-trip through the generic {@code PValue} dispatch, so that it can be held
     * by any order-sensitive aggregate rather than only by the one that introduced it.
     */
    @Test
    void serializationRoundTripPreservesValue() {
        for (final RequestedSortOrder sortOrder : List.of(RequestedSortOrder.ASCENDING, RequestedSortOrder.DESCENDING,
                RequestedSortOrder.ASCENDING_NULLS_LAST, RequestedSortOrder.DESCENDING_NULLS_FIRST)) {
            final SortKeysValue value =
                    sortKeysValue(new SortKeysValue.SortKey(new LiteralValue<>(LONG_TYPE, 1L), sortOrder));

            final PlanSerializationContext context = PlanSerializationContext.newForCurrentMode();
            final Value deserialized = Value.fromValueProto(context, value.toValueProto(context));

            assertThat(deserialized).isInstanceOf(SortKeysValue.class);
            assertThat(deserialized).isEqualTo(value);
            assertThat(((SortKeysValue)deserialized).getSortKeys()).isEqualTo(value.getSortKeys());
        }
    }

    /**
     * Tests that replacing the children replaces the sort key expressions while keeping their sort orders.
     */
    @Test
    void withChildrenKeepsSortOrders() {
        final SortKeysValue value =
                sortKeysValue(new SortKeysValue.SortKey(new LiteralValue<>(LONG_TYPE, 1L),
                        RequestedSortOrder.DESCENDING));
        final Value newSortKey = new LiteralValue<>(LONG_TYPE, 2L);

        final SortKeysValue withNewChildren = value.withChildren(ImmutableList.of(newSortKey));

        assertThat(withNewChildren.getSortKeys())
                .containsExactly(new SortKeysValue.SortKey(newSortKey, RequestedSortOrder.DESCENDING));
    }

    /**
     * Tests that the conversion to ordering parts rebases the sort key expressions onto {@link Quantifier#current()},
     * which is what {@link OrderingPart} insists on, and that it keeps the sort orders and the declared order.
     */
    @Test
    void toOrderingPartsOnCurrentRebasesOntoCurrent() {
        final CorrelationIdentifier source = CorrelationIdentifier.of("q0");
        final Value first = QuantifiedObjectValue.of(source, LONG_TYPE);
        final Value second = QuantifiedObjectValue.of(source, LONG_TYPE);
        final SortKeysValue value = sortKeysValue(
                new SortKeysValue.SortKey(first, RequestedSortOrder.ASCENDING),
                new SortKeysValue.SortKey(second, RequestedSortOrder.DESCENDING));

        final List<RequestedOrderingPart> orderingParts = value.toOrderingPartsOnCurrent(source);

        assertThat(orderingParts).hasSize(2);
        assertThat(orderingParts).allSatisfy(orderingPart ->
                assertThat(orderingPart.getValue().getCorrelatedTo()).containsExactly(Quantifier.current()));
        assertThat(orderingParts).extracting(RequestedOrderingPart::getSortOrder)
                .containsExactly(RequestedSortOrder.ASCENDING, RequestedSortOrder.DESCENDING);
    }

    /**
     * Tests that a sort key upholds the {@code equals}/{@code hashCode} contract, and that neither the expression nor
     * the direction alone decides equality.
     */
    @Test
    void sortKeyEqualsAndHashCode() {
        final Value first = new LiteralValue<>(LONG_TYPE, 1L);
        final Value second = new LiteralValue<>(LONG_TYPE, 2L);
        final var ascending = new SortKeysValue.SortKey(first, RequestedSortOrder.ASCENDING);

        assertThat(ascending).isEqualTo(new SortKeysValue.SortKey(first, RequestedSortOrder.ASCENDING));
        assertThat(ascending).hasSameHashCodeAs(new SortKeysValue.SortKey(first, RequestedSortOrder.ASCENDING));
        // Differing only in the direction, and differing only in the expression.
        assertThat(ascending).isNotEqualTo(new SortKeysValue.SortKey(first, RequestedSortOrder.DESCENDING));
        assertThat(ascending).isNotEqualTo(new SortKeysValue.SortKey(second, RequestedSortOrder.ASCENDING));
    }

    /**
     * Tests that a sort key renders as its expression followed by the direction arrow, which is what makes a plan or a
     * debugger dump readable.
     */
    @Test
    void sortKeyToStringAppendsTheArrow() {
        final Value value = new LiteralValue<>(LONG_TYPE, 1L);

        assertThat(new SortKeysValue.SortKey(value, RequestedSortOrder.ASCENDING))
                .hasToString(value + RequestedSortOrder.ASCENDING.getArrowIndicator());
        assertThat(new SortKeysValue.SortKey(value, RequestedSortOrder.DESCENDING).toString())
                .endsWith(RequestedSortOrder.DESCENDING.getArrowIndicator());
    }

    @Nonnull
    private static SortKeysValue sortKeysValue(@Nonnull final SortKeysValue.SortKey... sortKeys) {
        return new SortKeysValue(ImmutableList.copyOf(sortKeys));
    }
}
