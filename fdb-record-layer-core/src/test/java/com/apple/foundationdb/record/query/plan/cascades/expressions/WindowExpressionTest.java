/*
 * WindowExpressionTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RankWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.base.VerifyException;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link WindowExpression}: what it flows out, what it is correlated to, and -- since a window expression is
 * memoized by the planner -- that expressions differing in their partitioning or ordering do not collapse into one.
 */
class WindowExpressionTest {

    private static final Type.Record TYPE_T = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("b"))
    ));

    @Nonnull
    private static Quantifier.ForEach baseQuantifier() {
        final var scan = new FullUnorderedScanExpression(ImmutableSet.of("T"),
                Type.Record.fromFields(ImmutableList.of()), new AccessHints());
        final var filter = LogicalTypeFilterExpression.of(ImmutableSet.of("T"),
                Quantifier.forEach(Reference.initialOf(scan)), TYPE_T);
        return Quantifier.forEach(Reference.initialOf(filter));
    }

    @Nonnull
    private static Value field(@Nonnull final Quantifier quantifier, @Nonnull final String fieldName) {
        return FieldValue.ofFieldName(QuantifiedObjectValue.of(quantifier.getAlias(), TYPE_T), fieldName);
    }

    @Nonnull
    private static WindowValue rankOver(@Nonnull final Value argument) {
        return new RankWindowValue(ImmutableList.of(argument), WindowFrameSpecification.defaultSpecification());
    }

    @Test
    void requestedOrderingWithoutPartitioningOrOrderingPreservesTheOrder() {
        final var inner = baseQuantifier();
        final var expression = new WindowExpression(rankOver(field(inner, "a")), ImmutableList.of(),
                ImmutableList.of(field(inner, "a")), ImmutableList.of(), inner);

        assertThat(expression.getRequestedOrdering().isPreserve())
                .as("a window with neither PARTITION BY nor ORDER BY does not request any ordering")
                .isTrue();
    }

    @Test
    void requestedOrderingOfUncorrelatedPartsPreservesTheOrder() {
        final var inner = baseQuantifier();
        final var literal = new LiteralValue<>(1L);
        final var expression = new WindowExpression(rankOver(field(inner, "a")), ImmutableList.of(),
                ImmutableList.of(field(inner, "a")),
                ImmutableList.of(new WindowOrderingPart(literal, OrderingPart.RequestedSortOrder.ASCENDING)), inner);

        assertThat(expression.getRequestedOrdering().isPreserve())
                .as("ordering by a literal does not request any ordering")
                .isTrue();
    }

    @Test
    void requestedOrderingRebasesOrderingValuesOntoTheCurrentQuantifier() {
        final var inner = baseQuantifier();
        final var expression = new WindowExpression(rankOver(field(inner, "a")), ImmutableList.of(),
                ImmutableList.of(field(inner, "a")),
                ImmutableList.of(new WindowOrderingPart(field(inner, "a"),
                        OrderingPart.RequestedSortOrder.DESCENDING)), inner);

        final var requestedOrdering = expression.getRequestedOrdering();

        assertThat(requestedOrdering.isPreserve()).isFalse();
        final var part = Iterables.getOnlyElement(requestedOrdering.getOrderingParts());
        assertThat(part.getValue().getCorrelatedTo())
                .as("the ordering value should be rebased onto the current quantifier")
                .containsExactly(Quantifier.current());
        assertThat(part.getSortOrder())
                .as("the sort direction should be carried over")
                .isEqualTo(OrderingPart.RequestedSortOrder.DESCENDING);
        assertThat(requestedOrdering.getDistinctness())
                .isEqualTo(RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);
    }

    @Test
    void requestedOrderingRejectsPartsCorrelatedToMoreThanOneAlias() {
        final var inner = baseQuantifier();
        final var otherInner = baseQuantifier();
        final var expression = new WindowExpression(rankOver(field(inner, "a")), ImmutableList.of(),
                ImmutableList.of(field(inner, "a")),
                ImmutableList.of(new WindowOrderingPart(field(inner, "a"),
                                OrderingPart.RequestedSortOrder.ASCENDING),
                        new WindowOrderingPart(field(otherInner, "a"),
                                OrderingPart.RequestedSortOrder.ASCENDING)), inner);

        assertThatThrownBy(expression::getRequestedOrdering)
                .as("a window's partitioning and ordering columns must all come from the quantifier underneath it")
                .isInstanceOf(VerifyException.class);
    }

    @Nonnull
    private static WindowExpression windowExpression(@Nonnull final Quantifier.ForEach inner,
                                                    @Nonnull final ImmutableList<Value> partitioningValues,
                                                    @Nonnull final ImmutableList<WindowOrderingPart> windowOrderingParts) {
        return new WindowExpression(rankOver(field(inner, "a")), partitioningValues,
                ImmutableList.of(field(inner, "a")), windowOrderingParts, inner);
    }

    @Test
    void thePartitioningColumnsAreFlowedThenTheAdditionalResultsThenTheWindowValue() {
        final var inner = baseQuantifier();
        final var partitioning = field(inner, "b");
        final var additionalResult = field(inner, "a");
        final var windowValue = rankOver(field(inner, "a"));
        final var expression = new WindowExpression(windowValue, ImmutableList.of(partitioning),
                ImmutableList.of(additionalResult), ImmutableList.of(), inner);

        final var flowed = Values.deconstructRecord(expression.getResultValue());

        // the expression flows its own partitioning columns rather than leaving the caller to repeat them among the
        // additional results: a column a window partitions by is almost always wanted above it, and listing it twice
        // made the same column appear twice in the inputs
        assertThat(flowed).hasSize(3);
        assertThat(flowed.get(0)).isEqualTo(partitioning);
        assertThat(flowed.get(1)).isEqualTo(additionalResult);
        assertThat(flowed.get(2)).isEqualTo(windowValue);
    }

    @Test
    void thereIsExactlyOneRelationalChild() {
        final var inner = baseQuantifier();
        final var expression = windowExpression(inner, ImmutableList.of(field(inner, "b")),
                ImmutableList.of());

        assertThat(expression.getRelationalChildCount()).isEqualTo(1);
        assertThat(Iterables.getOnlyElement(expression.getQuantifiers())).isEqualTo(inner);
        assertThat(expression.getInnerQuantifier()).isEqualTo(inner);
    }

    @Test
    void theExpressionIsCorrelatedToWhateverItsValuesReference() {
        final var inner = baseQuantifier();
        final var expression = windowExpression(inner, ImmutableList.of(field(inner, "b")),
                ImmutableList.of());

        assertThat(expression.getCorrelatedToWithoutChildren()).containsExactly(inner.getAlias());
    }

    @Test
    void expressionsDifferingOnlyInTheirPartitioningAreNotEqual() {
        final var inner = baseQuantifier();
        final var partitionedByB = windowExpression(inner, ImmutableList.of(field(inner, "b")),
                ImmutableList.of());
        final var partitionedByA = windowExpression(inner, ImmutableList.of(field(inner, "a")),
                ImmutableList.of());

        assertThat(partitionedByB).isNotEqualTo(partitionedByA);
    }

    @Test
    void expressionsDifferingOnlyInTheirRequestedOrderingAreNotEqual() {
        final var inner = baseQuantifier();
        final var preserving = windowExpression(inner, ImmutableList.of(field(inner, "b")),
                ImmutableList.of());
        // the ORDER BY is stated over the expression's own input; the expression rebases it onto Quantifier.current()
        // when it derives the requested ordering
        final var orderedByA = windowExpression(inner, ImmutableList.of(field(inner, "b")),
                ImmutableList.of(new WindowOrderingPart(field(inner, "a"),
                        OrderingPart.RequestedSortOrder.ASCENDING)));

        assertThat(preserving).isNotEqualTo(orderedByA);
    }

    @Test
    void equalExpressionsHaveEqualHashCodes() {
        final var inner = baseQuantifier();
        final var one = windowExpression(inner, ImmutableList.of(field(inner, "b")), ImmutableList.of());
        final var other = windowExpression(inner, ImmutableList.of(field(inner, "b")), ImmutableList.of());

        assertThat(one).isEqualTo(other);
        assertThat(one.hashCode()).isEqualTo(other.hashCode());
        assertThat(one.semanticHashCode()).isEqualTo(other.semanticHashCode());
    }

    @Test
    void translateCorrelationsKeepsTheOrderingAndTheStructure() {
        final var inner = baseQuantifier();
        final var expression = windowExpression(inner, ImmutableList.of(field(inner, "b")), ImmutableList.of());

        final var otherInner = baseQuantifier();
        final var translated = (WindowExpression)expression.translateCorrelations(
                com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap.ofAliases(
                        inner.getAlias(), otherInner.getAlias()),
                false, ImmutableList.of(otherInner));

        assertThat(translated.getRequestedOrdering()).isEqualTo(expression.getRequestedOrdering());
        assertThat(translated.getPartitioningValues()).hasSize(1);
        assertThat(translated.getAdditionalResultValues()).hasSize(1);
        assertThat(translated.getWindowValue()).isInstanceOf(RankWindowValue.class);
        assertThat(translated.getCorrelatedToWithoutChildren()).containsExactly(otherInner.getAlias());
    }
}
