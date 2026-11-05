/*
 * CompileTimeWindowValueTest.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue.WindowFunction;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CompileTimeWindowValue}: the layout of its children, how they are put back together, its SQL
 * rendering, its serialization, the fact that it refuses to be evaluated at all -- it only ever exists to be rewritten
 * away -- and the {@link CompileTimeWindowValue.RankFn} that builds one from a {@code RANK} call site.
 */
class CompileTimeWindowValueTest {

    @Nonnull
    private static final Value PARTITION_A = LiteralValue.ofScalar(1L);
    @Nonnull
    private static final Value PARTITION_B = LiteralValue.ofScalar(2L);
    @Nonnull
    private static final Value ARGUMENT = LiteralValue.ofScalar(3L);
    @Nonnull
    private static final Value ORDERING = LiteralValue.ofScalar(4L);

    @Nonnull
    private static WindowOrderingPart orderingPart(@Nonnull final OrderingPart.RequestedSortOrder sortOrder) {
        return new WindowOrderingPart(ORDERING, sortOrder);
    }

    @Nonnull
    private static CompileTimeWindowValue rank(@Nonnull final Iterable<WindowOrderingPart> orderingParts,
                                               @Nonnull final WindowFrameSpecification frameSpecification) {
        return new CompileTimeWindowValue(WindowFunction.RANK, ImmutableList.of(ARGUMENT),
                ImmutableList.of(PARTITION_A, PARTITION_B), orderingParts, frameSpecification);
    }

    @Nonnull
    private static CompileTimeWindowValue fullyPopulated() {
        return rank(ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.DESCENDING)),
                WindowFrameSpecification.defaultSpecification());
    }

    @Nonnull
    private static String explain(@Nonnull final Value value) {
        return value.explain().getExplainTokens()
                .render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString();
    }

    @ParameterizedTest
    @EnumSource(WindowFunction.class)
    void everyWindowFunctionHasANameAndAResultType(@Nonnull final WindowFunction windowFunction) {
        assertThat(windowFunction.getFunctionName()).isNotBlank();
        assertThat(windowFunction.getResultType()).isNotNull();

        final var value = new CompileTimeWindowValue(windowFunction, ImmutableList.of(), ImmutableList.of());
        assertThat(value.getName()).isEqualTo(windowFunction.getFunctionName());
        assertThat(value.getResultType()).isEqualTo(windowFunction.getResultType());
        assertThat(value.getWindowFunction()).isEqualTo(windowFunction);
    }

    @Test
    void rankProducesALongRank() {
        assertThat(WindowFunction.RANK.getFunctionName()).isEqualTo("rank");
        assertThat(WindowFunction.RANK.getResultType()).isEqualTo(Type.primitiveType(Type.TypeCode.LONG));
    }

    @Test
    void childrenAreLaidOutAsPartitioningThenArgumentsThenOrderingValues() {
        assertThat(ImmutableList.copyOf(fullyPopulated().getChildren()))
                .isEqualTo(ImmutableList.of(PARTITION_A, PARTITION_B, ARGUMENT, ORDERING));
    }

    @Test
    void withChildrenRedistributesTheChildrenAndKeepsTheSortOrders() {
        final var value = rank(ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.DESCENDING)),
                WindowFrameSpecification.defaultSpecification());

        final var rebuilt = value.withChildren(ImmutableList.of(ORDERING, ARGUMENT, PARTITION_A, PARTITION_B));

        assertThat(rebuilt.getPartitioningValues()).isEqualTo(ImmutableList.of(ORDERING, ARGUMENT));
        assertThat(rebuilt.getArgumentValues()).isEqualTo(ImmutableList.of(PARTITION_A));
        assertThat(rebuilt.getOrderingParts()).isEqualTo(ImmutableList.of(
                new WindowOrderingPart(PARTITION_B, OrderingPart.RequestedSortOrder.DESCENDING)));
        assertThat(rebuilt.getWindowFunction()).isEqualTo(WindowFunction.RANK);
    }

    @Test
    void withOrderingPartsReplacesOnlyTheOrderingParts() {
        final var value = fullyPopulated();
        final var newOrderingParts = ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.ASCENDING));

        final var withNewOrderingParts = value.withOrderingParts(newOrderingParts);

        assertThat(withNewOrderingParts.getOrderingParts()).isEqualTo(newOrderingParts);
        assertThat(withNewOrderingParts.getPartitioningValues()).isEqualTo(value.getPartitioningValues());
        assertThat(withNewOrderingParts.getArgumentValues()).isEqualTo(value.getArgumentValues());
        assertThat(withNewOrderingParts.getWindowFrameSpecification()).isEqualTo(value.getWindowFrameSpecification());
    }

    @Test
    void explainRendersArgumentsPartitioningAndOrdering() {
        assertThat(explain(fullyPopulated())).isEqualTo("rank(3l) OVER (PARTITION BY 1l, 2l ORDER BY 4l DESC)");
    }

    @Test
    void explainRendersAnEmptyOverGroupWhenThereIsNoSpecification() {
        final var value = new CompileTimeWindowValue(WindowFunction.RANK, ImmutableList.of(ARGUMENT),
                ImmutableList.of());
        assertThat(explain(value)).isEqualTo("rank(3l) OVER ()");
    }

    @Test
    void explainOmitsTheSortOrderOfAnUnconstrainedOrderingPart() {
        final var value = new CompileTimeWindowValue(WindowFunction.RANK, ImmutableList.of(), ImmutableList.of(),
                ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.ANY)),
                WindowFrameSpecification.defaultSpecification());
        assertThat(explain(value)).isEqualTo("rank() OVER (ORDER BY 4l)");
    }

    @Test
    void explainRendersANonDefaultFrameSpecification() {
        final var frameSpecification = new WindowFrameSpecification(WindowFrameSpecification.FrameType.ROW,
                new WindowFrameSpecification.Bounded(LiteralValue.ofScalar(1L)),
                new WindowFrameSpecification.CurrentRow(), WindowFrameSpecification.Exclusion.NO_OTHER);
        final var value = new CompileTimeWindowValue(WindowFunction.RANK, ImmutableList.of(), ImmutableList.of(),
                ImmutableList.of(), frameSpecification);

        assertThat(explain(value)).isEqualTo("rank() OVER (ROWS BETWEEN 1l PRECEDING AND CURRENT ROW)");
    }

    @Test
    void describeRendersTheNameAndTheFrameSpecification() {
        assertThat(fullyPopulated().describe()
                .render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString())
                .startsWith("rank");
    }

    @Test
    void protoRoundTripKeepsEveryComponent() {
        final var frameSpecification = new WindowFrameSpecification(WindowFrameSpecification.FrameType.GROUPS,
                WindowFrameSpecification.Unbounded.INSTANCE, new WindowFrameSpecification.CurrentRow(),
                WindowFrameSpecification.Exclusion.TIES);
        final var value = rank(ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.DESCENDING)),
                frameSpecification);

        final var roundTripped = CompileTimeWindowValue.fromProto(PlanSerializationContext.newForCurrentMode(),
                value.toProto(PlanSerializationContext.newForCurrentMode()));

        assertThat(roundTripped.getWindowFunction()).isEqualTo(WindowFunction.RANK);
        assertThat(roundTripped.getPartitioningValues()).isEqualTo(value.getPartitioningValues());
        assertThat(roundTripped.getArgumentValues()).isEqualTo(value.getArgumentValues());
        assertThat(roundTripped.getOrderingParts()).isEqualTo(value.getOrderingParts());
        assertThat(roundTripped.getWindowFrameSpecification()).isEqualTo(frameSpecification);
    }

    @Test
    void valueProtoRoundTripGoesThroughTheGenericValueDeserializer() {
        final var value = fullyPopulated();

        final var roundTripped = Value.fromValueProto(PlanSerializationContext.newForCurrentMode(),
                value.toValueProto(PlanSerializationContext.newForCurrentMode()));

        assertThat(roundTripped).isInstanceOf(CompileTimeWindowValue.class).isEqualTo(value);
    }

    @Test
    void evalAtRuntimeThrows() {
        assertThatThrownBy(() -> fullyPopulated().eval(null, EvaluationContext.empty()))
                .isInstanceOf(RecordCoreException.class);
    }

    @Test
    void evalWithoutStoreThrowsBecauseTheValueMustBeRewrittenAway() {
        assertThatThrownBy(() -> fullyPopulated().evalWithoutStore(EvaluationContext.empty()))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("consumed during plan rewriting");
    }

    @Test
    void valuesAgreeingOnFunctionOrderingAndFrameAreSemanticallyEqual() {
        assertThat(fullyPopulated()).isEqualTo(fullyPopulated());
        assertThat(fullyPopulated().hashCode()).isEqualTo(fullyPopulated().hashCode());
    }

    @Test
    void valuesDifferingInTheSortOrderOfAnOrderingPartAreNotEqual() {
        final var descending = fullyPopulated();
        final var ascending = descending.withOrderingParts(
                ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.ASCENDING)));

        assertThat(descending).isNotEqualTo(ascending);
        assertThat(descending.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(ascending.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void valuesDifferingInTheFrameSpecificationAreNotEqual() {
        final var withDefaultFrame = fullyPopulated();
        final var withGroupsFrame = rank(ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.DESCENDING)),
                new WindowFrameSpecification(WindowFrameSpecification.FrameType.GROUPS,
                        WindowFrameSpecification.Unbounded.INSTANCE, WindowFrameSpecification.Unbounded.INSTANCE,
                        WindowFrameSpecification.Exclusion.NO_OTHER));

        assertThat(withDefaultFrame).isNotEqualTo(withGroupsFrame);
        assertThat(withDefaultFrame.hashCodeWithoutChildren())
                .isNotEqualTo(withGroupsFrame.hashCodeWithoutChildren());
        assertThat(withDefaultFrame.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(withGroupsFrame.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void rankFnBuildsTheValueFromTheCallSiteWindowSpecification() {
        final var orderingParts = ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.DESCENDING));
        final var frameSpecification = new WindowFrameSpecification(WindowFrameSpecification.FrameType.ROW,
                new WindowFrameSpecification.Bounded(LiteralValue.ofScalar(1L)),
                new WindowFrameSpecification.CurrentRow(), WindowFrameSpecification.Exclusion.NO_OTHER);
        final var callSiteArguments = CallSiteArguments.ofPositional(ImmutableList.of(ARGUMENT))
                .withWindowSpecification(new CallSiteArguments.WindowSpecification(
                        ImmutableList.of(PARTITION_A, PARTITION_B), orderingParts, frameSpecification));

        final var value = new CompileTimeWindowValue.RankFn().encapsulate(callSiteArguments);

        assertThat(value).isInstanceOf(CompileTimeWindowValue.class);
        final var windowValue = (CompileTimeWindowValue)value;
        assertThat(windowValue.getWindowFunction()).isEqualTo(WindowFunction.RANK);
        assertThat(windowValue.getArgumentValues()).isEqualTo(ImmutableList.of(ARGUMENT));
        assertThat(windowValue.getPartitioningValues()).isEqualTo(ImmutableList.of(PARTITION_A, PARTITION_B));
        assertThat(windowValue.getOrderingParts()).isEqualTo(orderingParts);
        assertThat(windowValue.getWindowFrameSpecification()).isEqualTo(frameSpecification);
    }

    @Test
    void rankFnFallsBackToTheDefaultFrameWhenTheCallSiteDoesNotSpellOneOut() {
        final var callSiteArguments = CallSiteArguments.ofPositional(ImmutableList.of(ARGUMENT))
                .withWindowSpecification(new CallSiteArguments.WindowSpecification(
                        ImmutableList.of(PARTITION_A), ImmutableList.of()));

        final var windowValue = (CompileTimeWindowValue)new CompileTimeWindowValue.RankFn()
                .encapsulate(callSiteArguments);

        assertThat(windowValue.getWindowFrameSpecification())
                .isEqualTo(WindowFrameSpecification.defaultSpecification());
    }

    @Test
    void rankIsDiscoverableInTheBuiltInFunctionCatalog() {
        //
        // RankFn is registered via AutoService, so the catalog should hand it out for a rank call site with one or more
        // arguments. rank() without arguments is not a valid call site.
        //
        assertThat(BuiltInFunctionCatalog.resolve("rank", 1)).containsInstanceOf(CompileTimeWindowValue.RankFn.class);
        assertThat(BuiltInFunctionCatalog.resolve("rank", 3)).containsInstanceOf(CompileTimeWindowValue.RankFn.class);
        assertThat(BuiltInFunctionCatalog.resolve("rank", 0)).isEmpty();
    }

    @Test
    void rankFnRejectsNamedArguments() {
        final var callSiteArguments = CallSiteArguments.ofNamed(ImmutableMap.of("anArgument", ARGUMENT));
        final var rankFn = new CompileTimeWindowValue.RankFn();

        //
        // BuiltInFunction.encapsulate is the single funnel for every built-in and rejects the named convention itself,
        // so a window function does not have to check for it.
        //
        assertThatThrownBy(() -> rankFn.encapsulate(callSiteArguments))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("named argument calling conventions");
    }
}
