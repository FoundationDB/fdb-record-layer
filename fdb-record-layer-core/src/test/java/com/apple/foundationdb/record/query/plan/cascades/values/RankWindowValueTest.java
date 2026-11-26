/*
 * RankWindowValueTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue.WindowFunction;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RankWindowValue} and the {@link WindowValue} machinery it inherits: its arguments and frame, its SQL
 * rendering, its serialization, and the fact that it cannot be evaluated because it is index-only.
 */
class RankWindowValueTest {

    @Nonnull
    private static final Value ARGUMENT = LiteralValue.ofScalar(3L);
    @Nonnull
    private static final Value OTHER_ARGUMENT = LiteralValue.ofScalar(4L);

    @Nonnull
    private static RankWindowValue rank(@Nonnull final WindowFrameSpecification frameSpecification) {
        return new RankWindowValue(ImmutableList.of(ARGUMENT), frameSpecification);
    }

    @Nonnull
    private static WindowFrameSpecification nonDefaultFrame() {
        return new WindowFrameSpecification(WindowFrameSpecification.FrameType.ROW,
                new WindowFrameSpecification.Bounded(LiteralValue.ofScalar(1L)),
                new WindowFrameSpecification.CurrentRow(), WindowFrameSpecification.Exclusion.NO_OTHER);
    }

    @Nonnull
    private static String explain(@Nonnull final Value value) {
        return value.explain().getExplainTokens()
                .render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString();
    }

    @Test
    void rankProducesALongRank() {
        final var value = rank(WindowFrameSpecification.defaultSpecification());

        assertThat(value.getName()).isEqualTo("rank");
        assertThat(value.getResultType()).isEqualTo(Type.primitiveType(Type.TypeCode.LONG));
    }

    @Test
    void theNameAndResultTypeAgreeWithTheCallSiteCatalog() {
        // the call-site enum and this value declare the function independently, so they have to be kept in step
        final var value = rank(WindowFrameSpecification.defaultSpecification());

        assertThat(value.getName()).isEqualTo(WindowFunction.RANK.getFunctionName());
        assertThat(value.getResultType()).isEqualTo(WindowFunction.RANK.getResultType());
    }

    @Test
    void theArgumentsAreTheChildren() {
        final var value = new RankWindowValue(ImmutableList.of(ARGUMENT, OTHER_ARGUMENT),
                WindowFrameSpecification.defaultSpecification());

        assertThat(ImmutableList.copyOf(value.getChildren())).isEqualTo(ImmutableList.of(ARGUMENT, OTHER_ARGUMENT));
        assertThat(value.getArgumentValues()).isEqualTo(ImmutableList.of(ARGUMENT, OTHER_ARGUMENT));
    }

    @Test
    void withChildrenReplacesTheArgumentsAndKeepsTheFrame() {
        final var frameSpecification = nonDefaultFrame();
        final var rebuilt = rank(frameSpecification).withChildren(ImmutableList.of(OTHER_ARGUMENT));

        assertThat(rebuilt.getArgumentValues()).isEqualTo(ImmutableList.of(OTHER_ARGUMENT));
        assertThat(rebuilt.getWindowFrameSpecification()).isEqualTo(frameSpecification);
    }

    @Test
    void explainSpellsOutAnEmptyOverGroupForTheDefaultFrame() {
        assertThat(explain(rank(WindowFrameSpecification.defaultSpecification()))).isEqualTo("rank(3l) OVER ()");
    }

    @Test
    void explainRendersANonDefaultFrameInsideTheOverGroup() {
        assertThat(explain(rank(nonDefaultFrame())))
                .isEqualTo("rank(3l) OVER (ROWS BETWEEN 1l PRECEDING AND CURRENT ROW)");
    }

    @Test
    void rankIsIndexOnlyAndRefusesToBeEvaluated() {
        final var value = rank(WindowFrameSpecification.defaultSpecification());

        assertThatThrownBy(() -> value.eval(null, EvaluationContext.empty()))
                .isInstanceOf(RecordCoreException.class);
    }

    @Test
    void valuesDifferingOnlyInTheFrameAreNotEqual() {
        assertThat(rank(nonDefaultFrame())).isNotEqualTo(rank(WindowFrameSpecification.defaultSpecification()));
    }

    @Test
    void valuesDifferingOnlyInTheirArgumentsDoNotSharePlanHashes() {
        final var one = new RankWindowValue(ImmutableList.of(ARGUMENT), WindowFrameSpecification.defaultSpecification());
        final var other = new RankWindowValue(ImmutableList.of(OTHER_ARGUMENT),
                WindowFrameSpecification.defaultSpecification());

        assertThat(one.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(other.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void protoRoundTripKeepsEveryComponent() {
        final var frameSpecification = new WindowFrameSpecification(WindowFrameSpecification.FrameType.GROUPS,
                WindowFrameSpecification.Unbounded.INSTANCE, new WindowFrameSpecification.CurrentRow(),
                WindowFrameSpecification.Exclusion.TIES);
        final var value = new RankWindowValue(ImmutableList.of(ARGUMENT, OTHER_ARGUMENT), frameSpecification);

        final var roundTripped = RankWindowValue.fromProto(PlanSerializationContext.newForCurrentMode(),
                value.toProto(PlanSerializationContext.newForCurrentMode()));

        assertThat(roundTripped.getArgumentValues()).isEqualTo(value.getArgumentValues());
        assertThat(roundTripped.getWindowFrameSpecification()).isEqualTo(frameSpecification);
        assertThat(roundTripped).isEqualTo(value);
    }

    @Test
    void valueProtoRoundTripGoesThroughTheGenericValueDeserializer() {
        final var value = new RankWindowValue(ImmutableList.of(ARGUMENT), nonDefaultFrame());

        final var roundTripped = Value.fromValueProto(PlanSerializationContext.newForCurrentMode(),
                value.toValueProto(PlanSerializationContext.newForCurrentMode()));

        assertThat(roundTripped).isInstanceOf(RankWindowValue.class);
        assertThat(roundTripped).isEqualTo(value);
    }

    @Test
    void compileTimeWindowValueConvertsToARankWindowValueWithoutPartitioningOrOrdering() {
        final var frameSpecification = nonDefaultFrame();
        final var compileTimeValue = new CompileTimeWindowValue(WindowFunction.RANK, ImmutableList.of(ARGUMENT),
                ImmutableList.of(LiteralValue.ofScalar(1L)), ImmutableList.of(), frameSpecification);

        final var windowValue = compileTimeValue.toWindowValue();

        assertThat(windowValue).isInstanceOf(RankWindowValue.class);
        assertThat(windowValue.getArgumentValues()).isEqualTo(ImmutableList.of(ARGUMENT));
        assertThat(windowValue.getWindowFrameSpecification()).isEqualTo(frameSpecification);
        // the partitioning column is not carried over: it belongs to the window expression, not to the value
        assertThat(ImmutableList.copyOf(windowValue.getChildren())).isEqualTo(ImmutableList.of(ARGUMENT));
    }
}
