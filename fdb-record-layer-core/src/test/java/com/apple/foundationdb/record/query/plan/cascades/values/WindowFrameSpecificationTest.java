/*
 * WindowFrameSpecificationTest.java
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

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification.Bounded;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification.CurrentRow;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification.Exclusion;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification.FrameBoundary;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification.FrameType;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification.Unbounded;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link WindowFrameSpecification}, covering the default specification, proto round-trips over every
 * boundary shape, the SQL rendering produced by {@link WindowFrameSpecification#explain()}, and the constant-limit
 * invariant on {@link Bounded}.
 */
class WindowFrameSpecificationTest {

    @Nonnull
    private static Value constantLimit(final long limit) {
        return LiteralValue.ofScalar(limit);
    }

    @Nonnull
    private static WindowFrameSpecification roundTrip(@Nonnull final WindowFrameSpecification specification) {
        return WindowFrameSpecification.fromProto(PlanSerializationContext.newForCurrentMode(),
                specification.toProto(PlanSerializationContext.newForCurrentMode()));
    }

    @Nonnull
    private static String explain(@Nonnull final WindowFrameSpecification specification) {
        return specification.explain().render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString();
    }

    @Test
    void defaultSpecificationSpansWholePartition() {
        final var specification = WindowFrameSpecification.defaultSpecification();
        assertThat(specification.isDefault()).isTrue();
        assertThat(specification.frameType()).isEqualTo(FrameType.ROW);
        assertThat(specification.left()).isEqualTo(Unbounded.INSTANCE);
        assertThat(specification.right()).isEqualTo(Unbounded.INSTANCE);
        assertThat(specification.exclusion()).isEqualTo(Exclusion.NO_OTHER);
    }

    @ParameterizedTest
    @MethodSource
    void isDefaultDeviationFromDefaultReturnsFalse(@Nonnull final WindowFrameSpecification specification) {
        assertThat(specification.isDefault()).isFalse();
    }

    static Stream<WindowFrameSpecification> isDefaultDeviationFromDefaultReturnsFalse() {
        return Stream.of(
                // deviates in the frame type only
                new WindowFrameSpecification(FrameType.RANGE, Unbounded.INSTANCE, Unbounded.INSTANCE, Exclusion.NO_OTHER),
                new WindowFrameSpecification(FrameType.GROUPS, Unbounded.INSTANCE, Unbounded.INSTANCE, Exclusion.NO_OTHER),
                // deviates in one boundary only
                new WindowFrameSpecification(FrameType.ROW, new CurrentRow(), Unbounded.INSTANCE, Exclusion.NO_OTHER),
                new WindowFrameSpecification(FrameType.ROW, Unbounded.INSTANCE, new CurrentRow(), Exclusion.NO_OTHER),
                new WindowFrameSpecification(FrameType.ROW, new Bounded(constantLimit(1L)), Unbounded.INSTANCE, Exclusion.NO_OTHER),
                // deviates in the exclusion only
                new WindowFrameSpecification(FrameType.ROW, Unbounded.INSTANCE, Unbounded.INSTANCE, Exclusion.CURRENT_ROW));
    }

    @ParameterizedTest
    @MethodSource
    void toProtoFromProtoRoundTripsSpecification(@Nonnull final WindowFrameSpecification specification) {
        assertThat(roundTrip(specification)).isEqualTo(specification);
    }

    static Stream<WindowFrameSpecification> toProtoFromProtoRoundTripsSpecification() {
        final var boundaries = Stream.<FrameBoundary>of(Unbounded.INSTANCE, new CurrentRow(),
                new Bounded(constantLimit(7L)));
        return boundaries.flatMap(left -> Stream.of(FrameType.values())
                .flatMap(frameType -> Stream.of(Exclusion.values())
                        .map(exclusion -> new WindowFrameSpecification(frameType, left, new CurrentRow(), exclusion))));
    }

    @Test
    void toProtoFromProtoRoundTripsBoundedLimitValue() {
        final var specification = new WindowFrameSpecification(FrameType.ROW, new Bounded(constantLimit(42L)),
                new CurrentRow(), Exclusion.NO_OTHER);
        final var roundTripped = roundTrip(specification);
        assertThat(roundTripped.left()).isInstanceOf(Bounded.class);
        assertThat(((Bounded)roundTripped.left()).limit()).isEqualTo(constantLimit(42L));
    }

    @ParameterizedTest
    @CsvSource({"ROW,ROWS", "RANGE,RANGE", "GROUPS,GROUPS"})
    void explainRendersFrameTypeAndUnboundedBoundaries(@Nonnull final FrameType frameType,
                                                       @Nonnull final String expectedKeyword) {
        final var specification =
                new WindowFrameSpecification(frameType, Unbounded.INSTANCE, Unbounded.INSTANCE, Exclusion.NO_OTHER);
        // the expected keyword is spelled out rather than derived from the enum name: SQL says ROWS, the constant is ROW
        assertThat(explain(specification))
                .isEqualTo(expectedKeyword + " BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");
    }

    @Test
    void explainRendersCurrentRowBoundary() {
        final var specification = new WindowFrameSpecification(FrameType.ROW, Unbounded.INSTANCE, new CurrentRow(),
                Exclusion.NO_OTHER);
        assertThat(explain(specification)).isEqualTo("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    void explainRendersBoundedBoundaries() {
        final var specification = new WindowFrameSpecification(FrameType.ROW, new Bounded(constantLimit(1L)),
                new Bounded(constantLimit(3L)), Exclusion.NO_OTHER);
        assertThat(explain(specification)).isEqualTo("ROWS BETWEEN 1l PRECEDING AND 3l FOLLOWING");
    }

    @ParameterizedTest
    @MethodSource
    void explainRendersExclusion(@Nonnull final Exclusion exclusion, @Nonnull final String expectedSuffix) {
        final var specification =
                new WindowFrameSpecification(FrameType.ROW, Unbounded.INSTANCE, new CurrentRow(), exclusion);
        assertThat(explain(specification))
                .isEqualTo("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" + expectedSuffix);
    }

    static Stream<Arguments> explainRendersExclusion() {
        return Stream.of(
                // the default exclusion is left implicit
                Arguments.of(Exclusion.NO_OTHER, ""),
                Arguments.of(Exclusion.CURRENT_ROW, " EXCLUDE CURRENT ROW"),
                Arguments.of(Exclusion.GROUP, " EXCLUDE GROUP"),
                Arguments.of(Exclusion.TIES, " EXCLUDE TIES"));
    }

    @Test
    void boundedNonConstantLimitThrows() {
        final var nonConstant = QuantifiedObjectValue.of(CorrelationIdentifier.of("q"),
                Type.primitiveType(Type.TypeCode.LONG));
        assertThatThrownBy(() -> new Bounded(nonConstant))
                .isInstanceOf(RecordCoreArgumentException.class)
                .hasMessageContaining("constant");
    }
}
