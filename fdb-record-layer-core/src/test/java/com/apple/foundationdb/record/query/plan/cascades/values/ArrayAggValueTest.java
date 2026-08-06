/*
 * ArrayAggValueTest.java
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
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link ArrayAggValue} and of its {@link ArrayAggValue.ArrayAccumulator}. The accumulator keeps its partial
 * state as a growing list and has to serialize that list into, and restore it from, a continuation. This is an aspect
 * of {@code ARRAY_AGG()} that has no analog among the other aggregates, whose partial state is a scalar; hence the
 * specific tests here.
 */
class ArrayAggValueTest {
    @Nonnull
    private static final Type LONG_TYPE = Type.primitiveType(Type.TypeCode.LONG, false);

    /**
     * Restores a new accumulator from the partial state of the given one, in the way a continuation would.
     */
    @Nonnull
    private static ArrayAggValue.ArrayAccumulator restore(@Nonnull final Type elementType,
                                                          final boolean ignoreNulls,
                                                          @Nonnull final ArrayAggValue.ArrayAccumulator accumulator) {
        final List<RecordCursorProto.AccumulatorState> states = accumulator.getAccumulatorStates();
        assertThat(states).hasSize(1);
        return new ArrayAggValue.ArrayAccumulator(ArrayAggValue.wrapperDescriptorFor(elementType),
                repositoryFor(elementType), elementType, ignoreNulls, states.get(0));
    }

    /**
     * Returns a type repository the given element type is registered in, as the accumulator converts its elements
     * against it.
     */
    @Nonnull
    private static TypeRepository repositoryFor(@Nonnull final Type elementType) {
        return TypeRepository.newBuilder().addTypeIfNeeded(elementType).build();
    }

    /**
     * Returns a new accumulator for the given element type.
     */
    @Nonnull
    private static ArrayAggValue.ArrayAccumulator accumulator(@Nonnull final Type elementType,
                                                             final boolean ignoreNulls) {
        return new ArrayAggValue.ArrayAccumulator(ArrayAggValue.wrapperDescriptorFor(elementType),
                repositoryFor(elementType), elementType, ignoreNulls);
    }

    /**
     * Returns the aggregated elements of the given accumulator.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    private static List<Object> finish(@Nonnull final ArrayAggValue.ArrayAccumulator accumulator) {
        return (List<Object>)accumulator.finish();
    }

    /**
     * Tests that a group which saw no rows at all reports no state, which is how {@code AggregateCursor} recognizes an
     * empty scan.
     */
    @Test
    void getAccumulatorStatesWithoutRowsReturnsNoState() {
        final var accumulator = accumulator(LONG_TYPE, true);

        assertThat(accumulator.getAccumulatorStates()).isEmpty();
        assertThat(finish(accumulator)).isEmpty();
    }

    /**
     * Tests that a group which saw rows, all of them {@code NULL}, is distinct from a group that saw no rows: it
     * reports state, and it aggregates to an empty array rather than to {@code NULL}.
     */
    @Test
    void getAccumulatorStatesWithOnlyNullRowsReturnsState() {
        final var accumulator = accumulator(LONG_TYPE, true);
        accumulator.accumulate(null);

        assertThat(accumulator.getAccumulatorStates()).hasSize(1);
        assertThat(finish(accumulator)).isEmpty();

        final var restored = restore(LONG_TYPE, true, accumulator);
        assertThat(restored.getAccumulatorStates()).hasSize(1);
        assertThat(finish(restored)).isEmpty();
    }

    /**
     * Tests that resuming mid-group carries the already-collected elements across the continuation boundary, in order,
     * and that accumulation continues from there.
     */
    @Test
    void accumulateAfterRestoringStatePreservesElementsAndOrder() {
        final var accumulator = accumulator(LONG_TYPE, true);
        accumulator.accumulate(100L);
        accumulator.accumulate(200L);

        final var restored = restore(LONG_TYPE, true, accumulator);
        assertThat(finish(restored)).containsExactly(100L, 200L);

        restored.accumulate(300L);
        assertThat(finish(restored)).containsExactly(100L, 200L, 300L);
    }

    /**
     * Tests that the elements survive a round-trip on every single row, so that already-restored state is itself
     * serialized again.
     */
    @Test
    void accumulateAfterRestoringStateRepeatedlyPreservesElements() {
        var accumulator = accumulator(LONG_TYPE, true);
        for (long i = 1L; i <= 5L; i++) {
            accumulator.accumulate(i);
            accumulator = restore(LONG_TYPE, true, accumulator);
        }

        assertThat(finish(accumulator)).containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    /**
     * Tests that a {@code NULL} input is skipped under {@code IGNORE NULLS}, both before and after a round-trip.
     */
    @Test
    void accumulateNullWithIgnoreNullsSkipsElement() {
        final var accumulator = accumulator(LONG_TYPE, true);
        accumulator.accumulate(100L);
        accumulator.accumulate(null);
        accumulator.accumulate(200L);

        assertThat(finish(accumulator)).containsExactly(100L, 200L);
        assertThat(finish(restore(LONG_TYPE, true, accumulator))).containsExactly(100L, 200L);
    }

    /**
     * Tests that a {@code NULL} input under {@code RESPECT NULLS} is reported rather than silently dropped. Nulls
     * currently cannot be represented in the protobuf repeated field backing an array.
     */
    @Test
    void accumulateNullWithRespectNullsThrowsUnsupported() {
        final var accumulator = accumulator(LONG_TYPE, false);
        accumulator.accumulate(100L);

        assertThatThrownBy(() -> accumulator.accumulate(null))
                .isInstanceOf(SemanticException.class)
                .extracting(e -> ((SemanticException)e).getErrorCode())
                .isEqualTo(SemanticException.ErrorCode.UNSUPPORTED);
    }

    /**
     * Tests that a round-trip preserves record-typed elements, which the accumulator has to serialize as nested
     * messages rather than as scalars.
     */
    @Test
    void accumulateAfterRestoringStateWithRecordElementsPreservesElements() {
        final Type.Record elementType = Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(LONG_TYPE, Optional.of("a")),
                Type.Record.Field.of(LONG_TYPE, Optional.of("b"))));
        final Message first = record(elementType, 1L, 2L);
        final Message second = record(elementType, 3L, 4L);

        final var accumulator = accumulator(elementType, true);
        accumulator.accumulate(first);
        final var restored = restore(elementType, true, accumulator);
        restored.accumulate(second);

        // A restored element is parsed against the accumulator’s own descriptor, so it is compared on the wire rather
        // than by identity of its descriptor.
        final List<Object> elements = finish(restored);
        assertThat(elements).hasSize(2);
        assertThat(((Message)elements.get(0)).toByteString()).isEqualTo(first.toByteString());
        assertThat(((Message)elements.get(1)).toByteString()).isEqualTo(second.toByteString());
    }

    /**
     * Tests that the result type is a nullable array whose element type mirrors the child, except under
     * {@code IGNORE NULLS}, where it is forced to be non-nullable.
     */
    @Test
    void getResultTypeIsNullableArrayOverElementType() {
        final Type nullableLong = Type.primitiveType(Type.TypeCode.LONG, true);
        final Value child = new LiteralValue<>(nullableLong, 1L);

        final Type respectNullsType = new ArrayAggValue(child, false).getResultType();
        assertThat(respectNullsType.isNullable()).isTrue();
        assertThat(((Type.Array)respectNullsType).getElementType()).isEqualTo(nullableLong);

        final Type ignoreNullsType = new ArrayAggValue(child, true).getResultType();
        assertThat(ignoreNullsType.isNullable()).isTrue();
        assertThat(((Type.Array)ignoreNullsType).getElementType()).isEqualTo(nullableLong.notNullable());
    }

    /**
     * Tests that the value cannot be evaluated row-wise. It is an aggregate, so it is evaluated through an accumulator
     * and {@code evalToPartial()} instead.
     */
    @Test
    void evalThrows() {
        final ArrayAggValue value = new ArrayAggValue(new LiteralValue<>(LONG_TYPE, 1L), true);

        assertThatThrownBy(() -> value.eval(null, EvaluationContext.empty()))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Tests that two values over the same child are not equal if their null treatment differs, and that equal values
     * hash equally.
     */
    @Test
    void equalsAndHashCodeAccountForNullTreatment() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final ArrayAggValue ignoreNulls = new ArrayAggValue(child, true);
        final ArrayAggValue respectNulls = new ArrayAggValue(child, false);

        assertThat(ignoreNulls).isNotEqualTo(respectNulls);
        assertThat(ignoreNulls).isEqualTo(new ArrayAggValue(child, true));
        assertThat(ignoreNulls.hashCode()).isEqualTo(new ArrayAggValue(child, true).hashCode());
        assertThat(ignoreNulls.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(respectNulls.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    /**
     * Tests that a value survives a round-trip through its proto representation, null treatment and element type
     * included.
     */
    @Test
    void serializationRoundTripPreservesValue() {
        final Value child = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG, true), 1L);
        for (final boolean ignoreNulls : List.of(true, false)) {
            final ArrayAggValue value = new ArrayAggValue(child, ignoreNulls);

            final PlanSerializationContext context = PlanSerializationContext.newForCurrentMode();
            final Value deserialized = Value.fromValueProto(context, value.toValueProto(context));

            assertThat(deserialized).isInstanceOf(ArrayAggValue.class);
            assertThat(deserialized).isEqualTo(value);
            assertThat(deserialized.getResultType()).isEqualTo(value.getResultType());
        }
    }

    /**
     * Tests that replacing the child keeps the null treatment and re-derives the element type from the new child.
     */
    @Test
    void withChildrenKeepsNullTreatment() {
        final ArrayAggValue value = new ArrayAggValue(new LiteralValue<>(LONG_TYPE, 1L), true);
        final Value newChild = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING, true), "x");

        final ArrayAggValue withNewChild = value.withChildren(ImmutableList.of(newChild));

        assertThat(withNewChild.getResultType())
                .isEqualTo(new Type.Array(true, Type.primitiveType(Type.TypeCode.STRING, false)));
        assertThat(withNewChild).isNotEqualTo(value);
    }

    /**
     * Tests that the function rejects a null treatment argument that is not a boolean literal, as the grammar is
     * supposed to guarantee it.
     */
    @Test
    void encapsulateRejectsNonBooleanNullTreatment() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value notABooleanLiteral = new LiteralValue<>(LONG_TYPE, 42L);

        assertThatThrownBy(() -> new ArrayAggValue.ArrayAggFn()
                .encapsulate(CallSiteArguments.ofPositional(child, notABooleanLiteral)))
                .isInstanceOf(RecordCoreException.class);
    }

    /**
     * Builds a two-field record message of the given type.
     */
    @Nonnull
    private static Message record(@Nonnull final Type.Record recordType, final long a, final long b) {
        final TypeRepository typeRepository = TypeRepository.newBuilder().addTypeIfNeeded(recordType).build();
        final Descriptors.Descriptor descriptor = typeRepository.getMessageDescriptor(recordType);
        assertThat(descriptor).isNotNull();
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("a"), a)
                .setField(descriptor.findFieldByName("b"), b)
                .build();
    }
}
