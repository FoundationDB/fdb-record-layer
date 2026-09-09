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
     * A value together with the type repository its accumulators resolve descriptors against, i.e. the pair that a
     * single plan execution works with. Restoring from a continuation <em>within</em> one execution reuses that one
     * repository, which is what makes a restored element share its descriptor with a freshly collected one.
     */
    private static final class Fixture {
        @Nonnull
        private final ArrayAggValue value;
        @Nonnull
        private final TypeRepository typeRepository;

        private Fixture(@Nonnull final Type elementType, final boolean ignoreNulls) {
            // The element type is derived from the child, and `notNullable()` is a no-op for the non-nullable types
            // used here, so the child type doubles as the element type.
            this.value = new ArrayAggValue(new LiteralValue<>(elementType, null), ignoreNulls);
            // Mirrors the plan-wide repository, which is built from the plan's used types. Registering the value's
            // (nullable) array result type registers both the wrapper record the accumulator serializes its partial
            // state through and the element type it converts its elements against.
            this.typeRepository = TypeRepository.newBuilder().addTypeIfNeeded(value.getResultType()).build();
        }

        @Nonnull
        private Accumulator accumulator() {
            return value.createAccumulatorWithInitialState(typeRepository, null);
        }

        /**
         * Restores a new accumulator from the partial state of the given one, in the way a continuation would.
         */
        @Nonnull
        private Accumulator restore(@Nonnull final Accumulator accumulator) {
            final List<RecordCursorProto.AccumulatorState> states = accumulator.getAccumulatorStates();
            assertThat(states).hasSize(1);
            return value.createAccumulatorWithInitialState(typeRepository, states);
        }

        /**
         * Builds a two-field record message against this fixture's repository, i.e. the way an element arriving from
         * the input would already be represented.
         */
        @Nonnull
        private Message record(@Nonnull final Type.Record recordType, final long a, final long b) {
            final Descriptors.Descriptor descriptor = typeRepository.getMessageDescriptor(recordType);
            assertThat(descriptor).isNotNull();
            return DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("a"), a)
                    .setField(descriptor.findFieldByName("b"), b)
                    .build();
        }
    }

    /**
     * Returns the aggregated elements of the given accumulator.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    private static List<Object> finish(@Nonnull final Accumulator accumulator) {
        return (List<Object>)accumulator.finish();
    }

    /**
     * The two-field record type used by the record-element tests.
     */
    @Nonnull
    private static Type.Record recordElementType() {
        return Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(LONG_TYPE, Optional.of("a")),
                Type.Record.Field.of(LONG_TYPE, Optional.of("b"))));
    }

    /**
     * Tests that a group which saw no rows at all reports no state, which is how {@code AggregateCursor} recognizes an
     * empty scan.
     */
    @Test
    void getAccumulatorStatesWithoutRowsReturnsNoState() {
        final var accumulator = new Fixture(LONG_TYPE, true).accumulator();

        assertThat(accumulator.getAccumulatorStates()).isEmpty();
        assertThat(finish(accumulator)).isEmpty();
    }

    /**
     * Tests that a group which saw rows, all of them {@code NULL}, is distinct from a group that saw no rows: it
     * reports state, and it aggregates to an empty array rather than to {@code NULL}.
     */
    @Test
    void getAccumulatorStatesWithOnlyNullRowsReturnsState() {
        final var fixture = new Fixture(LONG_TYPE, true);
        final var accumulator = fixture.accumulator();
        accumulator.accumulate(null);

        assertThat(accumulator.getAccumulatorStates()).hasSize(1);
        assertThat(finish(accumulator)).isEmpty();

        final var restored = fixture.restore(accumulator);
        assertThat(restored.getAccumulatorStates()).hasSize(1);
        assertThat(finish(restored)).isEmpty();
    }

    /**
     * Tests that resuming mid-group carries the already-collected elements across the continuation boundary, in order,
     * and that accumulation continues from there.
     */
    @Test
    void accumulateAfterRestoringStatePreservesElementsAndOrder() {
        final var fixture = new Fixture(LONG_TYPE, true);
        final var accumulator = fixture.accumulator();
        accumulator.accumulate(100L);
        accumulator.accumulate(200L);

        final var restored = fixture.restore(accumulator);
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
        final var fixture = new Fixture(LONG_TYPE, true);
        var accumulator = fixture.accumulator();
        for (long i = 1L; i <= 5L; i++) {
            accumulator.accumulate(i);
            accumulator = fixture.restore(accumulator);
        }

        assertThat(finish(accumulator)).containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    /**
     * Tests that a {@code NULL} input is skipped under {@code IGNORE NULLS}, both before and after a round-trip.
     */
    @Test
    void accumulateNullWithIgnoreNullsSkipsElement() {
        final var fixture = new Fixture(LONG_TYPE, true);
        final var accumulator = fixture.accumulator();
        accumulator.accumulate(100L);
        accumulator.accumulate(null);
        accumulator.accumulate(200L);

        assertThat(finish(accumulator)).containsExactly(100L, 200L);
        assertThat(finish(fixture.restore(accumulator))).containsExactly(100L, 200L);
    }

    /**
     * Tests that a {@code NULL} input under {@code RESPECT NULLS} is reported rather than silently dropped. Nulls
     * currently cannot be represented in the protobuf repeated field backing an array.
     */
    @Test
    void accumulateNullWithRespectNullsThrowsUnsupported() {
        final var accumulator = new Fixture(LONG_TYPE, false).accumulator();
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
        final Type.Record elementType = recordElementType();
        final var fixture = new Fixture(elementType, true);
        final Message first = fixture.record(elementType, 1L, 2L);
        final Message second = fixture.record(elementType, 3L, 4L);

        final var accumulator = fixture.accumulator();
        accumulator.accumulate(first);
        final var restored = fixture.restore(accumulator);
        restored.accumulate(second);

        // The wrapper the partial state is parsed against comes from the same repository the elements are converted
        // against, so a restored element is backed by the same descriptor as the original and compares equal outright.
        assertThat(finish(restored)).containsExactly(first, second);
    }

    /**
     * Tests that a restored record-typed element is backed by the very same descriptor as a freshly collected one, and
     * not by one from a repository built on the side. Only then can the enclosing {@link RecordConstructorValue} set
     * both on the same repeated field of the nullable-array wrapper without a descriptor mismatch.
     */
    @Test
    void restoredRecordElementSharesDescriptorWithFreshlyCollectedOne() {
        final Type.Record elementType = recordElementType();
        final var fixture = new Fixture(elementType, true);

        final var accumulator = fixture.accumulator();
        accumulator.accumulate(fixture.record(elementType, 1L, 2L));
        // The first element is restored from the partial state, the second is collected directly.
        final var restored = fixture.restore(accumulator);
        restored.accumulate(fixture.record(elementType, 3L, 4L));

        final List<Object> elements = finish(restored);
        assertThat(elements).hasSize(2);
        assertThat(((Message)elements.get(0)).getDescriptorForType())
                .isSameAs(((Message)elements.get(1)).getDescriptorForType());
        assertThat(((Message)elements.get(0)).getDescriptorForType())
                .isSameAs(fixture.typeRepository.getMessageDescriptor(elementType));
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
     * Tests that a value survives a round-trip through its proto representation, null treatment included. The element
     * type is not serialized: it is re-derived from the deserialized child and the null treatment, which is what the
     * result-type assertion below pins down.
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
}
