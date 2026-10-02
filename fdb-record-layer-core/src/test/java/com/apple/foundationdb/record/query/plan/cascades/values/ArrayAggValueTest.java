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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.google.common.base.VerifyException;
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

        private Fixture(@Nonnull final Type elementType, final boolean ignoreNulls, final int limit) {
            // The element type is derived from the child, and `notNullable()` is a no-op for the non-nullable types
            // used here, so the child type doubles as the element type.
            this.value = new ArrayAggValue(new LiteralValue<>(elementType, null), ignoreNulls, limit, null);
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
        final var accumulator = new Fixture(LONG_TYPE, true, ArrayAggValue.NO_LIMIT).accumulator();

        assertThat(accumulator.getAccumulatorStates()).isEmpty();
        assertThat(finish(accumulator)).isEmpty();
    }

    /**
     * Tests that a group which saw rows, all of them {@code NULL}, is distinct from a group that saw no rows: it
     * reports state, and it aggregates to an empty array rather than to {@code NULL}.
     */
    @Test
    void getAccumulatorStatesWithOnlyNullRowsReturnsState() {
        final var fixture = new Fixture(LONG_TYPE, true, ArrayAggValue.NO_LIMIT);
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
        final var fixture = new Fixture(LONG_TYPE, true, ArrayAggValue.NO_LIMIT);
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
        final var fixture = new Fixture(LONG_TYPE, true, ArrayAggValue.NO_LIMIT);
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
        final var fixture = new Fixture(LONG_TYPE, true, ArrayAggValue.NO_LIMIT);
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
        final var accumulator = new Fixture(LONG_TYPE, false, ArrayAggValue.NO_LIMIT).accumulator();
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
        final var fixture = new Fixture(elementType, true, ArrayAggValue.NO_LIMIT);
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
        final var fixture = new Fixture(elementType, true, ArrayAggValue.NO_LIMIT);

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

        final Type respectNullsType = new ArrayAggValue(child, false, ArrayAggValue.NO_LIMIT, null).getResultType();
        assertThat(respectNullsType.isNullable()).isTrue();
        assertThat(((Type.Array)respectNullsType).getElementType()).isEqualTo(nullableLong);

        final Type ignoreNullsType = new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null).getResultType();
        assertThat(ignoreNullsType.isNullable()).isTrue();
        assertThat(((Type.Array)ignoreNullsType).getElementType()).isEqualTo(nullableLong.notNullable());
    }

    /**
     * Tests that the value cannot be evaluated row-wise. It is an aggregate, so it is evaluated through an accumulator
     * and {@code evalToPartial()} instead.
     */
    @Test
    void evalThrows() {
        final ArrayAggValue value = new ArrayAggValue(new LiteralValue<>(LONG_TYPE, 1L), true, ArrayAggValue.NO_LIMIT, null);

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
        final ArrayAggValue ignoreNulls = new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null);
        final ArrayAggValue respectNulls = new ArrayAggValue(child, false, ArrayAggValue.NO_LIMIT, null);

        assertThat(ignoreNulls).isNotEqualTo(respectNulls);
        assertThat(ignoreNulls).isEqualTo(new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null));
        assertThat(ignoreNulls.hashCode()).isEqualTo(new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null).hashCode());
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
            final ArrayAggValue value = new ArrayAggValue(child, ignoreNulls, ArrayAggValue.NO_LIMIT, null);

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
        final ArrayAggValue value = new ArrayAggValue(new LiteralValue<>(LONG_TYPE, 1L), true, ArrayAggValue.NO_LIMIT, null);
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
        final Value noLimit = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), ArrayAggValue.NO_LIMIT);

        assertThatThrownBy(() -> new ArrayAggValue.ArrayAggFn()
                .encapsulate(CallSiteArguments.ofPositional(child, notABooleanLiteral, noLimit)))
                .isInstanceOf(RecordCoreException.class);
    }

    /**
     * Tests that the function rejects a limit argument that is not an integer literal, as the grammar is supposed to
     * guarantee it.
     */
    @Test
    void encapsulateRejectsNonIntegerLimit() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value ignoreNulls = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
        final Value notAnIntegerLiteral = new LiteralValue<>(LONG_TYPE, 42L);

        assertThatThrownBy(() -> new ArrayAggValue.ArrayAggFn()
                .encapsulate(CallSiteArguments.ofPositional(child, ignoreNulls, notAnIntegerLiteral)))
                .isInstanceOf(RecordCoreException.class);
    }

    /**
     * Tests that an in-call limit caps the collected elements, and that the cap survives a round-trip.
     */
    @Test
    void accumulateWithLimitCapsElements() {
        final var fixture = new Fixture(LONG_TYPE, true, 2);
        final var accumulator = fixture.accumulator();
        accumulator.accumulate(100L);
        accumulator.accumulate(200L);
        accumulator.accumulate(300L);

        assertThat(finish(accumulator)).containsExactly(100L, 200L);
        assertThat(finish(fixture.restore(accumulator))).containsExactly(100L, 200L);
    }

    /**
     * Tests that the cap is a total for the group rather than a per-continuation budget. A resumed accumulator starts
     * out already at the cap, so it keeps discarding instead of collecting a further {@code limit} elements.
     */
    @Test
    void accumulateAfterRestoringStateStillRespectsLimit() {
        final var fixture = new Fixture(LONG_TYPE, true, 2);
        final var accumulator = fixture.accumulator();
        accumulator.accumulate(100L);
        accumulator.accumulate(200L);

        final var restored = fixture.restore(accumulator);
        restored.accumulate(300L);
        restored.accumulate(400L);

        assertThat(finish(restored)).containsExactly(100L, 200L);
    }

    /**
     * Tests that a limit of 0 collects nothing, yet still reports a non-empty state, so that the group remains
     * distinguishable from an empty one.
     */
    @Test
    void accumulateWithZeroLimitCollectsNothing() {
        final var fixture = new Fixture(LONG_TYPE, true, 0);
        final var accumulator = fixture.accumulator();
        accumulator.accumulate(100L);

        assertThat(finish(accumulator)).isEmpty();
        assertThat(accumulator.getAccumulatorStates()).isNotEmpty();
    }

    /**
     * Tests that a {@code NULL} arriving once the cap is reached is dropped rather than reported as unsupported, even
     * under {@code RESPECT NULLS}.
     */
    @Test
    void accumulateNullBeyondLimitIsDropped() {
        final var fixture = new Fixture(LONG_TYPE, false, 1);
        final var accumulator = fixture.accumulator();
        accumulator.accumulate(100L);
        accumulator.accumulate(null);

        assertThat(finish(accumulator)).containsExactly(100L);
    }

    /**
     * Tests that the limit survives serialization, and that a plan serialized without the field is uncapped.
     */
    @Test
    void limitSurvivesSerialization() {
        final ArrayAggValue value = new ArrayAggValue(new LiteralValue<>(LONG_TYPE, null), true, 7, null);
        final PlanSerializationContext serializationContext = PlanSerializationContext.newForCurrentMode();
        assertThat(ArrayAggValue.fromProto(serializationContext, value.toProto(serializationContext)))
                .isEqualTo(value);
    }

    /**
     * Tests that the sort keys of an in-call {@code ORDER BY} clause are held as one further child, following the
     * aggregated expression, and in the declared order.
     */
    @Test
    void sortKeysAreChildrenFollowingTheAggregatedExpression() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value first = new LiteralValue<>(LONG_TYPE, 2L);
        final Value second = new LiteralValue<>(LONG_TYPE, 3L);
        final List<SortKeysValue.SortKey> sortKeys =
                ImmutableList.of(new SortKeysValue.SortKey(first, RequestedSortOrder.ASCENDING),
                        new SortKeysValue.SortKey(second, RequestedSortOrder.DESCENDING));
        final ArrayAggValue value = new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, new SortKeysValue(sortKeys));

        assertThat(ImmutableList.<Value>copyOf(value.getChildren()))
                .containsExactly(child, new SortKeysValue(sortKeys));
        assertThat(value.getSortKeys()).isEqualTo(sortKeys);
        // The sort keys do not influence the result type, which is derived from the aggregated expression alone.
        assertThat(value.getResultType()).isEqualTo(new Type.Array(true, LONG_TYPE));
    }

    /**
     * Tests that an {@code ARRAY_AGG()} without an in-call {@code ORDER BY} clause holds no sort keys child at all,
     * which keeps its children as they were before the clause existed.
     */
    @Test
    void withoutSortKeysTheAggregatedExpressionIsTheOnlyChild() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final ArrayAggValue value = new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null);

        assertThat(ImmutableList.<Value>copyOf(value.getChildren())).containsExactly(child);
        assertThat(value.getSortKeys()).isEmpty();
    }

    /**
     * Tests that two values differing only in the sort order of a sort key are neither equal nor plan-hash equally.
     */
    @Test
    void equalsAndHashCodeAccountForSortOrder() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value sortKey = new LiteralValue<>(LONG_TYPE, 2L);
        final ArrayAggValue ascending = arrayAggOrderedBy(child, sortKey, RequestedSortOrder.ASCENDING);
        final ArrayAggValue descending = arrayAggOrderedBy(child, sortKey, RequestedSortOrder.DESCENDING);

        assertThat(ascending).isNotEqualTo(descending);
        assertThat(ascending).isNotEqualTo(new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null));
        assertThat(ascending).isEqualTo(arrayAggOrderedBy(child, sortKey, RequestedSortOrder.ASCENDING));
        assertThat(ascending.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isNotEqualTo(descending.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    /**
     * Tests that the sort keys, values and sort orders alike, survive a round-trip through the proto representation.
     */
    @Test
    void sortKeysSurviveSerialization() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        for (final RequestedSortOrder sortOrder : List.of(RequestedSortOrder.ASCENDING, RequestedSortOrder.DESCENDING,
                RequestedSortOrder.ASCENDING_NULLS_LAST, RequestedSortOrder.DESCENDING_NULLS_FIRST)) {
            final ArrayAggValue value = arrayAggOrderedBy(child, new LiteralValue<>(LONG_TYPE, 2L), sortOrder);
            final PlanSerializationContext serializationContext = PlanSerializationContext.newForCurrentMode();
            final ArrayAggValue deserialized =
                    ArrayAggValue.fromProto(serializationContext, value.toProto(serializationContext));

            assertThat(deserialized).isEqualTo(value);
            assertThat(deserialized.getSortKeys()).isEqualTo(value.getSortKeys());
        }
    }

    /**
     * Tests that replacing the children replaces both the aggregated expression and the sort keys child.
     */
    @Test
    void withChildrenReplacesSortKeyValues() {
        final Value sortKey = new LiteralValue<>(LONG_TYPE, 2L);
        final ArrayAggValue value =
                arrayAggOrderedBy(new LiteralValue<>(LONG_TYPE, 1L), sortKey, RequestedSortOrder.DESCENDING);
        final Value newChild = new LiteralValue<>(LONG_TYPE, 10L);
        final Value newSortKey = new LiteralValue<>(LONG_TYPE, 20L);
        final SortKeysValue newSortKeysValue =
                new SortKeysValue(ImmutableList.of(new SortKeysValue.SortKey(newSortKey,
                        RequestedSortOrder.DESCENDING)));

        final ArrayAggValue withNewChildren = value.withChildren(ImmutableList.of(newChild, newSortKeysValue));

        assertThat(ImmutableList.<Value>copyOf(withNewChildren.getChildren()))
                .containsExactly(newChild, newSortKeysValue);
        assertThat(withNewChildren.getSortKeys())
                .containsExactly(new SortKeysValue.SortKey(newSortKey, RequestedSortOrder.DESCENDING));
    }

    /**
     * Tests that the function resolves the trailing sort keys argument of an in-call {@code ORDER BY} clause.
     */
    @Test
    void encapsulateResolvesSortKeys() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value ignoreNulls = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
        final Value noLimit = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), ArrayAggValue.NO_LIMIT);
        final Value sortKey = new LiteralValue<>(LONG_TYPE, 2L);
        final SortKeysValue sortKeysValue =
                new SortKeysValue(ImmutableList.of(new SortKeysValue.SortKey(sortKey,
                        RequestedSortOrder.DESCENDING)));

        final Typed value = new ArrayAggValue.ArrayAggFn()
                .encapsulate(CallSiteArguments.ofPositional(child, ignoreNulls, noLimit, sortKeysValue));

        assertThat(value).isEqualTo(arrayAggOrderedBy(child, sortKey, RequestedSortOrder.DESCENDING));
    }

    /**
     * Tests that the function rejects a trailing argument that is not a bundle of sort keys, as the front end is
     * supposed to pass the whole in-call {@code ORDER BY} clause as one.
     */
    @Test
    void encapsulateRejectsNonSortKeysTrailingArgument() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value ignoreNulls = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
        final Value noLimit = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), ArrayAggValue.NO_LIMIT);
        final Value bareSortKey = new LiteralValue<>(LONG_TYPE, 2L);

        assertThatThrownBy(() -> new ArrayAggValue.ArrayAggFn()
                .encapsulate(CallSiteArguments.ofPositional(child, ignoreNulls, noLimit, bareSortKey)))
                .isInstanceOf(RecordCoreException.class);
    }

    /**
     * Tests that the common sort keys of several aggregates are the ones they agree on, and that aggregates without an
     * in-call {@code ORDER BY} clause do not interfere.
     */
    @Test
    void commonSortKeysOfIgnoresUnorderedAggregates() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value sortKey = new LiteralValue<>(LONG_TYPE, 2L);
        final ArrayAggValue ordered = arrayAggOrderedBy(child, sortKey, RequestedSortOrder.ASCENDING);
        final ArrayAggValue unordered = new ArrayAggValue(new LiteralValue<>(LONG_TYPE, 3L), true,
                ArrayAggValue.NO_LIMIT, null);

        assertThat(SortKeysValue.commonSortKeysOf(RecordConstructorValue.ofUnnamed(ImmutableList.of(unordered))))
                .isEmpty();
        assertThat(SortKeysValue.commonSortKeysOf(
                RecordConstructorValue.ofUnnamed(ImmutableList.of(ordered, unordered))))
                .contains(new SortKeysValue(ordered.getSortKeys()));
    }

    /**
     * Tests that two aggregates demanding different orderings are reported as a conflict, since one ordered stream
     * cannot serve both.
     */
    @Test
    void commonSortKeysOfRejectsConflictingOrderings() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final Value sortKey = new LiteralValue<>(LONG_TYPE, 2L);
        final Value aggregates = RecordConstructorValue.ofUnnamed(ImmutableList.of(
                arrayAggOrderedBy(child, sortKey, RequestedSortOrder.ASCENDING),
                arrayAggOrderedBy(child, sortKey, RequestedSortOrder.DESCENDING)));

        assertThatThrownBy(() -> SortKeysValue.commonSortKeysOf(aggregates))
                .isInstanceOf(RecordCoreException.class);
    }

    /**
     * Tests that the explain string spells out the in-call {@code ORDER BY} clause, with an arrow per sort key.
     */
    @Test
    void explainRendersSortKeys() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final SortKeysValue sortKeysValue = new SortKeysValue(ImmutableList.of(
                new SortKeysValue.SortKey(new LiteralValue<>(LONG_TYPE, 2L), RequestedSortOrder.ASCENDING),
                new SortKeysValue.SortKey(new LiteralValue<>(LONG_TYPE, 3L), RequestedSortOrder.DESCENDING)));
        final ArrayAggValue value = new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, sortKeysValue);

        assertThat(explain(value)).isEqualTo("array_agg(1l IGNORE NULLS ORDER BY 2l ↑, 3l ↓)");
    }

    /**
     * Tests that an {@code ARRAY_AGG()} without the clause explains exactly as it did before the clause existed, which
     * matters because the explain strings are asserted all over the yamsql suites.
     */
    @Test
    void explainWithoutSortKeysIsUnchanged() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);

        assertThat(explain(new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null)))
                .isEqualTo("array_agg(1l IGNORE NULLS)");
        assertThat(explain(new ArrayAggValue(child, false, 2, null))).isEqualTo("array_agg(1l LIMIT 2)");
    }

    /**
     * Tests that {@code withChildren} insists on a child count matching the presence of the sort keys, rather than
     * silently dropping or inventing them.
     */
    @Test
    void withChildrenRejectsMismatchedChildCount() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final ArrayAggValue unordered = new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT, null);
        final ArrayAggValue ordered = arrayAggOrderedBy(child, new LiteralValue<>(LONG_TYPE, 2L),
                RequestedSortOrder.ASCENDING);

        assertThatThrownBy(() -> unordered.withChildren(ImmutableList.of(child, child)))
                .isInstanceOf(VerifyException.class);
        assertThatThrownBy(() -> ordered.withChildren(ImmutableList.of(child)))
                .isInstanceOf(VerifyException.class);
    }

    /**
     * Tests that {@code withChildren} rejects a second child that is not a bundle of sort keys.
     */
    @Test
    void withChildrenRejectsNonSortKeysSecondChild() {
        final Value child = new LiteralValue<>(LONG_TYPE, 1L);
        final ArrayAggValue ordered = arrayAggOrderedBy(child, new LiteralValue<>(LONG_TYPE, 2L),
                RequestedSortOrder.ASCENDING);

        assertThatThrownBy(() -> ordered.withChildren(ImmutableList.of(child, child)))
                .isInstanceOf(ClassCastException.class);
    }

    @Nonnull
    private static String explain(@Nonnull final Value value) {
        return value.explain().getExplainTokens()
                .render(DefaultExplainFormatter.forDebugging()).toString();
    }

    @Nonnull
    private static ArrayAggValue arrayAggOrderedBy(@Nonnull final Value child, @Nonnull final Value sortKey,
                                                   @Nonnull final RequestedSortOrder sortOrder) {
        return new ArrayAggValue(child, true, ArrayAggValue.NO_LIMIT,
                new SortKeysValue(ImmutableList.of(new SortKeysValue.SortKey(sortKey, sortOrder))));
    }
}
