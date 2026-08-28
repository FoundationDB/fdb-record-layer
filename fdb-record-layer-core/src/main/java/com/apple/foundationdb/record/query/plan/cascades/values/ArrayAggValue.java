/*
 * ArrayAggValue.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.planprotos.PArrayAggValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An aggregate {@link Value} implementing {@code ARRAY_AGG(«expr»)}, which collects the values of its child expression
 * across a group of rows into a single {@link Type.Array}-typed value.
 *
 * <p>This implementation is <em>streaming-only</em>. It implements {@link StreamableAggregateValue}, but it
 * is intentionally not an {@link IndexableAggregateValue}, as there is no index type that materializes
 * {@code ARRAY_AGG()}.
 *
 * <p><b>Null treatment:</b> Under {@code IGNORE NULLS}, {@code NULL} inputs are skipped. Under {@code RESPECT NULLS}
 * nulls are reported as an unsupported operation, as they cannot be represented in the resulting array currently;
 * see Issue #3646. The element type of the resulting array is declared non-nullable whenever {@code IGNORE NULLS} is
 * used or the child expression type is already non-nullable; only a nullable child expression evaluated under
 * {@code RESPECT NULLS} produces a nullable element type (even though the array cannot actually hold a {@code NULL}
 * element, currently).
 *
 * <p>{@code DISTINCT} and in-call {@code ORDER BY} clauses are not supported yet.
 */
@API(API.Status.EXPERIMENTAL)
public class ArrayAggValue extends AbstractValue implements AggregateValue, StreamableAggregateValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Array-Agg-Value");

    @Nonnull
    private final Value child;

    /**
     * Whether {@code NULL} inputs are skipped ({@code IGNORE NULLS}) rather than collected ({@code RESPECT NULLS}).
     */
    private final boolean ignoreNulls;

    @Nonnull
    private final Supplier<Type.Array> resultTypeSupplier;

    /**
     * Constructs a value whose element type is derived from the child’s result type. Under {@code ignoreNulls},
     * the element type will be non-nullable, since {@code NULL} values are then skipped rather than collected.
     */
    public ArrayAggValue(@Nonnull final Value child, final boolean ignoreNulls) {
        this.child = child;
        this.ignoreNulls = ignoreNulls;
        // Note: The result type is always nullable, since ARRAY_AGG() must yield a NULL array for empty input.
        this.resultTypeSupplier = Suppliers.memoize(
                () -> new Type.Array(true, ignoreNulls ? child.getResultType().notNullable() : child.getResultType()));
    }

    /**
     * Resolves the descriptor of the message the accumulator wraps its collected elements in for serialization: a
     * message with a single repeated {@code values} field, i.e., the same wrapper a nullable array uses.
     *
     * <p>The descriptor is taken from the given plan-wide repository rather than built on the side, so that a restored
     * element is backed by the very same descriptor as a freshly collected one. The repository is guaranteed to hold
     * the wrapper type: it is built from the plan’s used types, and registering this value’s (nullable) array result
     * type registers the wrapper along with it; see {@link Type.Array#defineProtoType} and
     * {@link Type.Array#addProtoField}. A repository without it is a plan-construction bug, which the repository
     * itself reports.
     *
     * @param typeRepository the plan-wide repository to resolve the wrapper type in
     * @param elementType the type of the collected elements
     *
     * @return the descriptor of the wrapper message for {@code elementType}
     */
    @Nonnull
    private static Descriptors.Descriptor wrapperDescriptorIn(@Nonnull final TypeRepository typeRepository,
                                                             @Nonnull final Type elementType) {
        final Type.Record wrapperType = NullableArrayTypeUtils.wrapperTypeFor(elementType);
        return Verify.verifyNotNull(typeRepository.getMessageDescriptor(wrapperType));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to eval an aggregation function with eval()");
    }

    /**
     * {@inheritDoc}
     * <p>The “partial” for {@code ARRAY_AGG()} is simply the element to be collected for the current row.
     */
    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store,
                                                    @Nonnull final EvaluationContext context) {
        return child.eval(store, context);
    }

    @Nonnull
    @Override
    public Accumulator createAccumulatorWithInitialState(
            @Nonnull final TypeRepository typeRepository,
            @Nullable final List<RecordCursorProto.AccumulatorState> initialState) {
        final Type elementType = getElementType();
        final Descriptors.Descriptor wrapperDescriptor = wrapperDescriptorIn(typeRepository, elementType);
        if (initialState == null) {
            return new ArrayAccumulator(wrapperDescriptor, typeRepository, elementType, ignoreNulls);
        } else {
            Verify.verify(initialState.size() == 1);
            return new ArrayAccumulator(wrapperDescriptor, typeRepository, elementType, ignoreNulls,
                    initialState.get(0));
        }
    }

    /**
     * Returns the element type of the resulting array, i.e. the type of the collected elements. It is derived from the
     * child’s result type and the null treatment; see {@link #ArrayAggValue(Value, boolean)}.
     *
     * @return the type of the collected elements
     */
    @Nonnull
    private Type getElementType() {
        return Verify.verifyNotNull(resultTypeSupplier.get().getElementType());
    }

    /**
     * Returns the type of the resulting array. The array type is always <em>nullable</em>, since {@code ARRAY_AGG()}
     * over an empty, ungrouped input returns {@code NULL}, so the enclosing {@link RecordConstructorValue} has to be
     * able to hold {@code NULL} here. Such an array is stored in the nullable-array wrapper message, which
     * {@link RecordConstructorValue} wraps a plain {@link List} result into, in both its {@code eval()} and its
     * accumulator {@code finish()} path.
     *
     * @return the type of the resulting array
     */
    @Nonnull
    @Override
    public Type getResultType() {
        return resultTypeSupplier.get();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(child);
    }

    @Nonnull
    @Override
    public ArrayAggValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        return new ArrayAggValue(Iterables.get(newChildren, 0), ignoreNulls);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(
            @Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final ExplainTokens argument =
                new ExplainTokens().addNested(Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens());
        if (ignoreNulls) {
            // Only the non-default IGNORE NULLS treatment is spelled out, so that a plain ARRAY_AGG() remains plain
            // in the explain string.
            argument.addWhitespace().addKeyword("IGNORE").addWhitespace().addKeyword("NULLS");
        }
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("array_agg", argument));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, ignoreNulls);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, child, ignoreNulls);
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> ignoreNulls == ((ArrayAggValue)other).ignoreNulls);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PArrayAggValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PArrayAggValue.newBuilder()
                .setChild(child.toValueProto(serializationContext))
                .setIgnoreNulls(ignoreNulls)
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setArrayAggValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ArrayAggValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PArrayAggValue arrayAggValueProto) {
        final Value child = Value.fromValueProto(serializationContext,
                Objects.requireNonNull(arrayAggValueProto.getChild()));
        return new ArrayAggValue(child, arrayAggValueProto.getIgnoreNulls());
    }

    /**
     * The {@code ARRAY_AGG(«expr»)} aggregation function.
     *
     * <p>Note that this function takes a second argument, which is not a user-facing argument. It carries the null
     * treatment resolved from the call’s {@code {IGNORE|RESPECT} NULLS} clause as a boolean literal, and is consumed
     * during encapsulation rather than passed as a child to the resulting {@link ArrayAggValue}.
     */
    @AutoService(BuiltInFunction.class)
    @SuppressWarnings("PMD.UnusedFormalParameter")
    public static class ArrayAggFn extends BuiltInFunction<AggregateValue> {
        public ArrayAggFn() {
            super("ARRAY_AGG",
                    ImmutableList.of(new Type.Any(), Type.primitiveType(Type.TypeCode.BOOLEAN)),
                    ArrayAggFn::encapsulate);
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull final BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final CallSiteArguments callSiteArguments) {
            final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
            Verify.verify(arguments.size() == 2);
            final Typed arg0 = arguments.get(0);
            final Typed arg1 = arguments.get(1);
            if (!(arg1 instanceof LiteralValue<?> nullTreatment)
                    || !(nullTreatment.getLiteralValue() instanceof Boolean ignoreNulls)) {
                throw new RecordCoreException("null treatment must be a boolean literal");
            }
            // Reject an argument whose type cannot be determined, such as an untyped NULL.
            SemanticException.check(!arg0.getResultType().isUnresolved(),
                    SemanticException.ErrorCode.UNKNOWN_TYPE,
                    "Cannot resolve the argument type of ARRAY_AGG()");
            return new ArrayAggValue((Value)arg0, ignoreNulls);
        }
    }

    /**
     * Accumulator that collects elements into a growing list. Partial state is serialized for continuations by
     * wrapping the collected elements in a nullable-array protobuf wrapper message (a message with a single repeated
     * {@code values} field) and stashing its bytes in the {@code bytes} slot of an
     * {@link RecordCursorProto.AccumulatorState}.
     *
     * <p>Elements are converted from their runtime representation to protobuf via
     * {@link RecordConstructorValue#deepCopyIfNeeded}. Since the collected elements have to be serialized
     * into the wrapper message anyway, holding them in that form keeps a restored element indistinguishable from a
     * freshly collected one — the wrapper descriptor is resolved from the same plan-wide {@link TypeRepository} the
     * elements are converted against, so both are backed by the very same descriptors. That lets {@link #finish()}
     * hand its result straight to the enclosing {@link RecordConstructorValue}, whose accumulator path does not do any
     * conversion.
     */
    static final class ArrayAccumulator implements Accumulator {
        @Nonnull
        private final TypeRepository typeRepository;
        @Nonnull
        private final Type elementType;
        @Nonnull
        private final Descriptors.Descriptor wrapperDescriptor;
        @Nonnull
        private final Descriptors.FieldDescriptor valuesField;
        @Nonnull
        private final List<Object> elements;

        private final boolean ignoreNulls;

        /**
         * Whether any input row was seen for the current group. This is independent of {@code NULL}-skipping. A group
         * consisting solely of {@code NULL}s has still “seen” rows and must emit an (empty) array, whereas an empty
         * group must produce no state so that {@code AggregateCursor.isNoRecords()} can detect an empty scan (it
         * treats a null {@code PartialAggregationResult}, i.e. an empty {@link #getAccumulatorStates()}, as
         * “no records”).
         */
        private boolean seenAnyRow;

        /**
         * Creates an accumulator for a group that has not seen any rows yet.
         *
         * @param wrapperDescriptor descriptor of the message the partial state is serialized through, as built by
         *        {@link #wrapperDescriptorIn}
         * @param typeRepository the type repository the collected elements are converted against
         * @param elementType the type of the collected elements
         * @param ignoreNulls whether {@code NULL} inputs are skipped rather than collected
         */
        ArrayAccumulator(@Nonnull final Descriptors.Descriptor wrapperDescriptor,
                         @Nonnull final TypeRepository typeRepository,
                         @Nonnull final Type elementType,
                         final boolean ignoreNulls) {
            this.typeRepository = typeRepository;
            this.elementType = elementType;
            this.wrapperDescriptor = wrapperDescriptor;
            this.valuesField = Verify.verifyNotNull(
                    wrapperDescriptor.findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()));
            this.elements = new ArrayList<>();
            this.ignoreNulls = ignoreNulls;
            this.seenAnyRow = false;
        }

        /**
         * Creates an accumulator that resumes a group from the partial state carried in a continuation, as produced by
         * {@link #getAccumulatorStates()}.
         *
         * @param wrapperDescriptor descriptor of the message the partial state is serialized through, as built by
         *        {@link #wrapperDescriptorIn}
         * @param typeRepository the type repository the collected elements are converted against
         * @param elementType the type of the collected elements
         * @param ignoreNulls whether {@code NULL} inputs are skipped rather than collected
         * @param initialState the partial state to restore
         */
        ArrayAccumulator(@Nonnull final Descriptors.Descriptor wrapperDescriptor,
                         @Nonnull final TypeRepository typeRepository,
                         @Nonnull final Type elementType,
                         final boolean ignoreNulls,
                         @Nonnull final RecordCursorProto.AccumulatorState initialState) {
            this(wrapperDescriptor, typeRepository, elementType, ignoreNulls);
            Verify.verify(initialState.getStateList().size() == 1);
            Verify.verify(initialState.getState(0).hasBytesState());
            try {
                final Message wrapper =
                        DynamicMessage.parseFrom(wrapperDescriptor, initialState.getState(0).getBytesState());
                @SuppressWarnings("unchecked")
                final List<Object> restored = (List<Object>)wrapper.getField(valuesField);
                this.elements.addAll(restored);
                // Non-null restored state means the group had already seen at least one row.
                this.seenAnyRow = true;
            } catch (final InvalidProtocolBufferException e) {
                throw new RecordCoreException("unable to deserialize array_agg accumulator state", e);
            }
        }

        /**
         * Collects the given element.
         *
         * @param currentObject the element to collect, or {@code null}
         */
        @Override
        public void accumulate(@Nullable final Object currentObject) {
            seenAnyRow = true;
            if (currentObject == null) {
                // Under IGNORE NULLS a NULL is simply dropped. Under RESPECT NULLS (the SQL default) it would have to
                // be collected. However, nulls are not representable currently.
                SemanticException.check(ignoreNulls, SemanticException.ErrorCode.UNSUPPORTED,
                        "An ARRAY value cannot have NULL elements");
                return;
            }
            // Use `deepCopyIfNeeded()` to ensure that the element will be held in its protobuf representation.
            final Object element = Verify.verifyNotNull(
                    RecordConstructorValue.deepCopyIfNeeded(typeRepository, elementType, currentObject));
            elements.add(element);
        }

        /**
         * Returns the collected elements as a plain {@link List}, which is how an array is represented at runtime. The
         * elements themselves are in their protobuf representation.
         *
         * @return the collected elements
         */
        @Nullable
        @Override
        public Object finish() {
            return ImmutableList.copyOf(elements);
        }

        /**
         * Serializes the elements collected so far, so that the group can be resumed from a continuation. The elements
         * are wrapped in the nullable-array wrapper message and its bytes are stashed in the {@code bytes} slot of a
         * single {@link RecordCursorProto.AccumulatorState}, as {@code OneOfTypedState} has no list-shaped slot.
         *
         * <p>A group that has not seen any rows reports no state at all; see {@link #seenAnyRow}.
         *
         * @return the partial state of this accumulator, or an empty list if the group has seen no rows
         */
        @Nonnull
        @Override
        public List<RecordCursorProto.AccumulatorState> getAccumulatorStates() {
            if (!seenAnyRow) {
                return List.of();
            }
            final ByteString bytes
                    = MessageHelpers.wrapNullableArray(wrapperDescriptor, valuesField, elements).toByteString();
            return List.of(RecordCursorProto.AccumulatorState.newBuilder()
                    .addState(RecordCursorProto.OneOfTypedState.newBuilder().setBytesState(bytes))
                    .build());
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PArrayAggValue, ArrayAggValue> {
        @Nonnull
        @Override
        public Class<PArrayAggValue> getProtoMessageClass() {
            return PArrayAggValue.class;
        }

        @Nonnull
        @Override
        public ArrayAggValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PArrayAggValue arrayAggValueProto) {
            return ArrayAggValue.fromProto(serializationContext, arrayAggValueProto);
        }
    }
}
