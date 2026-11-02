/*
 * ExplodeValue.java
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
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PExplodeValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * A {@link StreamingValue} that iterates over the elements of an array. For every element it produces a struct whose
 * first anonymous field is the element. If the explode is {@linkplain #hasOrdinality() with ordinality}, the struct has
 * a second anonymous field that holds the 0-based ordinal of the element within the array, akin to SQL's
 * {@code UNNEST ... WITH ORDINALITY}.
 * <p>
 * An explode without ordinality does not expose the positions of the elements, so its collection can be replaced by
 * any permutation of it, for example by a sorted one.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ExplodeValue extends AbstractValue implements StreamingValue, CreatesDynamicTypesValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Explode-Value");

    /**
     * The name of the field of the struct an explode produces that holds the element.
     */
    @Nonnull
    public static final String ELEMENT_FIELD_NAME = "_0";

    /**
     * The type of the ordinal.
     */
    @Nonnull
    private static final Type ORDINAL_TYPE = Type.primitiveType(Type.TypeCode.INT, false);

    /**
     * The collection value. Must evaluate to an array type.
     */
    @Nonnull
    private final Value collectionValue;

    /**
     * The element type of the collection.
     */
    @Nonnull
    private final Type elementType;

    /**
     * Whether the struct this explode produces carries the ordinal of the element.
     */
    private final boolean withOrdinality;

    /**
     * The alias the element being produced is bound to while its struct is evaluated.
     */
    @Nonnull
    private final CorrelationIdentifier elementAlias;

    /**
     * The alias the ordinal of the element being produced is bound to while its struct is evaluated. Unused if this
     * explode is not with ordinality.
     */
    @Nonnull
    private final CorrelationIdentifier ordinalAlias;

    /**
     * The struct produced per element: a record constructor of the element and, if with ordinality, its ordinal.
     */
    @Nonnull
    private final RecordConstructorValue structValue;

    public ExplodeValue(@Nonnull final Value collectionValue) {
        this(collectionValue, false);
    }

    public ExplodeValue(@Nonnull final Value collectionValue, final boolean withOrdinality) {
        Verify.verify(collectionValue.getResultType().isArray());
        this.collectionValue = collectionValue;
        this.withOrdinality = withOrdinality;
        this.elementType = Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType());
        this.elementAlias = CorrelationIdentifier.uniqueId();
        this.ordinalAlias = CorrelationIdentifier.uniqueId();
        final ImmutableList.Builder<Column<? extends Value>> columnsBuilder = ImmutableList.builder();
        columnsBuilder.add(Column.unnamedOf(QuantifiedObjectValue.of(elementAlias, elementType)));
        if (withOrdinality) {
            columnsBuilder.add(Column.unnamedOf(QuantifiedObjectValue.of(ordinalAlias, ORDINAL_TYPE)));
        }
        this.structValue = RecordConstructorValue.ofColumns(columnsBuilder.build(), false);
        Verify.verify(ELEMENT_FIELD_NAME.equals(structValue.getResultType().getField(0).getFieldName()));
    }

    @Nonnull
    public Value getCollectionValue() {
        return collectionValue;
    }

    public boolean hasOrdinality() {
        return withOrdinality;
    }

    /**
     * Returns an explode of a different collection that is with ordinality if and only if this one is.
     * @param newCollectionValue the new collection
     * @return an explode of the new collection
     */
    @Nonnull
    public ExplodeValue withCollectionValue(@Nonnull final Value newCollectionValue) {
        return new ExplodeValue(newCollectionValue, withOrdinality);
    }

    @Nonnull
    public Type getElementType() {
        return elementType;
    }

    /**
     * Returns the element of a struct produced by an explode.
     *
     * @param explodedValue a value of the {@linkplain #getResultType() result type} of an explode, typically the
     *        flowed object value of a quantifier ranging over the explode
     * @return the element the struct carries
     */
    @Nonnull
    public static Value elementValue(@Nonnull final Value explodedValue) {
        return FieldValue.ofOrdinalNumberAndFuseIfPossible(explodedValue, 0);
    }

    /**
     * Returns the 0-based ordinal of a struct produced by an explode {@linkplain #hasOrdinality() with ordinality}.
     *
     * @param explodedValue a value of the {@linkplain #getResultType() result type} of an explode with ordinality,
     *        typically the flowed object value of a quantifier ranging over the explode
     * @return the ordinal the struct carries
     */
    @Nonnull
    public static Value ordinalValue(@Nonnull final Value explodedValue) {
        return FieldValue.ofOrdinalNumberAndFuseIfPossible(explodedValue, 1);
    }

    @Nonnull
    @Override
    public Type.Record getResultType() {
        return structValue.getResultType();
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> evalAsStream(@Nonnull final FDBRecordStoreBase<M> store,
                                                                      @Nonnull final EvaluationContext context,
                                                                      @Nullable final byte[] continuation,
                                                                      @Nonnull final ExecuteProperties executeProperties) {
        return new Cursor(store.getExecutor(), elementsOf(store, context),
                (elements, ordinal) -> structAt(context, elements, ordinal), continuation)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    /**
     * Evaluates this explode eagerly, producing the structs {@link #evalAsStream} would stream.
     * @param store the record store, may be {@code null} if the collection does not need one
     * @param context the evaluation context
     * @return the structs of the elements of the collection, in the order of the collection
     */
    @Nonnull
    public List<Object> evalAsList(@Nullable final FDBRecordStoreBase<?> store, @Nonnull final EvaluationContext context) {
        final List<?> elements = elementsOf(store, context);
        return IntStream.range(0, elements.size())
                .mapToObj(ordinal -> structAt(context, elements, ordinal))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Evaluates the collection. A {@code null} collection has no elements.
     */
    @Nonnull
    private List<?> elementsOf(@Nullable final FDBRecordStoreBase<?> store, @Nonnull final EvaluationContext context) {
        final Object result = collectionValue.eval(store, context);
        return result == null ? List.of() : (List<?>)result;
    }

    @Nonnull
    private Object structAt(@Nonnull final EvaluationContext context, @Nonnull final List<?> elements, final int ordinal) {
        return structOf(context, Verify.verifyNotNull(elements.get(ordinal), "array elements must be non-null"), ordinal);
    }

    @Nonnull
    private Object structOf(@Nonnull final EvaluationContext context, @Nonnull final Object element, final int ordinal) {
        EvaluationContext elementContext = context
                .withBinding(Bindings.Internal.CORRELATION, elementAlias,
                        QueryResult.ofComputed(coerceElement(context.getTypeRepository(), element)));
        if (withOrdinality) {
            elementContext = elementContext.withBinding(Bindings.Internal.CORRELATION, ordinalAlias,
                    QueryResult.ofComputed(ordinal));
        }
        return Objects.requireNonNull(structValue.eval(null, elementContext));
    }

    /**
     * Coerces an element to the representation of the element type in the struct. Elements of literal lists, such
     * as the lists of IN predicates, are plain Java objects that do not necessarily use that representation: numbers
     * may be narrower than the element type, and enums are Java enums or {@link ProtoUtils.DynamicEnum}s rather than
     * enum value descriptors.
     */
    @Nonnull
    private Object coerceElement(@Nonnull final TypeRepository typeRepository, @Nonnull final Object element) {
        final Type elementType = structValue.getResultType().getField(0).getFieldType();
        if (elementType instanceof Type.Enum) {
            if (element instanceof ProtocolMessageEnum) {
                return ((ProtocolMessageEnum)element).getValueDescriptor();
            }
            if (element instanceof ProtoUtils.DynamicEnum) {
                return Objects.requireNonNull(typeRepository.getEnumValue(typeRepository.getProtoTypeName(elementType),
                        ((ProtoUtils.DynamicEnum)element).getName()));
            }
            return element;
        }
        if (element instanceof final Number number && elementType.isPrimitive()) {
            return switch (elementType.getTypeCode()) {
                case LONG -> element instanceof Long ? element : (Object)number.longValue();
                case DOUBLE -> element instanceof Double ? element : (Object)number.doubleValue();
                case FLOAT -> element instanceof Float ? element : (Object)number.floatValue();
                default -> element;
            };
        }
        return element;
    }

    @Nonnull
    @Override
    public CardinalitiesProperty.Cardinalities getCardinalities() {
        final int size;
        if (collectionValue instanceof AbstractArrayConstructorValue) {
            size = Iterables.size(collectionValue.getChildren());
        } else if (collectionValue instanceof LiteralValue<?> &&
                ((LiteralValue<?>)collectionValue).getLiteralValue() instanceof List<?>) {
            size = ((List<?>)((LiteralValue<?>)collectionValue).getLiteralValue()).size();
        } else {
            return CardinalitiesProperty.Cardinalities.unknownMaxCardinality();
        }
        final var cardinality = CardinalitiesProperty.Cardinality.ofCardinality(size);
        return new CardinalitiesProperty.Cardinalities(cardinality, cardinality);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to eval an streaming value with eval()");
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(collectionValue);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        final Value newCollectionValue = Iterables.getOnlyElement(newChildren);
        if (newCollectionValue == collectionValue) {
            return this;
        }
        return withCollectionValue(newCollectionValue);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("explode",
                Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens()));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, withOrdinality);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, collectionValue, withOrdinality);
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> withOrdinality == ((ExplodeValue)other).hasOrdinality());
    }

    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public PExplodeValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PExplodeValue.newBuilder()
                .setCollectionValue(collectionValue.toValueProto(serializationContext))
                .setWithOrdinality(withOrdinality)
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setExplodeValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ExplodeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PExplodeValue explodeValueProto) {
        return new ExplodeValue(Value.fromValueProto(serializationContext,
                Objects.requireNonNull(explodeValueProto.getCollectionValue())), explodeValueProto.getWithOrdinality());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PExplodeValue, ExplodeValue> {
        @Nonnull
        @Override
        public Class<PExplodeValue> getProtoMessageClass() {
            return PExplodeValue.class;
        }

        @Nonnull
        @Override
        public ExplodeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PExplodeValue explodeValueProto) {
            return ExplodeValue.fromProto(serializationContext, explodeValueProto);
        }
    }

    /**
     * A cursor over the elements of an array, producing a struct of each element and its ordinal. A struct is only
     * created once the cursor reaches its element. The continuation records the position of the next element.
     */
    public static class Cursor implements RecordCursor<QueryResult> {
        @Nonnull
        private final Executor executor;

        @Nonnull
        private final List<?> elements;

        /**
         * Creates the struct of the element at an ordinal of the array, and that ordinal.
         */
        @Nonnull
        private final BiFunction<List<?>, Integer, Object> structCreator;

        private int nextPosition; // position of the next element to return

        private boolean closed = false;

        Cursor(@Nonnull final Executor executor, @Nonnull final List<?> elements,
                @Nonnull final BiFunction<List<?>, Integer, Object> structCreator, @Nullable final byte[] continuation) {
            this(executor, elements, structCreator,
                    continuation == null ? 0 : Continuation.from(continuation).getNextPosition());
        }

        private Cursor(@Nonnull final Executor executor, @Nonnull final List<?> elements,
                       @Nonnull final BiFunction<List<?>, Integer, Object> structCreator, final int nextPosition) {
            if (nextPosition < 0 || nextPosition > elements.size()) {
                throw new RecordCoreException("invalid continuation position for explode")
                        .addLogInfo("nextPosition", nextPosition)
                        .addLogInfo("arraySize", elements.size());
            }
            this.executor = executor;
            this.elements = elements;
            this.structCreator = structCreator;
            this.nextPosition = nextPosition;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
            return CompletableFuture.completedFuture(getNext());
        }

        @Nonnull
        @Override
        public RecordCursorResult<QueryResult> getNext() {
            if (nextPosition >= elements.size()) {
                return RecordCursorResult.exhausted();
            }
            final int ordinal = nextPosition;
            nextPosition++;
            return RecordCursorResult.withNextValue(
                    QueryResult.ofComputed(structCreator.apply(elements, ordinal)),
                    new Continuation(nextPosition));
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
            visitor.visitEnter(this);
            return visitor.visitLeave(this);
        }

        @Override
        @Nonnull
        public Executor getExecutor() {
            return executor;
        }

        private static class Continuation implements RecordCursorContinuation {
            private final int nextPosition;

            Continuation(final int nextPosition) {
                this.nextPosition = nextPosition;
            }

            @Override
            public boolean isEnd() {
                // A continuation returned alongside a value must not be an end continuation, per the contract of
                // RecordCursorResult, even if the cursor knows there is nothing after that value. Exhaustion is
                // signalled by the exhausted result, which carries the end continuation.
                return false;
            }

            public int getNextPosition() {
                return nextPosition;
            }

            @Nonnull
            @Override
            public ByteString toByteString() {
                return RecordCursorProto.ExplodeCursorContinuation.newBuilder()
                        .setNextPosition(nextPosition)
                        .build()
                        .toByteString();
            }

            @Nullable
            @Override
            public byte[] toBytes() {
                return toByteString().toByteArray();
            }

            @Nonnull
            public static Continuation from(@Nonnull final byte[] unparsedContinuationBytes) {
                try {
                    final var parsed = RecordCursorProto.ExplodeCursorContinuation.parseFrom(unparsedContinuationBytes);
                    return new Continuation(parsed.getNextPosition());
                } catch (InvalidProtocolBufferException ex) {
                    throw new RecordCoreException("invalid continuation", ex)
                            .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsedContinuationBytes));
                }
            }
        }
    }
}
