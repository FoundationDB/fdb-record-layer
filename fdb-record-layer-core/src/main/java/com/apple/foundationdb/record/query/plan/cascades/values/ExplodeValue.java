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
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * A {@link StreamingValue} that iterates over the elements of an array. For every element it produces a struct of two
 * anonymous fields: the element, and the 0-based ordinal of the element within the array, akin to SQL's
 * {@code UNNEST ... WITH ORDINALITY}.
 *
 * <p>The struct has the same shape whether or not the explode {@linkplain #preservesOrder() preserves the order} of
 * its collection. An explode that preserves the order produces the elements in the order of the array, so each ordinal
 * is the position of its element in the array, and the planner must not reorder the collection. Any other explode
 * does not observe its ordinals, so its collection can be replaced by any permutation of it, for example by a sorted
 * one, even though that renumbers the ordinals it produces.
 *
 * <p>An explode that is {@linkplain #isSorted() sorted} produces the elements of its collection in ascending order, or
 * in descending order if it is also {@linkplain #isReverse() reverse}, and numbers the ordinals in that order. The
 * order is a property of the explode rather than of its collection, so it does not have to be recovered from whatever
 * computes the collection. Only an explode that does not preserve the order of its collection can be sorted.
 */
@API(API.Status.EXPERIMENTAL)
public class ExplodeValue extends AbstractValue implements StreamingValue, CreatesDynamicTypesValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Explode-Value");

    @Nonnull
    public static final String ELEMENT_FIELD_NAME = "_0";

    @Nonnull
    private static final Type ORDINAL_TYPE = Type.primitiveType(Type.TypeCode.INT, false);

    @Nonnull
    @SuppressWarnings("unchecked")
    private static final Comparator<Object> ELEMENT_COMPARATOR = Comparator.comparing(Comparable.class::cast);

    @Nonnull
    private final Value collectionValue;

    @Nonnull
    private final Type elementType;

    /**
     * Whether the planner must keep the collection in its order. If set, every ordinal this explode produces is the
     * position of its element in the array, so the collection must not be replaced by a permutation of it, for example
     * by a sorted one. If not set, the ordinals are not observed and the collection may be reordered. Either way the
     * explode flows the same {@code (element, ordinal)} struct.
     */
    private final boolean preserveOrder;

    /**
     * Whether the explode produces the elements of its collection sorted rather than in the order of the array.
     */
    private final boolean sorted;

    /**
     * Whether a sorted explode sorts in descending rather than ascending order.
     */
    private final boolean reverse;

    @Nonnull
    private final CorrelationIdentifier elementAlias;

    @Nonnull
    private final CorrelationIdentifier ordinalAlias;

    @Nonnull
    private final RecordConstructorValue structValue;

    public ExplodeValue(@Nonnull final Value collectionValue) {
        this(collectionValue, false);
    }

    public ExplodeValue(@Nonnull final Value collectionValue, final boolean preserveOrder) {
        this(collectionValue, preserveOrder, false, false);
    }

    public ExplodeValue(@Nonnull final Value collectionValue, final boolean preserveOrder, final boolean sorted,
                        final boolean reverse) {
        Verify.verify(collectionValue.getResultType().isArray());
        Verify.verify(!preserveOrder || !sorted, "cannot sort an explode that preserves its order");
        Verify.verify(sorted || !reverse, "cannot reverse an explode that is not sorted");
        this.collectionValue = collectionValue;
        this.preserveOrder = preserveOrder;
        this.sorted = sorted;
        this.reverse = reverse;
        this.elementType = Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType());
        Verify.verify(!sorted || isSortable(elementType), "cannot sort elements of type %s", elementType);
        this.elementAlias = CorrelationIdentifier.uniqueId();
        this.ordinalAlias = CorrelationIdentifier.uniqueId();
        final ImmutableList.Builder<Column<? extends Value>> columnsBuilder = ImmutableList.builder();
        columnsBuilder.add(Column.unnamedOf(QuantifiedObjectValue.of(elementAlias, elementType)));
        columnsBuilder.add(Column.unnamedOf(QuantifiedObjectValue.of(ordinalAlias, ORDINAL_TYPE)));
        this.structValue = RecordConstructorValue.ofColumns(columnsBuilder.build(), false);
        Verify.verify(ELEMENT_FIELD_NAME.equals(structValue.getResultType().getField(0).getFieldName()));
    }

    @Nonnull
    public Value getCollectionValue() {
        return collectionValue;
    }

    public boolean preservesOrder() {
        return preserveOrder;
    }

    public boolean isSorted() {
        return sorted;
    }

    public boolean isReverse() {
        return reverse;
    }

    /**
     * Whether elements of a type can be sorted, which needs their representation to be {@link Comparable}.
     *
     * @param elementType the type of the elements
     * @return {@code true} if an explode of an array of such elements can be sorted
     */
    public static boolean isSortable(@Nonnull final Type elementType) {
        if (elementType.isEnum()) {
            return true;
        }
        return elementType.isPrimitive() &&
                elementType.getTypeCode() != Type.TypeCode.BYTES && elementType.getTypeCode() != Type.TypeCode.VECTOR;
    }

    /**
     * Returns an explode of a different collection that preserves its order, and is sorted, if and only if this one
     * does and is.
     *
     * @param newCollectionValue the new collection
     * @return an explode of the new collection
     */
    @Nonnull
    public ExplodeValue withCollectionValue(@Nonnull final Value newCollectionValue) {
        return new ExplodeValue(newCollectionValue, preserveOrder, sorted, reverse);
    }

    /**
     * Returns an explode of the same collection that produces its elements sorted.
     *
     * @param reverse whether to sort in descending order
     * @return a sorted explode of the same collection
     */
    @Nonnull
    public ExplodeValue sorted(final boolean reverse) {
        return new ExplodeValue(collectionValue, preserveOrder, true, reverse);
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

    @Nonnull
    public List<Object> evalAsList(@Nullable final FDBRecordStoreBase<?> store, @Nonnull final EvaluationContext context) {
        final List<?> elements = elementsOf(store, context);
        return IntStream.range(0, elements.size())
                .mapToObj(ordinal -> structAt(context, elements, ordinal))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Evaluates the collection, and sorts it if the explode is sorted. A {@code null} collection has no elements.
     */
    @Nonnull
    private List<?> elementsOf(@Nullable final FDBRecordStoreBase<?> store, @Nonnull final EvaluationContext context) {
        final Object result = collectionValue.eval(store, context);
        if (result == null) {
            return List.of();
        }
        final List<?> elements = (List<?>)result;
        if (!sorted) {
            return elements;
        }
        final List<Object> sortedElements = new ArrayList<>(elements);
        sortedElements.sort(reverse ? ELEMENT_COMPARATOR.reversed() : ELEMENT_COMPARATOR);
        return sortedElements;
    }

    @Nonnull
    private Object structAt(@Nonnull final EvaluationContext context, @Nonnull final List<?> elements, final int ordinal) {
        return structOf(context, Verify.verifyNotNull(elements.get(ordinal), "array elements must be non-null"), ordinal);
    }

    @Nonnull
    private Object structOf(@Nonnull final EvaluationContext context, @Nonnull final Object element, final int ordinal) {
        final EvaluationContext elementContext = context
                .withBinding(Bindings.Internal.CORRELATION, elementAlias, QueryResult.ofComputed(element))
                .withBinding(Bindings.Internal.CORRELATION, ordinalAlias, QueryResult.ofComputed(ordinal));
        return Objects.requireNonNull(structValue.eval(null, elementContext));
    }

    @Nonnull
    @Override
    public CardinalitiesProperty.Cardinalities getCardinalities() {
        final int size;
        if (collectionValue instanceof AbstractArrayConstructorValue) {
            size = Iterables.size(collectionValue.getChildren());
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
        final var explainTokens = new ExplainTokens().addFunctionCall("explode",
                Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens());
        if (sorted) {
            explainTokens.addWhitespace().addKeyword("SORTED");
            if (reverse) {
                explainTokens.addWhitespace().addKeyword("DESC");
            }
        }
        return ExplainTokensWithPrecedence.of(explainTokens);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, preserveOrder, sorted,
                reverse);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, collectionValue, preserveOrder, sorted, reverse);
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> {
                    final var otherExplodeValue = (ExplodeValue)other;
                    return preserveOrder == otherExplodeValue.preservesOrder() &&
                            sorted == otherExplodeValue.isSorted() && reverse == otherExplodeValue.isReverse();
                });
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
                .setPreserveOrder(preserveOrder)
                .setSorted(sorted)
                .setReverse(reverse)
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
                Objects.requireNonNull(explodeValueProto.getCollectionValue())), explodeValueProto.getPreserveOrder(),
                explodeValueProto.getSorted(), explodeValueProto.getReverse());
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
     * A cursor over the elements of an array, producing a struct of each element and its ordinal. The continuation
     * records the position of the next element.
     *
     * <p>The continuation is not tied to the array it came from. Resuming is only correct if the array evaluates to the
     * same elements in the same order as it did when the continuation was created.
     */
    static class Cursor implements RecordCursor<QueryResult> {
        @Nonnull
        private final Executor executor;

        @Nonnull
        private final List<?> elements;

        @Nonnull
        private final BiFunction<List<?>, Integer, Object> structCreator;

        private int nextPosition;

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
