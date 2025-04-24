/*
 * RangeValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.planprotos.PRangeValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInTableFunction;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link StreamingValue} that is able to return a range that is defined as the following:
 * <ul>
 *     <li>an optional inclusive start (0L by default).</li>
 *     <li>an exclusive end.</li>
 *     <li>an optional step (1L by default).</li>
 * </ul>
 * For more information, see relational SQL {@code range} table-valued function.
 */
public class RangeValue extends AbstractValue implements StreamingValue, CreatesDynamicTypesValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Range-Value");

    @Nonnull
    private final Value endExclusive;

    @Nonnull
    private final Value beginInclusive;

    @Nonnull
    private final Value step;

    @Nonnull
    private final Value currentRangeValue;

    public RangeValue(@Nonnull final Value endExclusive, @Nonnull final Value beginInclusive,
                      @Nonnull final Value step) {
        this.endExclusive = endExclusive;
        this.beginInclusive = beginInclusive;
        this.step = step;
        currentRangeValue = RecordConstructorValue.ofColumns(ImmutableList.of(Column.of(Optional.of("ID"), LiteralValue.ofScalar(-1L))));
    }

    @Nonnull
    @Override
    public Type.Record getResultType() {
        return (Type.Record)currentRangeValue.getResultType();
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> evalAsStream(@Nonnull final FDBRecordStoreBase<M> store,
                                                                      @Nonnull final EvaluationContext context,
                                                                      @Nullable final byte[] continuation,
                                                                      @Nonnull final ExecuteProperties executeProperties) {
        final long endExclusiveValue = (Long)Verify.verifyNotNull(endExclusive.eval(store, context));
        final var beginInclusiveValue = (Long)Verify.verifyNotNull(this.beginInclusive.eval(store, context));
        final var stepValue = (Long)Verify.verifyNotNull(step.eval(store, context));
        return new Cursor(store.getExecutor(), endExclusiveValue, beginInclusiveValue, stepValue, rangeValueAsLong -> Objects.requireNonNull(currentRangeValue.replace(v -> {
            if (v instanceof LiteralValue) {
                return LiteralValue.ofScalar(rangeValueAsLong);
            }
            return v;
        })).eval(store, context), continuation).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var endExplainTokens = Iterables.get(explainSuppliers, 0).get().getExplainTokens();
        final var beginExplainTokens = Iterables.get(explainSuppliers, 1).get().getExplainTokens();
        final var stepExplainTokens = Iterables.get(explainSuppliers, 2).get().getExplainTokens();

        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("range",
                new ExplainTokens().addSequence(() -> new ExplainTokens().addCommaAndWhiteSpace(),
                        beginExplainTokens,
                        endExplainTokens,
                        new ExplainTokens().addKeyword("STEP").addWhitespace().addNested(stepExplainTokens))));
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to eval an streaming value with eval()");
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH);
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setRangeValue(toProto(serializationContext)).build();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectsPlanHash(hashMode, BASE_HASH, getChildren());
    }

    @Nonnull
    @Override
    public PRangeValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRangeValue.Builder builder = PRangeValue.newBuilder();
        builder.setEndExclusiveChild(endExclusive.toValueProto(serializationContext));
        builder.setBeginInclusiveChild(beginInclusive.toValueProto(serializationContext));
        builder.setStepChild(step.toValueProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        final ImmutableList.Builder<Value> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(endExclusive);
        childrenBuilder.add(beginInclusive);
        childrenBuilder.add(step);
        return childrenBuilder.build();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional.
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var newChildrenSize = Iterables.size(newChildren);
        Verify.verify(newChildrenSize == 3);
        final var newEndExclusive = Iterables.get(newChildren, 0);
        final var newBeginInclusive = Iterables.get(newChildren, 1);
        final var newStep = Iterables.get(newChildren, 2);

        if (newEndExclusive == endExclusive && newBeginInclusive == beginInclusive && newStep == step) {
            return this;
        }

        return new RangeValue(newEndExclusive, newBeginInclusive, newStep);
    }

    @Nonnull
    public static RangeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PRangeValue rangeValueProto) {
        final var endExclusive = Value.fromValueProto(serializationContext, rangeValueProto.getEndExclusiveChild());
        final var beginInclusive = Value.fromValueProto(serializationContext, rangeValueProto.getBeginInclusiveChild());
        final var step = Value.fromValueProto(serializationContext, rangeValueProto.getStepChild());
        return new RangeValue(endExclusive, beginInclusive, step);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRangeValue, RangeValue> {
        @Nonnull
        @Override
        public Class<PRangeValue> getProtoMessageClass() {
            return PRangeValue.class;
        }

        @Nonnull
        @Override
        public RangeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PRangeValue rangeValueProto) {
            return RangeValue.fromProto(serializationContext, rangeValueProto);
        }
    }

    public static class Cursor implements RecordCursor<QueryResult> {

        @Nonnull
        private final Executor executor;

        private final long endExclusive;

        private final long step;

        private long nextPosition; // position of the next value to return

        private boolean closed = false;

        @Nonnull
        private final Function<Long, Object> rangeValueCreator;

        Cursor(@Nonnull final Executor executor, final long endExclusive, final long beginInclusive,
                final long step, @Nonnull final Function<Long, Object> rangeValueCreator, @Nullable final byte[] continuation) {
            this(executor, endExclusive, step, rangeValueCreator,
                    continuation == null ? beginInclusive : Continuation.from(continuation, endExclusive, step).getNextPosition());
        }

        private Cursor(@Nonnull final Executor executor, final long endExclusive, final long step,
                       @Nonnull final Function<Long, Object> rangeValueCreator, final long nextPosition) {
            checkValidRange(nextPosition, endExclusive, step);
            this.executor = executor;
            this.endExclusive = endExclusive;
            this.step = step;
            this.nextPosition = nextPosition;
            this.rangeValueCreator = rangeValueCreator;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<QueryResult>> onNext() {
            return CompletableFuture.completedFuture(getNext());
        }

        @Nonnull
        @Override
        public RecordCursorResult<QueryResult> getNext() {
            RecordCursorResult<QueryResult> nextResult;
            if (nextPosition < endExclusive) {
                final var continuation = new Continuation(endExclusive, nextPosition + step, step);
                nextResult = RecordCursorResult.withNextValue(QueryResult.ofComputed(rangeValueCreator.apply(nextPosition)), continuation);
                nextPosition += step;
            } else {
                nextResult = RecordCursorResult.exhausted();
            }
            return nextResult;
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
        public boolean accept(@Nonnull RecordCursorVisitor visitor) {
            visitor.visitEnter(this);
            return visitor.visitLeave(this);
        }

        @Override
        @Nonnull
        public Executor getExecutor() {
            return executor;
        }

        private static void checkValidRange(final long position, final long endExclusive, final long step) {
            if (position < 0L) {
                throw new RecordCoreException("only non-negative position is allowed in range");
            }
            if (endExclusive < 0L) {
                throw new RecordCoreException("only non-negative exclusive end is allowed in range");
            }
            if (step <= 0L) {
                throw new RecordCoreException("only positive step is allowed in range");
            }
        }

        private static class Continuation implements RecordCursorContinuation {
            private final long endExclusive;
            private final long nextPosition;
            private final long step;

            public Continuation(final long endExclusive, final long nextPosition, final long step) {
                this.nextPosition = nextPosition;
                this.endExclusive = endExclusive;
                this.step = step;
            }

            @Override
            public boolean isEnd() {
                // If a next value is returned as part of a cursor result, the continuation must not be an end continuation
                // (i.e., isEnd() must be false), per the contract of RecordCursorResult. This is the case even if the
                // cursor knows for certain that there is no more after that result, as in the ListCursor.
                return nextPosition >= endExclusive + step;
            }

            public long getNextPosition() {
                return nextPosition;
            }

            @Nonnull
            @Override
            public ByteString toByteString() {
                if (isEnd()) {
                    return ByteString.EMPTY;
                }
                final var protoBuilder = RecordCursorProto.RangeCursorContinuation.newBuilder().setNextPosition(nextPosition);
                return protoBuilder.build().toByteString();
            }

            @Nullable
            @Override
            public byte[] toBytes() {
                return toByteString().toByteArray();
            }

            @Nonnull
            public static Continuation from(@Nonnull final RecordCursorProto.RangeCursorContinuation message,
                                            final long endExclusive, final long step) {
                final var nextPosition = message.getNextPosition();
                return new Continuation(endExclusive, nextPosition, step);
            }

            @Nonnull
            public static Continuation from(@Nonnull final byte[] unparsedContinuationBytes, final long endExclusive,
                                            final long step) {
                try {
                    final var parsed = RecordCursorProto.RangeCursorContinuation.parseFrom(unparsedContinuationBytes);
                    return from(parsed, endExclusive, step);
                } catch (InvalidProtocolBufferException ex) {
                    throw new RecordCoreException("invalid continuation", ex)
                            .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsedContinuationBytes));
                }
            }
        }
    }

    /**
     * The {@code range} table function.
     */
    @AutoService(BuiltInFunction.class)
    public static class RangeFn extends BuiltInTableFunction {

        @Nonnull
        private static StreamingValue encapsulateInternal(@Nonnull final List<? extends Typed> arguments) {
            Verify.verify(!arguments.isEmpty());
            final Value endExclusive;
            final Value beginInclusiveMaybe;
            final Value stepMaybe;
            if (arguments.size() == 1) {
                endExclusive = PromoteValue.inject((Value)arguments.get(0), Type.primitiveType(Type.TypeCode.LONG));
                checkValidBoundaryType(endExclusive);
                beginInclusiveMaybe = LiteralValue.ofScalar(0L);
                stepMaybe = LiteralValue.ofScalar(1L);
            } else {
                checkValidBoundaryType((Value)arguments.get(0));
                beginInclusiveMaybe = PromoteValue.inject((Value)arguments.get(0), Type.primitiveType(Type.TypeCode.LONG));
                checkValidBoundaryType((Value)arguments.get(1));
                endExclusive = PromoteValue.inject((Value)arguments.get(1), Type.primitiveType(Type.TypeCode.LONG));
                if (arguments.size() > 2) {
                    checkValidBoundaryType((Value)arguments.get(2));
                    stepMaybe = PromoteValue.inject((Value)arguments.get(2), Type.primitiveType(Type.TypeCode.LONG));
                } else {
                    stepMaybe = LiteralValue.ofScalar(1L);
                }
            }
            return new RangeValue(endExclusive, beginInclusiveMaybe, stepMaybe);
        }

        public RangeFn() {
            super("range", ImmutableList.of(), new Type.Any(), (ignored, arguments) -> encapsulateInternal(arguments));
        }
    }

    private static void checkValidBoundaryType(@Nonnull final Value value) {
        final var type = value.getResultType();
        SemanticException.check(type.isPrimitive(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        final var maxType = Type.maximumType(type, Type.primitiveType(Type.TypeCode.LONG));
        SemanticException.check(maxType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        // currently, we do not support non-deterministic boundaries since we do not pre-compute them and store
        // them in the continuation, but we can change enable this if required.
        SemanticException.check(value.preOrderStream().filter(NondeterministicValue.class::isInstance).findAny().isEmpty(),
                SemanticException.ErrorCode.UNSUPPORTED);
    }
}
