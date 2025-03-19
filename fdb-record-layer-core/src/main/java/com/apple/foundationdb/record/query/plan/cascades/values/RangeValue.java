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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * A Value that is able to return a range between 0 (inclusive) and a given number (inclusive).
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RangeValue extends AbstractValue implements StreamingValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Range-Value");

    @Nonnull
    private final Value endExclusive;

    @Nonnull
    private final Optional<Value> beginInclusive;

    @Nonnull
    private final Optional<Value> step;

    public RangeValue(@Nonnull final Value endExclusive, @Nonnull final Optional<Value> beginInclusive,
                      @Nonnull final Optional<Value> step) {
        this.endExclusive = endExclusive;
        this.beginInclusive = beginInclusive;
        this.step = step;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> evalAsStream(@Nonnull final FDBRecordStoreBase<M> store,
                                                                      @Nonnull final EvaluationContext context,
                                                                      @Nullable final byte[] continuation,
                                                                      @Nonnull final ExecuteProperties executeProperties) {
        final long endExclusiveValue = (Long)Verify.verifyNotNull(endExclusive.eval(store, context));
        final var beginInclusiveMaybe = beginInclusive.map(b -> (Long)Verify.verifyNotNull(b.eval(store, context)));
        final var stepMaybe = step.map(s -> (Long)Verify.verifyNotNull(s.eval(store, context)));
        return new Cursor(store.getExecutor(), endExclusiveValue, beginInclusiveMaybe, stepMaybe, continuation);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("range")); // todo improve
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to eval an aggregation function with eval()");
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
        beginInclusive.ifPresent(b -> builder.setBeginInclusiveChild(b.toValueProto(serializationContext)));
        step.ifPresent(s -> builder.setStepChild(s.toValueProto(serializationContext)));
        return builder.build();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        final ImmutableList.Builder<Value> childrenBuilder = ImmutableList.builder();
        childrenBuilder.add(endExclusive);
        beginInclusive.ifPresent(childrenBuilder::add);
        step.ifPresent(childrenBuilder::add);
        return childrenBuilder.build();
    }

    @Nonnull
    @Override
    public Value withChildren(final Iterable<? extends Value> newChildren) {
        final var newChildrenSize = Iterables.size(newChildren);
        Verify.verify(newChildrenSize <= 3 && newChildrenSize > 0);
        final var end = Iterables.get(newChildren, 0);
        final Optional<Value> begin = newChildrenSize > 1 ? Optional.of(Iterables.get(newChildren, 1)) : Optional.empty();
        final Optional<Value> step = newChildrenSize > 2 ? Optional.of(Iterables.get(newChildren, 2)) : Optional.empty();
        if (end != this.endExclusive || begin != this.beginInclusive || step != this.step) {
            return new RangeValue(end, begin, step);
        }
        return this;
    }

    @Nonnull
    public static RangeValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PRangeValue rangeValueProto) {
        final var endExclusive = Value.fromValueProto(serializationContext, rangeValueProto.getEndExclusiveChild());
        final Optional<Value> beginInclusive = rangeValueProto.hasBeginInclusiveChild()
                                               ? Optional.of(Value.fromValueProto(serializationContext, rangeValueProto.getBeginInclusiveChild()))
                                               : Optional.empty();
        final Optional<Value> step = rangeValueProto.hasStepChild()
                                     ? Optional.of(Value.fromValueProto(serializationContext, rangeValueProto.getStepChild()))
                                     : Optional.empty();
        return new RangeValue(endExclusive, beginInclusive, step);
    }

    public static class Cursor implements RecordCursor<QueryResult> {

        @Nonnull
        private final Executor executor;

        private final long endExclusive;

        @Nonnull
        private final Optional<Long> step;

        private long nextPosition; // position of the next value to return

        private boolean closed = false;

        public Cursor(@Nonnull final Executor executor, final long endExclusive, @Nonnull final Optional<Long> beginInclusive,
                      @Nonnull final Optional<Long> step, @Nullable final byte[] continuation) {
            this(executor, endExclusive, step, continuation == null ? beginInclusive.orElse(0L) : Continuation.from(continuation).getNextPosition());
        }

        private Cursor(@Nonnull final Executor executor, final long endExclusive, @Nonnull final Optional<Long> step, final long nextPosition) {
            this.executor = executor;
            this.endExclusive = endExclusive;
            this.step = step;
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
            RecordCursorResult<QueryResult> nextResult;
            if (nextPosition < endExclusive) {
                nextResult = RecordCursorResult.withNextValue(QueryResult.ofComputed(nextPosition),
                        new Continuation(endExclusive, nextPosition + step.orElse(1L), step));
                nextPosition += step.orElse(1L);
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

        private static class Continuation implements RecordCursorContinuation {
            private final long endExclusive;
            private final long nextPosition;
            @Nonnull
            private final Optional<Long> step;

            public Continuation(long endExclusive, long nextPosition, @Nonnull final Optional<Long> step) {
                this.nextPosition = nextPosition;
                this.endExclusive = endExclusive;
                this.step = step;
            }

            @Override
            public boolean isEnd() {
                // If a next value is returned as part of a cursor result, the continuation must not be an end continuation
                // (i.e., isEnd() must be false), per the contract of RecordCursorResult. This is the case even if the
                // cursor knows for certain that there is no more after that result, as in the ListCursor.
                // Concretely, this means that we really need a > here, rather than >=.
                return nextPosition > endExclusive;
            }

            public long getEndExclusive() {
                return endExclusive;
            }

            public long getNextPosition() {
                return nextPosition;
            }

            @Nonnull
            public Optional<Long> getStep() {
                return step;
            }

            @Nonnull
            @Override
            public ByteString toByteString() {
                if (isEnd()) {
                    return ByteString.EMPTY;
                }
                final var protoBuilder = RecordCursorProto.RangeCursorContinuation.newBuilder()
                        .setEndExclusive(endExclusive).setNextPosition(nextPosition);
                step.ifPresent(protoBuilder::setStep);
                return protoBuilder.build().toByteString();
            }

            @Nullable
            @Override
            public byte[] toBytes() {
                return toByteString().toByteArray();
            }

            @Nonnull
            public static Continuation from(@Nonnull final RecordCursorProto.RangeCursorContinuation message) {
                final var endExclusive = message.getEndExclusive();
                final var nextPosition = message.getNextPosition();
                final Optional<Long> step = message.hasStep() ? Optional.of(message.getStep()) : Optional.empty();
                return new Continuation(endExclusive, nextPosition, step);
            }

            @Nonnull
            public static Continuation from(@Nonnull final byte[] unparsedContinuationBytes) {
                try {
                    final var parsed = RecordCursorProto.RangeCursorContinuation.parseFrom(unparsedContinuationBytes);
                    return from(parsed);
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
        private static StreamingValue encapsulateInternal(@Nonnull final BuiltInFunction<StreamingValue> builtInFunction,
                                                          @Nonnull final List<? extends Typed> arguments) {
            Verify.verify(!arguments.isEmpty());
            final var endExclusive = PromoteValue.inject((Value)arguments.get(0), Type.primitiveType(Type.TypeCode.LONG));
            final Optional<Value> beginInclusive = arguments.size() > 1 ? Optional.of(PromoteValue.inject((Value)arguments.get(1), Type.primitiveType(Type.TypeCode.LONG))) : Optional.empty();
            final Optional<Value> step = arguments.size() > 2 ? Optional.of(PromoteValue.inject((Value)arguments.get(2), Type.primitiveType(Type.TypeCode.LONG))) : Optional.empty();
            return new RangeValue(endExclusive, beginInclusive, step);
        }

        public RangeFn() {
            super("range", ImmutableList.of(), new Type.Any(), RangeFn::encapsulateInternal);
        }
    }
}
