/*
 * IndexOnlyAggregateValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.planprotos.PIndexOnlyAggregateValue;
import com.apple.foundationdb.record.planprotos.PIndexOnlyAggregateValue.PPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PMaxEverLongValue;
import com.apple.foundationdb.record.planprotos.PMinEverLongValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a compile-time aggregation value that must be backed by an aggregation index, and can not be evaluated
 * at runtime by a streaming aggregation operator.
 * This value will be absorbed by a matching aggregation index at optimisation phase.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class IndexOnlyAggregateValue extends AbstractValue implements AggregateValue, Value.CompileTimeValue, ValueWithChild, IndexableAggregateValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Only-Aggregate-Value");

    protected enum PhysicalOperator {
        MAX_EVER_LONG,
        MIN_EVER_LONG;

        @Nonnull
        @SuppressWarnings("unused")
        PPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            switch (this) {
                case MAX_EVER_LONG:
                    return PPhysicalOperator.MAX_EVER_LONG;
                case MIN_EVER_LONG:
                    return PPhysicalOperator.MIN_EVER_LONG;
                default:
                    throw new RecordCoreException("unknown operator mapping. did you forget to add it?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        static PhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PPhysicalOperator physicalOperatorProto) {
            switch (physicalOperatorProto) {
                case MAX_EVER_LONG:
                    return MAX_EVER_LONG;
                case MIN_EVER_LONG:
                    return MIN_EVER_LONG;
                default:
                    throw new RecordCoreException("unknown operator mapping. did you forget to add it?");
            }
        }
    }

    @Nonnull
    protected final PhysicalOperator operator;

    @Nonnull
    private final Value child;

    protected IndexOnlyAggregateValue(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PIndexOnlyAggregateValue indexOnlyAggregateValueProto) {
        this(PhysicalOperator.fromProto(serializationContext, Objects.requireNonNull(indexOnlyAggregateValueProto.getOperator())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(indexOnlyAggregateValueProto.getChild())));
    }

    /**
     * Creates a new instance of {@link IndexOnlyAggregateValue}.
     * @param operator the aggregation function.
     * @param child the child {@link Value}.
     */
    protected IndexOnlyAggregateValue(@Nonnull final PhysicalOperator operator,
                                      @Nonnull final Value child) {
        this.operator = operator;
        this.child = child;
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return child.getResultType();
    }

    @Nonnull
    @Override
    public Accumulator createAccumulator(@Nonnull final TypeRepository typeRepository) {
        throw new IllegalStateException("unable to create accumulator in a compile-time aggregation function");
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to evalToPartial in a compile-time aggregation function");
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase(Locale.ROOT) + "(" + child + ")";
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, child);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other)
                .filter(ignored -> operator.equals(((IndexOnlyAggregateValue)other).operator));
    }

    @Nonnull
    PIndexOnlyAggregateValue toIndexOnlyAggregateValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexOnlyAggregateValue.newBuilder()
                .setOperator(operator.toProto(serializationContext))
                .setChild(child.toValueProto(serializationContext))
                .build();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    /**
     * Class to represent MIN_EVER(field) which can only be provided by a suitable index.
     */
    public static class MinEverLongValue extends IndexOnlyAggregateValue {

        MinEverLongValue(@Nonnull final PlanSerializationContext serializationContext,
                         @Nonnull final PMinEverLongValue minEverLongValueProto) {
            super(serializationContext, Objects.requireNonNull(minEverLongValueProto.getSuper()));
        }

        /**
         * Creates a new instance of {@link MinEverLongValue}.
         *
         * @param operator the aggregation function.
         * @param child the child {@link Value}.
         */
        MinEverLongValue(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        @Nonnull
        @Override
        public String getIndexTypeName() {
            return IndexTypes.MIN_EVER_LONG;
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 1);
            final Typed arg0 = arguments.get(0);
            final Type type0 = arg0.getResultType();
            SemanticException.check(type0.isNumeric(), SemanticException.ErrorCode.UNKNOWN, "only numeric types allowed in " + IndexTypes.MIN_EVER_LONG + " aggregation operation");
            return new MinEverLongValue(PhysicalOperator.MIN_EVER_LONG, (Value)arg0);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
            return new MinEverLongValue(operator, rebasedChild);
        }

        @Nonnull
        @Override
        public PMinEverLongValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PMinEverLongValue.newBuilder()
                    .setSuper(toIndexOnlyAggregateValueProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setMinEverLongValue(toProto(serializationContext)).build();
        }

        @Nonnull
        public static MinEverLongValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PMinEverLongValue minEverLongValueProto) {
            return new MinEverLongValue(serializationContext, minEverLongValueProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMinEverLongValue, MinEverLongValue> {
            @Nonnull
            @Override
            public Class<PMinEverLongValue> getProtoMessageClass() {
                return PMinEverLongValue.class;
            }

            @Nonnull
            @Override
            public MinEverLongValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PMinEverLongValue minEverLongValueProto) {
                return MinEverLongValue.fromProto(serializationContext, minEverLongValueProto);
            }
        }
    }

    /**
     * Class to represent MIN_EVER(field) which can only be provided by a suitable index.
     */
    public static class MaxEverLongValue extends IndexOnlyAggregateValue {
        MaxEverLongValue(@Nonnull final PlanSerializationContext serializationContext,
                         @Nonnull final PMaxEverLongValue maxEverLongValueProto) {
            super(serializationContext, Objects.requireNonNull(maxEverLongValueProto.getSuper()));
        }

        /**
         * Creates a new instance of {@link MaxEverLongValue}.
         *
         * @param operator the aggregation function.
         * @param child the child {@link Value}.
         */
        MaxEverLongValue(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        @Nonnull
        @Override
        public String getIndexTypeName() {
            return IndexTypes.MAX_EVER_LONG;
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 1);
            final Typed arg0 = arguments.get(0);
            final Type type0 = arg0.getResultType();
            SemanticException.check(type0.isNumeric(), SemanticException.ErrorCode.UNKNOWN, "only numeric types allowed in " + IndexTypes.MAX_EVER_LONG + " aggregation operation");
            return new MaxEverLongValue(PhysicalOperator.MAX_EVER_LONG, (Value)arg0);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
            return new MaxEverLongValue(operator, rebasedChild);
        }

        @Nonnull
        @Override
        public PMaxEverLongValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PMaxEverLongValue.newBuilder()
                    .setSuper(toIndexOnlyAggregateValueProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setMaxEverLongValue(toProto(serializationContext)).build();
        }

        @Nonnull
        public static MaxEverLongValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PMaxEverLongValue maxEverLongValueProto) {
            return new MaxEverLongValue(serializationContext, maxEverLongValueProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMaxEverLongValue, MaxEverLongValue> {
            @Nonnull
            @Override
            public Class<PMaxEverLongValue> getProtoMessageClass() {
                return PMaxEverLongValue.class;
            }

            @Nonnull
            @Override
            public MaxEverLongValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PMaxEverLongValue maxEverLongValueProto) {
                return MaxEverLongValue.fromProto(serializationContext, maxEverLongValueProto);
            }
        }
    }

    /**
     * The {@code min_ever} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MinEverLongFn extends BuiltInFunction<AggregateValue> {
        public MinEverLongFn() {
            super("MIN_EVER", ImmutableList.of(new Type.Any()), (ignored, arguments) -> MinEverLongValue.encapsulate(arguments));
        }
    }

    /**
     * The {@code max_ever} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MaxEverLongFn extends BuiltInFunction<AggregateValue> {
        public MaxEverLongFn() {
            super("MAX_EVER", ImmutableList.of(new Type.Any()), (ignored, arguments) -> MaxEverLongValue.encapsulate(arguments));
        }
    }
}
