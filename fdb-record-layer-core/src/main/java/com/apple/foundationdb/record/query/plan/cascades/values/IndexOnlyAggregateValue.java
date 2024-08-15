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
import com.apple.foundationdb.record.planprotos.PMaxEverValue;
import com.apple.foundationdb.record.planprotos.PMinEverValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
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
        return semanticEquals(other, AliasMap.emptyMap());
    }

    /**
     * Class to represent {@code MIN_EVER(field)} where {@code field} which can only be provided by a suitable index.
     */
    public static class MinEverValue extends IndexOnlyAggregateValue {

        MinEverValue(@Nonnull final PlanSerializationContext serializationContext,
                     @Nonnull final PMinEverValue minEverValueProto) {
            super(serializationContext, Objects.requireNonNull(minEverValueProto.getSuper()));
        }

        /**
         * Creates a new instance of {@link MinEverValue}.
         *
         * @param operator the aggregation function.
         * @param child the child {@link Value}.
         */
        MinEverValue(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        @SuppressWarnings("deprecation")
        @Nonnull
        @Override
        public String getIndexTypeName() {
            return IndexTypes.MIN_EVER;
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 1);
            final Typed arg0 = arguments.get(0);
            return new MinEverValue(PhysicalOperator.MIN_EVER_LONG, (Value)arg0);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
            return new MinEverValue(operator, rebasedChild);
        }

        @Nonnull
        @Override
        public PMinEverValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PMinEverValue.newBuilder()
                    .setSuper(toIndexOnlyAggregateValueProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setMinEverValue(toProto(serializationContext)).build();
        }

        @Nonnull
        public static MinEverValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PMinEverValue minEverValueProto) {
            return new MinEverValue(serializationContext, minEverValueProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMinEverValue, MinEverValue> {
            @Nonnull
            @Override
            public Class<PMinEverValue> getProtoMessageClass() {
                return PMinEverValue.class;
            }

            @Nonnull
            @Override
            public MinEverValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PMinEverValue minEverValueProto) {
                return MinEverValue.fromProto(serializationContext, minEverValueProto);
            }
        }
    }

    /**
     * Class to represent {@code MAX_EVER(field)} where {@code field} which can only be provided by a suitable index.
     */
    public static class MaxEverValue extends IndexOnlyAggregateValue {
        MaxEverValue(@Nonnull final PlanSerializationContext serializationContext,
                     @Nonnull final PMaxEverValue maxEverValueProto) {
            super(serializationContext, Objects.requireNonNull(maxEverValueProto.getSuper()));
        }

        /**
         * Creates a new instance of {@link MaxEverValue}.
         *
         * @param operator the aggregation function.
         * @param child the child {@link Value}.
         */
        MaxEverValue(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        @SuppressWarnings("deprecation")
        @Nonnull
        @Override
        public String getIndexTypeName() {
            return IndexTypes.MAX_EVER;
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 1);
            final Typed arg0 = arguments.get(0);
            return new MaxEverValue(PhysicalOperator.MAX_EVER_LONG, (Value)arg0);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
            return new MaxEverValue(operator, rebasedChild);
        }

        @Nonnull
        @Override
        public PMaxEverValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PMaxEverValue.newBuilder()
                    .setSuper(toIndexOnlyAggregateValueProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setMaxEverValue(toProto(serializationContext)).build();
        }

        @Nonnull
        public static MaxEverValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PMaxEverValue maxEverValueProto) {
            return new MaxEverValue(serializationContext, maxEverValueProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMaxEverValue, MaxEverValue> {
            @Nonnull
            @Override
            public Class<PMaxEverValue> getProtoMessageClass() {
                return PMaxEverValue.class;
            }

            @Nonnull
            @Override
            public MaxEverValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PMaxEverValue maxEverValueProto) {
                return MaxEverValue.fromProto(serializationContext, maxEverValueProto);
            }
        }
    }

    /**
     * The {@code min_ever} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MinEverFn extends BuiltInFunction<AggregateValue> {
        public MinEverFn() {
            super("MIN_EVER", ImmutableList.of(new Type.Any()), (ignored, arguments) -> MinEverValue.encapsulate(arguments));
        }
    }

    /**
     * The {@code max_ever} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MaxEverFn extends BuiltInFunction<AggregateValue> {
        public MaxEverFn() {
            super("MAX_EVER", ImmutableList.of(new Type.Any()), (ignored, arguments) -> MaxEverValue.encapsulate(arguments));
        }
    }
}
