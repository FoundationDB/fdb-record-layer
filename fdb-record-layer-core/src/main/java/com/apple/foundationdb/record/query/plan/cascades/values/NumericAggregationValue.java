/*
 * NumericAggregationValue.java
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PAvg;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PMax;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PMin;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PSum;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.auto.service.AutoService;
import com.google.common.base.Enums;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.function.UnaryOperator.identity;

/**
 * Aggregation over numeric values.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class NumericAggregationValue extends AbstractValue implements ValueWithChild, AggregateValue {
    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sum-Value");
    @Nonnull
    private static final Supplier<Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(NumericAggregationValue::computeOperatorMap);

    @Nonnull
    protected final PhysicalOperator operator;
    @Nonnull
    private final Value child;

    protected NumericAggregationValue(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PNumericAggregationValue numericAggregationValueProto) {
        this.operator = PhysicalOperator.fromProto(serializationContext, Objects.requireNonNull(numericAggregationValueProto.getOperator()));
        this.child = Value.fromValueProto(serializationContext, Objects.requireNonNull(numericAggregationValueProto.getChild()));
    }

    protected NumericAggregationValue(@Nonnull final PhysicalOperator operator,
                                      @Nonnull final Value child) {
        this.operator = operator;
        this.child = child;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to eval an aggregation function with eval()");
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return operator.evalInitialToPartial(child.eval(store, context));
    }

    @Nonnull
    @Override
    public Accumulator createAccumulator(final @Nonnull TypeRepository typeRepository) {
        return new NumericAccumulator(operator);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return operator.name().toLowerCase(Locale.ROOT) + child.explain(formatter) + ")";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(operator.getResultTypeCode());
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

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, child);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase(Locale.ROOT) + "(" + child + ")";
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
    public PNumericAggregationValue toNumericAggregationValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        PNumericAggregationValue.Builder builder = PNumericAggregationValue.newBuilder();
        builder.setOperator(operator.toProto(serializationContext));
        builder.setChild(child.toValueProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    private static Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
    private static AggregateValue encapsulate(@Nonnull final String functionName,
                                              @Nonnull final List<? extends Typed> arguments,
                                              @Nonnull final BiFunction<PhysicalOperator, Value, NumericAggregationValue> valueSupplier) {
        Verify.verify(arguments.size() == 1);
        final Typed arg0 = arguments.get(0);
        final Type type0 = arg0.getResultType();
        SemanticException.check(type0.isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_ARITHMETIC_OPERATOR_IS_OF_COMPLEX_TYPE);

        final Optional<LogicalOperator> logicalOperatorOptional = Enums.getIfPresent(LogicalOperator.class, functionName.toUpperCase(Locale.ROOT)).toJavaUtil();
        Verify.verify(logicalOperatorOptional.isPresent());
        final LogicalOperator logicalOperator = logicalOperatorOptional.get();

        final PhysicalOperator physicalOperator =
                getOperatorMap().get(Pair.of(logicalOperator, type0.getTypeCode()));

        Verify.verifyNotNull(physicalOperator, "unable to encapsulate aggregate operation due to type mismatch(es)");

        return valueSupplier.apply(physicalOperator, (Value)arg0);
    }

    private static Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator> computeOperatorMap() {
        final ImmutableMap.Builder<Pair<LogicalOperator, TypeCode>, PhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final PhysicalOperator operator : PhysicalOperator.values()) {
            mapBuilder.put(Pair.of(operator.getLogicalOperator(), operator.getArgType()), operator);
        }
        return mapBuilder.build();
    }

    /**
     * Sum aggregation {@code Value}.
     */
    public static class Sum extends NumericAggregationValue implements StreamableAggregateValue, IndexableAggregateValue {
        public Sum(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        protected Sum(@Nonnull final PlanSerializationContext serializationContext,
                      @Nonnull final PSum sumProto) {
            super(serializationContext, Objects.requireNonNull(sumProto.getSuper()));
        }

        @Nonnull
        @Override
        public String getIndexTypeName() {
            return IndexTypes.SUM;
        }

        @Nonnull
        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(@Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final List<? extends Typed> arguments) {
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Sum::new);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value newChild) {
            return new Sum(operator, newChild);
        }

        @Nonnull
        @Override
        public PSum toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PSum.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueSum(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Sum fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PSum sumProto) {
            return new Sum(serializationContext, sumProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PSum, Sum> {
            @Nonnull
            @Override
            public Class<PSum> getProtoMessageClass() {
                return PSum.class;
            }

            @Nonnull
            @Override
            public Sum fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PSum sumProto) {
                return Sum.fromProto(serializationContext, sumProto);
            }
        }
    }

    /**
     * Average aggregation {@code Value}.
     */
    public static class Avg extends NumericAggregationValue implements StreamableAggregateValue {
        public Avg(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        protected Avg(@Nonnull final PlanSerializationContext serializationContext,
                      @Nonnull final PAvg avgProto) {
            super(serializationContext, Objects.requireNonNull(avgProto.getSuper()));
        }

        @Nonnull
        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(@Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final List<? extends Typed> arguments) {
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Avg::new);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value newChild) {
            return new Avg(operator, newChild);
        }

        @Nonnull
        @Override
        public PAvg toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PAvg.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueAvg(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Avg fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PAvg avgProto) {
            return new Avg(serializationContext, avgProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PAvg, Avg> {
            @Nonnull
            @Override
            public Class<PAvg> getProtoMessageClass() {
                return PAvg.class;
            }

            @Nonnull
            @Override
            public Avg fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PAvg avgProto) {
                return Avg.fromProto(serializationContext, avgProto);
            }
        }
    }

    /**
     * Min aggregation {@code Value}.
     */
    public static class Min extends NumericAggregationValue implements StreamableAggregateValue, IndexableAggregateValue {
        public Min(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        protected Min(@Nonnull final PlanSerializationContext serializationContext,
                      @Nonnull final PMin minProto) {
            super(serializationContext, Objects.requireNonNull(minProto.getSuper()));
        }

        @Nonnull
        @Override
        public String getIndexTypeName() {
            return IndexTypes.PERMUTED_MIN;
        }

        @Nonnull
        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(@Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final List<? extends Typed> arguments) {
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Min::new);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value newChild) {
            return new Min(operator, newChild);
        }

        @Nonnull
        @Override
        public PMin toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PMin.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueMin(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Min fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PMin minProto) {
            return new Min(serializationContext, minProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMin, Min> {
            @Nonnull
            @Override
            public Class<PMin> getProtoMessageClass() {
                return PMin.class;
            }

            @Nonnull
            @Override
            public Min fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PMin minProto) {
                return Min.fromProto(serializationContext, minProto);
            }
        }
    }

    /**
     * Max aggregation {@code Value}.
     */
    public static class Max extends NumericAggregationValue implements StreamableAggregateValue, IndexableAggregateValue {
        public Max(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        protected Max(@Nonnull final PlanSerializationContext serializationContext,
                      @Nonnull final PMax maxProto) {
            super(serializationContext, Objects.requireNonNull(maxProto.getSuper()));
        }

        @Nonnull
        @Override
        public String getIndexTypeName() {
            return IndexTypes.PERMUTED_MAX;
        }

        @Nonnull
        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(@Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final List<? extends Typed> arguments) {
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Max::new);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value newChild) {
            return new Max(operator, newChild);
        }

        @Nonnull
        @Override
        public PMax toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PMax.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueMax(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Max fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PMax maxProto) {
            return new Max(serializationContext, maxProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMax, Max> {
            @Nonnull
            @Override
            public Class<PMax> getProtoMessageClass() {
                return PMax.class;
            }

            @Nonnull
            @Override
            public Max fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PMax maxProto) {
                return Max.fromProto(serializationContext, maxProto);
            }
        }
    }

    /**
     * The {@code sum} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class SumFn extends BuiltInFunction<AggregateValue> {
        public SumFn() {
            super("SUM",
                    ImmutableList.of(new Type.Any()), Sum::encapsulate);
        }
    }

    /**
     * The {@code avg} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class AvgFn extends BuiltInFunction<AggregateValue> {
        public AvgFn() {
            super("AVG",
                    ImmutableList.of(new Type.Any()), Avg::encapsulate);
        }
    }

    /**
     * The {@code min} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MinFn extends BuiltInFunction<AggregateValue> {
        public MinFn() {
            super("MIN",
                    ImmutableList.of(new Type.Any()), Min::encapsulate);
        }
    }

    /**
     * The {@code max} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MaxFn extends BuiltInFunction<AggregateValue> {
        public MaxFn() {
            super("MAX",
                    ImmutableList.of(new Type.Any()), Max::encapsulate);
        }
    }

    private enum LogicalOperator {
        SUM,
        AVG,
        MIN,
        MAX
    }

    /**
     * The function- x type-specific numeric aggregator.
     */
    public enum PhysicalOperator {
        SUM_I(LogicalOperator.SUM, TypeCode.INT, TypeCode.INT, Objects::requireNonNull, (s, v) -> Math.addExact((int)s, (int)v), identity()),
        SUM_L(LogicalOperator.SUM, TypeCode.LONG, TypeCode.LONG, Objects::requireNonNull, (s, v) -> Math.addExact((long)s, (long)v), identity()),
        SUM_F(LogicalOperator.SUM, TypeCode.FLOAT, TypeCode.FLOAT, Objects::requireNonNull, (s, v) -> (float)s + (float)v, identity()),
        SUM_D(LogicalOperator.SUM, TypeCode.DOUBLE, TypeCode.DOUBLE, Objects::requireNonNull, (s, v) -> (double)s + (double)v, identity()),

        AVG_I(LogicalOperator.AVG, TypeCode.INT, TypeCode.DOUBLE,
                v -> Pair.of(v, 1L),
                (s1, s2) -> {
                    final Pair<?, ?> pair1 = (Pair<?, ?>)s1;
                    final Pair<?, ?> pair2 = (Pair<?, ?>)s2;
                    return Pair.of(Math.addExact((int)pair1.getKey(), (int)pair2.getKey()), Math.addExact((long)pair1.getValue(), (long)pair2.getValue()));
                },
                s -> {
                    final Pair<?, ?> pair = (Pair<?, ?>)s;
                    return (double)(Integer)pair.getKey() / (long)pair.getValue();
                }),
        AVG_L(LogicalOperator.AVG, TypeCode.LONG, TypeCode.DOUBLE,
                v -> Pair.of(v, 1L),
                (s1, s2) -> {
                    final Pair<?, ?> pair1 = (Pair<?, ?>)s1;
                    final Pair<?, ?> pair2 = (Pair<?, ?>)s2;
                    return Pair.of(Math.addExact((long)pair1.getKey(), (long)pair2.getKey()), Math.addExact((long)pair1.getValue(), (long)pair2.getValue()));
                },
                s -> {
                    final Pair<?, ?> pair = (Pair<?, ?>)s;
                    return (double)(Long)pair.getKey() / (long)pair.getValue();
                }),
        AVG_F(LogicalOperator.AVG, TypeCode.FLOAT, TypeCode.DOUBLE,
                v -> Pair.of(v, 1L),
                (s1, s2) -> {
                    final Pair<?, ?> pair1 = (Pair<?, ?>)s1;
                    final Pair<?, ?> pair2 = (Pair<?, ?>)s2;
                    return Pair.of((float)pair1.getKey() + (float)pair2.getKey(), Math.addExact((long)pair1.getValue(), (long)pair2.getValue()));
                },
                s -> {
                    final Pair<?, ?> pair = (Pair<?, ?>)s;
                    return (double)(Float)pair.getKey() / (long)pair.getValue();
                }),
        AVG_D(LogicalOperator.AVG, TypeCode.DOUBLE, TypeCode.DOUBLE,
                v -> Pair.of(v, 1L),
                (s1, s2) -> {
                    final Pair<?, ?> pair1 = (Pair<?, ?>)s1;
                    final Pair<?, ?> pair2 = (Pair<?, ?>)s2;
                    return Pair.of((double)pair1.getKey() + (double)pair2.getKey(), Math.addExact((long)pair1.getValue(), (long)pair2.getValue()));
                },
                s -> {
                    final Pair<?, ?> pair = (Pair<?, ?>)s;
                    return (double)pair.getKey() / (long)pair.getValue();
                }),

        MIN_I(LogicalOperator.MIN, TypeCode.INT, TypeCode.INT, Objects::requireNonNull, (s, v) -> Math.min((int)s, (int)v), identity()),
        MIN_L(LogicalOperator.MIN, TypeCode.LONG, TypeCode.LONG, Objects::requireNonNull, (s, v) -> Math.min((long)s, (long)v), identity()),
        MIN_F(LogicalOperator.MIN, TypeCode.FLOAT, TypeCode.FLOAT, Objects::requireNonNull, (s, v) -> Math.min((float)s, (float)v), identity()),
        MIN_D(LogicalOperator.MIN, TypeCode.DOUBLE, TypeCode.DOUBLE, Objects::requireNonNull, (s, v) -> Math.min((double)s, (double)v), identity()),

        MAX_I(LogicalOperator.MAX, TypeCode.INT, TypeCode.INT, Objects::requireNonNull, (s, v) -> Math.max((int)s, (int)v), identity()),
        MAX_L(LogicalOperator.MAX, TypeCode.LONG, TypeCode.LONG, Objects::requireNonNull, (s, v) -> Math.max((long)s, (long)v), identity()),
        MAX_F(LogicalOperator.MAX, TypeCode.FLOAT, TypeCode.FLOAT, Objects::requireNonNull, (s, v) -> Math.max((float)s, (float)v), identity()),
        MAX_D(LogicalOperator.MAX, TypeCode.DOUBLE, TypeCode.DOUBLE, Objects::requireNonNull, (s, v) -> Math.max((double)s, (double)v), identity());

        @Nonnull
        private static final Supplier<BiMap<PhysicalOperator, PPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(PhysicalOperator.class, PPhysicalOperator.class));

        @Nonnull
        private final LogicalOperator logicalOperator;

        @Nonnull
        private final TypeCode argType;

        @Nonnull
        private final TypeCode resultType;

        @Nonnull
        private final UnaryOperator<Object> initialToPartialFunction;

        @Nonnull
        private final BinaryOperator<Object> partialToPartialFunction;

        @Nonnull
        private final UnaryOperator<Object> partialToFinalFunction;

        PhysicalOperator(@Nonnull LogicalOperator logicalOperator,
                         @Nonnull final TypeCode argType,
                         @Nonnull final TypeCode resultType,
                         @Nonnull final UnaryOperator<Object> initialToPartialFunction,
                         @Nonnull final BinaryOperator<Object> partialToPartialFunction,
                         @Nonnull final UnaryOperator<Object> partialToFinalFunction) {
            this.logicalOperator = logicalOperator;
            this.argType = argType;
            this.resultType = resultType;
            this.initialToPartialFunction = initialToPartialFunction;
            this.partialToPartialFunction = partialToPartialFunction;
            this.partialToFinalFunction = partialToFinalFunction;
        }

        @Nonnull
        private LogicalOperator getLogicalOperator() {
            return logicalOperator;
        }

        @Nonnull
        public TypeCode getArgType() {
            return argType;
        }

        @Nonnull
        public TypeCode getResultTypeCode() {
            return resultType;
        }

        @Nonnull
        @SuppressWarnings("unused")
        public UnaryOperator<Object> getInitialToPartialFunction() {
            return initialToPartialFunction;
        }

        @Nonnull
        @SuppressWarnings("unused")
        public BinaryOperator<Object> getPartialToPartialFunction() {
            return partialToPartialFunction;
        }

        @Nonnull
        @SuppressWarnings("unused")
        public UnaryOperator<Object> getPartialToFinalFunction() {
            return partialToFinalFunction;
        }

        @Nullable
        public Object evalInitialToPartial(@Nullable Object object) {
            if (object == null) {
                return null;
            }
            return initialToPartialFunction.apply(object);
        }

        @Nullable
        public Object evalPartialToPartial(@Nullable Object object1, @Nullable Object object2) {
            if (object1 == null) {
                return object2;
            }

            if (object2 == null) {
                return object1;
            }

            return partialToPartialFunction.apply(object1, object2);
        }

        @Nullable
        public Object evalPartialToFinal(@Nullable Object object) {
            if (object == null) {
                return null;
            }
            return partialToFinalFunction.apply(object);
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static PhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PPhysicalOperator physicalOperatorProto) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(physicalOperatorProto));
        }

        @Nonnull
        private static BiMap<PhysicalOperator, PPhysicalOperator> getProtoEnumBiMap() {
            return protoEnumBiMapSupplier.get();
        }
    }

    /**
     * Accumulator for aggregation using a type- and function-specific operator.
     */
    public static class NumericAccumulator implements Accumulator {
        private final PhysicalOperator physicalOperator;
        Object state = null;

        public NumericAccumulator(@Nonnull final PhysicalOperator physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public void accumulate(@Nullable final Object currentObject) {
            this.state = physicalOperator.evalPartialToPartial(state, currentObject);
        }

        @Nullable
        @Override
        public Object finish() {
            return physicalOperator.evalPartialToFinal(state);
        }
    }
}
