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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PAvg;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PBitmapConstructAgg;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PMax;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PMin;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PNumericAggregationValue.PSum;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.indexes.BitmapValueIndexMaintainer;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
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
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.Arrays;
import java.util.BitSet;
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
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sum-Value");
    private static final Supplier<Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(NumericAggregationValue::computeOperatorMap);

    protected final PhysicalOperator operator;
    private final Value child;

    protected NumericAggregationValue(final PlanSerializationContext serializationContext,
                                      final PNumericAggregationValue numericAggregationValueProto) {
        this.operator = PhysicalOperator.fromProto(serializationContext, Objects.requireNonNull(numericAggregationValueProto.getOperator()));
        this.child = Value.fromValueProto(serializationContext, Objects.requireNonNull(numericAggregationValueProto.getChild()));
    }

    protected NumericAggregationValue(final PhysicalOperator operator,
                                      final Value child) {
        this.operator = operator;
        this.child = child;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        throw new IllegalStateException("unable to eval an aggregation function with eval()");
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        return operator.evalInitialToPartial(child.eval(store, context));
    }

    @Override
    public Accumulator createAccumulatorWithInitialState(final TypeRepository typeRepository, @Nullable List<RecordCursorProto.AccumulatorState> initialState) {
        if (initialState == null) {
            return new NumericAccumulator(operator);
        } else {
            Verify.verify(initialState.size() == 1);
            return new NumericAccumulator(operator, initialState.get(0));
        }
    }

    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens()
                .addFunctionCall(operator.name().toLowerCase(Locale.ROOT),
                        Iterables.getOnlyElement(explainSuppliers).get().getExplainTokens()));
    }

    @Override
    public Type getResultType() {
        return Type.primitiveType(operator.getResultTypeCode());
    }

    @Override
    public Value getChild() {
        return child;
    }

    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, child);
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

    public PNumericAggregationValue toNumericAggregationValueProto(final PlanSerializationContext serializationContext) {
        PNumericAggregationValue.Builder builder = PNumericAggregationValue.newBuilder();
        builder.setOperator(operator.toProto(serializationContext));
        builder.setChild(child.toValueProto(serializationContext));
        return builder.build();
    }

    private static Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    private static AggregateValue encapsulate(final String functionName,
                                              final List<? extends Typed> arguments,
                                              final BiFunction<PhysicalOperator, Value, NumericAggregationValue> valueSupplier) {
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
     * Bitmap aggregation {@code Value}.
     */
    public static class BitmapConstructAgg extends NumericAggregationValue implements StreamableAggregateValue, IndexableAggregateValue {
        public BitmapConstructAgg(final PhysicalOperator operator, final Value child) {
            super(operator, child);
        }

        protected BitmapConstructAgg(final PlanSerializationContext serializationContext,
                                     final PBitmapConstructAgg bitMapProto) {
            super(serializationContext, Objects.requireNonNull(bitMapProto.getSuper()));
        }

        @Override
        public String getIndexTypeName() {
            return IndexTypes.BITMAP_VALUE;
        }

        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(BuiltInFunction<AggregateValue> builtInFunction,
                                                  final CallSiteArguments callSiteArguments) {
            final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, BitmapConstructAgg::new);
        }

        @Override
        public ValueWithChild withNewChild(final Value newChild) {
            return new BitmapConstructAgg(operator, newChild);
        }

        @Override
        public PBitmapConstructAgg toProto(final PlanSerializationContext serializationContext) {
            return PBitmapConstructAgg.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Override
        public PValue toValueProto(final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueBitmapConstructAgg(toProto(serializationContext)).build();
        }

        public static BitmapConstructAgg fromProto(final PlanSerializationContext serializationContext, final PBitmapConstructAgg bitMapProto) {
            return new BitmapConstructAgg(serializationContext, bitMapProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PBitmapConstructAgg, BitmapConstructAgg> {
            @Override
            public Class<PBitmapConstructAgg> getProtoMessageClass() {
                return PBitmapConstructAgg.class;
            }

            @Override
            public BitmapConstructAgg fromProto(final PlanSerializationContext serializationContext,
                                                final PBitmapConstructAgg bitMapProto) {
                return BitmapConstructAgg.fromProto(serializationContext, bitMapProto);
            }
        }
    }

    /**
     * Sum aggregation {@code Value}.
     */
    public static class Sum extends NumericAggregationValue implements StreamableAggregateValue, IndexableAggregateValue {
        public Sum(final PhysicalOperator operator, final Value child) {
            super(operator, child);
        }

        protected Sum(final PlanSerializationContext serializationContext,
                      final PSum sumProto) {
            super(serializationContext, Objects.requireNonNull(sumProto.getSuper()));
        }

        @Override
        public String getIndexTypeName() {
            return IndexTypes.SUM;
        }

        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(BuiltInFunction<AggregateValue> builtInFunction,
                                                  final CallSiteArguments callSiteArguments) {
            final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Sum::new);
        }

        @Override
        public ValueWithChild withNewChild(final Value newChild) {
            return new Sum(operator, newChild);
        }

        @Override
        public PSum toProto(final PlanSerializationContext serializationContext) {
            return PSum.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Override
        public PValue toValueProto(final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueSum(toProto(serializationContext)).build();
        }

        public static Sum fromProto(final PlanSerializationContext serializationContext, final PSum sumProto) {
            return new Sum(serializationContext, sumProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PSum, Sum> {
            @Override
            public Class<PSum> getProtoMessageClass() {
                return PSum.class;
            }

            @Override
            public Sum fromProto(final PlanSerializationContext serializationContext,
                                 final PSum sumProto) {
                return Sum.fromProto(serializationContext, sumProto);
            }
        }
    }

    /**
     * Average aggregation {@code Value}.
     */
    public static class Avg extends NumericAggregationValue implements StreamableAggregateValue {
        public Avg(final PhysicalOperator operator, final Value child) {
            super(operator, child);
        }

        protected Avg(final PlanSerializationContext serializationContext,
                      final PAvg avgProto) {
            super(serializationContext, Objects.requireNonNull(avgProto.getSuper()));
        }

        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(BuiltInFunction<AggregateValue> builtInFunction,
                                                  final CallSiteArguments callSiteArguments) {
            final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Avg::new);
        }

        @Override
        public ValueWithChild withNewChild(final Value newChild) {
            return new Avg(operator, newChild);
        }

        @Override
        public PAvg toProto(final PlanSerializationContext serializationContext) {
            return PAvg.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Override
        public PValue toValueProto(final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueAvg(toProto(serializationContext)).build();
        }

        public static Avg fromProto(final PlanSerializationContext serializationContext, final PAvg avgProto) {
            return new Avg(serializationContext, avgProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PAvg, Avg> {
            @Override
            public Class<PAvg> getProtoMessageClass() {
                return PAvg.class;
            }

            @Override
            public Avg fromProto(final PlanSerializationContext serializationContext,
                                 final PAvg avgProto) {
                return Avg.fromProto(serializationContext, avgProto);
            }
        }
    }

    /**
     * Min aggregation {@code Value}.
     */
    public static class Min extends NumericAggregationValue implements StreamableAggregateValue, IndexableAggregateValue {
        public Min(final PhysicalOperator operator, final Value child) {
            super(operator, child);
        }

        protected Min(final PlanSerializationContext serializationContext,
                      final PMin minProto) {
            super(serializationContext, Objects.requireNonNull(minProto.getSuper()));
        }

        @Override
        public String getIndexTypeName() {
            return IndexTypes.PERMUTED_MIN;
        }

        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(BuiltInFunction<AggregateValue> builtInFunction,
                                                  final CallSiteArguments callSiteArguments) {
            final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Min::new);
        }

        @Override
        public ValueWithChild withNewChild(final Value newChild) {
            return new Min(operator, newChild);
        }

        @Override
        public PMin toProto(final PlanSerializationContext serializationContext) {
            return PMin.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Override
        public PValue toValueProto(final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueMin(toProto(serializationContext)).build();
        }

        public static Min fromProto(final PlanSerializationContext serializationContext, final PMin minProto) {
            return new Min(serializationContext, minProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMin, Min> {
            @Override
            public Class<PMin> getProtoMessageClass() {
                return PMin.class;
            }

            @Override
            public Min fromProto(final PlanSerializationContext serializationContext,
                                 final PMin minProto) {
                return Min.fromProto(serializationContext, minProto);
            }
        }
    }

    /**
     * Max aggregation {@code Value}.
     */
    public static class Max extends NumericAggregationValue implements StreamableAggregateValue, IndexableAggregateValue {
        public Max(final PhysicalOperator operator, final Value child) {
            super(operator, child);
        }

        protected Max(final PlanSerializationContext serializationContext,
                      final PMax maxProto) {
            super(serializationContext, Objects.requireNonNull(maxProto.getSuper()));
        }

        @Override
        public String getIndexTypeName() {
            return IndexTypes.PERMUTED_MAX;
        }

        @SuppressWarnings("PMD.UnusedFormalParameter")
        private static AggregateValue encapsulate(BuiltInFunction<AggregateValue> builtInFunction,
                                                  final CallSiteArguments callSiteArguments) {
            final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
            return NumericAggregationValue.encapsulate(builtInFunction.getFunctionName(), arguments, Max::new);
        }

        @Override
        public ValueWithChild withNewChild(final Value newChild) {
            return new Max(operator, newChild);
        }

        @Override
        public PMax toProto(final PlanSerializationContext serializationContext) {
            return PMax.newBuilder().setSuper(toNumericAggregationValueProto(serializationContext)).build();
        }

        @Override
        public PValue toValueProto(final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setNumericAggregationValueMax(toProto(serializationContext)).build();
        }

        public static Max fromProto(final PlanSerializationContext serializationContext, final PMax maxProto) {
            return new Max(serializationContext, maxProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PMax, Max> {
            @Override
            public Class<PMax> getProtoMessageClass() {
                return PMax.class;
            }

            @Override
            public Max fromProto(final PlanSerializationContext serializationContext,
                                 final PMax maxProto) {
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
     * The {@code bitmap} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class BitmapConstructAggFn extends BuiltInFunction<AggregateValue> {
        public BitmapConstructAggFn() {
            super("BITMAP_CONSTRUCT_AGG",
                    ImmutableList.of(new Type.Any()), BitmapConstructAgg::encapsulate);
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
        MAX,
        BITMAP_CONSTRUCT_AGG
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
        MAX_D(LogicalOperator.MAX, TypeCode.DOUBLE, TypeCode.DOUBLE, Objects::requireNonNull, (s, v) -> Math.max((double)s, (double)v), identity()),
        BITMAP_CONSTRUCT_AGG_L(LogicalOperator.BITMAP_CONSTRUCT_AGG, TypeCode.LONG, TypeCode.BYTES,
                s -> {
                    BitSet sset = new BitSet();
                    sset.set(((Long)s).intValue());
                    return sset;
                },
                (s, v) -> {
                    BitSet sset = (BitSet)s;
                    sset.or((BitSet)v);
                    return sset;
                },
                s -> {
                    int fixedResultArraySize = BitmapValueIndexMaintainer.DEFAULT_ENTRY_SIZE / 8;
                    byte[] res = ((BitSet)s).toByteArray();
                    if (res.length > BitmapValueIndexMaintainer.MAX_ENTRY_SIZE / 8) {
                        throw new RecordCoreArgumentException("entry size option is too large")
                                .addLogInfo("entrySize", res.length * 8, "maxEntrySize", BitmapValueIndexMaintainer.MAX_ENTRY_SIZE);
                    } else if (res.length > fixedResultArraySize) {
                        return res;
                    } else {
                        return Arrays.copyOf(res, fixedResultArraySize);
                    }
                }),
        BITMAP_CONSTRUCT_AGG_I(LogicalOperator.BITMAP_CONSTRUCT_AGG, TypeCode.INT, TypeCode.BYTES,
                s -> {
                    BitSet sset = new BitSet();
                    sset.set((int)s);
                    return sset;
                },
                (s, v) -> {
                    BitSet sset = (BitSet)s;
                    sset.or((BitSet)v);
                    return sset;
                },
                s -> {
                    int fixedResultArraySize = BitmapValueIndexMaintainer.DEFAULT_ENTRY_SIZE / 8;
                    byte[] res = ((BitSet)s).toByteArray();
                    if (res.length > BitmapValueIndexMaintainer.MAX_ENTRY_SIZE / 8) {
                        throw new RecordCoreArgumentException("entry size option is too large")
                                .addLogInfo("entrySize", res.length * 8, "maxEntrySize", BitmapValueIndexMaintainer.MAX_ENTRY_SIZE);
                    } else if (res.length > fixedResultArraySize) {
                        return res;
                    } else {
                        return Arrays.copyOf(res, fixedResultArraySize);
                    }
                });

        private static final Supplier<BiMap<PhysicalOperator, PPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(PhysicalOperator.class, PPhysicalOperator.class));

        private final LogicalOperator logicalOperator;

        private final TypeCode argType;

        private final TypeCode resultType;

        private final UnaryOperator<Object> initialToPartialFunction;

        private final BinaryOperator<Object> partialToPartialFunction;

        private final UnaryOperator<Object> partialToFinalFunction;

        PhysicalOperator(LogicalOperator logicalOperator,
                         final TypeCode argType,
                         final TypeCode resultType,
                         final UnaryOperator<Object> initialToPartialFunction,
                         final BinaryOperator<Object> partialToPartialFunction,
                         final UnaryOperator<Object> partialToFinalFunction) {
            this.logicalOperator = logicalOperator;
            this.argType = argType;
            this.resultType = resultType;
            this.initialToPartialFunction = initialToPartialFunction;
            this.partialToPartialFunction = partialToPartialFunction;
            this.partialToFinalFunction = partialToFinalFunction;
        }

        private LogicalOperator getLogicalOperator() {
            return logicalOperator;
        }

        public TypeCode getArgType() {
            return argType;
        }

        public TypeCode getResultTypeCode() {
            return resultType;
        }

        @SuppressWarnings("unused")
        public UnaryOperator<Object> getInitialToPartialFunction() {
            return initialToPartialFunction;
        }

        @SuppressWarnings("unused")
        public BinaryOperator<Object> getPartialToPartialFunction() {
            return partialToPartialFunction;
        }

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

        @SuppressWarnings("unused")
        public PPhysicalOperator toProto(final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @SuppressWarnings("unused")
        public static PhysicalOperator fromProto(final PlanSerializationContext serializationContext,
                                                 final PPhysicalOperator physicalOperatorProto) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(physicalOperatorProto));
        }

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

        public NumericAccumulator(final PhysicalOperator physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        public NumericAccumulator(final PhysicalOperator physicalOperator, final RecordCursorProto.AccumulatorState initialState) {
            this.physicalOperator = physicalOperator;
            switch (physicalOperator) {
                case SUM_I:
                case MAX_I:
                case MIN_I:
                    Verify.verify(initialState.getStateList().size() == 1);
                    Verify.verify(initialState.getState(0).hasInt32State());
                    state = initialState.getState(0).getInt32State();
                    break;
                case SUM_L:
                case MAX_L:
                case MIN_L:
                    Verify.verify(initialState.getStateList().size() == 1);
                    Verify.verify(initialState.getState(0).hasInt64State());
                    state = initialState.getState(0).getInt64State();
                    break;
                case SUM_D:
                case MAX_D:
                case MIN_D:
                    Verify.verify(initialState.getStateList().size() == 1);
                    Verify.verify(initialState.getState(0).hasDoubleState());
                    state = initialState.getState(0).getDoubleState();
                    break;
                case SUM_F:
                case MAX_F:
                case MIN_F:
                    Verify.verify(initialState.getStateList().size() == 1);
                    Verify.verify(initialState.getState(0).hasFloatState());
                    state = initialState.getState(0).getFloatState();
                    break;
                case AVG_I:
                    Verify.verify(initialState.getStateList().size() == 2);
                    Verify.verify(initialState.getState(0).hasInt32State());
                    Verify.verify(initialState.getState(1).hasInt64State());
                    state = Pair.of(initialState.getState(0).getInt32State(), initialState.getState(1).getInt64State());
                    break;
                case AVG_L:
                    Verify.verify(initialState.getStateList().size() == 2);
                    Verify.verify(initialState.getState(0).hasInt64State());
                    Verify.verify(initialState.getState(1).hasInt64State());
                    state = Pair.of(initialState.getState(0).getInt64State(), initialState.getState(1).getInt64State());
                    break;
                case AVG_D:
                    Verify.verify(initialState.getStateList().size() == 2);
                    Verify.verify(initialState.getState(0).hasDoubleState());
                    Verify.verify(initialState.getState(1).hasInt64State());
                    state = Pair.of(initialState.getState(0).getDoubleState(), initialState.getState(1).getInt64State());
                    break;
                case AVG_F:
                    Verify.verify(initialState.getStateList().size() == 2);
                    Verify.verify(initialState.getState(0).hasFloatState());
                    Verify.verify(initialState.getState(1).hasInt64State());
                    state = Pair.of(initialState.getState(0).getFloatState(), initialState.getState(1).getInt64State());
                    break;
                case BITMAP_CONSTRUCT_AGG_I:
                case BITMAP_CONSTRUCT_AGG_L:
                    Verify.verify(initialState.getStateList().size() == 1);
                    Verify.verify(initialState.getState(0).hasBytesState());
                    state = BitSet.valueOf(initialState.getState(0).getBytesState().toByteArray());
                    break;
                default:
                    throw new RecordCoreException("Unsupported physical operator name in initial accumulator state.");
            }
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

        @Override
        public List<RecordCursorProto.AccumulatorState> getAccumulatorStates() {
            if (state ==  null) {
                return List.of();
            }
            RecordCursorProto.AccumulatorState.Builder builder = RecordCursorProto.AccumulatorState.newBuilder();
            Pair<?, ?> pair;
            switch (physicalOperator) {
                case SUM_I:
                case MAX_I:
                case MIN_I:
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt32State((int)state));
                    break;
                case SUM_L:
                case MAX_L:
                case MIN_L:
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt64State((long)state));
                    break;
                case SUM_D:
                case MAX_D:
                case MIN_D:
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setDoubleState((double)state));
                    break;
                case SUM_F:
                case MAX_F:
                case MIN_F:
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setFloatState((float)state));
                    break;
                case AVG_I:
                    pair = (Pair<?, ?>) state;
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt32State((int)pair.getLeft()))
                            .addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt64State((long)pair.getRight()));
                    break;
                case AVG_L:
                    pair = (Pair<?, ?>) state;
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt64State((long)pair.getLeft()))
                            .addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt64State((long)pair.getRight()));
                    break;
                case AVG_D:
                    pair = (Pair<?, ?>) state;
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setDoubleState((double)pair.getLeft()))
                            .addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt64State((long)pair.getRight()));
                    break;
                case AVG_F:
                    pair = (Pair<?, ?>) state;
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setFloatState((float)pair.getLeft()))
                            .addState(RecordCursorProto.OneOfTypedState.newBuilder().setInt64State((long)pair.getRight()));
                    break;
                case BITMAP_CONSTRUCT_AGG_I:
                case BITMAP_CONSTRUCT_AGG_L:
                    builder.addState(RecordCursorProto.OneOfTypedState.newBuilder().setBytesState(ByteString.copyFrom(((BitSet)state).toByteArray())));
                    break;
                default:
                    break;

            }
            return List.of(builder.build());
        }
    }
}
