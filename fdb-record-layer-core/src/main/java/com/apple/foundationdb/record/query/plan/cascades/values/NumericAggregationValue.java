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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.ParserContext;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Enums;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.function.UnaryOperator.identity;

/**
 * Aggregation over numeric values.
 */
@API(API.Status.EXPERIMENTAL)
public class NumericAggregationValue implements ValueWithChild, AggregateValue {
    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sum-Value");
    @Nonnull
    private static final Supplier<Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(NumericAggregationValue::computeOperatorMap);

    @Nonnull
    private final PhysicalOperator operator;
    @Nonnull
    private final Value child;
    
    public NumericAggregationValue(@Nonnull PhysicalOperator operator,
                                   @Nonnull Value child) {
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
        return operator.name().toLowerCase(Locale.getDefault()) + child.explain(formatter) + ")";
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
    public ValueWithChild withNewChild(@Nonnull final Value newChild) {
        return new NumericAggregationValue(this.operator, newChild);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, operator, child);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase(Locale.getDefault()) + "(" + child + ")";
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Nonnull
    private static Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
    @SuppressWarnings("PMD.UnusedFormalParameter")
    private static AggregateValue encapsulate(@Nonnull ParserContext parserContext,
                                              @Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                              @Nonnull final List<Typed> arguments) {
        return encapsulate(builtInFunction.getFunctionName(), arguments);
    }

    @Nonnull
    private static AggregateValue encapsulate(@Nonnull final String functionName, @Nonnull final List<Typed> arguments) {
        Verify.verify(arguments.size() == 1);
        final Typed arg0 = arguments.get(0);
        final Type type0 = arg0.getResultType();
        SemanticException.check(type0.isPrimitive(), "only primitive types allowed in numeric aggregation operation");

        final Optional<LogicalOperator> logicalOperatorOptional = Enums.getIfPresent(LogicalOperator.class, functionName.toUpperCase(Locale.getDefault())).toJavaUtil();
        Verify.verify(logicalOperatorOptional.isPresent());
        final LogicalOperator logicalOperator = logicalOperatorOptional.get();

        final PhysicalOperator physicalOperator =
                getOperatorMap().get(Pair.of(logicalOperator, type0.getTypeCode()));

        Verify.verifyNotNull(physicalOperator, "unable to encapsulate aggregate operation due to type mismatch(es)");

        return new NumericAggregationValue(physicalOperator, (Value)arg0);
    }

    private static Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator> computeOperatorMap() {
        final ImmutableMap.Builder<Pair<LogicalOperator, TypeCode>, PhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final PhysicalOperator operator : PhysicalOperator.values()) {
            mapBuilder.put(Pair.of(operator.getLogicalOperator(), operator.getArgType()), operator);
        }
        return mapBuilder.build();
    }

    /**
     * The {@code sum} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class SumFn extends BuiltInFunction<AggregateValue> {
        public SumFn() {
            super("sum",
                    ImmutableList.of(new Type.Any()), NumericAggregationValue::encapsulate);
        }
    }

    /**
     * The {@code avg} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class AvgFn extends BuiltInFunction<AggregateValue> {
        public AvgFn() {
            super("avg",
                    ImmutableList.of(new Type.Any()), NumericAggregationValue::encapsulate);
        }
    }

    /**
     * The {@code min} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MinFn extends BuiltInFunction<AggregateValue> {
        public MinFn() {
            super("min",
                    ImmutableList.of(new Type.Any()), NumericAggregationValue::encapsulate);
        }
    }

    /**
     * The {@code max} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MaxFn extends BuiltInFunction<AggregateValue> {
        public MaxFn() {
            super("max",
                    ImmutableList.of(new Type.Any()), NumericAggregationValue::encapsulate);
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
        public LogicalOperator getLogicalOperator() {
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
        public UnaryOperator<Object> getInitialToPartialFunction() {
            return initialToPartialFunction;
        }

        @Nonnull
        public BinaryOperator<Object> getPartialToPartialFunction() {
            return partialToPartialFunction;
        }

        @Nonnull
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
