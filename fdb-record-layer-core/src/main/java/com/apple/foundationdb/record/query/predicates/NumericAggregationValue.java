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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.temp.Formatter;
import com.apple.foundationdb.record.query.plan.temp.ParserContext;
import com.apple.foundationdb.record.query.plan.temp.SemanticException;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.temp.Typed;
import com.apple.foundationdb.record.query.plan.temp.dynamic.TypeRepository;
import com.google.auto.service.AutoService;
import com.google.common.base.Enums;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

@API(API.Status.EXPERIMENTAL)
public class NumericAggregationValue implements Value, AggregateValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sum-Value");

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
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        throw new IllegalStateException("unable to eval an aggregation function with eval()");
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        return operator.evalInitialToPartial(child.eval(store, context, record, message));
    }

    @Nonnull
    @Override
    public Accumulator createAccumulator(final @Nonnull TypeRepository typeRepository) {
        return new NumericAccumulator(operator);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "sum(" + child.explain(formatter) + ")";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(operator.getResultTypeCode());
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return ImmutableList.of(child);
    }

    @Nonnull
    @Override
    public NumericAggregationValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        return new NumericAggregationValue(this.operator, Iterables.get(newChildren, 1));
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, operator, child);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, operator, child);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase() + "(" + child + ")";
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
    private static final Supplier<Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(NumericAggregationValue::computeOperatorMap);

    @Nonnull
    private static Map<Pair<LogicalOperator, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
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

        final Optional<LogicalOperator> logicalOperatorOptional = Enums.getIfPresent(LogicalOperator.class, functionName.toUpperCase()).toJavaUtil();
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

    @AutoService(BuiltInFunction.class)
    public static class SumFn extends BuiltInFunction<AggregateValue> {
        public SumFn() {
            super("sum",
                    ImmutableList.of(new Type.Any()), NumericAggregationValue::encapsulate);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class AvgFn extends BuiltInFunction<AggregateValue> {
        public AvgFn() {
            super("avg",
                    ImmutableList.of(new Type.Any()), NumericAggregationValue::encapsulate);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class MinFn extends BuiltInFunction<AggregateValue> {
        public MinFn() {
            super("min",
                    ImmutableList.of(new Type.Any()), NumericAggregationValue::encapsulate);
        }
    }

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

    public enum PhysicalOperator {
        SUM_I(LogicalOperator.SUM, TypeCode.INT, TypeCode.INT, v -> (int)v, (s, v) -> Math.addExact((int)s, (int)v), s -> (int)s),
        SUM_L(LogicalOperator.SUM, TypeCode.LONG, TypeCode.LONG, v -> (long)v, (s, v) -> Math.addExact((long)s, (long)v), s -> (long)s),
        SUM_F(LogicalOperator.SUM, TypeCode.FLOAT, TypeCode.FLOAT, v -> (float)v, (s, v) -> (float)s + (float)v, s -> (float)s),
        SUM_D(LogicalOperator.SUM, TypeCode.DOUBLE, TypeCode.DOUBLE, v -> (double)v, (s, v) -> (double)s + (double)v, s -> (double)s),

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

        MIN_I(LogicalOperator.MIN, TypeCode.INT, TypeCode.INT, v -> (int)v, (s, v) -> Math.min((int)s, (int)v), s -> (int)s),
        MIN_L(LogicalOperator.MIN, TypeCode.LONG, TypeCode.LONG, v -> (long)v, (s, v) -> Math.min((long)s, (long)v), s -> (long)s),
        MIN_F(LogicalOperator.MIN, TypeCode.FLOAT, TypeCode.FLOAT, v -> (float)v, (s, v) -> Math.min((float)s, (float)v), s -> (float)s),
        MIN_D(LogicalOperator.MIN, TypeCode.DOUBLE, TypeCode.DOUBLE, v -> (double)v, (s, v) -> Math.min((double)s, (double)v), s -> (double)s),

        MAX_I(LogicalOperator.MAX, TypeCode.INT, TypeCode.INT, v -> (int)v, (s, v) -> Math.max((int)s, (int)v), s -> (int)s),
        MAX_L(LogicalOperator.MAX, TypeCode.LONG, TypeCode.LONG, v -> (long)v, (s, v) -> Math.max((long)s, (long)v), s -> (long)s),
        MAX_F(LogicalOperator.MAX, TypeCode.FLOAT, TypeCode.FLOAT, v -> (float)v, (s, v) -> Math.max((float)s, (float)v), s -> (float)s),
        MAX_D(LogicalOperator.MAX, TypeCode.DOUBLE, TypeCode.DOUBLE, v -> (double)v, (s, v) -> Math.max((double)s, (double)v), s -> (double)s);

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
