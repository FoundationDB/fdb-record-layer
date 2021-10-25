/*
 * ArithmeticValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.cursors.aggregate.Accumulator;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.norse.SemanticException;
import com.apple.foundationdb.record.query.norse.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.predicates.Type.TypeCode;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A value merges the input messages given to it into an output message.
 */
@API(API.Status.EXPERIMENTAL)
public class SumValue implements Value, AggregateValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sum-Value");

    @Nonnull
    private final PhysicalOperator operator;
    @Nonnull
    private final Value child;

    public SumValue(@Nonnull PhysicalOperator operator,
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
    public Accumulator createAccumulator(final @Nonnull DynamicSchema dynamicSchema) {
        return new SumAccumulator(operator);
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
    public SumValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        return new SumValue(this.operator, Iterables.get(newChildren, 1));
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
    private static final Supplier<Map<TypeCode, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(SumValue::computeOperatorMap);

    @Nonnull
    private static Map<TypeCode, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
    private static AggregateValue encapsulate(@Nonnull ParserContext parserContext,
                                              @Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                              @Nonnull final List<Atom> arguments) {
        return encapsulate(builtInFunction.getFunctionName(), arguments);
    }

    @Nonnull
    private static AggregateValue encapsulate(@Nonnull final String functionName, @Nonnull final List<Atom> arguments) {
        Verify.verify(arguments.size() == 1);
        final Atom arg0 = arguments.get(0);
        final Type type0 = arg0.getResultType();
        SemanticException.check(type0.isPrimitive(), "only primitive types allowed in sum operation");

        final PhysicalOperator physicalOperator =
                getOperatorMap().get(type0.getTypeCode());

        Verify.verifyNotNull(physicalOperator, "unable to encapsulate arithmetic operation due to type mismatch(es)");

        return new SumValue(physicalOperator, (Value)arg0);
    }

    @AutoService(BuiltInFunction.class)
    public static class SumFn extends BuiltInFunction<AggregateValue> {
        public SumFn() {
            super("sum",
                    ImmutableList.of(new Type.Any()), SumValue::encapsulate);
        }
    }

    private static Map<TypeCode, PhysicalOperator> computeOperatorMap() {
        final ImmutableMap.Builder<TypeCode, PhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final PhysicalOperator operation : PhysicalOperator.values()) {
            mapBuilder.put(operation.getArgType(), operation);
        }
        return mapBuilder.build();
    }

    private enum PhysicalOperator {
        SUM_I(TypeCode.INT, TypeCode.INT, v -> (int)v, (s, v) -> (int)s + (int)v, s -> (int)s),
        SUM_L(TypeCode.LONG, TypeCode.LONG, v -> (long)v, (s, v) -> (long)s + (long)v, s -> (long)s),
        SUM_F(TypeCode.FLOAT, TypeCode.FLOAT, v -> (float)v, (s, v) -> (float)s + (float)v, s -> (float)s),
        SUM_D(TypeCode.DOUBLE, TypeCode.DOUBLE, v -> (double)v, (s, v) -> (double)s + (double)v, s -> (double)s);

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

        PhysicalOperator(@Nonnull final TypeCode argType,
                         @Nonnull final TypeCode resultType,
                         @Nonnull final UnaryOperator<Object> initialToPartialFunction,
                         @Nonnull final BinaryOperator<Object> partialToPartialFunction,
                         @Nonnull final UnaryOperator<Object> partialToFinalFunction) {
            this.argType = argType;
            this.resultType = resultType;
            this.initialToPartialFunction = initialToPartialFunction;
            this.partialToPartialFunction = partialToPartialFunction;
            this.partialToFinalFunction = partialToFinalFunction;
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
            return object;
        }
    }

    public static class SumAccumulator implements Accumulator {
        private final PhysicalOperator physicalOperator;
        Object state = null;

        public SumAccumulator(@Nonnull final PhysicalOperator physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @SuppressWarnings("unchecked")
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
