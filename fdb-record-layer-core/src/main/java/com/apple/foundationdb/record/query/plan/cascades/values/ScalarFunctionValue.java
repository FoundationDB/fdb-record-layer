/*
 * ScalarFunctionValue.java
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
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Value} that applies an arithmetic operation on its child expressions.
 */
@API(API.Status.EXPERIMENTAL)
public class ScalarFunctionValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Scalar-Function-Value");

    @Nonnull
    private final PhysicalOperator operation;
    @Nonnull
    private final List<Value> children;

    @Nonnull
    private static final Supplier<Map<Pair<ScalarFunction, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(ScalarFunctionValue::computeOperatorMap);

    /**
     * Constructs a new instance of {@link ScalarFunctionValue}.
     * @param operation The arithmetic operation.
     * @param children The children.
     */
    public ScalarFunctionValue(@Nonnull PhysicalOperator operation,
                               @Nonnull List<Value> children) {
        this.operation = operation;
        this.children = children;
    }

    @Nonnull
    public ScalarFunction getScalarFunction() {
        return operation.getScalarFunction();
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return operation.eval(children.stream().map(c -> c.eval(store, context)).collect(Collectors.toList()));
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return operation.name().toLowerCase(Locale.getDefault()) + "(" + children.stream().map(c -> c.explain(formatter)).collect(Collectors.joining(",")) + ")";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(operation.getResultType());
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return ImmutableList.copyOf(children);
    }

    @Nonnull
    @Override
    public ScalarFunctionValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new ScalarFunctionValue(this.operation, ImmutableList.copyOf(newChildren));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, operation);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, operation, children);
    }

    @Override
    public String toString() {
        return operation.name().toLowerCase(Locale.getDefault()) + "(" + children.stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
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
    private static Map<Pair<ScalarFunction, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction,
                                     @Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() >= 2);
        Type resultType = arguments.get(0).getResultType();
        for (final var arg : arguments) {
            Type argType = arg.getResultType();
            SemanticException.check(argType.isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_ARITHMETIC_OPERATOR_IS_OF_COMPLEX_TYPE);
            if (resultType.isUnresolved()) {
                resultType = argType;
            } else if (!argType.isUnresolved()) {
                resultType = Type.maximumType(resultType, argType);
            }
            SemanticException.check(resultType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        }

        final Optional<ScalarFunction> scalarFunction = Enums.getIfPresent(ScalarFunction.class, builtInFunction.getFunctionName().toUpperCase(Locale.getDefault())).toJavaUtil();
        Verify.verify(scalarFunction.isPresent());

        final PhysicalOperator physicalOperator = getOperatorMap().get(Pair.of(scalarFunction.get(), resultType.getTypeCode()));

        Verify.verifyNotNull(physicalOperator, "unable to encapsulate scalar function due to type mismatch(es)");

        List<Value> promotedArgs = new ArrayList<>();
        for (final var arg: arguments) {
            promotedArgs.add(PromoteValue.inject((Value) arg, resultType));
        }
        return new ScalarFunctionValue(physicalOperator, promotedArgs);
    }

    private static Map<Pair<ScalarFunction, TypeCode>, PhysicalOperator> computeOperatorMap() {
        final ImmutableMap.Builder<Pair<ScalarFunction, TypeCode>, PhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final PhysicalOperator operator : PhysicalOperator.values()) {
            mapBuilder.put(Pair.of(operator.getScalarFunction(), operator.getResultType()), operator);
        }
        return mapBuilder.build();
    }

    /**
     * The {@code greatest} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class GreatestFn extends BuiltInFunction<Value> {
        public GreatestFn() {
            super("greatest",
                    ImmutableList.of(), new Type.Any(), ScalarFunctionValue::encapsulate);
        }
    }

    /**
     * The {@code least} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class LeastFn extends BuiltInFunction<Value> {
        public LeastFn() {
            super("least",
                    ImmutableList.of(), new Type.Any(), ScalarFunctionValue::encapsulate);
        }
    }

    /**
     * Logical operator.
     */
    public enum ScalarFunction {
        GREATEST,
        LEAST;
    }

    /**
     * Physical operators.
     */
    @VisibleForTesting
    public enum PhysicalOperator {
        GREATEST_INT(ScalarFunction.GREATEST, TypeCode.INT, args -> greatest(args, Integer.class)),
        GREATEST_LONG(ScalarFunction.GREATEST, TypeCode.LONG, args -> greatest(args, Long.class)),
        GREATEST_BOOLEAN(ScalarFunction.GREATEST, TypeCode.BOOLEAN, args -> greatest(args, Boolean.class)),
        GREATEST_STRING(ScalarFunction.GREATEST, TypeCode.STRING, args -> greatest(args, String.class)),
        GREATEST_FLOAT(ScalarFunction.GREATEST, TypeCode.FLOAT, args -> greatest(args, Float.class)),
        GREATEST_DOUBLE(ScalarFunction.GREATEST, TypeCode.DOUBLE, args -> greatest(args, Double.class)),

        LEAST_INT(ScalarFunction.LEAST, TypeCode.INT, args -> least(args, Integer.class)),
        LEAST_LONG(ScalarFunction.LEAST, TypeCode.LONG, args -> least(args, Long.class)),
        LEAST_BOOLEAN(ScalarFunction.LEAST, TypeCode.BOOLEAN, args -> least(args, Boolean.class)),
        LEAST_STRING(ScalarFunction.LEAST, TypeCode.STRING, args -> least(args, String.class)),
        LEAST_FLOAT(ScalarFunction.LEAST, TypeCode.FLOAT, args -> least(args, Float.class)),
        LEAST_DOUBLE(ScalarFunction.LEAST, TypeCode.DOUBLE, args -> least(args, Double.class));

        @Nonnull
        private final ScalarFunction scalarFunction;

        @Nonnull
        private final TypeCode type;

        @Nonnull
        private final Function<List<Object>, Object> evaluateFunction;

        PhysicalOperator(@Nonnull final ScalarFunction scalarFunction,
                         @Nonnull final TypeCode type,
                         @Nonnull final Function<List<Object>, Object> evaluateFunction) {
            this.scalarFunction = scalarFunction;
            this.type = type;
            this.evaluateFunction = evaluateFunction;
        }

        @Nonnull
        public ScalarFunction getScalarFunction() {
            return scalarFunction;
        }

        @Nonnull
        public TypeCode getResultType() {
            return type;
        }

        @Nullable
        public Object eval(List<Object> args) {
            if (args.contains(null)) {
                return null;
            }
            return evaluateFunction.apply(args);
        }

        @SuppressWarnings("unchecked")
        private static <T extends Comparable<T>> T greatest(final List<Object> args, Class<T> clazz) {
            T max = (T) args.get(0);
            for (Object i : args) {
                if (((T) i).compareTo(max) > 0) {
                    max = (T) i;
                }
            }
            return max;
        }

        @SuppressWarnings("unchecked")
        private static <T extends Comparable<T>> T least(final List<Object> args, Class<T> clazz) {
            T min = (T) args.get(0);
            for (Object i : args) {
                if (((T) i).compareTo(min) < 0) {
                    min = (T) i;
                }
            }
            return min;
        }
    }
}
