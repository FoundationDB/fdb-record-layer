/*
 * VariadicFunctionValue.java
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Value} that applies an arithmetic operation on its child expressions.
 */
@API(API.Status.EXPERIMENTAL)
public class VariadicFunctionValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Scalar-Function-Value");

    @Nonnull
    private final PhysicalOperator operation;
    @Nonnull
    private final List<Value> children;

    @Nonnull
    private static final Supplier<Map<Pair<ScalarFunction, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(VariadicFunctionValue::computeOperatorMap);

    /**
     * Constructs a new instance of {@link VariadicFunctionValue}.
     * @param operation The arithmetic operation.
     * @param children The children.
     */
    public VariadicFunctionValue(@Nonnull PhysicalOperator operation,
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
    public VariadicFunctionValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new VariadicFunctionValue(this.operation, ImmutableList.copyOf(newChildren));
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
        Type resultType = null;
        for (final var arg : arguments) {
            Type argType = arg.getResultType();
            SemanticException.check(argType.isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_ARITHMETIC_OPERATOR_IS_OF_COMPLEX_TYPE);
            if (resultType == null || resultType.isUnresolved()) {
                resultType = argType;
            } else if (!argType.isUnresolved()) {
                resultType = Type.maximumType(resultType, argType);
                SemanticException.check(resultType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }
        }

        final PhysicalOperator physicalOperator = getOperatorMap().get(Pair.of((((ScalarFn)builtInFunction).getScalarFunction()), resultType.getTypeCode()));

        Verify.verifyNotNull(physicalOperator, "unable to encapsulate scalar function due to type mismatch(es)");

        List<Value> promotedArgs = new ArrayList<>();
        for (final var arg: arguments) {
            promotedArgs.add(PromoteValue.inject((Value) arg, resultType));
        }
        return new VariadicFunctionValue(physicalOperator, promotedArgs);
    }

    private static Map<Pair<ScalarFunction, TypeCode>, PhysicalOperator> computeOperatorMap() {
        final ImmutableMap.Builder<Pair<ScalarFunction, TypeCode>, PhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final PhysicalOperator operator : PhysicalOperator.values()) {
            mapBuilder.put(Pair.of(operator.getScalarFunction(), operator.getResultType()), operator);
        }
        return mapBuilder.build();
    }

    private static class ScalarFn extends BuiltInFunction<Value> {
        private final ScalarFunction scalarFunction;

        public ScalarFn(String name, ScalarFunction scalarFunction) {
            super(name, ImmutableList.of(), new Type.Any(), VariadicFunctionValue::encapsulate);
            this.scalarFunction = scalarFunction;
        }

        public ScalarFunction getScalarFunction() {
            return scalarFunction;
        }
    }

    /**
     * The {@code greatest} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class GreatestFn extends ScalarFn {
        public GreatestFn() {
            super("greatest", ScalarFunction.GREATEST);
        }
    }

    /**
     * The {@code least} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class LeastFn extends ScalarFn {
        public LeastFn() {
            super("least", ScalarFunction.LEAST);
        }
    }

    /**
     * The {@code coalesce} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class CoalesceFn extends ScalarFn {
        public CoalesceFn() {
            super("coalesce", ScalarFunction.COALESCE);
        }
    }

    /**
     * Logical operator.
     */
    public enum ScalarFunction {
        GREATEST,
        LEAST,
        COALESCE;
    }

    /**
     * Physical operators.
     */
    @VisibleForTesting
    @SuppressWarnings({"PMD.ControlStatementBraces", "checkstyle:NeedBraces"})
    public enum PhysicalOperator {
        GREATEST_INT(ScalarFunction.GREATEST, TypeCode.INT, args -> {
            int max = Integer.MIN_VALUE;
            for (Object i : args) {
                if (i == null) return null;
                if ((int) i > max) max = (int) i;
            }
            return max;
        }),
        GREATEST_LONG(ScalarFunction.GREATEST, TypeCode.LONG, args -> {
            long max = Long.MIN_VALUE;
            for (Object l : args) {
                if (l == null) return null;
                if ((long) l > max) max = (long) l;
            }
            return max;
        }),
        GREATEST_BOOLEAN(ScalarFunction.GREATEST, TypeCode.BOOLEAN, args -> {
            boolean max = false;
            for (Object b : args) {
                if (b == null) return null;
                if ((boolean) b) max = true;
            }
            return max;
        }),
        GREATEST_STRING(ScalarFunction.GREATEST, TypeCode.STRING, args -> {
            String max = (String) args.get(0);
            for (Object s : args) {
                if (s == null) return null;
                if (((String) s).compareTo(max) > 0) max = (String) s;
            }
            return max;
        }),
        GREATEST_FLOAT(ScalarFunction.GREATEST, TypeCode.FLOAT, args -> {
            float max = Float.MIN_VALUE;
            for (Object f : args) {
                if (f == null) return null;
                if ((float) f > max) max = (float) f;
            }
            return max;
        }),
        GREATEST_DOUBLE(ScalarFunction.GREATEST, TypeCode.DOUBLE, args -> {
            double max = Double.MIN_VALUE;
            for (Object d : args) {
                if (d == null) return null;
                if ((double) d > max) max = (double) d;
            }
            return max;
        }),

        LEAST_INT(ScalarFunction.LEAST, TypeCode.INT, args -> {
            int min = Integer.MAX_VALUE;
            for (Object i : args) {
                if (i == null) return null;
                if ((int) i < min) min = (int) i;
            }
            return min;
        }),
        LEAST_LONG(ScalarFunction.LEAST, TypeCode.LONG, args -> {
            long min = Long.MAX_VALUE;
            for (Object l : args) {
                if (l == null) return null;
                if ((long) l < min) min = (long) l;
            }
            return min;
        }),
        LEAST_BOOLEAN(ScalarFunction.LEAST, TypeCode.BOOLEAN, args -> {
            boolean min = true;
            for (Object b : args) {
                if (b == null) return null;
                if (!((boolean) b)) min = false;
            }
            return min;
        }),
        LEAST_STRING(ScalarFunction.LEAST, TypeCode.STRING, args -> {
            String min = (String) args.get(0);
            for (Object s : args) {
                if (s == null) return null;
                if (((String) s).compareTo(min) < 0) min = (String) s;
            }
            return min;
        }),
        LEAST_FLOAT(ScalarFunction.LEAST, TypeCode.FLOAT, args -> {
            float min = Float.MAX_VALUE;
            for (Object f : args) {
                if (f == null) return null;
                if ((float) f < min) min = (Float) f;
            }
            return min;
        }),
        LEAST_DOUBLE(ScalarFunction.LEAST, TypeCode.DOUBLE, args -> {
            double min = Double.MAX_VALUE;
            for (Object d : args) {
                if (d == null) return null;
                if ((double) d < min) min = (Double) d;
            }
            return min;
        }),

        COALESCE_INT(ScalarFunction.COALESCE, TypeCode.INT, args -> coalesce(args)),
        COALESCE_LONG(ScalarFunction.COALESCE, TypeCode.LONG, args -> coalesce(args)),
        COALESCE_BOOLEAN(ScalarFunction.COALESCE, TypeCode.BOOLEAN, args -> coalesce(args)),
        COALESCE_STRING(ScalarFunction.COALESCE, TypeCode.STRING, args -> coalesce(args)),
        COALESCE_FLOAT(ScalarFunction.COALESCE, TypeCode.FLOAT, args -> coalesce(args)),
        COALESCE_DOUBLE(ScalarFunction.COALESCE, TypeCode.DOUBLE, args -> coalesce(args));

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
            return evaluateFunction.apply(args);
        }

        private static Object coalesce(final List<Object> args) {
            for (Object i : args) {
                if (i != null) {
                    return i;
                }
            }
            return null;
        }
    }
}
