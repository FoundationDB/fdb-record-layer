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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.planprotos.PVariadicFunctionValue;
import com.apple.foundationdb.record.planprotos.PVariadicFunctionValue.PPhysicalOperator;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Value} that applies an arithmetic operation on its child expressions.
 */
@API(API.Status.EXPERIMENTAL)
public class VariadicFunctionValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Variadic-Function-Value");

    @Nonnull
    private final PhysicalOperator operator;
    @Nonnull
    private final List<Value> children;

    @Nonnull
    private static final Supplier<Map<NonnullPair<ComparisonFunction, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(VariadicFunctionValue::computeOperatorMap);

    /**
     * Constructs a new instance of {@link VariadicFunctionValue}.
     * @param operator The arithmetic operation.
     * @param children The children.
     */
    public VariadicFunctionValue(@Nonnull PhysicalOperator operator,
                                 @Nonnull List<Value> children) {
        this.operator = operator;
        this.children = ImmutableList.copyOf(children);
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return operator.eval(children.stream().map(c -> c.eval(store, context)).collect(Collectors.toList()));
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return operator.name().toLowerCase(Locale.ROOT) + "(" + children.stream().map(c -> c.explain(formatter)).collect(Collectors.joining(",")) + ")";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return children.get(0).getResultType();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Nonnull
    @Override
    public VariadicFunctionValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) >= 2);
        return new VariadicFunctionValue(this.operator, ImmutableList.copyOf(newChildren));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, children);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase(Locale.ROOT) + "(" + children.stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
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
    @Override
    public PVariadicFunctionValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PVariadicFunctionValue.Builder builder = PVariadicFunctionValue.newBuilder();

        builder.setOperator(operator.toProto(serializationContext));
        for (final Value child : children) {
            builder.addChildren(child.toValueProto(serializationContext));
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setVariadicFunctionValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static VariadicFunctionValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PVariadicFunctionValue variadicFunctionValueProto) {
        final ImmutableList.Builder<Value> childrenBuilder = ImmutableList.builder();
        for (int i = 0; i < variadicFunctionValueProto.getChildrenCount(); i ++) {
            final Value child = Value.fromValueProto(serializationContext, variadicFunctionValueProto.getChildren(i));
            childrenBuilder.add(child);
        }
        return new VariadicFunctionValue(PhysicalOperator.fromProto(serializationContext, Objects.requireNonNull(variadicFunctionValueProto.getOperator())),
                childrenBuilder.build());
    }

    @Nonnull
    private static Map<NonnullPair<ComparisonFunction, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
    private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction,
                                     @Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() >= 2);
        Type resultType = null;
        for (final var arg : arguments) {
            Type argType = arg.getResultType();
            if (resultType == null || resultType.isUnresolved()) {
                resultType = argType;
            } else if (!argType.isUnresolved()) {
                resultType = Type.maximumType(resultType, argType);
                SemanticException.check(resultType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }
        }

        final PhysicalOperator physicalOperator =
                getOperatorMap()
                        .get(NonnullPair.of((((ComparisonFn)builtInFunction).getComparisonFunction()), resultType.getTypeCode()));
        SemanticException.check(physicalOperator != null, SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);

        final ImmutableList.Builder<Value> promotedArgs = ImmutableList.builder();
        for (final var arg: arguments) {
            promotedArgs.add(PromoteValue.inject((Value) arg, resultType));
        }
        return new VariadicFunctionValue(physicalOperator, promotedArgs.build());
    }

    private static Map<NonnullPair<ComparisonFunction, TypeCode>, PhysicalOperator> computeOperatorMap() {
        final ImmutableMap.Builder<NonnullPair<ComparisonFunction, TypeCode>, PhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final PhysicalOperator operator : PhysicalOperator.values()) {
            mapBuilder.put(NonnullPair.of(operator.getComparisonFunction(), operator.getResultType()), operator);
        }
        return mapBuilder.build();
    }

    private static class ComparisonFn extends BuiltInFunction<Value> {
        private final ComparisonFunction comparisonFunction;

        public ComparisonFn(String name, ComparisonFunction comparisonFunction) {
            super(name, ImmutableList.of(), new Type.Any(), VariadicFunctionValue::encapsulate);
            this.comparisonFunction = comparisonFunction;
        }

        public ComparisonFunction getComparisonFunction() {
            return comparisonFunction;
        }
    }

    /**
     * The {@code greatest} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class GreatestFn extends ComparisonFn {
        public GreatestFn() {
            super("greatest", ComparisonFunction.GREATEST);
        }
    }

    /**
     * The {@code least} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class LeastFn extends ComparisonFn {
        public LeastFn() {
            super("least", ComparisonFunction.LEAST);
        }
    }

    /**
     * The {@code coalesce} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class CoalesceFn extends ComparisonFn {
        public CoalesceFn() {
            super("coalesce", ComparisonFunction.COALESCE);
        }
    }

    /**
     * Logical operator.
     */
    public enum ComparisonFunction {
        GREATEST,
        LEAST,
        COALESCE
    }

    /**
     * Physical operators.
     */
    @VisibleForTesting
    @SuppressWarnings({"PMD.ControlStatementBraces", "checkstyle:NeedBraces"})
    public enum PhysicalOperator {
        GREATEST_INT(ComparisonFunction.GREATEST, TypeCode.INT, args -> {
            int max = Integer.MIN_VALUE;
            for (Object i : args) {
                if (i == null) return null;
                if ((int) i > max) max = (int) i;
            }
            return max;
        }),
        GREATEST_LONG(ComparisonFunction.GREATEST, TypeCode.LONG, args -> {
            long max = Long.MIN_VALUE;
            for (Object l : args) {
                if (l == null) return null;
                if ((long) l > max) max = (long) l;
            }
            return max;
        }),
        GREATEST_BOOLEAN(ComparisonFunction.GREATEST, TypeCode.BOOLEAN, args -> {
            boolean max = false;
            for (Object b : args) {
                if (b == null) return null;
                if ((boolean) b) max = true;
            }
            return max;
        }),
        GREATEST_STRING(ComparisonFunction.GREATEST, TypeCode.STRING, args -> {
            String max = (String) args.get(0);
            for (Object s : args) {
                if (s == null) return null;
                if (((String) s).compareTo(max) > 0) max = (String) s;
            }
            return max;
        }),
        GREATEST_FLOAT(ComparisonFunction.GREATEST, TypeCode.FLOAT, args -> {
            float max = Float.MIN_VALUE;
            for (Object f : args) {
                if (f == null) return null;
                if ((float) f > max) max = (float) f;
            }
            return max;
        }),
        GREATEST_DOUBLE(ComparisonFunction.GREATEST, TypeCode.DOUBLE, args -> {
            double max = Double.MIN_VALUE;
            for (Object d : args) {
                if (d == null) return null;
                if ((double) d > max) max = (double) d;
            }
            return max;
        }),

        LEAST_INT(ComparisonFunction.LEAST, TypeCode.INT, args -> {
            int min = Integer.MAX_VALUE;
            for (Object i : args) {
                if (i == null) return null;
                if ((int) i < min) min = (int) i;
            }
            return min;
        }),
        LEAST_LONG(ComparisonFunction.LEAST, TypeCode.LONG, args -> {
            long min = Long.MAX_VALUE;
            for (Object l : args) {
                if (l == null) return null;
                if ((long) l < min) min = (long) l;
            }
            return min;
        }),
        LEAST_BOOLEAN(ComparisonFunction.LEAST, TypeCode.BOOLEAN, args -> {
            boolean min = true;
            for (Object b : args) {
                if (b == null) return null;
                if (!((boolean) b)) min = false;
            }
            return min;
        }),
        LEAST_STRING(ComparisonFunction.LEAST, TypeCode.STRING, args -> {
            String min = (String) args.get(0);
            for (Object s : args) {
                if (s == null) return null;
                if (((String) s).compareTo(min) < 0) min = (String) s;
            }
            return min;
        }),
        LEAST_FLOAT(ComparisonFunction.LEAST, TypeCode.FLOAT, args -> {
            float min = Float.MAX_VALUE;
            for (Object f : args) {
                if (f == null) return null;
                if ((float) f < min) min = (Float) f;
            }
            return min;
        }),
        LEAST_DOUBLE(ComparisonFunction.LEAST, TypeCode.DOUBLE, args -> {
            double min = Double.MAX_VALUE;
            for (Object d : args) {
                if (d == null) return null;
                if ((double) d < min) min = (Double) d;
            }
            return min;
        }),

        COALESCE_INT(ComparisonFunction.COALESCE, TypeCode.INT, PhysicalOperator::coalesce),
        COALESCE_LONG(ComparisonFunction.COALESCE, TypeCode.LONG, PhysicalOperator::coalesce),
        COALESCE_BOOLEAN(ComparisonFunction.COALESCE, TypeCode.BOOLEAN, PhysicalOperator::coalesce),
        COALESCE_STRING(ComparisonFunction.COALESCE, TypeCode.STRING, PhysicalOperator::coalesce),
        COALESCE_FLOAT(ComparisonFunction.COALESCE, TypeCode.FLOAT, PhysicalOperator::coalesce),
        COALESCE_DOUBLE(ComparisonFunction.COALESCE, TypeCode.DOUBLE, PhysicalOperator::coalesce),
        COALESCE_RECORD(ComparisonFunction.COALESCE, TypeCode.RECORD, PhysicalOperator::coalesce),
        COALESCE_ARRAY(ComparisonFunction.COALESCE, TypeCode.ARRAY, PhysicalOperator::coalesce);

        @Nonnull
        private static final Supplier<BiMap<PhysicalOperator, PPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(PhysicalOperator.class, PPhysicalOperator.class));

        @Nonnull
        private final ComparisonFunction comparisonFunction;

        @Nonnull
        private final TypeCode type;

        @Nonnull
        private final transient Function<List<Object>, Object> evaluateFunction;

        PhysicalOperator(@Nonnull final ComparisonFunction comparisonFunction,
                         @Nonnull final TypeCode type,
                         @Nonnull final Function<List<Object>, Object> evaluateFunction) {
            this.comparisonFunction = comparisonFunction;
            this.type = type;
            this.evaluateFunction = evaluateFunction;
        }

        @Nonnull
        public ComparisonFunction getComparisonFunction() {
            return comparisonFunction;
        }

        @Nonnull
        public TypeCode getResultType() {
            return type;
        }

        @Nullable
        public Object eval(List<Object> args) {
            return evaluateFunction.apply(args);
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

        private static Object coalesce(final List<Object> args) {
            for (Object i : args) {
                if (i != null) {
                    return i;
                }
            }
            return null;
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PVariadicFunctionValue, VariadicFunctionValue> {
        @Nonnull
        @Override
        public Class<PVariadicFunctionValue> getProtoMessageClass() {
            return PVariadicFunctionValue.class;
        }

        @Nonnull
        @Override
        public VariadicFunctionValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                               @Nonnull final PVariadicFunctionValue variadicFunctionValueProto) {
            return VariadicFunctionValue.fromProto(serializationContext, variadicFunctionValueProto);
        }
    }
}
