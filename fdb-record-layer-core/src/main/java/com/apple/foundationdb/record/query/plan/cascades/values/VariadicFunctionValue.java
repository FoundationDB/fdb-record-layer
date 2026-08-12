/*
 * VariadicFunctionValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.planprotos.PVariadicFunctionValue;
import com.apple.foundationdb.record.planprotos.PVariadicFunctionValue.PPhysicalOperator;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
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
import com.google.common.collect.Streams;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Value} that applies a variadic comparison function, such as {@code GREATEST()}, {@code LEAST()} or
 * {@code COALESCE()}, to its child expressions.
 */
@API(API.Status.EXPERIMENTAL)
public class VariadicFunctionValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Variadic-Function-Value");

    @Nonnull
    private final PhysicalOperator operator;
    @Nonnull
    private final List<Value> children;
    @Nonnull
    private final Type resultType;

    @Nonnull
    private static final Supplier<Map<NonnullPair<ComparisonFunction, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(VariadicFunctionValue::computeOperatorMap);

    /**
     * Constructs a new instance of {@link VariadicFunctionValue}.
     * @param operator The physical operator implementing the comparison function.
     * @param children The children.
     * @param resultType The result type, which must be consistent with the children's types.
     */
    private VariadicFunctionValue(@Nonnull final PhysicalOperator operator,
                                  @Nonnull final ImmutableList<Value> children,
                                  @Nonnull final Type resultType) {
        this.operator = operator;
        this.children = children;
        this.resultType = resultType;
    }

    /**
     * Creates a new instance of {@link VariadicFunctionValue}, deriving the result type from the given children.
     * @param operator The physical operator implementing the comparison function.
     * @param children The children, of which there must be at least two.
     * @return a new {@link VariadicFunctionValue}
     */
    @Nonnull
    public static VariadicFunctionValue of(@Nonnull final PhysicalOperator operator,
                                           @Nonnull final Iterable<? extends Value> children) {
        final ImmutableList<Value> childrenList = ImmutableList.copyOf(children);
        final Type resultType = computeResultType(operator, childrenList);
        return new VariadicFunctionValue(operator, childrenList, resultType);
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return operator.eval(children.stream().map(c -> c.eval(store, context)).collect(Collectors.toList()));
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    private static Type computeResultType(@Nonnull final PhysicalOperator operator,
                                          @Nonnull final List<Value> children) {
        // Verify that all children have a suitable common type, which is done by injecting promotions in
        // `encapsulate()`. The argument types may only differ in their nullability, which is combined into the
        // nullability of the result according to the semantics of the comparison function at hand.
        Verify.verify(children.size() >= 2, "`VariadicFunctionValue` must have at least two children");
        final Type maximumType = children.get(0).getResultType();
        for (final Value child : children.subList(1, children.size())) {
            final Type childType = child.getResultType();
            Verify.verify(childType.nullable().equals(maximumType.nullable()),
                    "`VariadicFunctionValue` children must share a common type, but %s differs from %s",
                    childType, maximumType);
        }
        return maximumType.withNullability(operator.getComparisonFunction().isResultNullable(children));
    }

    @Nonnull
    public ComparisonFunction getComparisonFunction() {
        return operator.getComparisonFunction();
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Nonnull
    @Override
    public VariadicFunctionValue withChildren(final Iterable<? extends Value> newChildren) {
        return VariadicFunctionValue.of(this.operator, newChildren);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, children);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        return ExplainTokensWithPrecedence.of(new ExplainTokens()
                .addFunctionCall(operator.name().toLowerCase(Locale.ROOT),
                        Value.explainFunctionArguments(explainSuppliers)));
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
        final ImmutableList<Value> children = childrenBuilder.build();
        final PhysicalOperator operator = PhysicalOperator.fromProto(
                serializationContext,
                Objects.requireNonNull(variadicFunctionValueProto.getOperator()));
        return VariadicFunctionValue.of(operator, children);
    }

    @Nonnull
    private static Map<NonnullPair<ComparisonFunction, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
    private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction,
                                     @Nonnull final CallSiteArguments callSiteArguments) {
        // Determine the common type of the arguments, rejecting the call if they are mutually incompatible.
        //
        // All arguments must have a resolved type, with the exception that some (though not all) of them may be of
        // the unresolved `NullType` (e.g., a NULL literal). `Type.maximumType()` handles such a `NullType` correctly
        // by returning the appropriate concrete type with its nullability set to true. If *every* argument is NULL,
        // the overall maximum type will be `NullType` and the operator lookup below will fail.
        final List<? extends Typed> arguments = callSiteArguments.getArgumentsList();
        Verify.verify(arguments.size() >= 2);
        Type commonType = arguments.get(0).getResultType();
        Verify.verify(commonType.isNull() || !commonType.isUnresolved());
        for (final Typed arg : arguments.subList(1, arguments.size())) {
            final Type argType = arg.getResultType();
            Verify.verify(argType.isNull() || !argType.isUnresolved());
            final Type maximumType = Type.maximumType(commonType, argType);
            SemanticException.check(maximumType != null, SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            commonType = maximumType;
        }

        // Look up the physical operator implementing the comparison function for the common type of the arguments.
        final ComparisonFunction comparisonFunction = ((ComparisonFn)builtInFunction).getComparisonFunction();
        final PhysicalOperator physicalOperator =
                getOperatorMap().get(NonnullPair.of(comparisonFunction, commonType.getTypeCode()));
        SemanticException.check(
                physicalOperator != null,
                SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);

        // Determine the result type with the appropriate nullability for the comparison function at hand.
        final Type resultType = commonType.withNullability(comparisonFunction.isResultNullable(arguments));

        // Promote each argument to the common type while retaining its own nullability, so that the information about
        // which arguments are nullable is not lost.
        final ImmutableList.Builder<Value> promotedArguments = ImmutableList.builder();
        for (final Typed arg : arguments) {
            final Type promoteToType = resultType.withNullability(arg.getResultType().isNullable());
            promotedArguments.add(PromoteValue.inject((Value)arg, promoteToType));
        }
        final ImmutableList<Value> children = promotedArguments.build();

        return new VariadicFunctionValue(physicalOperator, children, resultType);
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
     * The comparison function that a {@link VariadicFunctionValue} applies. Each function defines how the nullabilities
     * of the arguments combine into the nullability of the result.
     */
    public enum ComparisonFunction {
        /**
         * The {@code GREATEST()} function. Returns {@code NULL} if any of its arguments is {@code NULL}.
         */
        GREATEST,
        /**
         * The {@code LEAST()} function. Returns {@code NULL} if any of its arguments is {@code NULL}.
         */
        LEAST,
        /**
         * The {@code COALESCE()} function. Returns its first non-{@code NULL} argument; so it is nullable only if
         * all its arguments are nullable.
         */
        COALESCE(Boolean::logicalAnd),
        ;

        @Nonnull
        private final BinaryOperator<Boolean> nullabilityCombiner;

        /**
         * Constructs a new instance of {@link ComparisonFunction} whose result is nullable if any of its arguments is
         * nullable, which is the most common case.
         */
        ComparisonFunction() {
            this(Boolean::logicalOr);
        }

        /**
         * Constructs a new instance of {@link ComparisonFunction}.
         *
         * <p>The given {@code nullabilityCombiner} combines the nullabilities of the arguments to derive the
         * nullability of the result.
         */
        ComparisonFunction(@Nonnull final BinaryOperator<Boolean> nullabilityCombiner) {
            this.nullabilityCombiner = nullabilityCombiner;
        }

        /**
         * Computes whether the result of this function is nullable, given its arguments.
         * @param arguments the arguments this function is applied to
         * @return {@code true} if the result of this function is nullable, {@code false} otherwise
         */
        public boolean isResultNullable(@Nonnull final Iterable<? extends Typed> arguments) {
            return Streams.stream(arguments)
                    .map(argument -> argument.getResultType().isNullable())
                    .reduce(nullabilityCombiner)
                    .orElseThrow(() -> new RecordCoreException("function must have at least one argument"));
        }
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
