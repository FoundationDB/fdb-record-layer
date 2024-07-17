/*
 * ArithmeticValue.java
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
import com.apple.foundationdb.record.planprotos.PArithmeticValue;
import com.apple.foundationdb.record.planprotos.PArithmeticValue.PPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Triple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * A {@link Value} that applies an arithmetic operation on its child expressions.
 */
@API(API.Status.EXPERIMENTAL)
public class ArithmeticValue extends AbstractValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Arithmetic-Value");

    @Nonnull
    private final PhysicalOperator operator;
    @Nonnull
    private final Value leftChild;
    @Nonnull
    private final Value rightChild;

    @Nonnull
    private static final Supplier<Map<Triple<LogicalOperator, TypeCode, TypeCode>, PhysicalOperator>> operatorMapSupplier =
            Suppliers.memoize(ArithmeticValue::computeOperatorMap);

    /**
     * Constructs a new instance of {@link ArithmeticValue}.
     * @param operator The arithmetic operation.
     * @param leftChild The left child.
     * @param rightChild The right child.
     */
    public ArithmeticValue(@Nonnull PhysicalOperator operator,
                           @Nonnull Value leftChild,
                           @Nonnull Value rightChild) {
        this.operator = operator;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    @Nonnull
    public LogicalOperator getLogicalOperator() {
        return operator.getLogicalOperator();
    }

    @Nullable
    @Override
    @SuppressWarnings("java:S6213")
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return operator.eval(leftChild.eval(store, context),
                rightChild.eval(store, context));
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "(" + leftChild.explain(formatter) + " " + operator.getLogicalOperator().getInfixNotation() + " " + rightChild.explain(formatter) + ")";
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(operator.getResultTypeCode());
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(leftChild, rightChild);
    }

    @Nonnull
    @Override
    public ArithmeticValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new ArithmeticValue(this.operator,
                Iterables.get(newChildren, 0),
                Iterables.get(newChildren, 1));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, leftChild, rightChild);
    }

    @Nonnull
    @Override
    public BooleanWithConstraint equalsWithoutChildren(@Nonnull final Value other) {
        return super.equalsWithoutChildren(other).filter(ignored -> {
            ArithmeticValue otherArithmetic = (ArithmeticValue)other;
            return operator.equals(otherArithmetic.operator);
        });
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase(Locale.ROOT) + "(" + leftChild + ", " + rightChild + ")";
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
    public PArithmeticValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PArithmeticValue.newBuilder()
                .setOperator(operator.toProto(serializationContext))
                .setLeftChild(leftChild.toValueProto(serializationContext))
                .setRightChild(rightChild.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setArithmeticValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static ArithmeticValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PArithmeticValue arithmeticValueProto) {
        return new ArithmeticValue(PhysicalOperator.fromProto(serializationContext, Objects.requireNonNull(arithmeticValueProto.getOperator())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(arithmeticValueProto.getLeftChild())),
                Value.fromValueProto(serializationContext, Objects.requireNonNull(arithmeticValueProto.getRightChild())));
    }

    @Nonnull
    private static Map<Triple<LogicalOperator, TypeCode, TypeCode>, PhysicalOperator> getOperatorMap() {
        return operatorMapSupplier.get();
    }

    @Nonnull
    private static Value encapsulateInternal(@Nonnull BuiltInFunction<Value> builtInFunction,
                                             @Nonnull final List<? extends Typed> arguments) {
        return encapsulate(builtInFunction.getFunctionName(), arguments);
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final String functionName, @Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() == 2);
        final Typed arg0 = arguments.get(0);
        final Type type0 = arg0.getResultType();
        SemanticException.check(type0.isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_ARITHMETIC_OPERATOR_IS_OF_COMPLEX_TYPE);
        final Typed arg1 = arguments.get(1);
        final Type type1 = arg1.getResultType();
        SemanticException.check(type1.isPrimitive(), SemanticException.ErrorCode.ARGUMENT_TO_ARITHMETIC_OPERATOR_IS_OF_COMPLEX_TYPE);

        final Optional<LogicalOperator> logicalOperatorOptional = Enums.getIfPresent(LogicalOperator.class, functionName.toUpperCase(Locale.ROOT)).toJavaUtil();
        Verify.verify(logicalOperatorOptional.isPresent());
        final LogicalOperator logicalOperator = logicalOperatorOptional.get();

        final PhysicalOperator physicalOperator =
                getOperatorMap().get(Triple.of(logicalOperator, type0.getTypeCode(), type1.getTypeCode()));

        Verify.verifyNotNull(physicalOperator, "unable to encapsulate arithmetic operation due to type mismatch(es)");

        return new ArithmeticValue(physicalOperator, (Value)arg0, (Value)arg1);
    }

    private static Map<Triple<LogicalOperator, TypeCode, TypeCode>, PhysicalOperator> computeOperatorMap() {
        final ImmutableMap.Builder<Triple<LogicalOperator, TypeCode, TypeCode>, PhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final PhysicalOperator operator : PhysicalOperator.values()) {
            mapBuilder.put(Triple.of(operator.getLogicalOperator(), operator.getLeftArgType(), operator.getRightArgType()), operator);
        }
        return mapBuilder.build();
    }

    /**
     * The {@code add} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class AddFn extends BuiltInFunction<Value> {
        public AddFn() {
            super("add",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * The {@code sub} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class SubFn extends BuiltInFunction<Value> {
        public SubFn() {
            super("sub",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * The {@code mul} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MulFn extends BuiltInFunction<Value> {
        public MulFn() {
            super("mul",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * The {@code div} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class DivFn extends BuiltInFunction<Value> {
        public DivFn() {
            super("div",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * The {@code mod} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class ModFn extends BuiltInFunction<Value> {
        public ModFn() {
            super("mod",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * The bitwise {@code or} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class BitOrFn extends BuiltInFunction<Value> {
        public BitOrFn() {
            super("bitor",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * The bitwise {@code and} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class BitAndFn extends BuiltInFunction<Value> {
        public BitAndFn() {
            super("bitand",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * The bitwise {@code xor} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class BitXorFn extends BuiltInFunction<Value> {
        public BitXorFn() {
            super("bitxor",
                    ImmutableList.of(Type.any(), Type.any()), ArithmeticValue::encapsulateInternal);
        }
    }

    /**
     * Logical operator.
     */
    public enum LogicalOperator {
        ADD("+"),
        SUB("-"),
        MUL("*"),
        DIV("/"),
        MOD("%"),
        BITOR("|"),
        BITAND("&"),
        BITXOR("^"),
        ;

        @Nonnull
        private final String infixNotation;

        LogicalOperator(@Nonnull final String infixNotation) {
            this.infixNotation = infixNotation;
        }

        @Nonnull
        public String getInfixNotation() {
            return infixNotation;
        }
    }

    /**
     * Physical operators.
     */
    @VisibleForTesting
    public enum PhysicalOperator {
        ADD_II(LogicalOperator.ADD, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> Math.addExact((int)l, (int)r)),
        ADD_IL(LogicalOperator.ADD, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> Math.addExact((int)l, (long)r)),
        ADD_IF(LogicalOperator.ADD, TypeCode.INT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (int)l + (float)r),
        ADD_ID(LogicalOperator.ADD, TypeCode.INT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (int)l + (double)r),
        ADD_IS(LogicalOperator.ADD, TypeCode.INT, TypeCode.STRING, TypeCode.STRING, (l, r) -> (int)l + (String)r),
        ADD_LI(LogicalOperator.ADD, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> Math.addExact((long)l, (int)r)),
        ADD_LL(LogicalOperator.ADD, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> Math.addExact((long)l, (long)r)),
        ADD_LF(LogicalOperator.ADD, TypeCode.LONG, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (long)l + (float)r),
        ADD_LD(LogicalOperator.ADD, TypeCode.LONG, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (long)l + (double)r),
        ADD_LS(LogicalOperator.ADD, TypeCode.LONG, TypeCode.STRING, TypeCode.STRING, (l, r) -> (long)l + (String)r),
        ADD_FI(LogicalOperator.ADD, TypeCode.FLOAT, TypeCode.INT, TypeCode.FLOAT, (l, r) -> (float)l + (int)r),
        ADD_FL(LogicalOperator.ADD, TypeCode.FLOAT, TypeCode.LONG, TypeCode.FLOAT, (l, r) -> (float)l + (long)r),
        ADD_FF(LogicalOperator.ADD, TypeCode.FLOAT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (float)l + (float)r),
        ADD_FD(LogicalOperator.ADD, TypeCode.FLOAT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (float)l + (double)r),
        ADD_FS(LogicalOperator.ADD, TypeCode.FLOAT, TypeCode.STRING, TypeCode.STRING, (l, r) -> (float)l + (String)r),
        ADD_DI(LogicalOperator.ADD, TypeCode.DOUBLE, TypeCode.INT, TypeCode.DOUBLE, (l, r) -> (double)l + (int)r),
        ADD_DL(LogicalOperator.ADD, TypeCode.DOUBLE, TypeCode.LONG, TypeCode.DOUBLE, (l, r) -> (double)l + (long)r),
        ADD_DF(LogicalOperator.ADD, TypeCode.DOUBLE, TypeCode.FLOAT, TypeCode.DOUBLE, (l, r) -> (double)l + (float)r),
        ADD_DD(LogicalOperator.ADD, TypeCode.DOUBLE, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (double)l + (double)r),
        ADD_DS(LogicalOperator.ADD, TypeCode.DOUBLE, TypeCode.STRING, TypeCode.STRING, (l, r) -> (double)l + (String)r),
        ADD_SI(LogicalOperator.ADD, TypeCode.STRING, TypeCode.INT, TypeCode.STRING, (l, r) -> (String)l + (int)r),
        ADD_SL(LogicalOperator.ADD, TypeCode.STRING, TypeCode.LONG, TypeCode.STRING, (l, r) -> (String)l + (long)r),
        ADD_SF(LogicalOperator.ADD, TypeCode.STRING, TypeCode.FLOAT, TypeCode.STRING, (l, r) -> (String)l + (float)r),
        ADD_SD(LogicalOperator.ADD, TypeCode.STRING, TypeCode.DOUBLE, TypeCode.STRING, (l, r) -> (String)l + (double)r),
        ADD_SS(LogicalOperator.ADD, TypeCode.STRING, TypeCode.STRING, TypeCode.STRING, (l, r) -> l + (String)r),

        SUB_II(LogicalOperator.SUB, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> Math.subtractExact((int)l, (int)r)),
        SUB_IL(LogicalOperator.SUB, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> Math.subtractExact((int)l, (long)r)),
        SUB_IF(LogicalOperator.SUB, TypeCode.INT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (int)l - (float)r),
        SUB_ID(LogicalOperator.SUB, TypeCode.INT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (int)l - (double)r),
        SUB_LI(LogicalOperator.SUB, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> Math.subtractExact((long)l, (int)r)),
        SUB_LL(LogicalOperator.SUB, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> Math.subtractExact((long)l, (long)r)),
        SUB_LF(LogicalOperator.SUB, TypeCode.LONG, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (long)l - (float)r),
        SUB_LD(LogicalOperator.SUB, TypeCode.LONG, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (long)l - (double)r),
        SUB_FI(LogicalOperator.SUB, TypeCode.FLOAT, TypeCode.INT, TypeCode.FLOAT, (l, r) -> (float)l - (int)r),
        SUB_FL(LogicalOperator.SUB, TypeCode.FLOAT, TypeCode.LONG, TypeCode.FLOAT, (l, r) -> (float)l - (long)r),
        SUB_FF(LogicalOperator.SUB, TypeCode.FLOAT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (float)l - (float)r),
        SUB_FD(LogicalOperator.SUB, TypeCode.FLOAT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (float)l - (double)r),
        SUB_DI(LogicalOperator.SUB, TypeCode.DOUBLE, TypeCode.INT, TypeCode.DOUBLE, (l, r) -> (double)l - (int)r),
        SUB_DL(LogicalOperator.SUB, TypeCode.DOUBLE, TypeCode.LONG, TypeCode.DOUBLE, (l, r) -> (double)l - (long)r),
        SUB_DF(LogicalOperator.SUB, TypeCode.DOUBLE, TypeCode.FLOAT, TypeCode.DOUBLE, (l, r) -> (double)l - (float)r),
        SUB_DD(LogicalOperator.SUB, TypeCode.DOUBLE, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (double)l - (double)r),

        MUL_II(LogicalOperator.MUL, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> Math.multiplyExact((int)l, (int)r)),
        MUL_IL(LogicalOperator.MUL, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> Math.multiplyExact((int)l, (long)r)),
        MUL_IF(LogicalOperator.MUL, TypeCode.INT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (int)l * (float)r),
        MUL_ID(LogicalOperator.MUL, TypeCode.INT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (int)l * (double)r),
        MUL_LI(LogicalOperator.MUL, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> Math.multiplyExact((long)l, (long)(int)r)), // keep this Java 8-friendly
        MUL_LL(LogicalOperator.MUL, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> Math.multiplyExact((long)l, (long)r)),
        MUL_LF(LogicalOperator.MUL, TypeCode.LONG, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (long)l * (float)r),
        MUL_LD(LogicalOperator.MUL, TypeCode.LONG, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (long)l * (double)r),
        MUL_FI(LogicalOperator.MUL, TypeCode.FLOAT, TypeCode.INT, TypeCode.FLOAT, (l, r) -> (float)l * (int)r),
        MUL_FL(LogicalOperator.MUL, TypeCode.FLOAT, TypeCode.LONG, TypeCode.FLOAT, (l, r) -> (float)l * (long)r),
        MUL_FF(LogicalOperator.MUL, TypeCode.FLOAT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (float)l * (float)r),
        MUL_FD(LogicalOperator.MUL, TypeCode.FLOAT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (float)l * (double)r),
        MUL_DI(LogicalOperator.MUL, TypeCode.DOUBLE, TypeCode.INT, TypeCode.DOUBLE, (l, r) -> (double)l * (int)r),
        MUL_DL(LogicalOperator.MUL, TypeCode.DOUBLE, TypeCode.LONG, TypeCode.DOUBLE, (l, r) -> (double)l * (long)r),
        MUL_DF(LogicalOperator.MUL, TypeCode.DOUBLE, TypeCode.FLOAT, TypeCode.DOUBLE, (l, r) -> (double)l * (float)r),
        MUL_DD(LogicalOperator.MUL, TypeCode.DOUBLE, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (double)l * (double)r),

        DIV_II(LogicalOperator.DIV, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> (int)l / (int)r),
        DIV_IL(LogicalOperator.DIV, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> (int)l / (long)r),
        DIV_IF(LogicalOperator.DIV, TypeCode.INT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (int)l / (float)r),
        DIV_ID(LogicalOperator.DIV, TypeCode.INT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (int)l / (double)r),
        DIV_LI(LogicalOperator.DIV, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> (long)l / (int)r),
        DIV_LL(LogicalOperator.DIV, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> (long)l / (long)r),
        DIV_LF(LogicalOperator.DIV, TypeCode.LONG, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (long)l / (float)r),
        DIV_LD(LogicalOperator.DIV, TypeCode.LONG, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (long)l / (double)r),
        DIV_FI(LogicalOperator.DIV, TypeCode.FLOAT, TypeCode.INT, TypeCode.FLOAT, (l, r) -> (float)l / (int)r),
        DIV_FL(LogicalOperator.DIV, TypeCode.FLOAT, TypeCode.LONG, TypeCode.FLOAT, (l, r) -> (float)l / (long)r),
        DIV_FF(LogicalOperator.DIV, TypeCode.FLOAT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (float)l / (float)r),
        DIV_FD(LogicalOperator.DIV, TypeCode.FLOAT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (float)l / (double)r),
        DIV_DI(LogicalOperator.DIV, TypeCode.DOUBLE, TypeCode.INT, TypeCode.DOUBLE, (l, r) -> (double)l / (int)r),
        DIV_DL(LogicalOperator.DIV, TypeCode.DOUBLE, TypeCode.LONG, TypeCode.DOUBLE, (l, r) -> (double)l / (long)r),
        DIV_DF(LogicalOperator.DIV, TypeCode.DOUBLE, TypeCode.FLOAT, TypeCode.DOUBLE, (l, r) -> (double)l / (float)r),
        DIV_DD(LogicalOperator.DIV, TypeCode.DOUBLE, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (double)l / (double)r),

        MOD_II(LogicalOperator.MOD, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> (int)l % (int)r),
        MOD_IL(LogicalOperator.MOD, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> (int)l % (long)r),
        MOD_IF(LogicalOperator.MOD, TypeCode.INT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (int)l % (float)r),
        MOD_ID(LogicalOperator.MOD, TypeCode.INT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (int)l % (double)r),
        MOD_LI(LogicalOperator.MOD, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> (long)l % (int)r),
        MOD_LL(LogicalOperator.MOD, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> (long)l % (long)r),
        MOD_LF(LogicalOperator.MOD, TypeCode.LONG, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (long)l % (float)r),
        MOD_LD(LogicalOperator.MOD, TypeCode.LONG, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (long)l % (double)r),
        MOD_FI(LogicalOperator.MOD, TypeCode.FLOAT, TypeCode.INT, TypeCode.FLOAT, (l, r) -> (float)l % (int)r),
        MOD_FL(LogicalOperator.MOD, TypeCode.FLOAT, TypeCode.LONG, TypeCode.FLOAT, (l, r) -> (float)l % (long)r),
        MOD_FF(LogicalOperator.MOD, TypeCode.FLOAT, TypeCode.FLOAT, TypeCode.FLOAT, (l, r) -> (float)l % (float)r),
        MOD_FD(LogicalOperator.MOD, TypeCode.FLOAT, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (float)l % (double)r),
        MOD_DI(LogicalOperator.MOD, TypeCode.DOUBLE, TypeCode.INT, TypeCode.DOUBLE, (l, r) -> (double)l % (int)r),
        MOD_DL(LogicalOperator.MOD, TypeCode.DOUBLE, TypeCode.LONG, TypeCode.DOUBLE, (l, r) -> (double)l % (long)r),
        MOD_DF(LogicalOperator.MOD, TypeCode.DOUBLE, TypeCode.FLOAT, TypeCode.DOUBLE, (l, r) -> (double)l % (float)r),
        MOD_DD(LogicalOperator.MOD, TypeCode.DOUBLE, TypeCode.DOUBLE, TypeCode.DOUBLE, (l, r) -> (double)l % (double)r),

        BITOR_II(LogicalOperator.BITOR, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> (int)l | (int)r),
        BITOR_IL(LogicalOperator.BITOR, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> (int)l | (long)r),
        BITOR_LI(LogicalOperator.BITOR, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> (long)l | (int)r),
        BITOR_LL(LogicalOperator.BITOR, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> (long)l | (long)r),

        BITAND_II(LogicalOperator.BITAND, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> (int)l & (int)r),
        BITAND_IL(LogicalOperator.BITAND, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> (int)l & (long)r),
        BITAND_LI(LogicalOperator.BITAND, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> (long)l & (int)r),
        BITAND_LL(LogicalOperator.BITAND, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> (long)l & (long)r),

        BITXOR_II(LogicalOperator.BITXOR, TypeCode.INT, TypeCode.INT, TypeCode.INT, (l, r) -> (int)l ^ (int)r),
        BITXOR_IL(LogicalOperator.BITXOR, TypeCode.INT, TypeCode.LONG, TypeCode.LONG, (l, r) -> (int)l ^ (long)r),
        BITXOR_LI(LogicalOperator.BITXOR, TypeCode.LONG, TypeCode.INT, TypeCode.LONG, (l, r) -> (long)l ^ (int)r),
        BITXOR_LL(LogicalOperator.BITXOR, TypeCode.LONG, TypeCode.LONG, TypeCode.LONG, (l, r) -> (long)l ^ (long)r),
        ;

        @Nonnull
        private static final Supplier<BiMap<PhysicalOperator, PPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(PhysicalOperator.class,
                        PPhysicalOperator.class));

        @Nonnull
        private final LogicalOperator logicalOperator;

        @Nonnull
        private final TypeCode leftArgType;

        @Nonnull
        private final TypeCode rightArgType;

        @Nonnull
        private final TypeCode resultType;

        @Nonnull
        private final BinaryOperator<Object> evaluateFunction;

        PhysicalOperator(@Nonnull final LogicalOperator logicalOperator,
                         @Nonnull final TypeCode leftArgType,
                         @Nonnull final TypeCode rightArgType,
                         @Nonnull final TypeCode resultType,
                         @Nonnull final BinaryOperator<Object> evaluateFunction) {
            this.logicalOperator = logicalOperator;
            this.leftArgType = leftArgType;
            this.rightArgType = rightArgType;
            this.resultType = resultType;
            this.evaluateFunction = evaluateFunction;
        }

        @Nonnull
        public LogicalOperator getLogicalOperator() {
            return logicalOperator;
        }

        @Nonnull
        public TypeCode getLeftArgType() {
            return leftArgType;
        }

        @Nonnull
        public TypeCode getRightArgType() {
            return rightArgType;
        }

        @Nonnull
        public TypeCode getResultTypeCode() {
            return resultType;
        }

        @Nullable
        public Object eval(@Nullable final Object arg1, @Nullable final Object arg2) {
            if (arg1 == null || arg2 == null) {
                return null;
            }
            return evaluateFunction.apply(arg1, arg2);
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
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PArithmeticValue, ArithmeticValue> {
        @Nonnull
        @Override
        public Class<PArithmeticValue> getProtoMessageClass() {
            return PArithmeticValue.class;
        }

        @Nonnull
        @Override
        public ArithmeticValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PArithmeticValue arithmeticValueProto) {
            return ArithmeticValue.fromProto(serializationContext, arithmeticValueProto);
        }
    }
}
