/*
 * RelOpValue.java
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
import com.apple.foundationdb.record.planprotos.PBinaryRelOpValue;
import com.apple.foundationdb.record.planprotos.PBinaryRelOpValue.PBinaryPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PRelOpValue;
import com.apple.foundationdb.record.planprotos.PUnaryRelOpValue;
import com.apple.foundationdb.record.planprotos.PUnaryRelOpValue.PUnaryPhysicalOperator;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence.Precedence;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A {@link Value} that returns the comparison result between its children.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class RelOpValue extends AbstractValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Rel-Op-Value");

    @Nonnull
    private final String functionName;
    @Nonnull
    private final Comparisons.Type comparisonType;
    @Nonnull
    private final Iterable<? extends Value> children;

    @Nonnull
    private static final Supplier<Map<UnaryComparisonSignature, UnaryPhysicalOperator>> unaryOperatorMapSupplier =
            Suppliers.memoize(RelOpValue::computeUnaryOperatorMap);

    @Nonnull
    private static final Supplier<Map<BinaryComparisonSignature, BinaryPhysicalOperator>> binaryOperatorMapSupplier =
            Suppliers.memoize(RelOpValue::computeBinaryOperatorMap);

    protected RelOpValue(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PRelOpValue relOpValueProto) {
        this(Objects.requireNonNull(relOpValueProto.getFunctionName()),
                Comparisons.Type.fromProto(serializationContext, Objects.requireNonNull(relOpValueProto.getComparisonType())),
                relOpValueProto.getChildrenList().stream().map(valueProto -> Value.fromValueProto(serializationContext, valueProto))
                        .collect(ImmutableList.toImmutableList()));
    }

    /**
     * Creates a new instance of {@link RelOpValue}.
     * @param functionName The function name.
     * @param comparisonType The comparison type.
     * @param children The child expression(s).
     */
    protected RelOpValue(@Nonnull final String functionName,
                         @Nonnull final Comparisons.Type comparisonType,
                         @Nonnull final Iterable<? extends Value> children) {
        Verify.verify(!Iterables.isEmpty(children));
        this.functionName = functionName;
        this.comparisonType = comparisonType;
        this.children = children;
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return children;
    }

    @Nonnull
    public String getFunctionName() {
        return functionName;
    }

    @Nonnull
    public Comparisons.Type getComparisonType() {
        return comparisonType;
    }

    @SuppressWarnings("java:S3776")
    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nullable final TypeRepository typeRepository,
                                                     @Nonnull final CorrelationIdentifier innermostAlias) {
        final Iterator<? extends Value> it = children.iterator();
        int childrenCount = Iterables.size(children);
        if (childrenCount == 1) {
            Value child = children.iterator().next();
            final Set<CorrelationIdentifier> childCorrelatedTo = child.getCorrelatedTo();
            if (!childCorrelatedTo.contains(innermostAlias) && typeRepository != null) {
                // it seems this is a constant expression, try to evaluate it.
                return tryBoxSelfAsConstantPredicate(typeRepository);
            }
            // AFAIU [NOT] NULL are the only unary predicates
            return Optional.of(new ValuePredicate(child, new Comparisons.NullComparison(comparisonType)));
        } else if (childrenCount == 2) {
            // only binary comparison functions are commutative.
            // one side of the relop can be correlated to the innermost alias and only to that one; the other one
            // can be correlated (or not) to anything except the innermostAlias
            final Value leftChild = it.next();
            final Value rightChild = it.next();
            final Set<CorrelationIdentifier> leftChildCorrelatedTo = leftChild.getCorrelatedTo();
            final Set<CorrelationIdentifier> rightChildCorrelatedTo = rightChild.getCorrelatedTo();

            if (leftChildCorrelatedTo.isEmpty() && rightChildCorrelatedTo.isEmpty() && typeRepository != null) {
                return tryBoxSelfAsConstantPredicate(typeRepository);
            }
            if (rightChildCorrelatedTo.contains(innermostAlias) && !leftChildCorrelatedTo.contains(innermostAlias)) {
                // the operands are swapped inside this if branch
                return promoteOperandsAndCreatePredicate(leftChildCorrelatedTo.isEmpty() ? typeRepository : null,
                        rightChild,
                        leftChild,
                        swapBinaryComparisonOperator(comparisonType));
            } else {
                return promoteOperandsAndCreatePredicate(rightChildCorrelatedTo.isEmpty() ? typeRepository : null,
                        leftChild,
                        rightChild,
                        comparisonType);
            }
        }
        return Optional.empty();
    }

    @Nonnull
    @SpotBugsSuppressWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    private static Optional<QueryPredicate> promoteOperandsAndCreatePredicate(@Nullable final TypeRepository typeRepository,
                                                                              @Nonnull Value leftChild,
                                                                              @Nonnull Value rightChild,
                                                                              @Nonnull final Comparisons.Type comparisonType) {
        if (leftChild.getResultType().getTypeCode() == Type.TypeCode.NULL && rightChild.getResultType().getTypeCode() == Type.TypeCode.NULL) {
            if (comparisonType == Comparisons.Type.NOT_DISTINCT_FROM) {
                return Optional.of(ConstantPredicate.TRUE);
            } else if (comparisonType == Comparisons.Type.IS_DISTINCT_FROM) {
                return Optional.of(ConstantPredicate.FALSE);
            }
        }
        // maximumType may return null, but only for non-primitive types which is not possible here
        final var maxtype = Verify.verifyNotNull(Type.maximumType(leftChild.getResultType(), rightChild.getResultType()));

        // inject is idempotent AND does not modify the Value if its result is already max type
        leftChild = PromoteValue.inject(leftChild, maxtype);
        rightChild = PromoteValue.inject(rightChild, maxtype);

        if (typeRepository != null) {
            final Object comparand = rightChild.evalWithoutStore(EvaluationContext.forTypeRepository(typeRepository));
            return comparand == null
                   ? Optional.of(new ConstantPredicate(false))
                   : Optional.of(new ValuePredicate(leftChild, new Comparisons.SimpleComparison(comparisonType, comparand)));
        } else {
            return Optional.of(new ValuePredicate(leftChild, new Comparisons.ValueComparison(comparisonType, rightChild)));
        }
    }

    /**
     * Attempt to compile-time evaluate {@code this} predicate as a constant {@link QueryPredicate}.
     * <br/>
     * <b>Note:</b> doing the compile-time evaluation like this is probably incorrect. We should, instead, have
     * and explicit phase that does compactions, simplifications, and compile-time evaluations as an explicit
     * preprocessing step.
     *
     * @param typeRepository The type repository, used to create an {@link EvaluationContext}.
     * @return if successful, a constant {@link QueryPredicate}, otherwise an empty {@link Optional}.
     */
    @Nonnull
    @SpotBugsSuppressWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    private Optional<QueryPredicate> tryBoxSelfAsConstantPredicate(@Nonnull TypeRepository typeRepository) {
        final Object constantValue = evalWithoutStore(EvaluationContext.forTypeRepository(typeRepository));
        if (constantValue instanceof Boolean) {
            if ((boolean)constantValue) {
                return Optional.of(ConstantPredicate.TRUE);
            } else {
                return Optional.of(ConstantPredicate.FALSE);
            }
        } else if (constantValue == null) {
            return Optional.of(ConstantPredicate.NULL);
        }
        return Optional.empty();
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
    public PRelOpValue toRelOpValueProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRelOpValue.Builder builder = PRelOpValue.newBuilder();
        builder.setFunctionName(functionName);
        builder.setComparisonType(comparisonType.toProto(serializationContext));
        for (final Value child : children) {
            builder.addChildren(child.toValueProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    private static Comparisons.Type swapBinaryComparisonOperator(@Nonnull Comparisons.Type type) {
        switch (type) {
            case EQUALS:
            case NOT_EQUALS:
            case NOT_DISTINCT_FROM:
            case IS_DISTINCT_FROM:
                return type;
            case LESS_THAN:
                return Comparisons.Type.GREATER_THAN;
            case LESS_THAN_OR_EQUALS:
                return Comparisons.Type.GREATER_THAN_OR_EQUALS;
            case GREATER_THAN:
                return Comparisons.Type.LESS_THAN;
            case GREATER_THAN_OR_EQUALS:
                return Comparisons.Type.LESS_THAN_OR_EQUALS;
            default:
                throw new IllegalArgumentException("cannot swap comparison " + type);
        }
    }

    @Nonnull
    private static Value encapsulate(@Nonnull final String functionName,
                                     @Nonnull final Comparisons.Type comparisonType,
                                     @Nonnull final List<? extends Typed> arguments) {
        Verify.verify(arguments.size() == 1 || arguments.size() == 2);
        final Typed arg0 = arguments.get(0);
        final Type res0 = arg0.getResultType();
        SemanticException.check(res0.isPrimitive() || res0.isEnum() || res0.isUuid(), SemanticException.ErrorCode.COMPARAND_TO_COMPARISON_IS_OF_COMPLEX_TYPE);
        if (arguments.size() == 1) {
            final UnaryPhysicalOperator physicalOperator =
                    getUnaryOperatorMap().get(new UnaryComparisonSignature(comparisonType, res0.getTypeCode()));

            Verify.verifyNotNull(physicalOperator, "unable to encapsulate comparison operation due to type mismatch(es)");

            return new UnaryRelOpValue(functionName,
                    comparisonType,
                    arguments.stream().map(Value.class::cast).collect(Collectors.toList()),
                    physicalOperator);
        } else {
            final Typed arg1 = arguments.get(1);
            final Type res1 = arg1.getResultType();
            if ("isDistinctFrom".equals(functionName)) {
                if (res0.getTypeCode() == Type.TypeCode.NULL && res1.getTypeCode() != Type.TypeCode.NULL) {
                    return encapsulate("notNull", Comparisons.Type.NOT_NULL, List.of(arg1));
                } else if (res1.getTypeCode() == Type.TypeCode.NULL && res0.getTypeCode() != Type.TypeCode.NULL) {
                    return encapsulate("notNull", Comparisons.Type.NOT_NULL, List.of(arg0));
                }
            }
            if ("notDistinctFrom".equals(functionName)) {
                if (res0.getTypeCode() == Type.TypeCode.NULL && res1.getTypeCode() != Type.TypeCode.NULL) {
                    return encapsulate("isNull", Comparisons.Type.IS_NULL, List.of(arg1));
                } else if (res1.getTypeCode() == Type.TypeCode.NULL && res0.getTypeCode() != Type.TypeCode.NULL) {
                    return encapsulate("isNull", Comparisons.Type.IS_NULL, List.of(arg0));
                }
            }
            SemanticException.check(res1.isPrimitive() || res1.isEnum() || res1.isUuid(), SemanticException.ErrorCode.COMPARAND_TO_COMPARISON_IS_OF_COMPLEX_TYPE);

            final BinaryPhysicalOperator physicalOperator =
                    getBinaryOperatorMap().get(new BinaryComparisonSignature(comparisonType, res0.getTypeCode(), res1.getTypeCode()));

            Verify.verifyNotNull(physicalOperator, "unable to encapsulate comparison operation due to type mismatch(es)");

            return new BinaryRelOpValue(functionName,
                    comparisonType,
                    arguments.stream().map(Value.class::cast).collect(Collectors.toList()),
                    physicalOperator);
        }
    }

    @Nonnull
    private static Map<UnaryComparisonSignature, UnaryPhysicalOperator> computeUnaryOperatorMap() {
        final ImmutableMap.Builder<UnaryComparisonSignature, UnaryPhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final UnaryPhysicalOperator operator : UnaryPhysicalOperator.values()) {
            mapBuilder.put(new UnaryComparisonSignature(operator.getType(), operator.getArgType()), operator);
        }
        return mapBuilder.build();
    }

    @Nonnull
    private static Map<UnaryComparisonSignature, UnaryPhysicalOperator> getUnaryOperatorMap() {
        return unaryOperatorMapSupplier.get();
    }

    private static Map<BinaryComparisonSignature, BinaryPhysicalOperator> computeBinaryOperatorMap() {
        final ImmutableMap.Builder<BinaryComparisonSignature, BinaryPhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final BinaryPhysicalOperator operator : BinaryPhysicalOperator.values()) {
            mapBuilder.put(new BinaryComparisonSignature(operator.getType(), operator.getLeftArgType(), operator.getRightArgType()), operator);
        }
        return mapBuilder.build();
    }

    @Nonnull
    private static Map<BinaryComparisonSignature, BinaryPhysicalOperator> getBinaryOperatorMap() {
        return binaryOperatorMapSupplier.get();
    }

    /**
     * The {@code equals} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class EqualsFn extends BuiltInFunction<Value> {
        public EqualsFn() {
            super("equals",
                    List.of(new Type.Any(), new Type.Any()), EqualsFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.EQUALS, arguments);
        }
    }

    /**
     * The {@code notequals} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class NotEqualsFn extends BuiltInFunction<Value> {
        public NotEqualsFn() {
            super("notEquals",
                    List.of(new Type.Any(), new Type.Any()), NotEqualsFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.NOT_EQUALS, arguments);
        }
    }

    /**
     * The {@code lt} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class LtFn extends BuiltInFunction<Value> {
        public LtFn() {
            super("lt",
                    List.of(new Type.Any(), new Type.Any()), LtFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.LESS_THAN, arguments);
        }
    }

    /**
     * The {@code lte} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class LteFn extends BuiltInFunction<Value> {
        public LteFn() {
            super("lte",
                    List.of(new Type.Any(), new Type.Any()), LteFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.LESS_THAN_OR_EQUALS, arguments);
        }
    }

    /**
     * The {@code gt} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class GtFn extends BuiltInFunction<Value> {
        public GtFn() {
            super("gt",
                    List.of(new Type.Any(), new Type.Any()), GtFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.GREATER_THAN, arguments);
        }
    }

    /**
     * The {@code gte} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class GteFn extends BuiltInFunction<Value> {
        public GteFn() {
            super("gte",
                    List.of(new Type.Any(), new Type.Any()), GteFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.GREATER_THAN_OR_EQUALS, arguments);
        }
    }

    /**
     * The {@code isNull} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class IsNullFn extends BuiltInFunction<Value> {
        public IsNullFn() {
            super("isNull",
                    List.of(new Type.Any()), IsNullFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.IS_NULL, arguments);
        }
    }

    /**
     * The {@code notNull} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class NotNullFn extends BuiltInFunction<Value> {
        public NotNullFn() {
            super("notNull",
                    List.of(new Type.Any()), NotNullFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.NOT_NULL, arguments);
        }
    }

    /**
     * The {@code isDistinctFrom} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class IsDistinctFromFn extends BuiltInFunction<Value> {
        public IsDistinctFromFn() {
            super("isDistinctFrom",
                    List.of(new Type.Any(), new Type.Any()), IsDistinctFromFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.IS_DISTINCT_FROM, arguments);
        }
    }

    /**
     * The {@code notDistinctFrom} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class NotDistinctFromFn extends BuiltInFunction<Value> {
        public NotDistinctFromFn() {
            super("notDistinctFrom",
                    List.of(new Type.Any(), new Type.Any()), NotDistinctFromFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            return RelOpValue.encapsulate(builtInFunction.getFunctionName(), Comparisons.Type.NOT_DISTINCT_FROM, arguments);
        }
    }

    private enum BinaryPhysicalOperator {
        // TODO think about equality epsilon for floating-point types.
        EQ_BU(Comparisons.Type.EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_BB(Comparisons.Type.EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.BOOLEAN, (l, r) -> (boolean)l == (boolean)r),
        EQ_IU(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_II(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l == (int)r),
        EQ_IL(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l == (long)r),
        EQ_IF(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l == (float)r),
        EQ_ID(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l == (double)r),
        EQ_LU(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_LI(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l == (int)r),
        EQ_LL(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l == (long)r),
        EQ_LF(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l == (float)r),
        EQ_LD(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l == (double)r),
        EQ_FU(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_FI(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l == (int)r),
        EQ_FL(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l == (long)r),
        EQ_FF(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l == (float)r),
        EQ_FD(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l == (double)r),
        EQ_DU(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_DI(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l == (int)r),
        EQ_DL(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l == (long)r),
        EQ_DF(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l == (float)r),
        EQ_DD(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l == (double)r),
        EQ_SU(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_SS(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.STRING, Object::equals), // TODO: locale-aware comparison
        EQ_UU(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_UB(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> null),
        EQ_UI(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> null),
        EQ_UL(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> null),
        EQ_UF(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> null),
        EQ_UD(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> null),
        EQ_US(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> null),
        EQ_UV(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> null),
        EQ_VU(Comparisons.Type.EQUALS, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_VV(Comparisons.Type.EQUALS, Type.TypeCode.VERSION, Type.TypeCode.VERSION, Object::equals),

        NEQ_BU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_BB(Comparisons.Type.NOT_EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.BOOLEAN, (l, r) -> (boolean)l != (boolean)r),
        NEQ_IU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_II(Comparisons.Type.NOT_EQUALS, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l != (int)r),
        NEQ_IL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l != (long)r),
        NEQ_IF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l != (float)r),
        NEQ_ID(Comparisons.Type.NOT_EQUALS, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l != (double)r),
        NEQ_LU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_LI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l != (int)r),
        NEQ_LL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l != (long)r),
        NEQ_LF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l != (float)r),
        NEQ_LD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l != (double)r),
        NEQ_FU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_FI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l != (int)r),
        NEQ_FL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l != (long)r),
        NEQ_FF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l != (float)r),
        NEQ_FD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l != (double)r),
        NEQ_DU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_DI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l != (int)r),
        NEQ_DL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l != (long)r),
        NEQ_DF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l != (float)r),
        NEQ_DD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l != (double)r),
        NEQ_SU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_SS(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.STRING, (l, r) -> !l.equals(r)), // TODO: locale-aware comparison
        NEQ_UU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_UB(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> null),
        NEQ_UI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> null),
        NEQ_UL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> null),
        NEQ_UF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> null),
        NEQ_UD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> null),
        NEQ_US(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> null),
        NEQ_UV(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> null),
        NEQ_VU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_VV(Comparisons.Type.NOT_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.VERSION, (l, r) -> !l.equals(r)),
        LT_IU(Comparisons.Type.LESS_THAN, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_II(Comparisons.Type.LESS_THAN, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l < (int)r),
        LT_IL(Comparisons.Type.LESS_THAN, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l < (long)r),
        LT_IF(Comparisons.Type.LESS_THAN, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l < (float)r),
        LT_ID(Comparisons.Type.LESS_THAN, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l < (double)r),
        LT_LU(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_LI(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l < (int)r),
        LT_LL(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l < (long)r),
        LT_LF(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l < (float)r),
        LT_LD(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l < (double)r),
        LT_FU(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_FI(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l < (int)r),
        LT_FL(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l < (long)r),
        LT_FF(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l < (float)r),
        LT_FD(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l < (double)r),
        LT_DU(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_DI(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l < (int)r),
        LT_DL(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l < (long)r),
        LT_DF(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l < (float)r),
        LT_DD(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l < (double)r),
        LT_SU(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_SS(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.STRING, (l, r) -> ((String)l).compareTo((String)r) < 0), // TODO: locale-aware comparison
        LT_UU(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_UB(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> null),
        LT_UI(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> null),
        LT_UL(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> null),
        LT_UF(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> null),
        LT_UD(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> null),
        LT_US(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> null),
        LT_UV(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> null),
        LT_VU(Comparisons.Type.LESS_THAN, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_VV(Comparisons.Type.LESS_THAN, Type.TypeCode.VERSION, Type.TypeCode.VERSION, (l, r) -> ((FDBRecordVersion)l).compareTo((FDBRecordVersion) r) < 0),
        LTE_IU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_II(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l <= (int)r),
        LTE_IL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l <= (long)r),
        LTE_IF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l <= (float)r),
        LTE_ID(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l <= (double)r),
        LTE_LU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_LI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l <= (int)r),
        LTE_LL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l <= (long)r),
        LTE_LF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l <= (float)r),
        LTE_LD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l <= (double)r),
        LTE_FU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_FI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l <= (int)r),
        LTE_FL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l <= (long)r),
        LTE_FF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l <= (float)r),
        LTE_FD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l <= (double)r),
        LTE_DU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_DI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l <= (int)r),
        LTE_DL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l <= (long)r),
        LTE_DF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l <= (float)r),
        LTE_DD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l <= (double)r),
        LTE_SU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_SS(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.STRING, (l, r) -> ((String)l).compareTo((String)r) <= 0), // TODO: locale-aware comparison
        LTE_UU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_UB(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> null),
        LTE_UI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> null),
        LTE_UL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> null),
        LTE_UF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> null),
        LTE_UD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> null),
        LTE_US(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> null),
        LTE_UV(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> null),
        LTE_VU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_VV(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.VERSION, (l, r) -> ((FDBRecordVersion)l).compareTo((FDBRecordVersion) r) <= 0),
        GT_IU(Comparisons.Type.GREATER_THAN, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_II(Comparisons.Type.GREATER_THAN, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l > (int)r),
        GT_IL(Comparisons.Type.GREATER_THAN, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l > (long)r),
        GT_IF(Comparisons.Type.GREATER_THAN, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l > (float)r),
        GT_ID(Comparisons.Type.GREATER_THAN, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l > (double)r),
        GT_LU(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_LI(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l > (int)r),
        GT_LL(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l > (long)r),
        GT_LF(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l > (float)r),
        GT_LD(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l > (double)r),
        GT_FU(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_FI(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l > (int)r),
        GT_FL(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l > (long)r),
        GT_FF(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l > (float)r),
        GT_FD(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l > (double)r),
        GT_DU(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_DI(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l > (int)r),
        GT_DL(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l > (long)r),
        GT_DF(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l > (float)r),
        GT_DD(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l > (double)r),
        GT_SU(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_SS(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.STRING, (l, r) -> ((String)l).compareTo((String)r) > 0), // TODO: locale-aware comparison
        GT_UU(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_UB(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> null),
        GT_UI(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> null),
        GT_UL(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> null),
        GT_UF(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> null),
        GT_UD(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> null),
        GT_US(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> null),
        GT_UV(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> null),
        GT_VU(Comparisons.Type.GREATER_THAN, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_VV(Comparisons.Type.GREATER_THAN, Type.TypeCode.VERSION, Type.TypeCode.VERSION, (l, r) -> ((FDBRecordVersion)l).compareTo((FDBRecordVersion) r) > 0),
        GTE_IU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_II(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l >= (int)r),
        GTE_IL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l >= (long)r),
        GTE_IF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l >= (float)r),
        GTE_ID(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l >= (double)r),
        GTE_LU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_LI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l >= (int)r),
        GTE_LL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l >= (long)r),
        GTE_LF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l >= (float)r),
        GTE_LD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l >= (double)r),
        GTE_FU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_FI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l >= (int)r),
        GTE_FL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l >= (long)r),
        GTE_FF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l >= (float)r),
        GTE_FD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l >= (double)r),
        GTE_DU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_DI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l >= (int)r),
        GTE_DL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l >= (long)r),
        GTE_DF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l >= (float)r),
        GTE_DD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l >= (double)r),
        GTE_SU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_SS(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.STRING, (l, r) -> ((String)l).compareTo((String)r) >= 0), // TODO: locale-aware comparison
        GTE_UU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_UB(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> null),
        GTE_UI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> null),
        GTE_UL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> null),
        GTE_UF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> null),
        GTE_UD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> null),
        GTE_US(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> null),
        GTE_UV(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> null),
        GTE_VU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_VV(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.VERSION, (l, r) -> ((FDBRecordVersion)l).compareTo((FDBRecordVersion) r) >= 0),

        EQ_BYU(Comparisons.Type.EQUALS, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_BYBY(Comparisons.Type.EQUALS, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.EQUALS, l, r)),
        EQ_UBY(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.BYTES, (l, r) -> null),
        NEQ_BYU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_BYBY(Comparisons.Type.NOT_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_EQUALS, l, r)),
        NEQ_UBY(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.BYTES, (l, r) -> null),
        LT_BYU(Comparisons.Type.LESS_THAN, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_BYBY(Comparisons.Type.LESS_THAN, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN, l, r)),
        LTE_BYU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_BYBY(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, l, r)),
        GT_BYU(Comparisons.Type.GREATER_THAN, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_BYBY(Comparisons.Type.GREATER_THAN, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN, l, r)),
        GTE_BYU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_BYBY(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, l, r)),

        EQ_EE(Comparisons.Type.EQUALS, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.EQUALS, l, r)),
        EQ_ES(Comparisons.Type.EQUALS, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.EQUALS, l, otherValue);
        }),
        EQ_SE(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.EQUALS, otherValue, r);
        }),
        EQ_EU(Comparisons.Type.EQUALS, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_UE(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> null),
        NEQ_EE(Comparisons.Type.NOT_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_EQUALS, l, r)),
        NEQ_ES(Comparisons.Type.NOT_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.NOT_EQUALS, l, otherValue);
        }),
        NEQ_SE(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.EQUALS, otherValue, r);
        }),
        NEQ_EU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_UE(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> null),
        LT_EE(Comparisons.Type.LESS_THAN, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN, l, r)),
        LT_ES(Comparisons.Type.LESS_THAN, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.LESS_THAN, l, otherValue);
        }),
        LT_SE(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.LESS_THAN, otherValue, r);
        }),
        LT_EU(Comparisons.Type.LESS_THAN, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_UE(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> null),
        LTE_EE(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, l, r)),
        LTE_ES(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.LESS_THAN, l, otherValue);
        }),
        LTE_SE(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.LESS_THAN, otherValue, r);
        }),
        LTE_EU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_UE(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> null),
        GT_EE(Comparisons.Type.GREATER_THAN, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN, l, r)),
        GT_ES(Comparisons.Type.GREATER_THAN, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.GREATER_THAN, l, otherValue);
        }),
        GT_SE(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.GREATER_THAN, otherValue, r);
        }),
        GT_EU(Comparisons.Type.GREATER_THAN, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_UE(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> null),
        GTE_EE(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, l, r)),
        GTE_ES(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, l, otherValue);
        }),
        GTE_SE(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, otherValue, r);
        }),
        GTE_EU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_UE(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> null),

        EQ_IDID(Comparisons.Type.EQUALS, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.EQUALS, l, r)),
        EQ_IDS(Comparisons.Type.EQUALS, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.EQUALS, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        EQ_SID(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.EQUALS, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        EQ_UID(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> null),
        EQ_IDU(Comparisons.Type.EQUALS, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_IDID(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_EQUALS, l, r)),
        NEQ_IDS(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_EQUALS, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        NEQ_SID(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_EQUALS, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        NEQ_UID(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> null),
        NEQ_IDU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_IDID(Comparisons.Type.LESS_THAN, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN, l, r)),
        LT_IDS(Comparisons.Type.LESS_THAN, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        LT_SID(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        LT_UID(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> null),
        LT_IDU(Comparisons.Type.LESS_THAN, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_IDID(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, l, r)),
        LTE_IDS(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        LTE_SID(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        LTE_UID(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> null),
        LTE_IDU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_IDID(Comparisons.Type.GREATER_THAN, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN, l, r)),
        GT_IDS(Comparisons.Type.GREATER_THAN, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        GT_SID(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        GT_UID(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> null),
        GT_IDU(Comparisons.Type.GREATER_THAN, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_IDID(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, l, r)),
        GTE_IDS(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        GTE_SID(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        GTE_UID(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> null),
        GTE_IDU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> null),


        EQ_BN(Comparisons.Type.EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.NULL, (l, r) -> null),
        EQ_IN(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.NULL, (l, r) -> null),
        EQ_LN(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.NULL, (l, r) -> null),
        EQ_FN(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.NULL, (l, r) -> null),
        EQ_DN(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.NULL, (l, r) -> null),
        EQ_SN(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.NULL, (l, r) -> null),
        EQ_NN(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> null),
        EQ_NU(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_UN(Comparisons.Type.EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.NULL, (l, r) -> null),
        EQ_NB(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (l, r) -> null),
        EQ_NI(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.INT, (l, r) -> null),
        EQ_NL(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.LONG, (l, r) -> null),
        EQ_NF(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.FLOAT, (l, r) -> null),
        EQ_ND(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (l, r) -> null),
        EQ_NS(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.STRING, (l, r) -> null),
        EQ_NV(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.VERSION, (l, r) -> null),
        EQ_VN(Comparisons.Type.EQUALS, Type.TypeCode.VERSION, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_BN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_IN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.INT, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_LN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_FN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_DN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_SN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_NN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_UN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_NU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_NB(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (l, r) -> null),
        NEQ_NI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.INT, (l, r) -> null),
        NEQ_NL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.LONG, (l, r) -> null),
        NEQ_NF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.FLOAT, (l, r) -> null),
        NEQ_ND(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (l, r) -> null),
        NEQ_NS(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.STRING, (l, r) -> null),
        NEQ_NV(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.VERSION, (l, r) -> null),
        NEQ_VN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.NULL, (l, r) -> null),
        LT_IN(Comparisons.Type.LESS_THAN, Type.TypeCode.INT, Type.TypeCode.NULL, (l, r) -> null),
        LT_LN(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.NULL, (l, r) -> null),
        LT_FN(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.NULL, (l, r) -> null),
        LT_DN(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.NULL, (l, r) -> null),
        LT_SN(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.NULL, (l, r) -> null),
        LT_NN(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> null),
        LT_UN(Comparisons.Type.LESS_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.NULL, (l, r) -> null),
        LT_NU(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_NB(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (l, r) -> null),
        LT_NI(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.INT, (l, r) -> null),
        LT_NL(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.LONG, (l, r) -> null),
        LT_NF(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.FLOAT, (l, r) -> null),
        LT_ND(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (l, r) -> null),
        LT_NS(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.STRING, (l, r) -> null),
        LT_NV(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.VERSION, (l, r) -> null),
        LT_VN(Comparisons.Type.LESS_THAN, Type.TypeCode.VERSION, Type.TypeCode.NULL, (l, r) -> null),
        LTE_IN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.NULL, (l, r) -> null),
        LTE_LN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.NULL, (l, r) -> null),
        LTE_FN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.NULL, (l, r) -> null),
        LTE_DN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.NULL, (l, r) -> null),
        LTE_SN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.NULL, (l, r) -> null),
        LTE_NN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> null),
        LTE_UN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.NULL, (l, r) -> null),
        LTE_NU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_NB(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (l, r) -> null),
        LTE_NI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.INT, (l, r) -> null),
        LTE_NL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.LONG, (l, r) -> null),
        LTE_NF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.FLOAT, (l, r) -> null),
        LTE_ND(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (l, r) -> null),
        LTE_NS(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.STRING, (l, r) -> null),
        LTE_NV(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.VERSION, (l, r) -> null),
        LTE_VN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.NULL, (l, r) -> null),
        GT_IN(Comparisons.Type.GREATER_THAN, Type.TypeCode.INT, Type.TypeCode.NULL, (l, r) -> null),
        GT_LN(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.NULL, (l, r) -> null),
        GT_FN(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.NULL, (l, r) -> null),
        GT_DN(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.NULL, (l, r) -> null),
        GT_SN(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.NULL, (l, r) -> null),
        GT_NN(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> null),
        GT_UN(Comparisons.Type.GREATER_THAN, Type.TypeCode.UNKNOWN, Type.TypeCode.NULL, (l, r) -> null),
        GT_NU(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_NB(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (l, r) -> null),
        GT_NI(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.INT, (l, r) -> null),
        GT_NL(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.LONG, (l, r) -> null),
        GT_NF(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.FLOAT, (l, r) -> null),
        GT_ND(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (l, r) -> null),
        GT_NS(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.STRING, (l, r) -> null),
        GT_NV(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.VERSION, (l, r) -> null),
        GT_VN(Comparisons.Type.GREATER_THAN, Type.TypeCode.VERSION, Type.TypeCode.NULL, (l, r) -> null),
        GTE_IN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.NULL, (l, r) -> null),
        GTE_LN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.NULL, (l, r) -> null),
        GTE_FN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.NULL, (l, r) -> null),
        GTE_DN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.NULL, (l, r) -> null),
        GTE_SN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.NULL, (l, r) -> null),
        GTE_NN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> null),
        GTE_NU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_UN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UNKNOWN, Type.TypeCode.NULL, (l, r) -> null),
        GTE_NB(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.BOOLEAN, (l, r) -> null),
        GTE_NI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.INT, (l, r) -> null),
        GTE_NL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.LONG, (l, r) -> null),
        GTE_NF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.FLOAT, (l, r) -> null),
        GTE_ND(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.DOUBLE, (l, r) -> null),
        GTE_NS(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.STRING, (l, r) -> null),
        GTE_NV(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.VERSION, (l, r) -> null),
        GTE_VN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.VERSION, Type.TypeCode.NULL, (l, r) -> null),
        EQ_BYN(Comparisons.Type.EQUALS, Type.TypeCode.BYTES, Type.TypeCode.NULL, (l, r) -> null),
        EQ_NBY(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.BYTES, (l, r) -> null),
        NEQ_BYN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_NBY(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.BYTES, (l, r) -> null),
        LT_BYN(Comparisons.Type.LESS_THAN, Type.TypeCode.BYTES, Type.TypeCode.NULL, (l, r) -> null),
        LTE_BYN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.NULL, (l, r) -> null),
        GT_BYN(Comparisons.Type.GREATER_THAN, Type.TypeCode.BYTES, Type.TypeCode.NULL, (l, r) -> null),
        GTE_BYN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.BYTES, Type.TypeCode.NULL, (l, r) -> null),
        EQ_EN(Comparisons.Type.EQUALS, Type.TypeCode.ENUM, Type.TypeCode.NULL, (l, r) -> null),
        EQ_NE(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.ENUM, (l, r) -> null),
        NEQ_EN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_NE(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.ENUM, (l, r) -> null),
        LT_EN(Comparisons.Type.LESS_THAN, Type.TypeCode.ENUM, Type.TypeCode.NULL, (l, r) -> null),
        LT_NE(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.ENUM, (l, r) -> null),
        LTE_EN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.NULL, (l, r) -> null),
        LTE_NE(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.ENUM, (l, r) -> null),
        GT_EN(Comparisons.Type.GREATER_THAN, Type.TypeCode.ENUM, Type.TypeCode.NULL, (l, r) -> null),
        GT_NE(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.ENUM, (l, r) -> null),
        GTE_EN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.ENUM, Type.TypeCode.NULL, (l, r) -> null),
        GTE_NE(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.ENUM, (l, r) -> null),
        EQ_NID(Comparisons.Type.EQUALS, Type.TypeCode.NULL, Type.TypeCode.UUID, (l, r) -> null),
        EQ_IDN(Comparisons.Type.EQUALS, Type.TypeCode.UUID, Type.TypeCode.NULL, (l, r) -> null),
        NEQ_NID(Comparisons.Type.NOT_EQUALS, Type.TypeCode.NULL, Type.TypeCode.UUID, (l, r) -> null),
        NEQ_IDN(Comparisons.Type.NOT_EQUALS, Type.TypeCode.UUID, Type.TypeCode.NULL, (l, r) -> null),
        LT_NID(Comparisons.Type.LESS_THAN, Type.TypeCode.NULL, Type.TypeCode.UUID, (l, r) -> null),
        LT_IDN(Comparisons.Type.LESS_THAN, Type.TypeCode.UUID, Type.TypeCode.NULL, (l, r) -> null),
        LTE_NID(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.UUID, (l, r) -> null),
        LTE_IDN(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.NULL, (l, r) -> null),
        GT_NID(Comparisons.Type.GREATER_THAN, Type.TypeCode.NULL, Type.TypeCode.UUID, (l, r) -> null),
        GT_IDN(Comparisons.Type.GREATER_THAN, Type.TypeCode.UUID, Type.TypeCode.NULL, (l, r) -> null),
        GTE_NID(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.NULL, Type.TypeCode.UUID, (l, r) -> null),
        GTE_IDN(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.UUID, Type.TypeCode.NULL, (l, r) -> null),

        IS_DISTINCT_FROM_BU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.BOOLEAN, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_BB(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.BOOLEAN, Type.TypeCode.BOOLEAN, (l, r) -> (boolean)l != (boolean)r),
        IS_DISTINCT_FROM_IU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_II(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l != (int)r),
        IS_DISTINCT_FROM_IL(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l != (long)r),
        IS_DISTINCT_FROM_IF(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l != (float)r),
        IS_DISTINCT_FROM_ID(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l != (double)r),
        IS_DISTINCT_FROM_LU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_LI(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l != (int)r),
        IS_DISTINCT_FROM_LL(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l != (long)r),
        IS_DISTINCT_FROM_LF(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l != (float)r),
        IS_DISTINCT_FROM_LD(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l != (double)r),
        IS_DISTINCT_FROM_FU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_FI(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l != (int)r),
        IS_DISTINCT_FROM_FL(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l != (long)r),
        IS_DISTINCT_FROM_FF(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l != (float)r),
        IS_DISTINCT_FROM_FD(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l != (double)r),
        IS_DISTINCT_FROM_DU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_DI(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l != (int)r),
        IS_DISTINCT_FROM_DL(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l != (long)r),
        IS_DISTINCT_FROM_DF(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l != (float)r),
        IS_DISTINCT_FROM_DD(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l != (double)r),
        IS_DISTINCT_FROM_SU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_SS(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.STRING, (l, r) -> !l.equals(r)), // TODO: locale-aware comparison
        IS_DISTINCT_FROM_UU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> false),
        IS_DISTINCT_FROM_UB(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> true),
        IS_DISTINCT_FROM_UI(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> true),
        IS_DISTINCT_FROM_UL(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> true),
        IS_DISTINCT_FROM_UF(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> true),
        IS_DISTINCT_FROM_UD(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> true),
        IS_DISTINCT_FROM_US(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> true),
        IS_DISTINCT_FROM_UV(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> true),
        IS_DISTINCT_FROM_VU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> null),
        IS_DISTINCT_FROM_VV(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.VERSION, Type.TypeCode.VERSION, (l, r) -> !l.equals(r)),
        IS_DISTINCT_FROM_BYU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> null),
        IS_DISTINCT_FROM_BYBY(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.IS_DISTINCT_FROM, l, r)),
        IS_DISTINCT_FROM_UBY(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.BYTES, (l, r) -> true),
        IS_DISTINCT_FROM_EE(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.IS_DISTINCT_FROM, l, r)),
        IS_DISTINCT_FROM_ES(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.IS_DISTINCT_FROM, l, otherValue);
        }),
        IS_DISTINCT_FROM_SE(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.EQUALS, otherValue, r);
        }),
        IS_DISTINCT_FROM_EU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_UE(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> true),
        IS_DISTINCT_FROM_IDID(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.IS_DISTINCT_FROM, l, r)),
        IS_DISTINCT_FROM_IDS(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.IS_DISTINCT_FROM, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        IS_DISTINCT_FROM_SID(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.IS_DISTINCT_FROM, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        IS_DISTINCT_FROM_UID(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> true),
        IS_DISTINCT_FROM_IDU(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> true),
        IS_DISTINCT_FROM_NN(Comparisons.Type.IS_DISTINCT_FROM, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> false),

        NOT_DISTINCT_FROM_BU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.BOOLEAN, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_BB(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.BOOLEAN, Type.TypeCode.BOOLEAN, (l, r) -> (boolean)l == (boolean)r),
        NOT_DISTINCT_FROM_IU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_II(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l == (int)r),
        NOT_DISTINCT_FROM_IL(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l == (long)r),
        NOT_DISTINCT_FROM_IF(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l == (float)r),
        NOT_DISTINCT_FROM_ID(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l == (double)r),
        NOT_DISTINCT_FROM_LU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_LI(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l == (int)r),
        NOT_DISTINCT_FROM_LL(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l == (long)r),
        NOT_DISTINCT_FROM_LF(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l == (float)r),
        NOT_DISTINCT_FROM_LD(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l == (double)r),
        NOT_DISTINCT_FROM_FU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_FI(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l == (int)r),
        NOT_DISTINCT_FROM_FL(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l == (long)r),
        NOT_DISTINCT_FROM_FF(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l == (float)r),
        NOT_DISTINCT_FROM_FD(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l == (double)r),
        NOT_DISTINCT_FROM_DU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_DI(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l == (int)r),
        NOT_DISTINCT_FROM_DL(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l == (long)r),
        NOT_DISTINCT_FROM_DF(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l == (float)r),
        NOT_DISTINCT_FROM_DD(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l == (double)r),
        NOT_DISTINCT_FROM_SU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_SS(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.STRING, Object::equals), // TODO: locale-aware comparison
        NOT_DISTINCT_FROM_UU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.UNKNOWN, (l, r) -> true),
        NOT_DISTINCT_FROM_UB(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.BOOLEAN, (l, r) -> false),
        NOT_DISTINCT_FROM_UI(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.INT, (l, r) -> false),
        NOT_DISTINCT_FROM_UL(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.LONG, (l, r) -> false),
        NOT_DISTINCT_FROM_UF(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.FLOAT, (l, r) -> false),
        NOT_DISTINCT_FROM_UD(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.DOUBLE, (l, r) -> false),
        NOT_DISTINCT_FROM_US(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.STRING, (l, r) -> false),
        NOT_DISTINCT_FROM_UV(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.VERSION, (l, r) -> false),
        NOT_DISTINCT_FROM_VU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.VERSION, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_VV(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.VERSION, Type.TypeCode.VERSION, Object::equals),

        NOT_DISTINCT_FROM_BYU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.BYTES, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_BYBY(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.BYTES, Type.TypeCode.BYTES, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_DISTINCT_FROM, l, r)),
        NOT_DISTINCT_FROM_UBY(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.BYTES, (l, r) -> false),

        NOT_DISTINCT_FROM_EE(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.ENUM, Type.TypeCode.ENUM, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_DISTINCT_FROM, l, r)),
        NOT_DISTINCT_FROM_ES(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.ENUM, Type.TypeCode.STRING, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) l).getType(), (String) r);
            return Comparisons.evalComparison(Comparisons.Type.NOT_DISTINCT_FROM, l, otherValue);
        }),
        NOT_DISTINCT_FROM_SE(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.ENUM, (l, r) -> {
            final var otherValue = PromoteValue.PhysicalOperator.stringToEnumValue(((Descriptors.EnumValueDescriptor) r).getType(), (String) l);
            return Comparisons.evalComparison(Comparisons.Type.NOT_DISTINCT_FROM, otherValue, r);
        }),
        NOT_DISTINCT_FROM_EU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.ENUM, Type.TypeCode.UNKNOWN, (l, r) -> false),
        NOT_DISTINCT_FROM_UE(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.ENUM, (l, r) -> false),

        NOT_DISTINCT_FROM_IDID(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UUID, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_DISTINCT_FROM, l, r)),
        NOT_DISTINCT_FROM_IDS(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UUID, Type.TypeCode.STRING, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_DISTINCT_FROM, l, PromoteValue.PhysicalOperator.stringToUuidValue((String) r))),
        NOT_DISTINCT_FROM_SID(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.STRING, Type.TypeCode.UUID, (l, r) -> Comparisons.evalComparison(Comparisons.Type.NOT_DISTINCT_FROM, PromoteValue.PhysicalOperator.stringToUuidValue((String) l), r)),
        NOT_DISTINCT_FROM_UID(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UNKNOWN, Type.TypeCode.UUID, (l, r) -> false),
        NOT_DISTINCT_FROM_IDU(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.UUID, Type.TypeCode.UNKNOWN, (l, r) -> false),

        NOT_DISTINCT_FROM_NN(Comparisons.Type.NOT_DISTINCT_FROM, Type.TypeCode.NULL, Type.TypeCode.NULL, (l, r) -> true),

        ;
        // We can pass down UUID or String till here.

        @Nonnull
        private static final Supplier<BiMap<BinaryPhysicalOperator, PBinaryPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(BinaryPhysicalOperator.class,
                        PBinaryPhysicalOperator.class));

        @Nonnull
        private final Comparisons.Type type;

        @Nonnull
        private final Type.TypeCode leftArgType;

        @Nonnull
        private final Type.TypeCode rightArgType;

        @Nonnull
        private final BinaryOperator<Object> evaluateFunction;

        BinaryPhysicalOperator(@Nonnull Comparisons.Type type,
                               @Nonnull Type.TypeCode leftArgType,
                               @Nonnull Type.TypeCode rightArgType,
                               @Nonnull BinaryOperator<Object> evaluateFunction) {
            this.type = type;
            this.leftArgType = leftArgType;
            this.rightArgType = rightArgType;
            this.evaluateFunction = evaluateFunction;
        }

        @Nullable
        public Object eval(@Nullable final Object arg1, @Nullable final Object arg2) {
            if (arg1 == null || arg2 == null) {
                return null;
            }
            return evaluateFunction.apply(arg1, arg2);
        }

        @Nonnull
        public Comparisons.Type getType() {
            return type;
        }

        @Nonnull
        public Type.TypeCode getLeftArgType() {
            return leftArgType;
        }

        @Nonnull
        public Type.TypeCode getRightArgType() {
            return rightArgType;
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PBinaryPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static BinaryPhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PBinaryPhysicalOperator binaryPhysicalOperatorProto) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(binaryPhysicalOperatorProto));
        }

        @Nonnull
        private static BiMap<BinaryPhysicalOperator, PBinaryPhysicalOperator> getProtoEnumBiMap() {
            return protoEnumBiMapSupplier.get();
        }
    }

    private enum UnaryPhysicalOperator {
        IS_NULL_UI(Comparisons.Type.IS_NULL, Type.TypeCode.UNKNOWN, Objects::isNull),
        IS_NULL_II(Comparisons.Type.IS_NULL, Type.TypeCode.INT, Objects::isNull),
        IS_NULL_LI(Comparisons.Type.IS_NULL, Type.TypeCode.LONG, Objects::isNull),
        IS_NULL_FI(Comparisons.Type.IS_NULL, Type.TypeCode.FLOAT, Objects::isNull),
        IS_NULL_DI(Comparisons.Type.IS_NULL, Type.TypeCode.DOUBLE, Objects::isNull),
        IS_NULL_SS(Comparisons.Type.IS_NULL, Type.TypeCode.STRING, Objects::isNull),
        IS_NULL_BI(Comparisons.Type.IS_NULL, Type.TypeCode.BOOLEAN, Objects::isNull),

        IS_NOT_NULL_UI(Comparisons.Type.NOT_NULL, Type.TypeCode.UNKNOWN, Objects::nonNull),
        IS_NOT_NULL_II(Comparisons.Type.NOT_NULL, Type.TypeCode.INT, Objects::nonNull),
        IS_NOT_NULL_LI(Comparisons.Type.NOT_NULL, Type.TypeCode.LONG, Objects::nonNull),
        IS_NOT_NULL_FI(Comparisons.Type.NOT_NULL, Type.TypeCode.FLOAT, Objects::nonNull),
        IS_NOT_NULL_DI(Comparisons.Type.NOT_NULL, Type.TypeCode.DOUBLE, Objects::nonNull),
        IS_NOT_NULL_SS(Comparisons.Type.NOT_NULL, Type.TypeCode.STRING, Objects::nonNull),
        IS_NOT_NULL_BI(Comparisons.Type.NOT_NULL, Type.TypeCode.BOOLEAN, Objects::nonNull),

        IS_NULL_BY(Comparisons.Type.IS_NULL, Type.TypeCode.BYTES, Objects::isNull),
        IS_NOT_NULL_BY(Comparisons.Type.NOT_NULL, Type.TypeCode.BYTES, Objects::nonNull),

        IS_NULL_EI(Comparisons.Type.IS_NULL, Type.TypeCode.ENUM, Objects::isNull),
        IS_NOT_NULL_EI(Comparisons.Type.NOT_NULL, Type.TypeCode.ENUM, Objects::nonNull),

        IS_NULL_ID(Comparisons.Type.IS_NULL, Type.TypeCode.UUID, Objects::isNull),
        IS_NOT_NULL_ID(Comparisons.Type.NOT_NULL, Type.TypeCode.UUID, Objects::nonNull);

        @Nonnull
        private static final Supplier<BiMap<UnaryPhysicalOperator, PUnaryPhysicalOperator>> protoEnumBiMapSupplier =
                Suppliers.memoize(() -> PlanSerialization.protoEnumBiMap(UnaryPhysicalOperator.class,
                        PUnaryPhysicalOperator.class));

        @Nonnull
        private final Comparisons.Type type;

        @Nonnull
        private final Type.TypeCode argType;

        @Nonnull
        private final UnaryOperator<Object> evaluateFunction;

        UnaryPhysicalOperator(@Nonnull Comparisons.Type type, @Nonnull Type.TypeCode argType, @Nonnull UnaryOperator<Object> evaluateFunction) {
            this.type = type;
            this.argType = argType;
            this.evaluateFunction = evaluateFunction;
        }

        @Nullable
        public Object eval(@Nullable final Object arg1) {
            return evaluateFunction.apply(arg1);
        }

        @Nonnull
        public Comparisons.Type getType() {
            return type;
        }

        @Nonnull
        public Type.TypeCode getArgType() {
            return argType;
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PUnaryPhysicalOperator toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return Objects.requireNonNull(getProtoEnumBiMap().get(this));
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static UnaryPhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PUnaryPhysicalOperator unaryPhysicalOperator) {
            return Objects.requireNonNull(getProtoEnumBiMap().inverse().get(unaryPhysicalOperator));
        }

        @Nonnull
        private static BiMap<UnaryPhysicalOperator, PUnaryPhysicalOperator> getProtoEnumBiMap() {
            return protoEnumBiMapSupplier.get();
        }
    }

    /**
     * Binary rel ops.
     */
    public static class BinaryRelOpValue extends RelOpValue {
        @Nonnull
        private final BinaryPhysicalOperator operator;

        private BinaryRelOpValue(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PBinaryRelOpValue binaryRelOpValueProto) {
            super(serializationContext, Objects.requireNonNull(binaryRelOpValueProto.getSuper()));
            this.operator = BinaryPhysicalOperator.fromProto(serializationContext, Objects.requireNonNull(binaryRelOpValueProto.getOperator()));
        }

        private BinaryRelOpValue(@Nonnull final String functionName,
                                 @Nonnull final Comparisons.Type comparisonType,
                                 @Nonnull final Iterable<? extends Value> children,
                                 @Nonnull final BinaryPhysicalOperator operator) {
            super(functionName, comparisonType, children);
            this.operator = operator;
        }

        @Nonnull
        @Override
        public RelOpValue withChildren(final Iterable<? extends Value> newChildren) {
            Verify.verify(Iterables.size(newChildren) == 2);
            return new BinaryRelOpValue(getFunctionName(),
                    getComparisonType(),
                    newChildren,
                    operator);
        }

        @Override
        public int hashCodeWithoutChildren() {
            return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, getComparisonType(), operator);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            // TODO incorporate the physical operator into a new plan hash mode
            return PlanHashable.objectsPlanHash(mode, BASE_HASH, getComparisonType(),
                    StreamSupport.stream(getChildren().spliterator(), false).toArray(Value[]::new));
        }

        @Nullable
        @Override
        public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            final var evaluatedChildrenIterator =
                    Streams.stream(getChildren())
                            .map(child -> child.eval(store, context))
                            .iterator();

            return operator.eval(evaluatedChildrenIterator.next(), evaluatedChildrenIterator.next());
        }

        @Nonnull
        @Override
        public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
            final var left = Iterables.get(explainSuppliers, 0).get();
            final var right = Iterables.get(explainSuppliers, 1).get();
            return ExplainTokensWithPrecedence.of(Precedence.COMPARISONS,
                    Precedence.COMPARISONS.parenthesizeChild(left).addWhitespace().addToString(getFunctionName())
                            .addWhitespace().addNested(Precedence.COMPARISONS.parenthesizeChild(right)));
        }

        @Nonnull
        @Override
        public PBinaryRelOpValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PBinaryRelOpValue.newBuilder()
                    .setSuper(toRelOpValueProto(serializationContext))
                    .setOperator(operator.toProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setBinaryRelOpValue(toProto(serializationContext)).build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static BinaryRelOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PBinaryRelOpValue binaryRelOpValueProto) {
            return new BinaryRelOpValue(serializationContext, binaryRelOpValueProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PBinaryRelOpValue, BinaryRelOpValue> {
            @Nonnull
            @Override
            public Class<PBinaryRelOpValue> getProtoMessageClass() {
                return PBinaryRelOpValue.class;
            }

            @Nonnull
            @Override
            public BinaryRelOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PBinaryRelOpValue binaryRelOpValueProto) {
                return BinaryRelOpValue.fromProto(serializationContext, binaryRelOpValueProto);
            }
        }
    }

    /**
     * Unary rel ops.
     */
    public static class UnaryRelOpValue extends RelOpValue {
        @Nonnull
        private final UnaryPhysicalOperator operator;

        private UnaryRelOpValue(@Nonnull final PlanSerializationContext serializationContext,
                                @Nonnull final PUnaryRelOpValue unaryRelOpValueProto) {
            super(serializationContext, Objects.requireNonNull(unaryRelOpValueProto.getSuper()));
            this.operator = UnaryPhysicalOperator.fromProto(serializationContext, Objects.requireNonNull(unaryRelOpValueProto.getOperator()));
        }

        private UnaryRelOpValue(@Nonnull final String functionName,
                                @Nonnull final Comparisons.Type comparisonType,
                                @Nonnull final Iterable<? extends Value> children,
                                @Nonnull final UnaryPhysicalOperator operator) {
            super(functionName, comparisonType, children);
            this.operator = operator;
        }

        @Nonnull
        @Override
        public RelOpValue withChildren(final Iterable<? extends Value> newChildren) {
            Verify.verify(Iterables.size(newChildren) == 1);
            return new UnaryRelOpValue(getFunctionName(),
                    getComparisonType(),
                    newChildren,
                    operator);
        }

        @Override
        public int hashCodeWithoutChildren() {
            return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, getComparisonType(), operator);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            // TODO incorporate the physical operator into a new plan hash mode
            return PlanHashable.objectsPlanHash(mode, BASE_HASH, getComparisonType(),
                    StreamSupport.stream(getChildren().spliterator(), false).toArray(Value[]::new));
        }

        @Nullable
        @Override
        public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            final var evaluatedChildrenIterator =
                    Streams.stream(getChildren())
                            .map(child -> child.eval(store, context))
                            .iterator();

            return operator.eval(evaluatedChildrenIterator.next());
        }

        @Nonnull
        @Override
        public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
            final var onlyChild = Iterables.getOnlyElement(explainSuppliers).get();
            return ExplainTokensWithPrecedence.of(Precedence.UNARY_MINUS_BITWISE_NOT,
                    new ExplainTokens().addToString(getFunctionName())
                            .addNested(Precedence.UNARY_MINUS_BITWISE_NOT.parenthesizeChild(onlyChild)));
        }

        @Nonnull
        @Override
        public PUnaryRelOpValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PUnaryRelOpValue.newBuilder()
                    .setSuper(toRelOpValueProto(serializationContext))
                    .setOperator(operator.toProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PValue.newBuilder().setUnaryRelOpValue(toProto(serializationContext)).build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static UnaryRelOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PUnaryRelOpValue unaryRelOpValueProto) {
            return new UnaryRelOpValue(serializationContext, unaryRelOpValueProto);
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PUnaryRelOpValue, UnaryRelOpValue> {
            @Nonnull
            @Override
            public Class<PUnaryRelOpValue> getProtoMessageClass() {
                return PUnaryRelOpValue.class;
            }

            @Nonnull
            @Override
            public UnaryRelOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PUnaryRelOpValue unaryRelOpValueProto) {
                return UnaryRelOpValue.fromProto(serializationContext, unaryRelOpValueProto);
            }
        }
    }

    static final class UnaryComparisonSignature {

        @Nonnull
        private final Comparisons.Type comparisonType;
        @Nonnull
        private final Type.TypeCode argumentType;

        UnaryComparisonSignature(@Nonnull Comparisons.Type comparisonType, @Nonnull Type.TypeCode argumentType) {
            this.comparisonType = comparisonType;
            this.argumentType = argumentType;
        }

        @Nonnull
        public Comparisons.Type getComparisonType() {
            return comparisonType;
        }

        @Nonnull
        public Type.TypeCode getArgumentType() {
            return argumentType;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final UnaryComparisonSignature that = (UnaryComparisonSignature)o;
            return comparisonType == that.comparisonType && argumentType == that.argumentType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(comparisonType, argumentType);
        }

        @Override
        public String toString() {
            return  comparisonType + "(" + argumentType + ")";
        }
    }

    static final class BinaryComparisonSignature {
        @Nonnull
        private final Comparisons.Type comparisonType;
        @Nonnull
        private final Type.TypeCode leftType;
        @Nonnull
        private final Type.TypeCode rightType;

        BinaryComparisonSignature(@Nonnull Comparisons.Type comparisonType, @Nonnull Type.TypeCode leftType, @Nonnull Type.TypeCode rightType) {
            this.comparisonType = comparisonType;
            this.leftType = leftType;
            this.rightType = rightType;
        }

        @Nonnull
        public Comparisons.Type getComparisonType() {
            return comparisonType;
        }

        @Nonnull
        public Type.TypeCode getLeftType() {
            return leftType;
        }

        @Nonnull
        public Type.TypeCode getRightType() {
            return rightType;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final BinaryComparisonSignature that = (BinaryComparisonSignature)o;
            return comparisonType == that.comparisonType && leftType == that.leftType && rightType == that.rightType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(comparisonType, leftType, rightType);
        }

        @Override
        public String toString() {
            return comparisonType + "(" + leftType + ", " + rightType + ")";
        }
    }
}
