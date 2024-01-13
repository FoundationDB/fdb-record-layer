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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordQueryPlanProto;
import com.apple.foundationdb.record.RecordQueryPlanProto.PBinaryRelOpValue;
import com.apple.foundationdb.record.RecordQueryPlanProto.PBinaryRelOpValue.PBinaryPhysicalOperator;
import com.apple.foundationdb.record.RecordQueryPlanProto.PRelOpValue;
import com.apple.foundationdb.record.RecordQueryPlanProto.PUnaryRelOpValue;
import com.apple.foundationdb.record.RecordQueryPlanProto.PUnaryRelOpValue.PUnaryPhysicalOperator;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.annotation.ProtoMessage;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

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
    private static final Supplier<Map<Pair<Comparisons.Type, Type.TypeCode>, UnaryPhysicalOperator>> unaryOperatorMapSupplier =
            Suppliers.memoize(RelOpValue::computeUnaryOperatorMap);

    @Nonnull
    private static final Supplier<Map<Triple<Comparisons.Type, Type.TypeCode, Type.TypeCode>, BinaryPhysicalOperator>> binaryOperatorMapSupplier =
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
    public Iterable<? extends Value> getChildren() {
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
        // maximumType may return null, but only for non-primitive types which is not possible here
        final var maxtype = Verify.verifyNotNull(Type.maximumType(leftChild.getResultType(), rightChild.getResultType()));

        // inject is idempotent AND does not modify the Value if its result is already max type
        leftChild = PromoteValue.inject(leftChild, maxtype);
        rightChild = PromoteValue.inject(rightChild, maxtype);

        if (typeRepository != null) {
            final Object comparand = rightChild.compileTimeEval(EvaluationContext.forTypeRepository(typeRepository));
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
        final Object constantValue = compileTimeEval(EvaluationContext.forTypeRepository(typeRepository));
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
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
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
        SemanticException.check(res0.isPrimitive(), SemanticException.ErrorCode.COMPARAND_TO_COMPARISON_IS_OF_COMPLEX_TYPE);
        if (arguments.size() == 1) {
            final UnaryPhysicalOperator physicalOperator =
                    getUnaryOperatorMap().get(Pair.of(comparisonType, res0.getTypeCode()));

            Verify.verifyNotNull(physicalOperator, "unable to encapsulate comparison operation due to type mismatch(es)");

            return new UnaryRelOpValue(functionName,
                    comparisonType,
                    arguments.stream().map(Value.class::cast).collect(Collectors.toList()),
                    physicalOperator);
        } else {
            final Typed arg1 = arguments.get(1);
            final Type res1 = arg1.getResultType();
            SemanticException.check(res1.isPrimitive(), SemanticException.ErrorCode.COMPARAND_TO_COMPARISON_IS_OF_COMPLEX_TYPE);

            final BinaryPhysicalOperator physicalOperator =
                    getBinaryOperatorMap().get(Triple.of(comparisonType, res0.getTypeCode(), res1.getTypeCode()));

            Verify.verifyNotNull(physicalOperator, "unable to encapsulate comparison operation due to type mismatch(es)");

            return new BinaryRelOpValue(functionName,
                    comparisonType,
                    arguments.stream().map(Value.class::cast).collect(Collectors.toList()),
                    physicalOperator);
        }
    }

    @Nonnull
    private static Map<Pair<Comparisons.Type, Type.TypeCode>, UnaryPhysicalOperator> computeUnaryOperatorMap() {
        final ImmutableMap.Builder<Pair<Comparisons.Type, Type.TypeCode>, UnaryPhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final UnaryPhysicalOperator operator : UnaryPhysicalOperator.values()) {
            mapBuilder.put(Pair.of(operator.getType(), operator.getArgType()), operator);
        }
        return mapBuilder.build();
    }

    @Nonnull
    private static Map<Pair<Comparisons.Type, Type.TypeCode>, UnaryPhysicalOperator> getUnaryOperatorMap() {
        return unaryOperatorMapSupplier.get();
    }

    private static Map<Triple<Comparisons.Type, Type.TypeCode, Type.TypeCode>, BinaryPhysicalOperator> computeBinaryOperatorMap() {
        final ImmutableMap.Builder<Triple<Comparisons.Type, Type.TypeCode, Type.TypeCode>, BinaryPhysicalOperator> mapBuilder = ImmutableMap.builder();
        for (final BinaryPhysicalOperator operator : BinaryPhysicalOperator.values()) {
            mapBuilder.put(Triple.of(operator.getType(), operator.getLeftArgType(), operator.getRightArgType()), operator);
        }
        return mapBuilder.build();
    }

    @Nonnull
    private static Map<Triple<Comparisons.Type, Type.TypeCode, Type.TypeCode>, BinaryPhysicalOperator> getBinaryOperatorMap() {
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

    private enum BinaryPhysicalOperator {
        // TODO think about equality epsilon for floating-point types.
        EQ_BU(Comparisons.Type.EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_BB(Comparisons.Type.EQUALS, Type.TypeCode.BOOLEAN, Type.TypeCode.BOOLEAN, (l, r) -> (boolean)l == (boolean)r),
        EQ_IU(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_II(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.INT, (l, r) -> (int)l == (int)r),
        EQ_IL(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.LONG, (l, r) -> (int)l == (long)r),
        EQ_IF(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.FLOAT, (l, r) -> (int)l == (float)r),
        EQ_ID(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.DOUBLE, (l, r) -> (int)l == (double)r),
        // EQ_IS(Comparisons.Type.EQUALS, Type.TypeCode.INT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        EQ_LU(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_LI(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l == (int)r),
        EQ_LL(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l == (long)r),
        EQ_LF(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l == (float)r),
        EQ_LD(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l == (double)r),
        // EQ_LS(Comparisons.Type.EQUALS, Type.TypeCode.LONG, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        EQ_FU(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_FI(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l == (int)r),
        EQ_FL(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l == (long)r),
        EQ_FF(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l == (float)r),
        EQ_FD(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l == (double)r),
        // EQ_FS(Comparisons.Type.EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        EQ_DU(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        EQ_DI(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l == (int)r),
        EQ_DL(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l == (long)r),
        EQ_DF(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l == (float)r),
        EQ_DD(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l == (double)r),
        // EQ_DS(Comparisons.Type.EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        // EQ_SI(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.INT, (l, r) -> ??), // invalid
        // EQ_SL(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.LONG, (l, r) -> ??), // invalid
        // EQ_SF(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.FLOAT, (l, r) -> ??), // invalid
        // EQ_SD(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.DOUBLE, (l, r) -> ??), // invalid
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
        // NEQ_IS(Comparisons.Type.NOT_EQUALS, Type.TypeCode.INT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        NEQ_LU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_LI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l != (int)r),
        NEQ_LL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l != (long)r),
        NEQ_LF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l != (float)r),
        NEQ_LD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l != (double)r),
        // NEQ_LS(Comparisons.Type.NOT_EQUALS, Type.TypeCode.LONG, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        NEQ_FU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_FI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l != (int)r),
        NEQ_FL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l != (long)r),
        NEQ_FF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l != (float)r),
        NEQ_FD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l != (double)r),
        // NEQ_FS(Comparisons.Type.NOT_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        NEQ_DU(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        NEQ_DI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l != (int)r),
        NEQ_DL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l != (long)r),
        NEQ_DF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l != (float)r),
        NEQ_DD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l != (double)r),
        // NEQ_DS(Comparisons.Type.NOT_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        // NEQ_SI(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.INT, (l, r) -> ??), // invalid
        // NEQ_SL(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.LONG, (l, r) -> ??), // invalid
        // NEQ_SF(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.FLOAT, (l, r) -> ??), // invalid
        // NEQ_SD(Comparisons.Type.NOT_EQUALS, Type.TypeCode.STRING, Type.TypeCode.DOUBLE, (l, r) -> ??), // invalid
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
        // LT_IS(Comparisons.Type.LESS_THAN, Type.TypeCode.INT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        LT_LU(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_LI(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l < (int)r),
        LT_LL(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l < (long)r),
        LT_LF(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l < (float)r),
        LT_LD(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l < (double)r),
        // LT_LS(Comparisons.Type.LESS_THAN, Type.TypeCode.LONG, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        LT_FU(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_FI(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l < (int)r),
        LT_FL(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l < (long)r),
        LT_FF(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l < (float)r),
        LT_FD(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l < (double)r),
        // LT_FS(Comparisons.Type.LESS_THAN, Type.TypeCode.FLOAT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        LT_DU(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LT_DI(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l < (int)r),
        LT_DL(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l < (long)r),
        LT_DF(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l < (float)r),
        LT_DD(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l < (double)r),
        // LT_DS(Comparisons.Type.LESS_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        // LT_SI(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.INT, (l, r) -> ??), // invalid
        // LT_SL(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.LONG, (l, r) -> ??), // invalid
        // LT_SF(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.FLOAT, (l, r) -> ??), // invalid
        // LT_SD(Comparisons.Type.LESS_THAN, Type.TypeCode.STRING, Type.TypeCode.DOUBLE, (l, r) -> ??), // invalid
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
        // LTE_IS(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        LTE_LU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_LI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l <= (int)r),
        LTE_LL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l <= (long)r),
        LTE_LF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l <= (float)r),
        LTE_LD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l <= (double)r),
        // LTE_LS(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        LTE_FU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_FI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l <= (int)r),
        LTE_FL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l <= (long)r),
        LTE_FF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l <= (float)r),
        LTE_FD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l <= (double)r),
        // LTE_FS(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        LTE_DU(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        LTE_DI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l <= (int)r),
        LTE_DL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l <= (long)r),
        LTE_DF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l <= (float)r),
        LTE_DD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l <= (double)r),
        // LTE_DS(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        // LTE_SI(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.INT, (l, r) -> ??), // invalid
        // LTE_SL(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.LONG, (l, r) -> ??), // invalid
        // LTE_SF(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.FLOAT, (l, r) -> ??), // invalid
        // LTE_SD(Comparisons.Type.LESS_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.DOUBLE, (l, r) -> ??), // invalid
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
        // GT_IS(Comparisons.Type.GREATER_THAN, Type.TypeCode.INT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        GT_LU(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_LI(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l > (int)r),
        GT_LL(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l > (long)r),
        GT_LF(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l > (float)r),
        GT_LD(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l > (double)r),
        // GT_LS(Comparisons.Type.GREATER_THAN, Type.TypeCode.LONG, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        GT_FU(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_FI(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l > (int)r),
        GT_FL(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l > (long)r),
        GT_FF(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l > (float)r),
        GT_FD(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l > (double)r),
        // GT_FS(Comparisons.Type.GREATER_THAN, Type.TypeCode.FLOAT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        GT_DU(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GT_DI(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l > (int)r),
        GT_DL(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l > (long)r),
        GT_DF(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l > (float)r),
        GT_DD(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l > (double)r),
        // GT_DS(Comparisons.Type.GREATER_THAN, Type.TypeCode.DOUBLE, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        // GT_SI(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.INT, (l, r) -> ??), // invalid
        // GT_SL(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.LONG, (l, r) -> ??), // invalid
        // GT_SF(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.FLOAT, (l, r) -> ??), // invalid
        // GT_SD(Comparisons.Type.GREATER_THAN, Type.TypeCode.STRING, Type.TypeCode.DOUBLE, (l, r) -> ??), // invalid
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
        // GTE_IS(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.INT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        GTE_LU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_LI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.INT, (l, r) -> (long)l >= (int)r),
        GTE_LL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.LONG, (l, r) -> (long)l >= (long)r),
        GTE_LF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.FLOAT, (l, r) -> (long)l >= (float)r),
        GTE_LD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.DOUBLE, (l, r) -> (long)l >= (double)r),
        // GTE_LS(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.LONG, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        GTE_FU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_FI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.INT, (l, r) -> (float)l >= (int)r),
        GTE_FL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.LONG, (l, r) -> (float)l >= (long)r),
        GTE_FF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.FLOAT, (l, r) -> (float)l >= (float)r),
        GTE_FD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.DOUBLE, (l, r) -> (float)l >= (double)r),
        // GTE_FS(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.FLOAT, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        GTE_DU(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.UNKNOWN, (l, r) -> null),
        GTE_DI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.INT, (l, r) -> (double)l >= (int)r),
        GTE_DL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.LONG, (l, r) -> (double)l >= (long)r),
        GTE_DF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.FLOAT, (l, r) -> (double)l >= (float)r),
        GTE_DD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.DOUBLE, (l, r) -> (double)l >= (double)r),
        // GTE_DS(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.DOUBLE, Type.TypeCode.STRING, (l, r) -> ??), // invalid
        // GTE_SI(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.INT, (l, r) -> ??), // invalid
        // GTE_SL(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.LONG, (l, r) -> ??), // invalid
        // GTE_SF(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.FLOAT, (l, r) -> ??), // invalid
        // GTE_SD(Comparisons.Type.GREATER_THAN_OR_EQUALS, Type.TypeCode.STRING, Type.TypeCode.DOUBLE, (l, r) -> ??), // invalid
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
        ;

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
            //<editor-fold default-state="collapsed" description="big switch statement">
            switch (this) {
                case EQ_BU:
                    return PBinaryPhysicalOperator.EQ_BU;
                case EQ_BB:
                    return PBinaryPhysicalOperator.EQ_BB;
                case EQ_IU:
                    return PBinaryPhysicalOperator.EQ_IU;
                case EQ_II:
                    return PBinaryPhysicalOperator.EQ_II;
                case EQ_IL:
                    return PBinaryPhysicalOperator.EQ_IL;
                case EQ_IF:
                    return PBinaryPhysicalOperator.EQ_IF;
                case EQ_ID:
                    return PBinaryPhysicalOperator.EQ_ID;
                case EQ_LU:
                    return PBinaryPhysicalOperator.EQ_LU;
                case EQ_LI:
                    return PBinaryPhysicalOperator.EQ_LI;
                case EQ_LL:
                    return PBinaryPhysicalOperator.EQ_LL;
                case EQ_LF:
                    return PBinaryPhysicalOperator.EQ_LF;
                case EQ_LD:
                    return PBinaryPhysicalOperator.EQ_LD;
                case EQ_FU:
                    return PBinaryPhysicalOperator.EQ_FU;
                case EQ_FI:
                    return PBinaryPhysicalOperator.EQ_FI;
                case EQ_FL:
                    return PBinaryPhysicalOperator.EQ_FL;
                case EQ_FF:
                    return PBinaryPhysicalOperator.EQ_FF;
                case EQ_FD:
                    return PBinaryPhysicalOperator.EQ_FD;
                case EQ_DU:
                    return PBinaryPhysicalOperator.EQ_DU;
                case EQ_DI:
                    return PBinaryPhysicalOperator.EQ_DI;
                case EQ_DL:
                    return PBinaryPhysicalOperator.EQ_DL;
                case EQ_DF:
                    return PBinaryPhysicalOperator.EQ_DF;
                case EQ_DD:
                    return PBinaryPhysicalOperator.EQ_DD;
                case EQ_SU:
                    return PBinaryPhysicalOperator.EQ_SU;
                case EQ_SS:
                    return PBinaryPhysicalOperator.EQ_SS;
                case EQ_UU:
                    return PBinaryPhysicalOperator.EQ_UU;
                case EQ_UB:
                    return PBinaryPhysicalOperator.EQ_UB;
                case EQ_UI:
                    return PBinaryPhysicalOperator.EQ_UI;
                case EQ_UL:
                    return PBinaryPhysicalOperator.EQ_UL;
                case EQ_UF:
                    return PBinaryPhysicalOperator.EQ_UF;
                case EQ_UD:
                    return PBinaryPhysicalOperator.EQ_UD;
                case EQ_US:
                    return PBinaryPhysicalOperator.EQ_US;
                case EQ_UV:
                    return PBinaryPhysicalOperator.EQ_UV;
                case EQ_VU:
                    return PBinaryPhysicalOperator.EQ_VU;
                case EQ_VV:
                    return PBinaryPhysicalOperator.EQ_VV;
                case NEQ_BU:
                    return PBinaryPhysicalOperator.NEQ_BU;
                case NEQ_BB:
                    return PBinaryPhysicalOperator.NEQ_BB;
                case NEQ_IU:
                    return PBinaryPhysicalOperator.NEQ_IU;
                case NEQ_II:
                    return PBinaryPhysicalOperator.NEQ_II;
                case NEQ_IL:
                    return PBinaryPhysicalOperator.NEQ_IL;
                case NEQ_IF:
                    return PBinaryPhysicalOperator.NEQ_IF;
                case NEQ_ID:
                    return PBinaryPhysicalOperator.NEQ_ID;
                case NEQ_LU:
                    return PBinaryPhysicalOperator.NEQ_LU;
                case NEQ_LI:
                    return PBinaryPhysicalOperator.NEQ_LI;
                case NEQ_LL:
                    return PBinaryPhysicalOperator.NEQ_LL;
                case NEQ_LF:
                    return PBinaryPhysicalOperator.NEQ_LF;
                case NEQ_LD:
                    return PBinaryPhysicalOperator.NEQ_LD;
                case NEQ_FU:
                    return PBinaryPhysicalOperator.NEQ_FU;
                case NEQ_FI:
                    return PBinaryPhysicalOperator.NEQ_FI;
                case NEQ_FL:
                    return PBinaryPhysicalOperator.NEQ_FL;
                case NEQ_FF:
                    return PBinaryPhysicalOperator.NEQ_FF;
                case NEQ_FD:
                    return PBinaryPhysicalOperator.NEQ_FD;
                case NEQ_DU:
                    return PBinaryPhysicalOperator.NEQ_DU;
                case NEQ_DI:
                    return PBinaryPhysicalOperator.NEQ_DI;
                case NEQ_DL:
                    return PBinaryPhysicalOperator.NEQ_DL;
                case NEQ_DF:
                    return PBinaryPhysicalOperator.NEQ_DF;
                case NEQ_DD:
                    return PBinaryPhysicalOperator.NEQ_DD;
                case NEQ_SU:
                    return PBinaryPhysicalOperator.NEQ_SU;
                case NEQ_SS:
                    return PBinaryPhysicalOperator.NEQ_SS;
                case NEQ_UU:
                    return PBinaryPhysicalOperator.NEQ_UU;
                case NEQ_UB:
                    return PBinaryPhysicalOperator.NEQ_UB;
                case NEQ_UI:
                    return PBinaryPhysicalOperator.NEQ_UI;
                case NEQ_UL:
                    return PBinaryPhysicalOperator.NEQ_UL;
                case NEQ_UF:
                    return PBinaryPhysicalOperator.NEQ_UF;
                case NEQ_UD:
                    return PBinaryPhysicalOperator.NEQ_UD;
                case NEQ_US:
                    return PBinaryPhysicalOperator.NEQ_US;
                case NEQ_UV:
                    return PBinaryPhysicalOperator.NEQ_UV;
                case NEQ_VU:
                    return PBinaryPhysicalOperator.NEQ_VU;
                case NEQ_VV:
                    return PBinaryPhysicalOperator.NEQ_VV;
                case LT_IU:
                    return PBinaryPhysicalOperator.LT_IU;
                case LT_II:
                    return PBinaryPhysicalOperator.LT_II;
                case LT_IL:
                    return PBinaryPhysicalOperator.LT_IL;
                case LT_IF:
                    return PBinaryPhysicalOperator.LT_IF;
                case LT_ID:
                    return PBinaryPhysicalOperator.LT_ID;
                case LT_LU:
                    return PBinaryPhysicalOperator.LT_LU;
                case LT_LI:
                    return PBinaryPhysicalOperator.LT_LI;
                case LT_LL:
                    return PBinaryPhysicalOperator.LT_LL;
                case LT_LF:
                    return PBinaryPhysicalOperator.LT_LF;
                case LT_LD:
                    return PBinaryPhysicalOperator.LT_LD;
                case LT_FU:
                    return PBinaryPhysicalOperator.LT_FU;
                case LT_FI:
                    return PBinaryPhysicalOperator.LT_FI;
                case LT_FL:
                    return PBinaryPhysicalOperator.LT_FL;
                case LT_FF:
                    return PBinaryPhysicalOperator.LT_FF;
                case LT_FD:
                    return PBinaryPhysicalOperator.LT_FD;
                case LT_DU:
                    return PBinaryPhysicalOperator.LT_DU;
                case LT_DI:
                    return PBinaryPhysicalOperator.LT_DI;
                case LT_DL:
                    return PBinaryPhysicalOperator.LT_DL;
                case LT_DF:
                    return PBinaryPhysicalOperator.LT_DF;
                case LT_DD:
                    return PBinaryPhysicalOperator.LT_DD;
                case LT_SU:
                    return PBinaryPhysicalOperator.LT_SU;
                case LT_SS:
                    return PBinaryPhysicalOperator.LT_SS;
                case LT_UU:
                    return PBinaryPhysicalOperator.LT_UU;
                case LT_UB:
                    return PBinaryPhysicalOperator.LT_UB;
                case LT_UI:
                    return PBinaryPhysicalOperator.LT_UI;
                case LT_UL:
                    return PBinaryPhysicalOperator.LT_UL;
                case LT_UF:
                    return PBinaryPhysicalOperator.LT_UF;
                case LT_UD:
                    return PBinaryPhysicalOperator.LT_UD;
                case LT_US:
                    return PBinaryPhysicalOperator.LT_US;
                case LT_UV:
                    return PBinaryPhysicalOperator.LT_UV;
                case LT_VU:
                    return PBinaryPhysicalOperator.LT_VU;
                case LT_VV:
                    return PBinaryPhysicalOperator.LT_VV;
                case LTE_IU:
                    return PBinaryPhysicalOperator.LTE_IU;
                case LTE_II:
                    return PBinaryPhysicalOperator.LTE_II;
                case LTE_IL:
                    return PBinaryPhysicalOperator.LTE_IL;
                case LTE_IF:
                    return PBinaryPhysicalOperator.LTE_IF;
                case LTE_ID:
                    return PBinaryPhysicalOperator.LTE_ID;
                case LTE_LU:
                    return PBinaryPhysicalOperator.LTE_LU;
                case LTE_LI:
                    return PBinaryPhysicalOperator.LTE_LI;
                case LTE_LL:
                    return PBinaryPhysicalOperator.LTE_LL;
                case LTE_LF:
                    return PBinaryPhysicalOperator.LTE_LF;
                case LTE_LD:
                    return PBinaryPhysicalOperator.LTE_LD;
                case LTE_FU:
                    return PBinaryPhysicalOperator.LTE_FU;
                case LTE_FI:
                    return PBinaryPhysicalOperator.LTE_FI;
                case LTE_FL:
                    return PBinaryPhysicalOperator.LTE_FL;
                case LTE_FF:
                    return PBinaryPhysicalOperator.LTE_FF;
                case LTE_FD:
                    return PBinaryPhysicalOperator.LTE_FD;
                case LTE_DU:
                    return PBinaryPhysicalOperator.LTE_DU;
                case LTE_DI:
                    return PBinaryPhysicalOperator.LTE_DI;
                case LTE_DL:
                    return PBinaryPhysicalOperator.LTE_DL;
                case LTE_DF:
                    return PBinaryPhysicalOperator.LTE_DF;
                case LTE_DD:
                    return PBinaryPhysicalOperator.LTE_DD;
                case LTE_SU:
                    return PBinaryPhysicalOperator.LTE_SU;
                case LTE_SS:
                    return PBinaryPhysicalOperator.LTE_SS;
                case LTE_UU:
                    return PBinaryPhysicalOperator.LTE_UU;
                case LTE_UB:
                    return PBinaryPhysicalOperator.LTE_UB;
                case LTE_UI:
                    return PBinaryPhysicalOperator.LTE_UI;
                case LTE_UL:
                    return PBinaryPhysicalOperator.LTE_UL;
                case LTE_UF:
                    return PBinaryPhysicalOperator.LTE_UF;
                case LTE_UD:
                    return PBinaryPhysicalOperator.LTE_UD;
                case LTE_US:
                    return PBinaryPhysicalOperator.LTE_US;
                case LTE_UV:
                    return PBinaryPhysicalOperator.LTE_UV;
                case LTE_VU:
                    return PBinaryPhysicalOperator.LTE_VU;
                case LTE_VV:
                    return PBinaryPhysicalOperator.LTE_VV;
                case GT_IU:
                    return PBinaryPhysicalOperator.GT_IU;
                case GT_II:
                    return PBinaryPhysicalOperator.GT_II;
                case GT_IL:
                    return PBinaryPhysicalOperator.GT_IL;
                case GT_IF:
                    return PBinaryPhysicalOperator.GT_IF;
                case GT_ID:
                    return PBinaryPhysicalOperator.GT_ID;
                case GT_LU:
                    return PBinaryPhysicalOperator.GT_LU;
                case GT_LI:
                    return PBinaryPhysicalOperator.GT_LI;
                case GT_LL:
                    return PBinaryPhysicalOperator.GT_LL;
                case GT_LF:
                    return PBinaryPhysicalOperator.GT_LF;
                case GT_LD:
                    return PBinaryPhysicalOperator.GT_LD;
                case GT_FU:
                    return PBinaryPhysicalOperator.GT_FU;
                case GT_FI:
                    return PBinaryPhysicalOperator.GT_FI;
                case GT_FL:
                    return PBinaryPhysicalOperator.GT_FL;
                case GT_FF:
                    return PBinaryPhysicalOperator.GT_FF;
                case GT_FD:
                    return PBinaryPhysicalOperator.GT_FD;
                case GT_DU:
                    return PBinaryPhysicalOperator.GT_DU;
                case GT_DI:
                    return PBinaryPhysicalOperator.GT_DI;
                case GT_DL:
                    return PBinaryPhysicalOperator.GT_DL;
                case GT_DF:
                    return PBinaryPhysicalOperator.GT_DF;
                case GT_DD:
                    return PBinaryPhysicalOperator.GT_DD;
                case GT_SU:
                    return PBinaryPhysicalOperator.GT_SU;
                case GT_SS:
                    return PBinaryPhysicalOperator.GT_SS;
                case GT_UU:
                    return PBinaryPhysicalOperator.GT_UU;
                case GT_UB:
                    return PBinaryPhysicalOperator.GT_UB;
                case GT_UI:
                    return PBinaryPhysicalOperator.GT_UI;
                case GT_UL:
                    return PBinaryPhysicalOperator.GT_UL;
                case GT_UF:
                    return PBinaryPhysicalOperator.GT_UF;
                case GT_UD:
                    return PBinaryPhysicalOperator.GT_UD;
                case GT_US:
                    return PBinaryPhysicalOperator.GT_US;
                case GT_UV:
                    return PBinaryPhysicalOperator.GT_UV;
                case GT_VU:
                    return PBinaryPhysicalOperator.GT_VU;
                case GT_VV:
                    return PBinaryPhysicalOperator.GT_VV;
                case GTE_IU:
                    return PBinaryPhysicalOperator.GTE_IU;
                case GTE_II:
                    return PBinaryPhysicalOperator.GTE_II;
                case GTE_IL:
                    return PBinaryPhysicalOperator.GTE_IL;
                case GTE_IF:
                    return PBinaryPhysicalOperator.GTE_IF;
                case GTE_ID:
                    return PBinaryPhysicalOperator.GTE_ID;
                case GTE_LU:
                    return PBinaryPhysicalOperator.GTE_LU;
                case GTE_LI:
                    return PBinaryPhysicalOperator.GTE_LI;
                case GTE_LL:
                    return PBinaryPhysicalOperator.GTE_LL;
                case GTE_LF:
                    return PBinaryPhysicalOperator.GTE_LF;
                case GTE_LD:
                    return PBinaryPhysicalOperator.GTE_LD;
                case GTE_FU:
                    return PBinaryPhysicalOperator.GTE_FU;
                case GTE_FI:
                    return PBinaryPhysicalOperator.GTE_FI;
                case GTE_FL:
                    return PBinaryPhysicalOperator.GTE_FL;
                case GTE_FF:
                    return PBinaryPhysicalOperator.GTE_FF;
                case GTE_FD:
                    return PBinaryPhysicalOperator.GTE_FD;
                case GTE_DU:
                    return PBinaryPhysicalOperator.GTE_DU;
                case GTE_DI:
                    return PBinaryPhysicalOperator.GTE_DI;
                case GTE_DL:
                    return PBinaryPhysicalOperator.GTE_DL;
                case GTE_DF:
                    return PBinaryPhysicalOperator.GTE_DF;
                case GTE_DD:
                    return PBinaryPhysicalOperator.GTE_DD;
                case GTE_SU:
                    return PBinaryPhysicalOperator.GTE_SU;
                case GTE_SS:
                    return PBinaryPhysicalOperator.GTE_SS;
                case GTE_UU:
                    return PBinaryPhysicalOperator.GTE_UU;
                case GTE_UB:
                    return PBinaryPhysicalOperator.GTE_UB;
                case GTE_UI:
                    return PBinaryPhysicalOperator.GTE_UI;
                case GTE_UL:
                    return PBinaryPhysicalOperator.GTE_UL;
                case GTE_UF:
                    return PBinaryPhysicalOperator.GTE_UF;
                case GTE_UD:
                    return PBinaryPhysicalOperator.GTE_UD;
                case GTE_US:
                    return PBinaryPhysicalOperator.GTE_US;
                case GTE_UV:
                    return PBinaryPhysicalOperator.GTE_UV;
                case GTE_VU:
                    return PBinaryPhysicalOperator.GTE_VU;
                case GTE_VV:
                    return PBinaryPhysicalOperator.GTE_VV;
                default:
                    throw new RecordCoreException("unknown binary physical operator. did you forget to add it here?");
            }
            //</editor-fold>
        }

        @Nonnull
        @SuppressWarnings({"unused", "DuplicateBranchesInSwitch"})
        public static BinaryPhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                       @Nonnull final PBinaryPhysicalOperator binaryPhysicalOperatorProto) {
            //<editor-fold default-state="collapsed" description="big switch statement">
            switch (binaryPhysicalOperatorProto) {
                case EQ_BU:
                    return EQ_BU;
                case EQ_BB:
                    return EQ_BB;
                case EQ_IU:
                    return EQ_IU;
                case EQ_II:
                    return EQ_II;
                case EQ_IL:
                    return EQ_IL;
                case EQ_IF:
                    return EQ_IF;
                case EQ_ID:
                    return EQ_ID;
                case EQ_IS:
                    throw new RecordCoreException("unsupported operator");
                case EQ_LU:
                    return EQ_LU;
                case EQ_LI:
                    return EQ_LI;
                case EQ_LL:
                    return EQ_LL;
                case EQ_LF:
                    return EQ_LF;
                case EQ_LD:
                    return EQ_LD;
                case EQ_LS:
                    throw new RecordCoreException("unsupported operator");
                case EQ_FU:
                    return EQ_FU;
                case EQ_FI:
                    return EQ_FI;
                case EQ_FL:
                    return EQ_FL;
                case EQ_FF:
                    return EQ_FF;
                case EQ_FD:
                    return EQ_FD;
                case EQ_FS:
                    throw new RecordCoreException("unsupported operator");
                case EQ_DU:
                    return EQ_DU;
                case EQ_DI:
                    return EQ_DI;
                case EQ_DL:
                    return EQ_DL;
                case EQ_DF:
                    return EQ_DF;
                case EQ_DD:
                    return EQ_DD;
                case EQ_DS:
                case EQ_SI:
                case EQ_SL:
                case EQ_SF:
                case EQ_SD:
                    throw new RecordCoreException("unsupported operator");
                case EQ_SU:
                    return EQ_SU;
                case EQ_SS:
                    return EQ_SS;
                case EQ_UU:
                    return EQ_UU;
                case EQ_UB:
                    return EQ_UB;
                case EQ_UI:
                    return EQ_UI;
                case EQ_UL:
                    return EQ_UL;
                case EQ_UF:
                    return EQ_UF;
                case EQ_UD:
                    return EQ_UD;
                case EQ_US:
                    return EQ_US;
                case EQ_UV:
                    return EQ_UV;
                case EQ_VU:
                    return EQ_VU;
                case EQ_VV:
                    return EQ_VV;
                case NEQ_BU:
                    return NEQ_BU;
                case NEQ_BB:
                    return NEQ_BB;
                case NEQ_IU:
                    return NEQ_IU;
                case NEQ_II:
                    return NEQ_II;
                case NEQ_IL:
                    return NEQ_IL;
                case NEQ_IF:
                    return NEQ_IF;
                case NEQ_ID:
                    return NEQ_ID;
                case NEQ_IS:
                    throw new RecordCoreException("unsupported operator");
                case NEQ_LU:
                    return NEQ_LU;
                case NEQ_LI:
                    return NEQ_LI;
                case NEQ_LL:
                    return NEQ_LL;
                case NEQ_LF:
                    return NEQ_LF;
                case NEQ_LD:
                    return NEQ_LD;
                case NEQ_LS:
                    throw new RecordCoreException("unsupported operator");
                case NEQ_FU:
                    return NEQ_FU;
                case NEQ_FI:
                    return NEQ_FI;
                case NEQ_FL:
                    return NEQ_FL;
                case NEQ_FF:
                    return NEQ_FF;
                case NEQ_FD:
                    return NEQ_FD;
                case NEQ_FS:
                    throw new RecordCoreException("unsupported operator");
                case NEQ_DU:
                    return NEQ_DU;
                case NEQ_DI:
                    return NEQ_DI;
                case NEQ_DL:
                    return NEQ_DL;
                case NEQ_DF:
                    return NEQ_DF;
                case NEQ_DD:
                    return NEQ_DD;
                case NEQ_DS:
                case NEQ_SI:
                case NEQ_SL:
                case NEQ_SF:
                case NEQ_SD:
                    throw new RecordCoreException("unsupported operator");
                case NEQ_SU:
                    return NEQ_SU;
                case NEQ_SS:
                    return NEQ_SS;
                case NEQ_UU:
                    return NEQ_UU;
                case NEQ_UB:
                    return NEQ_UB;
                case NEQ_UI:
                    return NEQ_UI;
                case NEQ_UL:
                    return NEQ_UL;
                case NEQ_UF:
                    return NEQ_UF;
                case NEQ_UD:
                    return NEQ_UD;
                case NEQ_US:
                    return NEQ_US;
                case NEQ_UV:
                    return NEQ_UV;
                case NEQ_VU:
                    return NEQ_VU;
                case NEQ_VV:
                    return NEQ_VV;
                case LT_IU:
                    return LT_IU;
                case LT_II:
                    return LT_II;
                case LT_IL:
                    return LT_IL;
                case LT_IF:
                    return LT_IF;
                case LT_ID:
                    return LT_ID;
                case LT_IS:
                    throw new RecordCoreException("unsupported operator");
                case LT_LU:
                    return LT_LU;
                case LT_LI:
                    return LT_LI;
                case LT_LL:
                    return LT_LL;
                case LT_LF:
                    return LT_LF;
                case LT_LD:
                    return LT_LD;
                case LT_LS:
                    throw new RecordCoreException("unsupported operator");
                case LT_FU:
                    return LT_FU;
                case LT_FI:
                    return LT_FI;
                case LT_FL:
                    return LT_FL;
                case LT_FF:
                    return LT_FF;
                case LT_FD:
                    return LT_FD;
                case LT_FS:
                    throw new RecordCoreException("unsupported operator");
                case LT_DU:
                    return LT_DU;
                case LT_DI:
                    return LT_DI;
                case LT_DL:
                    return LT_DL;
                case LT_DF:
                    return LT_DF;
                case LT_DD:
                    return LT_DD;
                case LT_DS:
                case LT_SI:
                case LT_SL:
                case LT_SF:
                case LT_SD:
                    throw new RecordCoreException("unsupported operator");
                case LT_SU:
                    return LT_SU;
                case LT_SS:
                    return LT_SS;
                case LT_UU:
                    return LT_UU;
                case LT_UB:
                    return LT_UB;
                case LT_UI:
                    return LT_UI;
                case LT_UL:
                    return LT_UL;
                case LT_UF:
                    return LT_UF;
                case LT_UD:
                    return LT_UD;
                case LT_US:
                    return LT_US;
                case LT_UV:
                    return LT_UV;
                case LT_VU:
                    return LT_VU;
                case LT_VV:
                    return LT_VV;
                case LTE_IU:
                    return LTE_IU;
                case LTE_II:
                    return LTE_II;
                case LTE_IL:
                    return LTE_IL;
                case LTE_IF:
                    return LTE_IF;
                case LTE_ID:
                    return LTE_ID;
                case LTE_IS:
                    throw new RecordCoreException("unsupported operator");
                case LTE_LU:
                    return LTE_LU;
                case LTE_LI:
                    return LTE_LI;
                case LTE_LL:
                    return LTE_LL;
                case LTE_LF:
                    return LTE_LF;
                case LTE_LD:
                    return LTE_LD;
                case LTE_LS:
                    throw new RecordCoreException("unsupported operator");
                case LTE_FU:
                    return LTE_FU;
                case LTE_FI:
                    return LTE_FI;
                case LTE_FL:
                    return LTE_FL;
                case LTE_FF:
                    return LTE_FF;
                case LTE_FD:
                    return LTE_FD;
                case LTE_FS:
                    throw new RecordCoreException("unsupported operator");
                case LTE_DU:
                    return LTE_DU;
                case LTE_DI:
                    return LTE_DI;
                case LTE_DL:
                    return LTE_DL;
                case LTE_DF:
                    return LTE_DF;
                case LTE_DD:
                    return LTE_DD;
                case LTE_DS:
                case LTE_SI:
                case LTE_SL:
                case LTE_SF:
                case LTE_SD:
                    throw new RecordCoreException("unsupported operator");
                case LTE_SU:
                    return LTE_SU;
                case LTE_SS:
                    return LTE_SS;
                case LTE_UU:
                    return LTE_UU;
                case LTE_UB:
                    return LTE_UB;
                case LTE_UI:
                    return LTE_UI;
                case LTE_UL:
                    return LTE_UL;
                case LTE_UF:
                    return LTE_UF;
                case LTE_UD:
                    return LTE_UD;
                case LTE_US:
                    return LTE_US;
                case LTE_UV:
                    return LTE_UV;
                case LTE_VU:
                    return LTE_VU;
                case LTE_VV:
                    return LTE_VV;
                case GT_IU:
                    return GT_IU;
                case GT_II:
                    return GT_II;
                case GT_IL:
                    return GT_IL;
                case GT_IF:
                    return GT_IF;
                case GT_ID:
                    return GT_ID;
                case GT_IS:
                    throw new RecordCoreException("unsupported operator");
                case GT_LU:
                    return GT_LU;
                case GT_LI:
                    return GT_LI;
                case GT_LL:
                    return GT_LL;
                case GT_LF:
                    return GT_LF;
                case GT_LD:
                    return GT_LD;
                case GT_LS:
                    throw new RecordCoreException("unsupported operator");
                case GT_FU:
                    return GT_FU;
                case GT_FI:
                    return GT_FI;
                case GT_FL:
                    return GT_FL;
                case GT_FF:
                    return GT_FF;
                case GT_FD:
                    return GT_FD;
                case GT_FS:
                    throw new RecordCoreException("unsupported operator");
                case GT_DU:
                    return GT_DU;
                case GT_DI:
                    return GT_DI;
                case GT_DL:
                    return GT_DL;
                case GT_DF:
                    return GT_DF;
                case GT_DD:
                    return GT_DD;
                case GT_DS:
                case GT_SI:
                case GT_SL:
                case GT_SF:
                case GT_SD:
                    throw new RecordCoreException("unsupported operator");
                case GT_SU:
                    return GT_SU;
                case GT_SS:
                    return GT_SS;
                case GT_UU:
                    return GT_UU;
                case GT_UB:
                    return GT_UB;
                case GT_UI:
                    return GT_UI;
                case GT_UL:
                    return GT_UL;
                case GT_UF:
                    return GT_UF;
                case GT_UD:
                    return GT_UD;
                case GT_US:
                    return GT_US;
                case GT_UV:
                    return GT_UV;
                case GT_VU:
                    return GT_VU;
                case GT_VV:
                    return GT_VV;
                case GTE_IU:
                    return GTE_IU;
                case GTE_II:
                    return GTE_II;
                case GTE_IL:
                    return GTE_IL;
                case GTE_IF:
                    return GTE_IF;
                case GTE_ID:
                    return GTE_ID;
                case GTE_IS:
                    throw new RecordCoreException("unsupported operator");
                case GTE_LU:
                    return GTE_LU;
                case GTE_LI:
                    return GTE_LI;
                case GTE_LL:
                    return GTE_LL;
                case GTE_LF:
                    return GTE_LF;
                case GTE_LD:
                    return GTE_LD;
                case GTE_LS:
                    throw new RecordCoreException("unsupported operator");
                case GTE_FU:
                    return GTE_FU;
                case GTE_FI:
                    return GTE_FI;
                case GTE_FL:
                    return GTE_FL;
                case GTE_FF:
                    return GTE_FF;
                case GTE_FD:
                    return GTE_FD;
                case GTE_FS:
                    throw new RecordCoreException("unsupported operator");
                case GTE_DU:
                    return GTE_DU;
                case GTE_DI:
                    return GTE_DI;
                case GTE_DL:
                    return GTE_DL;
                case GTE_DF:
                    return GTE_DF;
                case GTE_DD:
                    return GTE_DD;
                case GTE_DS:
                case GTE_SI:
                case GTE_SL:
                case GTE_SF:
                case GTE_SD:
                    throw new RecordCoreException("unsupported operator");
                case GTE_SU:
                    return GTE_SU;
                case GTE_SS:
                    return GTE_SS;
                case GTE_UU:
                    return GTE_UU;
                case GTE_UB:
                    return GTE_UB;
                case GTE_UI:
                    return GTE_UI;
                case GTE_UL:
                    return GTE_UL;
                case GTE_UF:
                    return GTE_UF;
                case GTE_UD:
                    return GTE_UD;
                case GTE_US:
                    return GTE_US;
                case GTE_UV:
                    return GTE_UV;
                case GTE_VU:
                    return GTE_VU;
                case GTE_VV:
                    return GTE_VV;
                default:
                    throw new RecordCoreException("unknown binary physical operator. did you forget to add it here?");
            }
            //</editor-fold>
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
        IS_NOT_NULL_BI(Comparisons.Type.NOT_NULL, Type.TypeCode.BOOLEAN, Objects::nonNull);

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
            switch (this) {
                case IS_NULL_UI:
                    return PUnaryPhysicalOperator.IS_NULL_UI;
                case IS_NULL_II:
                    return PUnaryPhysicalOperator.IS_NULL_II;
                case IS_NULL_LI:
                    return PUnaryPhysicalOperator.IS_NULL_LI;
                case IS_NULL_FI:
                    return PUnaryPhysicalOperator.IS_NULL_FI;
                case IS_NULL_DI:
                    return PUnaryPhysicalOperator.IS_NULL_DI;
                case IS_NULL_SS:
                    return PUnaryPhysicalOperator.IS_NULL_SS;
                case IS_NULL_BI:
                    return PUnaryPhysicalOperator.IS_NULL_BI;
                case IS_NOT_NULL_UI:
                    return PUnaryPhysicalOperator.IS_NOT_NULL_UI;
                case IS_NOT_NULL_II:
                    return PUnaryPhysicalOperator.IS_NOT_NULL_II;
                case IS_NOT_NULL_LI:
                    return PUnaryPhysicalOperator.IS_NOT_NULL_LI;
                case IS_NOT_NULL_FI:
                    return PUnaryPhysicalOperator.IS_NOT_NULL_FI;
                case IS_NOT_NULL_DI:
                    return PUnaryPhysicalOperator.IS_NOT_NULL_DI;
                case IS_NOT_NULL_SS:
                    return PUnaryPhysicalOperator.IS_NOT_NULL_SS;
                case IS_NOT_NULL_BI:
                    return PUnaryPhysicalOperator.IS_NOT_NULL_BI;
                default:
                    throw new RecordCoreException("unknown binary physical operator. did you forget to add it here?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static UnaryPhysicalOperator fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PUnaryPhysicalOperator unaryPhysicalOperatorProto) {
            switch (unaryPhysicalOperatorProto) {
                case IS_NULL_UI:
                    return IS_NULL_UI;
                case IS_NULL_II:
                    return IS_NULL_II;
                case IS_NULL_LI:
                    return IS_NULL_LI;
                case IS_NULL_FI:
                    return IS_NULL_FI;
                case IS_NULL_DI:
                    return IS_NULL_DI;
                case IS_NULL_SS:
                    return IS_NULL_SS;
                case IS_NULL_BI:
                    return IS_NULL_BI;
                case IS_NOT_NULL_UI:
                    return IS_NOT_NULL_UI;
                case IS_NOT_NULL_II:
                    return IS_NOT_NULL_II;
                case IS_NOT_NULL_LI:
                    return IS_NOT_NULL_LI;
                case IS_NOT_NULL_FI:
                    return IS_NOT_NULL_FI;
                case IS_NOT_NULL_DI:
                    return IS_NOT_NULL_DI;
                case IS_NOT_NULL_SS:
                    return IS_NOT_NULL_SS;
                case IS_NOT_NULL_BI:
                    return IS_NOT_NULL_BI;
                default:
                    throw new RecordCoreException("unknown binary physical operator. did you forget to add it here?");
            }
        }
    }

    /**
     * Binary rel ops.
     */
    @AutoService(PlanSerializable.class)
    @ProtoMessage(PBinaryRelOpValue.class)
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
        public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            final var evaluatedChildrenIterator =
                    Streams.stream(getChildren())
                            .map(child -> child.eval(store, context))
                            .iterator();

            return operator.eval(evaluatedChildrenIterator.next(), evaluatedChildrenIterator.next());
        }

        @Nonnull
        @Override
        public String explain(@Nonnull final Formatter formatter) {
            final var childrenIterator = getChildren().iterator();
            return "(" + childrenIterator.next().explain(formatter) + " " + getFunctionName() + " " + childrenIterator.next().explain(formatter) + ")";
        }

        @Nonnull
        @Override
        public String toString() {
            final var childrenIterator = getChildren().iterator();
            return "(" + childrenIterator.next() + " " + getFunctionName() + " " + childrenIterator.next() + ")";
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
        public RecordQueryPlanProto.PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return RecordQueryPlanProto.PValue.newBuilder().setBinaryRelOpValue(toProto(serializationContext)).build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static BinaryRelOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PBinaryRelOpValue binaryRelOpValueProto) {
            return new BinaryRelOpValue(serializationContext, binaryRelOpValueProto);
        }
    }

    /**
     * Unary rel ops.
     */
    @AutoService(PlanSerializable.class)
    @ProtoMessage(PUnaryRelOpValue.class)
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
        public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            final var evaluatedChildrenIterator =
                    Streams.stream(getChildren())
                            .map(child -> child.eval(store, context))
                            .iterator();

            return operator.eval(evaluatedChildrenIterator.next());
        }

        @Nonnull
        @Override
        public String explain(@Nonnull final Formatter formatter) {
            final var onlyChild = Iterables.getOnlyElement(getChildren());
            return "(" + getFunctionName() + onlyChild.explain(formatter) + ")";
        }

        @Nonnull
        @Override
        public String toString() {
            final var onlyChild = Iterables.getOnlyElement(getChildren());
            return "(" + getFunctionName() + onlyChild + ")";
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
        public RecordQueryPlanProto.PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            return RecordQueryPlanProto.PValue.newBuilder().setUnaryRelOpValue(toProto(serializationContext)).build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static UnaryRelOpValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PUnaryRelOpValue unaryRelOpValueProto) {
            return new UnaryRelOpValue(serializationContext, unaryRelOpValueProto);
        }
    }
}
