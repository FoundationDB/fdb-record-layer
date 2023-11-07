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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A {@link Value} that returns the comparison result between its children.
 */
@API(API.Status.EXPERIMENTAL)
public class RelOpValue extends AbstractValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Rel-Op-Value");

    @Nonnull
    private final String functionName;
    @Nonnull
    private final Comparisons.Type comparisonType;
    @Nonnull
    private final Iterable<? extends Value> children;
    @Nonnull
    private final Function<Iterable<Object>, Object> physicalEvalFn;

    @Nonnull
    private static final Supplier<Map<Pair<Comparisons.Type, Type.TypeCode>, UnaryPhysicalOperator>> unaryOperatorMapSupplier =
            Suppliers.memoize(RelOpValue::computeUnaryOperatorMap);

    @Nonnull
    private static final Supplier<Map<Triple<Comparisons.Type, Type.TypeCode, Type.TypeCode>, BinaryPhysicalOperator>> binaryOperatorMapSupplier =
            Suppliers.memoize(RelOpValue::computeBinaryOperatorMap);

    /**
     * Creates a new instance of {@link RelOpValue}.
     * @param functionName The function name.
     * @param comparisonType The comparison type.
     * @param children The child expression(s).
     * @param physicalEvalFn The physical comparison function.
     */
    private RelOpValue(@Nonnull final String functionName,
                       @Nonnull final Comparisons.Type comparisonType,
                       @Nonnull final Iterable<? extends Value> children,
                       @Nonnull final Function<Iterable<Object>, Object> physicalEvalFn) {
        this.functionName = functionName;
        this.comparisonType = comparisonType;
        this.children = children;
        this.physicalEvalFn = physicalEvalFn;
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return children;
    }

    @Nonnull
    @Override
    public RelOpValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == Iterables.size(children));
        return new RelOpValue(this.functionName,
                this.comparisonType,
                newChildren,
                physicalEvalFn);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return physicalEvalFn.apply(StreamSupport.stream(children.spliterator(), false)
                .map(v -> v.eval(store, context))
                .collect(Collectors.toList())
        );
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
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, comparisonType);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, comparisonType,
                StreamSupport.stream(children.spliterator(), false).toArray(Value[]::new));
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return functionName + "(" + StreamSupport.stream(children.spliterator(), false).map(c -> c.explain(formatter)).collect(Collectors.joining(",")) + ")";
    }

    @Override
    public String toString() {
        return functionName + "(" + StreamSupport.stream(children.spliterator(), false).map(Value::toString).collect(Collectors.joining(",")) + ")";
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

            return new RelOpValue(functionName,
                    comparisonType,
                    arguments.stream().map(Value.class::cast).collect(Collectors.toList()),
                    objects -> {
                        Verify.verify(Iterables.size(objects) == 1);
                        return physicalOperator.eval(objects.iterator().next());
                    });
        } else {
            final Typed arg1 = arguments.get(1);
            final Type res1 = arg1.getResultType();
            SemanticException.check(res1.isPrimitive(), SemanticException.ErrorCode.COMPARAND_TO_COMPARISON_IS_OF_COMPLEX_TYPE);

            final BinaryPhysicalOperator physicalOperator =
                    getBinaryOperatorMap().get(Triple.of(comparisonType, res0.getTypeCode(), res1.getTypeCode()));

            Verify.verifyNotNull(physicalOperator, "unable to encapsulate comparison operation due to type mismatch(es)");

            return new RelOpValue(functionName,
                    comparisonType,
                    arguments.stream().map(Value.class::cast).collect(Collectors.toList()),
                    objects -> {
                        Verify.verify(Iterables.size(objects) == 2);
                        Iterator<Object> it = objects.iterator();
                        return physicalOperator.eval(it.next(), it.next());
                    });
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
        EQ_SS(Comparisons.Type.EQUALS, Type.TypeCode.STRING, Type.TypeCode.STRING, (l, r) -> l.equals(r)), // TODO: locale-aware comparison
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
        public BinaryOperator<Object> getEvaluateFunction() {
            return evaluateFunction;
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
        public UnaryOperator<Object> getEvaluateFunction() {
            return evaluateFunction;
        }
    }
}
