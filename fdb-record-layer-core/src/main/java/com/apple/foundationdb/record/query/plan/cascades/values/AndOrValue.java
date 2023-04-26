/*
 * AndOrValue.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * A {@link Value} that applies conjunction/disjunction on its boolean children, and if possible, simplifies its boolean children.
 */
@API(API.Status.EXPERIMENTAL)
public class AndOrValue extends AbstractValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("And-Or-Value");
    @Nonnull
    protected final String functionName;
    @Nonnull
    protected final Value leftChild;
    @Nonnull
    protected final Value rightChild;
    @Nonnull
    protected final Operator operator;

    private enum Operator {
        AND("AND"),
        OR("OR");

        @Nonnull
        private final String infixRepresentation;

        Operator(@Nonnull final String infixRepresentation) {
            this.infixRepresentation = infixRepresentation;
        }

        @Nonnull
        public String getInfixRepresentation() {
            return infixRepresentation;
        }
    }

    /**
     * Constructs a new instance of {@link AndOrValue}.
     *
     * @param functionName The function name.
     * @param leftChild The left child.
     * @param rightChild The right child.
     * @param operator The actual comparison operator.
     */
    protected AndOrValue(@Nonnull String functionName,
                         @Nonnull Operator operator,
                         @Nonnull Value leftChild,
                         @Nonnull Value rightChild) {
        this.functionName = functionName;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.operator = operator;
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return "(" + leftChild.explain(formatter) + " " + operator.getInfixRepresentation() + " " + rightChild.explain(formatter) + ")";
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return ImmutableList.of(leftChild, rightChild);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, functionName);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, functionName, leftChild, rightChild);
    }

    @Override
    public String toString() {
        return functionName + "(" + leftChild + ", " + rightChild + ")";
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

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store,
                                           @Nonnull final EvaluationContext context) {
        final Object leftResult = leftChild.eval(store, context);
        if (operator == Operator.AND && Boolean.FALSE.equals(leftResult)) {
            return false;
        }
        if (operator == Operator.OR && Boolean.TRUE.equals(leftResult)) {
            return true;
        }
        final Object rightResult = rightChild.eval(store, context);
        if (operator == Operator.AND && Boolean.FALSE.equals(rightResult)) {
            return false;
        }
        if (operator == Operator.OR && Boolean.TRUE.equals(rightResult)) {
            return true;
        }
        if (leftResult == null || rightResult == null) {
            return null;
        }
        if (operator == Operator.OR) {
            return (Boolean)rightResult || (Boolean)leftResult;
        }
        return (Boolean)rightResult && (Boolean)leftResult;
    }

    @SuppressWarnings("java:S3776")
    @Override
    public Optional<QueryPredicate> toQueryPredicate(@Nullable final TypeRepository typeRepository,
                                                     @Nonnull final CorrelationIdentifier innermostAlias) {
        Verify.verify(leftChild instanceof BooleanValue);
        Verify.verify(rightChild instanceof BooleanValue);
        final Optional<QueryPredicate> leftPredicateOptional = ((BooleanValue)leftChild).toQueryPredicate(typeRepository, innermostAlias);
        if (leftPredicateOptional.isPresent()) {
            final QueryPredicate leftPredicate = leftPredicateOptional.get();
            if (operator == Operator.AND && leftPredicate.equals(ConstantPredicate.FALSE)) {
                return leftPredicateOptional; // short-cut, even if RHS evaluates to null.
            }
            if (operator == Operator.OR && leftPredicate.equals(ConstantPredicate.TRUE)) {
                return leftPredicateOptional; // short-cut, even if RHS evaluates to null.
            }
            final Optional<QueryPredicate> rightPredicateOptional = ((BooleanValue)rightChild).toQueryPredicate(typeRepository, innermostAlias);
            if (rightPredicateOptional.isPresent()) {
                final QueryPredicate rightPredicate = rightPredicateOptional.get();
                if (operator == Operator.AND && rightPredicate.equals(ConstantPredicate.FALSE)) {
                    return rightPredicateOptional;
                }
                if (operator == Operator.OR && rightPredicate.equals(ConstantPredicate.TRUE)) {
                    return rightPredicateOptional;
                }
                if (leftPredicate.equals(ConstantPredicate.NULL) || rightPredicate.equals(ConstantPredicate.NULL)) {
                    return Optional.of(ConstantPredicate.NULL);
                }
                if (leftPredicate instanceof ConstantPredicate && rightPredicate instanceof ConstantPredicate) { // aggressive eval
                    if (operator == Operator.AND) {
                        return Optional.of((leftPredicate.isTautology() && rightPredicate.isTautology()) ? ConstantPredicate.TRUE : ConstantPredicate.FALSE);
                    } else {
                        return Optional.of((leftPredicate.isTautology() || rightPredicate.isTautology()) ? ConstantPredicate.TRUE : ConstantPredicate.FALSE);
                    }
                }
                if (operator == Operator.AND) {
                    return Optional.of(AndPredicate.and(leftPredicate, rightPredicate));
                } else {
                    return Optional.of(OrPredicate.or(leftPredicate, rightPredicate));
                }
            }
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public AndOrValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new AndOrValue(this.functionName,
                operator,
                Iterables.get(newChildren, 0),
                Iterables.get(newChildren, 1));
    }

    /**
     * The {@code and} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class AndFn extends BuiltInFunction<Value> {
        public AndFn() {
            super("and",
                    List.of(Type.primitiveType(Type.TypeCode.BOOLEAN), Type.primitiveType(Type.TypeCode.BOOLEAN)),
                    AndFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            Verify.verify(Iterables.size(arguments) == 2);
            return new AndOrValue(builtInFunction.getFunctionName(), Operator.AND, (Value)arguments.get(0), (Value)arguments.get(1));
        }
    }


    /**
     * The {@code or} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class OrFn extends BuiltInFunction<Value> {
        public OrFn() {
            super("or",
                    ImmutableList.of(Type.primitiveType(Type.TypeCode.BOOLEAN), Type.primitiveType(Type.TypeCode.BOOLEAN)),
                    OrFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<? extends Typed> arguments) {
            Verify.verify(Iterables.size(arguments) == 2);
            return new AndOrValue(builtInFunction.getFunctionName(), Operator.OR, (Value)arguments.get(0), (Value)arguments.get(1));
        }
    }
}
