/*
 * MergeValue.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link Value} that returns the comparison result between its children.
 */
@API(API.Status.EXPERIMENTAL)
public class RelOpValue implements BooleanValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Rel-Op-Value");

    @Nonnull
    private final String functionName;
    @Nonnull
    private final Comparisons.Type comparisonType;
    @Nonnull
    private final Value leftChild;
    @Nonnull
    private final Value rightChild;
    @Nonnull
    private final Function<Value, Object> compileTimeEvalFn;

    /**
     * Constructs a new instance of {@link RelOpValue}.
     * @param functionName The function name.
     * @param comparisonType The comparison type.
     * @param leftChild The left child.
     * @param rightChild The right child.
     * @param compileTimeEvalFn The compile-time evaluation function.
     */
    private RelOpValue(@Nonnull final String functionName,
                       @Nonnull final Comparisons.Type comparisonType,
                       @Nonnull final Value leftChild,
                       @Nonnull final Value rightChild,
                       @Nonnull final Function<Value, Object> compileTimeEvalFn) {
        this.functionName = functionName;
        this.comparisonType = comparisonType;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.compileTimeEvalFn = compileTimeEvalFn;
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        return List.of(leftChild, rightChild);
    }

    @Nonnull
    @Override
    public RelOpValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 2);
        return new RelOpValue(this.functionName,
                this.comparisonType,
                Iterables.get(newChildren, 0),
                Iterables.get(newChildren, 1),
                compileTimeEvalFn);
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> fdbRecord, @Nullable final M message) {
        if (comparisonType == Comparisons.Type.EQUALS) {
            final Object leftResult = leftChild.eval(store, context, fdbRecord, message);
            if (leftResult == null) {
                return false;
            }
            final Object rightResult = rightChild.eval(store, context, fdbRecord, message);
            if (rightResult == null) {
                return false;
            }
            return leftResult.equals(rightResult);
        }
        throw new UnsupportedOperationException("inequality comparison not supported yet");
    }

    @Override
    public Optional<ValuePredicate> toQueryPredicate(@Nonnull final CorrelationIdentifier innermostAlias) {
        // one side of the relop has to be correlated to the innermost alias and only to that one; the other one
        // can be correlated (or not) to anything except the innermostAlias
        final Set<CorrelationIdentifier> innermostAliasSet = Set.of(innermostAlias);

        final Set<CorrelationIdentifier> leftChildCorrelatedTo = leftChild.getCorrelatedTo();
        final Set<CorrelationIdentifier> rightChildCorrelatedTo = rightChild.getCorrelatedTo();
        if (leftChildCorrelatedTo.equals(innermostAliasSet) &&
                !rightChildCorrelatedTo.contains(innermostAlias)) {
            final Object comparand = compileTimeEvalFn.apply(rightChild);
            if (comparand == null) {
                return Optional.empty();
            }
            return Optional.of(new ValuePredicate(leftChild, new Comparisons.SimpleComparison(comparisonType, comparand)));
        } else if (rightChildCorrelatedTo.equals(innermostAliasSet) &&
                   !leftChildCorrelatedTo.contains(innermostAlias)) {
            final Object comparand = compileTimeEvalFn.apply(leftChild);
            if (comparand == null) {
                return Optional.empty();
            }
            return Optional.of(new ValuePredicate(rightChild, new Comparisons.SimpleComparison(swap(comparisonType), comparand)));
        }
        return Optional.empty();
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, comparisonType, leftChild, rightChild);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, comparisonType, leftChild, rightChild);
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

    @Nonnull
    private static Comparisons.Type swap(@Nonnull Comparisons.Type type) {
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
                throw new IllegalArgumentException("cannot swap comarison " + type);
        }
    }

    private static Value encapsulate(@Nonnull DynamicSchema dynamicSchema, @Nonnull final String functionName, @Nonnull Comparisons.Type comparisonType, @Nonnull final List<Typed> arguments) {
        Verify.verify(arguments.size() == 2);
        final Typed arg0 = arguments.get(0);
        final Type res0 = arg0.getResultType();
        SemanticException.check(res0.isPrimitive(), "only primitives can be compared with (non)-equalities");
        final Typed arg1 = arguments.get(1);
        final Type res1 = arg1.getResultType();
        SemanticException.check(res1.isPrimitive(), "only primitives can be compared with (non)-equalities");
        SemanticException.check((res0.isNumeric() && res1.isNumeric()) || res0.getTypeCode() == res1.getTypeCode(), "comparands are not compatible");
        return new RelOpValue(functionName,
                comparisonType,
                (Value)arg0,
                (Value)arg1,
                value -> value.compileTimeEval(EvaluationContext.forDynamicSchema(dynamicSchema)));
    }

    private static Value encapsulateComparable(@Nonnull DynamicSchema dynamicSchema, @Nonnull final String functionName, @Nonnull Comparisons.Type comparisonType, @Nonnull final List<Typed> arguments) {
        Verify.verify(arguments.size() == 2);
        final Typed arg0 = arguments.get(0);
        final Type res0 = arg0.getResultType();
        final Typed arg1 = arguments.get(1);
        final Type res1 = arg1.getResultType();

        SemanticException.check((res0.isNumeric() && res1.isNumeric()) ||
                                ((res0.getTypeCode() == Type.TypeCode.STRING) &&
                                 (res1.getTypeCode() == Type.TypeCode.STRING)), "comparands are not compatible");
        return new RelOpValue(functionName,
                comparisonType,
                (Value)arg0,
                (Value)arg1,
                value -> value.compileTimeEval(EvaluationContext.forDynamicSchema(dynamicSchema)));
    }

    @AutoService(BuiltInFunction.class)
    public static class EqualsFn extends BuiltInFunction<Value> {
        public EqualsFn() {
            super("equals",
                    List.of(new Type.Any(), new Type.Any()), EqualsFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Typed> arguments) {
            return RelOpValue.encapsulate(parserContext.getDynamicSchemaBuilder().build(), builtInFunction.getFunctionName(), Comparisons.Type.EQUALS, arguments);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class NotEqualsFn extends BuiltInFunction<Value> {
        public NotEqualsFn() {
            super("notEquals",
                    List.of(new Type.Any(), new Type.Any()), NotEqualsFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Typed> arguments) {
            return RelOpValue.encapsulate(parserContext.getDynamicSchemaBuilder().build(), builtInFunction.getFunctionName(), Comparisons.Type.NOT_EQUALS, arguments);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class LtFn extends BuiltInFunction<Value> {
        public LtFn() {
            super("lt",
                    List.of(new Type.Any(), new Type.Any()), LtFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Typed> arguments) {
            return RelOpValue.encapsulateComparable(parserContext.getDynamicSchemaBuilder().build(), builtInFunction.getFunctionName(), Comparisons.Type.LESS_THAN, arguments);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class LteFn extends BuiltInFunction<Value> {
        public LteFn() {
            super("lte",
                    List.of(new Type.Any(), new Type.Any()), LteFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Typed> arguments) {
            return RelOpValue.encapsulateComparable(parserContext.getDynamicSchemaBuilder().build(), builtInFunction.getFunctionName(), Comparisons.Type.LESS_THAN_OR_EQUALS, arguments);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class GtFn extends BuiltInFunction<Value> {
        public GtFn() {
            super("gt",
                    List.of(new Type.Any(), new Type.Any()), GtFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Typed> arguments) {
            return RelOpValue.encapsulateComparable(parserContext.getDynamicSchemaBuilder().build(), builtInFunction.getFunctionName(), Comparisons.Type.GREATER_THAN, arguments);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class GteFn extends BuiltInFunction<Value> {
        public GteFn() {
            super("gte",
                    List.of(new Type.Any(), new Type.Any()), GteFn::encapsulate);
        }

        private static Value encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<Value> builtInFunction, @Nonnull final List<Typed> arguments) {
            return RelOpValue.encapsulateComparable(parserContext.getDynamicSchemaBuilder().build(), builtInFunction.getFunctionName(), Comparisons.Type.GREATER_THAN_OR_EQUALS, arguments);
        }
    }

    private enum PhysicalOperator {
        EQ_
    }
}
