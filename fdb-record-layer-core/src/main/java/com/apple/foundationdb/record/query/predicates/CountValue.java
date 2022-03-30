/*
 * CountValue.java
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.temp.Formatter;
import com.apple.foundationdb.record.query.plan.temp.ParserContext;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.Type.TypeCode;
import com.apple.foundationdb.record.query.plan.temp.Typed;
import com.apple.foundationdb.record.query.plan.temp.dynamic.TypeRepository;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

@API(API.Status.EXPERIMENTAL)
public class CountValue implements Value, AggregateValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Sum-Value");

    @Nonnull
    private final PhysicalOperator operator;
    @Nullable
    private final Value child;

    public CountValue(@Nonnull PhysicalOperator operator,
                      @Nullable Value child) {
        this.operator = operator;
        this.child = child;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        throw new IllegalStateException("unable to eval an aggregation function with eval()");
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        if (child != null) {
            return operator.evalInitialToPartial(child.eval(store, context, record, message));
        } else {
            return operator.evalInitialToPartial(null);
        }
    }

    @Nonnull
    @Override
    public Accumulator createAccumulator(final @Nonnull TypeRepository typeRepository) {
        return new SumAccumulator(operator);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        if (child != null) {
            return "count(" + child.explain(formatter) + ")";
        } else {
            return "count()";
        }
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(operator.getResultTypeCode());
    }

    @Nonnull
    @Override
    public Iterable<? extends Value> getChildren() {
        if (child != null) {
            return ImmutableList.of(child);
        } else {
            return ImmutableList.of();
        }
    }

    @Nonnull
    @Override
    public CountValue withChildren(final Iterable<? extends Value> newChildren) {
        Verify.verify(Iterables.size(newChildren) == 1);
        return new CountValue(this.operator, Iterables.get(newChildren, 1));
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, operator, child);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, operator, child);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase() + "(" + child + ")";
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

    @AutoService(BuiltInFunction.class)
    public static class CountFn extends BuiltInFunction<AggregateValue> {
        public CountFn() {
            super("count",
                    ImmutableList.of(new Type.Any()), CountFn::encapsulate);
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull ParserContext parserContext,
                                                  @Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final List<Typed> arguments) {
            final Typed arg0 = arguments.get(0);
            return new CountValue(PhysicalOperator.COUNT, (Value)arg0);
        }
    }

    @AutoService(BuiltInFunction.class)
    public static class CountStarFn extends BuiltInFunction<AggregateValue> {
        public CountStarFn() {
            super("count",
                    ImmutableList.of(), CountStarFn::encapsulate);
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull ParserContext parserContext,
                                                  @Nonnull BuiltInFunction<AggregateValue> builtInFunction,
                                                  @Nonnull final List<Typed> arguments) {
            return new CountValue(PhysicalOperator.COUNT_STAR, null);
        }
    }

    public enum PhysicalOperator {
        COUNT(TypeCode.LONG, v -> v == null ? 0L : 1L, (s, v) -> Math.addExact((long)s, (long)v), s -> (long)s),
        COUNT_STAR(TypeCode.LONG, v -> 1L, (s, v) -> Math.addExact((long)s, (long)v), s -> (long)s);

        @Nonnull
        private final TypeCode resultType;

        @Nonnull
        private final UnaryOperator<Object> initialToPartialFunction;

        @Nonnull
        private final BinaryOperator<Object> partialToPartialFunction;

        @Nonnull
        private final UnaryOperator<Object> partialToFinalFunction;

        PhysicalOperator(@Nonnull final TypeCode resultType,
                         @Nonnull final UnaryOperator<Object> initialToPartialFunction,
                         @Nonnull final BinaryOperator<Object> partialToPartialFunction,
                         @Nonnull final UnaryOperator<Object> partialToFinalFunction) {
            this.resultType = resultType;
            this.initialToPartialFunction = initialToPartialFunction;
            this.partialToPartialFunction = partialToPartialFunction;
            this.partialToFinalFunction = partialToFinalFunction;
        }

        @Nonnull
        public TypeCode getResultTypeCode() {
            return resultType;
        }

        @Nonnull
        public UnaryOperator<Object> getInitialToPartialFunction() {
            return initialToPartialFunction;
        }

        @Nonnull
        public BinaryOperator<Object> getPartialToPartialFunction() {
            return partialToPartialFunction;
        }

        @Nonnull
        public UnaryOperator<Object> getPartialToFinalFunction() {
            return partialToFinalFunction;
        }

        @Nullable
        public Object evalInitialToPartial(@Nullable Object object) {
            return initialToPartialFunction.apply(object);
        }

        @Nullable
        public Object evalPartialToPartial(@Nullable Object object1, @Nullable Object object2) {
            return partialToPartialFunction.apply(object1 == null ? 0L : object1, object2 == null ? 0L : object2);
        }

        @Nullable
        public Object evalPartialToFinal(@Nullable Object object) {
            if (object == null) {
                return 0L;
            }
            return partialToFinalFunction.apply(object);
        }
    }

    public static class SumAccumulator implements Accumulator {
        private final PhysicalOperator physicalOperator;
        Object state = null;

        public SumAccumulator(@Nonnull final PhysicalOperator physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public void accumulate(@Nullable final Object currentObject) {
            this.state = physicalOperator.evalPartialToPartial(state, currentObject);
        }

        @Nullable
        @Override
        public Object finish() {
            return physicalOperator.evalPartialToFinal(state);
        }
    }
}
