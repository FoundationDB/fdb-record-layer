/*
 * IndexOnlyAggregateValue.java
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

/**
 * Represents a compile-time aggregation value that must be backed by an aggregation index, and can not be evaluated
 * at runtime by a streaming aggregation operator.
 * This value will be absorbed by a matching aggregation index at optimisation phase.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class IndexOnlyAggregateValue extends AbstractValue implements AggregateValue, Value.CompileTimeValue, ValueWithChild, IndexableAggregateValue {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Index-Only-Aggregate-Value");

    enum PhysicalOperator {
        MAX_EVER_LONG,
        MIN_EVER_LONG
    }

    @Nonnull
    protected final PhysicalOperator operator;

    @Nonnull
    private final Value child;

    /**
     * Creates a new instance of {@link IndexOnlyAggregateValue}.
     * @param operator the aggregation function.
     * @param child the child {@link Value}.
     */
    protected IndexOnlyAggregateValue(@Nonnull final PhysicalOperator operator,
                                      @Nonnull final Value child) {
        this.operator = operator;
        this.child = child;
    }

    @Nonnull
    @Override
    public Value getChild() {
        return child;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return child.getResultType();
    }

    @Nonnull
    @Override
    public Accumulator createAccumulator(@Nonnull final TypeRepository typeRepository) {
        throw new IllegalStateException("unable to create accumulator in a compile-time aggregation function");
    }

    @Nullable
    @Override
    public <M extends Message> Object evalToPartial(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        throw new IllegalStateException("unable to evalToPartial in a compile-time aggregation function");
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, operator);
    }

    @Override
    public String toString() {
        return operator.name().toLowerCase(Locale.getDefault()) + "(" + child + ")";
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, operator, child);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        return other.getClass() == getClass() && ((IndexOnlyAggregateValue)other).operator.equals(operator);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    static class MinEverLongValue extends IndexOnlyAggregateValue {

        /**
         * Creates a new instance of {@link MinEverLongValue}.
         *
         * @param operator the aggregation function.
         * @param child the child {@link Value}.
         */
        MinEverLongValue(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        @Nonnull
        @Override
        public String getIndexName() {
            return IndexTypes.MIN_EVER_LONG;
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 1);
            final Typed arg0 = arguments.get(0);
            final Type type0 = arg0.getResultType();
            SemanticException.check(type0.isNumeric(), SemanticException.ErrorCode.UNKNOWN, String.format("only numeric types allowed in %s aggregation operation", IndexTypes.MIN_EVER_LONG));
            return new MinEverLongValue(PhysicalOperator.MIN_EVER_LONG, (Value)arg0);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
            return new MinEverLongValue(operator, rebasedChild);
        }
    }

    static class MaxEverLongValue extends IndexOnlyAggregateValue {

        /**
         * Creates a new instance of {@link MaxEverLongValue}.
         *
         * @param operator the aggregation function.
         * @param child the child {@link Value}.
         */
        MaxEverLongValue(@Nonnull final PhysicalOperator operator, @Nonnull final Value child) {
            super(operator, child);
        }

        @Nonnull
        @Override
        public String getIndexName() {
            return IndexTypes.MAX_EVER_LONG;
        }

        @Nonnull
        private static AggregateValue encapsulate(@Nonnull final List<? extends Typed> arguments) {
            Verify.verify(arguments.size() == 1);
            final Typed arg0 = arguments.get(0);
            final Type type0 = arg0.getResultType();
            SemanticException.check(type0.isNumeric(), SemanticException.ErrorCode.UNKNOWN, String.format("only numeric types allowed in %s aggregation operation", IndexTypes.MAX_EVER_LONG));
            return new MaxEverLongValue(PhysicalOperator.MAX_EVER_LONG, (Value)arg0);
        }

        @Nonnull
        @Override
        public ValueWithChild withNewChild(@Nonnull final Value rebasedChild) {
            return new MaxEverLongValue(operator, rebasedChild);
        }
    }

    /**
     * The {@code min_ever} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MinEverLongFn extends BuiltInFunction<AggregateValue> {
        public MinEverLongFn() {
            super("MIN_EVER", ImmutableList.of(new Type.Any()), (ignored, arguments) -> MinEverLongValue.encapsulate(arguments));
        }
    }

    /**
     * The {@code max_ever} function.
     */
    @AutoService(BuiltInFunction.class)
    public static class MaxEverLongFn extends BuiltInFunction<AggregateValue> {
        public MaxEverLongFn() {
            super("MAX_EVER", ImmutableList.of(new Type.Any()), (ignored, arguments) -> MaxEverLongValue.encapsulate(arguments));
        }
    }
}
