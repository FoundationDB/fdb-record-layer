/*
 * AggregateValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.cursors.aggregate.RecordValueAccumulator;
import com.apple.foundationdb.record.query.plan.temp.Formatter;
import com.apple.foundationdb.record.query.plan.temp.Type;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;

/**
 * A value representing an aggregate: a value calculated (derived) from other values by applying an aggregation operator
 * (e.g. SUM, MIN, AVG etc.).
 * This class (as all {@link Value}s) is stateless. It is used to evaluate and return runtime results based on the
 * given parameters.
 *
 * @param <T> the type of value being accumulAted by this Value.
 * @param <R> the type of result returned when the accumulation is done
 */
public class AggregateValue<T, R> extends DerivedValue {
    private static final ObjectPlanHash OBJECT_PLAN_HASH = new ObjectPlanHash("Aggregate-Value");

    // The type of aggregation (used for logging)
    @Nonnull
    private final AggregateType aggregateType;

    // The Function used to create an accumulator based off of this value.
    @Nonnull
    private final Function<Value, RecordValueAccumulator<T, R>> accumulatorFunction;

    public enum AggregateType { SUM, MIN, MAX, AVG }

    /**
     * Package protected constructor for use only by {@link AggregateValues} static methods.
     *
     * @param child the inner value that will be evaluated to produce the accumulated numbers
     * @param aggregateType the type of aggregation to perform (used for logging)
     * @param accumulatorFunction the factory function that creates a new accumulator when needed.
     */
    AggregateValue(@Nonnull final Value child, @Nonnull final AggregateType aggregateType, @Nonnull final Function<Value, RecordValueAccumulator<T, R>> accumulatorFunction) {
        super(Collections.singletonList(child));
        this.aggregateType = aggregateType;
        this.accumulatorFunction = accumulatorFunction;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        // it is not possible to simply dispatch on <R> due to type erasure, have to infer the type manually.
        switch (aggregateType) {
            case SUM: // fallthrough
            case MIN: // fallthrough
            case MAX: // fallthrough
                return getChild().getResultType();
            case AVG:
                return Type.primitiveType(Type.TypeCode.DOUBLE);
            default:
                throw new IllegalArgumentException("unexpected aggregation type " + aggregateType);
        }
    }

    /**
     * Create a new accumulator from the Value. This accumulator will perform state management and accumulate evaluated
     * values representing this value. Note that the accumulator contains the {@link #getChild} of this value, as this
     * value
     * does not evaluate.
     *
     * @return a new {@link RecordValueAccumulator} for aggregating values for this Value.
     */
    @Nonnull
    public RecordValueAccumulator<T, R> createAccumulator() {
        return accumulatorFunction.apply(getChild());
    }

    /**
     * Return the single chile {@link Value} for this Value.
     *
     * @return the child Value.
     */
    @Nonnull
    public Value getChild() {
        return getChildren().iterator().next();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, OBJECT_PLAN_HASH, aggregateType, super.planHash(hashKind));
    }

    @Nonnull
    public String explain(@Nonnull final Formatter formatter) {
        return aggregateType.name() + '(' + getChild().explain(formatter) + ')';
    }

    @Override
    public String toString() {
        return "AggregateValue: " + aggregateType.name() + '(' + getChild() + ')';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final AggregateValue<?, ?> that = (AggregateValue<?, ?>)o;
        return aggregateType == that.aggregateType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggregateType);
    }
}
