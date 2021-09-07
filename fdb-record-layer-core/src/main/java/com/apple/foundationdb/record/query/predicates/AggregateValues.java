/*
 * SumAggregateValue.java
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

import javax.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A utility class to offer specific implementations of {@link AggregateValue}s.
 */
public class AggregateValues {

    // ------------------ SUM Aggregators ------------------------------

    /**
     * Aggregate value for Sum of Integers.
     */
    public static class SumInteger extends PrimitiveAggregateBase<Integer> {
        public SumInteger(@Nonnull final Value inner) {
            super(inner, 0, Math::addExact, "SUM", "Sum-Integer-Aggregate-Value", SumInteger::new);
        }
    }

    /**
     * Aggregate value for Sum of Longs.
     */
    public static class SumLong extends PrimitiveAggregateBase<Long> {
        public SumLong(@Nonnull final Value inner) {
            super(inner, 0L, Math::addExact, "SUM", "Sum-Long-Aggregate-Value", SumLong::new);
        }
    }

    /**
     * Aggregate value for Sum of Floats.
     */
    public static class SumFloat extends PrimitiveAggregateBase<Float> {
        public SumFloat(@Nonnull final Value inner) {
            super(inner, 0.0F, Float::sum, "SUM", "Sum-Float-Aggregate-Value", SumFloat::new);
        }
    }

    /**
     * Aggregate value for Sum of Doubles.
     */
    public static class SumDouble extends PrimitiveAggregateBase<Double> {
        public SumDouble(@Nonnull final Value inner) {
            super(inner, 0.0D, Double::sum, "SUM", "Sum-Double-Aggregate-Value", SumDouble::new);
        }
    }

    // ------------------ MIN Aggregators ------------------------------

    /**
     * Aggregate value for Minimum of Integers.
     */
    public static class MinInteger extends PrimitiveAggregateBase<Integer> {
        public MinInteger(@Nonnull final Value inner) {
            super(inner, Integer.MAX_VALUE, Math::min, "MIN", "Min-Integer-Aggregate-Value", MinInteger::new);
        }
    }

    /**
     * Aggregate value for Minimum of Longs.
     */
    public static class MinLong extends PrimitiveAggregateBase<Long> {
        public MinLong(@Nonnull final Value inner) {
            super(inner, Long.MAX_VALUE, Math::min, "MIN", "Min-Long-Aggregate-Value", MinLong::new);
        }
    }

    /**
     * Aggregate value for Minimum of Floats.
     */
    public static class MinFloat extends PrimitiveAggregateBase<Float> {
        public MinFloat(@Nonnull final Value inner) {
            super(inner, Float.MAX_VALUE, Math::min, "MIN", "Min-Float-Aggregate-Value", MinFloat::new);
        }
    }

    /**
     * Aggregate value for Minimum of Doubles.
     */
    public static class MinDouble extends PrimitiveAggregateBase<Double> {
        public MinDouble(@Nonnull final Value inner) {
            super(inner, Double.MAX_VALUE, Math::min, "MIN", "Min-Double-Aggregate-Value", MinDouble::new);
        }
    }

    // ------------------ MAX Aggregators ------------------------------

    /**
     * Aggregate value for Maximum of Integers.
     */
    public static class MaxInteger extends PrimitiveAggregateBase<Integer> {
        public MaxInteger(@Nonnull final Value inner) {
            super(inner, Integer.MIN_VALUE, Math::max, "MAX", "Max-Integer-Aggregate-Value", MaxInteger::new);
        }
    }

    /**
     * Aggregate value for Maximum of Longs.
     */
    public static class MaxLong extends PrimitiveAggregateBase<Long> {
        public MaxLong(@Nonnull final Value inner) {
            super(inner, Long.MIN_VALUE, Math::max, "MAX", "Max-Long-Aggregate-Value", MaxLong::new);
        }
    }

    /**
     * Aggregate value for Maximum of Floats.
     */
    public static class MaxFloat extends PrimitiveAggregateBase<Float> {
        public MaxFloat(@Nonnull final Value inner) {
            super(inner, Float.MIN_VALUE, Math::max, "MAX", "Max-Float-Aggregate-Value", MaxFloat::new);
        }
    }

    /**
     * Aggregate value for Maximum of Doubles.
     */
    public static class MaxDouble extends PrimitiveAggregateBase<Double> {
        public MaxDouble(@Nonnull final Value inner) {
            super(inner, Double.MIN_VALUE, Math::max, "MAX", "Max-Double-Aggregate-Value", MaxDouble::new);
        }
    }

    // ------------------ AVG Aggregators ------------------------------

    /**
     * Aggregate value for Average of Integers.
     */
    public static class AvgInteger extends AverageAggregateBase<Integer> {
        public AvgInteger(@Nonnull final Value inner) {
            super(inner, 0, Math::addExact, (sum, count) -> (sum.doubleValue() / count),
                    "AVG", "Avg-Integer-Aggregate-Value", AvgInteger::new);
        }
    }

    /**
     * Aggregate value for Average of Longs.
     */
    public static class AvgLong extends AverageAggregateBase<Long> {
        public AvgLong(@Nonnull final Value inner) {
            super(inner, 0L, Math::addExact, (sum, count) -> (sum.doubleValue() / count),
                    "AVG", "Avg-Long-Aggregate-Value", AvgLong::new);
        }
    }

    /**
     * Aggregate value for Average of Floats.
     */
    public static class AvgFloat extends AverageAggregateBase<Float> {
        public AvgFloat(@Nonnull final Value inner) {
            super(inner, 0.0F, Float::sum, (sum, count) -> (sum.doubleValue() / count),
                    "AVG", "Avg-Float-Aggregate-Value", AvgFloat::new);
        }
    }

    /**
     * Aggregate value for Average of Doubles.
     */
    public static class AvgDouble extends AverageAggregateBase<Double> {
        public AvgDouble(@Nonnull final Value inner) {
            super(inner, 0.0D, Double::sum, (sum, count) -> (sum / count),
                    "AVG", "Avg-Double-Aggregate-Value", AvgDouble::new);
        }
    }

    /**
     * A base class for {@link AggregateValue} of primitives.
     * For primitive values, the state is the same as the aggregated value and so there is no need to convert S to T.
     *
     * @param <T> the state and aggregated value type.
     */
    private abstract static class PrimitiveAggregateBase<T> extends BaseAggregateValue<T, T> {
        @Nonnull
        private BiFunction<T, T, T> accumulateOp; // the function to apply in order to accumulate values

        public PrimitiveAggregateBase(@Nonnull final Value inner, final T initial,
                                      @Nonnull final BiFunction<T, T, T> accumulateOp,
                                      @Nonnull final String name, @Nonnull final String hashObjectName,
                                      @Nonnull final Function<Value, Value> withChildrenOp) {
            super(inner, initial, name, hashObjectName, withChildrenOp);
            this.accumulateOp = accumulateOp;
        }

        @Nonnull
        @Override
        protected T accumulate(final T currentState, final T next) {
            return accumulateOp.apply(currentState, next);
        }

        @Nonnull
        @Override
        public Object finish(final T currentState) {
            return currentState;
        }
    }

    /**
     * A base class for {@link AggregateValue} of averages.
     * Average is a special case as the state type is different than the final value type. For average, we hold two
     * values for the
     * state: sum and count. For every value we accumulate into sum and increment the count. For finish(), we divide
     * the sum by count. {@link AverageState} holds the actual state value.
     *
     * @param <T> the type of value that this class aggregates
     */
    private abstract static class AverageAggregateBase<T> extends BaseAggregateValue<AverageState<T>, T> {
        @Nonnull
        private final BiFunction<T, T, T> accumulateOp; // the operation used to accumulate values into the sum
        @Nonnull
        private final BiFunction<T, Integer, Double> finishOp; // the operation used to derive result from the state

        public AverageAggregateBase(@Nonnull final Value inner, final T initial,
                                    @Nonnull final BiFunction<T, T, T> accumulateOp, @Nonnull final BiFunction<T, Integer, Double> finishOp,
                                    @Nonnull final String name, @Nonnull final String hashObjectName,
                                    @Nonnull final Function<Value, Value> withChildrenOp) {
            super(inner, new AverageState<>(initial, accumulateOp, finishOp), name, hashObjectName, withChildrenOp);
            this.accumulateOp = accumulateOp;
            this.finishOp = finishOp;
        }

        @Nonnull
        @Override
        protected AverageState<T> accumulate(final AverageState<T> currentState, final T next) {
            return currentState.accumulate(next);
        }

        @Nonnull
        @Override
        public Object finish(final AverageState<T> currentState) {
            return currentState.finish();
        }
    }

    /**
     * Accumulated state for average operations.
     * The accumulator ({@link #sum}) is kept as the same type as the accumulated values, to allow for better accuracy.
     * The final conversion is done when {@link #finish} is called (that's when the values are converted into {@link Double}).
     */
    private static class AverageState<T> {
        @Nonnull
        private final T initial; // store initial to return in the empty case
        @Nonnull
        private final T sum; // The running sum of all the values so far
        private int count; // The running count of items accumulated so far
        @Nonnull
        private final BiFunction<T, T, T> accumulateOp; // The operation to use when accumulating values
        @Nonnull
        private final BiFunction<T, Integer, Double> finishOp; // The operation to use when calculating final result

        public AverageState(final T initial,
                            @Nonnull final BiFunction<T, T, T> accumulateOp,
                            @Nonnull final BiFunction<T, Integer, Double> finishOp) {
            this.sum = initial;
            this.initial = initial;
            this.accumulateOp = accumulateOp;
            this.finishOp = finishOp;
            this.count = 0;
        }

        // Private constructor for when we instantiate a new state following accumulation (since state is immutable)
        private AverageState(final T sum, int count, T initial,
                             @Nonnull final BiFunction<T, T, T> accumulateOp,
                             @Nonnull final BiFunction<T, Integer, Double> finishOp) {
            this.sum = sum;
            this.count = count;
            this.initial = initial;
            this.accumulateOp = accumulateOp;
            this.finishOp = finishOp;
        }

        public AverageState<T> accumulate(T nextValue) {
            // Construct a new state so that the state is immutable (initial state should not be modified)
            return new AverageState<>(accumulateOp.apply(sum, nextValue), count + 1, initial, accumulateOp, finishOp);
        }

        public Object finish() {
            if (count == 0) {
                return 0.0D;
            }
            return finishOp.apply(sum, count);
        }
    }
}
