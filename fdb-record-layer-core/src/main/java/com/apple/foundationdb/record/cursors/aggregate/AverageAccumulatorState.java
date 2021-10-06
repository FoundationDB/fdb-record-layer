/*
 * AverageAccumulatorState.java
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

package com.apple.foundationdb.record.cursors.aggregate;

import javax.annotation.Nullable;

/**
 * Accumulator state for AVERAGE operations. Average is unique as the return value from the aggregation ({@link
 * Double})
 * is different that the accumulated values (for example {@link Integer}).
 * {@link AverageAccumulatorState} holds an {@link AccumulatorState} (of SUM) to add up the values, and divides the
 * total
 * by the counts on {@link #finish}.
 *
 * @param <T> the type of values that are being accumulated by this state
 */
public class AverageAccumulatorState<T extends Number> implements AccumulatorState<T, Double> {
    private AccumulatorState<T, T> total;
    private long count;

    // Static method to create and return properly wired accumulators.
    public static AverageAccumulatorState<Integer> intAverageState() {
        return new AverageAccumulatorState<>(new IntegerState(PrimitiveAccumulatorOperation.SUM));
    }

    public static AverageAccumulatorState<Long> longAverageState() {
        return new AverageAccumulatorState<>(new LongState(PrimitiveAccumulatorOperation.SUM));
    }

    public static AverageAccumulatorState<Float> floatAverageState() {
        return new AverageAccumulatorState<>(new FloatState(PrimitiveAccumulatorOperation.SUM));
    }

    public static AverageAccumulatorState<Double> doubleAverageState() {
        return new AverageAccumulatorState<>(new DoubleState(PrimitiveAccumulatorOperation.SUM));
    }

    /**
     * Private constructor (only to be used through the static initializers).
     */
    private AverageAccumulatorState(AccumulatorState<T, T> total) {
        this.total = total;
    }

    @Override
    public void accumulate(@Nullable final T value) {
        if (value != null) {
            total.accumulate(value);
            count++;
        }
    }

    @Override
    @Nullable
    public Double finish() {
        if (count == 0) {
            return null;
        }

        return total.finish().doubleValue() / count;
    }
}
