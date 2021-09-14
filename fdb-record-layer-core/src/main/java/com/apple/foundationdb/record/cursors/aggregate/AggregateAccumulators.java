/*
 * AggregateAccumulators.java
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

import com.apple.foundationdb.record.query.predicates.Value;

import javax.annotation.Nonnull;

/**
 * A class that contains static initializers for various aggregate accumulators.
 */
public class AggregateAccumulators {

    // ------------------ SUM Aggregators ------------------------------

    public static RecordValueAccumulator<Integer, Integer> sumInteger(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new IntegerState(PrimitiveAccumulatorOperation.SUM));
    }

    public static RecordValueAccumulator<Long, Long> sumLong(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new LongState(PrimitiveAccumulatorOperation.SUM));
    }

    public static RecordValueAccumulator<Float, Float> sumFloat(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new FloatState(PrimitiveAccumulatorOperation.SUM));
    }

    public static RecordValueAccumulator<Double, Double> sumDouble(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new DoubleState(PrimitiveAccumulatorOperation.SUM));
    }

    // ------------------ MIN Aggregators ------------------------------

    public static RecordValueAccumulator<Integer, Integer> minInteger(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new IntegerState(PrimitiveAccumulatorOperation.MIN));
    }

    public static RecordValueAccumulator<Long, Long> minLong(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new LongState(PrimitiveAccumulatorOperation.MIN));
    }

    public static RecordValueAccumulator<Float, Float> minFloat(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new FloatState(PrimitiveAccumulatorOperation.MIN));
    }

    public static RecordValueAccumulator<Double, Double> minDouble(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new DoubleState(PrimitiveAccumulatorOperation.MIN));
    }

    // ------------------ MAX Aggregators ------------------------------

    public static RecordValueAccumulator<Integer, Integer> maxInteger(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new IntegerState(PrimitiveAccumulatorOperation.MAX));
    }

    public static RecordValueAccumulator<Long, Long> maxLong(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new LongState(PrimitiveAccumulatorOperation.MAX));
    }

    public static RecordValueAccumulator<Float, Float> maxFloat(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new FloatState(PrimitiveAccumulatorOperation.MAX));
    }

    public static RecordValueAccumulator<Double, Double> maxDouble(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, new DoubleState(PrimitiveAccumulatorOperation.MAX));
    }

    // ------------------ AVERAGE Aggregators ------------------------------

    public static RecordValueAccumulator<Integer, Double> averageInt(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, AverageAccumulatorState.intAverageState());
    }

    public static RecordValueAccumulator<Long, Double> averageLong(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, AverageAccumulatorState.longAverageState());
    }

    public static RecordValueAccumulator<Float, Double> averageFloat(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, AverageAccumulatorState.floatAverageState());
    }

    public static RecordValueAccumulator<Double, Double> averageDouble(@Nonnull final Value value) {
        return new RecordValueAccumulator<>(value, AverageAccumulatorState.doubleAverageState());
    }


    private AggregateAccumulators() {
        // private constructor - static methods only in this class. No instances.
    }
}
