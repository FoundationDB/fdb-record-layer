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

import com.apple.foundationdb.record.cursors.aggregate.AggregateAccumulators;

import javax.annotation.Nonnull;

/**
 * A utility class to offer specific instances of {@link OAggregateValue}s.
 */
public class AggregateValues {

    // ------------------ SUM Aggregators ------------------------------

    /**
     * Aggregate value for Sum of Integers.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Integer, Integer> sumInt(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.SUM, AggregateAccumulators::sumInteger);
    }

    /**
     * Aggregate value for Sum of Longs.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Long, Long> sumLong(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.SUM, AggregateAccumulators::sumLong);
    }

    /**
     * Aggregate value for Sum of Floats.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Float, Float> sumFloat(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.SUM, AggregateAccumulators::sumFloat);
    }

    /**
     * Aggregate value for Sum of Doubles.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Double, Double> sumDouble(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.SUM, AggregateAccumulators::sumDouble);
    }

    // ------------------ MIN Aggregators ------------------------------

    /**
     * Aggregate value for Min of Integers.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Integer, Integer> minInt(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MIN, AggregateAccumulators::minInteger);
    }

    /**
     * Aggregate value for Min of Longs.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Long, Long> minLong(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MIN, AggregateAccumulators::minLong);
    }

    /**
     * Aggregate value for Min of Floats.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Float, Float> minFloat(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MIN, AggregateAccumulators::minFloat);
    }

    /**
     * Aggregate value for Min of Doubles.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Double, Double> minDouble(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MIN, AggregateAccumulators::minDouble);
    }

    // ------------------ MAX Aggregators ------------------------------

    /**
     * Aggregate value for Max of Integers.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Integer, Integer> maxInt(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MAX, AggregateAccumulators::maxInteger);
    }

    /**
     * Aggregate value for Max of Longs.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Long, Long> maxLong(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MAX, AggregateAccumulators::maxLong);
    }

    /**
     * Aggregate value for Max of Floats.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Float, Float> maxFloat(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MAX, AggregateAccumulators::maxFloat);
    }

    /**
     * Aggregate value for Max of Doubles.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Double, Double> maxDouble(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.MAX, AggregateAccumulators::maxDouble);
    }

    // ------------------ AVERAGE Aggregators ------------------------------

    /**
     * Aggregate value for Average of Integers.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Integer, Double> averageInt(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.AVG, AggregateAccumulators::averageInt);
    }

    /**
     * Aggregate value for Average of Longs.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Long, Double> averageLong(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.AVG, AggregateAccumulators::averageLong);
    }

    /**
     * Aggregate value for Average of FLoats.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Float, Double> averageFloat(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.AVG, AggregateAccumulators::averageFloat);
    }

    /**
     * Aggregate value for Average of Double.
     *
     * @param child the inner value to use to evaluate records with.
     *
     * @return an OAggregateValue for the operation and inner
     */
    public static OAggregateValue<Double, Double> averageDouble(@Nonnull Value child) {
        return new OAggregateValue<>(child, OAggregateValue.AggregateType.AVG, AggregateAccumulators::averageDouble);
    }
}
