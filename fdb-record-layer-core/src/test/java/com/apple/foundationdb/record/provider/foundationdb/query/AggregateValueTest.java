/*
 * AggregateValueTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.query.plan.temp.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.temp.dynamic.TypeRepository;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.CountValue;
import com.apple.foundationdb.record.query.predicates.NumericAggregationValue;
import com.apple.foundationdb.record.query.predicates.NumericAggregationValue.PhysicalOperator;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.query.predicates.LiteralValue.ofScalar;

/**
 * Test the AggregateValues implementation through mock inner Value usage.
 */
class AggregateValueTest {
    private Integer[] ints;
    private Integer[] intsWithNulls;
    private Integer[] intsOnlyNull;
    private Long[] longs;
    private Long[] longsWithNulls;
    private Long[] longsOnlyNull;
    private Float[] floats;
    private Float[] floatsWithNulls;
    private Float[] floatsOnlyNull;
    private Double[] doubles;
    private Double[] doublesWithNulls;
    private Double[] doublesOnlyNull;

    @BeforeEach
    void setup() {
        ints = new Integer[] { 1, 2, 3, 4, 5, 6 };
        intsWithNulls = new Integer[] { 1, 2, null, 4, 5, 6 };
        intsOnlyNull = new Integer[] { null };

        longs = new Long[] { 1L, 2L, 3L, 4L, 5L, 6L };
        longsWithNulls = new Long[] { 1L, 2L, null, 4L, 5L, 6L };
        longsOnlyNull = new Long[] { null };

        floats = new Float[] { 1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F };
        floatsWithNulls = new Float[] { 1.0F, 2.0F, null, 4.0F, 5.0F, 6.0F };
        floatsOnlyNull = new Float[] { null };

        doubles = new Double[] { 1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D };
        doublesWithNulls = new Double[] { 1.0D, 2.0D, null, 4.0D, 5.0D, 6.0D };
        doublesOnlyNull = new Double[] { null };
    }

    @Test
    void testSum() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_I, ofScalar(1)), ints, 21);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_L, ofScalar(1L)), longs, 21L);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_F, ofScalar(1F)), floats, 21F);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_D, ofScalar(1D)), doubles, 21D);
    }

    @Test
    void testSumWithNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_I, ofScalar(1)), intsWithNulls, 18);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_L, ofScalar(1L)), longsWithNulls, 18L);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_F, ofScalar(1F)), floatsWithNulls, 18F);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_D, ofScalar(1D)), doublesWithNulls, 18D);
    }

    @Test
    void testSumOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_I, ofScalar(1)), intsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_L, ofScalar(1L)), longsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_F, ofScalar(1F)), floatsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.SUM_D, ofScalar(1D)), doublesOnlyNull, (Object)null);
    }

    @Test
    void testMin() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_I, ofScalar(1)), ints, 1);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_L, ofScalar(1L)), longs, 1L);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_F, ofScalar(1F)), floats, 1F);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_D, ofScalar(1D)), doubles, 1D);
    }

    @Test
    void testMinWithNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_I, ofScalar(1)), intsWithNulls, 1);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_L, ofScalar(1L)), longsWithNulls, 1L);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_F, ofScalar(1F)), floatsWithNulls, 1F);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_D, ofScalar(1D)), doublesWithNulls, 1D);
    }

    @Test
    void testMinOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_I, ofScalar(1)), intsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_L, ofScalar(1L)), longsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_F, ofScalar(1F)), floatsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MIN_D, ofScalar(1D)), doublesOnlyNull, (Object)null);
    }

    @Test
    void testMax() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_I, ofScalar(1)), ints, 6);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_L, ofScalar(1L)), longs, 6L);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_F, ofScalar(1F)), floats, 6F);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_D, ofScalar(1D)), doubles, 6D);
    }

    @Test
    void testMaxWithNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_I, ofScalar(1)), intsWithNulls, 6);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_L, ofScalar(1L)), longsWithNulls, 6L);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_F, ofScalar(1F)), floatsWithNulls, 6F);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_D, ofScalar(1D)), doublesWithNulls, 6D);
    }

    @Test
    void testMaxOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_I, ofScalar(1)), intsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_L, ofScalar(1L)), longsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_F, ofScalar(1F)), floatsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.MAX_D, ofScalar(1D)), doublesOnlyNull, (Object)null);
    }

    @Test
    void testAvg() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_I, ofScalar(1)), pairsForAvg(ints), 3.5D);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_L, ofScalar(1L)), pairsForAvg(longs), 3.5D);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_F, ofScalar(1F)), pairsForAvg(floats), 3.5D);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_D, ofScalar(1D)), pairsForAvg(doubles), 3.5D);
    }

    @Test
    void testAvgWithNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_I, ofScalar(1)), pairsForAvg(intsWithNulls), 3.6D);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_L, ofScalar(1L)), pairsForAvg(longsWithNulls), 3.6D);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_F, ofScalar(1F)), pairsForAvg(floatsWithNulls), 3.6D);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_D, ofScalar(1D)), pairsForAvg(doublesWithNulls), 3.6D);
    }

    @Test
    void testAvgOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_I, ofScalar(1)), pairsForAvg(intsOnlyNull), (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_L, ofScalar(1L)), pairsForAvg(longsOnlyNull), (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_F, ofScalar(1F)), pairsForAvg(floatsOnlyNull), (Object)null);
        accumulateAndAssert(new NumericAggregationValue(PhysicalOperator.AVG_D, ofScalar(1D)), pairsForAvg(doublesOnlyNull), (Object)null);
    }

    private Object[] pairsForAvg(Object[] objects) {
        return Arrays.stream(objects)
                .map(object -> object == null ? null : Pair.of(object, 1L)) // left for the sum, right for the count
                .toArray();
    }

    @Test
    void testTupleSumCount() {
        final var tuples =
                Arrays.stream(ints)
                        .map(i -> ImmutableList.of(i, 1L)) // left for the sum, right for the count
                        .toArray();

        final var recordConstructorValue =
                RecordConstructorValue.ofUnnamed(ImmutableList.of(
                        new NumericAggregationValue(PhysicalOperator.SUM_I, ofScalar(1)),
                        new CountValue(CountValue.PhysicalOperator.COUNT, ofScalar(1))
                ));

        accumulateAndAssert(recordConstructorValue, tuples, actual -> {
            Assertions.assertTrue(actual instanceof Message);
            final var actualMessage = (Message)actual;
            final var descriptorForType = actualMessage.getDescriptorForType();
            final var sum = (int)actualMessage.getField(descriptorForType.findFieldByName("_0"));
            final var count = (long)actualMessage.getField(descriptorForType.findFieldByName("_1"));
            Assertions.assertEquals(21, sum);
            Assertions.assertEquals(6L, count);
        });
    }

    private void accumulateAndAssert(final AggregateValue aggregateValue, final Object[] items, @Nullable final Object expected) {
        accumulateAndAssert(aggregateValue, items, actual -> Assertions.assertEquals(expected, actual));
    }

    private void accumulateAndAssert(final AggregateValue aggregateValue, final Object[] items, @Nonnull final Consumer<Object> consumer) {
        final var typeRepository =
                TypeRepository.newBuilder()
                        .addAllTypes(aggregateValue.getDynamicTypes())
                        .build();
        final var accumulator = aggregateValue.createAccumulator(typeRepository);
        Arrays.asList(items).forEach(accumulator::accumulate);
        final var finish = accumulator.finish();
        consumer.accept(finish);
    }
}
