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

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue.PhysicalOperator;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.PlanHashable.CURRENT_FOR_CONTINUATION;
import static com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue.ofScalar;

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
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_I, ofScalar(1)), ints, 21);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_L, ofScalar(1L)), longs, 21L);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_F, ofScalar(1F)), floats, 21F);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_D, ofScalar(1D)), doubles, 21D);
    }

    @Test
    void testSumWithNulls() {
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_I, ofScalar(1)), intsWithNulls, 18);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_L, ofScalar(1L)), longsWithNulls, 18L);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_F, ofScalar(1F)), floatsWithNulls, 18F);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_D, ofScalar(1D)), doublesWithNulls, 18D);
    }

    @Test
    void testSumOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_I, ofScalar(1)), intsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_L, ofScalar(1L)), longsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_F, ofScalar(1F)), floatsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Sum(PhysicalOperator.SUM_D, ofScalar(1D)), doublesOnlyNull, (Object)null);
    }

    @Test
    void testBitmap() {
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, ofScalar(1)), bitsetForBitmap(new Object[]{0L, 1L, 2L, 0L}), Arrays.asList(0L, 1L, 2L), 1250); // 111
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, ofScalar(1)), bitsetForBitmap(new Object[]{0L, 1L, 2L, 0L, 64L, 65L, 66L}), Arrays.asList(0L, 1L, 2L, 64L, 65L, 66L), 1250); // 111
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, ofScalar(1)), bitsetForBitmap(new Object[]{0L, 1L, 2L, 0L, 64L, 65L, 66L, 10000L, 10001L, 10100L}), Arrays.asList(0L, 1L, 2L, 64L, 65L, 66L, 10000L, 10001L, 10100L), 1263); // 111
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, ofScalar(1)), bitsetForBitmap(longs), Arrays.asList(longs), 1250); // 1111110
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, ofScalar(1)), bitsetForBitmap(longsWithNulls), List.of(1L, 2L, 4L, 5L, 6L), 1250); // 1110110
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, ofScalar(1)), bitsetForBitmap(longsOnlyNull), null, 1250);
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_I, ofScalar(1)), bitsetForBitmap(ints), Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), 1250); // 1111110
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_I, ofScalar(1)), bitsetForBitmap(intsWithNulls), Arrays.asList(1L, 2L, 4L, 5L, 6L), 1250); // 1110110
        accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_I, ofScalar(1)), bitsetForBitmap(intsOnlyNull), null, 1250);
        Assertions.assertThrows(RecordCoreArgumentException.class, () -> accumulateAndAssertByteArray(new NumericAggregationValue.BitmapConstructAgg(PhysicalOperator.BITMAP_CONSTRUCT_AGG_L, ofScalar(1)), bitsetForBitmap(new Object[]{250001L}), List.of(250001L), 1250)); // 111
    }

    @Test
    void testMin() {
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_I, ofScalar(1)), ints, 1);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_L, ofScalar(1L)), longs, 1L);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_F, ofScalar(1F)), floats, 1F);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_D, ofScalar(1D)), doubles, 1D);
    }

    @Test
    void testMinWithNulls() {
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_I, ofScalar(1)), intsWithNulls, 1);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_L, ofScalar(1L)), longsWithNulls, 1L);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_F, ofScalar(1F)), floatsWithNulls, 1F);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_D, ofScalar(1D)), doublesWithNulls, 1D);
    }

    @Test
    void testMinOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_I, ofScalar(1)), intsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_L, ofScalar(1L)), longsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_F, ofScalar(1F)), floatsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Min(PhysicalOperator.MIN_D, ofScalar(1D)), doublesOnlyNull, (Object)null);
    }

    @Test
    void testMax() {
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_I, ofScalar(1)), ints, 6);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_L, ofScalar(1L)), longs, 6L);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_F, ofScalar(1F)), floats, 6F);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_D, ofScalar(1D)), doubles, 6D);
    }

    @Test
    void testMaxWithNulls() {
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_I, ofScalar(1)), intsWithNulls, 6);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_L, ofScalar(1L)), longsWithNulls, 6L);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_F, ofScalar(1F)), floatsWithNulls, 6F);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_D, ofScalar(1D)), doublesWithNulls, 6D);
    }

    @Test
    void testMaxOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_I, ofScalar(1)), intsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_L, ofScalar(1L)), longsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_F, ofScalar(1F)), floatsOnlyNull, (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Max(PhysicalOperator.MAX_D, ofScalar(1D)), doublesOnlyNull, (Object)null);
    }

    @Test
    void testAvg() {
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_I, ofScalar(1)), pairsForAvg(ints), 3.5D);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_L, ofScalar(1L)), pairsForAvg(longs), 3.5D);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_F, ofScalar(1F)), pairsForAvg(floats), 3.5D);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_D, ofScalar(1D)), pairsForAvg(doubles), 3.5D);
    }

    @Test
    void testAvgWithNulls() {
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_I, ofScalar(1)), pairsForAvg(intsWithNulls), 3.6D);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_L, ofScalar(1L)), pairsForAvg(longsWithNulls), 3.6D);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_F, ofScalar(1F)), pairsForAvg(floatsWithNulls), 3.6D);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_D, ofScalar(1D)), pairsForAvg(doublesWithNulls), 3.6D);
    }

    @Test
    void testAvgOnlyNulls() {
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_I, ofScalar(1)), pairsForAvg(intsOnlyNull), (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_L, ofScalar(1L)), pairsForAvg(longsOnlyNull), (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_F, ofScalar(1F)), pairsForAvg(floatsOnlyNull), (Object)null);
        accumulateAndAssert(new NumericAggregationValue.Avg(PhysicalOperator.AVG_D, ofScalar(1D)), pairsForAvg(doublesOnlyNull), (Object)null);
    }

    @Nonnull
    private Object[] pairsForAvg(@Nonnull Object[] objects) {
        return Arrays.stream(objects)
                .map(object -> object == null ? null : Pair.of(object, 1L)) // left for the sum, right for the count
                .toArray();
    }

    @Nonnull
    private Object[] bitsetForBitmap(@Nonnull Object[] objects) {
        return Arrays.stream(objects)
                .map(object -> {
                    if (object == null) {
                        return null;
                    }
                    final var result = new BitSet();
                    if (object instanceof Integer) {
                        result.set((int)object);
                    } else {
                        result.set(((Long)object).intValue());
                    }
                    return result;
                })
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
                        new NumericAggregationValue.Sum(PhysicalOperator.SUM_I, ofScalar(1)),
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

    @Test
    void testUnmatchedAggregateValueTest() {
        final var value = new GroupByExpression.UnmatchedAggregateValue(CorrelationIdentifier.of("a"));
        Assertions.assertEquals("unmatched(a)", value.toString());

        final var serializationContext = PlanSerializationContext.newForCurrentMode();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> value.toValueProto(serializationContext));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> value.planHash(CURRENT_FOR_CONTINUATION));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> value.toProto(serializationContext));
        Assertions.assertEquals(value, value.withChildren(ImmutableList.of()));
    }

    @Nullable
    public static List<Long> collectOnBits(@Nullable byte[] bitmap, int expectedArrayLength) {
        if (bitmap == null) {
            return null;
        }
        Assertions.assertEquals(expectedArrayLength, bitmap.length);
        final List<Long> result = new ArrayList<>();
        for (int i = 0; i < bitmap.length; i++) {
            if (bitmap[i] != 0) {
                for (int j = 0; j < 8; j++) {
                    if ((bitmap[i] & (1 << j)) != 0) {
                        result.add(i * 8L + j);
                    }
                }
            }
        }
        return result;
    }

    private void accumulateAndAssertByteArray(final AggregateValue aggregateValue, final Object[] items, @Nullable final Object expected, final int expectedArrayLength) {
        accumulateAndAssert(aggregateValue, items, actual -> Assertions.assertEquals(expected, collectOnBits((byte[])actual, expectedArrayLength)));
    }

    private void accumulateAndAssert(final AggregateValue aggregateValue, final Object[] items, @Nullable final Object expected) {
        accumulateAndAssert(aggregateValue, items, actual -> Assertions.assertEquals(expected, actual));
    }

    private void accumulateAndAssert(final AggregateValue aggregateValue, final Object[] items, @Nonnull final Consumer<Object> consumer) {
        final var typeRepository =
                TypeRepository.newBuilder()
                        .addAllTypes(aggregateValue.getDynamicTypes())
                        .build();
        final var accumulator = aggregateValue.createAccumulatorWithInitialState(typeRepository, null);
        Arrays.asList(items).forEach(accumulator::accumulate);
        final var finish = accumulator.finish();
        consumer.accept(finish);
    }
}
