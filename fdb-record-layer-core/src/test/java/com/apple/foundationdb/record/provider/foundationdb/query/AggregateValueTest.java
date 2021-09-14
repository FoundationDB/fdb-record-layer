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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.cursors.aggregate.RecordValueAccumulator;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.predicates.AggregateValue;
import com.apple.foundationdb.record.query.predicates.AggregateValues;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * Test the AggregateValues implementation through mock inner Value usage.
 */
public class AggregateValueTest {
    private MockValue<Integer> intValue;
    private MockValue<Long> longValue;
    private MockValue<Float> floatValue;
    private MockValue<Double> doubleValue;

    @BeforeEach
    void setup() throws Exception {
        intValue = new MockValue<>(new Integer[] {1, 2, 3, 4, 5, 6});
        longValue = new MockValue<>(new Long[] {1L, 2L, 3L, 4L, 5L, 6L});
        floatValue = new MockValue<>(new Float[] {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F});
        doubleValue = new MockValue<>(new Double[] {1.0D, 2.0D, 3.0D, 4.0D, 5.0D, 6.0D});
    }

    @Test
    void testSumInteger() throws Exception {
        accumulateAndAssert(AggregateValues.sumInt(intValue), 6, 21);
    }

    @Test
    void testSumLong() throws Exception {
        accumulateAndAssert(AggregateValues.sumLong(longValue), 6, 21L);
    }

    @Test
    void testSumFloat() throws Exception {
        accumulateAndAssert(AggregateValues.sumFloat(floatValue), 6, 21.0F);
    }

    @Test
    void testSumDouble() throws Exception {
        accumulateAndAssert(AggregateValues.sumDouble(doubleValue), 6, 21.0D);
    }

    @Test
    void testMinInteger() throws Exception {
        accumulateAndAssert(AggregateValues.minInt(intValue), 6, 1);
    }

    @Test
    void testMinLong() throws Exception {
        accumulateAndAssert(AggregateValues.minLong(longValue), 6, 1L);
    }

    @Test
    void testMinFloat() throws Exception {
        accumulateAndAssert(AggregateValues.minFloat(floatValue), 6, 1.0F);
    }

    @Test
    void testMinDouble() throws Exception {
        accumulateAndAssert(AggregateValues.minDouble(doubleValue), 6, 1.0D);
    }

    @Test
    void testMaxInteger() throws Exception {
        accumulateAndAssert(AggregateValues.maxInt(intValue), 6, 6);
    }

    @Test
    void testMaxLong() throws Exception {
        accumulateAndAssert(AggregateValues.maxLong(longValue), 6, 6L);
    }

    @Test
    void testMaxFloat() throws Exception {
        accumulateAndAssert(AggregateValues.maxFloat(floatValue), 6, 6.0F);
    }

    @Test
    void testMaxDouble() throws Exception {
        accumulateAndAssert(AggregateValues.maxDouble(doubleValue), 6, 6.0D);
    }

    @Test
    void testAvgInteger() throws Exception {
        accumulateAndAssert(AggregateValues.averageInt(intValue), 6, 3.5D);
    }

    @Test
    void testAvgLong() throws Exception {
        accumulateAndAssert(AggregateValues.averageLong(longValue), 6, 3.5D);
    }

    @Test
    void testAvgFloat() throws Exception {
        accumulateAndAssert(AggregateValues.averageFloat(floatValue), 6, 3.5D);
    }

    @Test
    void testAvgDouble() throws Exception {
        accumulateAndAssert(AggregateValues.averageDouble(doubleValue), 6, 3.5D);
    }

    private <S> void accumulateAndAssert(final AggregateValue<?, ?> aggregateValue, final int accumulateCount, final Object value) {
        RecordValueAccumulator<?, ?> accumulator = aggregateValue.createAccumulator();
        accumulate(accumulateCount, accumulator);
        assertFinish(accumulator, value);
    }

    private <S> void assertFinish(final RecordValueAccumulator<?, ?> accumulator, final Object value) {
        Assertions.assertEquals(value, accumulator.finish());
    }

    private <S> void accumulate(final int count, final RecordValueAccumulator<?, ?> accumulator) {
        for (int i = 0; i < count; i++) {
            accumulator.accumulate(null, null, null, null);
        }
    }

    private static class MockValue<T> implements Value {
        @Nonnull
        List<T> values; // The objects to be returned by this value's eval() method

        int index = 0;

        public MockValue(final T[] values) {
            this.values = Arrays.asList(values);
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return 0;
        }

        @Override
        public int semanticHashCode() {
            return 0;
        }

        @Nonnull
        @Override
        public Iterable<? extends Value> getChildren() {
            return null;
        }

        @Nonnull
        @Override
        public Value withChildren(final Iterable<? extends Value> newChildren) {
            return null;
        }

        @Nullable
        @Override
        public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
            return values.get((index++) % values.size());
        }
    }
}
