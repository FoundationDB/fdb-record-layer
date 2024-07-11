/*
 * InvertibleFunctionKeyExpressionTest.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the {@link InvertibleFunctionKeyExpression}.
 */
public class InvertibleFunctionKeyExpressionTest {
    private static final FunctionKeyExpression ABS_VALUE = function(AbsoluteValueFunctionKeyExpression.NAME, field("field"));
    private static final FunctionKeyExpression WRAP_INT = function(IntWrappingFunction.NAME, field("field"));

    private static InvertibleFunctionKeyExpression asInvertible(FunctionKeyExpression function) {
        assertThat(function, instanceOf(InvertibleFunctionKeyExpression.class));
        return (InvertibleFunctionKeyExpression) function;
    }

    @ParameterizedTest(name = "basicInjectiveFunction[value={0}]")
    @ValueSource(longs = {0, 1, 500, -1, -500, Long.MAX_VALUE, Long.MIN_VALUE})
    void basicInjectiveFunction(long value) {
        List<Key.Evaluated> wrappedList = WRAP_INT.evaluateFunction(null, null, Key.Evaluated.scalar(value));
        assertThat(wrappedList, hasSize(1));
        Key.Evaluated wrapped = wrappedList.get(0);
        assertEquals(1, wrapped.size());
        assertThat(wrapped.getObject(0), instanceOf(String.class));

        assertTrue(asInvertible(WRAP_INT).isInjective());
        List<Key.Evaluated> unwrappedList = asInvertible(WRAP_INT).evaluateInverse(wrapped);
        assertThat(unwrappedList, hasSize(1));
        Key.Evaluated unwrapped = unwrappedList.get(0);
        assertEquals(1, unwrapped.size());
        assertEquals(value, unwrapped.getLong(0));
    }

    @ParameterizedTest(name = "basicNonInjectiveFunction[value={0}]")
    @ValueSource(longs = {0, 1, 500, -1, -500, Long.MAX_VALUE, Long.MIN_VALUE + 1})
    void basicNonInjectiveFunction(long value) {
        List<Key.Evaluated> absValueList = ABS_VALUE.evaluateFunction(null, null, Key.Evaluated.scalar(value));
        assertThat(absValueList, hasSize(1));
        Key.Evaluated absValue = absValueList.get(0);
        assertEquals(Math.abs(value), absValue.getLong(0));

        assertFalse(asInvertible(ABS_VALUE).isInjective());
        List<Key.Evaluated> withAbsValue = asInvertible(ABS_VALUE).evaluateInverse(absValue);
        if (value == 0) {
            assertThat(withAbsValue, hasSize(1));
            assertEquals(List.of(Key.Evaluated.scalar(0L)), withAbsValue);
        } else {
            assertThat(withAbsValue, hasSize(2));
            assertThat(withAbsValue, containsInAnyOrder(Key.Evaluated.scalar(value), Key.Evaluated.scalar(value * -1L)));

            Key.Evaluated unreachable = Key.Evaluated.scalar(-1L * Math.abs(value));
            assertThat(asInvertible(ABS_VALUE).evaluateInverse(unreachable), empty());
        }
    }

    @Test
    void injectiveSingleComparison() {
        Comparisons.ComparisonWithParameter comparison = new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "foo");
        Comparisons.InvertedFunctionComparison invertedFunctionComparison = Comparisons.InvertedFunctionComparison.from(asInvertible(WRAP_INT), comparison);
        assertEquals(Comparisons.Type.EQUALS, invertedFunctionComparison.getType());

        List<Long> values = List.of(-500L, -1L, 0L, 1L, 500L);
        for (long value : values) {
            EvaluationContext evaluationContext = EvaluationContext.forBindings(Bindings.newBuilder()
                    .set("foo", "i:" + value)
                    .build());
            assertEquals(value, invertedFunctionComparison.getComparand(null, evaluationContext));
            for (long comparisonValue : values) {
                assertEquals(comparisonValue == value, invertedFunctionComparison.eval(null, evaluationContext, comparisonValue));
            }
            assertNull(invertedFunctionComparison.eval(null, evaluationContext, null));
        }
    }

    @Test
    void injectiveListComparison() {
        List<Long> longValues = List.of(-500L, -1L, 0L, 1L, 500L);
        List<String> wrappedValues = longValues.stream()
                .map(i -> "i:" + i)
                .collect(Collectors.toList());
        Comparisons.ListComparison comparison = new Comparisons.ListComparison(Comparisons.Type.IN, wrappedValues);
        Comparisons.InvertedFunctionComparison invertedFunctionComparison = Comparisons.InvertedFunctionComparison.from(asInvertible(WRAP_INT), comparison);
        assertEquals(Comparisons.Type.IN, invertedFunctionComparison.getType());

        LongStream.range(-10L, 10L).forEach(val ->
                assertEquals(longValues.contains(val), invertedFunctionComparison.eval(null, EvaluationContext.EMPTY, val)));
    }

    @Test
    void nonInjectiveSingleComparison() {
        Comparisons.ComparisonWithParameter comparison = new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "foo");
        Comparisons.InvertedFunctionComparison invertedFunctionComparison = Comparisons.InvertedFunctionComparison.from(asInvertible(ABS_VALUE), comparison);
        assertEquals(Comparisons.Type.IN, invertedFunctionComparison.getType());

        List<Long> values = List.of(-10L, -5L, 0L, 5L, 10L);
        for (long absValue : values) {
            EvaluationContext evaluationContext = EvaluationContext.forBindings(Bindings.newBuilder()
                    .set("foo", absValue)
                    .build());
            Object comparand = invertedFunctionComparison.getComparand(null, evaluationContext);
            assertThat(comparand, instanceOf(List.class));
            List<?> listComparand = (List<?>)comparand;
            if (absValue < 0L) {
                assertThat(listComparand, empty());
            } else if (absValue == 0L) {
                assertThat(listComparand, hasSize(1));
            } else {
                assertThat(listComparand, hasSize(2));
            }

            for (long comparisonValue : values) {
                assertEquals(Math.abs(comparisonValue) == absValue, invertedFunctionComparison.eval(null, evaluationContext, comparisonValue));
            }

            assertNull(invertedFunctionComparison.eval(null, evaluationContext, null));
        }
    }

    @Test
    void nonInjectiveMultiComparison() {
        List<Long> absValues = List.of(-4L, -1L, 0L, 2L, 5L);
        Comparisons.ListComparison comparison = new Comparisons.ListComparison(Comparisons.Type.IN, absValues);
        Comparisons.InvertedFunctionComparison invertedFunctionComparison = Comparisons.InvertedFunctionComparison.from(asInvertible(ABS_VALUE), comparison);
        assertEquals(Comparisons.Type.IN, invertedFunctionComparison.getType());

        Object comparand = invertedFunctionComparison.getComparand(null, EvaluationContext.EMPTY);
        assertThat(comparand, instanceOf(List.class));
        List<?> listComparand = (List<?>) comparand;
        assertThat(listComparand, containsInAnyOrder(0L, 2L, -2L, 5L, -5L));

        LongStream.range(-10L, 10L).forEach(val ->
                assertEquals(absValues.contains(Math.abs(val)), invertedFunctionComparison.eval(null, EvaluationContext.EMPTY, val)));
    }

    /**
     * Validate that unhandled comparison types throw an error. If support is added for one of these types
     * and this test begins to fail, the author should consider adding unit tests of the additional comparison
     * and then add the type to the exclude list.
     *
     * @param type comparison type to validate fails
     */
    @ParameterizedTest(name = "unsupportedComparisonTypes[type={0}]")
    @EnumSource(value = Comparisons.Type.class, names = {"IN", "EQUALS"}, mode = EnumSource.Mode.EXCLUDE)
    void unsupportedComparisonTypes(Comparisons.Type type) {
        Comparisons.SimpleComparison comparison = new Comparisons.SimpleComparison(type, "foo");
        assertThrows(RecordCoreArgumentException.class, () -> Comparisons.InvertedFunctionComparison.from(asInvertible(WRAP_INT), comparison));
        assertThrows(RecordCoreArgumentException.class, () -> Comparisons.InvertedFunctionComparison.from(asInvertible(ABS_VALUE), comparison));
    }
}
