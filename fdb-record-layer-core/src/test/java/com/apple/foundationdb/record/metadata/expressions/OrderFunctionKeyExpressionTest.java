/*
 * OrderFunctionKeyExpressionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleOrdering;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.KeyExpressionTest.evaluate;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link OrderFunctionKeyExpression}.
 */
class OrderFunctionKeyExpressionTest {

    private static final KeyExpression STR_FIELD = field("str_value_indexed");

    private Message buildMessage(@Nullable String str) {
        final TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder();
        if (str != null) {
            builder.setStrValueIndexed(str);
        }
        return builder.build();
    }

    private static final String[] STRINGS = {
        null, "", "a", "ab", "az"
    };

    @ParameterizedTest
    @EnumSource(TupleOrdering.Direction.class)
    void testOrdering(TupleOrdering.Direction direction) {
        final KeyExpression expression = orderExpression(direction);
        final TreeMap<Tuple, Object> ordered = new TreeMap<>();
        for (String value : STRINGS) {
            Key.Evaluated eval = Iterables.getOnlyElement(evaluate(expression, buildMessage(value)));
            ordered.put(eval.toTuple(), value);
        }
        List<String> expected = Lists.newArrayList(STRINGS);
        if (direction.isCounterflowNulls()) {
            expected.remove(0);
            expected.add(null);
        }
        if (direction.isInverted()) {
            expected = Lists.reverse(expected);
        }
        assertEquals(expected, Lists.newArrayList(ordered.values()));
    }

    @ParameterizedTest
    @EnumSource(TupleOrdering.Direction.class)
    void testInversion(TupleOrdering.Direction direction) {
        final FunctionKeyExpression expression = orderExpression(direction);
        for (String value : STRINGS) {
            Message message = buildMessage(value);
            Key.Evaluated applied = Iterables.getOnlyElement(evaluate(expression, message));
            Key.Evaluated inverted = Iterables.getOnlyElement(((InvertibleFunctionKeyExpression)expression).evaluateInverse(applied));
            Key.Evaluated original = Iterables.getOnlyElement(evaluate(STR_FIELD, message));
            assertEquals(original.toTupleAppropriateList(), inverted.toTupleAppropriateList());
        }
    }

    @Nonnull
    private static FunctionKeyExpression orderExpression(@Nonnull TupleOrdering.Direction direction) {
        return function(OrderFunctionKeyExpressionFactory.FUNCTION_NAME_PREFIX + direction.name().toLowerCase(Locale.ROOT), STR_FIELD);
    }
}
