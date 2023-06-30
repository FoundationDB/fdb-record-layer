/*
 * ArithmeticValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ScalarFunctionValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tests evaluation of {@link ScalarFunctionValue}.
 */
class ScalarFunctionValueTest {
    private static final FieldValue F = FieldValue.ofFieldName(QuantifiedObjectValue.of(CorrelationIdentifier.of("ident"), Type.Record.fromFields(true, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("rec_no"))))), "rec_no");
    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 3);
    private static final LiteralValue<Integer> INT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), null);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Long> LONG_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L);
    private static final LiteralValue<Long> LONG_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 3L);
    private static final LiteralValue<Long> LONG_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), null);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Float> FLOAT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 2.0F);
    private static final LiteralValue<Float> FLOAT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 3.0F);
    private static final LiteralValue<Float> FLOAT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), null);
    private static final LiteralValue<Double> DOUBLE_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 1.0);
    private static final LiteralValue<Double> DOUBLE_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 2.0);
    private static final LiteralValue<Double> DOUBLE_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 3.0);
    private static final LiteralValue<Double> DOUBLE_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), null);
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "a");
    private static final LiteralValue<String> STRING_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "b");
    private static final LiteralValue<String> STRING_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "c");
    private static final LiteralValue<String> STRING_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), null);
    private static final LiteralValue<Boolean> BOOLEAN_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    private static final LiteralValue<Boolean> BOOLEAN_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    private static final LiteralValue<Boolean> BOOLEAN_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), null);

    private static final TypeRepository.Builder typeRepositoryBuilder = TypeRepository.newBuilder().setName("foo").setPackage("a.b.c");
    @SuppressWarnings({"ConstantConditions"})
    private static final EvaluationContext evaluationContext = EvaluationContext.forBinding(Bindings.Internal.CORRELATION.bindingName("ident"), QueryResult.ofComputed(TestRecords7Proto.MyRecord1.newBuilder().setRecNo(4L).build()));

    static class BinaryPredicateTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    // Greatest Function
                    Arguments.of(List.of(INT_1, INT_1), new ScalarFunctionValue.GreatestFn(), 1, false),
                    Arguments.of(List.of(LONG_1, LONG_1), new ScalarFunctionValue.GreatestFn(), 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_1), new ScalarFunctionValue.GreatestFn(), 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), new ScalarFunctionValue.GreatestFn(), 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_1), new ScalarFunctionValue.GreatestFn(), "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_1), new ScalarFunctionValue.GreatestFn(), false, false),

                    Arguments.of(List.of(INT_1, INT_2), new ScalarFunctionValue.GreatestFn(), 2, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new ScalarFunctionValue.GreatestFn(), 2L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new ScalarFunctionValue.GreatestFn(), 2.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new ScalarFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2), new ScalarFunctionValue.GreatestFn(), "b", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2), new ScalarFunctionValue.GreatestFn(), true, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), new ScalarFunctionValue.GreatestFn(), 3, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), new ScalarFunctionValue.GreatestFn(), 3L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), new ScalarFunctionValue.GreatestFn(), 3.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), new ScalarFunctionValue.GreatestFn(), 3.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), new ScalarFunctionValue.GreatestFn(), "c", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), new ScalarFunctionValue.GreatestFn(), true, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), new ScalarFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), new ScalarFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(INT_1, LONG_2), new ScalarFunctionValue.GreatestFn(), 2L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new ScalarFunctionValue.GreatestFn(), 2L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new ScalarFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new ScalarFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new ScalarFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new ScalarFunctionValue.GreatestFn(), 2.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), new ScalarFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new ScalarFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new ScalarFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new ScalarFunctionValue.GreatestFn(), 2.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new ScalarFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new ScalarFunctionValue.GreatestFn(), 2.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), new ScalarFunctionValue.GreatestFn(), 3.0, false),

                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), new ScalarFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(F, INT_1), new ScalarFunctionValue.GreatestFn(), 4L, false),
                    Arguments.of(List.of(INT_1, F), new ScalarFunctionValue.GreatestFn(), 4L, false),

                    Arguments.of(List.of(F, INT_NULL), new ScalarFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new ScalarFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(INT_1, STRING_1), new ScalarFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new ScalarFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new ScalarFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new ScalarFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), new ScalarFunctionValue.GreatestFn(), null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), new ScalarFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), new ScalarFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), new ScalarFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), new ScalarFunctionValue.GreatestFn(), null, true),

                    // Least Function
                    Arguments.of(List.of(INT_3, INT_3), new ScalarFunctionValue.LeastFn(), 3, false),
                    Arguments.of(List.of(LONG_3, LONG_3), new ScalarFunctionValue.LeastFn(), 3L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_3), new ScalarFunctionValue.LeastFn(), 3.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_3), new ScalarFunctionValue.LeastFn(), 3.0, false),
                    Arguments.of(List.of(STRING_3, STRING_3), new ScalarFunctionValue.LeastFn(), "c", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_2), new ScalarFunctionValue.LeastFn(), true, false),

                    Arguments.of(List.of(INT_3, INT_2), new ScalarFunctionValue.LeastFn(), 2, false),
                    Arguments.of(List.of(LONG_3, LONG_2), new ScalarFunctionValue.LeastFn(), 2L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_2), new ScalarFunctionValue.LeastFn(), 2.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_2), new ScalarFunctionValue.LeastFn(), 2.0, false),
                    Arguments.of(List.of(STRING_3, STRING_2), new ScalarFunctionValue.LeastFn(), "b", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_1), new ScalarFunctionValue.LeastFn(), false, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), new ScalarFunctionValue.LeastFn(), 1, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), new ScalarFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), new ScalarFunctionValue.LeastFn(), 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), new ScalarFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), new ScalarFunctionValue.LeastFn(), "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), new ScalarFunctionValue.LeastFn(), false, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), new ScalarFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), new ScalarFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(INT_1, LONG_2), new ScalarFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new ScalarFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new ScalarFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new ScalarFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new ScalarFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new ScalarFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), new ScalarFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new ScalarFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new ScalarFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new ScalarFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new ScalarFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new ScalarFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), new ScalarFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), new ScalarFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(F, INT_1), new ScalarFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(INT_1, F), new ScalarFunctionValue.LeastFn(), 1L, false),

                    Arguments.of(List.of(F, INT_NULL), new ScalarFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new ScalarFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(INT_1, STRING_1), new ScalarFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new ScalarFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new ScalarFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new ScalarFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), new ScalarFunctionValue.LeastFn(), null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), new ScalarFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), new ScalarFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), new ScalarFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), new ScalarFunctionValue.LeastFn(), null, true)
            );
        }
    }

    @ParameterizedTest
    @SuppressWarnings({"rawtypes", "unchecked", "ConstantConditions"})
    @ArgumentsSource(BinaryPredicateTestProvider.class)
    void testPredicate(List<Value> args, BuiltInFunction function, Object result, boolean shouldFail) {
        if (shouldFail) {
            try {
                function.encapsulate(args);
                Assertions.fail("expected an exception to be thrown");
            } catch (Exception e) {
                Assertions.assertTrue(e instanceof SemanticException);
                Assertions.assertEquals(((SemanticException)e).getErrorCode(), SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
            }
        } else {
            Typed value = function.encapsulate(args);
            Assertions.assertTrue(value instanceof ScalarFunctionValue);
            Object actualValue = ((ScalarFunctionValue)value).eval(null, evaluationContext);
            Assertions.assertEquals(result, actualValue);
        }
    }
}
