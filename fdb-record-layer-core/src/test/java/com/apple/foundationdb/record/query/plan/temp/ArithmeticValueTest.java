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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.List;
import java.util.stream.Stream;

/**
 * Tests evaluation of {@link ArithmeticValue}.
 */
class ArithmeticValueTest {
    private static final FieldValue F = new FieldValue(QuantifiedObjectValue.of(CorrelationIdentifier.of("ident"), new Type.Any()), ImmutableList.of("rec_no"), Type.primitiveType(Type.TypeCode.LONG));
    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), null);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Long> LONG_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Float> FLOAT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 2.0F);
    private static final LiteralValue<Double> DOUBLE_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 1.0);
    private static final LiteralValue<Double> DOUBLE_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 2.0);
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "a");
    private static final LiteralValue<String> STRING_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "b");
    private static final DynamicSchema.Builder dynamicSchemaBuilder = DynamicSchema.newBuilder().setName("foo").setPackage("a.b.c");
    @SuppressWarnings({"ConstantConditions"})
    private static final ParserContext parserContext = new ParserContext(null, dynamicSchemaBuilder, null, null);
    private static final EvaluationContext evaluationContext = EvaluationContext.forBinding(Bindings.Internal.CORRELATION.bindingName("ident"), TestRecords7Proto.MyRecord1.newBuilder().setRecNo(4L).build());

    static class BinaryPredicateTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(List.of(INT_1, INT_1), new ArithmeticValue.AddFn(), 2, false),
                    Arguments.of(List.of(INT_1, INT_1), new ArithmeticValue.SubFn(), 0, false),
                    Arguments.of(List.of(INT_2, INT_2), new ArithmeticValue.MulFn(), 4, false),
                    Arguments.of(List.of(INT_2, INT_2), new ArithmeticValue.DivFn(), 1, false),
                    Arguments.of(List.of(INT_2, INT_1), new ArithmeticValue.ModFn(), 0, false),

                    Arguments.of(List.of(LONG_1, LONG_1), new ArithmeticValue.AddFn(), 2L, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new ArithmeticValue.SubFn(), -1L, false),
                    Arguments.of(List.of(LONG_2, LONG_2), new ArithmeticValue.MulFn(), 4L, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new ArithmeticValue.DivFn(), 0L, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new ArithmeticValue.ModFn(), 1L, false),

                    Arguments.of(List.of(FLOAT_1, FLOAT_1), new ArithmeticValue.AddFn(), 2.0F, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new ArithmeticValue.SubFn(), -1.0F, false),
                    Arguments.of(List.of(FLOAT_2, FLOAT_2), new ArithmeticValue.MulFn(), 4.0F, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new ArithmeticValue.DivFn(), 0.5F, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new ArithmeticValue.ModFn(), 1.0F, false),

                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), new ArithmeticValue.AddFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new ArithmeticValue.SubFn(), -1.0, false),
                    Arguments.of(List.of(DOUBLE_2, DOUBLE_2), new ArithmeticValue.MulFn(), 4.0, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new ArithmeticValue.DivFn(), 0.5, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new ArithmeticValue.ModFn(), 1.0, false),

                    Arguments.of(List.of(STRING_1, STRING_2), new ArithmeticValue.AddFn(), "ab", false),
                    Arguments.of(List.of(STRING_1, INT_1), new ArithmeticValue.AddFn(), "a1", false),
                    Arguments.of(List.of(INT_1, STRING_1), new ArithmeticValue.AddFn(), "1a", false),
                    Arguments.of(List.of(STRING_1, LONG_1), new ArithmeticValue.AddFn(), "a1", false),
                    Arguments.of(List.of(LONG_1, STRING_1), new ArithmeticValue.AddFn(), "1a", false),
                    Arguments.of(List.of(STRING_1, FLOAT_1), new ArithmeticValue.AddFn(), "a1.0", false),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new ArithmeticValue.AddFn(), "1.0a", false),
                    Arguments.of(List.of(STRING_1, DOUBLE_1), new ArithmeticValue.AddFn(), "a1.0", false),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new ArithmeticValue.AddFn(), "1.0a", false),

                    Arguments.of(List.of(LONG_1, INT_1), new ArithmeticValue.AddFn(), 2L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new ArithmeticValue.SubFn(), -1L, false),
                    Arguments.of(List.of(LONG_1, INT_1), new ArithmeticValue.MulFn(), 1L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new ArithmeticValue.DivFn(), 0L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new ArithmeticValue.ModFn(), 1L, false),

                    Arguments.of(List.of(INT_1, LONG_1), new ArithmeticValue.AddFn(), 2L, false),
                    Arguments.of(List.of(INT_1, LONG_2), new ArithmeticValue.SubFn(), -1L, false),
                    Arguments.of(List.of(INT_1, LONG_1), new ArithmeticValue.MulFn(), 1L, false),
                    Arguments.of(List.of(INT_1, LONG_2), new ArithmeticValue.DivFn(), 0L, false),
                    Arguments.of(List.of(INT_1, LONG_2), new ArithmeticValue.ModFn(), 1L, false),

                    Arguments.of(List.of(FLOAT_1, INT_1), new ArithmeticValue.AddFn(), 2.0F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new ArithmeticValue.SubFn(), -1.0F, false),
                    Arguments.of(List.of(FLOAT_1, INT_1), new ArithmeticValue.MulFn(), 1.0F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new ArithmeticValue.DivFn(), 0.5F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new ArithmeticValue.ModFn(), 1.0F, false),

                    Arguments.of(List.of(INT_1, FLOAT_1), new ArithmeticValue.AddFn(), 2.0F, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new ArithmeticValue.SubFn(), -1.0F, false),
                    Arguments.of(List.of(INT_1, FLOAT_1), new ArithmeticValue.MulFn(), 1.0F, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new ArithmeticValue.DivFn(), 0.5F, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new ArithmeticValue.ModFn(), 1.0F, false),

                    Arguments.of(List.of(DOUBLE_1, INT_1), new ArithmeticValue.AddFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new ArithmeticValue.SubFn(), -1.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_1), new ArithmeticValue.MulFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new ArithmeticValue.DivFn(), 0.5, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new ArithmeticValue.ModFn(), 1.0, false),

                    Arguments.of(List.of(INT_1, DOUBLE_1), new ArithmeticValue.AddFn(), 2.0, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new ArithmeticValue.SubFn(), -1.0, false),
                    Arguments.of(List.of(INT_1, DOUBLE_1), new ArithmeticValue.MulFn(), 1.0, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new ArithmeticValue.DivFn(), 0.5, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new ArithmeticValue.ModFn(), 1.0, false),

                    Arguments.of(List.of(FLOAT_1, LONG_1), new ArithmeticValue.AddFn(), 2.0F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new ArithmeticValue.SubFn(), -1.0F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_1), new ArithmeticValue.MulFn(), 1.0F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new ArithmeticValue.DivFn(), 0.5F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new ArithmeticValue.ModFn(), 1.0F, false),

                    Arguments.of(List.of(LONG_1, FLOAT_1), new ArithmeticValue.AddFn(), 2.0F, false),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new ArithmeticValue.SubFn(), -1.0F, false),
                    Arguments.of(List.of(LONG_1, FLOAT_1), new ArithmeticValue.MulFn(), 1.0F, false),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new ArithmeticValue.DivFn(), 0.5F, false),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new ArithmeticValue.ModFn(), 1.0F, false),

                    Arguments.of(List.of(DOUBLE_1, LONG_1), new ArithmeticValue.AddFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new ArithmeticValue.SubFn(), -1.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_1), new ArithmeticValue.MulFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new ArithmeticValue.DivFn(), 0.5, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new ArithmeticValue.ModFn(), 1.0, false),

                    Arguments.of(List.of(LONG_1, DOUBLE_1), new ArithmeticValue.AddFn(), 2.0, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new ArithmeticValue.SubFn(), -1.0, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_1), new ArithmeticValue.MulFn(), 1.0, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new ArithmeticValue.DivFn(), 0.5, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new ArithmeticValue.ModFn(), 1.0, false),

                    Arguments.of(List.of(DOUBLE_1, FLOAT_1), new ArithmeticValue.AddFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new ArithmeticValue.SubFn(), -1.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_1), new ArithmeticValue.MulFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new ArithmeticValue.DivFn(), 0.5, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new ArithmeticValue.ModFn(), 1.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_1), new ArithmeticValue.AddFn(), 2.0, false),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new ArithmeticValue.SubFn(), -1.0, false),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_1), new ArithmeticValue.MulFn(), 1.0, false),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new ArithmeticValue.DivFn(), 0.5, false),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new ArithmeticValue.ModFn(), 1.0, false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.AddFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.SubFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.MulFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.DivFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.ModFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.AddFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.SubFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.MulFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.DivFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.ModFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.AddFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.SubFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.MulFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.DivFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.ModFn(), null, false),

                    /* evaluation of ArithmeticValue having a FieldValue */
                    Arguments.of(List.of(F, INT_1), new ArithmeticValue.AddFn(), 5L, false),
                    Arguments.of(List.of(F, INT_1), new ArithmeticValue.SubFn(), 3L, false),
                    Arguments.of(List.of(F, INT_1), new ArithmeticValue.MulFn(), 4L, false),
                    Arguments.of(List.of(F, INT_1), new ArithmeticValue.DivFn(), 4L, false),
                    Arguments.of(List.of(F, INT_1), new ArithmeticValue.ModFn(), 0L, false),

                    Arguments.of(List.of(INT_1, F), new ArithmeticValue.AddFn(), 5L, false),
                    Arguments.of(List.of(INT_1, F), new ArithmeticValue.SubFn(), -3L, false),
                    Arguments.of(List.of(INT_1, F), new ArithmeticValue.MulFn(), 4L, false),
                    Arguments.of(List.of(INT_1, F), new ArithmeticValue.DivFn(), 0L, false),
                    Arguments.of(List.of(INT_1, F), new ArithmeticValue.ModFn(), 1L, false),

                    Arguments.of(List.of(F, INT_NULL), new ArithmeticValue.AddFn(), null, false),
                    Arguments.of(List.of(F, INT_NULL), new ArithmeticValue.SubFn(), null, false),
                    Arguments.of(List.of(F, INT_NULL), new ArithmeticValue.MulFn(), null, false),
                    Arguments.of(List.of(F, INT_NULL), new ArithmeticValue.DivFn(), null, false),
                    Arguments.of(List.of(F, INT_NULL), new ArithmeticValue.ModFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new ArithmeticValue.AddFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new ArithmeticValue.SubFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new ArithmeticValue.MulFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new ArithmeticValue.DivFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new ArithmeticValue.ModFn(), null, false),

                    /* negative tests */


                    Arguments.of(List.of(STRING_1, INT_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(STRING_1, INT_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(STRING_1, INT_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(STRING_1, INT_1), new ArithmeticValue.ModFn(), null, true),

                    Arguments.of(List.of(INT_1, STRING_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(INT_1, STRING_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(INT_1, STRING_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(INT_1, STRING_1), new ArithmeticValue.ModFn(), null, true),

                    Arguments.of(List.of(STRING_1, LONG_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(STRING_1, LONG_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(STRING_1, LONG_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(STRING_1, LONG_1), new ArithmeticValue.ModFn(), null, true),

                    Arguments.of(List.of(LONG_1, STRING_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new ArithmeticValue.ModFn(), null, true),

                    Arguments.of(List.of(STRING_1, FLOAT_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(STRING_1, FLOAT_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(STRING_1, FLOAT_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(STRING_1, FLOAT_1), new ArithmeticValue.ModFn(), null, true),

                    Arguments.of(List.of(FLOAT_1, STRING_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new ArithmeticValue.ModFn(), null, true),

                    Arguments.of(List.of(STRING_1, DOUBLE_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(STRING_1, DOUBLE_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(STRING_1, DOUBLE_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(STRING_1, DOUBLE_1), new ArithmeticValue.ModFn(), null, true),

                    Arguments.of(List.of(DOUBLE_1, STRING_1), new ArithmeticValue.SubFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new ArithmeticValue.MulFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new ArithmeticValue.DivFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new ArithmeticValue.ModFn(), null, true)
            );
        }
    }

    @ParameterizedTest
    @SuppressWarnings({"rawtypes", "unchecked", "ConstantConditions"})
    @ArgumentsSource(BinaryPredicateTestProvider.class)
    void testPredicate(List<Value> args, BuiltInFunction function, Object result, boolean shouldFail) {
        if (shouldFail) {
            try {
                function.encapsulate(parserContext, args);
                Assertions.fail("expected an exception to be thrown");
            } catch (Exception e) {
                Assertions.assertTrue(e instanceof VerifyException);
                Assertions.assertTrue(e.getMessage().contains("unable to encapsulate arithmetic operation due to type mismatch(es)"));
            }
        } else {
            Typed value = function.encapsulate(parserContext, args);
            Assertions.assertTrue(value instanceof ArithmeticValue);
            Object actualValue = ((ArithmeticValue)value).eval(null, evaluationContext, null, RecordMetaDataProto.Empty.getDefaultInstance());
            Assertions.assertEquals(result, actualValue);
        }
    }
}
