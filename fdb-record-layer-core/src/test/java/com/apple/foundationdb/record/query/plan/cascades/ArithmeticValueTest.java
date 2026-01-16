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

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tests evaluation of {@link ArithmeticValue}.
 */
class ArithmeticValueTest {
    private static final FieldValue F = FieldValue.ofFieldName(QuantifiedObjectValue.of(CorrelationIdentifier.of("ident"), Type.Record.fromFields(true, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("rec_no"))))), "rec_no");
    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_5 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 5);
    private static final LiteralValue<Integer> INT_minus_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), -1);
    private static final LiteralValue<Integer> INT_10000 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 10000);

    private static final LiteralValue<Integer> INT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), null);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Long> LONG_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L);
    private static final LiteralValue<Long> LONG_minus_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), -1L);
    private static final LiteralValue<Long> LONG_10000 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 10000L);

    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Float> FLOAT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 2.0F);
    private static final LiteralValue<Double> DOUBLE_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 1.0);
    private static final LiteralValue<Double> DOUBLE_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 2.0);
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "a");
    private static final LiteralValue<String> STRING_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "b");

    private static final LiteralValue<FloatRealVector> VECTOR_1_0_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {1.0f, 0.0f, 0.0f}));
    private static final LiteralValue<FloatRealVector> VECTOR_0_1_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {0.0f, 1.0f, 0.0f}));
    private static final LiteralValue<FloatRealVector> VECTOR_3_4_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {3.0f, 4.0f, 0.0f}));
    private static final LiteralValue<FloatRealVector> VECTOR_0_0_0 = new LiteralValue<>(Type.Vector.of(false, 32, 3), new FloatRealVector(new float[] {0.0f, 0.0f, 0.0f}));
    private static final LiteralValue<DoubleRealVector> VECTOR_DOUBLE_1_2_2 = new LiteralValue<>(Type.Vector.of(false, 64, 3), new DoubleRealVector(new double[] {1.0, 2.0, 2.0}));
    private static final LiteralValue<DoubleRealVector> VECTOR_DOUBLE_4_5_6 = new LiteralValue<>(Type.Vector.of(false, 64, 3), new DoubleRealVector(new double[] {4.0, 5.0, 6.0}));
    private static final LiteralValue<HalfRealVector> VECTOR_HALF_2_3_6 = new LiteralValue<>(Type.Vector.of(false, 16, 3), new HalfRealVector(new double[] {2.0, 3.0, 6.0}));
    private static final LiteralValue<HalfRealVector> VECTOR_HALF_5_6_9 = new LiteralValue<>(Type.Vector.of(false, 16, 3), new HalfRealVector(new double[] {5.0, 6.0, 9.0}));
    private static final LiteralValue<FloatRealVector> VECTOR_NULL = new LiteralValue<>(Type.Vector.of(false, 32, 3), null);

    private static final EvaluationContext evaluationContext = EvaluationContext.forBinding(Bindings.Internal.CORRELATION.bindingName("ident"), QueryResult.ofComputed(TestRecords7Proto.MyRecord1.newBuilder().setRecNo(4L).build()));

    static class BinaryPredicateTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(List.of(INT_1, INT_1), new ArithmeticValue.AddFn(), 2, false),
                    Arguments.of(List.of(INT_1, INT_1), new ArithmeticValue.SubFn(), 0, false),
                    Arguments.of(List.of(INT_2, INT_2), new ArithmeticValue.MulFn(), 4, false),
                    Arguments.of(List.of(INT_2, INT_2), new ArithmeticValue.DivFn(), 1, false),
                    Arguments.of(List.of(INT_2, INT_1), new ArithmeticValue.ModFn(), 0, false),
                    Arguments.of(List.of(INT_5, INT_2), new ArithmeticValue.BitmapBucketOffsetFn(), 4, false),
                    Arguments.of(List.of(INT_1, INT_2), new ArithmeticValue.BitmapBucketOffsetFn(), 0, false),
                    Arguments.of(List.of(INT_1, INT_10000), new ArithmeticValue.BitmapBucketOffsetFn(), 0, false),
                    Arguments.of(List.of(INT_10000, INT_10000), new ArithmeticValue.BitmapBucketOffsetFn(), 10000, false),
                    Arguments.of(List.of(INT_minus_1, INT_10000), new ArithmeticValue.BitmapBucketOffsetFn(), -10000, false),
                    Arguments.of(List.of(INT_5, INT_2), new ArithmeticValue.BitmapBucketNumberFn(), 2, false),
                    Arguments.of(List.of(INT_1, INT_2), new ArithmeticValue.BitmapBucketNumberFn(), 0, false),
                    Arguments.of(List.of(INT_1, INT_10000), new ArithmeticValue.BitmapBucketNumberFn(), 0, false),
                    Arguments.of(List.of(INT_10000, INT_10000), new ArithmeticValue.BitmapBucketNumberFn(), 1, false),
                    Arguments.of(List.of(INT_minus_1, INT_10000), new ArithmeticValue.BitmapBucketNumberFn(), -1, false),
                    Arguments.of(List.of(INT_1, INT_2), new ArithmeticValue.BitmapBitPositionFn(), 1, false),
                    Arguments.of(List.of(INT_1, INT_10000), new ArithmeticValue.BitmapBitPositionFn(), 1, false),
                    Arguments.of(List.of(INT_10000, INT_10000), new ArithmeticValue.BitmapBitPositionFn(), 0, false),
                    Arguments.of(List.of(INT_minus_1, INT_10000), new ArithmeticValue.BitmapBitPositionFn(), 9999, false),

                    Arguments.of(List.of(LONG_1, LONG_1), new ArithmeticValue.AddFn(), 2L, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new ArithmeticValue.SubFn(), -1L, false),
                    Arguments.of(List.of(LONG_2, LONG_2), new ArithmeticValue.MulFn(), 4L, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new ArithmeticValue.DivFn(), 0L, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new ArithmeticValue.ModFn(), 1L, false),
                    Arguments.of(List.of(LONG_1, LONG_10000), new ArithmeticValue.ModFn(), 1L, false),
                    Arguments.of(List.of(LONG_10000, LONG_10000), new ArithmeticValue.ModFn(), 0L, false),
                    Arguments.of(List.of(LONG_minus_1, LONG_10000), new ArithmeticValue.ModFn(), -1L, false),

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
                    Arguments.of(List.of(LONG_1, INT_2), new ArithmeticValue.BitmapBucketOffsetFn(), 0L, false),
                    Arguments.of(List.of(LONG_1, INT_10000), new ArithmeticValue.BitmapBucketOffsetFn(), 0L, false),
                    Arguments.of(List.of(LONG_10000, INT_10000), new ArithmeticValue.BitmapBucketOffsetFn(), 10000L, false),
                    Arguments.of(List.of(LONG_minus_1, INT_10000), new ArithmeticValue.BitmapBucketOffsetFn(), -10000L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new ArithmeticValue.BitmapBucketNumberFn(), 0L, false),
                    Arguments.of(List.of(LONG_1, INT_10000), new ArithmeticValue.BitmapBucketNumberFn(), 0L, false),
                    Arguments.of(List.of(LONG_10000, INT_10000), new ArithmeticValue.BitmapBucketNumberFn(), 1L, false),
                    Arguments.of(List.of(LONG_minus_1, INT_10000), new ArithmeticValue.BitmapBucketNumberFn(), -1L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new ArithmeticValue.BitmapBitPositionFn(), 1L, false),
                    Arguments.of(List.of(LONG_1, INT_10000), new ArithmeticValue.BitmapBitPositionFn(), 1L, false),
                    Arguments.of(List.of(LONG_10000, INT_10000), new ArithmeticValue.BitmapBitPositionFn(), 0L, false),
                    Arguments.of(List.of(LONG_minus_1, INT_10000), new ArithmeticValue.BitmapBitPositionFn(), 9999L, false),

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
                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.BitmapBucketOffsetFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.BitmapBucketNumberFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new ArithmeticValue.BitmapBitPositionFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.AddFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.SubFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.MulFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.DivFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.ModFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.BitmapBucketOffsetFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.BitmapBucketNumberFn(), null, false),
                    Arguments.of(List.of(INT_1, INT_NULL), new ArithmeticValue.BitmapBitPositionFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.AddFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.SubFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.MulFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.DivFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.ModFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.BitmapBucketOffsetFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.BitmapBucketNumberFn(), null, false),
                    Arguments.of(List.of(INT_NULL, INT_1), new ArithmeticValue.BitmapBitPositionFn(), null, false),

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

    static Stream<BuiltInFunction<?>> binaryFunctions() {
        return Stream.of(new ArithmeticValue.AddFn(), new ArithmeticValue.SubFn(), new ArithmeticValue.MulFn(), new ArithmeticValue.DivFn(), new ArithmeticValue.DivFn());
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
                Assertions.assertInstanceOf(VerifyException.class, e);
                Assertions.assertTrue(e.getMessage().contains("unable to encapsulate arithmetic operation due to type mismatch(es)"));
            }
        } else {
            Typed value = function.encapsulate(args);
            Assertions.assertInstanceOf(ArithmeticValue.class, value);
            Object actualValue = ((ArithmeticValue)value).eval(null, evaluationContext);
            Assertions.assertEquals(result, actualValue);
        }
    }

    @ParameterizedTest
    @MethodSource("binaryFunctions")
    void equalsWithSameArguments(BuiltInFunction<?> binaryFunction) {
        final List<Value> arguments = List.of(LONG_1, LONG_2);
        final Value value1 = (Value) binaryFunction.encapsulate(arguments);
        binaryFunctions().forEach(otherFunction -> {
            Value value2 = (Value) otherFunction.encapsulate(arguments);
            boolean sameFunction = binaryFunction.getClass().equals(otherFunction.getClass());
            Assertions.assertEquals(
                    sameFunction,
                    value1.semanticEquals(value2, AliasMap.emptyMap()),
                    () -> (value1 + " and " + value2 + " should only be equal if the functions are equal")
            );
            Assertions.assertEquals(
                    sameFunction,
                    value1.semanticHashCode() == value2.semanticHashCode(),
                    () -> (value1 + " and " + value2 + " should only have the same hash if the functions are equal")
            );
        });
    }

    @ParameterizedTest
    @MethodSource("binaryFunctions")
    void equalsWithDifferentArguments(BuiltInFunction<?> binaryFunction) {
        final List<Value> args1 = List.of(LONG_1, LONG_2);
        final List<Value> args2 = List.of(LONG_1, F);
        final List<Value> args3 = List.of(F, LONG_2);
        final List<List<Value>> argsLists = List.of(args1, args2, args3);
        for (int i = 0; i < argsLists.size(); i++) {
            final Value value1 = (Value) binaryFunction.encapsulate(argsLists.get(i));
            for (int j = 0; j < argsLists.size(); j++) {
                final Value value2 = (Value) binaryFunction.encapsulate(argsLists.get(j));
                Assertions.assertEquals(
                        i == j,
                        value1.semanticEquals(value2, AliasMap.emptyMap()),
                        () -> (value1 + " and " + value2 + " should only be equal if the arguments are equal")
                );
                Assertions.assertEquals(
                        i == j,
                        value1.semanticHashCode() == value2.semanticHashCode(),
                        () -> (value1 + " and " + value2 + " should only have the same hash if the arguments are equal")
                );
            }
        }
    }

    @ParameterizedTest
    @MethodSource("vectorDistanceFunctionTests")
    void testVectorDistanceFunctions(LiteralValue<?> vector1, LiteralValue<?> vector2, BuiltInFunction<?> function, Double expectedDistance) {
        final List<Value> arguments = List.of(vector1, vector2);
        final ArithmeticValue value = (ArithmeticValue) function.encapsulate(arguments);
        final Object result = value.evalWithoutStore(evaluationContext);
        if (expectedDistance == null) {
            Assertions.assertNull(result);
        } else {
            Assertions.assertNotNull(result, "Vector distance function should not return null for non-null vectors");
            Assertions.assertInstanceOf(Double.class, result, "Vector distance function should return a Double");
            Assertions.assertEquals(expectedDistance, (Double)result, 1e-6,
                    String.format("Expected %s(%s, %s) to be %f", function.getFunctionName(), vector1, vector2, expectedDistance));
        }
    }

    static Stream<Arguments> vectorDistanceFunctionTests() {
        return Stream.of(
                // Euclidean distance tests
                // Distance from (1,0,0) to (0,1,0) = sqrt(1^2 + 1^2) = sqrt(2) ≈ 1.414
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new ArithmeticValue.EuclideanDistanceFn(), Math.sqrt(2.0)),
                // Distance from (3,4,0) to (0,0,0) = sqrt(3^2 + 4^2) = 5.0
                Arguments.of(VECTOR_3_4_0, VECTOR_0_0_0, new ArithmeticValue.EuclideanDistanceFn(), 5.0),
                // Distance from same vector to itself = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new ArithmeticValue.EuclideanDistanceFn(), 0.0),
                // Distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new ArithmeticValue.EuclideanDistanceFn(),
                        new Metric.EuclideanMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Euclidean square distance tests
                // Squared distance from (1,0,0) to (0,1,0) = 1^2 + 1^2 = 2.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new ArithmeticValue.EuclideanSquareDistanceFn(), 2.0),
                // Squared distance from (3,4,0) to (0,0,0) = 3^2 + 4^2 = 25.0
                Arguments.of(VECTOR_3_4_0, VECTOR_0_0_0, new ArithmeticValue.EuclideanSquareDistanceFn(), 25.0),
                // Squared distance from same vector to itself = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new ArithmeticValue.EuclideanSquareDistanceFn(), 0.0),
                // Squared distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new ArithmeticValue.EuclideanSquareDistanceFn(),
                        new Metric.EuclideanSquareMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Manhattan distance tests
                // Manhattan distance from (1,0,0) to (0,1,0) = |1-0| + |0-1| + |0-0| = 2.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new ArithmeticValue.ManhattanDistanceFn(), 2.0),
                // Manhattan distance from (3,4,0) to (0,0,0) = |3| + |4| + |0| = 7.0
                Arguments.of(VECTOR_3_4_0, VECTOR_0_0_0, new ArithmeticValue.ManhattanDistanceFn(), 7.0),
                // Manhattan distance from same vector to itself = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new ArithmeticValue.ManhattanDistanceFn(), 0.0),
                // Manhattan distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new ArithmeticValue.ManhattanDistanceFn(),
                        new Metric.ManhattanMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Cosine distance tests
                // Cosine distance between orthogonal vectors (1,0,0) and (0,1,0) = 1.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new ArithmeticValue.CosineDistanceFn(), 1.0),
                // Cosine distance from same vector to itself = 0.0 (identical direction)
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new ArithmeticValue.CosineDistanceFn(), 0.0),
                // Cosine distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new ArithmeticValue.CosineDistanceFn(),
                        new Metric.CosineMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Dot product distance tests (negative dot product)
                // Dot product of (1,0,0) and (0,1,0) = 0, so distance = -0 = 0.0
                Arguments.of(VECTOR_1_0_0, VECTOR_0_1_0, new ArithmeticValue.DotProductDistanceFn(), 0.0),
                // Dot product of (1,0,0) and itself = 1, so distance = -1 = -1.0
                Arguments.of(VECTOR_1_0_0, VECTOR_1_0_0, new ArithmeticValue.DotProductDistanceFn(), -1.0),
                // Dot product distance with double vectors
                Arguments.of(VECTOR_DOUBLE_1_2_2, VECTOR_DOUBLE_4_5_6, new ArithmeticValue.DotProductDistanceFn(),
                        new Metric.DotProductMetric().distance(
                                new DoubleRealVector(new double[] {1.0, 2.0, 2.0}).getData(),
                                new DoubleRealVector(new double[] {4.0, 5.0, 6.0}).getData())),

                // Half precision vector tests
                // Euclidean distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new ArithmeticValue.EuclideanDistanceFn(),
                        new Metric.EuclideanMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Euclidean square distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new ArithmeticValue.EuclideanSquareDistanceFn(),
                        new Metric.EuclideanSquareMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Manhattan distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new ArithmeticValue.ManhattanDistanceFn(),
                        new Metric.ManhattanMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Cosine distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new ArithmeticValue.CosineDistanceFn(),
                        new Metric.CosineMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),
                // Dot product distance with half vectors
                Arguments.of(VECTOR_HALF_2_3_6, VECTOR_HALF_5_6_9, new ArithmeticValue.DotProductDistanceFn(),
                        new Metric.DotProductMetric().distance(
                                new HalfRealVector(new double[] {2.0, 3.0, 6.0}).getData(),
                                new HalfRealVector(new double[] {5.0, 6.0, 9.0}).getData())),

                // Null vector tests
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new ArithmeticValue.EuclideanDistanceFn(), null),
                Arguments.of(VECTOR_NULL, VECTOR_1_0_0, new ArithmeticValue.EuclideanDistanceFn(), null),
                Arguments.of(VECTOR_1_0_0, VECTOR_NULL, new ArithmeticValue.EuclideanDistanceFn(), null),

                Arguments.of(VECTOR_NULL, VECTOR_NULL, new ArithmeticValue.EuclideanSquareDistanceFn(), null),
                Arguments.of(VECTOR_NULL, VECTOR_1_0_0, new ArithmeticValue.EuclideanSquareDistanceFn(), null),
                Arguments.of(VECTOR_1_0_0, VECTOR_NULL, new ArithmeticValue.EuclideanSquareDistanceFn(), null),

                Arguments.of(VECTOR_NULL, VECTOR_NULL, new ArithmeticValue.ManhattanDistanceFn(), null),
                Arguments.of(VECTOR_NULL, VECTOR_1_0_0, new ArithmeticValue.ManhattanDistanceFn(), null),
                Arguments.of(VECTOR_1_0_0, VECTOR_NULL, new ArithmeticValue.ManhattanDistanceFn(), null),

                // Cosine distance with null vectors
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new ArithmeticValue.CosineDistanceFn(), null),
                Arguments.of(VECTOR_NULL, VECTOR_0_1_0, new ArithmeticValue.CosineDistanceFn(), null),
                Arguments.of(VECTOR_0_1_0, VECTOR_NULL, new ArithmeticValue.CosineDistanceFn(), null),

                // Dot product distance with null vectors
                Arguments.of(VECTOR_NULL, VECTOR_NULL, new ArithmeticValue.DotProductDistanceFn(), null),
                Arguments.of(VECTOR_NULL, VECTOR_0_1_0, new ArithmeticValue.DotProductDistanceFn(), null),
                Arguments.of(VECTOR_0_1_0, VECTOR_NULL, new ArithmeticValue.DotProductDistanceFn(), null)
        );
    }

    @ParameterizedTest
    @MethodSource("vectorDistanceFunctions")
    void testVectorDistanceSemanticEquality(BuiltInFunction<?> distanceFunction) {
        final List<Value> arguments1 = List.of(VECTOR_1_0_0, VECTOR_0_1_0);
        final List<Value> arguments2 = List.of(VECTOR_1_0_0, VECTOR_0_1_0);
        final List<Value> arguments3 = List.of(VECTOR_1_0_0, VECTOR_3_4_0);

        final Value value1 = (Value) distanceFunction.encapsulate(arguments1);
        final Value value2 = (Value) distanceFunction.encapsulate(arguments2);
        final Value value3 = (Value) distanceFunction.encapsulate(arguments3);

        // Same arguments should be semantically equal
        Assertions.assertTrue(value1.semanticEquals(value2, AliasMap.emptyMap()),
                value1 + " and " + value2 + " should be semantically equal with same arguments");
        Assertions.assertEquals(value1.semanticHashCode(), value2.semanticHashCode(),
                value1 + " and " + value2 + " should have same hash code");

        // Different arguments should not be semantically equal
        Assertions.assertFalse(value1.semanticEquals(value3, AliasMap.emptyMap()),
                value1 + " and " + value3 + " should not be semantically equal with different arguments");
    }

    static Stream<BuiltInFunction<?>> vectorDistanceFunctions() {
        return Stream.of(
                new ArithmeticValue.EuclideanDistanceFn(),
                new ArithmeticValue.EuclideanSquareDistanceFn(),
                new ArithmeticValue.ManhattanDistanceFn(),
                new ArithmeticValue.CosineDistanceFn(),
                new ArithmeticValue.DotProductDistanceFn()
        );
    }
}
