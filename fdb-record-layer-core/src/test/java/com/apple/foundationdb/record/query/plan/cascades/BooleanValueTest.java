/*
 * BooleanValueTest.java
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.InOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RelOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ThrowsValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tests different aspects of functionality provided by {@link BooleanValue}.
 * <ul>
 *   <li>Compile-time evaluation of simple constant expressions.</li>
 *   <li>Manipulation of field expressions such that they become map-able to {@link QueryPredicate}.</li>
 *   <li>Correctness of mapping {@link RelOpValue} functions to corresponding {@link QueryPredicate}s.</li>
 *   <li>Nullability tests.</li>
 * </ul>
 */
class BooleanValueTest {
    private static final FieldValue F = FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), Type.Record.fromFields(true, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("f"))))), "f");
    private static final LiteralValue<Boolean> BOOL_TRUE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    private static final LiteralValue<Boolean> BOOL_FALSE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    private static final LiteralValue<Boolean> BOOL_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), null);
    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 3);
    private static final LiteralValue<Integer> INT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), null);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Long> LONG_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L);
    private static final LiteralValue<Long> LONG_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 3L);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Float> FLOAT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 2.0F);
    private static final LiteralValue<Float> FLOAT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 3.0F);
    private static final LiteralValue<Double> DOUBLE_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 1.0);
    private static final LiteralValue<Double> DOUBLE_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 2.0);
    private static final LiteralValue<Double> DOUBLE_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 3.0);
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "a");
    private static final LiteralValue<String> STRING_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "b");
    private static final LiteralValue<String> STRING_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "c");
    private static final LiteralValue<byte[]> BYTES_1 = new LiteralValue<>("foo".getBytes(StandardCharsets.UTF_8));
    private static final LiteralValue<byte[]> BYTES_2 = new LiteralValue<>("bar".getBytes(StandardCharsets.UTF_8));
    private static final LiteralValue<byte[]> BYTES_3 = new LiteralValue<>("baz".getBytes(StandardCharsets.UTF_8));
    private static final ThrowsValue THROWS_VALUE = new ThrowsValue(Type.primitiveType(Type.TypeCode.INT));

    private static final TypeRepository.Builder typeRepositoryBuilder = TypeRepository.newBuilder().setName("foo").setPackage("a.b.c");

    private static final ArithmeticValue ADD_INTS_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(List.of(INT_1, INT_2));
    private static final ArithmeticValue ADD_LONGS_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(List.of(LONG_1, LONG_2));
    private static final ArithmeticValue ADD_FLOATS_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(List.of(FLOAT_1, FLOAT_2));
    private static final ArithmeticValue ADD_DOUBLE_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(List.of(DOUBLE_1, DOUBLE_2));

    static class BinaryPredicateTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(List.of(BOOL_TRUE, BOOL_TRUE), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(BOOL_FALSE, BOOL_TRUE), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(BOOL_TRUE, BOOL_TRUE), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(BOOL_FALSE, BOOL_TRUE), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(INT_1, INT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, INT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, INT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, INT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, INT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, INT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, INT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, INT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, INT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, INT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, INT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, INT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(LONG_1, LONG_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, LONG_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, LONG_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, LONG_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, LONG_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, LONG_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, LONG_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, LONG_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, LONG_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, LONG_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(FLOAT_1, FLOAT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, FLOAT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, FLOAT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, DOUBLE_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, DOUBLE_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(STRING_1, STRING_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_1, STRING_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(STRING_1, STRING_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(STRING_1, STRING_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_1, STRING_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_2, STRING_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(STRING_1, STRING_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(STRING_2, STRING_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_1, STRING_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_2, STRING_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(STRING_1, STRING_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_1, STRING_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(STRING_2, STRING_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_1, STRING_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(BYTES_1, BYTES_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(BYTES_1, BYTES_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(BYTES_1, BYTES_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(BYTES_1, BYTES_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(LONG_1, INT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, INT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, INT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, INT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, INT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, INT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, INT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, INT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, INT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, INT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, INT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, INT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(INT_1, LONG_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, LONG_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, LONG_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, LONG_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, LONG_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, LONG_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, LONG_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, LONG_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, LONG_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, LONG_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(FLOAT_1, INT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, INT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, INT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, INT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, INT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, INT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, INT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, INT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, INT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, INT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, INT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, INT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(INT_1, FLOAT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, FLOAT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, FLOAT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, FLOAT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, FLOAT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, FLOAT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, FLOAT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, FLOAT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, FLOAT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, FLOAT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(DOUBLE_1, INT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, INT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, INT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, INT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, INT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, INT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, INT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(INT_1, DOUBLE_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, DOUBLE_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, DOUBLE_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, DOUBLE_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_2, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_2, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_1, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(FLOAT_1, LONG_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, LONG_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, LONG_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, LONG_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(LONG_1, FLOAT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, FLOAT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, FLOAT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, FLOAT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, FLOAT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(DOUBLE_1, LONG_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, LONG_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, LONG_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, LONG_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, LONG_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, LONG_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),


                    Arguments.of(List.of(LONG_1, DOUBLE_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, DOUBLE_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, DOUBLE_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, DOUBLE_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_2, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_2, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_1, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(DOUBLE_1, FLOAT_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, FLOAT_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, FLOAT_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_2, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_2, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_1), new RelOpValue.EqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new RelOpValue.EqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_1), new RelOpValue.NotEqualsFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new RelOpValue.LtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, DOUBLE_1), new RelOpValue.LtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new RelOpValue.GtFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, DOUBLE_1), new RelOpValue.GtFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_2, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_1), new RelOpValue.LteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new RelOpValue.GteFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_2, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_1, DOUBLE_1), new RelOpValue.GteFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(INT_NULL, INT_NULL), new RelOpValue.EqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_2), new RelOpValue.EqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new RelOpValue.NotEqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_2), new RelOpValue.NotEqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_2), new RelOpValue.LtFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_2, INT_NULL), new RelOpValue.LtFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_2), new RelOpValue.GtFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_2, INT_NULL), new RelOpValue.GtFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_2), new RelOpValue.LteFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_2, INT_NULL), new RelOpValue.LteFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new RelOpValue.LteFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_2), new RelOpValue.GteFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_2, INT_NULL), new RelOpValue.GteFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(INT_NULL, INT_NULL), new RelOpValue.GteFn(), ConstantPredicate.NULL),

                    Arguments.of(List.of(BOOL_NULL, BOOL_NULL), new RelOpValue.EqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(BOOL_NULL, BOOL_FALSE), new RelOpValue.EqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(BOOL_NULL, BOOL_TRUE), new RelOpValue.EqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(BOOL_NULL, BOOL_NULL), new RelOpValue.NotEqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(BOOL_NULL, BOOL_FALSE), new RelOpValue.NotEqualsFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(BOOL_NULL, BOOL_TRUE), new RelOpValue.NotEqualsFn(), ConstantPredicate.NULL),

                    /* translation of predicates involving a field value, make sure field value is always LHS */
                    Arguments.of(List.of(F, INT_1), new RelOpValue.EqualsFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1))),
                    Arguments.of(List.of(INT_1, F), new RelOpValue.EqualsFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1))),
                    Arguments.of(List.of(F, INT_1), new RelOpValue.NotEqualsFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, 1))),
                    Arguments.of(List.of(INT_1, F), new RelOpValue.NotEqualsFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.NOT_EQUALS, 1))),
                    Arguments.of(List.of(F, INT_1), new RelOpValue.LtFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 1))),
                    Arguments.of(List.of(INT_1, F), new RelOpValue.LtFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1))),
                    Arguments.of(List.of(F, INT_1), new RelOpValue.GtFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1))),
                    Arguments.of(List.of(INT_1, F), new RelOpValue.GtFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, 1))),
                    Arguments.of(List.of(F, INT_1), new RelOpValue.LteFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1))),
                    Arguments.of(List.of(INT_1, F), new RelOpValue.LteFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1))),
                    Arguments.of(List.of(F, INT_1), new RelOpValue.GteFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, 1))),
                    Arguments.of(List.of(INT_1, F), new RelOpValue.GteFn(), new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN_OR_EQUALS, 1))),

                    Arguments.of(List.of(INT_1), new RelOpValue.IsNullFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(INT_1), new RelOpValue.NotNullFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(LONG_1), new RelOpValue.IsNullFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(LONG_1), new RelOpValue.NotNullFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(FLOAT_1), new RelOpValue.IsNullFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(FLOAT_1), new RelOpValue.NotNullFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(DOUBLE_1), new RelOpValue.IsNullFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(DOUBLE_1), new RelOpValue.NotNullFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(STRING_1), new RelOpValue.IsNullFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(STRING_1), new RelOpValue.NotNullFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(BYTES_1), new RelOpValue.IsNullFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(BYTES_1), new RelOpValue.NotNullFn(), ConstantPredicate.TRUE),

                    Arguments.of(List.of(INT_NULL), new RelOpValue.IsNullFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_NULL), new RelOpValue.NotNullFn(), ConstantPredicate.FALSE),

                    Arguments.of(List.of(BOOL_NULL), new RelOpValue.IsNullFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(BOOL_NULL), new RelOpValue.NotNullFn(), ConstantPredicate.FALSE),

                    Arguments.of(List.of(F), new RelOpValue.IsNullFn(), new ValuePredicate(F, new Comparisons.NullComparison(Comparisons.Type.IS_NULL))),
                    Arguments.of(List.of(F), new RelOpValue.NotNullFn(), new ValuePredicate(F, new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))),

                    /* AND */
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2))), new AndOrValue.AndFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2))), new AndOrValue.AndFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(F, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2))), new AndOrValue.AndFn(), AndPredicate.and(ImmutableList.of(new ValuePredicate(F,
                                    new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1)), ConstantPredicate.TRUE))),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_2)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(F, INT_1))), new AndOrValue.AndFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_NULL)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2))), new AndOrValue.AndFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_NULL, INT_2))), new AndOrValue.AndFn(), ConstantPredicate.NULL),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_NULL, INT_2))), new AndOrValue.AndFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_NULL)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_2))), new AndOrValue.AndFn(), ConstantPredicate.FALSE),
                    /* OR */
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_1))), new AndOrValue.OrFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_2))), new AndOrValue.OrFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(F, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2))), new AndOrValue.OrFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(F, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_2))), new AndOrValue.OrFn(), OrPredicate.or(ImmutableList.of(new ValuePredicate(F,
                                    new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1)), ConstantPredicate.FALSE))),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(F, INT_1))), new AndOrValue.OrFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_NULL)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2))), new AndOrValue.OrFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_2)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_NULL))), new AndOrValue.OrFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_1)),
                            new RelOpValue.EqualsFn().encapsulate(List.of(INT_NULL, INT_2))), new AndOrValue.OrFn(), ConstantPredicate.NULL),

                    /* NOT */
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_1))), new NotValue.NotFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_2, INT_1))), new NotValue.NotFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_NULL, INT_1))), new NotValue.NotFn(), ConstantPredicate.NULL),

                    /* IN */
                    // INT in [INT...]
                    Arguments.of(List.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2)), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // INT in [INT...] (w/ Arithmetic evaluation)
                    Arguments.of(List.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, ADD_INTS_1_2)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // INT in [INT..., LONG] -> LONG in [LONG...]
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(INT_1, INT_2, LONG_3))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // INT in [INT..., LONG] -> LONG in [LONG...] (w/ Arithmetic evaluation)
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(INT_1, INT_2, ADD_LONGS_1_2))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // INT in [INT..., FLOAT] -> FLOAT in [FLOAT...]
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(INT_1, INT_2, FLOAT_3))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // INT in [INT..., FLOAT] -> FLOAT in [FLOAT...] (w/ Arithmetic evaluation)
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(INT_1, INT_2, ADD_FLOATS_1_2))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(ADD_FLOATS_1_2, INT_1, INT_2))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // INT in [INT..., DOUBLE] -> DOUBLE in [DOUBLE...]
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(INT_1, INT_2, DOUBLE_3))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // INT in [INT..., DOUBLE] -> DOUBLE in [DOUBLE...] (w/ Arithmetic evaluation)
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(INT_1, INT_2, ADD_DOUBLE_1_2))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // INT in []
                    Arguments.of(List.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of())), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // INT in [STRING...]
                    Arguments.of(List.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3)), new InOpValue.InFn(), null),
                    // INT in [BYTES...]
                    Arguments.of(List.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2, BYTES_3)), new InOpValue.InFn(), null),
                    // INT in [BOOLEAN...]
                    Arguments.of(List.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE)), new InOpValue.InFn(), null),
                    // LONG in [INT...]
                    Arguments.of(List.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // LONG in [LONG...]
                    Arguments.of(List.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2)), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // LONG in [LONG..., FLOAT] -> FLOAT in [FLOAT...]
                    Arguments.of(List.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(LONG_1, LONG_2, FLOAT_3))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // LONG in [LONG..., FLOAT] -> FLOAT in [FLOAT...] (w/ Arithmetic evaluation)
                    Arguments.of(List.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(LONG_1, LONG_2, ADD_FLOATS_1_2))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // LONG in [LONG..., DOUBLE] -> DOUBLE in [DOUBLE...]
                    Arguments.of(List.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(LONG_1, LONG_2, DOUBLE_3))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // LONG in [LONG..., DOUBLE] -> DOUBLE in [DOUBLE...] (w/ Arithmetic evaluation)
                    Arguments.of(List.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(LONG_1, LONG_2, ADD_DOUBLE_1_2))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // LONG in [STRING...]
                    Arguments.of(List.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3)), new InOpValue.InFn(), null),
                    // LONG in [BYTES...]
                    Arguments.of(List.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2, BYTES_3)), new InOpValue.InFn(), null),
                    // LONG in [BOOLEAN...]
                    Arguments.of(List.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE)), new InOpValue.InFn(), null),
                    // LONG in []
                    Arguments.of(List.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of())), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // FLOAT in [INT...]
                    Arguments.of(List.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // FLOAT in [LONG...]
                    Arguments.of(List.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // FLOAT in [FLOAT...]
                    Arguments.of(List.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2)), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // FLOAT in [FLOAT..., DOUBLE] -> DOUBLE in [DOUBLE...]
                    Arguments.of(List.of(FLOAT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(FLOAT_1, FLOAT_2, DOUBLE_3))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // FLOAT in [FLOAT..., DOUBLE] -> DOUBLE in [DOUBLE...] (w/ Arithmetic evaluation)
                    Arguments.of(List.of(FLOAT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of(FLOAT_1, FLOAT_2, ADD_DOUBLE_1_2))), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // FLOAT in [STRING...]
                    Arguments.of(List.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3)), new InOpValue.InFn(), null),
                    // FLOAT in [BYTES...]
                    Arguments.of(List.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2, BYTES_3)), new InOpValue.InFn(), null),
                    // FLOAT in [BOOLEAN...]
                    Arguments.of(List.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE)), new InOpValue.InFn(), null),
                    // FLOAT in []
                    Arguments.of(List.of(FLOAT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of())), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // DOUBLE in [INT...]
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // DOUBLE in [LONG...]
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // DOUBLE in [FLOAT...]
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    // DOUBLE in [DOUBLE...]
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2, DOUBLE_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2)), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // DOUBLE in [STRING...]
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3)), new InOpValue.InFn(), null),
                    // DOUBLE in [BYTES...]
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2, BYTES_3)), new InOpValue.InFn(), null),
                    // DOUBLE in [BOOLEAN...]
                    Arguments.of(List.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE)), new InOpValue.InFn(), null),
                    // DOUBLE in []
                    Arguments.of(List.of(DOUBLE_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of())), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // STRING in [INT...]
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3)), new InOpValue.InFn(), null),
                    // STRING in [LONG...]
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3)), new InOpValue.InFn(), null),
                    // STRING in [FLOAT...]
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3)), new InOpValue.InFn(), null),
                    // STRING in [DOUBLE...]
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2, DOUBLE_3)), new InOpValue.InFn(), null),
                    // STRING in [STRING...]
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2)), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // STRING in [BYTES...]
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2, BYTES_3)), new InOpValue.InFn(), null),
                    // STRING in [BOOLEAN...]
                    Arguments.of(List.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE)), new InOpValue.InFn(), null),
                    // STRING in []
                    Arguments.of(List.of(STRING_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of())), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // BYTES in [INT...]
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3)), new InOpValue.InFn(), null),
                    // BYTES in [LONG...]
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3)), new InOpValue.InFn(), null),
                    // BYTES in [FLOAT...]
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3)), new InOpValue.InFn(), null),
                    // BYTES in [DOUBLE...]
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2, DOUBLE_3)), new InOpValue.InFn(), null),
                    // BYTES in [STRING...]
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3)), new InOpValue.InFn(), null),
                    // BYTES in [BYTES...]
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2, BYTES_3)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2)), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // BYTES in [BOOLEAN...]
                    Arguments.of(List.of(BYTES_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE)), new InOpValue.InFn(), null),
                    // BYTES in []
                    Arguments.of(List.of(BYTES_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of())), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // BOOLEAN in [INT...]
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3)), new InOpValue.InFn(), null),
                    // BOOLEAN in [LONG...]
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3)), new InOpValue.InFn(), null),
                    // BOOLEAN in [FLOAT...]
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3)), new InOpValue.InFn(), null),
                    // BOOLEAN in [DOUBLE...]
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2, DOUBLE_3)), new InOpValue.InFn(), null),
                    // BOOLEAN in [STRING...]
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3)), new InOpValue.InFn(), null),
                    // BOOLEAN in [BYTES...]
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BYTES_1, BYTES_2, BYTES_3)), new InOpValue.InFn(), null),
                    // BOOLEAN in [BOOLEAN...]
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE)), new InOpValue.InFn(), ConstantPredicate.TRUE),
                    Arguments.of(List.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_FALSE)), new InOpValue.InFn(), ConstantPredicate.FALSE),
                    // BOOLEAN in []
                    Arguments.of(List.of(BOOL_TRUE, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(List.of())), new InOpValue.InFn(), ConstantPredicate.FALSE)
            );
        }
    }

    static class LazyBinaryPredicateTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    /* lazy evaluation tests */
                    Arguments.of(List.of(new RelOpValue.NotEqualsFn().encapsulate(List.of(INT_1, INT_1)),
                                    new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, THROWS_VALUE))),
                            new AndOrValue.AndFn(), ConstantPredicate.FALSE),
                    Arguments.of(List.of(new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, INT_1)),
                                    new RelOpValue.EqualsFn().encapsulate(List.of(INT_1, THROWS_VALUE))),
                            new AndOrValue.OrFn(), ConstantPredicate.TRUE)
            );
        }
    }

    @ParameterizedTest
    @SuppressWarnings({"rawtypes", "unchecked"})
    @ArgumentsSource(BinaryPredicateTestProvider.class)
    void testPredicate(List<Value> args, BuiltInFunction function, QueryPredicate result) {
        if (result != null) {
            Typed value = function.encapsulate(args);
            Assertions.assertTrue(value instanceof BooleanValue);
            value = verifySerialization((Value)value);
            Optional<QueryPredicate> maybePredicate = ((BooleanValue)value).toQueryPredicate(typeRepositoryBuilder.build(), Quantifier.current());
            Assertions.assertFalse(maybePredicate.isEmpty());
            Assertions.assertEquals(result, maybePredicate.get());
        } else {
            Assertions.assertThrows(SemanticException.class, () -> function.encapsulate(args));
        }
    }

    @ParameterizedTest
    @SuppressWarnings({"rawtypes", "unchecked"})
    @ArgumentsSource(LazyBinaryPredicateTestProvider.class)
    void testLazyPredicate(List<Value> args, BuiltInFunction function, QueryPredicate result) {
        if (result != null) {
            Typed value = function.encapsulate(args);
            Assertions.assertTrue(value instanceof BooleanValue);
            Optional<QueryPredicate> maybePredicate = ((BooleanValue)value).toQueryPredicate(typeRepositoryBuilder.build(), Quantifier.current());
            Assertions.assertFalse(maybePredicate.isEmpty());
            Assertions.assertEquals(result, maybePredicate.get());
        } else {
            Assertions.assertThrows(SemanticException.class, () -> function.encapsulate(args));
        }
    }

    @Test
    void passingIncorrectNumberOfResolutionParameterToBuiltInFunctionThrows() {
        try {
            new RelOpValue.EqualsFn().resolveParameterTypes(-1);
            Assertions.fail("expected an exception to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof VerifyException);
            Assertions.assertTrue(e.getMessage().contains("unexpected number of arguments"));
        }
    }

    @Test
    void passingIncorrectIndexToBuiltInFunctionThrows() {
        try {
            new RelOpValue.EqualsFn().resolveParameterType(-1);
            Assertions.fail("expected an exception to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof VerifyException);
            Assertions.assertTrue(e.getMessage().contains("unexpected negative parameter index"));
        }
    }

    @Nonnull
    protected static Value verifySerialization(@Nonnull final Value value) {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        final PValue planProto = value.toValueProto(serializationContext);
        final byte[] serializedValue = planProto.toByteArray();
        final PValue parsedValueProto;
        try {
            parsedValueProto = PValue.parseFrom(serializedValue);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final Value deserializedValue =
                Value.fromValueProto(serializationContext, parsedValueProto);
        Assertions.assertEquals(value.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), deserializedValue.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        Assertions.assertEquals(value, deserializedValue);
        return deserializedValue;
    }
}
