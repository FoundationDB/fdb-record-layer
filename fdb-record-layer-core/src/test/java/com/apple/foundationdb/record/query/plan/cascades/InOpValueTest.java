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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BooleanValue;
import com.apple.foundationdb.record.query.plan.cascades.values.InOpValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

class InOpValueTest {

    private static final TypeRepository.Builder typeRepositoryBuilder = TypeRepository.newBuilder().setName("foo").setPackage("a.b.c");

    @SuppressWarnings("ConstantConditions")
    static class ThrowsValue implements BooleanValue {

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return 0;
        }

        @Override
        public int hashCodeWithoutChildren() {
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
        public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            throw new RuntimeException("Should not be called!");
        }

        @Override
        public Optional<QueryPredicate> toQueryPredicate(@Nonnull final CorrelationIdentifier innermostAlias) {
            return Optional.empty();
        }
    }

    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 3);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Long> LONG_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L);
    private static final LiteralValue<Long> LONG_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 3L);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Float> FLOAT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 2.0F);
    private static final LiteralValue<Float> FLOAT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 3.0F);
    private static final LiteralValue<Double> DOUBLE_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 1.0);
    private static final LiteralValue<Double> DOUBLE_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 2.0);
    private static final LiteralValue<Double> DOUBLE_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 3.0);
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "str1");
    private static final LiteralValue<String> STRING_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "str2");
    private static final LiteralValue<String> STRING_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "str3");
    private static final LiteralValue<Boolean> BOOL_TRUE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), true);
    private static final LiteralValue<Boolean> BOOL_FALSE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    private static final ArithmeticValue ADD_INTS_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(typeRepositoryBuilder, List.of(INT_1, INT_2));
    private static final ArithmeticValue ADD_LONGS_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(typeRepositoryBuilder, List.of(LONG_1, LONG_2));
    private static final ArithmeticValue ADD_FLOATS_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(typeRepositoryBuilder, List.of(FLOAT_1, FLOAT_2));
    private static final ArithmeticValue ADD_DOUBLE_1_2 = (ArithmeticValue) new ArithmeticValue.AddFn().encapsulate(typeRepositoryBuilder, List.of(DOUBLE_1, DOUBLE_2));

    static class TestCaseProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(

                    // INT in [INT...]
                    Arguments.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3), ConstantPredicate.TRUE),
                    Arguments.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2), ConstantPredicate.FALSE),
                    // INT in [INT...] (w/ Arithmetic evaluation)
                    Arguments.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, ADD_INTS_1_2), ConstantPredicate.TRUE),
                    // INT in [INT..., LONG] -> LONG in [LONG...]
                    Arguments.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(INT_1, INT_2, LONG_3)), ConstantPredicate.TRUE),
                    // INT in [INT..., LONG] -> LONG in [LONG...] (w/ Arithmetic evaluation)
                    Arguments.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(INT_1, INT_2, ADD_LONGS_1_2)), ConstantPredicate.TRUE),
                    // INT in [INT..., FLOAT] -> FLOAT in [FLOAT...]
                    Arguments.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(INT_1, INT_2, FLOAT_3)), ConstantPredicate.TRUE),
                    // INT in [INT..., FLOAT] -> FLOAT in [FLOAT...] (w/ Arithmetic evaluation)
                    Arguments.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(INT_1, INT_2, ADD_FLOATS_1_2)), ConstantPredicate.TRUE),
                    // INT in [INT..., DOUBLE] -> DOUBLE in [DOUBLE...]
                    Arguments.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(INT_1, INT_2, DOUBLE_3)), ConstantPredicate.TRUE),
                    // INT in [INT..., DOUBLE] -> DOUBLE in [DOUBLE...] (w/ Arithmetic evaluation)
                    Arguments.of(INT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(INT_1, INT_2, ADD_DOUBLE_1_2)), ConstantPredicate.TRUE),
                    // INT in [STRING...]
                    Arguments.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3), null),
                    // INT in [BOOLEAN...]
                    Arguments.of(INT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE), null),

                    // LONG in [INT...]
                    Arguments.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3), null),
                    // LONG in [LONG...]
                    Arguments.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3), ConstantPredicate.TRUE),
                    Arguments.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2), ConstantPredicate.FALSE),
                    // LONG in [LONG..., FLOAT] -> FLOAT in [FLOAT...]
                    Arguments.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(LONG_1, LONG_2, FLOAT_3)), ConstantPredicate.TRUE),
                    // LONG in [LONG..., FLOAT] -> FLOAT in [FLOAT...] (w/ Arithmetic evaluation)
                    Arguments.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(LONG_1, LONG_2, ADD_FLOATS_1_2)), ConstantPredicate.TRUE),
                    // LONG in [LONG..., DOUBLE] -> DOUBLE in [DOUBLE...]
                    Arguments.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(LONG_1, LONG_2, DOUBLE_3)), ConstantPredicate.TRUE),
                    // LONG in [LONG..., DOUBLE] -> DOUBLE in [DOUBLE...] (w/ Arithmetic evaluation)
                    Arguments.of(LONG_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(LONG_1, LONG_2, ADD_DOUBLE_1_2)), ConstantPredicate.TRUE),
                    // LONG in [STRING...]
                    Arguments.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3), null),
                    // LONG in [BOOLEAN...]
                    Arguments.of(LONG_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE), null),

                    // FLOAT in [INT...]
                    Arguments.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3), null),
                    // FLOAT in [LONG...]
                    Arguments.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3), null),
                    // FLOAT in [FLOAT...]
                    Arguments.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3), ConstantPredicate.TRUE),
                    Arguments.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2), ConstantPredicate.FALSE),
                    // FLOAT in [FLOAT..., DOUBLE] -> DOUBLE in [DOUBLE...]
                    Arguments.of(FLOAT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(FLOAT_1, FLOAT_2, DOUBLE_3)), ConstantPredicate.TRUE),
                    // FLOAT in [FLOAT..., DOUBLE] -> DOUBLE in [DOUBLE...] (w/ Arithmetic evaluation)
                    Arguments.of(FLOAT_3, (new AbstractArrayConstructorValue.ArrayFn()).encapsulate(typeRepositoryBuilder, List.of(FLOAT_1, FLOAT_2, ADD_DOUBLE_1_2)), ConstantPredicate.TRUE),
                    // FLOAT in [STRING...]
                    Arguments.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3), null),
                    // FLOAT in [BOOLEAN...]
                    Arguments.of(FLOAT_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE), null),

                    // DOUBLE in [INT...]
                    Arguments.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3), null),
                    // DOUBLE in [LONG...]
                    Arguments.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3), null),
                    // DOUBLE in [FLOAT...]
                    Arguments.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3), null),
                    // DOUBLE in [DOUBLE...]
                    Arguments.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), ConstantPredicate.TRUE),
                    Arguments.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2), ConstantPredicate.FALSE),
                    // DOUBLE in [STRING...]
                    Arguments.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3), null),
                    // DOUBLE in [BOOLEAN...]
                    Arguments.of(DOUBLE_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE), null),

                    // STRING in [INT...]
                    Arguments.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3), null),
                    // STRING in [LONG...]
                    Arguments.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3), null),
                    // STRING in [FLOAT...]
                    Arguments.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3), null),
                    // STRING in [DOUBLE...]
                    Arguments.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), null),
                    // STRING in [STRING...]
                    Arguments.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3), ConstantPredicate.TRUE),
                    Arguments.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2), ConstantPredicate.FALSE),
                    // STRING in [BOOLEAN...]
                    Arguments.of(STRING_3, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE), null),

                    // BOOLEAN in [INT...]
                    Arguments.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3), null),
                    // BOOLEAN in [LONG...]
                    Arguments.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3), null),
                    // BOOLEAN in [FLOAT...]
                    Arguments.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3), null),
                    // BOOLEAN in [DOUBLE...]
                    Arguments.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), null),
                    // BOOLEAN in [STRING...]
                    Arguments.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(STRING_1, STRING_2, STRING_3), null),
                    // BOOLEAN in [BOOLEAN...]
                    Arguments.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_TRUE, BOOL_FALSE), ConstantPredicate.TRUE),
                    Arguments.of(BOOL_TRUE, AbstractArrayConstructorValue.LightArrayConstructorValue.of(BOOL_FALSE), ConstantPredicate.FALSE)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(TestCaseProvider.class)
    public void testing(Value left, Value values, @Nullable QueryPredicate result) {

        if (result != null) {
            final var inOpValue = new InOpValue.InFn().encapsulate(typeRepositoryBuilder, List.of(left, values));
            Assertions.assertTrue(inOpValue instanceof InOpValue);
            final var actual = ((InOpValue) inOpValue).toQueryPredicate(Quantifier.current());
            Assertions.assertFalse(actual.isEmpty());
            Assertions.assertEquals(actual.get(), result);
        } else {
            Assertions.assertThrows(SemanticException.class, () -> new InOpValue.InFn().encapsulate(typeRepositoryBuilder, List.of(left, values)));
        }
    }
}
