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
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VariadicFunctionValue;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tests evaluation of {@link VariadicFunctionValue}.
 */
class VariadicFunctionValueTest {
    private static final FieldValue F = FieldValue.ofFieldName(QuantifiedObjectValue.of(CorrelationIdentifier.of("ident"), Type.Record.fromFields(true, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("rec_no"))))), "rec_no");
    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 3);
    private static final Typed LIST_INT_1 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3);
    private static final Typed LIST_INT_2 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_3, INT_2, INT_1);
    private static final Typed LIST_INT_3 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_2, INT_3, INT_1);
    private static final Typed LIST_INT_NULL = new LiteralValue<>(new Type.Array(Type.primitiveType(Type.TypeCode.INT)), null);
    private static final LiteralValue<Integer> INT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), null);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Long> LONG_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L);
    private static final LiteralValue<Long> LONG_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 3L);
    private static final Typed LIST_LONG_1 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3);
    private static final LiteralValue<Long> LONG_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), null);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Float> FLOAT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 2.0F);
    private static final LiteralValue<Float> FLOAT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 3.0F);
    private static final Typed LIST_FLOAT_1 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3);
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
    private static final Typed RECORD_1 = new RecordConstructorValue.RecordFn().encapsulate(List.of(STRING_1, INT_1, FLOAT_1));
    private static final Typed RECORD_2 = new RecordConstructorValue.RecordFn().encapsulate(List.of(STRING_2, INT_2, FLOAT_2));
    private static final Typed RECORD_3 = new RecordConstructorValue.RecordFn().encapsulate(List.of(STRING_3, INT_3, FLOAT_3));
    private static final Typed NULL_TYPED = new LiteralValue<>(Type.primitiveType(Type.TypeCode.NULL), null);
    private static final Typed RECORD_NAMED = RecordConstructorValue.ofColumns(ImmutableList.of(
            Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f1")), LiteralValue.ofScalar("sz")),
            Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("f2")), LiteralValue.ofScalar(100)),
            Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.FLOAT), Optional.of("f3")), LiteralValue.ofScalar(100.0f))
    ));

    private static final Type.Record recordTypeUnnamed = Type.Record.fromFields(false, ImmutableList.of(
            Type.Record.Field.unnamedOf(Type.primitiveType(Type.TypeCode.STRING)),
            Type.Record.Field.unnamedOf(Type.primitiveType(Type.TypeCode.INT)),
            Type.Record.Field.unnamedOf(Type.primitiveType(Type.TypeCode.FLOAT))));

    private static final Type.Record recordTypeNamed = Type.Record.fromFields(false, ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("f1")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("f2")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.FLOAT), Optional.of("f3"))));

    private static final Typed RECORD_NULL = new LiteralValue<>(recordTypeUnnamed, null);

    private static TypeRepository typeRepository;

    static {
        final TypeRepository.Builder typeRepositoryBuilder = TypeRepository.newBuilder().setName("foo").setPackage("a.b.c");
        recordTypeUnnamed.defineProtoType(typeRepositoryBuilder);
        recordTypeNamed.defineProtoType(typeRepositoryBuilder);
        typeRepository = typeRepositoryBuilder.build();
    }

    @SuppressWarnings({"ConstantConditions"})
    private static final EvaluationContext evaluationContext = EvaluationContext.forBindingsAndTypeRepository(
            Bindings.newBuilder().set(Bindings.Internal.CORRELATION.bindingName("ident"),
                    QueryResult.ofComputed(TestRecords7Proto.MyRecord1.newBuilder().setRecNo(4L).build())).build(),
            typeRepository);

    private static DynamicMessage getMessageForRecord1() {
        final var values = ImmutableList.of("a", 1, 1.0f);
        final var messageBuilder = typeRepository.newMessageBuilder(recordTypeUnnamed);
        for (int i = 0; i < recordTypeUnnamed.getFields().size(); i++) {
            messageBuilder.setField(messageBuilder.getDescriptorForType().getFields().get(i), values.get(i));
        }
        return messageBuilder.build();
    }

    private static DynamicMessage getMessageForRecordNamed() {
        final var values = ImmutableList.of("sz", 100, 100.0f);
        final var messageBuilder = typeRepository.newMessageBuilder(recordTypeNamed);
        for (int i = 0; i < recordTypeNamed.getFields().size(); i++) {
            messageBuilder.setField(messageBuilder.getDescriptorForType().getFields().get(i), values.get(i));
        }
        return messageBuilder.build();
    }

    static class BinaryPredicateTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    // Greatest Function
                    Arguments.of(List.of(INT_1, INT_1), new VariadicFunctionValue.GreatestFn(), 1, false),
                    Arguments.of(List.of(LONG_1, LONG_1), new VariadicFunctionValue.GreatestFn(), 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_1), new VariadicFunctionValue.GreatestFn(), 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), new VariadicFunctionValue.GreatestFn(), 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_1), new VariadicFunctionValue.GreatestFn(), "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_1), new VariadicFunctionValue.GreatestFn(), false, false),

                    Arguments.of(List.of(INT_1, INT_2), new VariadicFunctionValue.GreatestFn(), 2, false),
                    Arguments.of(List.of(LONG_1, LONG_2), new VariadicFunctionValue.GreatestFn(), 2L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), new VariadicFunctionValue.GreatestFn(), 2.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), new VariadicFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2), new VariadicFunctionValue.GreatestFn(), "b", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2), new VariadicFunctionValue.GreatestFn(), true, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), new VariadicFunctionValue.GreatestFn(), 3, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), new VariadicFunctionValue.GreatestFn(), 3L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), new VariadicFunctionValue.GreatestFn(), 3.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), new VariadicFunctionValue.GreatestFn(), 3.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), new VariadicFunctionValue.GreatestFn(), "c", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), new VariadicFunctionValue.GreatestFn(), true, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), new VariadicFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), new VariadicFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(INT_1, LONG_2), new VariadicFunctionValue.GreatestFn(), 2L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new VariadicFunctionValue.GreatestFn(), 2L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new VariadicFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new VariadicFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new VariadicFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new VariadicFunctionValue.GreatestFn(), 2.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), new VariadicFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new VariadicFunctionValue.GreatestFn(), 2F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new VariadicFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new VariadicFunctionValue.GreatestFn(), 2.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new VariadicFunctionValue.GreatestFn(), 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new VariadicFunctionValue.GreatestFn(), 2.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), new VariadicFunctionValue.GreatestFn(), 3.0, false),

                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), new VariadicFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(F, INT_1), new VariadicFunctionValue.GreatestFn(), 4L, false),
                    Arguments.of(List.of(INT_1, F), new VariadicFunctionValue.GreatestFn(), 4L, false),

                    Arguments.of(List.of(F, INT_NULL), new VariadicFunctionValue.GreatestFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new VariadicFunctionValue.GreatestFn(), null, false),

                    Arguments.of(List.of(INT_1, STRING_1), new VariadicFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new VariadicFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new VariadicFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new VariadicFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), new VariadicFunctionValue.GreatestFn(), null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), new VariadicFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), new VariadicFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), new VariadicFunctionValue.GreatestFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), new VariadicFunctionValue.GreatestFn(), null, true),

                    // Least Function
                    Arguments.of(List.of(INT_3, INT_3), new VariadicFunctionValue.LeastFn(), 3, false),
                    Arguments.of(List.of(LONG_3, LONG_3), new VariadicFunctionValue.LeastFn(), 3L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_3), new VariadicFunctionValue.LeastFn(), 3.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_3), new VariadicFunctionValue.LeastFn(), 3.0, false),
                    Arguments.of(List.of(STRING_3, STRING_3), new VariadicFunctionValue.LeastFn(), "c", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_2), new VariadicFunctionValue.LeastFn(), true, false),

                    Arguments.of(List.of(INT_3, INT_2), new VariadicFunctionValue.LeastFn(), 2, false),
                    Arguments.of(List.of(LONG_3, LONG_2), new VariadicFunctionValue.LeastFn(), 2L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_2), new VariadicFunctionValue.LeastFn(), 2.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_2), new VariadicFunctionValue.LeastFn(), 2.0, false),
                    Arguments.of(List.of(STRING_3, STRING_2), new VariadicFunctionValue.LeastFn(), "b", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_1), new VariadicFunctionValue.LeastFn(), false, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), new VariadicFunctionValue.LeastFn(), 1, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), new VariadicFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), new VariadicFunctionValue.LeastFn(), 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), new VariadicFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), new VariadicFunctionValue.LeastFn(), "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), new VariadicFunctionValue.LeastFn(), false, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), new VariadicFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), new VariadicFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(INT_1, LONG_2), new VariadicFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new VariadicFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new VariadicFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new VariadicFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new VariadicFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new VariadicFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), new VariadicFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new VariadicFunctionValue.LeastFn(), 1F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new VariadicFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new VariadicFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new VariadicFunctionValue.LeastFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new VariadicFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), new VariadicFunctionValue.LeastFn(), 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), new VariadicFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(F, INT_1), new VariadicFunctionValue.LeastFn(), 1L, false),
                    Arguments.of(List.of(INT_1, F), new VariadicFunctionValue.LeastFn(), 1L, false),

                    Arguments.of(List.of(F, INT_NULL), new VariadicFunctionValue.LeastFn(), null, false),
                    Arguments.of(List.of(INT_NULL, F), new VariadicFunctionValue.LeastFn(), null, false),

                    Arguments.of(List.of(INT_1, STRING_1), new VariadicFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new VariadicFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new VariadicFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new VariadicFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), new VariadicFunctionValue.LeastFn(), null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), new VariadicFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), new VariadicFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), new VariadicFunctionValue.LeastFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), new VariadicFunctionValue.LeastFn(), null, true),

                    // Coalesce
                    Arguments.of(List.of(INT_3, INT_3), new VariadicFunctionValue.CoalesceFn(), 3, false),
                    Arguments.of(List.of(LONG_3, LONG_3), new VariadicFunctionValue.CoalesceFn(), 3L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_3), new VariadicFunctionValue.CoalesceFn(), 3.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_3), new VariadicFunctionValue.CoalesceFn(), 3.0, false),
                    Arguments.of(List.of(STRING_3, STRING_3), new VariadicFunctionValue.CoalesceFn(), "c", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_2), new VariadicFunctionValue.CoalesceFn(), true, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_1), new VariadicFunctionValue.CoalesceFn(), List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_1), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_3, INT_2), new VariadicFunctionValue.CoalesceFn(), 3, false),
                    Arguments.of(List.of(LONG_3, LONG_2), new VariadicFunctionValue.CoalesceFn(), 3L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_2), new VariadicFunctionValue.CoalesceFn(), 3.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_2), new VariadicFunctionValue.CoalesceFn(), 3.0, false),
                    Arguments.of(List.of(STRING_3, STRING_2), new VariadicFunctionValue.CoalesceFn(), "c", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_1), new VariadicFunctionValue.CoalesceFn(), true, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_2), new VariadicFunctionValue.CoalesceFn(), List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_2), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),
                    Arguments.of(List.of(RECORD_1, RECORD_NAMED), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),
                    Arguments.of(List.of(RECORD_NAMED, RECORD_1), new VariadicFunctionValue.CoalesceFn(), getMessageForRecordNamed(), false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), new VariadicFunctionValue.CoalesceFn(), 1, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), new VariadicFunctionValue.CoalesceFn(), 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), new VariadicFunctionValue.CoalesceFn(), 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), new VariadicFunctionValue.CoalesceFn(), "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), new VariadicFunctionValue.CoalesceFn(), false, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_2, LIST_INT_3), new VariadicFunctionValue.CoalesceFn(), List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_2, RECORD_3), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), new VariadicFunctionValue.CoalesceFn(), 1, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), new VariadicFunctionValue.CoalesceFn(), 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), new VariadicFunctionValue.CoalesceFn(), 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), new VariadicFunctionValue.CoalesceFn(), "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), new VariadicFunctionValue.CoalesceFn(), false, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_2, LIST_INT_3, LIST_INT_NULL), new VariadicFunctionValue.CoalesceFn(), List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_2, RECORD_3, RECORD_NULL), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_NULL, INT_1, INT_2, INT_3, INT_NULL), new VariadicFunctionValue.CoalesceFn(), 1, false),
                    Arguments.of(List.of(LONG_NULL, LONG_1, LONG_2, LONG_3, LONG_NULL), new VariadicFunctionValue.CoalesceFn(), 1L, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), new VariadicFunctionValue.CoalesceFn(), 1.0F, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(STRING_NULL, STRING_1, STRING_2, STRING_3, STRING_NULL), new VariadicFunctionValue.CoalesceFn(), "a", false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), new VariadicFunctionValue.CoalesceFn(), false, false),
                    Arguments.of(List.of(LIST_INT_NULL, LIST_INT_1, LIST_INT_2, LIST_INT_3, LIST_INT_NULL), new VariadicFunctionValue.CoalesceFn(), List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_NULL, RECORD_1, RECORD_2, RECORD_3, RECORD_NULL), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),
                    Arguments.of(List.of(LIST_INT_NULL, LIST_INT_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),
                    Arguments.of(List.of(RECORD_NULL, RECORD_NULL), new VariadicFunctionValue.CoalesceFn(), null, false),

                    Arguments.of(List.of(INT_1, LONG_2), new VariadicFunctionValue.CoalesceFn(), 1L, false),
                    Arguments.of(List.of(LONG_1, INT_2), new VariadicFunctionValue.CoalesceFn(), 1L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), new VariadicFunctionValue.CoalesceFn(), 1F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), new VariadicFunctionValue.CoalesceFn(), 1F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), new VariadicFunctionValue.CoalesceFn(), 1.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), new VariadicFunctionValue.CoalesceFn(), 1F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), new VariadicFunctionValue.CoalesceFn(), 1F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), new VariadicFunctionValue.CoalesceFn(), 1.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), new VariadicFunctionValue.CoalesceFn(), 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), new VariadicFunctionValue.CoalesceFn(), 1.0, false),
                    Arguments.of(List.of(INT_NULL, LONG_NULL, FLOAT_3, DOUBLE_1), new VariadicFunctionValue.CoalesceFn(), 3.0, false),

                    Arguments.of(List.of(LIST_INT_2, LIST_LONG_1), new VariadicFunctionValue.CoalesceFn(), List.of(3L, 2L, 1L), false),
                    Arguments.of(List.of(LIST_LONG_1, LIST_INT_2), new VariadicFunctionValue.CoalesceFn(), List.of(1L, 2L, 3L), false),
                    Arguments.of(List.of(LIST_INT_2, LIST_FLOAT_1), new VariadicFunctionValue.CoalesceFn(), List.of(3.0f, 2.0f, 1.0f), false),
                    Arguments.of(List.of(LIST_FLOAT_1, LIST_INT_2), new VariadicFunctionValue.CoalesceFn(), List.of(1.0f, 2.0f, 3.0f), false),


                    Arguments.of(List.of(RECORD_1, RECORD_2), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),
                    Arguments.of(List.of(RECORD_1, RECORD_NAMED), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),
                    Arguments.of(List.of(NULL_TYPED, RECORD_1), new VariadicFunctionValue.CoalesceFn(), getMessageForRecord1(), false),

                    Arguments.of(List.of(F, INT_1), new VariadicFunctionValue.CoalesceFn(), 4L, false),
                    Arguments.of(List.of(INT_1, F), new VariadicFunctionValue.CoalesceFn(), 1L, false),

                    Arguments.of(List.of(F, INT_NULL), new VariadicFunctionValue.CoalesceFn(), 4L, false),
                    Arguments.of(List.of(INT_NULL, F), new VariadicFunctionValue.CoalesceFn(), 4L, false),

                    Arguments.of(List.of(INT_1, STRING_1), new VariadicFunctionValue.CoalesceFn(), null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), new VariadicFunctionValue.CoalesceFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), new VariadicFunctionValue.CoalesceFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), new VariadicFunctionValue.CoalesceFn(), null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), new VariadicFunctionValue.CoalesceFn(), null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), new VariadicFunctionValue.CoalesceFn(), null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), new VariadicFunctionValue.CoalesceFn(), null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), new VariadicFunctionValue.CoalesceFn(), null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), new VariadicFunctionValue.CoalesceFn(), null, true)
            );
        }
    }

    @ParameterizedTest
    @SuppressWarnings({"rawtypes", "unchecked", "ConstantConditions"})
    @ArgumentsSource(BinaryPredicateTestProvider.class)
    void testPredicate(List<Value> args, BuiltInFunction function, Object expectedValue, boolean shouldFail) {
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
            Assertions.assertTrue(value instanceof VariadicFunctionValue);
            Object actualValue = ((VariadicFunctionValue)value).eval(null, evaluationContext);
            Assertions.assertEquals(expectedValue, actualValue);
        }
    }
}
