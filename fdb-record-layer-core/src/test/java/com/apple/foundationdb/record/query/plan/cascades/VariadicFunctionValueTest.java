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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests evaluation of {@link VariadicFunctionValue}.
 */
class VariadicFunctionValueTest {
    private static final FieldValue F = FieldValue.ofFieldName(QuantifiedObjectValue.of(CorrelationIdentifier.of("ident"), Type.Record.fromFields(true, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("rec_no"))))), "rec_no");
    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Integer> INT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 2);
    private static final LiteralValue<Integer> INT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 3);
    private static final Value LIST_INT_1 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_1, INT_2, INT_3);
    private static final Value LIST_INT_2 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_3, INT_2, INT_1);
    private static final Value LIST_INT_3 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(INT_2, INT_3, INT_1);
    private static final Value LIST_INT_NULL = new LiteralValue<>(new Type.Array(Type.primitiveType(Type.TypeCode.INT)), null);
    private static final LiteralValue<Integer> INT_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), null);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Long> LONG_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 2L);
    private static final LiteralValue<Long> LONG_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 3L);
    private static final Value LIST_LONG_1 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(LONG_1, LONG_2, LONG_3);
    private static final LiteralValue<Long> LONG_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), null);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Float> FLOAT_2 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 2.0F);
    private static final LiteralValue<Float> FLOAT_3 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 3.0F);
    private static final Value LIST_FLOAT_1 = AbstractArrayConstructorValue.LightArrayConstructorValue.of(FLOAT_1, FLOAT_2, FLOAT_3);
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
    private static final Value RECORD_1 = (Value)new RecordConstructorValue.RecordFn().encapsulate(CallSiteArguments.ofPositional(STRING_1, INT_1, FLOAT_1));
    private static final Value RECORD_2 = (Value)new RecordConstructorValue.RecordFn().encapsulate(CallSiteArguments.ofPositional(STRING_2, INT_2, FLOAT_2));
    private static final Value RECORD_3 = (Value)new RecordConstructorValue.RecordFn().encapsulate(CallSiteArguments.ofPositional(STRING_3, INT_3, FLOAT_3));
    private static final Value NULL_TYPED = new LiteralValue<>(Type.primitiveType(Type.TypeCode.NULL), null);
    private static final LiteralValue<Integer> INT_1_NOT_NULLABLE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT, false), 1);
    private static final LiteralValue<Integer> INT_2_NOT_NULLABLE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT, false), 2);
    private static final LiteralValue<Long> LONG_1_NOT_NULLABLE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG, false), 1L);
    private static final LiteralValue<String> STRING_1_NOT_NULLABLE = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING, false), "a");
    private static final Value RECORD_NAMED = RecordConstructorValue.ofColumns(ImmutableList.of(
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

    private static final Value RECORD_NULL = new LiteralValue<>(recordTypeUnnamed, null);

    // NULL literals of the very same record and array type as the constants above, but nullable. These exercise
    // COALESCE() on non-primitive types, where the common type is determined structurally.
    private static final Value RECORD_1_NULLABLE = new LiteralValue<>(RECORD_1.getResultType().nullable(), null);
    private static final Value LIST_INT_1_NULLABLE = new LiteralValue<>(LIST_INT_1.getResultType().nullable(), null);

    // Pre-constructed instances of the built-in functions under test. (Since they are stateless, they can be shared by
    // all test cases.)
    private static final VariadicFunctionValue.GreatestFn GREATEST_FN = new VariadicFunctionValue.GreatestFn();
    private static final VariadicFunctionValue.LeastFn LEAST_FN = new VariadicFunctionValue.LeastFn();
    private static final VariadicFunctionValue.CoalesceFn COALESCE_FN = new VariadicFunctionValue.CoalesceFn();

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
                    Arguments.of(List.of(INT_1, INT_1), GREATEST_FN, 1, false),
                    Arguments.of(List.of(LONG_1, LONG_1), GREATEST_FN, 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_1), GREATEST_FN, 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_1), GREATEST_FN, 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_1), GREATEST_FN, "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_1), GREATEST_FN, false, false),

                    Arguments.of(List.of(INT_1, INT_2), GREATEST_FN, 2, false),
                    Arguments.of(List.of(LONG_1, LONG_2), GREATEST_FN, 2L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2), GREATEST_FN, 2.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2), GREATEST_FN, 2.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2), GREATEST_FN, "b", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2), GREATEST_FN, true, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), GREATEST_FN, 3, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), GREATEST_FN, 3L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), GREATEST_FN, 3.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), GREATEST_FN, 3.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), GREATEST_FN, "c", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), GREATEST_FN, true, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), GREATEST_FN, null, false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), GREATEST_FN, null, false),

                    Arguments.of(List.of(INT_1, LONG_2), GREATEST_FN, 2L, false),
                    Arguments.of(List.of(LONG_1, INT_2), GREATEST_FN, 2L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), GREATEST_FN, 2F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), GREATEST_FN, 2F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), GREATEST_FN, 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), GREATEST_FN, 2.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), GREATEST_FN, 2F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), GREATEST_FN, 2F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), GREATEST_FN, 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), GREATEST_FN, 2.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), GREATEST_FN, 2.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), GREATEST_FN, 2.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), GREATEST_FN, 3.0, false),

                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), GREATEST_FN, null, false),

                    Arguments.of(List.of(F, INT_1), GREATEST_FN, 4L, false),
                    Arguments.of(List.of(INT_1, F), GREATEST_FN, 4L, false),

                    Arguments.of(List.of(F, INT_NULL), GREATEST_FN, null, false),
                    Arguments.of(List.of(INT_NULL, F), GREATEST_FN, null, false),

                    Arguments.of(List.of(INT_1, STRING_1), GREATEST_FN, null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), GREATEST_FN, null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), GREATEST_FN, null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), GREATEST_FN, null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), GREATEST_FN, null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), GREATEST_FN, null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), GREATEST_FN, null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), GREATEST_FN, null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), GREATEST_FN, null, true),

                    // Least Function
                    Arguments.of(List.of(INT_3, INT_3), LEAST_FN, 3, false),
                    Arguments.of(List.of(LONG_3, LONG_3), LEAST_FN, 3L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_3), LEAST_FN, 3.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_3), LEAST_FN, 3.0, false),
                    Arguments.of(List.of(STRING_3, STRING_3), LEAST_FN, "c", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_2), LEAST_FN, true, false),

                    Arguments.of(List.of(INT_3, INT_2), LEAST_FN, 2, false),
                    Arguments.of(List.of(LONG_3, LONG_2), LEAST_FN, 2L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_2), LEAST_FN, 2.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_2), LEAST_FN, 2.0, false),
                    Arguments.of(List.of(STRING_3, STRING_2), LEAST_FN, "b", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_1), LEAST_FN, false, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), LEAST_FN, 1, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), LEAST_FN, 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), LEAST_FN, 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), LEAST_FN, 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), LEAST_FN, "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), LEAST_FN, false, false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), LEAST_FN, null, false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), LEAST_FN, null, false),

                    Arguments.of(List.of(INT_1, LONG_2), LEAST_FN, 1L, false),
                    Arguments.of(List.of(LONG_1, INT_2), LEAST_FN, 1L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), LEAST_FN, 1F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), LEAST_FN, 1F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), LEAST_FN, 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), LEAST_FN, 1.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), LEAST_FN, 1F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), LEAST_FN, 1F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), LEAST_FN, 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), LEAST_FN, 1.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), LEAST_FN, 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), LEAST_FN, 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), LEAST_FN, 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), LEAST_FN, null, false),

                    Arguments.of(List.of(F, INT_1), LEAST_FN, 1L, false),
                    Arguments.of(List.of(INT_1, F), LEAST_FN, 1L, false),

                    Arguments.of(List.of(F, INT_NULL), LEAST_FN, null, false),
                    Arguments.of(List.of(INT_NULL, F), LEAST_FN, null, false),

                    Arguments.of(List.of(INT_1, STRING_1), LEAST_FN, null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), LEAST_FN, null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), LEAST_FN, null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), LEAST_FN, null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), LEAST_FN, null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), LEAST_FN, null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), LEAST_FN, null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), LEAST_FN, null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), LEAST_FN, null, true),

                    // Coalesce
                    Arguments.of(List.of(INT_3, INT_3), COALESCE_FN, 3, false),
                    Arguments.of(List.of(LONG_3, LONG_3), COALESCE_FN, 3L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_3), COALESCE_FN, 3.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_3), COALESCE_FN, 3.0, false),
                    Arguments.of(List.of(STRING_3, STRING_3), COALESCE_FN, "c", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_2), COALESCE_FN, true, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_1), COALESCE_FN, List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_1), COALESCE_FN, getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_3, INT_2), COALESCE_FN, 3, false),
                    Arguments.of(List.of(LONG_3, LONG_2), COALESCE_FN, 3L, false),
                    Arguments.of(List.of(FLOAT_3, FLOAT_2), COALESCE_FN, 3.0F, false),
                    Arguments.of(List.of(DOUBLE_3, DOUBLE_2), COALESCE_FN, 3.0, false),
                    Arguments.of(List.of(STRING_3, STRING_2), COALESCE_FN, "c", false),
                    Arguments.of(List.of(BOOLEAN_2, BOOLEAN_1), COALESCE_FN, true, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_2), COALESCE_FN, List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_2), COALESCE_FN, getMessageForRecord1(), false),
                    Arguments.of(List.of(RECORD_1, RECORD_NAMED), COALESCE_FN, getMessageForRecord1(), false),
                    Arguments.of(List.of(RECORD_NAMED, RECORD_1), COALESCE_FN, getMessageForRecordNamed(), false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3), COALESCE_FN, 1, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3), COALESCE_FN, 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3), COALESCE_FN, 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3), COALESCE_FN, "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1), COALESCE_FN, false, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_2, LIST_INT_3), COALESCE_FN, List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_2, RECORD_3), COALESCE_FN, getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_1, INT_2, INT_3, INT_NULL), COALESCE_FN, 1, false),
                    Arguments.of(List.of(LONG_1, LONG_2, LONG_3, LONG_NULL), COALESCE_FN, 1L, false),
                    Arguments.of(List.of(FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), COALESCE_FN, 1.0F, false),
                    Arguments.of(List.of(DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(STRING_1, STRING_2, STRING_3, STRING_NULL), COALESCE_FN, "a", false),
                    Arguments.of(List.of(BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), COALESCE_FN, false, false),
                    Arguments.of(List.of(LIST_INT_1, LIST_INT_2, LIST_INT_3, LIST_INT_NULL), COALESCE_FN, List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_1, RECORD_2, RECORD_3, RECORD_NULL), COALESCE_FN, getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_NULL, INT_1, INT_2, INT_3, INT_NULL), COALESCE_FN, 1, false),
                    Arguments.of(List.of(LONG_NULL, LONG_1, LONG_2, LONG_3, LONG_NULL), COALESCE_FN, 1L, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_1, FLOAT_2, FLOAT_3, FLOAT_NULL), COALESCE_FN, 1.0F, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_1, DOUBLE_2, DOUBLE_3, DOUBLE_NULL), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(STRING_NULL, STRING_1, STRING_2, STRING_3, STRING_NULL), COALESCE_FN, "a", false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_1, BOOLEAN_2, BOOLEAN_1, BOOLEAN_NULL), COALESCE_FN, false, false),
                    Arguments.of(List.of(LIST_INT_NULL, LIST_INT_1, LIST_INT_2, LIST_INT_3, LIST_INT_NULL), COALESCE_FN, List.of(1, 2, 3), false),
                    Arguments.of(List.of(RECORD_NULL, RECORD_1, RECORD_2, RECORD_3, RECORD_NULL), COALESCE_FN, getMessageForRecord1(), false),

                    Arguments.of(List.of(INT_NULL, INT_NULL), COALESCE_FN, null, false),
                    Arguments.of(List.of(LONG_NULL, LONG_NULL), COALESCE_FN, null, false),
                    Arguments.of(List.of(FLOAT_NULL, FLOAT_NULL), COALESCE_FN, null, false),
                    Arguments.of(List.of(DOUBLE_NULL, DOUBLE_NULL), COALESCE_FN, null, false),
                    Arguments.of(List.of(STRING_NULL, STRING_NULL), COALESCE_FN, null, false),
                    Arguments.of(List.of(BOOLEAN_NULL, BOOLEAN_NULL), COALESCE_FN, null, false),
                    Arguments.of(List.of(LIST_INT_NULL, LIST_INT_NULL), COALESCE_FN, null, false),
                    Arguments.of(List.of(RECORD_NULL, RECORD_NULL), COALESCE_FN, null, false),

                    Arguments.of(List.of(INT_1, LONG_2), COALESCE_FN, 1L, false),
                    Arguments.of(List.of(LONG_1, INT_2), COALESCE_FN, 1L, false),
                    Arguments.of(List.of(INT_1, FLOAT_2), COALESCE_FN, 1F, false),
                    Arguments.of(List.of(FLOAT_1, INT_2), COALESCE_FN, 1F, false),
                    Arguments.of(List.of(INT_1, DOUBLE_2), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, INT_2), COALESCE_FN, 1.0, false),

                    Arguments.of(List.of(LONG_1, FLOAT_2), COALESCE_FN, 1F, false),
                    Arguments.of(List.of(FLOAT_1, LONG_2), COALESCE_FN, 1F, false),
                    Arguments.of(List.of(LONG_1, DOUBLE_2), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, LONG_2), COALESCE_FN, 1.0, false),

                    Arguments.of(List.of(FLOAT_1, DOUBLE_2), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(DOUBLE_1, FLOAT_2), COALESCE_FN, 1.0, false),

                    Arguments.of(List.of(INT_1, LONG_2, FLOAT_3, DOUBLE_1), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(INT_1, LONG_NULL, FLOAT_3, DOUBLE_1), COALESCE_FN, 1.0, false),
                    Arguments.of(List.of(INT_NULL, LONG_NULL, FLOAT_3, DOUBLE_1), COALESCE_FN, 3.0, false),

                    Arguments.of(List.of(LIST_INT_2, LIST_LONG_1), COALESCE_FN, List.of(3L, 2L, 1L), false),
                    Arguments.of(List.of(LIST_LONG_1, LIST_INT_2), COALESCE_FN, List.of(1L, 2L, 3L), false),
                    Arguments.of(List.of(LIST_INT_2, LIST_FLOAT_1), COALESCE_FN, List.of(3.0f, 2.0f, 1.0f), false),
                    Arguments.of(List.of(LIST_FLOAT_1, LIST_INT_2), COALESCE_FN, List.of(1.0f, 2.0f, 3.0f), false),


                    Arguments.of(List.of(RECORD_1, RECORD_2), COALESCE_FN, getMessageForRecord1(), false),
                    Arguments.of(List.of(RECORD_1, RECORD_NAMED), COALESCE_FN, getMessageForRecord1(), false),
                    Arguments.of(List.of(NULL_TYPED, RECORD_1), COALESCE_FN, getMessageForRecord1(), false),

                    Arguments.of(List.of(F, INT_1), COALESCE_FN, 4L, false),
                    Arguments.of(List.of(INT_1, F), COALESCE_FN, 1L, false),

                    Arguments.of(List.of(F, INT_NULL), COALESCE_FN, 4L, false),
                    Arguments.of(List.of(INT_NULL, F), COALESCE_FN, 4L, false),

                    Arguments.of(List.of(INT_1, STRING_1), COALESCE_FN, null, true),
                    Arguments.of(List.of(LONG_1, STRING_1), COALESCE_FN, null, true),
                    Arguments.of(List.of(FLOAT_1, STRING_1), COALESCE_FN, null, true),
                    Arguments.of(List.of(DOUBLE_1, STRING_1), COALESCE_FN, null, true),
                    Arguments.of(List.of(BOOLEAN_1, STRING_1), COALESCE_FN, null, true),

                    Arguments.of(List.of(INT_1, BOOLEAN_1), COALESCE_FN, null, true),
                    Arguments.of(List.of(LONG_1, BOOLEAN_1), COALESCE_FN, null, true),
                    Arguments.of(List.of(FLOAT_1, BOOLEAN_1), COALESCE_FN, null, true),
                    Arguments.of(List.of(DOUBLE_1, BOOLEAN_1), COALESCE_FN, null, true)
            );
        }
    }

    /**
     * Verifies that a comparison function evaluates to the expected value, or is rejected as expected.
     */
    @ParameterizedTest
    @ArgumentsSource(BinaryPredicateTestProvider.class)
    void testEval(List<Value> args, BuiltInFunction<Value> function, Object expectedValue, boolean shouldFail) {
        if (shouldFail) {
            assertThatThrownBy(() -> function.encapsulate(CallSiteArguments.ofPositional(args)))
                    .isInstanceOf(SemanticException.class)
                    .extracting(thrown -> ((SemanticException)thrown).getErrorCode())
                    .isEqualTo(SemanticException.ErrorCode.INCOMPATIBLE_TYPE);
        } else {
            final Typed value = function.encapsulate(CallSiteArguments.ofPositional(args));
            assertThat(value).isInstanceOf(VariadicFunctionValue.class);
            assertThat(((VariadicFunctionValue)value).eval(null, evaluationContext)).isEqualTo(expectedValue);
        }
    }

    /**
     * Verifies that a comparison function applied to only {@code NULL} arguments is rejected, as no physical operator
     * is defined for the {@code NULL} type.
     */
    @Test
    void testAllArgumentsNull() {
        for (final BuiltInFunction<Value> function : List.of(GREATEST_FN, LEAST_FN, COALESCE_FN)) {
            assertThatThrownBy(() -> function.encapsulate(CallSiteArguments.ofPositional(List.of(NULL_TYPED, NULL_TYPED))))
                    .as("%s applied to only NULL arguments", function.getFunctionName())
                    .isInstanceOf(SemanticException.class)
                    .extracting(thrown -> ((SemanticException)thrown).getErrorCode())
                    .isEqualTo(SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
        }
    }

    /**
     * A test case for {@link #testResultType}. Bundles the arguments the function-under-test is applied to together
     * with the expected type code and the nullability of the result.
     */
    record ResultTypeTestCase(BuiltInFunction<Value> function, List<? extends Value> arguments,
                              Type.TypeCode expectedTypeCode, boolean expectedIsNullable) {
        @Override
        public String toString() {
            // Render each argument with its type code, marking a nullable type with a trailing `?`.
            // (Note that `Type.toString()` does not indicate nullability, which is what matters in some tests.)
            final String renderedArguments = arguments.stream()
                    .map(argument -> {
                        final Type argumentType = argument.getResultType();
                        return argument + ":" + argumentType.getTypeCode() + (argumentType.isNullable() ? "?" : "");
                    })
                    .collect(Collectors.joining(", ", "(", ")"));
            return function.getFunctionName() + renderedArguments + " -> " + expectedTypeCode
                    + (expectedIsNullable ? "?" : "");
        }
    }

    static class ResultTypeTestProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    // GREATEST() and LEAST() are nullable if any of their arguments is nullable.
                    new ResultTypeTestCase(GREATEST_FN, List.of(INT_1, INT_2), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(GREATEST_FN, List.of(INT_1_NOT_NULLABLE, INT_1), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(GREATEST_FN, List.of(INT_1, INT_1_NOT_NULLABLE), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(GREATEST_FN, List.of(INT_1_NOT_NULLABLE, F), Type.TypeCode.LONG, true),
                    new ResultTypeTestCase(GREATEST_FN, List.of(INT_1_NOT_NULLABLE, NULL_TYPED), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(GREATEST_FN, List.of(INT_1_NOT_NULLABLE, LONG_1_NOT_NULLABLE), Type.TypeCode.LONG, false),
                    new ResultTypeTestCase(LEAST_FN, List.of(STRING_1_NOT_NULLABLE, STRING_2), Type.TypeCode.STRING, true),
                    new ResultTypeTestCase(LEAST_FN, List.of(INT_1, INT_1_NOT_NULLABLE), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(LEAST_FN, List.of(INT_1_NOT_NULLABLE, NULL_TYPED), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(LEAST_FN, List.of(INT_1_NOT_NULLABLE, LONG_1_NOT_NULLABLE, INT_2_NOT_NULLABLE), Type.TypeCode.LONG, false),

                    // COALESCE() returns its first non-NULL argument, hence its result is nullable only if all of its arguments are nullable.
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1, INT_2), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(COALESCE_FN, List.of(F, INT_1), Type.TypeCode.LONG, true),
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1, NULL_TYPED), Type.TypeCode.INT, true),
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1_NOT_NULLABLE, INT_2), Type.TypeCode.INT, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1, INT_2_NOT_NULLABLE), Type.TypeCode.INT, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(F, INT_1_NOT_NULLABLE), Type.TypeCode.LONG, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1_NOT_NULLABLE, F), Type.TypeCode.LONG, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(NULL_TYPED, INT_1_NOT_NULLABLE), Type.TypeCode.INT, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1_NOT_NULLABLE, NULL_TYPED), Type.TypeCode.INT, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1, F, LONG_1_NOT_NULLABLE), Type.TypeCode.LONG, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(INT_1, F, LONG_1), Type.TypeCode.LONG, true),

                    // COALESCE() over non-primitive types, where the common type is determined structurally.
                    new ResultTypeTestCase(COALESCE_FN, List.of(RECORD_1, RECORD_2), Type.TypeCode.RECORD, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(RECORD_1_NULLABLE, RECORD_1), Type.TypeCode.RECORD, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(RECORD_1, RECORD_1_NULLABLE), Type.TypeCode.RECORD, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(RECORD_1_NULLABLE, RECORD_1_NULLABLE), Type.TypeCode.RECORD, true),
                    new ResultTypeTestCase(COALESCE_FN, List.of(LIST_INT_1, LIST_INT_2), Type.TypeCode.ARRAY, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(LIST_INT_1_NULLABLE, LIST_INT_1), Type.TypeCode.ARRAY, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(LIST_INT_1, LIST_INT_1_NULLABLE), Type.TypeCode.ARRAY, false),
                    new ResultTypeTestCase(COALESCE_FN, List.of(LIST_INT_1_NULLABLE, LIST_INT_1_NULLABLE), Type.TypeCode.ARRAY, true),

                    // GREATEST() and LEAST() are not defined for non-primitive types, but COALESCE() is.
                    new ResultTypeTestCase(COALESCE_FN, List.of(RECORD_1, RECORD_2, RECORD_3), Type.TypeCode.RECORD, false)
            ).map(Arguments::of);
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ResultTypeTestProvider.class)
    void testResultType(ResultTypeTestCase testCase) {
        final List<? extends Value> arguments = testCase.arguments();
        final boolean expectedIsNullable = testCase.expectedIsNullable();

        final Typed value = testCase.function().encapsulate(CallSiteArguments.ofPositional(arguments));
        assertThat(value).isInstanceOf(VariadicFunctionValue.class);

        // Check the derived result type.
        final Type resultType = value.getResultType();
        assertThat(resultType.getTypeCode()).isEqualTo(testCase.expectedTypeCode());
        assertThat(resultType.isNullable()).isEqualTo(expectedIsNullable);

        // Check that, while every argument is promoted to the common result type, a non-nullable argument is widened to
        // a nullable one only if the result type is nullable anyway.
        final List<? extends Value> children = ImmutableList.copyOf(((VariadicFunctionValue)value).getChildren());
        for (int i = 0; i < arguments.size(); i++) {
            final boolean argumentIsNullable = arguments.get(i).getResultType().isNullable();
            final boolean expectedChildIsNullable = argumentIsNullable || expectedIsNullable;
            assertThat(children.get(i).getResultType().isNullable())
                    .as("nullability of promoted argument %d", i)
                    .isEqualTo(expectedChildIsNullable);
        }

        // Check that re-deriving the result type from the promoted children, as `withChildren()` and `fromProto()` do,
        // yields the same type and does not trip the consistency check in `computeResultType()`.
        assertThat(((VariadicFunctionValue)value).withChildren(children).getResultType()).isEqualTo(resultType);
    }
}
