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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.planprotos.PLikeOperatorValue;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LikeOperatorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PatternForLikeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Tests evaluation of {@link LikeOperatorValue}.
 */
class LikeOperatorValueTest {
    private static final FieldValue F = FieldValue.ofFieldName(QuantifiedObjectValue.of(CorrelationIdentifier.of("ident"), Type.Record.fromFields(true, ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("rec_no"))))), "rec_no");
    private static final LiteralValue<Integer> INT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT), 1);
    private static final LiteralValue<Long> LONG_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG), 1L);
    private static final LiteralValue<Float> FLOAT_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.FLOAT), 1.0F);
    private static final LiteralValue<Double> DOUBLE_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.DOUBLE), 1.0);
    private static final LiteralValue<Boolean> BOOLEAN_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.BOOLEAN), false);
    private static final LiteralValue<String> STRING_1 = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), "a");
    private static final LiteralValue<String> STRING_NULL = new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), null);

    @SuppressWarnings({"ConstantConditions"})
    private static final Bindings bindings = Bindings.newBuilder()
            .set(Bindings.Internal.CORRELATION.bindingName("ident"), QueryResult.ofComputed(TestRecords7Proto.MyRecord1.newBuilder().setRecNo(4L).build()))
            .build();

    static class InvalidInputArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(INT_1, INT_1, STRING_NULL),
                    Arguments.of(LONG_1, LONG_1, STRING_NULL),
                    Arguments.of(FLOAT_1, FLOAT_1, STRING_NULL),
                    Arguments.of(DOUBLE_1, DOUBLE_1, STRING_NULL),
                    Arguments.of(BOOLEAN_1, BOOLEAN_1, STRING_NULL),

                    Arguments.of(STRING_1, INT_1, STRING_NULL),
                    Arguments.of(STRING_1, LONG_1, STRING_NULL),
                    Arguments.of(STRING_1, FLOAT_1, STRING_NULL),
                    Arguments.of(STRING_1, DOUBLE_1, STRING_NULL),
                    Arguments.of(STRING_1, BOOLEAN_1, STRING_NULL),

                    Arguments.of(INT_1, STRING_1, STRING_NULL),
                    Arguments.of(LONG_1, STRING_1, STRING_NULL),
                    Arguments.of(FLOAT_1, STRING_1, STRING_NULL),
                    Arguments.of(DOUBLE_1, STRING_1, STRING_NULL),
                    Arguments.of(BOOLEAN_1, STRING_1, STRING_NULL)
            );
        }
    }

    static class ValidInputArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(null, null, null, null),
                    Arguments.of("a", null, null, null),
                    Arguments.of(null, "a", null, null),
                    Arguments.of("a", "a", null, true),
                    Arguments.of("aa", "a", null, false),
                    Arguments.of("a", "ab", null, false),
                    Arguments.of("aaaaaaaaaa", "aa%", null, true),
                    Arguments.of("aaaaaaaaaa", "%aa", null, true),
                    Arguments.of("aaaabaaaaa", "%aab", null, false),
                    Arguments.of("aababa", "aa%ba", null, true),
                    Arguments.of("bonjour", "%", null, true),
                    Arguments.of("bonjour", "%%", null, true),
                    Arguments.of("b", "%%", null, true),
                    Arguments.of("b", "_", null, true),
                    Arguments.of("bonjour", "_______", null, true),
                    Arguments.of("bonjour", "______", null, false),
                    Arguments.of("bonjour", "b_nj__r", null, true),
                    Arguments.of("bonjour", "b_nj%", null, true),
                    Arguments.of("bonjour", "_nj%", null, false),
                    Arguments.of("bonjour", "Bonjour", null, false),
                    Arguments.of("bonjour", "B%", null, false),
                    Arguments.of("Bonjour", "bonjour", null, false),
                    Arguments.of("école", "école", null, true),
                    Arguments.of("école", "è%", null, false),
                    Arguments.of("école", "_cole", null, true),
                    Arguments.of("école", "%cole", null, true),
                    Arguments.of("ありがとう", "_____", null, true),
                    Arguments.of("ありがとう", "あ_が_う", null, true),
                    Arguments.of("学校 ", "___", null, true),
                    Arguments.of("学校 ", "_校_", null, true),
                    Arguments.of("学校 ", "学%", null, true),
                    Arguments.of("学生", "大学%", null, false),
                    Arguments.of("مدرسة", "مدرسة", null, true),
                    Arguments.of("مدرسةa", "مدرسة_", null, true),
                    Arguments.of("مدرسةa", "مدرس__", null, true),
                    Arguments.of("مدرسة", "م%", null, true),
                    Arguments.of("(abc)", "(abc)", null, true),
                    Arguments.of("(abc)", "(%)", null, true),
                    Arguments.of("(abc)", "(___)", null, true),
                    Arguments.of("[abc]", "[abc]", null, true),
                    Arguments.of("[abc]", "[%]", null, true),
                    Arguments.of("[abc]", "[___]", null, true),
                    Arguments.of("{abc}", "{abc}", null, true),
                    Arguments.of("{abc}", "{%}", null, true),
                    Arguments.of("a", ".", null, false),
                    Arguments.of(".", ".", null, true),
                    Arguments.of(".a", ".%", null, true),
                    Arguments.of(",a", ".%", null, false),
                    Arguments.of("", "^$", null, false),
                    Arguments.of("^$", "^_", null, true),
                    Arguments.of("^$", "^$", null, true),
                    Arguments.of("^$", "%$", null, true),
                    Arguments.of("^$", "_$", null, true),
                    Arguments.of("a\\b\\c", "_\\_\\_", null, true),
                    Arguments.of("abbbde", "ab*de", null, false),
                    Arguments.of("ab*de", "ab*de", null, true),
                    Arguments.of("ab*dee", "ab*de", null, false),
                    Arguments.of("a", "a+", null, false),
                    Arguments.of("aa", "a+", null, false),
                    Arguments.of("a+", "a+", null, true),
                    Arguments.of("a", "a?", null, false),
                    Arguments.of("", "a?", null, false),
                    Arguments.of("a?", "a?", null, true),
                    Arguments.of("__text", "__%", null, true),
                    Arguments.of("abtext", "__%", null, true),
                    Arguments.of("abtext", "\\_\\_%", "\\", false),
                    Arguments.of("__text", "\\_\\_%", "\\", true),
                    Arguments.of("__", "\\_\\_%", "\\", true),
                    Arguments.of("abtext", "!_!_%", "!", false),
                    Arguments.of("__text", "!_!_%", "!", true),
                    Arguments.of("__", "!_!_%", "!", true),
                    Arguments.of("abtext", "|_|_%", "|", false),
                    Arguments.of("__text", "|_|_%", "|", true),
                    Arguments.of("__", "|_|_%", "|", true),
                    Arguments.of("\\\\|||", "_____", null, true),
                    // Text containing '%' — wildcard in pattern must not match '%' in text as a literal
                    Arguments.of("%%abcdef", "%abc%", null, true),
                    Arguments.of("%hello%", "%hello%", null, true),
                    Arguments.of("100%", "100%", null, true),
                    Arguments.of("100%", "100\\%", "\\", true),
                    Arguments.of("100%off", "100\\%", "\\", false),
                    Arguments.of("50%", "100%", null, false),
                    // Does not match if it ends with an escape char
                    Arguments.of("abc", "abc\\", "\\", false),
                    Arguments.of("abc", "abc", "c", false),
                    Arguments.of("abc", "ab%", "%", false),
                    // Escape always does a literal match against the next character
                    Arguments.of("\\abc", "\\%", null, true),
                    Arguments.of("%", "\\%", null, false),
                    Arguments.of("\\abc", "\\%", "\\", false),
                    Arguments.of("%", "\\%", "\\", true),
                    Arguments.of("%", "%%%", "\\", true),
                    Arguments.of("abcdef", "abc", "c", false),
                    Arguments.of("abc", "ab%%", null, true),
                    Arguments.of("abcdefgh", "ab%%", null, true),
                    Arguments.of("ab%", "ab%%", null, true),
                    Arguments.of("a%", "ab%%", null, false),
                    Arguments.of("ab%cdef", "ab%%", null, true),
                    Arguments.of("a%cdef", "ab%%", null, false),
                    Arguments.of("abc", "ab%%", "b", false),
                    Arguments.of("abcdefgh", "ab%%", "b", false),
                    Arguments.of("ab%", "ab%%", "b", false),
                    Arguments.of("a%", "ab%%", "b", true),
                    Arguments.of("ab%cdef", "ab%%", "b", false),
                    Arguments.of("a%cdef", "ab%%", "b", true),
                    Arguments.of("abcdef", "abcdef", null, true),
                    Arguments.of("abcdef", "abcdef", "b", false),
                    Arguments.of("acdef", "abcdef", null, false),
                    Arguments.of("acdef", "abcdef", "b", true),
                    // Special cases for the % escape character
                    Arguments.of("abc", "%%", null, true),
                    Arguments.of("%", "%%", null, true),
                    Arguments.of("abc", "%%", "%", false),
                    Arguments.of("%", "%%", "%", true),
                    Arguments.of("%", "%%%", null, true),
                    Arguments.of("%", "%%%", "%", false),
                    Arguments.of("abcdef", "abcdef%%", null, true),
                    Arguments.of("abcdef", "abcdef%%", "%", false),
                    Arguments.of("abcdef%", "abcdef%%", null, true),
                    Arguments.of("abcdef%", "abcdef%%", "%", true),
                    Arguments.of("abcdef%%", "abcdef%%", null, true),
                    Arguments.of("abcdef%%", "abcdef%%", "%", false),
                    Arguments.of("abcdef%%", "abcdef%%%%", null, true),
                    Arguments.of("abcdef%%", "abcdef%%%%", "%", true),
                    Arguments.of("abfg", "ab%%fg", null, true),
                    Arguments.of("abfg", "ab%%fg", "%", false),
                    Arguments.of("abcefg", "ab%%fg", null, true),
                    Arguments.of("abcefg", "ab%%fg", "%", false),
                    Arguments.of("abcdefg", "ab%%fg", null, true),
                    Arguments.of("abcdefg", "ab%%fg", "%", false),
                    Arguments.of("ab%fg", "ab%%fg", null, true),
                    Arguments.of("ab%fg", "ab%%fg", "%", true),
                    Arguments.of("ab%%fg", "ab%%fg", null, true),
                    Arguments.of("ab%%fg", "ab%%fg", "%", false),
                    // Special cases for the _ escape character
                    Arguments.of("_", "__", null, false),
                    Arguments.of("_", "__", "_", true),
                    Arguments.of("xy", "__", null, true),
                    Arguments.of("xy", "__", "_", false),
                    Arguments.of("", "%_%%", null, false),
                    Arguments.of("", "%_%%", "_", false),
                    Arguments.of("_", "%_%%", null, true),
                    Arguments.of("_", "%_%%", "_", false),
                    Arguments.of("%", "%_%%", null, true),
                    Arguments.of("%", "%_%%", "_", true),
                    Arguments.of("ab_cdef", "%_%%", null, true),
                    Arguments.of("ab_cdef", "%_%%", "_", false),
                    Arguments.of("ab%cdef", "%_%%", null, true),
                    Arguments.of("ab%cdef", "%_%%", "_", true),
                    Arguments.of("abcdef", "%_%%", null, true),
                    Arguments.of("abcdef", "%_%%", "_", false),
                    Arguments.of("aa", "_aa%", null, false),
                    Arguments.of("aa", "_aa%", "_", true),
                    Arguments.of("baah", "_aa%", null, true),
                    Arguments.of("baah", "_aa%", "_", false)
            );
        }
    }

    @ParameterizedTest
    @SuppressWarnings({"rawtypes", "unchecked"})
    @ArgumentsSource(InvalidInputArgumentsProvider.class)
    void testSemanticException(Value lhs, Value rhs, Value escapeChar) {
        BuiltInFunction like = new LikeOperatorValue.LikeFn();
        BuiltInFunction pattern = new PatternForLikeValue.PatternForLikeFn();
        try {
            like.encapsulate(CallSiteArguments.ofPositional(Arrays.asList(
                    lhs,
                    (Value) pattern.encapsulate(CallSiteArguments.ofPositional(Arrays.asList(rhs, escapeChar))))));
            Assertions.fail("expected an exception to be thrown");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof SemanticException);
            Assertions.assertEquals(((SemanticException)e).getErrorCode(), SemanticException.ErrorCode.OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING);
        }
    }

    @Nullable
    private Object evalLikeOperator(@Nonnull LikeOperatorValue value) {
        final TypeRepository typeRepository = TypeRepository.newBuilder().addAllTypes(value.getDynamicTypes()).build();
        return value.eval(null, EvaluationContext.forBindingsAndTypeRepository(bindings, typeRepository));
    }

    @ParameterizedTest
    @SuppressWarnings({"ConstantConditions"})
    @ArgumentsSource(ValidInputArgumentsProvider.class)
    void testLike(String lhs, String rhs, final String escapeChar, Boolean result) {
        final LikeOperatorValue value = createLikeOperatorValue(lhs, rhs, escapeChar);
        Assertions.assertEquals(result, evalLikeOperator(value));
    }

    @ParameterizedTest
    @SuppressWarnings({"ConstantConditions"})
    @ArgumentsSource(ValidInputArgumentsProvider.class)
    void testLikeSerialization(String lhs, String rhs, final String escapeChar, Boolean result) {
        final LikeOperatorValue value = createLikeOperatorValue(lhs, rhs, escapeChar);
        final PLikeOperatorValue proto = value.toProto(
                new PlanSerializationContext(new DefaultPlanSerializationRegistry(),
                        PlanHashable.CURRENT_FOR_CONTINUATION));
        final LikeOperatorValue deserialized = LikeOperatorValue.fromProto(new PlanSerializationContext(new DefaultPlanSerializationRegistry(),
                PlanHashable.CURRENT_FOR_CONTINUATION), proto);
        Assertions.assertEquals(result, evalLikeOperator(deserialized));
    }


    @SuppressWarnings({"rawtypes", "unchecked", "ConstantConditions"})
    @Nonnull
    private static LikeOperatorValue createLikeOperatorValue(final String lhs, final String rhs, final String escapeChar) {
        BuiltInFunction like = new LikeOperatorValue.LikeFn();
        BuiltInFunction pattern = new PatternForLikeValue.PatternForLikeFn();
        Typed value = like.encapsulate(CallSiteArguments.ofPositional(Arrays.asList(
                new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), lhs),
                (Value) pattern.encapsulate(CallSiteArguments.ofPositional(Arrays.asList(
                        new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), rhs),
                        new LiteralValue<>(Type.primitiveType(Type.TypeCode.STRING), escapeChar)))))));
        Assertions.assertInstanceOf(LikeOperatorValue.class, value);
        return (LikeOperatorValue) value;
    }
}
