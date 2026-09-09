/*
 * CallSiteOptionsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Tests for the typed call-site options, {@link CallSiteArguments.Option} and {@link CallSiteArguments.Options}.
 */
class CallSiteOptionsTest {

    private static final CallSiteArguments.Option<Integer> INT_OPTION = CallSiteArguments.Option.ofInteger("anInt");
    private static final CallSiteArguments.Option<Long> LONG_OPTION = CallSiteArguments.Option.ofLong("aLong");
    private static final CallSiteArguments.Option<Boolean> BOOLEAN_OPTION =
            CallSiteArguments.Option.ofBoolean("aBoolean");
    private static final CallSiteArguments.Option<Double> DOUBLE_OPTION = CallSiteArguments.Option.ofDouble("aDouble");
    private static final CallSiteArguments.Option<String> STRING_OPTION = CallSiteArguments.Option.ofString("aString");

    /**
     * An option with a bespoke conversion, to exercise {@link CallSiteArguments.Option#of}. It accepts the
     * single-character strings a front end would produce for a {@link Character}-typed option.
     */
    private static final CallSiteArguments.Option<Character> CHAR_OPTION =
            CallSiteArguments.Option.of("aChar", Character.class, (optionName, rawValue) -> {
                if (rawValue instanceof CharSequence && ((CharSequence)rawValue).length() == 1) {
                    return ((CharSequence)rawValue).charAt(0);
                }
                throw SemanticException.newException(SemanticException.ErrorCode.INCOMPATIBLE_TYPE,
                        "not a single character");
            });

    /**
     * An enum to exercise {@link CallSiteArguments.Option#ofEnum}.
     */
    private enum Fruit {
        APPLE,
        ORANGE
    }

    private static final CallSiteArguments.Option<Fruit> ENUM_OPTION =
            CallSiteArguments.Option.ofEnum("aFruit", Fruit.class);

    @Test
    void getReturnsTypedValue() {
        final var options = CallSiteArguments.Options.builder()
                .put(INT_OPTION, 42)
                .put(BOOLEAN_OPTION, true)
                .build();

        Assertions.assertEquals(42, options.get(INT_OPTION).orElseThrow());
        Assertions.assertEquals(true, options.get(BOOLEAN_OPTION).orElseThrow());
        Assertions.assertTrue(options.contains(INT_OPTION));
        Assertions.assertEquals(2, options.size());
    }

    @Test
    void getOnUnsetOptionIsEmpty() {
        final var options = CallSiteArguments.Options.builder().put(INT_OPTION, 42).build();

        Assertions.assertTrue(options.get(DOUBLE_OPTION).isEmpty());
        Assertions.assertFalse(options.contains(DOUBLE_OPTION));
        Assertions.assertEquals(7.5d, options.getOrDefault(DOUBLE_OPTION, 7.5d));
    }

    @Test
    void emptyOptions() {
        Assertions.assertTrue(CallSiteArguments.Options.empty().isEmpty());
        Assertions.assertTrue(CallSiteArguments.Options.builder().build().isEmpty());
        Assertions.assertEquals(0, CallSiteArguments.Options.empty().size());
    }

    @Test
    void rawIntegralValueIsCoercedToDeclaredType() {
        // a front end may produce a Long for an option declared as an Integer
        final var resolved = resolveAgainstAllOptions(CallSiteArguments.Options.builder()
                .putRaw(INT_OPTION.getName(), 100L)
                .build());

        Assertions.assertEquals(100, resolved.get(INT_OPTION).orElseThrow());
        Assertions.assertEquals(Integer.class, resolved.get(INT_OPTION).orElseThrow().getClass());
    }

    @Test
    void rawIntegralValueIsCoercedToDeclaredDouble() {
        final var resolved = resolveAgainstAllOptions(CallSiteArguments.Options.builder()
                .putRaw(DOUBLE_OPTION.getName(), 3)
                .build());

        Assertions.assertEquals(3.0d, resolved.get(DOUBLE_OPTION).orElseThrow());
    }

    @Test
    void outOfRangeIntegralValueIsRejected() {
        final var options = CallSiteArguments.Options.builder()
                .putRaw(INT_OPTION.getName(), 5_000_000_000L)
                .build();

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> resolveAgainstAllOptions(options));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, semanticException.getErrorCode());
        Assertions.assertEquals(INT_OPTION.getName(),
                semanticException.getLogInfo().get(LogMessageKeys.OPTION_NAME.toString()));
        Assertions.assertEquals(5_000_000_000L,
                semanticException.getLogInfo().get(LogMessageKeys.OPTION_VALUE.toString()));
    }

    @Test
    void fractionalValueIsNotRoundedIntoAnIntegralOption() {
        final var options = CallSiteArguments.Options.builder()
                .putRaw(INT_OPTION.getName(), 1.5d)
                .build();

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> resolveAgainstAllOptions(options));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, semanticException.getErrorCode());
    }

    @Test
    void wronglyTypedValueIsRejected() {
        final var options = CallSiteArguments.Options.builder()
                .putRaw(BOOLEAN_OPTION.getName(), "true")
                .build();

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> resolveAgainstAllOptions(options));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, semanticException.getErrorCode());
        Assertions.assertEquals("Boolean",
                semanticException.getLogInfo().get(LogMessageKeys.EXPECTED_TYPE.toString()));
        Assertions.assertEquals("String",
                semanticException.getLogInfo().get(LogMessageKeys.ACTUAL_TYPE.toString()));
    }

    @Test
    void unsupportedOptionNameIsRejected() {
        final var options = CallSiteArguments.Options.builder().putRaw("bogus", 1).build();

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> resolveAgainstAllOptions(options));
        Assertions.assertEquals(SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                semanticException.getErrorCode());
        Assertions.assertEquals("bogus", semanticException.getLogInfo().get(LogMessageKeys.OPTION_NAME.toString()));
        Assertions.assertEquals("aFunction", semanticException.getLogInfo().get(LogMessageKeys.FUNCTION.toString()));
    }

    @Test
    void nullOptionValueIsRejected() {
        final var builder = CallSiteArguments.Options.builder();

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> builder.putRaw(INT_OPTION.getName(), null));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, semanticException.getErrorCode());
    }

    @Test
    void sameOptionSetTwiceIsRejected() {
        final var builder = CallSiteArguments.Options.builder().put(INT_OPTION, 1);

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> builder.put(INT_OPTION, 2));
        Assertions.assertEquals(SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                semanticException.getErrorCode());
        Assertions.assertEquals(INT_OPTION.getName(),
                semanticException.getLogInfo().get(LogMessageKeys.OPTION_NAME.toString()));
    }

    @Test
    void enumOptionAcceptsConstantAndCaseInsensitiveString() {
        Assertions.assertEquals(Fruit.APPLE, ENUM_OPTION.coerce(Fruit.APPLE));
        Assertions.assertEquals(Fruit.ORANGE, ENUM_OPTION.coerce("orange"));
        Assertions.assertEquals(Fruit.ORANGE, ENUM_OPTION.coerce("ORANGE"));

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> ENUM_OPTION.coerce("banana"));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, semanticException.getErrorCode());
    }

    @Test
    void longOptionAcceptsAnyIntegralValue() {
        Assertions.assertEquals(5L, LONG_OPTION.coerce(5L));
        Assertions.assertEquals(5L, LONG_OPTION.coerce(5), "an Integer literal is exactly representable as a Long");
        Assertions.assertEquals(5L, LONG_OPTION.coerce((short)5));
        Assertions.assertEquals(5L, LONG_OPTION.coerce((byte)5));
        Assertions.assertEquals(Long.class, LONG_OPTION.coerce(5).getClass());

        // a Long option has no upper bound to exceed, unlike an Integer one
        final var resolved = resolveAgainstAllOptions(CallSiteArguments.Options.builder()
                .putRaw(LONG_OPTION.getName(), 5_000_000_000L)
                .build());
        Assertions.assertEquals(5_000_000_000L, resolved.get(LONG_OPTION).orElseThrow());
    }

    @Test
    void longOptionRejectsNonIntegralValues() {
        final var fractionalException = Assertions.assertThrows(SemanticException.class,
                () -> LONG_OPTION.coerce(1.5d));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, fractionalException.getErrorCode());
        Assertions.assertEquals("Long",
                fractionalException.getLogInfo().get(LogMessageKeys.EXPECTED_TYPE.toString()));

        Assertions.assertThrows(SemanticException.class, () -> LONG_OPTION.coerce("5"),
                "a numeric string is not an integral value");
    }

    @Test
    void stringOptionAcceptsAnyCharSequence() {
        Assertions.assertEquals("abc", STRING_OPTION.coerce("abc"));
        Assertions.assertEquals("abc", STRING_OPTION.coerce(new StringBuilder("abc")),
                "a CharSequence that is not a String should be converted");
        Assertions.assertEquals(String.class, STRING_OPTION.coerce(new StringBuilder("abc")).getClass());

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> STRING_OPTION.coerce(42));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, semanticException.getErrorCode());
        Assertions.assertEquals("String",
                semanticException.getLogInfo().get(LogMessageKeys.EXPECTED_TYPE.toString()));
        Assertions.assertEquals("Integer",
                semanticException.getLogInfo().get(LogMessageKeys.ACTUAL_TYPE.toString()));
    }

    @Test
    void optionWithBespokeCoercerUsesIt() {
        Assertions.assertEquals('x', CHAR_OPTION.coerce('x'), "a value of the declared type is handed back as-is");
        Assertions.assertEquals('x', CHAR_OPTION.coerce("x"));
        Assertions.assertEquals(Character.class, CHAR_OPTION.getType());

        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> CHAR_OPTION.coerce("xy"));
        Assertions.assertEquals(SemanticException.ErrorCode.INCOMPATIBLE_TYPE, semanticException.getErrorCode());

        // the bespoke conversion is applied on the way out of a resolved option set, just like the built-in ones
        final var resolved = resolveAgainstAllOptions(CallSiteArguments.Options.builder()
                .putRaw(CHAR_OPTION.getName(), "y")
                .build());
        Assertions.assertEquals('y', resolved.get(CHAR_OPTION).orElseThrow());
    }

    @Test
    void coercionIsIdempotent() {
        Assertions.assertEquals(100, INT_OPTION.coerce(INT_OPTION.coerce(100L)));
        Assertions.assertEquals("abc", STRING_OPTION.coerce(STRING_OPTION.coerce("abc")));
    }

    @Test
    void optionEqualityIsOnNameOnly() {
        Assertions.assertEquals(INT_OPTION, CallSiteArguments.Option.ofInteger(INT_OPTION.getName()));
        // a same-named key of a different type is equal: names are unique within one function's declared options
        Assertions.assertEquals(INT_OPTION, CallSiteArguments.Option.ofString(INT_OPTION.getName()));
        Assertions.assertNotEquals(INT_OPTION, DOUBLE_OPTION);
        Assertions.assertEquals(INT_OPTION.getName(), INT_OPTION.toString());
        Assertions.assertEquals(Integer.class, INT_OPTION.getType());
    }

    @Test
    void optionsEqualityIsIndependentOfInsertionOrder() {
        final var options = CallSiteArguments.Options.builder()
                .put(INT_OPTION, 1)
                .put(STRING_OPTION, "a")
                .build();
        final var reversedOptions = CallSiteArguments.Options.builder()
                .put(STRING_OPTION, "a")
                .put(INT_OPTION, 1)
                .build();

        Assertions.assertEquals(options, reversedOptions);
        Assertions.assertEquals(options.hashCode(), reversedOptions.hashCode());
        Assertions.assertEquals("[aString: a, anInt: 1]", options.toString());
        Assertions.assertEquals(Set.of(INT_OPTION.getName(), STRING_OPTION.getName()), options.names());
    }

    @Test
    void callSiteArgumentsWithOptionsRemainEqual() {
        final var value = LiteralValue.ofScalar(1);
        final var arguments = CallSiteArguments.ofPositional(value).withOption(INT_OPTION, 5);
        final var sameArguments = CallSiteArguments.ofPositional(value).withOption(INT_OPTION, 5);

        Assertions.assertEquals(arguments, sameArguments,
                "arguments carrying equal options should be equal");
        Assertions.assertEquals(arguments.hashCode(), sameArguments.hashCode());
        Assertions.assertEquals(5, arguments.getOption(INT_OPTION).orElseThrow());
        Assertions.assertTrue(arguments.hasOptions());
        Assertions.assertFalse(arguments.isSimplePositional(),
                "arguments carrying options are not simple");
    }

    @Test
    void toBuilderPreservesOptions() {
        final var options = CallSiteArguments.Options.builder().put(INT_OPTION, 1).build();

        final var extendedOptions = options.toBuilder().put(STRING_OPTION, "a").build();

        Assertions.assertEquals(1, extendedOptions.get(INT_OPTION).orElseThrow());
        Assertions.assertEquals("a", extendedOptions.get(STRING_OPTION).orElseThrow());
        Assertions.assertEquals(1, options.size(), "the original options should be unchanged");
    }

    /**
     * Resolves options the way {@link CatalogedFunction} does when a call is encapsulated, against a function that
     * declares every option used in this test.
     */
    @Nonnull
    private static CallSiteArguments.Options resolveAgainstAllOptions(
            @Nonnull final CallSiteArguments.Options options) {
        final var function = new CatalogedFunction("aFunction", ImmutableList.of(), null) {
            @Nonnull
            @Override
            public Set<CallSiteArguments.Option<?>> getSupportedOptions() {
                return ImmutableSet.of(INT_OPTION, LONG_OPTION, BOOLEAN_OPTION, DOUBLE_OPTION, STRING_OPTION,
                        ENUM_OPTION, CHAR_OPTION);
            }

            @Nonnull
            @Override
            public Typed encapsulate(@Nonnull final CallSiteArguments arguments) {
                return LiteralValue.ofScalar(1);
            }
        };
        return function.validateAndNormalizeOptions(CallSiteArguments.empty().withOptions(options)).getOptions();
    }
}
