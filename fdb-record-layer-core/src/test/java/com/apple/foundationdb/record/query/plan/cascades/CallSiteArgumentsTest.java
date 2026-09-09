/*
 * CallSiteArgumentsTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Tests for {@link CallSiteArguments}, its two calling conventions, and the components a call site carries alongside
 * its arguments: the {@link CallSiteArguments.WindowSpecification} and, through it, the {@link WindowOrderingPart}s of
 * an {@code ORDER BY}.
 */
class CallSiteArgumentsTest {

    @Nonnull
    private static final Value ONE = LiteralValue.ofScalar(1);
    @Nonnull
    private static final Value TWO = LiteralValue.ofScalar(2);
    @Nonnull
    private static final CallSiteArguments.Option<Integer> AN_OPTION = CallSiteArguments.Option.ofInteger("anInt");

    @Test
    void emptyCallSiteCarriesNothing() {
        final var empty = CallSiteArguments.empty();

        Assertions.assertSame(CallSiteArguments.EMPTY, empty, "empty() should hand out the shared instance");
        Assertions.assertTrue(empty.isEmpty());
        Assertions.assertEquals(0, empty.arity());
        Assertions.assertEquals(0, empty.size());
        Assertions.assertFalse(empty.isNamed(), "an empty call site uses the positional convention");
        Assertions.assertFalse(empty.isWindowed());
        Assertions.assertFalse(empty.hasOptions());
        Assertions.assertTrue(empty.isSimple(), "a call site without options or a window specification is simple");
    }

    @Test
    void positionalFactoriesAgree() {
        final var expected = CallSiteArguments.ofPositional(ImmutableList.of(ONE, TWO));

        Assertions.assertEquals(expected, CallSiteArguments.ofPositional(ONE, TWO),
                "the varargs factory should agree with the list factory");
        Assertions.assertEquals(expected,
                CallSiteArguments.ofPositional(Iterables.concat(ImmutableList.of(ONE), ImmutableList.of(TWO))),
                "the iterable factory should agree with the list factory");
        Assertions.assertEquals(expected.hashCode(), CallSiteArguments.ofPositional(ONE, TWO).hashCode());

        Assertions.assertEquals(ImmutableList.of(ONE, TWO), expected.getArgumentsList());
        Assertions.assertEquals(2, expected.arity());
        Assertions.assertEquals(expected.arity(), expected.size(), "arity() and size() count the same arguments");
        Assertions.assertFalse(expected.isEmpty());
    }

    @Test
    void singleArgumentPositionalFactoryAgreesWithListFactory() {
        Assertions.assertEquals(CallSiteArguments.ofPositional(ImmutableList.of(ONE)),
                CallSiteArguments.ofPositional(ONE));
        Assertions.assertEquals(1, CallSiteArguments.ofPositional(ONE).arity());
    }

    @Test
    void namedFactoriesAgree() {
        final var singleNamed = CallSiteArguments.ofNamed("a", ONE);

        Assertions.assertEquals(CallSiteArguments.ofNamed(ImmutableMap.of("a", ONE)), singleNamed,
                "the single-argument factory should agree with the map factory");
        Assertions.assertTrue(singleNamed.isNamed());
        Assertions.assertEquals(ImmutableMap.of("a", ONE), singleNamed.asNamedArguments().namedArguments());
        Assertions.assertEquals(1, singleNamed.arity());
    }

    @Test
    void namedArgumentsAreKeptInSuppliedOrder() {
        // the arguments come back in the map's iteration order, not sorted by name
        final var arguments = CallSiteArguments.ofNamed(ImmutableMap.of("b", TWO, "a", ONE));

        Assertions.assertEquals(ImmutableList.of(TWO, ONE), arguments.getArgumentsList());
        Assertions.assertEquals(ImmutableList.of(TWO, ONE), ImmutableList.copyOf(arguments.getArguments()));
        Assertions.assertEquals(2, arguments.arity());
    }

    @Test
    void positionalCallSiteIsSimpleOnlyWithoutOptionsAndWindow() {
        final var plain = CallSiteArguments.ofPositional(ONE);

        Assertions.assertTrue(plain.isSimple());
        Assertions.assertTrue(plain.isSimplePositional());
        Assertions.assertFalse(plain.isSimpleNamed(), "a positional call site is never simply named");

        final var withOption = plain.withOption(AN_OPTION, 7);
        Assertions.assertFalse(withOption.isSimple(), "options make a call site non-simple");
        Assertions.assertFalse(withOption.isSimplePositional());
        Assertions.assertTrue(withOption.hasOptions());

        final var windowed = plain.withWindowSpecification(windowSpecification());
        Assertions.assertFalse(windowed.isSimple(), "a window specification makes a call site non-simple");
        Assertions.assertTrue(windowed.isWindowed());
    }

    @Test
    void namedCallSiteIsSimpleOnlyWithoutOptionsAndWindow() {
        final var plain = CallSiteArguments.ofNamed("a", ONE);

        Assertions.assertTrue(plain.isSimple());
        Assertions.assertTrue(plain.isSimpleNamed());
        Assertions.assertFalse(plain.isSimplePositional(), "a named call site is never simply positional");

        Assertions.assertFalse(plain.withOption(AN_OPTION, 7).isSimpleNamed(),
                "options make a named call site non-simple");
        Assertions.assertFalse(plain.withWindowSpecification(windowSpecification()).isSimpleNamed(),
                "a window specification makes a named call site non-simple");
    }

    @Test
    void withArgumentsKeepsOptionsAndWindowSpecification() {
        final var arguments = CallSiteArguments.ofPositional(ONE)
                .withOption(AN_OPTION, 7)
                .withWindowSpecification(windowSpecification());

        final var withNewArguments = arguments.withArguments(ImmutableList.of(TWO));

        Assertions.assertEquals(ImmutableList.of(TWO), withNewArguments.getArgumentsList());
        assertOptionAndWindowRetained(withNewArguments);
        Assertions.assertEquals(ImmutableList.of(ONE), arguments.getArgumentsList(),
                "the original call site should be unchanged");
    }

    @Test
    void withNamedArgumentsSwitchesPositionalCallSiteToNamedConvention() {
        final var arguments = CallSiteArguments.ofPositional(ONE)
                .withOption(AN_OPTION, 7)
                .withWindowSpecification(windowSpecification());

        final var named = arguments.withNamedArguments(ImmutableMap.of("a", TWO));

        Assertions.assertTrue(named.isNamed(), "the call site should now use the named convention");
        Assertions.assertEquals(ImmutableMap.of("a", TWO), named.asNamedArguments().namedArguments());
        assertOptionAndWindowRetained(named);
        Assertions.assertFalse(arguments.isNamed(), "the original call site should still be positional");
    }

    @Test
    void withNamedArgumentsReplacesTheArgumentsOfANamedCallSite() {
        final var arguments = CallSiteArguments.ofNamed("a", ONE)
                .withOption(AN_OPTION, 7)
                .withWindowSpecification(windowSpecification());

        final var replaced = arguments.withNamedArguments(ImmutableMap.of("b", TWO));

        Assertions.assertTrue(replaced.isNamed());
        Assertions.assertEquals(ImmutableMap.of("b", TWO), replaced.asNamedArguments().namedArguments());
        assertOptionAndWindowRetained(replaced);
    }

    @Test
    void withArgumentsSwitchesNamedCallSiteToPositionalConvention() {
        final var arguments = CallSiteArguments.ofNamed("a", ONE)
                .withOption(AN_OPTION, 7)
                .withWindowSpecification(windowSpecification());

        final var positional = arguments.withArguments(ImmutableList.of(TWO));

        Assertions.assertFalse(positional.isNamed(), "the call site should now use the positional convention");
        Assertions.assertEquals(ImmutableList.of(TWO), positional.getArgumentsList());
        assertOptionAndWindowRetained(positional);
    }

    @Test
    void withOptionsKeepsConventionArgumentsAndWindowSpecification() {
        final var options = CallSiteArguments.Options.builder().put(AN_OPTION, 7).build();

        final var positional = CallSiteArguments.ofPositional(ONE)
                .withWindowSpecification(windowSpecification())
                .withOptions(options);
        Assertions.assertFalse(positional.isNamed());
        Assertions.assertEquals(ImmutableList.of(ONE), positional.getArgumentsList());
        assertOptionAndWindowRetained(positional);

        final var named = CallSiteArguments.ofNamed("a", ONE)
                .withWindowSpecification(windowSpecification())
                .withOptions(options);
        Assertions.assertTrue(named.isNamed());
        Assertions.assertEquals(ImmutableMap.of("a", ONE), named.asNamedArguments().namedArguments());
        assertOptionAndWindowRetained(named);
    }

    @Test
    void withOptionsCanClearTheOptionsOfACallSite() {
        final var arguments = CallSiteArguments.ofPositional(ONE).withOption(AN_OPTION, 7);

        final var withoutOptions = arguments.withOptions(CallSiteArguments.Options.empty());

        Assertions.assertFalse(withoutOptions.hasOptions());
        Assertions.assertTrue(withoutOptions.getOption(AN_OPTION).isEmpty());
        Assertions.assertTrue(withoutOptions.isSimplePositional());
        Assertions.assertTrue(arguments.hasOptions(), "the original call site should be unchanged");
    }

    @Test
    void withWindowSpecificationKeepsConventionArgumentsAndOptions() {
        final var specification = windowSpecification();

        final var positional = CallSiteArguments.ofPositional(ONE)
                .withOption(AN_OPTION, 7)
                .withWindowSpecification(specification);
        Assertions.assertFalse(positional.isNamed());
        Assertions.assertEquals(ImmutableList.of(ONE), positional.getArgumentsList());
        assertOptionAndWindowRetained(positional);

        final var named = CallSiteArguments.ofNamed("a", ONE)
                .withOption(AN_OPTION, 7)
                .withWindowSpecification(specification);
        Assertions.assertTrue(named.isNamed());
        Assertions.assertEquals(ImmutableMap.of("a", ONE), named.asNamedArguments().namedArguments());
        assertOptionAndWindowRetained(named);
    }

    @Test
    void withWindowSpecificationCanUnwindACallSite() {
        final var arguments = CallSiteArguments.ofPositional(ONE).withWindowSpecification(windowSpecification());

        final var unwound = arguments.withWindowSpecification(CallSiteArguments.WindowSpecification.NONE);

        Assertions.assertFalse(unwound.isWindowed());
        Assertions.assertTrue(unwound.isSimplePositional());
        Assertions.assertTrue(arguments.isWindowed(), "the original call site should be unchanged");
    }

    @Test
    void windowSpecificationIsNoneOnlyWhenBothComponentsAreEmpty() {
        Assertions.assertTrue(CallSiteArguments.WindowSpecification.NONE.isNone());
        Assertions.assertTrue(new CallSiteArguments.WindowSpecification(ImmutableList.of(), ImmutableList.of()).isNone(),
                "an independently built empty specification should also count as none");
        Assertions.assertFalse(
                new CallSiteArguments.WindowSpecification(ImmutableList.of(ONE), ImmutableList.of()).isNone(),
                "a PARTITION BY only specification is a window specification");
        Assertions.assertFalse(new CallSiteArguments.WindowSpecification(ImmutableList.of(),
                        ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.ASCENDING))).isNone(),
                "an ORDER BY only specification is a window specification");
    }

    @Test
    void functionThatDeclaresNoOptionsRejectsEveryOption() {
        // a function that does not override getSupportedOptions() understands no option at all
        final var function = new BuiltInFunction<Value>("aFunction", ImmutableList.of(),
                (builtInFunction, arguments) -> ONE) {
        };
        Assertions.assertTrue(function.getSupportedOptions().isEmpty(),
                "the default should be to declare no options");

        final var callSite = CallSiteArguments.ofPositional(ONE).withOption(AN_OPTION, 7);
        final var semanticException = Assertions.assertThrows(SemanticException.class,
                () -> function.encapsulate(callSite),
                "an option should be rejected rather than silently dropped");

        Assertions.assertEquals(SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES,
                semanticException.getErrorCode());
        Assertions.assertEquals(AN_OPTION.getName(),
                semanticException.getLogInfo().get(LogMessageKeys.OPTION_NAME.toString()));
        Assertions.assertEquals("aFunction",
                semanticException.getLogInfo().get(LogMessageKeys.FUNCTION.toString()));
    }

    @Test
    void functionThatDeclaresNoOptionsAcceptsACallSiteWithoutOptions() {
        final var function = new BuiltInFunction<Value>("aFunction", ImmutableList.of(),
                (builtInFunction, arguments) -> ONE) {
        };

        Assertions.assertEquals(ONE, function.encapsulate(CallSiteArguments.ofPositional(ONE)),
                "a call site without options should not be affected by the option validation");
    }

    /**
     * Tests for {@link WindowOrderingPart}, the {@code ORDER BY} column of a window specification.
     */
    @Nested
    class WindowOrderingPartTest {

        @Test
        void equalPartsHaveEqualHashCodes() {
            final var part = orderingPart(OrderingPart.RequestedSortOrder.ASCENDING);
            final var samePart = orderingPart(OrderingPart.RequestedSortOrder.ASCENDING);

            Assertions.assertEquals(part, samePart);
            Assertions.assertEquals(part.hashCode(), samePart.hashCode());
            Assertions.assertEquals(part.computeHashCode(), part.hashCode(),
                    "the memoized hash code should be the computed one");
            Assertions.assertEquals(part, part, "a part should be equal to itself");
        }

        @Test
        void partsDifferingInSortOrderAreNotEqual() {
            final var ascending = orderingPart(OrderingPart.RequestedSortOrder.ASCENDING);
            final var descending = orderingPart(OrderingPart.RequestedSortOrder.DESCENDING);

            Assertions.assertNotEquals(ascending, descending);
            Assertions.assertNotEquals(ascending.hashCode(), descending.hashCode());
        }

        @Test
        void partsDifferingInValueAreNotEqual() {
            final var onOne = new WindowOrderingPart(ONE, OrderingPart.RequestedSortOrder.ASCENDING);
            final var onTwo = new WindowOrderingPart(TWO, OrderingPart.RequestedSortOrder.ASCENDING);

            Assertions.assertNotEquals(onOne, onTwo);
            Assertions.assertNotEquals(onOne.hashCode(), onTwo.hashCode());
        }

        @Test
        void partIsNotEqualToOtherTypes() {
            Assertions.assertNotEquals(orderingPart(OrderingPart.RequestedSortOrder.ASCENDING), ONE);
        }

        @Test
        void getCorrelatedToIsDelegatedToTheOrderingValue() {
            final var alias = CorrelationIdentifier.of("q");
            final var correlatedValue =
                    QuantifiedObjectValue.of(alias, Type.primitiveType(Type.TypeCode.LONG));

            Assertions.assertEquals(Set.of(alias),
                    new WindowOrderingPart(correlatedValue, OrderingPart.RequestedSortOrder.ASCENDING)
                            .getCorrelatedTo());
            Assertions.assertTrue(orderingPart(OrderingPart.RequestedSortOrder.ASCENDING).getCorrelatedTo().isEmpty(),
                    "a part ordering by a literal is not correlated to anything");
        }

        @Test
        void directionalSortOrderIsKeptAndNonDirectionalOneFallsBackToTheDefault() {
            final var defaultSortOrder = OrderingPart.RequestedSortOrder.DESCENDING;

            Assertions.assertEquals(OrderingPart.RequestedSortOrder.ASCENDING,
                    orderingPart(OrderingPart.RequestedSortOrder.ASCENDING)
                            .getDirectionalSortOrderOrDefault(defaultSortOrder),
                    "a directional sort order should be kept");
            Assertions.assertEquals(OrderingPart.RequestedSortOrder.ASCENDING_NULLS_LAST,
                    orderingPart(OrderingPart.RequestedSortOrder.ASCENDING_NULLS_LAST)
                            .getDirectionalSortOrderOrDefault(defaultSortOrder),
                    "a directional sort order with counterflowing nulls should be kept as well");
            Assertions.assertEquals(defaultSortOrder,
                    orderingPart(OrderingPart.RequestedSortOrder.ANY)
                            .getDirectionalSortOrderOrDefault(defaultSortOrder),
                    "ANY is not directional, so the default should win");
        }

        @Test
        void toStringRendersTheValueFollowedByTheSortDirection() {
            final var ascending = orderingPart(OrderingPart.RequestedSortOrder.ASCENDING);
            final var descending = orderingPart(OrderingPart.RequestedSortOrder.DESCENDING);

            Assertions.assertEquals(ONE + OrderingPart.RequestedSortOrder.ASCENDING.getArrowIndicator(),
                    ascending.toString());
            Assertions.assertNotEquals(ascending.toString(), descending.toString(),
                    "the rendering should distinguish the sort direction");
            Assertions.assertTrue(ascending.toString().startsWith(ONE.toString()),
                    "the rendering should start with the ordering value");
        }
    }

    @Nonnull
    private static WindowOrderingPart orderingPart(@Nonnull final OrderingPart.RequestedSortOrder sortOrder) {
        return new WindowOrderingPart(ONE, sortOrder);
    }

    @Nonnull
    private static CallSiteArguments.WindowSpecification windowSpecification() {
        return new CallSiteArguments.WindowSpecification(ImmutableList.of(ONE),
                ImmutableList.of(orderingPart(OrderingPart.RequestedSortOrder.DESCENDING)));
    }

    private static void assertOptionAndWindowRetained(@Nonnull final CallSiteArguments arguments) {
        Assertions.assertEquals(7, arguments.getOption(AN_OPTION).orElseThrow(),
                "the option should have survived the derivation");
        Assertions.assertEquals(windowSpecification(), arguments.getWindowSpecification(),
                "the window specification should have survived the derivation");
    }
}
