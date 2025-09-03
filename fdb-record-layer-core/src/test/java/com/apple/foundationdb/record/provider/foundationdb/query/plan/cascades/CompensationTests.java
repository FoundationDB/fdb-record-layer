/*
 * CompensationTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query.plan.cascades;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.GroupByMappings;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ResultCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class CompensationTests {
    private static final Compensation SOME_COMPENSATION = new Compensation() {
        @Nonnull
        @Override
        public RelationalExpression apply(@Nonnull final Memoizer memoizer,
                                          @Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public RelationalExpression applyFinal(@Nonnull final Memoizer memoizer,
                                               @Nonnull final RelationalExpression relationalExpression,
                                               @Nonnull final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    void testImpossiblePrimitives() {
        assertThat(Compensation.impossibleCompensation())
                .satisfies(compensation -> assertThat(compensation.isImpossible()).isTrue())
                .satisfies(compensation ->
                        assertThat(compensation.intersect(SOME_COMPENSATION))
                                .isSameAs(SOME_COMPENSATION));

        assertThat(PredicateCompensationFunction.noCompensationNeeded())
                .satisfies(fn -> assertThat(fn.isNeeded()).isFalse())
                .satisfies(fn -> assertThat(fn.isImpossible()).isFalse())
                .satisfies(fn ->
                        assertThat(fn.amend(ImmutableBiMap.of(), ImmutableMap.of()))
                                .isSameAs(PredicateCompensationFunction.noCompensationNeeded()));

        assertThat(PredicateCompensationFunction.impossibleCompensation())
                .satisfies(fn -> assertThat(fn.isNeeded()).isTrue())
                .satisfies(fn -> assertThat(fn.isImpossible()).isTrue())
                .satisfies(fn ->
                        assertThat(fn.amend(ImmutableBiMap.of(), ImmutableMap.of()))
                                .isSameAs(PredicateCompensationFunction.impossibleCompensation()));

        assertThat(ResultCompensationFunction.noCompensationNeeded())
                .satisfies(fn -> assertThat(fn.isNeeded()).isFalse())
                .satisfies(fn -> assertThat(fn.isImpossible()).isFalse())
                .satisfies(fn ->
                        assertThat(fn.amend(ImmutableBiMap.of(), ImmutableMap.of()))
                                .isSameAs(ResultCompensationFunction.noCompensationNeeded()));

        assertThat(ResultCompensationFunction.impossibleCompensation())
                .satisfies(fn -> assertThat(fn.isNeeded()).isTrue())
                .satisfies(fn -> assertThat(fn.isImpossible()).isTrue())
                .satisfies(fn ->
                        assertThat(fn.amend(ImmutableBiMap.of(), ImmutableMap.of()))
                                .isSameAs(ResultCompensationFunction.impossibleCompensation()));
    }

    @Nonnull
    private static Stream<Arguments> intersectArguments() {
        final var memoizer = Memoizer.noMemoization(PlannerStage.PLANNED);
        final var fuse = fuse();

        return Streams.concat(intersectArgumentsBasic(memoizer, fuse),
                intersectArgumentsForMatch(memoizer, fuse));
    }

    @Nonnull
    private static Stream<Arguments> intersectArgumentsBasic(@Nonnull final Memoizer memoizer,
                                                             @Nonnull final RelationalExpression baseExpression) {
        final var compensationA =
                selectWithPredicateCompensation(CorrelationIdentifier.of("a"),
                        alias ->
                                ImmutableList.of(new ValuePredicate(
                                        FieldValue.ofFieldName(QuantifiedObjectValue.of(alias, someRecordType()), "one"),
                                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, (long)5))));
        final var compensationB =
                selectWithPredicateCompensation(CorrelationIdentifier.of("b"),
                        alias ->
                                ImmutableList.of(new ValuePredicate(
                                        FieldValue.ofFieldName(QuantifiedObjectValue.of(alias, someRecordType()), "one"),
                                        new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, (long)10))));
        return Stream.of(
                Arguments.of(memoizer, baseExpression, compensationA, compensationB, compensationA.apply(memoizer, compensationB.apply(memoizer, baseExpression, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, baseExpression, compensationB, compensationA, compensationB.apply(memoizer, compensationA.apply(memoizer, baseExpression, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, baseExpression, Compensation.noCompensation(), compensationB, null, translationMapFunction()),
                Arguments.of(memoizer, baseExpression, compensationA, Compensation.noCompensation(), null, translationMapFunction())
        );
    }

    @Nonnull
    private static Stream<Arguments> intersectArgumentsForMatch(@Nonnull final Memoizer memoizer,
                                                                @Nonnull final RelationalExpression baseExpression) {

        final var a = CorrelationIdentifier.of("a");
        final var reference = memoizer.memoizeExploratoryExpression(baseExpression);
        final var quantifier = Quantifier.forEach(reference, a);
        final var eqPredicate = new ValuePredicate(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(a, someRecordType()), "one"),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, (long)5));
        final var ltPredicate = new ValuePredicate(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(a, someRecordType()), "one"),
                new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, (long)10));

        final var predicateCompensationMapA =
                new LinkedIdentityMap<QueryPredicate, PredicateCompensationFunction>();
        predicateCompensationMapA.put(eqPredicate, PredicateCompensationFunction.ofPredicate(eqPredicate));

        final var compensationA =
                Compensation.noCompensation()
                        .derived(false,
                                predicateCompensationMapA,
                                ImmutableList.of(quantifier),
                                ImmutableSet.of(),
                                ImmutableSet.of(a),
                                ResultCompensationFunction.noCompensationNeeded(),
                                GroupByMappings.empty());

        final var predicateCompensationMapB =
                new LinkedIdentityMap<QueryPredicate, PredicateCompensationFunction>();
        predicateCompensationMapB.put(ltPredicate, PredicateCompensationFunction.ofPredicate(ltPredicate));

        final var compensationB =
                Compensation.noCompensation()
                        .derived(false,
                                predicateCompensationMapB,
                                ImmutableList.of(quantifier),
                                ImmutableSet.of(),
                                ImmutableSet.of(a),
                                ResultCompensationFunction.noCompensationNeeded(),
                                GroupByMappings.empty());

        return Stream.of(Arguments.of(memoizer, baseExpression, compensationA, compensationB, null));
    }

    @ParameterizedTest
    @MethodSource("intersectArguments")
    void testIntersect(@Nonnull final Memoizer memoizer,
                       @Nonnull final RelationalExpression base,
                       @Nonnull final Compensation compensationA,
                       @Nonnull final Compensation compensationB,
                       @Nullable final RelationalExpression expected) {
        final var intersectedCompensation = compensationA.intersect(compensationB);
        if (expected == null) {
            assertThat(intersectedCompensation.isNeeded()).isFalse();
        } else {
            assertThat(intersectedCompensation.apply(memoizer, base, translationMapFunction()))
                    .satisfies(actual -> assertThat(actual.semanticEquals(expected)).isTrue());
        }
    }

    @Nonnull
    private static Stream<Arguments> intersectArgumentsFinal() {
        final var memoizer = Memoizer.noMemoization(PlannerStage.PLANNED);

        final var compensationA =
                selectWithPredicateCompensation(CorrelationIdentifier.of("a"),
                        alias -> {
                            throw new UnsupportedOperationException();
                        });
        final var compensationB =
                selectWithPredicateCompensation(CorrelationIdentifier.of("b"),
                        alias -> {
                            throw new UnsupportedOperationException();
                        });
        final var fuse = fuse();

        return Stream.of(
                Arguments.of(memoizer, fuse, compensationA, compensationB, compensationA.applyFinal(memoizer, compensationB.applyFinal(memoizer, fuse, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, fuse, compensationB, compensationA, compensationB.applyFinal(memoizer, compensationA.applyFinal(memoizer, fuse, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, fuse, Compensation.noCompensation(), compensationB, null, translationMapFunction()),
                Arguments.of(memoizer, fuse, compensationA, Compensation.noCompensation(), null, translationMapFunction())
        );
    }

    @ParameterizedTest
    @MethodSource("intersectArgumentsFinal")
    void testBasicIntersectFinal(@Nonnull final Memoizer memoizer,
                                 @Nonnull final RelationalExpression base,
                                 @Nonnull final Compensation compensationA,
                                 @Nonnull final Compensation compensationB,
                                 @Nullable final RelationalExpression expected) {
        final var intersectedCompensation = compensationA.intersect(compensationB);
        if (expected == null) {
            assertThat(intersectedCompensation.isNeeded()).isFalse();
        } else {
            assertThat(intersectedCompensation.applyFinal(memoizer, base, translationMapFunction()))
                    .satisfies(actual -> assertThat(actual.semanticEquals(expected)).isTrue());
        }
    }

    @Nonnull
    private static Stream<Arguments> unionArguments() {
        final var memoizer = Memoizer.noMemoization(PlannerStage.PLANNED);
        final var fuse = fuse();
        return Streams.concat(unionArgumentsBasic(memoizer, fuse),
                unionArgumentsForMatch(memoizer, fuse));
    }

    @Nonnull
    private static Stream<Arguments> unionArgumentsBasic(@Nonnull final Memoizer memoizer,
                                                         @Nonnull final RelationalExpression baseExpression) {
        final var compensationA =
                selectWithPredicateCompensation(CorrelationIdentifier.of("a"),
                        alias ->
                                ImmutableList.of(new ValuePredicate(
                                        FieldValue.ofFieldName(QuantifiedObjectValue.of(alias, someRecordType()), "one"),
                                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, (long)5))));
        final var compensationB =
                selectWithPredicateCompensation(CorrelationIdentifier.of("b"),
                        alias ->
                                ImmutableList.of(new ValuePredicate(
                                        FieldValue.ofFieldName(QuantifiedObjectValue.of(alias, someRecordType()), "one"),
                                        new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, (long)10))));
        return Stream.of(
                Arguments.of(memoizer, baseExpression, compensationA, compensationB, compensationA.apply(memoizer, compensationB.apply(memoizer, baseExpression, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, baseExpression, compensationB, compensationA, compensationB.apply(memoizer, compensationA.apply(memoizer, baseExpression, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, baseExpression, Compensation.noCompensation(), compensationB, compensationB.apply(memoizer, baseExpression, translationMapFunction()), translationMapFunction()),
                Arguments.of(memoizer, baseExpression, compensationA, Compensation.noCompensation(), compensationA.apply(memoizer, baseExpression, translationMapFunction()), translationMapFunction())
        );
    }

    @Nonnull
    private static Stream<Arguments> unionArgumentsForMatch(@Nonnull final Memoizer memoizer,
                                                            @Nonnull final RelationalExpression baseExpression) {

        final var a = CorrelationIdentifier.of("a");
        final var reference = memoizer.memoizeExploratoryExpression(baseExpression);
        final var quantifier = Quantifier.forEach(reference, a);
        final var eqPredicate = new ValuePredicate(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(a, someRecordType()), "one"),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, (long)5));
        final var ltPredicate = new ValuePredicate(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(a, someRecordType()), "one"),
                new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, (long)10));

        final var predicateCompensationMapA =
                new LinkedIdentityMap<QueryPredicate, PredicateCompensationFunction>();
        predicateCompensationMapA.put(eqPredicate, PredicateCompensationFunction.ofPredicate(eqPredicate));

        final var compensationA =
                Compensation.noCompensation()
                        .derived(false,
                                predicateCompensationMapA,
                                ImmutableList.of(quantifier),
                                ImmutableSet.of(),
                                ImmutableSet.of(a),
                                ResultCompensationFunction.noCompensationNeeded(),
                                GroupByMappings.empty());

        final var predicateCompensationMapB =
                new LinkedIdentityMap<QueryPredicate, PredicateCompensationFunction>();
        predicateCompensationMapB.put(ltPredicate, PredicateCompensationFunction.ofPredicate(ltPredicate));

        final var compensationB =
                Compensation.noCompensation()
                        .derived(false,
                                predicateCompensationMapB,
                                ImmutableList.of(quantifier),
                                ImmutableSet.of(),
                                ImmutableSet.of(a),
                                ResultCompensationFunction.noCompensationNeeded(),
                                GroupByMappings.empty());

        final var compensatedExpression =
                new LogicalFilterExpression(
                        ImmutableList.of(eqPredicate, ltPredicate), quantifier);

        return Stream.of(Arguments.of(memoizer, baseExpression, compensationA, compensationB, compensatedExpression));
    }

    @ParameterizedTest
    @MethodSource("unionArguments")
    void testUnion(@Nonnull final Memoizer memoizer,
                   @Nonnull final RelationalExpression base,
                   @Nonnull final Compensation compensationA,
                   @Nonnull final Compensation compensationB,
                   @Nullable final RelationalExpression expected) {
        final var unionedCompensation = compensationA.union(compensationB);
        if (expected == null) {
            assertThat(unionedCompensation.isNeeded()).isFalse();
        } else {
            assertThat(unionedCompensation.apply(memoizer, base, translationMapFunction()))
                    .satisfies(actual -> assertThat(actual.semanticEquals(expected)).isTrue());
        }
    }

    @Nonnull
    private static Stream<Arguments> unionArgumentsFinal() {
        final var memoizer = Memoizer.noMemoization(PlannerStage.PLANNED);

        final var compensationA =
                selectWithPredicateCompensation(CorrelationIdentifier.of("a"),
                        alias -> {
                            throw new UnsupportedOperationException();
                        });
        final var compensationB =
                selectWithPredicateCompensation(CorrelationIdentifier.of("b"),
                        alias -> {
                            throw new UnsupportedOperationException();
                        });
        final var fuse = fuse();

        return Stream.of(
                Arguments.of(memoizer, fuse, compensationA, compensationB, compensationA.applyFinal(memoizer, compensationB.applyFinal(memoizer, fuse, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, fuse, compensationB, compensationA, compensationB.applyFinal(memoizer, compensationA.applyFinal(memoizer, fuse, translationMapFunction()), translationMapFunction())),
                Arguments.of(memoizer, fuse, Compensation.noCompensation(), compensationB, compensationB.applyFinal(memoizer, fuse, translationMapFunction()), translationMapFunction()),
                Arguments.of(memoizer, fuse, compensationA, Compensation.noCompensation(), compensationA.applyFinal(memoizer, fuse, translationMapFunction()), translationMapFunction())
        );
    }

    @ParameterizedTest
    @MethodSource("unionArgumentsFinal")
    void testBasicUnionFinal(@Nonnull final Memoizer memoizer,
                             @Nonnull final RelationalExpression base,
                             @Nonnull final Compensation compensationA,
                             @Nonnull final Compensation compensationB,
                             @Nullable final RelationalExpression expected) {
        final var unionedCompensation = compensationA.union(compensationB);
        if (expected == null) {
            assertThat(unionedCompensation.isNeeded()).isFalse();
        } else {
            assertThat(unionedCompensation.applyFinal(memoizer, base, translationMapFunction()))
                    .satisfies(actual -> assertThat(actual.semanticEquals(expected)).isTrue());
        }
    }

    @Nonnull
    private static Function<CorrelationIdentifier, TranslationMap> translationMapFunction() {
        return alias -> TranslationMap.empty();
    }

    @Nonnull
    private static FullUnorderedScanExpression fuse() {
        return new FullUnorderedScanExpression(ImmutableSet.of("someType"),
                someRecordType(), new AccessHints());
    }

    @Nonnull
    private static Type.Record someRecordType() {
        return RuleTestHelper.TYPE_S;
    }

    @Nonnull
    private static Compensation selectWithPredicateCompensation(@Nonnull final CorrelationIdentifier alias,
                                                                @Nonnull final Function<CorrelationIdentifier, Iterable<QueryPredicate>> predicatesFunction) {
        return new Compensation() {
            @Nonnull
            @Override
            public RelationalExpression apply(@Nonnull final Memoizer memoizer,
                                              @Nonnull final RelationalExpression relationalExpression,
                                              @Nonnull final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
                final var reference = memoizer.memoizeExploratoryExpression(relationalExpression);
                final var quantifier = Quantifier.forEach(reference, alias);
                return GraphExpansion.builder()
                        .addQuantifier(quantifier)
                        .addAllPredicates(predicatesFunction.apply(alias))
                        .build()
                        .buildSimpleSelectOverQuantifier(quantifier);
            }

            @Nonnull
            @Override
            public RelationalExpression applyFinal(@Nonnull final Memoizer memoizer,
                                                   @Nonnull final RelationalExpression relationalExpression,
                                                   @Nonnull final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
                final var reference = memoizer.memoizeExploratoryExpression(relationalExpression);
                final var quantifier = Quantifier.forEach(reference, alias);
                return GraphExpansion.builder()
                        .addQuantifier(quantifier)
                        .build()
                        .buildSimpleSelectOverQuantifier(quantifier);
            }

            @Override
            public String toString() {
                return "SELECT-compensation";
            }
        };
    }
}
