/*
 * QueryPredicateSimplificationRuleTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.projectColumn;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.areEqual;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.areEqualAsRange;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.areNotEqual;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.covFalse;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.covNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.covTrue;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNotNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litFalse;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litInt;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litString;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litTrue;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.notNullIntCov;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseBlah;

/**
 * A test suite for predicate simplification. A key component is a random predicate tree generator which, for a given
 * predicate, constructs randomized AND/OR trees. This allows testing predicate simplification in the context of complex,
 * production-like SQL statements. The generator creates a tree with the specified predicate embedded within it, providing
 * a realistic environment for evaluating simplification logic.
 * <br>
 * For example: Given a predicate {@code P1} that evaluates to {@code FALSE}, the framework puts it in a tree of
 * {@code AND} predicates and then carries on to make sure that the tree collapses to {@code FALSE}.
 * <pre>{@code
 *        AND
 *       /   \
 *     AND    C
 *     /  \
 *    A    AND
 *        /   \
 *       B     P1 <-- evaluates to false
 * }</pre>
 */
public class QueryPredicateSimplificationRuleTest {

    @Nonnull
    private static final QueryPredicateSimplificationRule rule = new QueryPredicateSimplificationRule();
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(rule);

    @Nonnull
    public static Stream<Arguments> randomPredicateProvider() {
        final var firstStream = RandomizedTestUtils.randomSeeds(123943, 92789234, 3498204, 20374023, 787234234, 89712321,
                        8971293, 87912321, 87123912, 789435, 98743534, 897432973, 879237492, 7197231, 871297831, 8781923)
                .map(seed -> {
                    final var randomPredicate = new RandomPredicateGenerator(new Random(seed));
                    final var instance = randomPredicate.generate();
                    return Arguments.arguments(instance.predicateUnderTest, instance.expectedPredicate, instance.evaluationContext);
                });
        final var secondStream = RandomizedTestUtils.randomArguments(random -> {
            final var randomPredicate = new RandomPredicateGenerator(random);
            final var instance = randomPredicate.generate();
            return Arguments.arguments(instance.predicateUnderTest, instance.expectedPredicate, instance.evaluationContext);
        });
        return Streams.concat(firstStream, secondStream);
    }

    @ParameterizedTest(name = "({1}) should be the simplified version of {0}")
    @MethodSource("randomPredicateProvider")
    void testPredicateSimplification(QueryPredicate actualPredicate, QueryPredicate expectedPredicate, EvaluationContext evaluationContext) {
        final Quantifier baseQun = baseBlah();

        final SelectExpression selectExpression = GraphExpansion.builder()
                .addQuantifier(baseQun)
                .addResultColumn(projectColumn(baseQun, "intField"))
                .addPredicate(actualPredicate)
                .build().buildSelect();

        final SelectExpression expected = GraphExpansion.builder()
                .addQuantifier(baseQun)
                .addResultColumn(projectColumn(baseQun, "intField"))
                .addPredicate(expectedPredicate)
                .build().buildSelect();
        testHelper.assertYields(selectExpression, evaluationContext, expected);
    }

    public static class RandomPredicateGenerator {

        public static class PredicateTest {
            @Nonnull
            private final QueryPredicate expectedPredicate;

            @Nonnull
            private final QueryPredicate predicateUnderTest;

            @Nonnull
            private final EvaluationContext evaluationContext;

            PredicateTest(@Nonnull final QueryPredicate expectedPredicate, @Nonnull final QueryPredicate predicateUnderTest,
                          @Nonnull final EvaluationContext evaluationContext) {
                this.expectedPredicate = expectedPredicate;
                this.predicateUnderTest = predicateUnderTest;
                this.evaluationContext = evaluationContext;
            }
        }

        @Nonnull
        private final List<NonnullPair<QueryPredicate, EvaluationContext>> nullConstantPredicates;

        @Nonnull
        private final List<NonnullPair<QueryPredicate, EvaluationContext>> trueConstantPredicates;

        @Nonnull
        private final List<QueryPredicate> trueConstantLiteralPredicates;

        @Nonnull
        private final List<NonnullPair<QueryPredicate, EvaluationContext>> falseConstantPredicates;

        @Nonnull
        private final List<QueryPredicate> falseConstantLiteralPredicates;

        @Nonnull
        private final Random random;

        public RandomPredicateGenerator(@Nonnull Random random) {
            this.random = random;
            this.nullConstantPredicates = generateNullConstantPredicates();
            this.trueConstantPredicates = buildTrueConstantPredicates();
            this.trueConstantLiteralPredicates = buildTrueConstantLiteralPredicates();
            this.falseConstantPredicates = buildFalseConstantPredicates();
            this.falseConstantLiteralPredicates = buildFalseConstantLiteralPredicates();
        }

        @Nonnull
        public PredicateTest generate() {
            final var isTrue = random.nextInt(3);
            if (isTrue == 0) {
                final var nullConstantPredicate = nullConstantPredicates.get(random.nextInt(nullConstantPredicates.size()));
                return new PredicateTest(ConstantPredicate.NULL, randomTreeForNull(nullConstantPredicate.getLeft()), nullConstantPredicate.getRight());
            } else if (isTrue == 1) {
                final var trueConstantPredicate = trueConstantPredicates.get(random.nextInt(trueConstantPredicates.size()));
                return new PredicateTest(ConstantPredicate.TRUE, randomOrTree(trueConstantPredicate.getLeft()), trueConstantPredicate.getRight());
            } else {
                final var falseConstantPredicate = falseConstantPredicates.get(random.nextInt(falseConstantPredicates.size()));
                return new PredicateTest(ConstantPredicate.FALSE, randomAndTree(falseConstantPredicate.getLeft()), falseConstantPredicate.getRight());
            }
        }

        /**
         * List of predicates that can not be evaluated at query compile time.
         */
        @Nonnull
        private final List<QueryPredicate> nonConstantPredicates = ImmutableList.of(
                fieldPredicate(baseBlah(), "intField", new Comparisons.NullComparison(Comparisons.Type.NOT_NULL)),
                fieldPredicate(baseBlah(), "stringField", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, litString("foo").value())),
                LiteralValue.ofScalar("something").withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(baseBlah(), "stringField"))),
                fieldPredicate(baseBlah(), "intField", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, litInt(42).value())),
                new NullValue(Type.primitiveType(Type.TypeCode.INT)).withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(baseBlah(), "intField")))
        );

        @Nonnull
        private static List<NonnullPair<QueryPredicate, EvaluationContext>> generateNullConstantPredicates() {
            final var nullConstantPredicatesBuilder = ImmutableList.<NonnullPair<QueryPredicate, EvaluationContext>>builder();
            {
                var op1 = covTrue();
                var op2 = covNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = covFalse();
                var op2 = covNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litNull();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litFalse();
                var op2 = litNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }

            {
                var op1 = litFalse();
                var op2 = litNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = covFalse();
                var op2 = covNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litNull();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litFalse();
                var op2 = litNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            return nullConstantPredicatesBuilder.build();
        }

        @Nonnull
        private static List<NonnullPair<QueryPredicate, EvaluationContext>> buildTrueConstantPredicates() {
            final var nullConstantPredicatesBuilder = ImmutableList.<NonnullPair<QueryPredicate, EvaluationContext>>builder();
            {
                var op1 = covTrue();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litFalse();
                var op2 = litFalse();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litFalse();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = notNullIntCov();
                nullConstantPredicatesBuilder.add(NonnullPair.of(isNotNull(op1.value()), op1.getEvaluationContext()));
            }
            {
                var op2 = litNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(isNull(op2.value()), EvaluationContext.empty()));
            }
            {
                var op1 = covFalse();
                var op2 = litFalse();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqualAsRange(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = covTrue();
                var op2 = covTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqualAsRange(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            return nullConstantPredicatesBuilder.build();
        }

        @Nonnull
        private static List<QueryPredicate> buildTrueConstantLiteralPredicates() {
            final var trueConstantLiteralPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
            {
                var op1 = litTrue();
                var op2 = litTrue();
                trueConstantLiteralPredicatesBuilder.add(areEqual(op1.value(), op2.value()));
            }
            {
                var op1 = litFalse();
                var op2 = litFalse();
                trueConstantLiteralPredicatesBuilder.add(areEqual(op1.value(), op2.value()));
            }
            {
                var op1 = litFalse();
                var op2 = litTrue();
                trueConstantLiteralPredicatesBuilder.add(areNotEqual(op1.value(), op2.value()));
            }
            {
                var op1 = litInt(42);
                trueConstantLiteralPredicatesBuilder.add(isNotNull(op1.value()));
            }
            {
                var op2 = litNull();
                trueConstantLiteralPredicatesBuilder.add(isNull(op2.value()));
            }
            {
                var op1 = litFalse();
                var op2 = litFalse();
                trueConstantLiteralPredicatesBuilder.add(areEqualAsRange(op1.value(), op2.value()));
            }
            {
                var op1 = litTrue();
                var op2 = litTrue();
                trueConstantLiteralPredicatesBuilder.add(areEqualAsRange(op1.value(), op2.value()));
            }
            return trueConstantLiteralPredicatesBuilder.build();
        }


        private static List<NonnullPair<QueryPredicate, EvaluationContext>> buildFalseConstantPredicates() {
            final var nullConstantPredicatesBuilder = ImmutableList.<NonnullPair<QueryPredicate, EvaluationContext>>builder();
            {
                var op1 = covTrue();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litFalse();
                var op2 = litFalse();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litFalse();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = litFalse();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = notNullIntCov();
                nullConstantPredicatesBuilder.add(NonnullPair.of(isNull(op1.value()), op1.getEvaluationContext()));
            }
            {
                var op2 = litNull();
                nullConstantPredicatesBuilder.add(NonnullPair.of(isNotNull(op2.value()), EvaluationContext.empty()));
            }
            {
                var op1 = covFalse();
                var op2 = litFalse();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            {
                var op1 = covTrue();
                var op2 = covTrue();
                nullConstantPredicatesBuilder.add(NonnullPair.of(areNotEqual(op1.value(), op2.value()), op1.mergeEvaluationContext(op2)));
            }
            return nullConstantPredicatesBuilder.build();
        }

        private static List<QueryPredicate> buildFalseConstantLiteralPredicates() {
            final var nullConstantPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
            {
                var op1 = litTrue();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(areNotEqual(op1.value(), op2.value()));
            }
            {
                var op1 = litFalse();
                var op2 = litFalse();
                nullConstantPredicatesBuilder.add(areNotEqual(op1.value(), op2.value()));
            }
            {
                var op1 = litFalse();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(areEqual(op1.value(), op2.value()));
            }
            {
                var op1 = litFalse();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(areEqualAsRange(op1.value(), op2.value()));
            }
            {
                var op1 = litInt(42);
                nullConstantPredicatesBuilder.add(isNull(op1.value()));
            }
            {
                var op2 = litNull();
                nullConstantPredicatesBuilder.add(isNotNull(op2.value()));
            }
            {
                var op1 = litFalse();
                var op2 = litFalse();
                nullConstantPredicatesBuilder.add(areNotEqual(op1.value(), op2.value()));
            }
            {
                var op1 = litTrue();
                var op2 = litTrue();
                nullConstantPredicatesBuilder.add(areNotEqual(op1.value(), op2.value()));
            }
            return nullConstantPredicatesBuilder.build();
        }

        @Nonnull
        private List<QueryPredicate> generateRandomPredicates(QueryPredicate needle, List<QueryPredicate> otherPredicates) {
            final int listSize = 2 + random.nextInt(8);
            final int needlePosition = random.nextInt(listSize);
            final var predicateBuilder = ImmutableList.<QueryPredicate>builder();
            for (int i = 0; i < listSize; i++) {
                if (needlePosition == i) {
                    predicateBuilder.add(needle);
                } else {
                    final int nonConstantPredicatePosition = random.nextInt(otherPredicates.size());
                    predicateBuilder.add(otherPredicates.get(nonConstantPredicatePosition));
                }
            }
            return predicateBuilder.build();
        }

        @Nonnull
        private QueryPredicate generateRandomOrPredicate(QueryPredicate needle, List<QueryPredicate> otherPredicates) {
            return OrPredicate.or(generateRandomPredicates(needle, otherPredicates));
        }

        @Nonnull
        private QueryPredicate generateRandomAndPredicate(QueryPredicate constantPredicate, List<QueryPredicate> otherPredicates) {
            return AndPredicate.and(generateRandomPredicates(constantPredicate, otherPredicates));
        }

        @Nonnull
        private QueryPredicate randomOrTree(QueryPredicate needleAtLeaf, List<QueryPredicate> otherPredicates) {
            final var depth = 2 + random.nextInt(4);
            var currentLevel = generateRandomOrPredicate(needleAtLeaf, otherPredicates);
            for (int i = 0; i < depth; i++) {
                currentLevel = generateRandomOrPredicate(currentLevel, otherPredicates);
            }
            return currentLevel;
        }

        @Nonnull
        private QueryPredicate randomOrTree(QueryPredicate needleAtLeaf) {
            return randomOrTree(needleAtLeaf, nonConstantPredicates);
        }

        @Nonnull
        private QueryPredicate randomAndTree(QueryPredicate needleAtLeaf, List<QueryPredicate> otherPredicates) {
            final var depth = 2 + random.nextInt(4);
            var currentLevel = generateRandomAndPredicate(needleAtLeaf, otherPredicates);
            for (int i = 0; i < depth; i++) {
                currentLevel = generateRandomAndPredicate(currentLevel, otherPredicates);
            }
            return currentLevel;
        }

        @Nonnull
        private QueryPredicate randomAndTree(QueryPredicate needleAtLeaf) {
            return randomAndTree(needleAtLeaf, nonConstantPredicates);
        }

        @Nonnull
        public QueryPredicate randomTreeForNull(QueryPredicate needleAtLeaf) {
            final var depth = 2 + random.nextInt(4);
            var isAnd = random.nextBoolean();
            var currentLevel = isAnd ? generateRandomAndPredicate(needleAtLeaf, trueConstantLiteralPredicates)
                               : generateRandomOrPredicate(needleAtLeaf, falseConstantLiteralPredicates);
            for (int i = 0; i < depth; i++) {
                isAnd = random.nextBoolean();
                currentLevel = isAnd ? generateRandomAndPredicate(currentLevel, trueConstantLiteralPredicates)
                      : generateRandomOrPredicate(needleAtLeaf, falseConstantLiteralPredicates);
            }
            return currentLevel;
        }
    }
}
