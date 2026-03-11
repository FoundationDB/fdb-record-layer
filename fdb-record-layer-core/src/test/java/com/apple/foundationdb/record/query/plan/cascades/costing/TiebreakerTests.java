/*
 * TiebreakerTests.java
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests of the {@link Tiebreaker} mechanics. There is a related test class, {@link TiebreakerImplementationTests}, that
 * should be used for testing individual {@link Tiebreaker} implementations.
 */
class TiebreakerTests {

    private static Set<RelationalExpression> fromHashes(int... hashCodes) {
        return IntStream.range(0, hashCodes.length)
                .mapToObj(i -> new TestExpressionWithStableHashCode("expr_" + i, hashCodes[i]))
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
    }

    private int compareByHash(@Nonnull RelationalExpression a, @Nonnull RelationalExpression b) {
        return RewritingCostModel.semanticHashTiebreaker().compare(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableMap.of(), ImmutableMap.of(),
                a, b);
    }

    /**
     * Test the most basic of comparisons. This is not a particularly useful test in and of itself as the
     * {@link Tiebreaker#compare(RecordQueryPlannerConfiguration, Map, Map, RelationalExpression,
     * RelationalExpression)}
     * method is an interface method with no implementation. Individual tests for the implementations should go
     * into {@link TiebreakerImplementationTests}. However, this basic smoke tests is useful to ensure that
     * we know how the
     * {@link com.apple.foundationdb.record.query.plan.cascades.costing.RewritingCostModel.SemanticHashTiebreaker}
     * interacts with {@link TestExpressionWithStableHashCode}, which informs the expected behavior of other tests.
     *
     * @see TiebreakerImplementationTests
     */
    @Test
    void basicComparison() {
        assertThat(compareByHash(new TestExpressionWithStableHashCode("a", 10), new TestExpressionWithStableHashCode("b", 20)))
                .isNegative();
        assertThat(compareByHash(new TestExpressionWithStableHashCode("a", 20), new TestExpressionWithStableHashCode("b", 10)))
                .isPositive();
        assertThat(compareByHash(new TestExpressionWithStableHashCode("a", 20), new TestExpressionWithStableHashCode("b", 20)))
                .isZero();
    }

    @Test
    void contextFiltersOutAllElements() {
        final TiebreakerResult<SelectExpression> result = Tiebreaker.ofContext(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableSet.of(),
                fromHashes(1, 2, 3, 4),
                SelectExpression.class,
                removed -> fail("should never call onRemove"));

        assertThat(result.getBestExpressions())
                .isEmpty();
        assertThat(result.getOnlyExpressionMaybe())
                .isEmpty();
    }

    @Test
    void contextFiltersOutAllElementsButOne() {
        final Set<RelationalExpression> expressions = fromHashes(1, 2, 3, 4);
        final FullUnorderedScanExpression scanExpression = new FullUnorderedScanExpression(ImmutableSet.of("foo"), Type.any(), new AccessHints());
        expressions.add(scanExpression);

        final TiebreakerResult<FullUnorderedScanExpression> result = Tiebreaker.ofContext(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableSet.of(),
                expressions,
                FullUnorderedScanExpression.class,
                removed -> fail("should never call onRemove"));

        assertThat(result.getBestExpressions())
                .containsExactly(scanExpression);
        assertThat(result.getOnlyExpressionMaybe())
                .containsSame(scanExpression);
    }

    @Test
    void contextFiltersToSingleType() {
        final Set<RelationalExpression> expressions = fromHashes(1, 2, 3, 4);
        final FullUnorderedScanExpression scanExpression = new FullUnorderedScanExpression(ImmutableSet.of("bar"), Type.any(), new AccessHints());
        final Set<RelationalExpression> newExpressions = ImmutableSet.<RelationalExpression>builder()
                .addAll(expressions)
                .add(scanExpression)
                .build();

        final TiebreakerResult<TestExpressionWithStableHashCode> result = Tiebreaker.ofContext(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableSet.of(),
                newExpressions,
                TestExpressionWithStableHashCode.class,
                removed -> fail("should never call onRemove"));

        assertThat(result.getBestExpressions())
                .hasSameSizeAs(expressions)
                .allMatch(expressions::contains);
        assertThatThrownBy(result::getOnlyExpressionMaybe)
                .isInstanceOf(VerifyException.class);
    }

    @Test
    void bestCanSelectSingleBestElement() {
        final Set<RelationalExpression> expressions = fromHashes(5, 7, 2, 10, 12);
        final LinkedIdentitySet<RelationalExpression> removedSet = new LinkedIdentitySet<>();
        final TiebreakerResult<RelationalExpression> result = Tiebreaker.ofContext(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableSet.of(),
                expressions,
                RelationalExpression.class,
                removed -> assertThat(removedSet.add(removed)).isTrue());

        assertThat(removedSet)
                .isEmpty();

        final RelationalExpression expected = expressions.stream()
                .filter(expr -> expr.semanticHashCode() == 2)
                .findFirst()
                .orElseGet(() -> fail("Unable to find minimal expression"));
        final TiebreakerResult<RelationalExpression> filteredResult = result.thenApply(RewritingCostModel.semanticHashTiebreaker());
        assertFilteredTo(filteredResult, expressions, expected, removedSet);
    }

    @Test
    void bestCanSelectMultipleInBestClass() {
        final Set<RelationalExpression> expressions = fromHashes(10, 3, 10, 3, 5, 8, 3, 12);
        final LinkedIdentitySet<RelationalExpression> removedSet = new LinkedIdentitySet<>();
        final TiebreakerResult<RelationalExpression> result = Tiebreaker.ofContext(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableSet.of(),
                expressions,
                RelationalExpression.class,
                removed -> assertThat(removedSet.add(removed)).isTrue());

        assertThat(removedSet)
                .isEmpty();

        final Set<RelationalExpression> expected = expressions.stream()
                .filter(expr -> expr.semanticHashCode() == 3)
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
        assertThat(expected)
                .hasSize(3);
        final TiebreakerResult<RelationalExpression> filteredResult = result.thenApply(RewritingCostModel.semanticHashTiebreaker());
        assertFilteredTo(filteredResult, expressions, expected, removedSet);
    }

    @Test
    void bestSelectsLeftMostElementWithFinalTiebreaker() {
        final Set<RelationalExpression> expressions = fromHashes(10, 6, 11, 3, 10, 3, 5, 8, 3, 12);
        final LinkedIdentitySet<RelationalExpression> removedSet = new LinkedIdentitySet<>();
        final TiebreakerResult<RelationalExpression> result = Tiebreaker.ofContext(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableSet.of(),
                expressions,
                RelationalExpression.class,
                removed -> assertThat(removedSet.add(removed)).isTrue());

        assertThat(removedSet)
                .isEmpty();
        final RelationalExpression expected = expressions.stream()
                .filter(expr -> expr.semanticHashCode() == 3)
                .findFirst()
                .orElseGet(() -> fail("Could not find the left-most expression"));
        assertThat(expected)
                .isInstanceOf(TestExpressionWithStableHashCode.class);
        assertThat(((TestExpressionWithStableHashCode)expected).getName())
                .isEqualTo("expr_3");

        // Use two calls to thenApply
        final TiebreakerResult<RelationalExpression> filteredResult = result
                .thenApply(RewritingCostModel.semanticHashTiebreaker())
                .thenApply(PickRightTiebreaker.pickRightTiebreaker());
        assertFilteredTo(filteredResult, expressions, expected, removedSet);

        // Use a single call to thenApply with a combined tiebreaker
        removedSet.clear();
        final TiebreakerResult<RelationalExpression> filteredResult2 = result
                .thenApply(Tiebreaker.combineTiebreakers(ImmutableList.of(RewritingCostModel.semanticHashTiebreaker(), PickRightTiebreaker.pickRightTiebreaker())));
        assertFilteredTo(filteredResult2, expressions, expected, removedSet);
    }

    private static void assertFilteredTo(@Nonnull TiebreakerResult<RelationalExpression> tiebreakerResult, @Nonnull Set<RelationalExpression> expressions, @Nonnull RelationalExpression expected, @Nonnull Set<RelationalExpression> removedSet) {
        final LinkedIdentitySet<RelationalExpression> expectedSet = new LinkedIdentitySet<>();
        expectedSet.add(expected);
        assertFilteredTo(tiebreakerResult, expressions, expectedSet, removedSet);
    }

    private static void assertFilteredTo(@Nonnull TiebreakerResult<RelationalExpression> tiebreakerResult, @Nonnull Set<RelationalExpression> expressions, @Nonnull Set<RelationalExpression> expected, @Nonnull Set<RelationalExpression> removedSet) {
        assertThat(tiebreakerResult.getBestExpressions())
                .containsExactlyInAnyOrderElementsOf(expected);
        if (expected.isEmpty()) {
            assertThat(tiebreakerResult.getOnlyExpressionMaybe())
                    .isEmpty();
        } else if (expected.size() == 1) {
            assertThat(tiebreakerResult.getOnlyExpressionMaybe())
                    .containsSame(Iterables.getOnlyElement(expected));
        } else {
            assertThatThrownBy(tiebreakerResult::getOnlyExpressionMaybe)
                    .isInstanceOf(VerifyException.class);
        }
        assertThat(removedSet)
                .hasSize(expressions.size() - expected.size())
                .allSatisfy(removed -> {
                    assertThat(expressions)
                            .contains(removed);
                    assertThat(expected)
                            .doesNotContain(removed);
                });
    }
}
