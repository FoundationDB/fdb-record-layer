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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests of the {@link Tiebreaker} mechanics. There is a related test class, {@link TiebreakerImplementationTests}, that
 * should be used for testing individual {@link Tiebreaker} implementations.
 */
class TiebreakerTests {

    @Nonnull
    private static Set<RelationalExpression> fromHashes(int... hashCodes) {
        return IntStream.range(0, hashCodes.length)
                .mapToObj(i -> new TestExpressionWithFixedSemanticHash("expr_" + i, hashCodes[i]))
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
     * interacts with {@link TestExpressionWithFixedSemanticHash}, which informs the expected behavior of other tests.
     *
     * @see TiebreakerImplementationTests
     */
    @Test
    void basicComparison() {
        assertThat(compareByHash(new TestExpressionWithFixedSemanticHash("a", 10), new TestExpressionWithFixedSemanticHash("b", 20)))
                .isNegative();
        assertThat(compareByHash(new TestExpressionWithFixedSemanticHash("a", 20), new TestExpressionWithFixedSemanticHash("b", 10)))
                .isPositive();
        assertThat(compareByHash(new TestExpressionWithFixedSemanticHash("a", 20), new TestExpressionWithFixedSemanticHash("b", 20)))
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

        final TiebreakerResult<TestExpressionWithFixedSemanticHash> result = Tiebreaker.ofContext(RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                ImmutableSet.of(),
                newExpressions,
                TestExpressionWithFixedSemanticHash.class,
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
                .isInstanceOf(TestExpressionWithFixedSemanticHash.class);
        assertThat(((TestExpressionWithFixedSemanticHash)expected).getName())
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

    @Test
    void collectorFiltersToMinimalElement() {
        final List<RelationalExpression> expressions = ImmutableList.copyOf(fromHashes(10, 3, 6, 2, 7, 5));
        final LinkedIdentitySet<RelationalExpression> removedSet = new LinkedIdentitySet<>();
        final var opsCache = Tiebreaker.createOpsCache(ImmutableSet.of());

        final var collector = Tiebreaker.toBestExpressions(
                RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                RewritingCostModel.semanticHashTiebreaker(),
                opsCache,
                removed -> assertThat(removedSet.add(removed)).isTrue());

        final Set<RelationalExpression> expected = expressions.stream()
                .filter(expr -> expr.semanticHashCode() == 2)
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
        assertThat(expected)
                .hasSize(1);
        validateCollector(expressions, expected, removedSet, collector);
    }

    @Test
    void collectorFiltersToMinimalSet() {
        final List<RelationalExpression> expressions = ImmutableList.copyOf(fromHashes(30, 40, 21, 20, 25, 40, 20, 23, 20));
        final LinkedIdentitySet<RelationalExpression> removedSet = new LinkedIdentitySet<>();
        final var opsCache = Tiebreaker.createOpsCache(ImmutableSet.of());

        final var collector = Tiebreaker.toBestExpressions(
                RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                RewritingCostModel.semanticHashTiebreaker(),
                opsCache,
                removed -> assertThat(removedSet.add(removed)).isTrue());

        final Set<RelationalExpression> expected = expressions.stream()
                .filter(expr -> expr.semanticHashCode() == 20)
                .collect(LinkedIdentitySet.toLinkedIdentitySet());
        assertThat(expected)
                .hasSize(3);
        validateCollector(expressions, expected, removedSet, collector);
    }

    @Test
    void collectorPicksLeftMostElementWithFinalTiebreaker() {
        final List<RelationalExpression> expressions = ImmutableList.copyOf(fromHashes(30, 40, 21, 20, 25, 40, 20, 23, 20));
        final LinkedIdentitySet<RelationalExpression> removedSet = new LinkedIdentitySet<>();
        final var opsCache = Tiebreaker.createOpsCache(ImmutableSet.of());

        final var collector = Tiebreaker.toBestExpressions(
                RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                Tiebreaker.combineTiebreakers(ImmutableList.of(RewritingCostModel.semanticHashTiebreaker(), PickRightTiebreaker.pickRightTiebreaker())),
                opsCache,
                removed -> assertThat(removedSet.add(removed)).isTrue());

        final RelationalExpression expected = expressions.stream()
                .filter(expr -> expr.semanticHashCode() == 20)
                .findFirst()
                .orElseGet(() -> fail("unable to find minimal plan element"));
        assertThat(expected)
                .isInstanceOf(TestExpressionWithFixedSemanticHash.class);
        assertThat(((TestExpressionWithFixedSemanticHash)expected).getName())
                .isEqualTo("expr_3");

        final LinkedIdentitySet<RelationalExpression> expectedSet = new LinkedIdentitySet<>();
        expectedSet.add(expected);
        validateCollector(expressions, expectedSet, removedSet, collector);
    }

    private static void validateCollector(@Nonnull List<RelationalExpression> expressions, @Nonnull Set<RelationalExpression> expected, @Nonnull Set<RelationalExpression> removedSet, @Nonnull Collector<RelationalExpression, LinkedIdentitySet<RelationalExpression>, Set<RelationalExpression>> collector) {
        // Validation 1: Should be able to just use the Stream functionality to filter to the expected set
        removedSet.clear();
        final Set<RelationalExpression> fromStream = expressions.stream().collect(collector);
        assertFilteredTo(expressions, fromStream, expected, removedSet);

        // Validation 2: Manually accumulate element by element
        removedSet.clear();
        final LinkedIdentitySet<RelationalExpression> manualInProgress = collector.supplier().get();
        for (RelationalExpression expr : expressions) {
            collector.accumulator().accept(manualInProgress, expr);
        }
        final Set<RelationalExpression> fromManualInvocation = collector.finisher().apply(manualInProgress);
        assertFilteredTo(expressions, fromManualInvocation, expected, removedSet);

        // Validation 3: Split the list into two at each possible split point, and then invoke the combiner on the collector to produce a single set
        for (int i = 0; i <= expressions.size(); i++) {
            removedSet.clear();
            final List<RelationalExpression> head = expressions.subList(0, i);
            final LinkedIdentitySet<RelationalExpression> fromHead = collector.supplier().get();
            head.forEach(expr -> collector.accumulator().accept(fromHead, expr));

            final List<RelationalExpression> tail = expressions.subList(i, expressions.size());
            final LinkedIdentitySet<RelationalExpression> fromTail = collector.supplier().get();
            tail.forEach(expr -> collector.accumulator().accept(fromTail, expr));

            final LinkedIdentitySet<RelationalExpression> combined = collector.combiner().apply(fromHead, fromTail);
            final Set<RelationalExpression> fromCombined = collector.finisher().apply(combined);
            assertFilteredTo(expressions, fromCombined, expected, removedSet);
        }
    }

    private static void assertFilteredTo(@Nonnull TiebreakerResult<RelationalExpression> tiebreakerResult, @Nonnull Set<RelationalExpression> expressions, @Nonnull RelationalExpression expected, @Nonnull Set<RelationalExpression> removedSet) {
        final LinkedIdentitySet<RelationalExpression> expectedSet = new LinkedIdentitySet<>();
        expectedSet.add(expected);
        assertFilteredTo(tiebreakerResult, expressions, expectedSet, removedSet);
    }

    private static void assertFilteredTo(@Nonnull TiebreakerResult<RelationalExpression> tiebreakerResult, @Nonnull Set<RelationalExpression> expressions, @Nonnull Set<RelationalExpression> expected, @Nonnull Set<RelationalExpression> removedSet) {
        assertFilteredTo(expressions, tiebreakerResult.getBestExpressions(), expected, removedSet);
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
    }

    private static void assertFilteredTo(@Nonnull Collection<RelationalExpression> expressions, @Nonnull Set<RelationalExpression> filteredTo, @Nonnull Set<RelationalExpression> expected, @Nonnull Set<RelationalExpression> removedSet) {
        assertThat(filteredTo)
                .containsExactlyInAnyOrderElementsOf(expected);
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
