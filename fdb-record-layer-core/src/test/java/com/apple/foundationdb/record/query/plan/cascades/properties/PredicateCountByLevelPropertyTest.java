/*
 * PredicateCountByLevelPropertyTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.properties.PredicateCountByLevelProperty.predicateCountByLevel;
import static org.assertj.core.api.Assertions.assertThat;

class PredicateCountByLevelPropertyTest {
    /**
     * Test that a simple expression with one predicate is evaluated correctly.
     * Query:
     *  </p>
     *  <pre>{@code
     *      SELECT a FROM T WHERE a = 42
     *  }</pre>
     */
    @Test
    void selectWithOnePredicateIsEvaluatedCorrectly() {
        final Quantifier baseQuantifier = baseT();
        final GraphExpansion.Builder graphBuilder = GraphExpansion.builder().addQuantifier(baseQuantifier);
        graphBuilder.addResultColumn(column(baseQuantifier, "a", "a"));
        graphBuilder.addAllPredicates(List.of(fieldPredicate(baseQuantifier, "a", EQUALS_42)));
        final SelectExpression expression = graphBuilder.build().buildSelect();

        final var info = predicateCountByLevel().evaluate(expression);

        assertThat(info.getHighestLevel()).isEqualTo(3);
        assertThat(info.getLevelToPredicateCount()).containsExactly(
                Map.entry(1, 0), // corresponds to the level with the FullUnorderedScanExpression
                Map.entry(2, 0), // corresponds to the level with the LogicalTypeFilterExpression
                Map.entry(3, 1)
        );
    }

    /**
     * Test that expressions with predicates at different levels are evaluated correctly.
     * Query:
     *  </p>
     *  <pre>{@code
     *      SELECT a
     *      FROM
     *          (
     *              SELECT a, b
     *              FROM (SELECT a, b FROM T WHERE a = 42) isq
     *              WHERE a = 42 AND b = 42
     *          ) sq
     *      WHERE
     *          sq.b = 42
     *  }</pre>
     */
    @Test
    void predicatesAtMultipleLevelsAreCountedCorrectly() {
        final Quantifier baseQuantifier = baseT();
        final Quantifier isqQuantifier = forEach(selectWithPredicates(
                baseQuantifier, List.of("a", "b"),
                fieldPredicate(baseQuantifier, "a", EQUALS_42)
        ));
        final Quantifier sqQuantifier = forEach(selectWithPredicates(
                isqQuantifier, List.of("a", "b"),
                fieldPredicate(baseQuantifier, "a", EQUALS_42),
                fieldPredicate(baseQuantifier, "b", EQUALS_42)
        ));
        final GraphExpansion.Builder graphBuilder = GraphExpansion.builder().addQuantifier(sqQuantifier);
        graphBuilder.addResultColumn(column(sqQuantifier, "a", "a"));
        graphBuilder.addAllPredicates(List.of(fieldPredicate(sqQuantifier, "b", EQUALS_42)));
        final SelectExpression expression = graphBuilder.build().buildSelect();

        final var info = predicateCountByLevel().evaluate(expression);

        // Should have 2 predicates total (1 at each level)
        assertThat(info.getHighestLevel()).isEqualTo(5);
        assertThat(info.getLevelToPredicateCount()).containsExactly(
                Map.entry(1, 0), // corresponds to the level with the FullUnorderedScanExpression
                Map.entry(2, 0), // corresponds to the level with the LogicalTypeFilterExpression
                Map.entry(3, 1),
                Map.entry(4, 2),
                Map.entry(5, 1)
        );
    }

    /**
     * Test that predicate counts for different expressions at the same level are added.
     * Query:
     * </p>
     * <pre>{@code
     *      SELECT
     *          sq1.a
     *      FROM
     *          (SELECT a, b FROM T WHERE a = 42 AND b = 42) sq1,
     *          (SELECT alpha from TAU WHERE alpha = 42) sq2
     *      WHERE
     *          sq1.b = 42
     * }</pre>
     */
    @Test
    void predicatesAtTheSameLevelForDifferentExpressionsAreCountedCorrectly() {
        final Quantifier sq1BaseQuantifier = baseT();
        final Quantifier sq2BaseQuantifier = baseTau();
        final Quantifier sq1Quantifier = forEach(selectWithPredicates(
                sq1BaseQuantifier, List.of("a", "b"),
                fieldPredicate(sq1BaseQuantifier, "a", EQUALS_42),
                fieldPredicate(sq1BaseQuantifier, "b", EQUALS_42)
        ));
        final Quantifier sq2Quantifier = forEach(selectWithPredicates(
                sq2BaseQuantifier, List.of("alpha"),
                fieldPredicate(sq2BaseQuantifier, "alpha", EQUALS_42)
        ));

        final GraphExpansion.Builder graphBuilder = GraphExpansion.builder().addQuantifier(sq1Quantifier).addQuantifier(sq2Quantifier);
        graphBuilder.addResultColumn(column(sq1Quantifier, "a", "a"));
        graphBuilder.addAllPredicates(List.of(fieldPredicate(sq1Quantifier, "b", EQUALS_42)));
        final SelectExpression expression = graphBuilder.build().buildSelect();

        final var info = predicateCountByLevel().evaluate(expression);

        assertThat(info.getHighestLevel()).isEqualTo(4);
        assertThat(info.getLevelToPredicateCount()).containsExactly(
                Map.entry(1, 0), // corresponds to the level with the FullUnorderedScanExpression
                Map.entry(2, 0), // corresponds to the level with the LogicalTypeFilterExpression
                Map.entry(3, 3),
                Map.entry(4, 1)
        );
    }

    @Test
    void predicateCountByLevelInfoInstancesAreCombinedCorrectly() {
        final var aInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 1, 2, 1, 3, 2), 3
        );
        final var bInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 0, 2, 1, 3, 1, 4, 4), 4
        );

        final var combinedInfo = PredicateCountByLevelProperty.PredicateCountByLevelInfo.combine(List.of(aInfo, bInfo));

        assertThat(combinedInfo.getHighestLevel()).isEqualTo(4);
        assertThat(combinedInfo.getLevelToPredicateCount()).containsExactly(
                Map.entry(1, 1),
                Map.entry(2, 2),
                Map.entry(3, 3),
                Map.entry(4, 4)
        );
    }

    @Test
    void compareReturnsComparisonBetweenFirstNonEqualLevel() {
        final PredicateCountByLevelProperty.PredicateCountByLevelInfo aInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 1, 2, 3, 3, 1), 3);
        final PredicateCountByLevelProperty.PredicateCountByLevelInfo bInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 1, 2, 2, 3, 1), 3);

        assertThat(PredicateCountByLevelProperty.PredicateCountByLevelInfo.compare(aInfo, bInfo)).isPositive();
    }

    @Test
    void compareReturnsInfoWithMoreLevelsInCaseOfEquality() {
        final PredicateCountByLevelProperty.PredicateCountByLevelInfo aInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 1, 2, 3, 3, 1), 3);
        final PredicateCountByLevelProperty.PredicateCountByLevelInfo bInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 1, 2, 3, 3, 1, 4, 1), 4);

        assertThat(PredicateCountByLevelProperty.PredicateCountByLevelInfo.compare(aInfo, bInfo)).isNegative();
    }

    @Test
    void compareReturnsZeroForEqualPredicateCounts() {
        final PredicateCountByLevelProperty.PredicateCountByLevelInfo aInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 1, 2, 3, 3, 1, 4, 1), 4);
        final PredicateCountByLevelProperty.PredicateCountByLevelInfo bInfo = new PredicateCountByLevelProperty.PredicateCountByLevelInfo(
                Map.of(1, 1, 2, 3, 3, 1, 4, 1), 4);

        assertThat(PredicateCountByLevelProperty.PredicateCountByLevelInfo.compare(aInfo, bInfo)).isZero();
    }
}
