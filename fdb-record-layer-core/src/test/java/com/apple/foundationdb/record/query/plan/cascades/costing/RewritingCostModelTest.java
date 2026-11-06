/*
 * RewritingCostModelTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistentialValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RankWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.exists;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RewritingCostModelTest {

    private static void assertPreferred(@Nonnull Set<? extends RelationalExpression> allExpressions, @Nonnull RelationalExpression expectedBest) {
        final RewritingCostModel costModel = new RewritingCostModel(RecordQueryPlannerConfiguration.defaultPlannerConfiguration());
        final Optional<RelationalExpression> bestExpression = costModel.getBestExpression(allExpressions);
        assertThat(bestExpression)
                .as("expected best expression of %s to be %s", allExpressions, expectedBest)
                .containsSame(expectedBest);
    }

    private static void assertBetterThan(@Nonnull RelationalExpression better, @Nonnull RelationalExpression worse) {
        final LinkedIdentitySet<RelationalExpression> expressions = new LinkedIdentitySet<>();
        expressions.add(better);
        expressions.add(worse);
        assertPreferred(expressions, better);

        // Validate order in the set can be flipped without the result changing
        expressions.clear();
        expressions.add(worse);
        expressions.add(better);
        assertPreferred(expressions, better);
    }

    /**
     * Test that the push-down of a single query predicate with other predicates left at the same level is preferred.
     * The following query:
     * <pre>{@code
     * SELECT a FROM sq (SELECT a, b FROM T WHERE a = 42) WHERE EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     * should be preferred over the query:
     * <pre>{@code
     * SELECT a FROM sq (SELECT a, b FROM T) WHERE a = 42 AND EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     */
    @Test
    void costModelsPrefersPushedDownPredicates() {
        final Quantifier baseQuantifier = baseT();

        final Quantifier innerQuantifierA = forEach(selectWithPredicates(baseQuantifier, List.of("a", "b")));
        final Quantifier existentialQun = exists(selectWithPredicates(baseTau(), List.of("alpha")));
        final GraphExpansion.Builder graphABuilder = GraphExpansion.builder().addQuantifier(innerQuantifierA).addQuantifier(existentialQun);
        graphABuilder.addResultColumn(column(innerQuantifierA, "a", "a"));
        graphABuilder.addAllPredicates(List.of(
                fieldPredicate(innerQuantifierA, "a", EQUALS_42),
                new ExistentialValuePredicate(QuantifiedObjectValue.of(existentialQun), new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))
        ));
        final SelectExpression expressionA = graphABuilder.build().buildSelect();

        final Quantifier innerQuantifierB = forEach(selectWithPredicates(
                baseQuantifier, List.of("a", "b"),
                fieldPredicate(baseQuantifier, "a", EQUALS_42)
        ));
        final GraphExpansion.Builder graphBBuilder = GraphExpansion.builder()
                .addQuantifier(innerQuantifierB).addQuantifier(existentialQun);
        graphBBuilder.addResultColumn(column(innerQuantifierB, "a", "a"));
        graphBBuilder.addAllPredicates(List.of(new ExistentialValuePredicate(QuantifiedObjectValue.of(existentialQun), new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))));
        final SelectExpression expressionB = graphBBuilder.build().buildSelect();

        assertBetterThan(expressionB, expressionA);
    }

    /**
     * Test that an expression with a simplified query predicate is preferred.
     * The following query:
     * <pre>{@code
     * SELECT a FROM T WHERE a = 42
     * }</pre>
     * should be preferred over the query:
     * <pre>{@code
     * SELECT a FROM T WHERE a = 42 and a = 42
     * }</pre>
     */
    @Test
    void costModelsPrefersSimplifiedPredicates() {
        final Quantifier baseQuantifier = baseT();
        final GraphExpansion.Builder graphABuilder = GraphExpansion.builder().addQuantifier(baseQuantifier);
        graphABuilder.addResultColumn(column(baseQuantifier, "a", "a"));
        graphABuilder.addAllPredicates(List.of(
                fieldPredicate(baseQuantifier, "a", EQUALS_42),
                fieldPredicate(baseQuantifier, "a", EQUALS_42)
        ));
        final SelectExpression expressionA = graphABuilder.build().buildSelect();

        final GraphExpansion.Builder graphBBuilder = GraphExpansion.builder().addQuantifier(baseQuantifier);
        graphBBuilder.addResultColumn(column(baseQuantifier, "b", "b"));
        graphBBuilder.addAllPredicates(List.of(fieldPredicate(baseQuantifier, "a", EQUALS_42)));
        final SelectExpression expressionB = graphBBuilder.build().buildSelect();

        assertBetterThan(expressionB, expressionA);
    }

    /**
     * Test that an expression with a tautology query predicate is preferred.
     * The following query:
     * <pre>{@code
     * SELECT a FROM T WHERE false
     * }</pre>
     * should be preferred over the query:
     * <pre>{@code
     * SELECT a FROM T WHERE a IS NULL
     * }</pre>
     */
    @Test
    void costModelPrefersEliminatedPredicates() {
        final Quantifier baseQuantifier = baseT();
        final GraphExpansion.Builder graphABuilder = GraphExpansion.builder().addQuantifier(baseQuantifier);
        graphABuilder.addResultColumn(column(baseQuantifier, "a", "a"));
        graphABuilder.addAllPredicates(List.of(
                fieldPredicate(baseQuantifier, "a", new Comparisons.NullComparison(Comparisons.Type.IS_NULL))
        ));
        final SelectExpression expressionA = graphABuilder.build().buildSelect();

        final GraphExpansion.Builder graphBBuilder = GraphExpansion.builder().addQuantifier(baseQuantifier);
        graphBBuilder.addResultColumn(column(baseQuantifier, "b", "b"));
        graphBBuilder.addAllPredicates(List.of(ConstantPredicate.FALSE));
        final SelectExpression expressionB = graphBBuilder.build().buildSelect();

        assertBetterThan(expressionB, expressionA);
    }

    /**
     * Test that an expression with a simplified query predicate at a deeper level is preferred.
     * The following query:
     * <pre>{@code
     * SELECT a FROM sq (SELECT a, b FROM T WHERE a = 42) WHERE EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     * should be preferred over the query:
     * <pre>{@code
     * SELECT a
     * FROM sq (
     *      SELECT a, b
     *      FROM T
     *      WHERE a = 42 OR (a = 43 AND false) OR (a is null AND false)
     * )
     * WHERE EXISTS (SELECT alpha FROM TAU).
     * }</pre>
     */
    @Test
    void costModelPrefersSimplifiedPredicatesAtDeeperLevels() {
        final Quantifier baseQuantifier = baseT();
        final Quantifier innerQuantifierA = forEach(selectWithPredicates(
                baseQuantifier, List.of("a", "b"),
                OrPredicate.or(
                    fieldPredicate(baseQuantifier, "a", EQUALS_42),
                        AndPredicate.and(
                                fieldPredicate(baseQuantifier, "a",
                                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 43L)),
                                ConstantPredicate.FALSE
                        ),
                        AndPredicate.and(
                                fieldPredicate(baseQuantifier, "a",
                                        new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 44L)),
                                ConstantPredicate.FALSE
                        )
                )
        ));
        final Quantifier existentialQun = exists(selectWithPredicates(baseTau(), List.of("alpha")));
        final GraphExpansion.Builder graphABuilder = GraphExpansion.builder().addQuantifier(innerQuantifierA).addQuantifier(existentialQun);
        graphABuilder.addResultColumn(column(innerQuantifierA, "a", "a"));
        graphABuilder.addAllPredicates(List.of(
                fieldPredicate(innerQuantifierA, "a", EQUALS_42),
                new ExistentialValuePredicate(QuantifiedObjectValue.of(existentialQun), new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))
        ));
        final SelectExpression expressionA = graphABuilder.build().buildSelect();

        final Quantifier innerQuantifierB = forEach(selectWithPredicates(
                baseQuantifier, List.of("a", "b"),
                fieldPredicate(baseQuantifier, "a", EQUALS_42)
        ));
        final GraphExpansion.Builder graphBBuilder = GraphExpansion.builder()
                .addQuantifier(innerQuantifierB).addQuantifier(existentialQun);
        graphBBuilder.addResultColumn(column(innerQuantifierB, "a", "a"));
        graphBBuilder.addAllPredicates(List.of(new ExistentialValuePredicate(QuantifiedObjectValue.of(existentialQun), new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))));
        final SelectExpression expressionB = graphBBuilder.build().buildSelect();

        assertBetterThan(expressionB, expressionA);
    }

    /**
     * Test that an expanded window expression is preferred over the compile-time placeholder it was rewritten from,
     * even though it costs an extra select box. The following query:
     * <pre>{@code
     * SELECT w._0 FROM w (WINDOW rank(s._1) PARTITION BY s._0 FROM s (SELECT b, a FROM T))
     * }</pre>
     *
     * <p>should be preferred over the query:
     * <pre>{@code
     * SELECT rank(a) OVER (PARTITION BY b) FROM T
     * }</pre>
     */
    @Test
    void costModelPrefersTheExpandedWindowExpression() {
        final Quantifier baseQuantifier = baseT();
        final SelectExpression expressionA = selectComputing(baseQuantifier, windowPlaceholder(baseQuantifier));
        final SelectExpression expressionB = expandedWindowExpression(baseQuantifier);

        assertBetterThan(expressionB, expressionA);
    }

    /**
     * Test that the same holds when the window function is only compared and never projected, which is the shape a
     * {@code QUALIFY} clause produces: the placeholder sits in a predicate rather than in the result value.
     */
    @Test
    void costModelPrefersTheExpandedWindowExpressionWhenThePlaceholderIsInAPredicate() {
        final Quantifier baseQuantifier = baseT();
        final GraphExpansion.Builder graphABuilder = GraphExpansion.builder().addQuantifier(baseQuantifier);
        graphABuilder.addResultColumn(column(baseQuantifier, "b", "b"));
        graphABuilder.addPredicate(windowPlaceholder(baseQuantifier).withComparison(EQUALS_42));
        final SelectExpression expressionA = graphABuilder.build().buildSelect();

        final SelectExpression expressionB = expandedWindowExpression(baseQuantifier);

        assertBetterThan(expressionB, expressionA);
    }

    /**
     * The placeholder the SQL layer produces for {@code rank(a) OVER (PARTITION BY b)}.
     */
    @Nonnull
    private static CompileTimeWindowValue windowPlaceholder(@Nonnull final Quantifier quantifier) {
        return new CompileTimeWindowValue(CompileTimeWindowValue.WindowFunction.RANK,
                List.of(fieldValue(quantifier, "a")),
                List.of(fieldValue(quantifier, "b")));
    }

    /**
     * The three levels the placeholder is rewritten into: a select underneath a window expression, and a select on top
     * that reads the window column positionally.
     */
    @Nonnull
    private static SelectExpression expandedWindowExpression(@Nonnull final Quantifier baseQuantifier) {
        final Quantifier lowerQuantifier = forEach(selectWithPredicates(baseQuantifier, List.of("b", "a")));
        final Value partitioningValue = fieldValue(lowerQuantifier, "b");
        final Value argumentValue = fieldValue(lowerQuantifier, "a");
        final Quantifier windowQuantifier = forEach(new WindowExpression(
                new RankWindowValue(List.of(argumentValue), WindowFrameSpecification.defaultSpecification()),
                List.of(partitioningValue), List.of(argumentValue), List.of(), lowerQuantifier));

        return selectComputing(windowQuantifier,
                FieldValue.ofOrdinalNumber(windowQuantifier.getFlowedObjectValue(), 0));
    }

    @Nonnull
    private static SelectExpression selectComputing(@Nonnull final Quantifier quantifier, @Nonnull final Value value) {
        return GraphExpansion.builder()
                .addQuantifier(quantifier)
                .addResultColumn(Column.unnamedOf(value))
                .build()
                .buildSelect();
    }
}
