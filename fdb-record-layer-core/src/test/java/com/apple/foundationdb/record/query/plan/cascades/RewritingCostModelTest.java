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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.exists;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class RewritingCostModelTest {
    /**
     * Test that simple rewrite of a single parameter predicate with the existence of other predicates is preferred.
     * The following query:
     * </p>
     * <pre>{@code
     * SELECT a FROM T (SELECT a, b FROM T WHERE a = 42) WHERE EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     * should be preferred over the query:
     * <pre>{@code
     * SELECT a FROM T (SELECT a, b FROM T) WHERE a = 42 AND EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     * <p>
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
                new ExistsPredicate(existentialQun.getAlias())
        ));
        final SelectExpression expressionA = graphABuilder.build().buildSelect();

        final Quantifier innerQuantifierB = forEach(selectWithPredicates(
                baseQuantifier, List.of("a", "b"),
                fieldPredicate(baseQuantifier, "a", EQUALS_42)
        ));
        final GraphExpansion.Builder graphBBuilder = GraphExpansion.builder()
                .addQuantifier(innerQuantifierB).addQuantifier(existentialQun);
        graphBBuilder.addResultColumn(column(innerQuantifierB, "a", "a"));
        graphBBuilder.addAllPredicates(List.of(new ExistsPredicate(existentialQun.getAlias())));
        final SelectExpression expressionB = graphBBuilder.build().buildSelect();

        assertThat(PlannerPhase.REWRITING
                .createCostModel(RecordQueryPlannerConfiguration.defaultPlannerConfiguration())
                .compare(expressionB, expressionA)).isNegative();
    }

    /**
     * Test that simple rewrite of a single parameter predicate with the existence of other predicates is preferred.
     * The following query:
     * <pre>{@code
     * SELECT a FROM T WHERE a = 42
     * }</pre>
     * <p>
     * should be preferred over the query:
     * </p>
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

        assertThat(PlannerPhase.REWRITING
                .createCostModel(RecordQueryPlannerConfiguration.defaultPlannerConfiguration())
                .compare(expressionB, expressionA)).isNegative();
    }
}
