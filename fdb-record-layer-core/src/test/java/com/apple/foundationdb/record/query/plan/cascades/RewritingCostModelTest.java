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

public class RewritingCostModelTest {
    /**
     * Test that simple rewrite of a single parameter predicate with the existence of other predicates is preferred.
     * The following query:
     * <pre>{@code
     * SELECT a FROM (SELECT a, b, d FROM T) WHERE a = 42 AND EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     * <p>
     * should have a higher cost than the query:
     * </p>
     * <pre>{@code
     * SELECT a FROM (SELECT a, b FROM T WHERE a = 42) WHERE EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     */
    @Test
    void costModelsPrefersPushedDownPredicates() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b")
        ));
        Quantifier existentialQun = exists(selectWithPredicates(
                baseTau(), List.of("alpha")
        ));

        GraphExpansion.Builder builder = GraphExpansion.builder().addQuantifier(lowerQun).addQuantifier(existentialQun);
        builder.addResultColumn(column(lowerQun, "b", "b"));
        builder.addAllPredicates(List.of(
                fieldPredicate(lowerQun, "a", EQUALS_42),
                new ExistsPredicate(existentialQun.getAlias())
        ));
        SelectExpression higher = builder.build().buildSelect();


        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b"),
                fieldPredicate(baseQun, "a", EQUALS_42)
        ));

        GraphExpansion.Builder newBuilder = GraphExpansion.builder().addQuantifier(newLowerQun).addQuantifier(existentialQun);
        newBuilder.addResultColumn(column(newLowerQun, "b", "b"));
        newBuilder.addAllPredicates(List.of(
                new ExistsPredicate(existentialQun.getAlias())
        ));
        SelectExpression newHigher = newBuilder.build().buildSelect();

        assertThat(PlannerPhase.REWRITING.createCostModel(RecordQueryPlannerConfiguration.defaultPlannerConfiguration()).compare(newHigher, higher)).isNegative();
    }
}
