/*
 * QueryPredicateSimplificationRule.java
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantFoldingRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.atLeastOne;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that traverses the predicates in a {@link SelectExpression} and attempts to simplify their conjunction, if
 * successful, it will create a new {@link SelectExpression} with the simplified predicate (or its simplified terms in
 * case of {@link AndPredicate}).
 * <br>
 * The rule will delegate the simplification legwork to the {@link QueryPredicate} simplification engine.
 * <br>
 * <b>Example 1</b>
 * <pre>{@code
 *  SELECT a, b, c
 *  FROM t
 *  WHERE a = 3 AND false
 *  }</pre>
 * simplifies to:
 * <pre>{@code
 *  SELECT a, b, c
 *  FROM t
 *  WHERE false
 *  }</pre>
 * <b>Example 2</b>
 * <pre>{@code
 *  SELECT a, b, c
 *  FROM t
 *  WHERE a = 3 AND ?p = true | p→false
 *  }</pre>
 * simplifies to:
 * <pre>{@code
 *  SELECT a, b, c
 *  FROM t
 *  WHERE false
 *  }</pre>
 * <b>Example 3</b>
 * <pre>{@code
 *  SELECT a, b, c
 *  FROM t
 *  WHERE (a = 3 AND ?p = true) AND (?q = 42) | p→true, q→null
 *  }</pre>
 * simplifies to:
 * <pre>{@code
 *  SELECT a, b, c
 *  FROM t
 *  WHERE null
 *  }</pre>
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "PMD.CompareObjectsWithEquals"})
public class QueryPredicateSimplificationRule extends ExplorationCascadesRule<SelectExpression> {

    @Nonnull
    private static final CollectionMatcher<QueryPredicate> predicateMatcher = atLeastOne(anyPredicate());
    @Nonnull
    private static final BindingMatcher<SelectExpression> rootMatcher = selectExpression(predicateMatcher, all(anyQuantifier()));

    public QueryPredicateSimplificationRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final var selectExpression = call.get(rootMatcher);
        final var predicates = call.get(predicateMatcher);
        final var conjunction = AndPredicate.and(predicates);
        final var aliasMap = AliasMap.emptyMap();
        final var constantAliases = Sets.difference(conjunction.getCorrelatedTo(),
                Quantifiers.aliases(selectExpression.getQuantifiers()));

        final var simplifiedConjunction = Simplification.optimize(conjunction, call.getEvaluationContext(), aliasMap,
                constantAliases, ConstantFoldingRuleSet.ofSimplificationRules());

        if (simplifiedConjunction.get().semanticEquals(conjunction, aliasMap)) {
            return;
        }

        final var resultValue = selectExpression.getResultValue();
        final var quantifier = selectExpression.getQuantifiers();
        final SelectExpression simplifiedSelectExpression;
        if (simplifiedConjunction instanceof AndPredicate) {
            simplifiedSelectExpression = new SelectExpression(resultValue, quantifier, ((AndPredicate)simplifiedConjunction).getChildren());
        } else {
            simplifiedSelectExpression = new SelectExpression(resultValue, quantifier, ImmutableList.of(simplifiedConjunction.get()));
        }

        call.yieldExploratoryExpression(simplifiedSelectExpression);
    }
}
