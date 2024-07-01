/*
 * NormalizePredicatesRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;

/**
 * Rule to create the conjunctive normal form of the predicates in a {@link SelectExpression}.
 * Note that this rule creates the precursor to all explorations done in {@link PredicateToLogicalUnionRule}.
 * It is possible that the CNF is deemed too complex to be created (see also
 * {@link RecordQueryPlannerConfiguration#getComplexityThreshold()}) which will then cause this rule to be
 * unproductive which in turn causes only incomplete exploration of the possible predicates space in
 * {@link PredicateToLogicalUnionRule}.
 */
@API(API.Status.EXPERIMENTAL)
public class NormalizePredicatesRule extends CascadesRule<SelectExpression> {
    private static final CollectionMatcher<QueryPredicate> predicatesMatcher = all(anyPredicate());
    private static final CollectionMatcher<Quantifier> innerQuantifiersMatcher = all(anyQuantifier());

    private static final BindingMatcher<SelectExpression> root =
            RelationalExpressionMatchers.selectExpression(predicatesMatcher, innerQuantifiersMatcher);

    public NormalizePredicatesRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final SelectExpression selectExpression = bindings.get(root);
        final Collection<? extends QueryPredicate> predicates = bindings.get(predicatesMatcher);
        final Collection<? extends Quantifier> quantifiers = bindings.get(innerQuantifiersMatcher);

        // create one big conjuncted predicate
        final QueryPredicate conjunctedPredicate = AndPredicate.and(predicates);

        final BooleanPredicateNormalizer cnfNormalizer = BooleanPredicateNormalizer.forConfiguration(
                BooleanPredicateNormalizer.Mode.CNF,
                call.getContext().getPlannerConfiguration());

        cnfNormalizer.normalizeAndSimplify(conjunctedPredicate, false)
                .ifPresent(cnfPredicate ->
                        call.yieldExpression(new SelectExpression(selectExpression.getResultValue(),
                                quantifiers.stream().map(quantifier -> quantifier.toBuilder().build(quantifier.getRangesOver())).collect(ImmutableList.toImmutableList()),
                                AndPredicate.conjuncts(cnfPredicate))));
    }
}
