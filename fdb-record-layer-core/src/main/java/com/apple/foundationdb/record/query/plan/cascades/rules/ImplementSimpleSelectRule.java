/*
 * ImplementSimpleSelectRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFirstOrDefaultPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that implements a select expression without predicates over a single partition as a
 * {@link RecordQueryMapPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementSimpleSelectRule extends CascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<Reference> innerReferenceMatcher =
            planPartitions(any(innerPlanPartitionMatcher));

    @Nonnull
    private static final BindingMatcher<Quantifier> innerQuantifierMatcher = anyQuantifierOverRef(innerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<QueryPredicate> predicateMatcher = anyPredicate();

    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(all(predicateMatcher), exactly(innerQuantifierMatcher));

    public ImplementSimpleSelectRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var planPartition = bindings.get(innerPlanPartitionMatcher);
        final var innerReference = bindings.get(innerReferenceMatcher);
        final var quantifier = bindings.get(innerQuantifierMatcher);
        final var predicates = bindings.getAll(predicateMatcher);

        var resultValue = selectExpression.getResultValue();
        var referenceBuilder = call.memoizeMemberPlansBuilder(innerReference, planPartition.getPlans());

        final var isSimpleResultValue =
                resultValue instanceof QuantifiedObjectValue &&
                ((QuantifiedObjectValue)resultValue).getAlias().equals(quantifier.getAlias());

        if (quantifier instanceof Quantifier.Existential) {
            referenceBuilder = call.memoizePlansBuilder(
                    new RecordQueryFirstOrDefaultPlan(
                            Quantifier.physicalBuilder()
                                    .withAlias(quantifier.getAlias())
                                    .build(referenceBuilder.reference()),
                            new NullValue(quantifier.getFlowedObjectType())));
        } else if (quantifier instanceof Quantifier.ForEach && ((Quantifier.ForEach)quantifier).isNullOnEmpty()) {
            referenceBuilder = call.memoizePlansBuilder(
                    new RecordQueryDefaultOnEmptyPlan(
                            Quantifier.physicalBuilder()
                                    .withAlias(quantifier.getAlias())
                                    .build(referenceBuilder.reference()),
                            new NullValue(quantifier.getFlowedObjectType())));
        }

        final var nonTautologyPredicates =
                predicates.stream()
                        .filter(predicate -> !predicate.isTautology())
                        .collect(ImmutableList.toImmutableList());
        if (nonTautologyPredicates.isEmpty() &&
                isSimpleResultValue) {
            call.yieldExpression(referenceBuilder.members());
            return;
        }

        if (!nonTautologyPredicates.isEmpty()) {
            referenceBuilder = call.memoizePlansBuilder(
                    new RecordQueryPredicatesFilterPlan(
                            Quantifier.physicalBuilder()
                                    .withAlias(quantifier.getAlias())
                                    .build(referenceBuilder.reference()),
                            nonTautologyPredicates.stream()
                                    .map(QueryPredicate::toResidualPredicate)
                                    .collect(ImmutableList.toImmutableList())));
        }

        if (!isSimpleResultValue) {
            final Quantifier.Physical beforeMapQuantifier;
            if (!nonTautologyPredicates.isEmpty()) {
                final var lowerAlias = quantifier.getAlias();
                beforeMapQuantifier = Quantifier.physical(referenceBuilder.reference());
                resultValue = resultValue.rebase(AliasMap.ofAliases(lowerAlias, beforeMapQuantifier.getAlias()));
            } else {
                beforeMapQuantifier = Quantifier.physicalBuilder()
                        .withAlias(quantifier.getAlias())
                        .build(referenceBuilder.reference());
            }

            referenceBuilder = call.memoizePlansBuilder(new RecordQueryMapPlan(beforeMapQuantifier, resultValue));
        }

        call.yieldExpression(referenceBuilder.members());
    }
}
