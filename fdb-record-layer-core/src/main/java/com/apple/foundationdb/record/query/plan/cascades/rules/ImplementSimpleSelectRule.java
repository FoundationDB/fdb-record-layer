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
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.rollUp;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that implements a select expression without predicates over a single {@link RecordQueryPlan} as a
 * {@link RecordQueryMapPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementSimpleSelectRule extends PlannerRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<PlanPartition> innerPlanPartitionMatcher = anyPlanPartition();

    @Nonnull
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> innerReferenceMatcher =
            planPartitions(rollUp(any(innerPlanPartitionMatcher)));

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(innerReferenceMatcher);

    @Nonnull
    private static final BindingMatcher<QueryPredicate> predicateMatcher = anyPredicate();

    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            selectExpression(all(predicateMatcher), exactly(innerQuantifierMatcher));

    public ImplementSimpleSelectRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var planPartition = bindings.get(innerPlanPartitionMatcher);

        final var predicates = bindings.getAll(predicateMatcher);
        Quantifier quantifier = bindings.get(innerQuantifierMatcher);
        var resultValue = selectExpression.getResultValue();
        var reference = GroupExpressionRef.from(planPartition.getPlans());

        final var isSimpleResultValue =
                resultValue instanceof QuantifiedObjectValue &&
                ((QuantifiedObjectValue)resultValue).getAlias().equals(quantifier.getAlias());

        if (predicates.isEmpty() &&
                isSimpleResultValue) {
            call.yield(reference);
            return;
        }

        if (!predicates.isEmpty()) {
            reference = GroupExpressionRef.of(new RecordQueryPredicatesFilterPlan(
                    Quantifier.physicalBuilder()
                            .withAlias(quantifier.getAlias())
                            .build(reference),
                    predicates.stream()
                            .map(QueryPredicate::toResidualPredicate)
                            .collect(ImmutableList.toImmutableList())));
        }

        if (!isSimpleResultValue) {
            final Quantifier.Physical beforeMapQuantifier;
            if (!predicates.isEmpty()) {
                final var lowerAlias = quantifier.getAlias();
                beforeMapQuantifier = Quantifier.physical(reference);
                resultValue = resultValue.rebase(AliasMap.of(lowerAlias, beforeMapQuantifier.getAlias()));
            } else {
                beforeMapQuantifier = Quantifier.physicalBuilder()
                        .withAlias(quantifier.getAlias())
                        .build(reference);
            }

            reference = GroupExpressionRef.of(
                    new RecordQueryMapPlan(beforeMapQuantifier,
                            resultValue));
        }

        call.yield(reference);
    }
}
