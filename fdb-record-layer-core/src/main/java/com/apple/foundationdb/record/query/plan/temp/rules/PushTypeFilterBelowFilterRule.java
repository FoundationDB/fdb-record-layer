/*
 * PushTypeFilterBelowFilterRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.predicatesFilter;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatchers.anyRefOverOnlyPlans;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.physicalQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;

/**
 * A rule that moves a {@link RecordQueryTypeFilterPlan} below a {@link RecordQueryFilterPlan}. While this doesn't make
 * a difference in terms of plan semantics it ensures that the generated plans have the same form as those produced by
 * the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}.
 *
 * <pre>
 * {@code
 *             +-----------------------------+                +----------------------------------+
 *             |                             |                |                                  |
 *             |  RecordQueryTypeFilterPlan  |                |  RecordQueryPredicatesFilterPlan |
 *             |                             |                |                           pred'  |
 *             +-------------+---------------+                |                                  |
 *                           |                                +-----------------+----------------+
 *                           |                      +----->                     |
 *                           |                                                  | newQun
 *           +---------------+------------------+                               |
 *           |                                  |             +-----------------+---------------+
 *           |  RecordQueryPredicatesFilterPlan |             |                                 |
 *           |                            pred  |             |  RecordQueryTypeFilterPlanPlan  |
 *           |                                  |             |                                 |
 *           +---------------+------------------+             +-----------------+---------------+
 *                           |                                                  /
 *                           | qun                                             /
 *                           |                                                /
 *                    +------+------+                                        /
 *                    |             |                                       /
 *                    |   any ref   | -------------------------------------+
 *                    |             |
 *                    +-------------+
 * }
 * </pre>
 *
 * where pred' is rebased along the translation from qun to newQun.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushTypeFilterBelowFilterRule extends PlannerRule<RecordQueryTypeFilterPlan> {
    private static final BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> innerMatcher = anyRefOverOnlyPlans();
    private static final BindingMatcher<Quantifier.Physical> qunMatcher = physicalQuantifierOverRef(innerMatcher);
    private static final BindingMatcher<QueryPredicate> predMatcher = anyPredicate();
    private static final BindingMatcher<RecordQueryTypeFilterPlan> root =
            typeFilter(exactly(physicalQuantifier(predicatesFilter(all(predMatcher), exactly(qunMatcher)))));

    public PushTypeFilterBelowFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final ExpressionRef<? extends RelationalExpression> inner = bindings.get(innerMatcher);
        final Quantifier.Physical qun = bindings.get(qunMatcher);
        final List<? extends QueryPredicate> predicates = bindings.getAll(predMatcher);
        final Collection<String> recordTypes = bindings.get(root).getRecordTypes();

        final RecordQueryTypeFilterPlan newTypeFilterPlan = new RecordQueryTypeFilterPlan(Quantifier.physical(inner), recordTypes, new Type.Any());
        final Quantifier.Physical newQun = Quantifier.physical(call.ref(newTypeFilterPlan));
        final List<QueryPredicate> rebasedPredicates =
                predicates.stream()
                        .map(queryPredicate -> queryPredicate.rebase(Quantifiers.translate(qun, newQun)))
                        .collect(ImmutableList.toImmutableList());

        call.yield(GroupExpressionRef.of(
                new RecordQueryPredicatesFilterPlan(
                        newQun,
                        rebasedPredicates)));
    }
}
