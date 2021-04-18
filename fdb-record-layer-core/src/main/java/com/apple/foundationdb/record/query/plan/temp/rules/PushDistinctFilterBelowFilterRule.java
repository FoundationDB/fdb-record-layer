/*
 * PushDistinctFilterBelowFilterRule.java
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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.physicalQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatchers.anyRefOverOnlyPlans;

/**
 * A rule that moves a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan}
 * below a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan}. While this doesn't make a
 * difference in terms of plan semantics it ensures that the generated plans have the same form as those produced by
 * the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}.
 *
 * <pre>
 * {@code
 *     +----------------------------------------------+             +----------------------------------+
 *     |                                              |             |                                  |
 *     |  RecordQueryUnorderedPrimaryKeyDistinctPlan  |             |  RecordQueryPredicatesFilterPlan |
 *     |                                              |             |                           pred'  |
 *     +---------------------+------------------------+             |                                  |
 *                           |                                      +---------------+------------------+
 *                           |                        +----->                       |
 *                           |                                                      | newQun
 *           +---------------+------------------+                                   |
 *           |                                  |             +---------------------+------------------------+
 *           |  RecordQueryPredicatesFilterPlan |             |                                              |
 *           |                            pred  |             |  RecordQueryUnorderedPrimaryKeyDistinctPlan  |
 *           |                                  |             |                                              |
 *           +---------------+------------------+             +---------------------+------------------------+
 *                           |                                                    /
 *                           | qun                                               /
 *                           |                                                  /
 *                    +------+------+                                          /
 *                    |             |                                         /
 *                    |   any ref   | ---------------------------------------+
 *                    |             |
 *                    +-------------+
 * }
 * </pre>
 *
 * where pred' is rebased along the translation from qun to newQun.
 */
public class PushDistinctFilterBelowFilterRule extends PlannerRule<RecordQueryUnorderedPrimaryKeyDistinctPlan> {
    @Nonnull
    private static final BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> innerRefMatcher = anyRefOverOnlyPlans();
    @Nonnull
    private static final BindingMatcher<Quantifier.Physical> innerQuantifierMatcher = physicalQuantifierOverRef(innerRefMatcher);
    @Nonnull
    private static final BindingMatcher<RecordQueryPredicatesFilterPlan> filterPlanMatcher = RecordQueryPredicatesFilterPlan.predicatesFilter(exactly(innerQuantifierMatcher));
    @Nonnull
    private static final BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> root =
            RecordQueryUnorderedPrimaryKeyDistinctPlan.unorderedPrimaryKeyDistinct(exactly(physicalQuantifier(filterPlanMatcher)));

    public PushDistinctFilterBelowFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<? extends RelationalExpression> inner = call.get(innerRefMatcher);
        final Quantifier.Physical qun = call.get(innerQuantifierMatcher);
        final RecordQueryPredicatesFilterPlan filterPlan = call.get(filterPlanMatcher);

        final RecordQueryUnorderedPrimaryKeyDistinctPlan newDistinctPlan =
                new RecordQueryUnorderedPrimaryKeyDistinctPlan(Quantifier.physical(inner));
        final Quantifier.Physical newQun = Quantifier.physical(call.ref(newDistinctPlan));
        final List<QueryPredicate> rebasedPredicates =
                filterPlan.getPredicates()
                        .stream()
                        .map(queryPredicate -> queryPredicate.rebase(Quantifiers.translate(qun, newQun)))
                        .collect(ImmutableList.toImmutableList());
        call.yield(call.ref(
                new RecordQueryPredicatesFilterPlan(newQun,
                        rebasedPredicates)));
    }
}
