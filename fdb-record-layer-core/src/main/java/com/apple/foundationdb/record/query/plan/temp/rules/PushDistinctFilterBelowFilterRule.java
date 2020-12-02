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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicateFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;

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
 *     |  RecordQueryUnorderedPrimaryKeyDistinctPlan  |             |  RecordQueryPredicateFilterPlan  |
 *     |                                              |             |                           pred'  |
 *     +---------------------+------------------------+             |                                  |
 *                           |                                      +---------------+------------------+
 *                           |                        +----->                       |
 *                           |                                                      | newQun
 *           +---------------+------------------+                                   |
 *           |                                  |             +---------------------+------------------------+
 *           |  RecordQueryPredicateFilterPlan  |             |                                              |
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
    private static final ExpressionMatcher<ExpressionRef<? extends RelationalExpression>> innerMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<Quantifier.Physical> innerQuantifierMatcher = QuantifierMatcher.physical(innerMatcher);
    private static final ExpressionMatcher<RecordQueryPredicateFilterPlan> filterPlanMatcher =
            TypeMatcher.of(RecordQueryPredicateFilterPlan.class, innerQuantifierMatcher);
    private static final ExpressionMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> root =
            TypeMatcher.of(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, QuantifierMatcher.physical(filterPlanMatcher));

    public PushDistinctFilterBelowFilterRule() {
        super(root);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RecordQueryPlan> inner = (ExpressionRef<RecordQueryPlan>)call.get(innerMatcher);
        final Quantifier.Physical qun = call.get(innerQuantifierMatcher);
        final RecordQueryPredicateFilterPlan filterPlan = call.get(filterPlanMatcher);

        final RecordQueryUnorderedPrimaryKeyDistinctPlan newDistinctPlan =
                new RecordQueryUnorderedPrimaryKeyDistinctPlan(Quantifier.physical(inner));
        final Quantifier.Physical newQun = Quantifier.physical(call.ref(newDistinctPlan));
        final QueryPredicate rebasedPred =
                filterPlan.getPredicate()
                        .rebase(Quantifiers.translate(qun, newQun));
        call.yield(call.ref(
                new RecordQueryPredicateFilterPlan(newQun,
                        rebasedPred)));
    }
}
