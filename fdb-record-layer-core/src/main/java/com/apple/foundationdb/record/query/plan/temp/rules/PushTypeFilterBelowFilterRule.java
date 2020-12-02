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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicateFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * A rule that moves a {@link RecordQueryTypeFilterPlan} below a {@link RecordQueryFilterPlan}. While this doesn't make
 * a difference in terms of plan semantics it ensures that the generated plans have the same form as those produced by
 * the {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}.
 *
 * <pre>
 * {@code
 *             +-----------------------------+                +----------------------------------+
 *             |                             |                |                                  |
 *             |  RecordQueryTypeFilterPlan  |                |  RecordQueryPredicateFilterPlan  |
 *             |                             |                |                           pred'  |
 *             +-------------+---------------+                |                                  |
 *                           |                                +-----------------+----------------+
 *                           |                      +----->                     |
 *                           |                                                  | newQun
 *           +---------------+------------------+                               |
 *           |                                  |             +-----------------+---------------+
 *           |  RecordQueryPredicateFilterPlan  |             |                                 |
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
public class PushTypeFilterBelowFilterRule extends PlannerRule<RecordQueryTypeFilterPlan> {
    private static final ExpressionMatcher<ExpressionRef<? extends RelationalExpression>> innerMatcher = ReferenceMatcher.anyRef();
    private static ExpressionMatcher<Quantifier.Physical> qunMatcher = QuantifierMatcher.physical(innerMatcher);
    private static final ExpressionMatcher<QueryPredicate> predMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<RecordQueryPredicateFilterPlan> filterPlanMatcher =
            TypeWithPredicateMatcher.ofPredicate(RecordQueryPredicateFilterPlan.class, predMatcher, qunMatcher);
    private static QuantifierMatcher<Quantifier.Physical> filterPlanQuantifierMatcher = QuantifierMatcher.physical(filterPlanMatcher);
    private static final ExpressionMatcher<RecordQueryTypeFilterPlan> root =
            TypeMatcher.of(RecordQueryTypeFilterPlan.class, filterPlanQuantifierMatcher);

    public PushTypeFilterBelowFilterRule() {
        super(root);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RecordQueryPlan> inner = (ExpressionRef<RecordQueryPlan>)call.get(innerMatcher);
        final Quantifier.Physical qun = call.get(qunMatcher);
        final QueryPredicate pred = call.get(predMatcher);
        final Collection<String> recordTypes = call.get(root).getRecordTypes();

        final RecordQueryTypeFilterPlan newTypeFilterPlan = new RecordQueryTypeFilterPlan(Quantifier.physical(inner), recordTypes);
        final Quantifier.Physical newQun = Quantifier.physical(call.ref(newTypeFilterPlan));
        final QueryPredicate rebasedPred = pred.rebase(Quantifiers.translate(qun, newQun));

        call.yield(GroupExpressionRef.of(
                new RecordQueryPredicateFilterPlan(
                        newQun,
                        rebasedPred)));
    }
}
