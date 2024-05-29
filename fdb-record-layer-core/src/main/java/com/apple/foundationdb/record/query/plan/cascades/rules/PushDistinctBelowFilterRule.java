/*
 * PushDistinctBelowFilterRule.java
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

import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyRefOverOnlyPlans;

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
public class PushDistinctBelowFilterRule extends CascadesRule<RecordQueryUnorderedPrimaryKeyDistinctPlan> {
    @Nonnull
    private static final BindingMatcher<? extends Reference> innerRefMatcher = anyRefOverOnlyPlans();
    @Nonnull
    private static final BindingMatcher<Quantifier.Physical> innerQuantifierMatcher = physicalQuantifierOverRef(innerRefMatcher);
    @Nonnull
    private static final BindingMatcher<RecordQueryPredicatesFilterPlan> filterPlanMatcher = RecordQueryPlanMatchers.predicatesFilter(exactly(innerQuantifierMatcher));
    @Nonnull
    private static final BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> root =
            RecordQueryPlanMatchers.unorderedPrimaryKeyDistinct(exactly(physicalQuantifier(filterPlanMatcher)));

    public PushDistinctBelowFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final Reference inner = call.get(innerRefMatcher);
        final Quantifier.Physical qun = call.get(innerQuantifierMatcher);
        final RecordQueryPredicatesFilterPlan filterPlan = call.get(filterPlanMatcher);

        final RecordQueryUnorderedPrimaryKeyDistinctPlan newDistinctPlan =
                new RecordQueryUnorderedPrimaryKeyDistinctPlan(Quantifier.physical(inner));
        final Quantifier.Physical newQun = Quantifier.physical(call.memoizePlans(newDistinctPlan));
        final List<QueryPredicate> rebasedPredicates =
                filterPlan.getPredicates()
                        .stream()
                        .map(queryPredicate -> queryPredicate.rebase(Quantifiers.translate(qun, newQun)))
                        .collect(ImmutableList.toImmutableList());
        call.yieldFinalExpression(new RecordQueryPredicatesFilterPlan(newQun,
                        rebasedPredicates));
    }
}
