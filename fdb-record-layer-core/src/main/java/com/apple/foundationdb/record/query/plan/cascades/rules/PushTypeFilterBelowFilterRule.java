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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.predicatesFilter;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyRefOverOnlyPlans;

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
public class PushTypeFilterBelowFilterRule extends CascadesRule<RecordQueryTypeFilterPlan> {
    private static final BindingMatcher<? extends Reference> innerMatcher = anyRefOverOnlyPlans();
    private static final BindingMatcher<Quantifier.Physical> qunMatcher = physicalQuantifierOverRef(innerMatcher);
    private static final BindingMatcher<QueryPredicate> predMatcher = anyPredicate();
    private static final BindingMatcher<RecordQueryTypeFilterPlan> root =
            typeFilter(exactly(physicalQuantifier(predicatesFilter(all(predMatcher), exactly(qunMatcher)))));

    public PushTypeFilterBelowFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final Reference inner = bindings.get(innerMatcher);
        final Quantifier.Physical qun = bindings.get(qunMatcher);
        final List<? extends QueryPredicate> predicates = bindings.getAll(predMatcher);
        final Collection<String> recordTypes = bindings.get(root).getRecordTypes();

        final RecordQueryTypeFilterPlan newTypeFilterPlan = new RecordQueryTypeFilterPlan(Quantifier.physical(inner), recordTypes, new Type.Any());
        final Quantifier.Physical newQun = Quantifier.physical(call.memoizePlans(newTypeFilterPlan));
        final List<QueryPredicate> rebasedPredicates =
                predicates.stream()
                        .map(queryPredicate -> queryPredicate.rebase(Quantifiers.translate(qun, newQun)))
                        .collect(ImmutableList.toImmutableList());

        call.yieldExpression(new RecordQueryPredicatesFilterPlan(
                        newQun,
                        rebasedPredicates));
    }
}
