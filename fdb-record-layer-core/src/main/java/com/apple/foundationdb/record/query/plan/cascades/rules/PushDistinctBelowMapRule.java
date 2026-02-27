/*
 * PushDistinctBelowMapRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyRefOverOnlyPlans;

/**
 * A rule that pushes a {@link RecordQueryUnorderedPrimaryKeyDistinctPlan} below a {@link RecordQueryMapPlan}.
 * This is a pinhole optimization, ensuring that we apply any distinct operations before applying any
 * map-operations.
 *
 * <pre>
 * {@code
 *         +-----------------------------------+                          +---------------------------------+
 *         |                                   |                          |                                 |
 *         |  UnorderedPrimaryKeyDistinctPlan  |                          |             MapPlan             |
 *         |                                   |                          |                                 |
 *         +-----------------+-----------------+                          +----------------+----------------+
 *                           |                                                             |
 *                           |                                                             |
 *                           |                    +------------------->                    |
 *           +---------------+--------------+                            +-----------------+-----------------+
 *           |                              |                            |                                   |
 *           |            MapPlan           |                            |  UnorderedPrimaryKeyDistinctPlan  |
 *           |                              |                            |                                   |
 *           +---------------+--------------+                            +-----------------+-----------------+
 *                           |                                                             |
 *                           |                                                             |
 *                           |                                                             |
 *                    +------+------+                                                      |
 *                    |             |                                                      |
 *                    |  innerPlan  |   +--------------------------------------------------+
 *                    |             |
 *                    +-------------+
 * }
 * </pre>
 *
 * <p>
 * Note that the {@link RecordQueryUnorderedPrimaryKeyDistinctPlan} uses the primary key that is baked
 * into the {@link com.apple.foundationdb.record.query.plan.plans.QueryResult}s that are retuned by its
 * underlying plan, and that that value is not modified by the map plan operator. One could imagine a different
 * distinct operator that took a {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} to
 * generate some kind of distinctness key. If we wanted to apply this rule to such an operator, we'd have to
 * be a little more careful. For one, we'd have to make sure to translate that value when pushing it down. But
 * moreover, we'd need to make sure that the map value is one-to-one, or more precisely, that it does not change
 * the number of distinct values.
 * </p>
 *
 * @see PushDistinctBelowFilterRule for a similar rule operating on filter plans
 * @see PushDistinctThroughFetchRule for a similar rule operating on fetch plans
 */
public class PushDistinctBelowMapRule extends ImplementationCascadesRule<RecordQueryUnorderedPrimaryKeyDistinctPlan> {
    @Nonnull
    private static final BindingMatcher<? extends Reference> innerRefMatcher = anyRefOverOnlyPlans();
    @Nonnull
    private static final BindingMatcher<Quantifier.Physical> innerQuantifierMatcher = physicalQuantifierOverRef(innerRefMatcher);
    @Nonnull
    private static final BindingMatcher<RecordQueryMapPlan> mapPlanMatcher = RecordQueryPlanMatchers.map(exactly(innerQuantifierMatcher));
    @Nonnull
    private static final BindingMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> root =
            RecordQueryPlanMatchers.unorderedPrimaryKeyDistinct(exactly(physicalQuantifier(mapPlanMatcher)));

    public PushDistinctBelowMapRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final Reference inner = call.get(innerRefMatcher);
        final Quantifier.Physical qun = call.get(innerQuantifierMatcher);
        final RecordQueryMapPlan mapPlan = call.get(mapPlanMatcher);

        // Reverse the order of the map and the unordered primary key distinct plan and yield
        final RecordQueryUnorderedPrimaryKeyDistinctPlan newDistinctPlan =
                new RecordQueryUnorderedPrimaryKeyDistinctPlan(Quantifier.physical(inner));
        final Quantifier.Physical newQun = Quantifier.physical(call.memoizePlan(newDistinctPlan));
        call.yieldPlan(new RecordQueryMapPlan(newQun, mapPlan.getResultValue().rebase(Quantifiers.translate(qun, newQun))));
    }
}
