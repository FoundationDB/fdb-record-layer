/*
 * PushFilterThroughFetchRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInParameterJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInValuesJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithChild;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.ofTypeOwning;

/**
 * A rule that pushes a {@link RecordQueryInJoinPlan} through a {@link RecordQueryFetchFromPartialRecordPlan}.
 *
 * This rule defers to a <em>push function</em> to translate values in an appropriate way. See
 * {@link TranslateValueFunction} for
 * details.
 *
 * <pre>
 * Case 1
 * {@code
 *         +------------------------+                                +------------------------------+
 *         |                        |                                |                              |
 *         |  InJoinPlan            |                                |  FetchFromPartialRecordPlan  |
 *         |               sources  |                                |                              |
 *         |                        |                                +---------------+--------------+
 *         +-----------+------------+                                                |
 *                     |                                                             |
 *                     |  overFetch                                                  |
 *                     |                    +------------------->                    |
 *     +---------------+--------------+                              +---------------+---------------+
 *     |                              |                              |                               |
 *     |  FetchFromPartialRecordPlan  |                              | InJoinPlan                    |
 *     |                              |                              |                       source  |
 *     +---------------+--------------+                              |                               |
 *                     |                                             +----------------+--------------+
 *                     |                                                              |
 *                     |                                                              |
 *              +------+------+                                                       |
 *              |             |                                                       |
 *              |  innerPlan  |   +---------------------------------------------------+
 *              |             |
 *              +-------------+
 * }
 * </pre>
 *
 * @param <P> the type parameter of the actual in-join plan to match. Currently this rule is used for both
 *        {@link RecordQueryInValuesJoinPlan} and {@link RecordQueryInParameterJoinPlan}.
 */
@API(API.Status.EXPERIMENTAL)
public class PushInJoinThroughFetchRule<P extends RecordQueryInJoinPlan> extends CascadesRule<P> {
    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> innerPlanMatcher = anyPlan();
    @Nonnull
    private static final BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchPlanMatcher =
            RecordQueryPlanMatchers.fetchFromPartialRecordPlan(innerPlanMatcher);
    @Nonnull
    private static final BindingMatcher<Quantifier.Physical> quantifierOverFetchMatcher =
            physicalQuantifier(fetchPlanMatcher);

    @Nonnull
    private static <P extends RecordQueryInJoinPlan> BindingMatcher<P> root(@Nonnull final Class<P> planClass) {
        return ofTypeOwning(planClass, any(quantifierOverFetchMatcher));
    }

    public PushInJoinThroughFetchRule(@Nonnull final Class<P> planClass) {
        super(root(planClass));
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final RecordQueryInJoinPlan inJoinPlan = bindings.get(getMatcher());
        final RecordQueryFetchFromPartialRecordPlan fetchPlan = bindings.get(fetchPlanMatcher);
        final RecordQueryPlan innerPlan = bindings.get(innerPlanMatcher);

        final RecordQueryPlanWithChild pushedInJoinPlan = inJoinPlan.withChild(call.memoizePlans(innerPlan));

        final var newFetchPlan =
                new RecordQueryFetchFromPartialRecordPlan(Quantifier.physical(call.memoizePlans(pushedInJoinPlan)),
                        fetchPlan.getPushValueFunction(),
                        Type.Relation.scalarOf(fetchPlan.getResultType()), fetchPlan.getFetchIndexRecords());

        call.yield(newFetchPlan);
    }
}
