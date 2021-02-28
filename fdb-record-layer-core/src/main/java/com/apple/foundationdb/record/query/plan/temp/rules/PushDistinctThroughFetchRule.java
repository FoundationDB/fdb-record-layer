/*
 * PushDistinctThroughFetchRule.java
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
import com.apple.foundationdb.record.query.plan.plans.PushValueFunction;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;

/**
 * A rule that pushes a set of predicates that can be evaluated on a partial record of keys and values that were
 * retrieved by an index scan underneath a {@link RecordQueryFetchFromPartialRecordPlan} in order to filter out records
 * not satisfying the pushed predicates prior to a potentially expensive fetch operation.
 *
 * A predicate can be pushed if and only if the predicate's leaves which (in the prior of this rule) refer to entities
 * in the fetched full record can be translated to counterparts in the index record. The translation is specific to
 * the kind of index and the therefore the kind of {@link com.apple.foundationdb.record.query.plan.temp.MatchCandidate}.
 *
 * This rule defers to a <em>push function</em> to translate values in an appropriate way. See
 * {@link PushValueFunction} for
 * details.
 *
 * A filter can have a number of predicates which can be separately pushed or not pushed, e.g. if the filter uses
 * {@code x > 5 AND y < 10} we may be able to push {@code x > 5} since {@code x} is covered by the underlying index, but
 * we cannot push {@code y < 10} since {@code y} is not covered by the index. We therefore partition all conjuncts of
 * the filter and try the maximum set of predicates which can be pushed.
 *
 * Once the classification of predicate conjuncts is done leading to a partitioning of all conjuncts into pushable and
 * not pushable (residual), we need to consider three distinct cases:
 * <ol>
 *   <li>pushed predicates list is empty -- don't yield anything and just return</li>
 *   <li>pushed predicates list is not empty, residual predicates list is empty; yield {@code FETCH(FILTER(inner, pushedPredicates))}</li>
 *   <li>pushed predicates list is not empty, but residual predicates list is also not empty; yield
 *       {@code FILTER(FETCH(FILTER(inner, pushedPredicates)), residualPredicates)}</li>
 * </ol>
 *
 * <pre>
 * <h2>Case 2</h2>
 * {@code
 *         +------------------------+                                +------------------------------+
 *         |                        |                                |                              |
 *         |  PredicatesFilterPlan  |                                |  FetchFromPartialRecordPlan  |
 *         |            predicates  |                                |                              |
 *         |                        |                                +---------------+--------------+
 *         +-----------+------------+                                                |
 *                     |                                                             |
 *                     |  overFetch                                                  |
 *                     |                    +------------------->                    |
 *     +---------------+--------------+                              +---------------+---------------+
 *     |                              |                              |                               |
 *     |  FetchFromPartialRecordPlan  |                              |  PredicatesFilterPlan         |
 *     |                              |                              |             pushedPredicates  |
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
 * <pre>
 * <h2>Case 3</h2>
 * {@code
 *         +------------------------+                               +---------------------------------+
 *         |                        |                               |                                 |
 *         |  PredicatesFilterPlan  |                               |  PredicatesFilterPlan           |
 *         |            predicates  |                               |             residualPredicates  |
 *         |                        |                               |                                 |
 *         +-----------+------------+                               +----------------+----------------+
 *                     |                                                             |
 *                     |  overFetch                                                  |
 *                     |                    +------------------->                    |
 *     +---------------+--------------+                              +---------------+--------------+
 *     |                              |                              |                              |
 *     |  FetchFromPartialRecordPlan  |                              |  FetchFromPartialRecordPlan  |
 *     |                              |                              |                              |
 *     +---------------+--------------+                              +---------------+--------------+
 *                     |                                                             |
 *                     |                                                             |
 *                     |                                                             |
 *                     |                                             +---------------+---------------+
 *                     |                                             |                               |
 *                     |                                             |  PredicatesFilterPlan         |
 *                     |                                             |             pushedPredicates  |
 *                     |                                             |                               |
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
 */
@API(API.Status.EXPERIMENTAL)
public class PushDistinctThroughFetchRule extends PlannerRule<RecordQueryUnorderedPrimaryKeyDistinctPlan> {
    @Nonnull
    private static final ExpressionMatcher<RecordQueryPlan> innerPlanMatcher = TypeMatcher.of(RecordQueryPlan.class, AnyChildrenMatcher.ANY);
    @Nonnull
    private static final ExpressionMatcher<RecordQueryFetchFromPartialRecordPlan> fetchPlanMatcher =
            TypeMatcher.of(RecordQueryFetchFromPartialRecordPlan.class, QuantifierMatcher.physical(innerPlanMatcher));
    @Nonnull
    private static final ExpressionMatcher<Quantifier.Physical> quantifierOverFetchMatcher =
            QuantifierMatcher.physical(fetchPlanMatcher);
    @Nonnull
    private static final ExpressionMatcher<RecordQueryUnorderedPrimaryKeyDistinctPlan> root =
            TypeMatcher.of(RecordQueryUnorderedPrimaryKeyDistinctPlan.class, quantifierOverFetchMatcher);

    public PushDistinctThroughFetchRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final RecordQueryFetchFromPartialRecordPlan fetchPlan = bindings.get(fetchPlanMatcher);
        final RecordQueryPlan innerPlan = bindings.get(innerPlanMatcher);
        
        final CorrelationIdentifier newInnerAlias = CorrelationIdentifier.uniqueID();

        // for case 2 and case 3 we can at least build a FILTER(inner, pushedPredicates) as that is
        // required both for case 2 nd 3

        final Quantifier.Physical newInnerQuantifier = Quantifier.physical(GroupExpressionRef.of(innerPlan), newInnerAlias);

        final RecordQueryUnorderedPrimaryKeyDistinctPlan pushedDistinctPlan =
                new RecordQueryUnorderedPrimaryKeyDistinctPlan(newInnerQuantifier);

        final RecordQueryFetchFromPartialRecordPlan newFetchPlan =
                new RecordQueryFetchFromPartialRecordPlan(pushedDistinctPlan, fetchPlan.getPushValueFunction());

        // case 2
        call.yield(call.ref(newFetchPlan));
    }
}
