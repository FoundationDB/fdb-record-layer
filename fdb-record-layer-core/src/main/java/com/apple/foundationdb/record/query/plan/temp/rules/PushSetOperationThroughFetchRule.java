/*
 * PushSetOperationThroughFetchRule.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetOperationPlan;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.MultiChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

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
 * @param <P> type parameter for the particular kind of set operation to match
 *
 */
@API(API.Status.EXPERIMENTAL)
public class PushSetOperationThroughFetchRule<P extends RecordQuerySetOperationPlan> extends PlannerRule<P> {
    @Nonnull
    private static final ExpressionMatcher<RecordQueryFetchFromPartialRecordPlan> fetchPlanMatcher =
            TypeMatcher.of(RecordQueryFetchFromPartialRecordPlan.class, AnyChildrenMatcher.ANY);

    @Nonnull
    private static final ExpressionMatcher<Quantifier.Physical> quantifierOverFetchMatcher =
            QuantifierMatcher.physical(fetchPlanMatcher);

    @Nonnull
    private static <P extends RecordQuerySetOperationPlan> ExpressionMatcher<P> root(@Nonnull final Class<P> planClass) {
        return TypeMatcher.of(planClass, MultiChildrenMatcher.someMatching(quantifierOverFetchMatcher));
    }

    public PushSetOperationThroughFetchRule(@Nonnull final Class<P> planClass) {
        super(root(planClass));
    }

    @Override
    @SuppressWarnings("java:S1905")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final RecordQuerySetOperationPlan setOperationPlan = bindings.get(getMatcher());
        final List<Quantifier.Physical> quantifiersOverFetches = bindings.getAll(quantifierOverFetchMatcher);
        if (quantifiersOverFetches.size() <= 1) {
            // pulling up the fetch is meaningless in this case
            return;
        }

        final Set<CorrelationIdentifier> aliasesOverFetches =
                quantifiersOverFetches.stream()
                        .map(Quantifier::getAlias)
                        .collect(ImmutableSet.toImmutableSet());

        final ImmutableList<Quantifier.Physical> quantifiersOverNonFetches =
                setOperationPlan.getQuantifiers()
                        .stream()
                        .map(quantifier -> (Quantifier.Physical)quantifier)
                        .filter(quantifier -> !aliasesOverFetches.contains(quantifier.getAlias()))
                        .collect(ImmutableList.toImmutableList());

        final List<RecordQueryFetchFromPartialRecordPlan> fetchPlans = bindings.getAll(fetchPlanMatcher);

        final ImmutableList<? extends RecordQueryPlan> newPushedInnerPlans =
                fetchPlans
                        .stream()
                        .map(RecordQueryFetchFromPartialRecordPlan::getChild)
                        .collect(ImmutableList.toImmutableList());

        Verify.verify(quantifiersOverFetches.size() + quantifiersOverNonFetches.size() == setOperationPlan.getQuantifiers().size());

        final PushValueFunction combinedPushValueFunction = setOperationPlan.pushValueFunction(fetchPlans.stream()
                .map(RecordQueryFetchFromPartialRecordPlan::getPushValueFunction)
                .collect(ImmutableList.toImmutableList()));

        final boolean allRequiredValuesPushable =
                setOperationPlan.getRequiredValues(CorrelationIdentifier.uniqueID())
                        .stream()
                        .allMatch(requiredValue -> combinedPushValueFunction.pushValue(requiredValue, QuantifiedColumnValue.of(CorrelationIdentifier.uniqueID(), 0)).isPresent());
        if (!allRequiredValuesPushable) {
            return;
        }

        final RecordQuerySetOperationPlan newSetOperationPlan = setOperationPlan.withChildren(newPushedInnerPlans);
        final RecordQueryFetchFromPartialRecordPlan newFetchPlan =
                new RecordQueryFetchFromPartialRecordPlan(newSetOperationPlan,
                        combinedPushValueFunction);

        if (quantifiersOverNonFetches.isEmpty()) {
            call.yield(GroupExpressionRef.of(newFetchPlan));
        } else {
            final ImmutableList<RecordQueryPlan> newFetchPlanAndResidualInners =
                    Streams.concat(Stream.of(newFetchPlan),
                            quantifiersOverNonFetches
                                    .stream()
                                    .map(Quantifier.Physical::getRangesOverPlan))
                            .collect(ImmutableList.toImmutableList());
            call.yield(GroupExpressionRef.of(setOperationPlan.withChildren(newFetchPlanAndResidualInners)));
        }
    }
}
