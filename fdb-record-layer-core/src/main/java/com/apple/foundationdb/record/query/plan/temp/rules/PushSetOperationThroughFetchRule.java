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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.ofTypeOwning;

/**
 * A (prototype) rule that pushes a set operation (e.g. a union or intersect) through a fetch such that the number of
 * records is reduced prior to the fetch operation.
 * Special complications arise if the fetch for some reason cannot be pushed through all legs of the set operation.
 *
 * <pre>
 * Case 1. We can push through all fetches
 * {@code
 *                                              +------------------------+                                                                +------------------------------+
 *                                              |                        |                                                                |                              |
 *                                              |  RecordQuerySetPlan    |                                                                |  FetchFromPartialRecordPlan  |
 *                                              |                        |                                                                |                              |
 *                                              +-----------+------------+                                                                +---------------+--------------+
 *                                                          |                                                                                             |
 *                             -----------------------------+----------------------------------                                                           |
 *                           /  overFetch                   |                                   \                                                         |
 *     +---------------+--------------+     +---------------+--------------+     +---------------+--------------+                             +-----------+------------+
 *     |                              |     |                              |     |                              |                             |                        |
 *     |  FetchFromPartialRecordPlan  |     |  FetchFromPartialRecordPlan  | ... |  FetchFromPartialRecordPlan  |     ------------>           |  RecordQuerySetPlan    |
 *     |                              |     |                              |     |                              |                             |                        |
 *     +---------------+--------------+     +---------------+--------------+     +---------------+--------------+                             +-----------+------------+
 *                     |                                    |                                    |                                                        |
 *                     |                                    |                                    |                                   ---------------------+-----+
 *                     |                                    |                                    |                                  /                    /      |
 *              +------+------+                      +------+------+                      +------+------+                          /                    /       |
 *              |             |                      |             |                      |             |                         /                    /        |
 *              |  innerPlan  | <--                  |  innerPlan  | <--                  |  innerPlan  | <-----------------------                    /         |
 *              |             |    \                 |             |    \                 |             |                                            /          |
 *              +-------------+     \                +-------------+     \                +-------------+                                           /           |
 *                                   \                                    \                                                                        /            |
 *                                    \                                    ------------------------------------------------------------------------             |
 *                                     -------------------------------------------------------------------------------------------------------------------------+
 * }
 * </pre>
 *
 * <pre>
 * Case 2. We cannot push through all fetches and we have to split up the set operation
 * {@code
 *                                                                                                                                               +------------------------+
 *                                                                                                                                               |                        |
 *                                                                                                                                               |  RecordQuerySetPlan    |
 *                                                                                                                                               |                        |
 *                                                                                                                                               +-----------+------------+
 *                                                                                                                                                           |                                                                                                                                                                                                                                                                                     |
 *                                                                                                                       ------------------------------------+-------------------------------------
 *                                                                                                                     /                                     |                                     \
 *                                 +------------------------+                                         +---------------+--------------+       +---------------+--------------+       +---------------+--------------+
 *                                 |                        |                                         |                              |       |                              |       |                              |
 *                                 |  RecordQuerySetPlan    |                                         |  FetchFromPartialRecordPlan  |       |  FetchFromPartialRecordPlan  |  ...  |  FetchFromPartialRecordPlan  |
 *                                 |                        |                                         |                              |       |                              |       |                              |
 *                                 +-----------+------------+                                         +---------------+--------------+       +---------------+--------------+       +---------------+--------------+
 *                                             |                                                                      |                                      |                                      |
 *                             ----------------+----------------                                                      |                                     /                                      /
 *                           /  overFetch                       \                                                     |                                    /                                      /
 *     +---------------+--------------+          +---------------+--------------+                        +------------+-----------+                       /                                      /
 *     |                              |          |                              |                        |                        |                      /                                      /
 *     |  FetchFromPartialRecordPlan  |    ...   |  FetchFromPartialRecordPlan  |   ------------>        |  RecordQuerySetPlan    |                     /                                      /
 *     |                              |          |                              |                        |                        |                    /                                      /
 *     +---------------+--------------+          +---------------+--------------+                        +------------+-----------+                   /                                      /
 *                     |                                         |                                                    |                              /                                      /
 *                     |                                         |                                                    |                             /                                      /
 *                     |                                         |                                                    |                            /                                      /
 *              +------+------+                           +------+------+                                             |                           /                                      /
 *              |             |                           |             |                                             |                          /                                      /
 *              |  innerPlan  | <--                       |  innerPlan  | <-------------------------------------------+----------------------------------------------------------------
 *              |             |    \                      |             |                                            /
 *              +-------------+     \                     +-------------+                                           /
 *                                   \                                                                             /
 *                                    ----------------------------------------------------------------------------
 * }
 * </pre>
 *
 * @param <P> type parameter for the particular kind of set operation to match
 *
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushSetOperationThroughFetchRule<P extends RecordQuerySetPlan> extends PlannerRule<P> {
    @Nonnull
    private static final BindingMatcher<RecordQueryFetchFromPartialRecordPlan> fetchPlanMatcher =
            fetchFromPartialRecordPlan(anyPlan());

    @Nonnull
    private static final BindingMatcher<Quantifier.Physical> quantifierOverFetchMatcher =
            physicalQuantifier(fetchPlanMatcher);

    @Nonnull
    private static <P extends RecordQuerySetPlan> BindingMatcher<P> root(@Nonnull final Class<P> planClass) {
        return ofTypeOwning(planClass, some(quantifierOverFetchMatcher));
    }

    public PushSetOperationThroughFetchRule(@Nonnull final Class<P> planClass) {
        super(root(planClass));
    }

    @Override
    @SuppressWarnings("java:S1905")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();

        final RecordQuerySetPlan setOperationPlan = bindings.get(getMatcher());
        final List<? extends Quantifier.Physical> quantifiersOverFetches = bindings.getAll(quantifierOverFetchMatcher);

        // if set operation is dynamic all quantifiers must have fetches
        if (setOperationPlan.isDynamic()) {
            if (quantifiersOverFetches.size() < setOperationPlan.getQuantifiers().size()) {
                return;
            }
        } else {
            if (quantifiersOverFetches.size() <= 1) {
                // pulling up the fetch is meaningless in this case
                return;
            }
        }

        final List<? extends RecordQueryFetchFromPartialRecordPlan> fetchPlans = bindings.getAll(fetchPlanMatcher);
        final ImmutableList<TranslateValueFunction> dependentFunctions =
                fetchPlans.stream()
                        .map(RecordQueryFetchFromPartialRecordPlan::getPushValueFunction)
                        .collect(ImmutableList.toImmutableList());

        Verify.verify(quantifiersOverFetches.size() == fetchPlans.size());
        Verify.verify(fetchPlans.size() == dependentFunctions.size());

        final List<? extends Value> requiredValues = setOperationPlan.getRequiredValues(CorrelationIdentifier.uniqueID(), new Type.Any());
        final Set<CorrelationIdentifier> pushableAliases = setOperationPlan.tryPushValues(dependentFunctions, quantifiersOverFetches, requiredValues);

        // if set operation is dynamic all aliases must be pushable
        if (setOperationPlan.isDynamic()) {
            if (pushableAliases.size() < setOperationPlan.getQuantifiers().size()) {
                return;
            }
        } else {
            if (pushableAliases.size() <= 1) {
                // pulling up the fetch is meaningless in this case
                return;
            }
        }

        final ImmutableList.Builder<Quantifier.Physical> pushableQuantifiersBuilder = ImmutableList.builder();
        final ImmutableList.Builder<RecordQueryFetchFromPartialRecordPlan> pushableFetchPlansBuilder = ImmutableList.builder();
        final ImmutableList.Builder<TranslateValueFunction> pushableDependentFunctionsBuilder = ImmutableList.builder();
        for (int i = 0; i < quantifiersOverFetches.size(); i++) {
            final Quantifier.Physical quantifier = quantifiersOverFetches.get(i);
            if (pushableAliases.contains(quantifier.getAlias())) {
                pushableQuantifiersBuilder.add(quantifier);
                pushableFetchPlansBuilder.add(fetchPlans.get(i));
                pushableDependentFunctionsBuilder.add(dependentFunctions.get(i));
            }
        }

        final ImmutableList<Quantifier.Physical> pushableQuantifiers = pushableQuantifiersBuilder.build();
        final ImmutableList<RecordQueryFetchFromPartialRecordPlan> pushableFetchPlans = pushableFetchPlansBuilder.build();
        final ImmutableList<TranslateValueFunction> pushableDependentFunctions = pushableDependentFunctionsBuilder.build();

        final ImmutableList<Quantifier.Physical> nonPushableQuantifiers =
                setOperationPlan.getQuantifiers()
                        .stream()
                        .map(quantifier -> (Quantifier.Physical)quantifier)
                        .filter(quantifier -> !pushableAliases.contains(quantifier.getAlias()))
                        .collect(ImmutableList.toImmutableList());
        
        final List<? extends ExpressionRef<RecordQueryPlan>> newPushedInnerPlans =
                pushableFetchPlans
                        .stream()
                        .map(RecordQueryFetchFromPartialRecordPlan::getChild)
                        .map(GroupExpressionRef::of)
                        .collect(ImmutableList.toImmutableList());

        Verify.verify(pushableQuantifiers.size() + nonPushableQuantifiers.size() == setOperationPlan.getQuantifiers().size());

        final TranslateValueFunction combinedTranslateValueFunction = setOperationPlan.pushValueFunction(pushableDependentFunctions);

        final RecordQuerySetPlan newSetOperationPlan = setOperationPlan.withChildrenReferences(newPushedInnerPlans);
        final RecordQueryFetchFromPartialRecordPlan newFetchPlan =
                new RecordQueryFetchFromPartialRecordPlan(newSetOperationPlan,
                        combinedTranslateValueFunction);

        if (nonPushableQuantifiers.isEmpty()) {
            call.yield(GroupExpressionRef.of(newFetchPlan));
        } else {
            final List<ExpressionRef<? extends RecordQueryPlan>> newFetchPlanAndResidualInners =
                    Streams.concat(Stream.of(GroupExpressionRef.of(newFetchPlan)),
                            nonPushableQuantifiers
                                    .stream()
                                    .map(Quantifier.Physical::getRangesOver)
                                    .map(RecordQueryPlan::narrowReference))
                            .collect(ImmutableList.toImmutableList());
            call.yield(GroupExpressionRef.of(setOperationPlan.withChildrenReferences(newFetchPlanAndResidualInners)));
        }
    }
}
