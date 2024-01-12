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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule.PhysicalOptimizationRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordPlanWithFetch;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.apple.foundationdb.record.query.plan.plans.TranslateValueFunction;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.physicalQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.withAnyFetchPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.ofTypeOwning;

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
public class PushSetOperationThroughFetchRule<P extends RecordQuerySetPlan> extends CascadesRule<P> implements PhysicalOptimizationRule {
    @Nonnull
    private static final BindingMatcher<RecordPlanWithFetch> fetchPlanMatcher = withAnyFetchPlan();

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
    public void onMatch(@Nonnull final CascadesRuleCall call) {
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

        final List<? extends RecordPlanWithFetch> fetchPlans = bindings.getAll(fetchPlanMatcher);
        final ImmutableList<TranslateValueFunction> dependentFunctions =
                fetchPlans.stream()
                        .map(RecordPlanWithFetch::getPushValueFunction)
                        .collect(ImmutableList.toImmutableList());

        Verify.verify(quantifiersOverFetches.size() == fetchPlans.size());
        Verify.verify(fetchPlans.size() == dependentFunctions.size());

        final CorrelationIdentifier sourceAlias = Quantifier.uniqueID();

        final List<? extends Value> requiredValues =
                setOperationPlan.getRequiredValues(sourceAlias, Quantifiers.getFlowedTypeForSetOperation(quantifiersOverFetches));
        final Set<CorrelationIdentifier> pushableAliasesByValue =
                setOperationPlan.tryPushValues(dependentFunctions, quantifiersOverFetches, requiredValues, sourceAlias);

        final ImmutableSet.Builder<CorrelationIdentifier> pushableAliasesBuilder = ImmutableSet.builder();
        final ImmutableList.Builder<Quantifier.Physical> pushableQuantifiersBuilder = ImmutableList.builder();
        final ImmutableList.Builder<RecordQueryPlan> withoutFetchPlansBuilder = ImmutableList.builder();
        final ImmutableList.Builder<TranslateValueFunction> pushableDependentFunctionsBuilder = ImmutableList.builder();
        RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords fetchIndexRecords = null;
        for (int i = 0; i < quantifiersOverFetches.size(); i++) {
            final Quantifier.Physical quantifier = quantifiersOverFetches.get(i);
            final CorrelationIdentifier alias = quantifier.getAlias();
            if (pushableAliasesByValue.contains(alias)) {
                final RecordPlanWithFetch fetchPlan = fetchPlans.get(i);
                final Optional<RecordQueryPlan> withoutFetchPlanOptional = fetchPlan.removeFetchMaybe();
                if (withoutFetchPlanOptional.isPresent()) {
                    pushableAliasesBuilder.add(alias);
                    pushableQuantifiersBuilder.add(quantifier);
                    withoutFetchPlansBuilder.add(withoutFetchPlanOptional.get());
                    pushableDependentFunctionsBuilder.add(dependentFunctions.get(i));

                    if (fetchIndexRecords == null) {
                        fetchIndexRecords = fetchPlan.getFetchIndexRecords();
                    } else {
                        if (fetchIndexRecords != fetchPlan.getFetchIndexRecords()) {
                            return;
                        }
                    }
                }
            }
        }

        final ImmutableSet<CorrelationIdentifier> pushableAliases = pushableAliasesBuilder.build();
        final ImmutableList<Quantifier.Physical> pushableQuantifiers = pushableQuantifiersBuilder.build();
        final ImmutableList<RecordQueryPlan> withoutFetchPlans = withoutFetchPlansBuilder.build();
        final ImmutableList<TranslateValueFunction> pushableDependentFunctions = pushableDependentFunctionsBuilder.build();

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

        final ImmutableList<Quantifier.Physical> nonPushableQuantifiers =
                setOperationPlan.getQuantifiers()
                        .stream()
                        .map(quantifier -> (Quantifier.Physical)quantifier)
                        .filter(quantifier -> !pushableAliases.contains(quantifier.getAlias()))
                        .collect(ImmutableList.toImmutableList());

        final List<? extends ExpressionRef<? extends RecordQueryPlan>> newPushedInnerPlans =
                withoutFetchPlans
                        .stream()
                        .map(call::memoizePlans)
                        .collect(ImmutableList.toImmutableList());

        Verify.verify(pushableQuantifiers.size() + nonPushableQuantifiers.size() == setOperationPlan.getQuantifiers().size());

        final TranslateValueFunction combinedTranslateValueFunction = setOperationPlan.pushValueFunction(pushableDependentFunctions);

        final RecordQuerySetPlan newSetOperationPlan = setOperationPlan.withChildrenReferences(newPushedInnerPlans);
        final RecordQueryFetchFromPartialRecordPlan newFetchPlan =
                new RecordQueryFetchFromPartialRecordPlan(Quantifier.physical(call.memoizePlans(newSetOperationPlan)),
                        combinedTranslateValueFunction,
                        Type.Relation.scalarOf(setOperationPlan.getResultType()),
                        Verify.verifyNotNull(fetchIndexRecords));

        if (nonPushableQuantifiers.isEmpty()) {
            call.yield(newFetchPlan);
        } else {
            final List<ExpressionRef<? extends RecordQueryPlan>> newFetchPlanAndResidualInners =
                    Streams.concat(Stream.of(call.memoizePlans(newFetchPlan)),
                            nonPushableQuantifiers
                                    .stream()
                                    .map(Quantifier.Physical::getRangesOver)
                                    .map(RecordQueryPlan::narrowReference))
                            .collect(ImmutableList.toImmutableList());
            call.yield(setOperationPlan.withChildrenReferences(newFetchPlanAndResidualInners));
        }
    }
}
