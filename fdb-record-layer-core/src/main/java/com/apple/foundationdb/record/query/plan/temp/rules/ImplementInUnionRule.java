/*
 * ImplementInUnionRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlanBase;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.Ordering;
import com.apple.foundationdb.record.query.plan.temp.OrderingAttribute;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty;
import com.apple.foundationdb.record.query.predicates.LiteralValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifierOverPlans;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.explodeExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.selectExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.SetMatcher.exactlyInAnyOrder;

/**
 * A rule that implements a SELECT over a VALUES and a correlated subexpression as a {@link RecordQueryInUnionPlan}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementInUnionRule extends PlannerRule<SelectExpression> {
    private static final CollectionMatcher<RecordQueryPlan> innerPlansMatcher = some(RecordQueryPlanMatchers.anyPlan());
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverPlans(innerPlansMatcher);
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final BindingMatcher<Quantifier.ForEach> explodeQuantifierMatcher = forEachQuantifier(explodeExpressionMatcher);

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(exactlyInAnyOrder(innerQuantifierMatcher, explodeQuantifierMatcher));

    public ImplementInUnionRule() {
        super(root, ImmutableSet.of(OrderingAttribute.ORDERING));
    }

    @SuppressWarnings({"unchecked", "java:S135"})
    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlanContext context = call.getContext();

        final PlannerBindings bindings = call.getBindings();

        final Optional<Set<Ordering>> requiredOrderingsOptional = call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (!requiredOrderingsOptional.isPresent()) {
            return;
        }

        final Set<Ordering> requiredOrderings = requiredOrderingsOptional.get();

        final KeyExpression commonPrimaryKey = context.getCommonPrimaryKey();
        if (commonPrimaryKey == null) {
            return;
        }

        final SelectExpression selectExpression = bindings.get(root);
        if (!selectExpression.getPredicates().isEmpty()) {
            return;
        }
        final List<? extends Value> resultValues = selectExpression.getResultValues();
        final Quantifier.ForEach innerQuantifier = bindings.get(innerQuantifierMatcher);
        if (resultValues.size() != 1) {
            return;
        }
        final Value onlyResultValue = Iterables.getOnlyElement(resultValues);
        if (!(onlyResultValue instanceof QuantifiedColumnValue) ||
                !((QuantifiedColumnValue)onlyResultValue).getAlias().equals(innerQuantifier.getAlias())) {
            return;
        }

        final ExplodeExpression explodeExpression = bindings.get(explodeExpressionMatcher);
        final List<? extends Value> explodeResultValues = explodeExpression.getResultValues();
        if (explodeResultValues.size() != 1) {
            return;
        }
        final Value explodeValue = Iterables.getOnlyElement(explodeResultValues);

        final Quantifier.ForEach explodeQuantifier = bindings.get(explodeQuantifierMatcher);
        final Collection<? extends RecordQueryPlan> recordQueryPlans = bindings.get(innerPlansMatcher);

        final Map<Ordering, ImmutableList<RecordQueryPlan>> groupedByOrdering =
                recordQueryPlans
                        .stream()
                        .flatMap(plan -> {
                            final Optional<Ordering> orderingForLegOptional =
                                    OrderingProperty.evaluate(plan, context).map(OrderingProperty.OrderingInfo::getOrdering);

                            return orderingForLegOptional
                                    .map(ordering -> Stream.of(Pair.of(ordering, plan)))
                                    .orElse(Stream.of());
                        })
                        .collect(Collectors.groupingBy(Pair::getLeft,
                                Collectors.mapping(Pair::getRight,
                                        ImmutableList.toImmutableList())));

        //
        // Create the source for the in-union plan
        //
        final RecordQueryInUnionPlan.InValuesSource inValuesSource;
        if (explodeValue instanceof LiteralValue<?>) {
            final Object literalValue = ((LiteralValue<?>)explodeValue).getLiteralValue();
            if (literalValue instanceof List<?>) {
                inValuesSource = new RecordQueryInUnionPlan.InValues(explodeQuantifier.getAlias().getId(), (List<Object>)literalValue);
            } else {
                return;
            }
        } else if (explodeValue instanceof QuantifiedColumnValue) {
            inValuesSource = new RecordQueryInUnionPlan.InParameter(explodeQuantifier.getAlias().getId(), ((QuantifiedColumnValue)explodeValue).getAlias().getId());
        } else {
            return;
        }

        final int attemptFailedInJoinAsUnionMaxSize = call.getContext().getPlannerConfiguration().getAttemptFailedInJoinAsUnionMaxSize();

        //
        // We currently only implement the case where there is exactly one equality-bound comparison in
        // the orderings of the plans that bind to the quantifier ranging over the explode.
        //
        for (final Map.Entry<Ordering, ImmutableList<RecordQueryPlan>> providedOrderingEntry : groupedByOrdering.entrySet()) {
            final GroupExpressionRef<RecordQueryPlan> newInnerPlanReference = GroupExpressionRef.from(providedOrderingEntry.getValue());

            for (final Ordering requiredOrdering : requiredOrderings) {
                final Ordering providedOrdering = providedOrderingEntry.getKey();
                KeyExpression matchingKeyExpression = null;
                for (Map.Entry<KeyExpression, Comparisons.Comparison> expressionComparisonEntry : providedOrdering.getEqualityBoundKeyMap().entries()) {
                    final Comparisons.Comparison comparison = expressionComparisonEntry.getValue();
                    if (comparison.getType() == Comparisons.Type.EQUALS && comparison instanceof Comparisons.ParameterComparison) {
                        final Comparisons.ParameterComparison parameterComparison = (Comparisons.ParameterComparison)comparison;
                        if (parameterComparison.getParameter().equals(explodeQuantifier.getAlias().getId())) {
                            matchingKeyExpression = expressionComparisonEntry.getKey();
                        }
                    }
                }
                if (matchingKeyExpression == null) {
                    continue;
                }

                // Compute a comparison key that satisfies the required ordering
                final Optional<Ordering> combinedOrderingOptional =
                        orderingForInUnion(providedOrdering, requiredOrdering, ImmutableSet.of(matchingKeyExpression));
                if (!combinedOrderingOptional.isPresent()) {
                    continue;
                }

                final Ordering combinedOrdering = combinedOrderingOptional.get();
                final List<KeyPart> orderingKeyParts = combinedOrdering.getOrderingKeyParts();

                final List<KeyExpression> orderingKeys =
                        orderingKeyParts
                                .stream()
                                .map(KeyPart::getNormalizedKeyExpression)
                                .collect(ImmutableList.toImmutableList());

                //
                // At this point we know we can implement the distinct union over the partitions of compatibly ordered plans
                //
                final KeyExpression comparisonKey =
                        orderingKeys.size() == 1
                        ? Iterables.getOnlyElement(orderingKeys) : Key.Expressions.concat(orderingKeys);

                final Quantifier.Physical newInnerQuantifier = Quantifier.physical(newInnerPlanReference);
                call.yield(call.ref(
                        new RecordQueryInUnionPlan(newInnerQuantifier,
                                ImmutableList.of(inValuesSource),
                                comparisonKey,
                                RecordQueryUnionPlanBase.isReversed(ImmutableList.of(newInnerQuantifier)),
                                attemptFailedInJoinAsUnionMaxSize)));
            }
        }
    }

    @SuppressWarnings("java:S135")
    public static Optional<Ordering> orderingForInUnion(@Nonnull Ordering providedOrdering,
                                                        @Nonnull Ordering requiredOrdering,
                                                        @Nonnull Set<KeyExpression> inBoundExpressions) {
        final Iterator<KeyPart> requiredOrderingIterator = requiredOrdering.getOrderingKeyParts().iterator();
        final ImmutableList.Builder<KeyPart> resultingOrderingKeyPartBuilder = ImmutableList.builder();
        for (final KeyPart providedKeyPart : providedOrdering.getOrderingKeyParts()) {
            KeyPart toBeAdded = providedKeyPart;
            while (requiredOrderingIterator.hasNext()) {
                final KeyPart requiredKeyPart = requiredOrderingIterator.next();
                if (requiredKeyPart.getNormalizedKeyExpression().equals(providedKeyPart.getNormalizedKeyExpression())) {
                    break;
                } else if (inBoundExpressions.contains(requiredKeyPart.getNormalizedKeyExpression())) {
                    resultingOrderingKeyPartBuilder.add(KeyPart.of(requiredKeyPart.getNormalizedKeyExpression(), ComparisonRange.Type.EMPTY));
                } else {
                    toBeAdded = null;
                    break;
                }
            }

            if (toBeAdded != null) {
                resultingOrderingKeyPartBuilder.add(toBeAdded);
            } else {
                return Optional.empty();
            }
        }

        final SetMultimap<KeyExpression, Comparisons.Comparison> resultEqualityBoundKeyMap = HashMultimap.create(providedOrdering.getEqualityBoundKeyMap());
        inBoundExpressions.forEach(resultEqualityBoundKeyMap::removeAll);

        return Optional.of(new Ordering(resultEqualityBoundKeyMap, resultingOrderingKeyPartBuilder.build()));
    }
}
