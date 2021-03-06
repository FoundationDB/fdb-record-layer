/*
 * ImplementDistinctUnionRule.java
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.CrossProduct;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.Ordering;
import com.apple.foundationdb.record.query.plan.temp.OrderingAttribute;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty.OrderingInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatchers.references;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.logicalDistinctExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.logicalUnionExpression;

/**
 * A rule that implements a distinct union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnionExpression} and create a
 * {@link RecordQueryUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementDistinctUnionRule extends PlannerRule<LogicalDistinctExpression> {

    @Nonnull
    private static final CollectionMatcher<RecordQueryPlan> unionLegPlansMatcher = some(RecordQueryPlanMatchers.anyPlan());

    @Nonnull
    private static final BindingMatcher<LogicalUnionExpression> unionExpressionMatcher =
            logicalUnionExpression(all(forEachQuantifierOverRef(references(unionLegPlansMatcher))));

    @Nonnull
    private static final BindingMatcher<LogicalDistinctExpression> root =
            logicalDistinctExpression(exactly(forEachQuantifier(unionExpressionMatcher)));

    public ImplementDistinctUnionRule() {
        super(root, ImmutableSet.of(OrderingAttribute.ORDERING));
    }

    @Override
    @SuppressWarnings("java:S135")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlanContext context = call.getContext();

        final Optional<Set<Ordering>> requiredOrderingsOptional = call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (!requiredOrderingsOptional.isPresent()) {
            return;
        }
        final Set<Ordering> requiredOrderings = requiredOrderingsOptional.get();

        final KeyExpression commonPrimaryKey = context.getCommonPrimaryKey();
        if (commonPrimaryKey == null) {
            return;
        }
        final List<KeyExpression> commonPrimaryKeyParts = commonPrimaryKey.normalizeKeyForPositions();

        final PlannerBindings bindings = call.getBindings();

        final List<? extends Collection<? extends RecordQueryPlan>> plansByQuantifier = bindings.getAll(unionLegPlansMatcher);

        // group each leg's plans by their "sorted-ness"

        final ImmutableList<Set<Map.Entry<Ordering, ImmutableList<RecordQueryPlan>>>> plansByQuantifierSortedness =
                plansByQuantifier
                        .stream()
                        .map(plansForQuantifier -> {
                            final Map<Ordering, ImmutableList<RecordQueryPlan>> groupedBySortedness =
                                    plansForQuantifier
                                            .stream()
                                            .flatMap(plan -> {
                                                final Optional<Ordering> orderingForLegOptional =
                                                        OrderingProperty.evaluate(plan, context).map(OrderingInfo::getOrdering);

                                                return orderingForLegOptional
                                                        .map(ordering -> Stream.of(Pair.of(ordering, plan)))
                                                        .orElse(Stream.of());
                                            })
                                            .collect(Collectors.groupingBy(Pair::getLeft,
                                                    Collectors.mapping(Pair::getRight,
                                                            ImmutableList.toImmutableList())));
                            return groupedBySortedness.entrySet();
                        }).collect(ImmutableList.toImmutableList());

        for (final List<Map.Entry<Ordering, ImmutableList<RecordQueryPlan>>> entries : CrossProduct.crossProduct(plansByQuantifierSortedness)) {
            final ImmutableList<Optional<Ordering>> orderingOptionals =
                    entries.stream()
                            .map(entry ->
                                    Optional.of(entry.getKey()))
                            .collect(ImmutableList.toImmutableList());

            for (final Ordering requiredOrdering : requiredOrderings) {
                final Optional<Ordering> combinedOrderingOptional =
                        OrderingProperty.fromOrderingsForSetPlan(orderingOptionals, requiredOrdering, Ordering::intersectEqualityBoundKeys);
                if (!combinedOrderingOptional.isPresent()) {
                    continue;
                }

                final Ordering ordering = combinedOrderingOptional.get();
                final Set<KeyExpression> equalityBoundKeys = ordering.getEqualityBoundKeys();
                final List<KeyPart> orderingKeyParts = ordering.getOrderingKeyParts();

                final List<KeyExpression> orderingKeys =
                        orderingKeyParts
                                .stream()
                                .map(KeyPart::getNormalizedKeyExpression)
                                .collect(ImmutableList.toImmutableList());

                // make sure the common primary key parts are either bound through equality or they are part of the ordering
                if (!isPrimaryKeyCompatibleWithOrdering(commonPrimaryKeyParts, orderingKeys, equalityBoundKeys)) {
                    continue;
                }

                //
                // At this point we know we can implement the distinct union over the partitions of compatibly ordered plans
                //
                final KeyExpression comparisonKey =
                        orderingKeys.size() == 1
                        ? Iterables.getOnlyElement(orderingKeys) : Key.Expressions.concat(orderingKeys);

                //
                // create new references
                //
                final ImmutableList<Quantifier.Physical> newQuantifiers = entries
                        .stream()
                        .map(Map.Entry::getValue)
                        .map(GroupExpressionRef::from)
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList());

                call.yield(call.ref(RecordQueryUnionPlan.fromQuantifiers(newQuantifiers, comparisonKey, true)));
            }
        }
    }

    private boolean isPrimaryKeyCompatibleWithOrdering(@Nonnull List<KeyExpression> primaryKeyParts,
                                                       @Nonnull List<KeyExpression> orderingKeys,
                                                       @Nonnull Set<KeyExpression> equalityBoundKeys) {
        for (final KeyExpression commonPrimaryKeyPart : primaryKeyParts) {
            if (!equalityBoundKeys.contains(commonPrimaryKeyPart) && !orderingKeys.contains(commonPrimaryKeyPart)) {
                return false;
            }
        }
        return true;
    }
}
