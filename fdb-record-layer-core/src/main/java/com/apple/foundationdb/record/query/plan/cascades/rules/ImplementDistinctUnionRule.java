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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.KeyPart;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.PropertiesMap.allAttributesExcept;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyPlanPartition;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.planPartitions;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.rollUpTo;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.where;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalDistinctExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalUnionExpression;
import static com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty.DISTINCT_RECORDS;
import static com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty.ORDERING;
import static com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty.STORED_RECORD;

/**
 * A rule that implements a distinct union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnionExpression} and create a
 * {@link RecordQueryUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementDistinctUnionRule extends PlannerRule<LogicalDistinctExpression> {

    @Nonnull
    private static final CollectionMatcher<PlanPartition> unionLegPlanPartitionsMatcher = all(anyPlanPartition());

    @Nonnull
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> unionLegReferenceMatcher =
            planPartitions(where(planPartition -> planPartition.getAttributeValue(STORED_RECORD),
                    rollUpTo(unionLegPlanPartitionsMatcher, allAttributesExcept(DISTINCT_RECORDS, STORED_RECORD))));

    @Nonnull
    private static final BindingMatcher<LogicalUnionExpression> unionExpressionMatcher =
            logicalUnionExpression(all(forEachQuantifierOverRef(unionLegReferenceMatcher)));

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> unionForEachQuantifierMatcher = forEachQuantifier(unionExpressionMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalDistinctExpression> root =
            logicalDistinctExpression(exactly(unionForEachQuantifierMatcher));

    public ImplementDistinctUnionRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    @SuppressWarnings("java:S135")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var context = call.getContext();

        final var requiredOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requiredOrderingsOptional.isEmpty()) {
            return;
        }
        final var requestedOrderings = requiredOrderingsOptional.get();

        final var commonPrimaryKey = context.getCommonPrimaryKey();
        if (commonPrimaryKey == null) {
            return;
        }
        final var commonPrimaryKeyParts = commonPrimaryKey.normalizeKeyForPositions();

        final var bindings = call.getBindings();

        final var unionForEachQuantifier = bindings.get(unionForEachQuantifierMatcher);
        final var planPartitionsByQuantifier = bindings.getAll(unionLegPlanPartitionsMatcher);

        final var partitionsCrossProduct = CrossProduct.crossProduct(planPartitionsByQuantifier);

        // for each requested ordering
        for (final RequestedOrdering requestedOrdering : requestedOrderings) {
            final var partitionsCrossProductIterator = partitionsCrossProduct.iterator();

            //
            // Iterate through the elements of the cross product of partitions.
            //
            // The idea is to fully utilize the skip() functionality of the enumerating iterator that we created
            // over the cross product of partitions:
            // Assume we have three different orderings on each leg of the union: a, b, c.
            // The cross product for a four-legged union would be [a, b, c] x [a, b, c] x [a, b, c] x [a, b, c].
            // But only a small subset of that cross product, i.e. (a, a, a, a), (b, b, b, b), and (c, c, c, c),
            // are viable candidates to form a compatible ordering. We would like to skip useless orderings
            // as aggressively as possible:
            // Let's assume the current iterator points to (a, b, a, a). This is also the first occurrence of
            // (a, b, ..., ...) in iteration order. We reject (a, b, a, a) and normally would just continue by calling
            // next() to get to (a, b, a, b), then (a, b, a, c), and after some more iterations we would eventually
            // get to (a, b, c, c) before advancing to (a, c, ..., ...). Even though we know that all (a, b, ..., ...)
            // won't form a viable compatible all-encompassing ordering, we still iterate through all elements of the
            // cross product with that prefix.
            // Instead of just calling next() we can tell the iterator to skip(1) when getting to (a, b, a, a)
            // which advances the iterator to the next element after (a, b, ..., ...), continuing the iteration at
            // (a, c, a, a). While (a, c, a, a) is still not viable we just skipped an entire subtree of options without
            // retrying any of the expensive tests.
            //

            // keep a side structure to avoid re-computation of the combined orderings
            final var merge =
                    Lists.<Pair<Ordering /* merged ordering */, Ordering /* current ordering */>>newArrayList();
            while (partitionsCrossProductIterator.hasNext()) {
                final var partitions = partitionsCrossProductIterator.next();
                final ImmutableList<Ordering> orderings =
                        partitions
                                .stream()
                                .map(planPartition -> planPartition.getAttributeValue(ORDERING))
                                .collect(ImmutableList.toImmutableList());
                pushInterestingOrders(call, unionForEachQuantifier, orderings, requestedOrdering);

                for (int i = 0; i < merge.size(); i ++) {
                    if (!orderings.get(i).equals(merge.get(i).getValue())) {
                        merge.subList(i, merge.size()).clear();
                        break;
                    }
                }

                while (merge.size() < orderings.size()) {
                    if (merge.isEmpty()) {
                        merge.add(Pair.of(orderings.get(0), orderings.get(0)));
                    } else {
                        //
                        // TODO We currently are unable to incrementally compute a merged order as the operation is
                        //  not associative in all cases. The reason for that is that orderings are not represented
                        //  as PartialOrder<KeyPart> but as List<KeyPart>. While we do the reasoning for all
                        //  ordering-related logic in PartialOrder space, we transform back to a list which proves
                        //  to be lossy in some cases causing associativity to not hold).
                        //  https://github.com/FoundationDB/fdb-record-layer/issues/1618
                        
                        // TODO
                        //  final var lastMerged = merge.size() - 1;
                        //  final Ordering mergedOrdering =
                        //      OrderingVisitor.deriveForUnionFromOrderings(ImmutableList.of(merge.get(lastMerged).getKey(), orderings.get(merge.size())),
                        //          RequestedOrdering.preserve(),
                        //          Ordering::intersectEqualityBoundKeys);
                        //
                        final var currentOrderings = ImmutableList.<Ordering>builder()
                                .addAll(merge.stream().map(Pair::getValue).collect(ImmutableList.toImmutableList()))
                                .add(orderings.get(merge.size()))
                                .build();
                        final var mergedOrdering =
                                OrderingProperty.OrderingVisitor.deriveForUnionFromOrderings(currentOrderings,
                                        RequestedOrdering.preserve(),
                                        Ordering::intersectEqualityBoundKeys);

                        final var equalityBoundKeys = mergedOrdering.getEqualityBoundKeys();
                        final var orderingKeyParts = mergedOrdering.getOrderingKeyParts();

                        final var orderingKeys =
                                orderingKeyParts
                                        .stream()
                                        .map(KeyPart::getNormalizedKeyExpression)
                                        .collect(ImmutableList.toImmutableList());

                        // make sure the common primary key parts are either bound through equality or they are part of the ordering
                        if (isPrimaryKeyCompatibleWithOrdering(commonPrimaryKeyParts, orderingKeys, equalityBoundKeys)) {
                            // this is a good merged ordering so far
                            merge.add(Pair.of(mergedOrdering, orderings.get(merge.size())));
                        } else {
                            // back track
                            partitionsCrossProductIterator.skip(merge.size());
                            break;
                        }
                    }
                }

                if (merge.size() == orderings.size()) {
                    final var ordering =
                            OrderingProperty.OrderingVisitor.deriveForUnionFromOrderings(orderings, requestedOrdering, Ordering::intersectEqualityBoundKeys);
                    if (ordering.isEmpty()) {
                        //
                        // Push interesting orders down the appropriate quantifiers.
                        //
                        continue;
                    }

                    final var equalityBoundKeys = ordering.getEqualityBoundKeys();
                    final var orderingKeyParts = ordering.getOrderingKeyParts();

                    final var orderingKeys =
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
                    final var comparisonKey =
                            orderingKeys.size() == 1
                            ? Iterables.getOnlyElement(orderingKeys) : Key.Expressions.concat(orderingKeys);

                    //
                    // create new quantifiers
                    //
                    final var newQuantifiers = partitions
                            .stream()
                            .map(PlanPartition::getPlans)
                            .map(GroupExpressionRef::from)
                            .map(Quantifier::physical)
                            .collect(ImmutableList.toImmutableList());

                    call.yield(call.ref(RecordQueryUnionPlan.fromQuantifiers(newQuantifiers, comparisonKey, true)));
                }
            }
        }
    }

    private void pushInterestingOrders(@Nonnull PlannerRuleCall call,
                                       @Nonnull final Quantifier unionForEachQuantifier,
                                       @Nonnull final ImmutableList<Ordering> providedOrderings,
                                       @Nonnull final RequestedOrdering requestedOrdering) {
        final var unionRef = unionForEachQuantifier.getRangesOver();
        for (final var providedOrdering : providedOrderings) {
            if (Ordering.satisfiesRequestedOrdering(providedOrdering, requestedOrdering)) {
                final var innerRequestedOrdering =
                        new RequestedOrdering(providedOrdering.getOrderingKeyParts(),
                                providedOrdering.isDistinct()
                                ? RequestedOrdering.Distinctness.DISTINCT
                                : RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);
                call.pushConstraint(unionRef, RequestedOrderingConstraint.REQUESTED_ORDERING, ImmutableSet.of(innerRequestedOrdering));
            }
        }
    }

    private boolean isPrimaryKeyCompatibleWithOrdering(@Nonnull List<KeyExpression> primaryKeyParts,
                                                       @Nonnull List<KeyExpression> orderingKeys,
                                                       @Nonnull Set<KeyExpression> equalityBoundKeys) {
        for (final var commonPrimaryKeyPart : primaryKeyParts) {
            if (!equalityBoundKeys.contains(commonPrimaryKeyPart) && !orderingKeys.contains(commonPrimaryKeyPart)) {
                return false;
            }
        }
        return true;
    }
}
