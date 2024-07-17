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
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;

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
import static com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty.PRIMARY_KEY;
import static com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty.STORED_RECORD;

/**
 * A rule that implements a distinct union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnionExpression} and create a
 * {@link RecordQueryUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ImplementDistinctUnionRule extends CascadesRule<LogicalDistinctExpression> {

    @Nonnull
    private static final CollectionMatcher<PlanPartition> unionLegPlanPartitionsMatcher = all(anyPlanPartition());

    @Nonnull
    private static final BindingMatcher<Reference> unionLegReferenceMatcher =
            planPartitions(where(planPartition -> planPartition.getAttributeValue(STORED_RECORD) &&
                                                  planPartition.getAttributeValue(PRIMARY_KEY).isPresent(),
                    rollUpTo(unionLegPlanPartitionsMatcher, allAttributesExcept(DISTINCT_RECORDS))));

    private static final CollectionMatcher<Quantifier.ForEach> allForEachQuantifiersMatcher =
            all(forEachQuantifierOverRef(unionLegReferenceMatcher));

    @Nonnull
    private static final BindingMatcher<LogicalUnionExpression> unionExpressionMatcher =
            logicalUnionExpression(allForEachQuantifiersMatcher);

    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> unionForEachQuantifierMatcher = forEachQuantifier(unionExpressionMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalDistinctExpression> root =
            logicalDistinctExpression(exactly(unionForEachQuantifierMatcher));

    public ImplementDistinctUnionRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    @SuppressWarnings({"java:S135", "UnstableApiUsage"})
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var requestedOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }
        final var requestedOrderings = requestedOrderingsOptional.get();

        final var bindings = call.getBindings();

        final var unionForEachQuantifier = bindings.get(unionForEachQuantifierMatcher);
        final var allForEachQuantifiers = bindings.get(allForEachQuantifiersMatcher);
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
                    Lists.<Pair<Ordering.Union /* merged ordering */, Ordering /* current ordering */>>newArrayList();
            while (partitionsCrossProductIterator.hasNext()) {
                final var partitions = partitionsCrossProductIterator.next();

                final var commonPrimaryKeyValuesMaybe =
                        PrimaryKeyProperty.commonPrimaryKeyValuesMaybeFromOptionals(partitions.stream()
                                .map(partition -> partition.getAttributeValue(PRIMARY_KEY))
                                .collect(ImmutableList.toImmutableList()));

                if (commonPrimaryKeyValuesMaybe.isEmpty()) {
                    continue;
                }
                final var commonPrimaryKeyValues = commonPrimaryKeyValuesMaybe.get();

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
                        merge.add(Pair.of(Ordering.UNION.createFromOrdering(orderings.get(0)), orderings.get(0)));
                    } else {
                        final var lastMerged = merge.size() - 1;
                        final var mergedOrdering =
                                Ordering.merge(ImmutableList.of(merge.get(lastMerged).getKey(),
                                                orderings.get(merge.size())),
                                        Ordering.UNION, (left, right) -> true);

                        // make sure the common primary key parts are either bound through equality or they are part of the ordering
                        if (isPrimaryKeyCompatibleWithOrdering(commonPrimaryKeyValues, mergedOrdering)) {
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
                    final var unionOrdering = merge.get(merge.size() - 1).getKey();

                    //
                    // create new quantifiers
                    //
                    final var newQuantifiers =
                            Streams.zip(partitions.stream(),
                                            allForEachQuantifiers.stream(),
                                            (partition, quantifier) -> call.memoizeMemberPlans(quantifier.getRangesOver(), partition.getPlans()))
                                    .map(Quantifier::physical)
                                    .collect(ImmutableList.toImmutableList());

                    final var enumeratedSatisfyingComparisonKeyValues =
                            unionOrdering.enumerateSatisfyingComparisonKeyValues(requestedOrdering);

                    for (final var comparisonKeyValues : enumeratedSatisfyingComparisonKeyValues) {
                        final var directionalOrderingParts =
                                unionOrdering.directionalOrderingParts(comparisonKeyValues, requestedOrdering, ProvidedSortOrder.FIXED);
                        final var comparisonDirectionOptional =
                                Ordering.resolveComparisonDirectionMaybe(directionalOrderingParts);
                        //
                        // At this point we know we can implement the distinct union over the partitions of compatibly-ordered plans
                        //
                        comparisonDirectionOptional.ifPresent(isReverse ->
                                call.yieldExpression(RecordQueryUnionPlan.fromQuantifiers(newQuantifiers,
                                        ImmutableList.copyOf(comparisonKeyValues), isReverse,
                                        true)));
                    }
                }
            }
        }
    }

    private void pushInterestingOrders(@Nonnull final CascadesRuleCall call,
                                       @Nonnull final Quantifier unionForEachQuantifier,
                                       @Nonnull final ImmutableList<Ordering> providedOrderings,
                                       @Nonnull final RequestedOrdering requestedOrdering) {
        final var unionRef = unionForEachQuantifier.getRangesOver();
        for (final var providedOrdering : providedOrderings) {
            final var requestedOrderings = providedOrdering.deriveRequestedOrderings(requestedOrdering);
            call.pushConstraint(unionRef, RequestedOrderingConstraint.REQUESTED_ORDERING, requestedOrderings);
        }
    }

    private boolean isPrimaryKeyCompatibleWithOrdering(@Nonnull final List<Value> primaryKeyValues,
                                                       @Nonnull final Ordering ordering) {
        final var orderingValues =
                ordering.getOrderingSet().getSet();
        for (final var primaryKeyValue : primaryKeyValues) {
            if (!orderingValues.contains(primaryKeyValue)) {
                return false;
            }
        }
        return true;
    }
}
