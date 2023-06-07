/*
 * Ordering.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.OrderingValueSimplificationRuleSet;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * This class captures an ordering property. An ordering is a list of parts that define the actual ordering of
 * records together with a (multi-)set of equality-bound keys. The expressions that are bound by equality is
 * disjoint from the set of expressions partaking in the list of orderings.
 *
 * <h2>
 * Compliant Example
 * </h2>
 * <pre>
 * {@code
 *   ordering parts: (a, b, c); equality-bound: d = 5, e = $p*
 * }
 * </pre>
 *
 * <h2>
 * Non-Compliant Example
 * </h2>
 * <pre>
 * {@code
 *   ordering parts: (a, b, c); equality-bound: c = 5
 * }
 * </pre>
 *
 * Note that such an ordering would always be:
 * <pre>
 * {@code
 *   ordering parts: (a, b); equality-bound: c = 5
 * }
 *
 * </pre>
 *
 * Instances of this class are used to communicate properties of plans as well as required properties.
 *
 */
public class Ordering {
    /**
     * Multimap from {@link Value} to a set of {@link Comparison}s to capture all expressions that are
     * bound through equality. This needs to be a multimap to accommodate for the case where an expression is
     * bound multiple times independently and where it is not immediately clear that both bound locations are
     * redundant or contradictory. For instance {@code x = 5} and {@code x = 6} together are effectively a
     * contradiction causing a predicate to always evaluate to {@code false}. In other cases, we may encounter
     * {@code x = 5} and {@code x = $p} where it is unclear if we just encountered a contradiction as well or
     * if the predicate is just redundant (where {@code $p} is bound to {@code 5} when the query is executed).
     */
    @Nonnull
    private final SetMultimap<Value, Comparison> equalityBoundValueMap;

    /**
     * A list of {@link OrderingPart}s where none of the contained expressions is equality-bound. This list
     * defines the actual order of records.
     */
    @Nonnull
    private final PartiallyOrderedSet<OrderingPart> orderingSet;

    private final boolean isDistinct;

    public Ordering(@Nonnull final SetMultimap<Value, Comparison> equalityBoundValueMap,
                    @Nonnull final List<OrderingPart> orderingParts,
                    final boolean isDistinct) {
        this(equalityBoundValueMap, computePartialOrder(equalityBoundValueMap, orderingParts), isDistinct);
    }

    public Ordering(@Nonnull final SetMultimap<Value, Comparison> equalityBoundValueMap,
                    @Nonnull final PartiallyOrderedSet<OrderingPart> orderingSet,
                    final boolean isDistinct) {
        Debugger.sanityCheck(() -> {
            final var normalizedOrderingSet = normalizeOrderingSet(equalityBoundValueMap, orderingSet);
            Verify.verify(orderingSet.equals(normalizedOrderingSet));
        });

        this.orderingSet = orderingSet;
        this.equalityBoundValueMap = ImmutableSetMultimap.copyOf(equalityBoundValueMap);
        this.isDistinct = isDistinct;
    }

    @Nonnull
    public SetMultimap<Value, Comparison> getEqualityBoundValueMap() {
        return equalityBoundValueMap;
    }

    @Nonnull
    public Set<Value> getEqualityBoundValues() {
        return equalityBoundValueMap.keySet();
    }

    @Nonnull
    public PartiallyOrderedSet<OrderingPart> getOrderingSet() {
        return orderingSet;
    }
    
    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isEmpty() {
        return equalityBoundValueMap.isEmpty() && orderingSet.isEmpty();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Ordering)) {
            return false;
        }
        final var ordering = (Ordering)o;
        return getEqualityBoundValues().equals(ordering.getEqualityBoundValues()) &&
               getOrderingSet().equals(ordering.getOrderingSet()) &&
               isDistinct() == ordering.isDistinct();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEqualityBoundValues(), getOrderingSet(), isDistinct());
    }

    @Override
    public String toString() {
        return "[" + (isDistinct ? "distinct " : " ") + orderingSet + "]";
    }

    @Nonnull
    public Set<Set<Value>> enumerateSatisfyingComparisonKeyValues(@Nonnull final RequestedOrdering requestedOrdering) {
        if (requestedOrdering.isDistinct() && !isDistinct()) {
            return ImmutableSet.of();
        }

        return Ordering.enumerateSatisfyingOrderingComparisonKeyValues(getOrderingSet(), getEqualityBoundValues(), requestedOrdering.getOrderingParts());
    }

    @Nonnull
    public Set<RequestedOrdering> deriveRequestedOrderings(@Nonnull final RequestedOrdering requestedOrdering) {
        if (requestedOrdering.isDistinct() && !isDistinct()) {
            return ImmutableSet.of();
        }

        final var satisfyingEnumeratedOrderings = enumerateSatisfyingOrderings(requestedOrdering);
        return Streams.stream(satisfyingEnumeratedOrderings)
                .map(keyParts -> new RequestedOrdering(keyParts, RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS))
                .collect(ImmutableSet.toImmutableSet());
    }

    public boolean satisfies(@Nonnull RequestedOrdering requestedOrdering) {
        return !Iterables.isEmpty(enumerateSatisfyingOrderings(requestedOrdering));
    }

    @Nonnull
    public Iterable<List<OrderingPart>> enumerateSatisfyingOrderings(@Nonnull RequestedOrdering requestedOrdering) {
        if (requestedOrdering.isDistinct() && !isDistinct()) {
            return ImmutableList.of();
        }
        final var requestedOrderingParts = requestedOrdering.getOrderingParts();
        return TopologicalSort.satisfyingPermutations(
                getOrderingSet(),
                requestedOrderingParts,
                Function.identity(),
                permutation -> requestedOrderingParts.size());
    }

    public boolean satisfiesGroupingValues(@Nonnull final Set<Value> requestedGroupingValues) {
        final var normalizedRequestedOrderingValues =
                requestedGroupingValues.stream()
                        .filter(requestedGroupingValue -> !equalityBoundValueMap.containsKey(requestedGroupingValue))
                        .collect(ImmutableSet.toImmutableSet());

        // no ordering is requested.
        if (normalizedRequestedOrderingValues.isEmpty()) {
            return true;
        }

        final var filteredOrderingSet = orderingSet.filterIndependentElements(keyPart -> !equalityBoundValueMap.containsKey(keyPart.getValue()));
        final var permutations = TopologicalSort.topologicalOrderPermutations(filteredOrderingSet);

        for (final var permutation : permutations) {
            final var containsAll = permutation.subList(0, normalizedRequestedOrderingValues.size())
                    .stream()
                    .allMatch(keyPart -> normalizedRequestedOrderingValues.contains(keyPart.getValue()));

            if (containsAll) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    public Ordering pullUp(@Nonnull Value value, @Nonnull AliasMap aliasMap, @Nonnull Set<CorrelationIdentifier> constantAliases) {
        //
        // Need to pull every participating value of this ordering through the value.
        //
        final var pulledUpOrderingParts =
                getOrderingSet()
                        .mapAll(orderingParts -> {
                            final var orderingKeyValues =
                                    Streams.stream(orderingParts)
                                            .map(OrderingPart::getValue)
                                            .collect(ImmutableList.toImmutableList());
                            final var pulledUpValuesMap = value.pullUp(orderingKeyValues, aliasMap, constantAliases, Quantifier.current());
                            final var resultMapBuilder = ImmutableBiMap.<OrderingPart, OrderingPart>builder();
                            for (final OrderingPart orderingPart : orderingParts) {
                                final var pulledUpOrderingValue = pulledUpValuesMap.get(orderingPart.getValue());
                                if (pulledUpOrderingValue != null) {
                                    resultMapBuilder.put(orderingPart, OrderingPart.of(pulledUpOrderingValue, orderingPart.isReverse()));
                                }
                            }
                            return resultMapBuilder.build();
                        });

        final var pulledUpEqualityBoundMap =
                translateEqualityBoundValueMap(equalityBoundValueMap,
                        toBePulledValues -> value.pullUp(toBePulledValues, aliasMap, constantAliases, Quantifier.current()));

        return new Ordering(pulledUpEqualityBoundMap, pulledUpOrderingParts, false);
    }

    @Nonnull
    public Ordering pushDown(@Nonnull Value value, @Nonnull AliasMap aliasMap, @Nonnull Set<CorrelationIdentifier> constantAliases) {
        //
        // Need to push every participating value of this ordering through the value.
        //
        final var pushedDownOrderingParts =
                getOrderingSet()
                        .mapAll(orderingParts -> {
                            final var orderingValues =
                                    Streams.stream(orderingParts)
                                            .map(OrderingPart::getValue)
                                            .collect(ImmutableList.toImmutableList());
                            final var pushedDownOrderingValues = value.pushDown(orderingValues, OrderingValueSimplificationRuleSet.ofOrderingSimplificationRules(), aliasMap, constantAliases, Quantifier.current());
                            final var resultMapBuilder = ImmutableBiMap.<OrderingPart, OrderingPart>builder();
                            final var orderingPartsIterator = orderingParts.iterator();
                            final var pushedDownOrderingValuesIterator = pushedDownOrderingValues.iterator();
                            while (orderingPartsIterator.hasNext() && pushedDownOrderingValuesIterator.hasNext()) {
                                final var orderingPart = orderingPartsIterator.next();
                                final var pushedDownOrderingValue = pushedDownOrderingValuesIterator.next();
                                resultMapBuilder.put(orderingPart, OrderingPart.of(pushedDownOrderingValue, orderingPart.isReverse()));
                            }
                            Verify.verify(!orderingPartsIterator.hasNext() && !pushedDownOrderingValuesIterator.hasNext());

                            return resultMapBuilder.build();
                        });

        final var pushedDownEqualityBoundMap =
                translateEqualityBoundValueMap(equalityBoundValueMap,
                        toBePushedValues -> {
                            final var pushedDownValues = value.pushDown(toBePushedValues, DefaultValueSimplificationRuleSet.ofSimplificationRules(), aliasMap, constantAliases, Quantifier.current());
                            final var resultMap = new LinkedIdentityMap<Value, Value>();
                            for (int i = 0; i < toBePushedValues.size(); i++) {
                                final Value toBePushedValue = toBePushedValues.get(i);
                                final Value pushedValue = Objects.requireNonNull(pushedDownValues.get(i));
                                resultMap.put(toBePushedValue, pushedValue);
                            }
                            return resultMap;
                        });

        return new Ordering(pushedDownEqualityBoundMap, pushedDownOrderingParts, isDistinct());
    }

    @Nonnull
    public Ordering withAdditionalDependencies(@Nonnull final PartiallyOrderedSet<OrderingPart> otherOrderingSet) {
        Debugger.sanityCheck(() -> Verify.verify(getOrderingSet().getSet().containsAll(otherOrderingSet.getSet())));

        final var otherDependencyMap = otherOrderingSet.getDependencyMap();
        final var resultDependencyMap =
                ImmutableSetMultimap.<OrderingPart, OrderingPart>builder()
                        .putAll(orderingSet.getDependencyMap())
                        .putAll(otherDependencyMap)
                        .build();
        final var resultOrderingSet = PartiallyOrderedSet.of(orderingSet.getSet(), resultDependencyMap);

        return new Ordering(equalityBoundValueMap, resultOrderingSet, isDistinct);
    }

    @Nonnull
    private static SetMultimap<Value, Comparison> translateEqualityBoundValueMap(@Nonnull final SetMultimap<Value, Comparison> equalityBoundValueMap,
                                                                                 @Nonnull final Function<List<Value>, Map<Value, Value>> translateFunction) {
        final var pulledEqualityBoundValueMapBuilder = ImmutableSetMultimap.<Value, Comparisons.Comparison>builder();

        for (final var entry : equalityBoundValueMap.entries()) {
            final var entryValue = entry.getKey();
            final var comparison = entry.getValue();
            if (comparison instanceof Comparisons.ValueComparison) {
                final var valueComparison = (Comparisons.ValueComparison)comparison;
                final var valueForValueComparison = valueComparison.getComparandValue();
                final var pulledEqualityBindingValuesMap = translateFunction.apply(ImmutableList.of(entryValue, valueForValueComparison));
                final var pulledEqualityBindingValue = pulledEqualityBindingValuesMap.get(entryValue);
                final var pulledComparisonValue = pulledEqualityBindingValuesMap.get(valueForValueComparison);
                if (pulledEqualityBindingValue == null || pulledComparisonValue == null) {
                    continue;
                }
                pulledEqualityBoundValueMapBuilder.put(pulledEqualityBindingValue, new Comparisons.ValueComparison(valueComparison.getType(), pulledComparisonValue));
            } else {
                final var pulledEqualityBindingValuesMap = translateFunction.apply(ImmutableList.of(entryValue));
                final var pulledEqualityBindingValue = pulledEqualityBindingValuesMap.get(entryValue);
                if (pulledEqualityBindingValue == null) {
                    continue;
                }
                pulledEqualityBoundValueMapBuilder.put(pulledEqualityBindingValue, comparison);
            }
        }

        return pulledEqualityBoundValueMapBuilder.build();
    }

    /**
     * Method to compute the {@link PartiallyOrderedSet} representing this {@link Ordering}.
     *
     * An ordering expresses the order of e.g. fields {@code a, b, x} and additionally declares some fields, e.g. {@code c}
     * to be equal-bound to a value (e.g. {@code 5}). That means that {@code c} can freely move in the order declaration of
     * {@code a, b, x} and satisfy {@link RequestedOrdering}s such as e.g. {@code a, c, b, x}, {@code a, b, x, c}
     * or similar.
     *
     * Generalizing this idea, for this example we can also say that in this case the plan is ordered by
     * {@code c} as well as all the values for {@code c} are identical. Generalizing further, a plan, or by extension,
     * a stream of data can actually be ordered by many things at the same time. For instance, a stream of four
     * fields {@code a, b, x, y} can be ordered by {@code a, b} and {@code x, y} at the same time (consider e.g. that
     * {@code x = 10 * a; y = 10 *b}. Both of these orderings are equally correct and representative.
     *
     * Based on these two independent orderings we can construct new orderings that are also correct:
     * {@code a, b, x, y}, {@code a, x, b, y}, or {@code x, y, a, b}, among others.
     *
     * In order to properly capture this multitude of orderings, we can use partial orders (see {@link PartiallyOrderedSet})
     * to define the ordering (unfortunate name clash). For our example, we can write
     *
     * <pre>
     * {@code
     * PartiallyOrderedSet([a, b, x, y], [a < b, x < y])
     * }
     * </pre>
     *
     * and mean all topologically correct permutations of {@code a, b, x, y}.
     *
     * @param equalityBoundValueMap a set multimap of equality-bound keys
     * @param orderingParts a list of ordering {@link OrderingPart}s
     * @return a {@link PartiallyOrderedSet} for this ordering
     */
    @Nonnull
    private static PartiallyOrderedSet<OrderingPart> computePartialOrder(@Nonnull final SetMultimap<Value, Comparison> equalityBoundValueMap,
                                                                         @Nonnull final List<OrderingPart> orderingParts) {
        final var filteredOrderingParts =
                orderingParts.stream()
                        .filter(orderingPart -> !equalityBoundValueMap.containsKey(orderingPart.getValue()))
                        .collect(ImmutableList.toImmutableList());

        return PartiallyOrderedSet.<OrderingPart>builder()
                .addListWithDependencies(filteredOrderingParts)
                .addAll(equalityBoundValueMap.keySet().stream().map(OrderingPart::of).collect(ImmutableSet.toImmutableSet()))
                .build();
    }

    /**
     * Method to <em>normalize</em> a partially-ordered set representing an ordering, that is, it removes all
     * dependencies from or to a particular element contained in the set if that element is also equality-bound. If an
     * element is equality-bound, that is, it is constant for all practical purposes, it is in also independent with
     * respect to all other elements in the set.
     * @param equalityBoundValueMap a multimap relating values and equality comparisons
     * @param orderingSet a partially ordered set representing the ordering set of an ordering
     * @return a new (normalized) partially ordered set representing the dependencies between elements in an ordering
     */
    @Nonnull
    private static PartiallyOrderedSet<OrderingPart> normalizeOrderingSet(@Nonnull final SetMultimap<Value, Comparison> equalityBoundValueMap,
                                                                          @Nonnull final PartiallyOrderedSet<OrderingPart> orderingSet) {
        final var transitiveClosure = orderingSet.getTransitiveClosure();
        final var normalizedDependencyMapBuilder = ImmutableSetMultimap.<OrderingPart, OrderingPart>builder();

        for (final var dependency : transitiveClosure.entries()) {
            if (!equalityBoundValueMap.containsKey(dependency.getKey().getValue()) && !equalityBoundValueMap.containsKey(dependency.getValue().getValue())) {
                normalizedDependencyMapBuilder.put(dependency);
            }
        }
        return PartiallyOrderedSet.of(orderingSet.getSet(), normalizedDependencyMapBuilder.build());
    }

    @Nonnull
    public static Set<Set<Value>> enumerateSatisfyingOrderingComparisonKeyValues(@Nonnull final PartiallyOrderedSet<OrderingPart> partiallyOrderedSet,
                                                                                 @Nonnull final Set<Value> equalityBoundKeyValues,
                                                                                 @Nonnull final List<OrderingPart> requestedOrderingParts) {
        final var normalizedRequestedOrderingParts = requestedOrderingParts.stream()
                .filter(requestedOrderingPart -> !equalityBoundKeyValues.contains(requestedOrderingPart.getValue()))
                .collect(ImmutableList.toImmutableList());

        final var filteredOrderingSet = partiallyOrderedSet.filterIndependentElements(keyPart -> !equalityBoundKeyValues.contains(keyPart.getValue()));

        final var satisfyingOrderingParts =
                TopologicalSort.satisfyingPermutations(
                        filteredOrderingSet,
                        normalizedRequestedOrderingParts,
                        Function.identity(),
                        permutation -> normalizedRequestedOrderingParts.size());

        return Streams.stream(satisfyingOrderingParts)
                .map(enumeratedOrdering ->
                        enumeratedOrdering.stream()
                                .map(OrderingPart::getValue)
                                .collect(ImmutableSet.toImmutableSet()))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @SuppressWarnings("java:S135")
    public static <K> PartiallyOrderedSet<K> mergePartialOrderOfOrderings(@Nonnull final PartiallyOrderedSet<K> left,
                                                                          @Nonnull final PartiallyOrderedSet<K> right) {
        final var leftDependencies = left.getDependencyMap();
        final var rightDependencies = right.getDependencyMap();

        final var elementBuilder = ImmutableSet.<K>builder();
        final var dependencyBuilder = ImmutableSetMultimap.<K, K>builder();

        var leftEligibleSet = left.eligibleSet();
        var rightEligibleSet = right.eligibleSet();

        Set<K> lastElements = ImmutableSet.of();
        while (!leftEligibleSet.isEmpty() && !rightEligibleSet.isEmpty()) {
            final var leftElements = leftEligibleSet.eligibleElements();
            final var rightElements = rightEligibleSet.eligibleElements();

            final var intersectedElements = Sets.intersection(leftElements, rightElements);

            if (intersectedElements.isEmpty()) {
                break;
            }

            elementBuilder.addAll(intersectedElements);
            for (final var intersectedElement : intersectedElements) {
                for (final var lastElement : lastElements) {
                    if (leftDependencies.get(intersectedElement).contains(lastElement) || rightDependencies.get(intersectedElement).contains(lastElement)) {
                        dependencyBuilder.put(intersectedElement, lastElement);
                    }
                }
            }

            leftEligibleSet = leftEligibleSet.removeEligibleElements(intersectedElements);
            rightEligibleSet = rightEligibleSet.removeEligibleElements(intersectedElements);

            lastElements = intersectedElements;
        }

        return PartiallyOrderedSet.of(elementBuilder.build(), dependencyBuilder.build());
    }

    @Nonnull
    @SuppressWarnings("java:S135")
    public static <K> PartiallyOrderedSet<K> mergePartialOrderOfOrderings(@Nonnull Iterable<PartiallyOrderedSet<K>> partialOrders) {
        return StreamSupport.stream(partialOrders.spliterator(), false)
                .reduce(Ordering::mergePartialOrderOfOrderings)
                .orElseThrow(() -> new IllegalStateException("must have a partial order"));
    }

    /**
     * Method to combine a list of {@link Ordering}s into one {@link Ordering}. This method is e.g. used
     * by logic to establish a comparison key that reasons ordering in the context of planning for a distinct set
     * operation such as intersection or a union distinct.
     * Two or more orderings can be compatible or incompatible. If they are incompatible, this method will return an
     * empty optional, otherwise the computed common ordering:
     *
     * <pre>
     * Example 1
     * {@code
     *   ordering 1: ordering keys: (a, rec_id) equality-bound keys: ∅
     *   ordering 2: ordering keys: (a, b, rec_id) equality-bound keys: ∅
     *   common ordering is: ordering keys: (a) equality-bound keys: ∅
     * }
     * </pre>
     *
     * <pre>
     * Example 2
     * {@code
     *   ordering 1: ordering keys: (rec_id) equality-bound keys: a = 3
     *   ordering 2: ordering keys: (rec_id) equality-bound keys: a = 3
     *   common ordering is: ordering keys: (rec_id) equality-bound keys: a = 3
     * }
     * </pre>
     *
     * <pre>
     * Example 3
     * {@code
     *   ordering 1: ordering keys: (a, rec_id) equality-bound keys: ∅
     *   ordering 2: ordering keys: (rec_id) equality-bound keys: a = $p
     *   common ordering is: ordering keys: (a, rec_id) equality-bound keys: ∅
     * }
     * </pre>
     *
     * If they are compatible, however, it is not clear per se what the resulting comparison key should
     * be, though.
     *
     * <pre>
     * Example 4
     * {@code
     *   ordering 1: ordering keys: (a, rec_id) equality-bound keys: a = 3
     *   ordering 2: ordering keys: (rec_id) equality-bound keys: a = 5
     * }
     * </pre>
     *
     * It is unclear as to what the common ordering should be. It could be just
     * <pre>
     * {@code
     *   common ordering: ordering keys: (rec_id) equality-bound keys: ∅
     * }
     * </pre>
     * That is too restrictive for unions. For a union, the caller can indicate what a desirable
     * outcome should be as the operator itself can establish that desirable order. For example, a distinct union where
     * one leg is equality-bound via {@code a = 3} and a second leg is bound via {@code a = 5} can use a comparison
     * key of {@code a, rec_id}. The resulting ordering would be
     * <pre>
     * {@code
     *   common ordering: ordering keys: (a, rec_id) equality-bound keys: ∅
     * }
     * </pre>
     *
     * @param orderings a collection of orderings
     * @param combineFn a combine function to combine two maps of equality-bound keys (and their bindings)
     * @param isDistinct indicator if the resulting order is thought to be distinct
     * @return an optional of a list of parts that defines the common ordering that also satisfies the required ordering,
     *         {@code Optional.empty()} if such a common ordering does not exist
     */
    @Nonnull
    public static Ordering mergeOrderings(@Nonnull final Collection<Ordering> orderings,
                                          @Nonnull final BinaryOperator<SetMultimap<Value, Comparison>> combineFn,
                                          final boolean isDistinct) {
        final var orderingSets = orderings.stream()
                .map(Ordering::getOrderingSet)
                .collect(ImmutableList.toImmutableList());

        final var mergedOrderingSet = mergePartialOrderOfOrderings(orderingSets);

        final var mergedEqualityBoundKeys =
                combineEqualityBoundKeys(orderings, combineFn);

        return Ordering.ofUnnormalized(mergedEqualityBoundKeys, mergedOrderingSet, isDistinct);
    }

    /**
     * Helper method to concatenate the ordering key parts of the participating orderings in iteration order.
     * @param orderings a collection of orderings
     * @param combineFn a combine function to combine two maps of equality-bound keys (and their bindings)
     * @return a new ordering representing a concatenation of the given left and right ordering
     */
    @Nonnull
    public static Ordering concatOrderings(@Nonnull final Collection<Ordering> orderings,
                                           @Nonnull final BinaryOperator<SetMultimap<Value, Comparison>> combineFn) {

        return orderings.stream()
                .reduce((left, right) -> concatOrderings(left, right, combineFn))
                .orElseThrow(() -> new RecordCoreException("unable to concatenate orderings"));
    }

    /**
     * Helper method to concatenate the ordering key parts of the participating orderings in iteration order.
     * @param leftOrdering an {@link Ordering}
     * @param rightOrdering another {@link Ordering} to be concatenated to {@code leftOrdering}
     * @param combineFn a combine function to combine two maps of equality-bound keys (and their bindings)
     * @return a list of {@link OrderingPart}s
     */
    @Nonnull
    public static Ordering concatOrderings(@Nonnull final Ordering leftOrdering,
                                           @Nonnull final Ordering rightOrdering,
                                           @Nonnull final BinaryOperator<SetMultimap<Value, Comparison>> combineFn) {
        final var leftOrderingSet = leftOrdering.getOrderingSet();
        final var rightOrderingSet = rightOrdering.getOrderingSet();

        Verify.verify(leftOrdering.isDistinct());
        Debugger.sanityCheck(() ->
                Verify.verify(
                        Sets.intersection(leftOrderingSet.getSet(), rightOrderingSet.getSet()).isEmpty()));

        final var orderingElements =
                ImmutableSet.<OrderingPart>builder()
                        .addAll(leftOrderingSet.getSet())
                        .addAll(rightOrderingSet.getSet())
                        .build();

        final var dependencyMapBuilder =
                ImmutableSetMultimap.<OrderingPart, OrderingPart>builder()
                        .putAll(leftOrderingSet.getDependencyMap())
                        .putAll(rightOrderingSet.getDependencyMap());

        //
        // Find the maximum values of the left ordering. The eligible set of a partially-ordered set are its minimum elements.
        // Thus, the maximum elements are the minimum elements (that is the eligible elements) of the dual of the
        // partially-ordered set.
        // Find the maximum values of the left ordering. Make the minimum elements of the right ordering become dependent
        // on the maximum elements of the left ordering.
        //
        final var leftDualOrdering = leftOrderingSet.dualOrder();
        final var leftMaxElements = leftDualOrdering.eligibleSet().eligibleElements();
        final var rightMinElements = rightOrderingSet.eligibleSet().eligibleElements();

        for (final var leftMaxElement : leftMaxElements) {
            for (final var rightMinElement : rightMinElements) {
                dependencyMapBuilder.put(rightMinElement, leftMaxElement);
            }
        }

        final var concatenatedOrderingSet = PartiallyOrderedSet.of(orderingElements, dependencyMapBuilder.build());

        final var combinedEqualityBoundValueMap = combineFn.apply(leftOrdering.getEqualityBoundValueMap(), rightOrdering.getEqualityBoundValueMap());
        return Ordering.ofUnnormalized(combinedEqualityBoundValueMap, concatenatedOrderingSet, rightOrdering.isDistinct());
    }

    /**
     * Method to combine the map of equality-bound keys (and their bindings) for multiple orderings.
     *
     * @param orderings a list of orderings
     * @param combineFn a {@link BinaryOperator} that can combine two maps of equality-bound keys (and their bindings)
     * @return a new combined multimap of equality-bound keys (and their bindings) for all the orderings passed in
     */
    @Nonnull
    public static SetMultimap<Value, Comparison> combineEqualityBoundKeys(@Nonnull final Collection<Ordering> orderings,
                                                                          @Nonnull final BinaryOperator<SetMultimap<Value, Comparison>> combineFn) {
        Verify.verify(!orderings.isEmpty());
        final Iterator<Ordering> orderingsIterator = orderings.iterator();

        final var commonOrderingInfo = orderingsIterator.next();
        var commonEqualityBoundValueMap = commonOrderingInfo.getEqualityBoundValueMap();

        while (orderingsIterator.hasNext()) {
            final var currentOrdering = orderingsIterator.next();

            final var currentEqualityBoundValueMap = currentOrdering.getEqualityBoundValueMap();
            commonEqualityBoundValueMap = combineFn.apply(commonEqualityBoundValueMap, currentEqualityBoundValueMap);
        }

        return commonEqualityBoundValueMap;
    }

    /**
     * Union the equality-bound keys of two orderings. This method is usually passed in as a method reference to
     * {@link #combineEqualityBoundKeys(Collection, BinaryOperator)} as the binary operator.
     * @param left multimap of equality-bound keys of the left ordering (and their bindings)
     * @param right multimap of equality-bound keys of the right ordering (and their bindings)
     * @return newly combined multimap of equality-bound keys (and their bindings)
     */
    @Nonnull
    public static SetMultimap<Value, Comparison> unionEqualityBoundKeys(@Nonnull SetMultimap<Value, Comparison> left,
                                                                        @Nonnull SetMultimap<Value, Comparison> right) {
        final var resultBuilder = ImmutableSetMultimap.<Value, Comparison>builder();
        resultBuilder.putAll(left);
        resultBuilder.putAll(right);
        return resultBuilder.build();
    }

    /**
     * Intersect the equality-bound keys of two orderings. This method is usually passed in as a method reference to
     * {@link #combineEqualityBoundKeys(Collection, BinaryOperator)} as the binary operator.
     * @param left multimap of equality-bound keys of the left ordering (and their bindings)
     * @param right multimap of equality-bound keys of the right ordering (and their bindings)
     * @return new combined multimap of equality-bound keys (and their bindings)
     */
    @Nonnull
    public static SetMultimap<Value, Comparison> intersectEqualityBoundKeys(@Nonnull SetMultimap<Value, Comparison> left,
                                                                            @Nonnull SetMultimap<Value, Comparison> right) {
        final var resultBuilder = ImmutableSetMultimap.<Value, Comparison>builder();
        
        for (final var rightEntry : right.asMap().entrySet()) {
            final var rightKey = rightEntry.getKey();
            if (left.containsKey(rightKey)) {
                //
                // Left side contains the same key. We can only retain this key in the result, however, if at least
                // one actual comparison on right is in left as well.
                //
                final Collection<Comparison> rightComparisons = rightEntry.getValue();
                final boolean anyMatchingComparison =
                        rightComparisons
                                .stream()
                                .anyMatch(rightComparison -> left.containsEntry(rightKey, rightComparison));
                if (anyMatchingComparison) {
                    resultBuilder.putAll(rightKey, rightComparisons);
                }
            }
        }

        return resultBuilder.build();
    }

    @Nonnull
    public static Ordering emptyOrder() {
        return new Ordering(ImmutableSetMultimap.of(), ImmutableList.of(), false);
    }

    @Nonnull
    public static Ordering ofUnnormalized(@Nonnull final SetMultimap<Value, Comparison> equalityBoundValueMap,
                                          @Nonnull final PartiallyOrderedSet<OrderingPart> orderingSet,
                                          final boolean isDistinct) {
        return new Ordering(equalityBoundValueMap, normalizeOrderingSet(equalityBoundValueMap, orderingSet), isDistinct);
    }
}
