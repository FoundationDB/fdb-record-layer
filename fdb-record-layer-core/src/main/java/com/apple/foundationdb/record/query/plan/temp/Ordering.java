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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.combinatorics.PartialOrder;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
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
     * Multimap from {@link KeyExpression} to a set of {@link Comparison}s to capture all expressions that are
     * bound through equality. This needs to be a multimap to accommodate for the case where an expression is
     * bound multiple times independently and where it is not immediately clear that both bound locations are
     * redundant or contradictory. For instance {@code x = 5} and {@code x = 6} together are effectively a
     * contradiction causing a predicate to always evaluate to {@code false}. In other cases, we may encounter
     * {@code x = 5} and {@code x = $p} where it is unclear if we just encountered a contradiction as well or
     * if the predicate is just redundant (where {@code $p} is bound to {@code 5} when the query is executed).
     */
    @Nonnull
    private final SetMultimap<KeyExpression, Comparison> equalityBoundKeyMap;

    /**
     * A list of {@link KeyExpression}s where none of the contained expressions is equality-bound. This list
     * defines the actual order of records.
     */
    @Nonnull
    private final List<KeyPart> orderingKeyParts;

    private final boolean isDistinct;

    @Nonnull
    private final Supplier<PartialOrder<KeyPart>> partialOrderSupplier;

    public Ordering(@Nonnull final SetMultimap<KeyExpression, Comparison> equalityBoundKeyMap,
                    @Nonnull final List<KeyPart> orderingKeyParts,
                    final boolean isDistinct) {
        this.orderingKeyParts = ImmutableList.copyOf(orderingKeyParts);
        this.equalityBoundKeyMap = ImmutableSetMultimap.copyOf(equalityBoundKeyMap);
        this.isDistinct = isDistinct;
        this.partialOrderSupplier = Suppliers.memoize(this::computePartialOrder);
    }

    /**
     * When expressing a requirement (see also {@link OrderingAttribute}), the requirement may be to preserve
     * the order of records that are being encountered. This is represented by a special value here.
     * @return {@code true} if the ordering needs to be preserved
     */
    public boolean isPreserve() {
        return orderingKeyParts.isEmpty();
    }

    @Nonnull
    public SetMultimap<KeyExpression, Comparison> getEqualityBoundKeyMap() {
        return equalityBoundKeyMap;
    }

    @Nonnull
    public Set<KeyExpression> getEqualityBoundKeys() {
        return equalityBoundKeyMap.keySet();
    }

    @Nonnull
    public List<KeyPart> getOrderingKeyParts() {
        return orderingKeyParts;
    }

    public boolean isDistinct() {
        return isDistinct;
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
        return getEqualityBoundKeys().equals(ordering.getEqualityBoundKeys()) &&
               getOrderingKeyParts().equals(ordering.getOrderingKeyParts()) &&
               isDistinct() == ordering.isDistinct();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEqualityBoundKeys(), getOrderingKeyParts(), isDistinct());
    }

    @Nonnull
    public PartialOrder<KeyPart> toPartialOrder() {
        return partialOrderSupplier.get();
    }

    /**
     * Method to compute the {@link PartialOrder} representing this {@link Ordering}.
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
     * In order to properly capture this multitude of orderings, we can use partial orders (see {@link PartialOrder})
     * to define the ordering (unfortunate name clash). For our example, we can write
     *
     * <pre>
     * {@code
     * PartialOrder([a, b, x, y], [a < b, x < y])
     * }
     * </pre>
     *
     * and mean all topologically correct permutations of {@code a, b, x, y}.
     *
     * @return a {@link PartialOrder} for this ordering
     */
    @Nonnull
    private PartialOrder<KeyPart> computePartialOrder() {
        return PartialOrder.<KeyPart>builder()
                .addListWithDependencies(this.getOrderingKeyParts())
                .addAll(equalityBoundKeyMap.keySet().stream().map(KeyPart::of).collect(ImmutableSet.toImmutableSet()))
                .build();
    }

    @Nonnull
    public static boolean satisfiesRequestedOrdering(@Nonnull Ordering providedOrdering,
                                                     @Nonnull RequestedOrdering requestedOrdering) {
        return satisfiesKeyPartsOrdering(providedOrdering.toPartialOrder(),
                requestedOrdering.getOrderingKeyParts(),
                Function.identity())
                .isPresent();
    }

    @Nonnull
    public Optional<List<KeyPart>> satisfiesRequestedOrdering(@Nonnull final List<KeyPart> requiredOrderingKeyParts,
                                                              @Nonnull final Set<KeyExpression> comparablyBoundKeys) {
        final var partialOrder =
                PartialOrder.<KeyPart>builder()
                        .addListWithDependencies(this.getOrderingKeyParts())
                        .addAll(equalityBoundKeyMap.keySet().stream().map(KeyPart::of).collect(ImmutableSet.toImmutableSet()))
                        .addAll(comparablyBoundKeys.stream().map(KeyPart::of).collect(ImmutableSet.toImmutableSet()))
                        .build();

        final var satisfyingPermutations =
                TopologicalSort.satisfyingPermutations(
                        partialOrder,
                        requiredOrderingKeyParts,
                        (t, p) -> true);

        return StreamSupport.stream(satisfyingPermutations.spliterator(), false)
                .findAny();
    }

    @Nonnull
    public static <T> Optional<List<T>> satisfiesKeyPartsOrdering(@Nonnull final PartialOrder<T> partialOrder,
                                                                  @Nonnull final List<KeyPart> requestedOrderingKeyParts,
                                                                  @Nonnull final Function<T, KeyPart> domainMapperFunction) {
        final var satisfyingPermutations =
                TopologicalSort.satisfyingPermutations(
                        partialOrder,
                        requestedOrderingKeyParts,
                        domainMapperFunction,
                        (t, p) -> true);

        return StreamSupport.stream(satisfyingPermutations.spliterator(), false)
                .findAny();
    }

    @Nonnull
    @SuppressWarnings("java:S135")
    public static <K> PartialOrder<K> mergePartialOrderOfOrderings(@Nonnull final PartialOrder<K> left,
                                                                   @Nonnull final PartialOrder<K> right) {
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

        return PartialOrder.of(elementBuilder.build(), dependencyBuilder.build());
    }

    @Nonnull
    @SuppressWarnings("java:S135")
    public static <K> PartialOrder<K> mergePartialOrderOfOrderings(@Nonnull Iterable<PartialOrder<K>> partialOrders) {
        return StreamSupport.stream(partialOrders.spliterator(), false)
                .reduce(Ordering::mergePartialOrderOfOrderings)
                .orElseThrow(() -> new IllegalStateException("must have a partial order"));
    }

    /**
     * Method to combine a list of {@link Ordering}s into one {@link Ordering} if possible. This method is e.g. used
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
     * @param orderingOptionals a list of orderings that should be combined to a common ordering
     * @param requestedOrdering an ordering that is desirable (or required). This parameter is ignored if the ordering
     *        passed in is to preserve ordering
     * @return an optional of a list of parts that defines the common ordering that also satisfies the required ordering,
     *         {@code Optional.empty()} if such a common ordering does not exist
     */
    @Nonnull
    public static Optional<List<KeyPart>> commonOrderingKeys(@Nonnull List<Optional<Ordering>> orderingOptionals,
                                                             @Nonnull RequestedOrdering requestedOrdering) {
        if (orderingOptionals.isEmpty()) {
            return Optional.empty();
        }

        if (orderingOptionals
                .stream()
                .anyMatch(Optional::isEmpty)) {
            return Optional.empty();
        }

        final var mergedPartialOrder =
                mergePartialOrderOfOrderings(
                        () -> orderingOptionals
                                .stream()
                                .map(orderingOptional -> orderingOptional.orElseThrow(() -> new IllegalStateException("optional cannot be empty")))
                                .map(Ordering::toPartialOrder)
                                .iterator());

        return satisfiesKeyPartsOrdering(mergedPartialOrder, requestedOrdering.getOrderingKeyParts(), Function.identity());
    }

    /**
     * Method to combine the map of equality-bound keys (and their bindings) for multiple orderings.
     *
     * @param orderingInfoOptionals a list of ordering optionals
     * @param combineFn a {@link BinaryOperator} that can combine two maps of equality-bound keys (and their bindings).
     * @return a new combined multimap of equality-bound keys (and their bindings) for all the orderings passed in
     */
    @Nonnull
    public static Optional<SetMultimap<KeyExpression, Comparison>> combineEqualityBoundKeys(@Nonnull final List<Optional<Ordering>> orderingInfoOptionals,
                                                                                            @Nonnull final BinaryOperator<SetMultimap<KeyExpression, Comparison>> combineFn) {
        final Iterator<Optional<Ordering>> membersIterator = orderingInfoOptionals.iterator();
        if (!membersIterator.hasNext()) {
            // don't bail on incorrect graph structure, just return empty()
            return Optional.empty();
        }

        final Optional<Ordering> commonOrderingInfoOptional = membersIterator.next();
        if (commonOrderingInfoOptional.isEmpty()) {
            return Optional.empty();
        }

        final var commonOrderingInfo = commonOrderingInfoOptional.get();
        SetMultimap<KeyExpression, Comparison> commonEqualityBoundKeyMap = commonOrderingInfo.getEqualityBoundKeyMap();

        while (membersIterator.hasNext()) {
            final Optional<Ordering> currentOrderingOptional = membersIterator.next();
            if (currentOrderingOptional.isEmpty()) {
                return Optional.empty();
            }

            final var currentOrdering = currentOrderingOptional.get();

            final SetMultimap<KeyExpression, Comparison> currentEqualityBoundKeyMap = currentOrdering.getEqualityBoundKeyMap();
            commonEqualityBoundKeyMap = combineFn.apply(commonEqualityBoundKeyMap, currentEqualityBoundKeyMap);
        }

        return Optional.of(commonEqualityBoundKeyMap);
    }

    /**
     * Union the equality-bound keys of two orderings. This method is usually passed in as a method reference to
     * {@link #combineEqualityBoundKeys(List, BinaryOperator)} as the binary operator.
     * @param left multimap of equality-bound keys of the left ordering (and their bindings)
     * @param right multimap of equality-bound keys of the right ordering (and their bindings)
     * @return new combined multimap of equality-bound keys (and their bindings)
     */
    @Nonnull
    public static SetMultimap<KeyExpression, Comparison> unionEqualityBoundKeys(@Nonnull SetMultimap<KeyExpression, Comparison> left,
                                                                                @Nonnull SetMultimap<KeyExpression, Comparison> right) {
        final ImmutableSetMultimap.Builder<KeyExpression, Comparison> resultBuilder = ImmutableSetMultimap.builder();
        resultBuilder.putAll(left);
        resultBuilder.putAll(right);
        return resultBuilder.build();
    }

    /**
     * Intersect the equality-bound keys of two orderings. This method is usually passed in as a method reference to
     * {@link #combineEqualityBoundKeys(List, BinaryOperator)} as the binary operator.
     * @param left multimap of equality-bound keys of the left ordering (and their bindings)
     * @param right multimap of equality-bound keys of the right ordering (and their bindings)
     * @return new combined multimap of equality-bound keys (and their bindings)
     */
    @Nonnull
    public static SetMultimap<KeyExpression, Comparison> intersectEqualityBoundKeys(@Nonnull SetMultimap<KeyExpression, Comparison> left,
                                                                                    @Nonnull SetMultimap<KeyExpression, Comparison> right) {
        final ImmutableSetMultimap.Builder<KeyExpression, Comparison> resultBuilder = ImmutableSetMultimap.builder();
        
        for (final Map.Entry<KeyExpression, Collection<Comparison>> rightEntry : right.asMap().entrySet()) {
            final KeyExpression rightKey = rightEntry.getKey();
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
}
