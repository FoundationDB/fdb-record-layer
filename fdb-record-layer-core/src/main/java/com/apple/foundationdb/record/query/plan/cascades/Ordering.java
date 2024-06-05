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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.SortOrder;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

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
    @Nonnull
    private static final Ordering EMPTY = new Ordering(ImmutableSetMultimap.of(), PartiallyOrderedSet.empty(), false);

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
    private final SetMultimap<Value, Binding> bindingMap;

    /**
     * A list of {@link OrderingPart}s where none of the contained expressions is equality-bound. This list
     * defines the actual order of records.
     */
    @Nonnull
    private final PartiallyOrderedSet<Value> orderingSet;

    private final boolean isDistinct;

    @Nonnull
    private final Supplier<SetMultimap<Value, Binding>> fixedBindingMapSupplier;

    private Ordering(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                     @Nonnull final PartiallyOrderedSet<Value> orderingSet,
                     final boolean isDistinct) {
        Debugger.sanityCheck(() -> {
            final var normalizedBindingMap = normalizeBindingMap(bindingMap);
            Verify.verify(bindingMap.equals(normalizedBindingMap));
            final var normalizedOrderingSet = normalizeOrderingSet(bindingMap, orderingSet);
            Verify.verify(orderingSet.equals(normalizedOrderingSet));
        });

        this.orderingSet = orderingSet;
        this.bindingMap = ImmutableSetMultimap.copyOf(bindingMap);
        this.isDistinct = isDistinct;
        this.fixedBindingMapSupplier = Suppliers.memoize(this::computeFixedBindingMap);
    }

    @Nonnull
    public SetMultimap<Value, Binding> getBindingMap() {
        return bindingMap;
    }

    @Nonnull
    public Set<Value> getEqualityBoundValues() {
        return getFixedBindingMap().keySet();
    }

    @Nonnull
    private SetMultimap<Value, Binding> getFixedBindingMap() {
        return fixedBindingMapSupplier.get();
    }

    @Nonnull
    private SetMultimap<Value, Binding> computeFixedBindingMap() {
        return ImmutableSetMultimap.copyOf(Multimaps.filterValues(getBindingMap(), Binding::isFixed));
    }

    @Nonnull
    public PartiallyOrderedSet<Value> getOrderingSet() {
        return orderingSet;
    }
    
    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isEmpty() {
        return bindingMap.isEmpty();
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
        return getBindingMap().equals(ordering.getBindingMap()) &&
               getOrderingSet().equals(ordering.getOrderingSet()) &&
               isDistinct() == ordering.isDistinct();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBindingMap(), getOrderingSet(), isDistinct());
    }

    @Override
    public String toString() {
        return "[" + (isDistinct ? "distinct " : " ") + orderingSet + "]";
    }

    @Nonnull
    public Iterable<List<Value>> enumerateSatisfyingComparisonKeyValues(@Nonnull final RequestedOrdering requestedOrdering) {
        if (requestedOrdering.isDistinct() && !isDistinct()) {
            return ImmutableList.of();
        }

        final var reducedRequestedOrderingValuesBuilder = ImmutableList.<Value>builder();
        for (final var requestedOrderingPart : requestedOrdering.getOrderingParts()) {
            if (!bindingMap.containsKey(requestedOrderingPart.getValue())) {
                return ImmutableList.of();
            }
            final var bindings = bindingMap.get(requestedOrderingPart.getValue());
            final var sortOrder = sortOrder(bindings);
            if (sortOrder.isDirectional() && sortOrder != requestedOrderingPart.getSortOrder()) {
                return ImmutableList.of();
            }

            if (sortOrder != SortOrder.FIXED) {
                reducedRequestedOrderingValuesBuilder.add(requestedOrderingPart.getValue());
            }
        }
        final var reducedRequestedOrderingValues = reducedRequestedOrderingValuesBuilder.build();

        // filter out all elements that only have fixed bindings
        final var filteredOrderingSet =
                orderingSet.filterElements(this::isDirectionalValue);

        return TopologicalSort.satisfyingPermutations(
                        filteredOrderingSet,
                        reducedRequestedOrderingValues,
                        Function.identity(),
                        permutation -> reducedRequestedOrderingValues.size());
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
    public Iterable<List<OrderingPart>> enumerateSatisfyingOrderings(@Nonnull final RequestedOrdering requestedOrdering) {
        if (requestedOrdering.isDistinct() && !isDistinct()) {
            return ImmutableList.of();
        }

        final var requestedOrderingValuesMapBuilder = ImmutableMap.<Value, OrderingPart>builder();
        for (final var requestedOrderingPart : requestedOrdering.getOrderingParts()) {
            if (!bindingMap.containsKey(requestedOrderingPart.getValue())) {
                return ImmutableList.of();
            }
            final var bindings = bindingMap.get(requestedOrderingPart.getValue());
            final var sortOrder = sortOrder(bindings);
            if (sortOrder.isDirectional() && sortOrder != requestedOrderingPart.getSortOrder()) {
                return ImmutableList.of();
            }

            requestedOrderingValuesMapBuilder.put(requestedOrderingPart.getValue(), requestedOrderingPart);
        }
        final var requestedOrderingValuesMap = requestedOrderingValuesMapBuilder.build();

        final var satisfyingValuePermutations =
                TopologicalSort.satisfyingPermutations(
                        getOrderingSet(),
                        requestedOrdering.getOrderingParts(),
                        value -> Objects.requireNonNull(requestedOrderingValuesMap.get(value)),
                        permutation -> requestedOrdering.getOrderingParts().size());
        return Iterables.transform(satisfyingValuePermutations,
                permutation -> permutation.stream()
                        .map(value -> Objects.requireNonNull(requestedOrderingValuesMap.get(value)))
                        .collect(ImmutableList.toImmutableList()));
    }

    public boolean satisfiesGroupingValues(@Nonnull final Set<Value> requestedGroupingValues) {
        if (requestedGroupingValues
                .stream()
                .anyMatch(groupingValue -> !bindingMap.containsKey(groupingValue))) {
            return false;
        }

        final var reducedRequestedOrderingValues =
                requestedGroupingValues.stream()
                        .filter(requestedGroupingValue -> isSingularDirectionalBinding(bindingMap.get(requestedGroupingValue)))
                        .collect(ImmutableSet.toImmutableSet());

        // no ordering left worth further considerations
        if (reducedRequestedOrderingValues.isEmpty()) {
            return true;
        }

        final var filteredOrderingSet =
                orderingSet.filterElements(value -> isSingularDirectionalBinding(bindingMap.get(value)));

        final var permutations = TopologicalSort.topologicalOrderPermutations(filteredOrderingSet);
        for (final var permutation : permutations) {
            final var containsAll =
                    reducedRequestedOrderingValues.containsAll(permutation.subList(0, reducedRequestedOrderingValues.size()));
            if (containsAll) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    public Ordering pullUp(@Nonnull Value value, @Nonnull AliasMap aliasMap, @Nonnull Set<CorrelationIdentifier> constantAliases) {
        final var pulledUpBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        for (final var entry : getBindingMap().asMap().entrySet()) {
            final var pulledUpBindings =
                    translateBindings(entry.getValue(),
                            toBePulledUpValues -> value.pullUp(toBePulledUpValues, aliasMap, constantAliases, Quantifier.current()));
            pulledUpBindingMapBuilder.putAll(entry.getKey(), pulledUpBindings);
        }

        // pull up the values we actually could also pull up some of the bindings for
        final var pulledUpBindingMap = pulledUpBindingMapBuilder.build();
        final var pulledUpValuesMap =
                value.pullUp(pulledUpBindingMap.keySet(), aliasMap, constantAliases, Quantifier.current());

        final var mappedOrderingSet = getOrderingSet().mapAll(pulledUpValuesMap);
        final var mappedValues = mappedOrderingSet.getSet();
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();

        for (final var entry : pulledUpValuesMap.entrySet()) {
            if (mappedValues.contains(entry.getValue())) {
                Verify.verify(pulledUpBindingMap.containsKey(entry.getKey()));
                bindingMapBuilder.putAll(entry.getValue(), pulledUpBindingMap.get(entry.getKey()));
            }
        }

        return Ordering.ofOrderingSet(bindingMapBuilder.build(), mappedOrderingSet, isDistinct());
    }

    @Nonnull
    public Ordering pushDown(@Nonnull Value value, @Nonnull AliasMap aliasMap, @Nonnull Set<CorrelationIdentifier> constantAliases) {
        final var pushedBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        for (final var entry : getBindingMap().asMap().entrySet()) {
            final var pushedBindings =
                    translateBindings(entry.getValue(),
                            toBePushedValues -> {
                                final var pushedDownValues =
                                        value.pushDown(toBePushedValues,
                                                DefaultValueSimplificationRuleSet.ofSimplificationRules(), aliasMap,
                                                constantAliases, Quantifier.current());
                                final var resultMap = new LinkedIdentityMap<Value, Value>();
                                for (int i = 0; i < toBePushedValues.size(); i++) {
                                    final Value toBePushedValue = toBePushedValues.get(i);
                                    final Value pushedValue = Objects.requireNonNull(pushedDownValues.get(i));
                                    resultMap.put(toBePushedValue, pushedValue);
                                }
                                return resultMap;
                            });
            pushedBindingMapBuilder.putAll(entry.getKey(), pushedBindings);
        }

        // pull up the values we actually could also pull up some of the the bindings for
        final var pushedBindingMap = pushedBindingMapBuilder.build();
        final var values = pushedBindingMap.keySet();
        final var pushedValues =
                value.pushDown(values, DefaultValueSimplificationRuleSet.ofSimplificationRules(),
                        aliasMap, constantAliases, Quantifier.current());

        final var pushedValuesMapBuilder = ImmutableMap.<Value, Value>builder();
        final var valuesIterator = values.iterator();
        final var pushedValuesIterator = pushedValues.iterator();
        while (valuesIterator.hasNext() && pushedValuesIterator.hasNext()) {
            pushedValuesMapBuilder.put(valuesIterator.next(), pushedValuesIterator.next());
        }
        Verify.verify(!valuesIterator.hasNext() && !pushedValuesIterator.hasNext());

        final var pushedValuesMap = pushedValuesMapBuilder.build();
        final var mappedOrderingSet = getOrderingSet().mapAll(pushedValuesMap);
        final var mappedValues = mappedOrderingSet.getSet();
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();

        for (final var entry : pushedValuesMap.entrySet()) {
            if (mappedValues.contains(entry.getValue())) {
                Verify.verify(pushedBindingMap.containsKey(entry.getKey()));
                bindingMapBuilder.putAll(entry.getValue(), pushedBindingMap.get(entry.getKey()));
            }
        }

        return Ordering.ofOrderingSet(bindingMapBuilder.build(), mappedOrderingSet, isDistinct());
    }

    @Nonnull
    public static SetMultimap<Value, Binding> bindingsForValues(@Nonnull final Collection<? extends Value> values,
                                                                @Nonnull final SortOrder sortOrder) {
        final var builder = ImmutableSetMultimap.<Value, Binding>builder();
        for (final var value : values) {
            builder.put(value, Binding.sorted(sortOrder));
        }
        return builder.build();
    }

    @Nonnull
    public Ordering applyComparisonKey(@Nonnull final List<? extends Value> comparisonKeyValues,
                                       @Nonnull final SetMultimap<Value, Binding> comparisonKeyBindingMap) {
        final var comparisonKeyOrderingSet =
                PartiallyOrderedSet.<Value>builder()
                        .addListWithDependencies(comparisonKeyValues)
                        .build();

        Debugger.sanityCheck(() -> Verify.verify(comparisonKeyOrderingSet.getSet().stream()
                .allMatch(otherValue -> orderingSet.getSet().stream().anyMatch(value -> value.equals(otherValue)))));

        final var resultBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        for (final Map.Entry<Value, Binding> entry : getBindingMap().entries()) {
            final var key = entry.getKey();
            if (!comparisonKeyBindingMap.containsKey(key)) {
                resultBindingMapBuilder.put(entry);
            } else {
                final var comparisonKeyBindings = comparisonKeyBindingMap.get(key);
                Verify.verify(comparisonKeyBindings.stream().noneMatch(Binding::isFixed));
                resultBindingMapBuilder.put(key, Iterables.getOnlyElement(comparisonKeyBindings));
            }
        }

        final var otherDependencyMap = comparisonKeyOrderingSet.getDependencyMap();
        final var resultDependencyMap =
                ImmutableSetMultimap.<Value, Value>builder()
                        .putAll(orderingSet.getDependencyMap())
                        .putAll(otherDependencyMap)
                        .build();
        final var resultOrderingSet = PartiallyOrderedSet.of(orderingSet.getSet(), resultDependencyMap);
        return Ordering.ofOrderingSet(resultBindingMapBuilder.build(), resultOrderingSet, isDistinct);
    }

    public boolean isDirectionalValue(@Nonnull final Value value) {
        Verify.verify(bindingMap.containsKey(value));
        final var bindings = bindingMap.get(value);
        if (isSingularDirectionalBinding(bindings)) {
            return true;
        }
        Debugger.sanityCheck(() -> Verify.verify(areAllBindingsFixed(bindingMap.get(value))));
        return false;
    }

    @Nonnull
    private static Set<Binding> translateBindings(@Nonnull final Collection<Binding> bindings,
                                                  @Nonnull final Function<List<Value>, Map<Value, Value>> translateFunction) {
        final var translatedBindingsBuilder = ImmutableSet.<Binding>builder();

        if (areAllBindingsFixed(bindings)) {
            final var toBeTranslatedValues = ImmutableList.<Value>builder();
            for (final var binding : bindings) {
                final var comparison = binding.getComparison();
                if (comparison instanceof Comparisons.ValueComparison) {
                    final var valueComparison = (Comparisons.ValueComparison)comparison;
                    toBeTranslatedValues.add(valueComparison.getValue());
                }
            }
            final var translationMap = translateFunction.apply(toBeTranslatedValues.build());
            for (final var binding : bindings) {
                final var comparison = binding.getComparison();
                if (comparison instanceof Comparisons.ValueComparison) {
                    final var valueComparison = (Comparisons.ValueComparison)comparison;
                    if (translationMap.containsKey(valueComparison.getValue())) {
                        final var translatedComparison =
                                new Comparisons.ValueComparison(valueComparison.getType(),
                                        translationMap.get(valueComparison.getValue()));
                        translatedBindingsBuilder.add(Binding.fixed(translatedComparison));
                    }
                } else {
                    translatedBindingsBuilder.add(binding);
                }
            }
        } else {
            translatedBindingsBuilder.add(Binding.sorted(sortOrder(bindings)));
        }

        return translatedBindingsBuilder.build();
    }

    /**
     * Method to compute the {@link PartiallyOrderedSet} representing this {@link Ordering}.
     * <br>
     * An ordering expresses the order of e.g. fields {@code a, b, x} and additionally declares some fields, e.g. {@code c}
     * to be equal-bound to a value (e.g. {@code 5}). That means that {@code c} can freely move in the order declaration of
     * {@code a, b, x} and satisfy {@link RequestedOrdering}s such as e.g. {@code a, c, b, x}, {@code a, b, x, c}
     * or similar.
     * <br>
     * Generalizing this idea, for this example we can also say that in this case the plan is ordered by
     * {@code c} as well as all the values for {@code c} are identical. Generalizing further, a plan, or by extension,
     * a stream of data can actually be ordered by many things at the same time. For instance, a stream of four
     * fields {@code a, b, x, y} can be ordered by {@code a, b} and {@code x, y} at the same time (consider e.g. that
     * {@code x = 10 * a; y = 10 *b}. Both of these orderings are equally correct and representative.
     * <br>
     * Based on these two independent orderings we can construct new orderings that are also correct:
     * {@code a, b, x, y}, {@code a, x, b, y}, or {@code x, y, a, b}, among others.
     * <br>
     * In order to properly capture this multitude of orderings, we can use partial orders (see {@link PartiallyOrderedSet})
     * to define the ordering (unfortunate name clash). For our example, we can write
     * <pre>
     * {@code
     * PartiallyOrderedSet([a, b, x, y], [a < b, x < y])
     * }
     * </pre>
     *
     * and mean all topologically correct permutations of {@code a, b, x, y}.
     *
     * @param bindingMap a normalized binding map
     * @param orderingValues a list of ordering {@link Value}s
     * @return a {@link PartiallyOrderedSet} for this ordering
     */
    @Nonnull
    private static PartiallyOrderedSet<Value> computeFromOrderingSequence(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                                                          @Nonnull final List<Value> orderingValues) {
        final var filteredOrderingValues =
                orderingValues.stream()
                        .peek(orderingValue -> Verify.verify(bindingMap.containsKey(orderingValue)))
                        .filter(orderingValue -> bindingMap.get(orderingValue).stream().anyMatch(Binding::isFixed))
                        .collect(ImmutableList.toImmutableList());

        return PartiallyOrderedSet.<Value>builder()
                .addAll(bindingMap.keySet())
                .addListWithDependencies(filteredOrderingValues)
                .build();
    }

    @Nonnull
    private static ImmutableSetMultimap<Value, Binding> normalizeBindingMap(@Nonnull final SetMultimap<Value, Binding> bindingMap) {
        final var normalizedBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        for (final Value value : bindingMap.keySet()) {
            final boolean isFixed = areAllBindingsFixed(bindingMap.get(value));
            final var bindings = bindingMap.get(value);
            SortOrder seenSortOrder = null;
            for (final Binding binding : bindings) {
                if (seenSortOrder != null) {
                    switch (binding.getSortOrder()) {
                        case ASCENDING:
                            Verify.verify(!isFixed);
                            Verify.verify(seenSortOrder != SortOrder.DESCENDING);
                            // We have seen an ASCENDING binding already -- skip
                            break;
                        case DESCENDING:
                            Verify.verify(!isFixed);
                            Verify.verify(seenSortOrder != SortOrder.ASCENDING);
                            // We have seen a DESCENDING binding already -- skip
                            break;
                        case FIXED:
                            //
                            // If it is not fixed there will be an ASCENDING or DESCENDING as well, so we don't want to
                            // add the fixed binding at all.
                            //
                            if (isFixed) {
                                normalizedBindingMapBuilder.put(value, binding);
                            }
                            break;
                        default:
                            throw new RecordCoreException("unknown binding");
                    }
                } else {
                    switch (binding.getSortOrder()) {
                        case ASCENDING:
                            Verify.verify(!isFixed);
                            normalizedBindingMapBuilder.put(value, binding);
                            break;
                        case DESCENDING:
                            Verify.verify(!isFixed);
                            normalizedBindingMapBuilder.put(value, binding);
                            break;
                        case FIXED:
                            //
                            // If it is not fixed there will be an ASCENDING or DESCENDING as well, so we don't want to
                            // add the fixed binding at all.
                            //
                            if (isFixed) {
                                normalizedBindingMapBuilder.put(value, binding);
                            }
                            break;
                        default:
                            throw new RecordCoreException("unknown binding");
                    }

                    seenSortOrder = binding.getSortOrder();
                }
            }
        }
        return normalizedBindingMapBuilder.build();
    }

    /**
     * Method to <em>normalize</em> a partially-ordered set representing an ordering, that is, it removes all
     * dependencies from or to a particular element contained in the set if that element is also equality-bound. If an
     * element is equality-bound, that is, it is constant for all practical purposes, it is in also independent with
     * respect to all other elements in the set.
     * @param bindingMap a multimap relating values and equality comparisons
     * @param orderingSet a partially ordered set representing the ordering set of an ordering
     * @return a new (normalized) partially ordered set representing the dependencies between elements in an ordering
     */
    @Nonnull
    private static PartiallyOrderedSet<Value> normalizeOrderingSet(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                                                   @Nonnull final PartiallyOrderedSet<Value> orderingSet) {
        final var transitiveClosure = orderingSet.getTransitiveClosure();
        final var normalizedDependencyMapBuilder = ImmutableSetMultimap.<Value, Value>builder();

        for (final var dependency : transitiveClosure.entries()) {
            Verify.verify(bindingMap.containsKey(dependency.getKey()));
            Verify.verify(bindingMap.containsKey(dependency.getValue()));
            final boolean isFixed = areAllBindingsFixed(bindingMap.get(dependency.getKey())) ||
                    areAllBindingsFixed(bindingMap.get(dependency.getValue()));
            if (!isFixed) {
                normalizedDependencyMapBuilder.put(dependency);
            }
        }
        return PartiallyOrderedSet.of(orderingSet.getSet(), normalizedDependencyMapBuilder.build());
    }

    private static boolean areAllBindingsFixed(@Nonnull final Collection<Binding> bindings) {
        return bindings.stream().allMatch(Binding::isFixed);
    }

    private static boolean isSingularDirectionalBinding(@Nonnull final Collection<Binding> bindings) {
        Verify.verify(!bindings.isEmpty());
        if (bindings.size() == 1) {
            return Iterables.getOnlyElement(bindings).getSortOrder().isDirectional();
        }
        return false;
    }

    private static SortOrder sortOrder(@Nonnull final Collection<Binding> bindings) {
        Verify.verify(!bindings.isEmpty());

        if (isSingularDirectionalBinding(bindings)) {
            return Iterables.getOnlyElement(bindings).getSortOrder();
        }

        if (areAllBindingsFixed(bindings)) {
            return SortOrder.FIXED;
        }

        throw new RecordCoreException("inconsistent ordering state");
    }

    @Nonnull
    @SuppressWarnings("java:S135")
    public static Ordering merge(@Nonnull final Iterable<Ordering> orderings,
                                 @Nonnull final BinaryOperator<Set<Binding>> combineOperator,
                                 @Nonnull final BiPredicate<Ordering, Ordering> isDistinctPredicate) {
        return Streams.stream(orderings)
                .reduce((left, right) -> merge(left, right, combineOperator, isDistinctPredicate.test(left, right)))
                .orElseThrow(() -> new IllegalStateException("must have an ordering"));
    }

    /**
     * Method to combine a list of {@link Ordering}s into one {@link Ordering}. This method is e.g. used
     * to establish a comparison key that reasons about the ordering in the context of planning for a distinct set
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
     * @param left an {@link Ordering}
     * @param right an {@link Ordering}
     * @param combineOperator a combine operator to combine two sets of {@link SortOrder#FIXED} bindings.
     * @param isDistinct indicator if the resulting order is thought to be distinct
     * @return an {@link Ordering}
     */
    @Nonnull
    @SuppressWarnings("java:S135")
    public static Ordering merge(@Nonnull final Ordering left,
                                 @Nonnull final Ordering right,
                                 @Nonnull final BinaryOperator<Set<Binding>> combineOperator,
                                 final boolean isDistinct) {
        final var leftOrderingSet = left.getOrderingSet();
        final var rightOrderingSet = right.getOrderingSet();
        final var leftDependencies = leftOrderingSet.getDependencyMap();
        final var rightDependencies = rightOrderingSet.getDependencyMap();
        final var leftBindingMap = left.getBindingMap();
        final var rightBindingMap = right.getBindingMap();

        final var elementsBuilder = ImmutableSet.<Value>builder();
        final var dependencyBuilder = ImmutableSetMultimap.<Value, Value>builder();
        final var bindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();

        var leftEligibleSet = leftOrderingSet.eligibleSet();
        var rightEligibleSet = rightOrderingSet.eligibleSet();

        Set<Value> lastElements = ImmutableSet.of();
        while (!leftEligibleSet.isEmpty() && !rightEligibleSet.isEmpty()) {
            final var leftElements = leftEligibleSet.eligibleElements();
            final var rightElements = rightEligibleSet.eligibleElements();

            //final var intersectedElements = Sets.intersection(leftElements, rightElements);

            //
            // "Intersect" the left elements with the right elements. Test their bindings for compatibility.
            //
            final var intersectedElementsBuilder = ImmutableSet.<Value>builder();
            for (final var leftElement : leftElements) {
                for (final var rightElement : rightElements) {
                    if (leftElement.equals(rightElement)) {
                        final var intersectedBindings =
                                combineBindings(leftBindingMap.get(leftElement),
                                        rightBindingMap.get(rightElement), combineOperator);
                        if (!intersectedBindings.isEmpty()) {
                            intersectedElementsBuilder.add(leftElement);
                            elementsBuilder.add(leftElement);
                            bindingMapBuilder.putAll(leftElement, intersectedBindings);
                        }
                    }
                }
            }

            final var intersectedElements = intersectedElementsBuilder.build();
            if (intersectedElements.isEmpty()) {
                break;
            }

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

        final var orderingSet =
                PartiallyOrderedSet.of(elementsBuilder.build(), dependencyBuilder.build());
        return Ordering.ofOrderingSet(bindingMapBuilder.build(), orderingSet, isDistinct);
    }

    @Nonnull
    private static Set<Binding> combineBindings(@Nonnull final Set<Binding> leftBindings,
                                                @Nonnull final Set<Binding> rightBindings,
                                                @Nonnull final BinaryOperator<Set<Binding>> combineOperator) {
        final var leftSortOrder = sortOrder(leftBindings);
        final var rightSortOrder = sortOrder(rightBindings);

        if (leftSortOrder.isDirectional() && rightSortOrder.isDirectional()) {
            if (leftSortOrder != rightSortOrder) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(Binding.sorted(leftSortOrder));
        }

        if (leftSortOrder.isDirectional() && rightSortOrder == SortOrder.FIXED) {
            return ImmutableSet.of(Binding.sorted(leftSortOrder));
        }
        if (leftSortOrder == SortOrder.FIXED && rightSortOrder.isDirectional()) {
            return ImmutableSet.of(Binding.sorted(rightSortOrder));
        }

        // delegate to the combine operator to deal with the fixed bindings
        return combineOperator.apply(leftBindings, rightBindings);
    }

    /**
     * Helper method to concatenate the ordering key parts of the participating orderings in iteration order.
     * @param orderings a collection of orderings
     * @param combineFn a combine function to combine two maps of {@link SortOrder#FIXED} bindings.
     * @return a new ordering representing a concatenation of the given left and right ordering
     */
    @Nonnull
    public static Ordering concatOrderings(@Nonnull final Collection<Ordering> orderings,
                                           @Nonnull final BinaryOperator<SetMultimap<Value, Binding>> combineFn) {

        return orderings.stream()
                .reduce((left, right) -> concatOrderings(left, right, combineFn))
                .orElseThrow(() -> new RecordCoreException("unable to concatenate orderings"));
    }

    /**
     * Helper method to concatenate the ordering key parts of the participating orderings in iteration order.
     * @param leftOrdering an {@link Ordering}
     * @param rightOrdering another {@link Ordering} to be concatenated to {@code leftOrdering}
     * @param combineFn a combine function to combine two maps of equality-bound keys (and their bindings)
     * @return a new {@link Ordering}
     */
    @Nonnull
    public static Ordering concatOrderings(@Nonnull final Ordering leftOrdering,
                                           @Nonnull final Ordering rightOrdering,
                                           @Nonnull final BinaryOperator<SetMultimap<Value, Binding>> combineFn) {
        final var leftOrderingSet = leftOrdering.getOrderingSet();
        final var rightOrderingSet = rightOrdering.getOrderingSet();

        Verify.verify(leftOrdering.isDistinct());
        Debugger.sanityCheck(() ->
                Verify.verify(
                        Sets.intersection(leftOrderingSet.getSet(), rightOrderingSet.getSet()).isEmpty()));

        final var orderingElements =
                ImmutableSet.<Value>builder()
                        .addAll(leftOrderingSet.getSet())
                        .addAll(rightOrderingSet.getSet())
                        .build();

        final var dependencyMapBuilder =
                ImmutableSetMultimap.<Value, Value>builder()
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

        final var combinedEqualityBoundValueMap =
                combineFn.apply(leftOrdering.getBindingMap(), rightOrdering.getBindingMap());
        return Ordering.ofOrderingSet(combinedEqualityBoundValueMap, concatenatedOrderingSet, rightOrdering.isDistinct());
    }


    /**
     * Union the bindings of a {@link Value} common to two orderings. This method is usually passed in as a method
     * reference to {@link #merge(Iterable, BinaryOperator, BiPredicate)} as the binary operator.
     * @param left set of bindings of the left ordering
     * @param right set of bindings of the right ordering
     * @return newly combined set of bindings
     */
    @Nonnull
    public static Set<Binding> unionBindings(@Nonnull final Set<Binding> left,
                                             @Nonnull final Set<Binding> right) {
        Debugger.sanityCheck(() -> {
            Verify.verify(areAllBindingsFixed(left));
            Verify.verify(areAllBindingsFixed(right));
        });
        return ImmutableSet.copyOf(Sets.union(left, right));
    }

    /**
     * Union the bindings common to two orderings. This method is usually passed in as a method
     * reference to {@link #concatOrderings(Collection, BinaryOperator)} as the binary operator.
     * @param left multimap of bindings of the left ordering
     * @param right multimap of bindings of the right ordering
     * @return newly combined multimap of bindings
     */
    @Nonnull
    public static SetMultimap<Value, Binding> unionBindingMaps(@Nonnull final SetMultimap<Value, Binding> left,
                                                               @Nonnull final SetMultimap<Value, Binding> right) {
        return ImmutableSetMultimap.<Value, Binding>builder()
                .putAll(left)
                .putAll(right)
                .build();
    }

    /**
     * Intersect the bindings of a {@link Value} common to two orderings. This method is usually passed in as a method
     * reference to {@link #merge(Iterable, BinaryOperator, BiPredicate)} as the binary operator.
     * @param left set of bindings of the left ordering
     * @param right set of bindings of the right ordering
     * @return newly combined set of bindings
     */
    @Nonnull
    public static Set<Binding> intersectBindings(@Nonnull final Set<Binding> left,
                                                 @Nonnull final Set<Binding> right) {
        Debugger.sanityCheck(() -> {
            Verify.verify(areAllBindingsFixed(left));
            Verify.verify(areAllBindingsFixed(right));
        });
        return ImmutableSet.copyOf(Sets.intersection(left, right));
    }

    @Nonnull
    public static Ordering emptyOrdering() {
        return EMPTY;
    }

    @Nonnull
    public static Ordering ofOrderingSet(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                         @Nonnull final PartiallyOrderedSet<Value> orderingSet,
                                         final boolean isDistinct) {
        // TODO normalization should not be required as a blanket cleansing step for all calls to this method
        final var normalizedBindingMap = normalizeBindingMap(bindingMap);
        return new Ordering(normalizedBindingMap, normalizeOrderingSet(normalizedBindingMap, orderingSet), isDistinct);
    }

    @Nonnull
    public static Ordering ofOrderingSequence(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                              @Nonnull final List<Value> orderingAsList,
                                              final boolean isDistinct) {
        final var normalizedBindingMap = normalizeBindingMap(bindingMap);
        return new Ordering(normalizedBindingMap, computeFromOrderingSequence(normalizedBindingMap, orderingAsList), isDistinct);
    }

    /**
     * TODO.
     */
    public static class Binding {
        @Nonnull
        private final SortOrder sortOrder;

        /**
         * Comparison is set if {@code sortOrder} is set to {@link SortOrder#FIXED}, {@code null} otherwise.
         */
        @Nullable
        private final Comparison comparison;

        private Binding(@Nonnull final SortOrder sortOrder, @Nullable final Comparison comparison) {
            this.sortOrder = sortOrder;
            this.comparison = comparison;
        }

        @Nonnull
        public SortOrder getSortOrder() {
            return sortOrder;
        }

        boolean isFixed() {
            return sortOrder == SortOrder.FIXED;
        }

        @Nullable
        public Comparison getComparison() {
            return comparison;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Binding)) {
                return false;
            }
            final Binding binding = (Binding)o;
            return sortOrder == binding.sortOrder && Objects.equals(comparison, binding.comparison);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sortOrder, comparison);
        }

        @Nonnull
        public static Binding ascending() {
            return sorted(SortOrder.ASCENDING);
        }

        @Nonnull
        public static Binding descending() {
            return sorted(SortOrder.DESCENDING);
        }

        @Nonnull
        public static Binding sorted(@Nonnull final SortOrder sortOrder) {
            Verify.verify(sortOrder == SortOrder.ASCENDING ||
                    sortOrder == SortOrder.DESCENDING);
            return new Binding(sortOrder, null);
        }

        @Nonnull
        public static Binding fixed(@Nonnull final Comparison comparison) {
            return new Binding(SortOrder.FIXED, comparison);
        }
    }
}
