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
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This class captures an ordering property.
 * <br>
 * Traditionally, an ordering is a list of ordering parts that define the actual ordering of
 * records that flow at runtime of a plan. Modelling an ordering in the planner using a list of ordering parts,
 * however, turns out to be inadequate.
 * <br>
 * Most plans are ordered in many different ways at the same time:
 * <ul>
 *     <li>
 *          A result set satisfying an ordering of {@code a, b, c} is automatically also ordered by {@code a, b} and
 *          {@code a}.
 *     </li>
 *     <li>
 *          A result set satisfying an ordering of {@code a, b, c} where {@code a} is bound to a e.g. value {@code 3}
 *          is automatically also ordered by {@code b, a, c} and {@code b, c, a} (in addition to the orderings above).
 *          In a sense the ordering part for {@code a} can be freely moved withing the ordering (as long as {@code b}
 *          comes before {@code c}.
 *     </li>
 *     <li>
 *          A result set satisfying an ordering of {@code a, b, c} where {@code d := 2 * b} is automatically also
 *          ordered by {@code a, d, c} (in addition to the orderings above).
 *     </li>
 * </ul>
 * <br>
 * It turns out that an ordering is much more naturally represented by a {@link PartiallyOrderedSet} that can model
 * the different dependencies between the ordering parts. A more formal definition for the dependencies in that
 * partially ordered set is:
 * <ul>
 *     <li>
 *         The set the partially-ordered set is based on {@link Value}s that are visible in the result set of an
 *         operation.
 *     </li>
 *     <li>
 *         The partially-ordered set contains a dependency from {@code b} to {@code a}, denoted by {@code a ← b},
 *         iff for a sorted {@code a}, for a run of fixed constant values for {@code a}, {@code b} is ascending or
 *         descending, i.e. if {@code b} is imposing some sort of ordering given a prefix {@code a}. If the
 *         partially-ordered set does not encode such a dependency from {@code b} to another {@link Value},
 *         {@code b} is considered to be globally sorted (ascending or descending).
 *     </li>
 *     <li>
 *         {@link Value}s that do not contribute to the ordering of the records in the result set of an operation
 *         are not contained in the partially-ordered set nor in its dependency map.
 *     </li>
 * </ul>
 * Using a partially-ordered set, we can enumerate all traditional orderings by enumerating all topologically-sound
 * permutations of that partially-ordered set.
 * <br>
 * In addition to just using a {@link PartiallyOrderedSet} of {@link Value}s, we also keep a multimap for the domain of
 * those {@link Value}s to {@link Binding}s. A {@link Binding} indicates if a {@link Value} is ascending, descending,
 * or if it is bound to one or multiple special {@link Value} via a {@link Comparison}.
 * <br>
 * Instances of this class are used to communicate properties of plans.
 */
public class Ordering {
    @Nonnull
    private static final Ordering EMPTY = new Ordering(ImmutableSetMultimap.of(), PartiallyOrderedSet.empty(),
            false, (b, o) -> { } );

    /**
     * Union merge operator to be used to obtain the resulting order of a distinct ordered union operation,
     * or an in-union operation.
     */
    public static final MergeOperator<Union> UNION = new MergeOperator<>() {
        @Nonnull
        @Override
        public Set<Binding> combineBindings(@Nonnull final Set<Binding> leftBindings, @Nonnull final Set<Binding> rightBindings) {
            return combineBindingsForUnion(leftBindings, rightBindings);
        }

        @Nonnull
        @Override
        public Union createOrdering(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                    @Nonnull final PartiallyOrderedSet<Value> orderingSet,
                                    final boolean isDistinct) {
            return new Union(bindingMap, orderingSet, isDistinct);
        }
    };

    /**
     * Intersection merge operator to be used to obtain the resulting order of a distinct intersection operation.
     */
    public static final MergeOperator<Intersection> INTERSECTION = new MergeOperator<>() {
        @Nonnull
        @Override
        public Set<Binding> combineBindings(@Nonnull final Set<Binding> leftBindings, @Nonnull final Set<Binding> rightBindings) {
            return combineBindingsForIntersection(leftBindings, rightBindings);
        }

        @Nonnull
        @Override
        public Intersection createOrdering(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                           @Nonnull final PartiallyOrderedSet<Value> orderingSet,
                                           final boolean isDistinct) {
            //
            // Unlike for union, we need to normalize the ordering set as values that were dependent on other
            // values in the participating orderings prior to the intersection can become independent due to a
            // stronger fixed binding on the other side.
            //
            return new Intersection(bindingMap, Ordering.normalizeOrderingSet(bindingMap, orderingSet), isDistinct);
        }
    };

    /**
     * Multimap from {@link Value} to a set of {@link Comparison}s to capture all expressions that are
     * bound through equality. This needs to be a multimap to accommodate for the case where an expression is
     * bound multiple times independently and where it is not immediately clear that both bound locations are
     * redundant or contradictory. For instance {@code x = 5} and {@code x = 6} together are effectively a
     * contradiction causing a predicate to always evaluate to {@code false}. In other cases, we may encounter
     * {@code x = 5} and {@code x = $p} where it is unclear if we just encountered a contradiction as well or
     * if the predicate is just redundant (where {@code $p} is bound to {@code 5} when the query is executed).
     * {@link SetOperationsOrdering}s.
     */
    @Nonnull
    private final SetMultimap<Value, Binding> bindingMap;

    /**
     * A {@link PartiallyOrderedSet} of {@link Value}s.
     */
    @Nonnull
    private final PartiallyOrderedSet<Value> orderingSet;

    /**
     * Indicator if the records flowed are to be considered distinct, thus producing a strict order. This field
     * should get deprecated as it is not correct to assign this property to all enumerated orderings. For instance,
     * an ordering that produces {@code a, b, c} is also ordered by {@code a, b}. While the former one might be strict,
     * the latter one may not. I think for now we should just interpret this indicator in a way that only enumerated
     * orderings that contain all values of the ordering are strict if this indicator is {@code true}.
     */
    private final boolean isDistinct;

    @Nonnull
    private final Supplier<SetMultimap<Value, Binding>> fixedBindingMapSupplier;

    /**
     * Primary constructor. Protected from the outside world.
     * @param bindingMap a multimap of bindings
     * @param orderingSet a {@link PartiallyOrderedSet} of {@link Value}s
     * @param isDistinct an indicator if this ordering is strict
     * @param sanityCheckConsumer a consumer that is executed in an insane environment
     */
    protected Ordering(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                       @Nonnull final PartiallyOrderedSet<Value> orderingSet,
                       final boolean isDistinct,
                       @Nonnull final BiConsumer<SetMultimap<Value, Binding>, PartiallyOrderedSet<Value>> sanityCheckConsumer) {
        Debugger.sanityCheck(() -> sanityCheckConsumer.accept(bindingMap, orderingSet));

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
        return "[" + (isDistinct ? "distinct " : "") + orderingSet + "; bindings: " + bindingMap + "]";
    }

    /**
     * Method to derive {@link RequestedOrdering}s from this {@link Ordering}. This logic is needed when a set operation
     * uses the provided ordering of one leg of the set operation to find compatible orderings among the other legs of
     * that operation.
     * @param requestedOrdering the {@link RequestedOrdering} of the set operation itself
     * @return a set of {@link RequestedOrdering}s that can be pushed down the legs of the set operation
     */
    @Nonnull
    public Set<RequestedOrdering> deriveRequestedOrderings(@Nonnull final RequestedOrdering requestedOrdering) {
        if (requestedOrdering.isDistinct() && !isDistinct()) {
            return ImmutableSet.of();
        }

        final var satisfyingEnumeratedOrderings = enumerateCompatibleRequestedOrderings(requestedOrdering);
        return Streams.stream(satisfyingEnumeratedOrderings)
                .map(keyParts -> new RequestedOrdering(keyParts, RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS))
                .collect(ImmutableSet.toImmutableSet());
    }

    public boolean satisfies(@Nonnull RequestedOrdering requestedOrdering) {
        return !Iterables.isEmpty(enumerateCompatibleRequestedOrderings(requestedOrdering));
    }

    @Nonnull
    public Iterable<List<RequestedOrderingPart>> enumerateCompatibleRequestedOrderings(@Nonnull final RequestedOrdering requestedOrdering) {
        if (requestedOrdering.isDistinct() && !isDistinct()) {
            return ImmutableList.of();
        }

        final var requestedOrderingValuesBuilder = ImmutableList.<Value>builder();
        final var requestedOrderingValuesMapBuilder = ImmutableMap.<Value, RequestedOrderingPart>builder();
        for (final var requestedOrderingPart : requestedOrdering.getOrderingParts()) {
            if (!bindingMap.containsKey(requestedOrderingPart.getValue())) {
                return ImmutableList.of();
            }
            final var bindings = bindingMap.get(requestedOrderingPart.getValue());
            final var sortOrder = sortOrder(bindings);
            if (!sortOrder.isCompatibleWithRequestedSortOrder(requestedOrderingPart.getSortOrder())) {
                return ImmutableList.of();
            }

            requestedOrderingValuesBuilder.add(requestedOrderingPart.getValue());
            requestedOrderingValuesMapBuilder.put(requestedOrderingPart.getValue(), requestedOrderingPart);
        }
        final var requestedOrderingValuesMap = requestedOrderingValuesMapBuilder.build();

        final var satisfyingValuePermutations =
                TopologicalSort.satisfyingPermutations(
                        getOrderingSet(),
                        ImmutableList.copyOf(requestedOrderingValuesMap.keySet()),
                        Function.identity(),
                        permutation -> requestedOrdering.getOrderingParts().size());
        return Iterables.transform(satisfyingValuePermutations,
                permutation -> permutation.stream()
                        .map(value -> {
                            final var bindings = bindingMap.get(value);
                            if (areAllBindingsFixed(bindings)) {
                                return new RequestedOrderingPart(value, RequestedSortOrder.ANY);
                            }
                            return new RequestedOrderingPart(value, sortOrder(bindings).toRequestedSortOrder());
                        })
                        .collect(ImmutableList.toImmutableList()));
    }

    public boolean satisfiesGroupingValues(@Nonnull final Set<Value> requestedGroupingValues) {
        // no ordering left worth further considerations
        if (requestedGroupingValues.isEmpty()) {
            return true;
        }

        if (orderingSet.size() < requestedGroupingValues.size()) {
            return false;
        }

        if (requestedGroupingValues
                .stream()
                .anyMatch(requestedGroupingValue -> {
                    if (!bindingMap.containsKey(requestedGroupingValue)) {
                        return true;
                    }
                    final var bindings = bindingMap.get(requestedGroupingValue);
                    return areAllBindingsFixed(bindings) && hasMultipleFixedBindings(bindings);
                })) {
            return false;
        }

        final var permutations = TopologicalSort.topologicalOrderPermutations(orderingSet);
        for (final var permutation : permutations) {
            final var containsAll =
                    requestedGroupingValues.containsAll(permutation.subList(0, requestedGroupingValues.size()));
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
    public static SetMultimap<Value, Binding> sortedBindingsForValues(@Nonnull final Collection<? extends Value> values,
                                                                      @Nonnull final ProvidedSortOrder sortOrder) {
        final var builder = ImmutableSetMultimap.<Value, Binding>builder();
        for (final var value : values) {
            builder.put(value, Binding.sorted(sortOrder));
        }
        return builder.build();
    }

    public boolean isSingularDirectionalValue(@Nonnull final Value value) {
        Verify.verify(bindingMap.containsKey(value));
        final var bindings = bindingMap.get(value);
        if (isSingularDirectionalBinding(bindings)) {
            return true;
        }
        Debugger.sanityCheck(() -> Verify.verify(areAllBindingsFixed(bindingMap.get(value))));
        return false;
    }

    public boolean isSingularFixedValue(@Nonnull final Value value) {
        Verify.verify(bindingMap.containsKey(value));
        final var bindings = bindingMap.get(value);
        return areAllBindingsFixed(bindings) && !hasMultipleFixedBindings(bindings);
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
     * Method to compute the {@link PartiallyOrderedSet} based on a binding map and a sequence of {@link Value}s.
     * <br>
     * The partially ordered set is created in a way that we insert a dependency for each element and it predecessor in
     * the ordering sequence that is passed in. For example, passing in {@code [a, b, c]} as ordering sequence without
     * any fixed bindings we create a partially ordered set {@code (a, b, c), (a ← b, b ← c)}.
     *
     * @param bindingMap a binding map that is used to avoid creating dependencies for fixed bindings
     * @param orderingValues a list of ordering {@link Value}s
     * @return a {@link PartiallyOrderedSet} for this ordering
     */
    @Nonnull
    private static PartiallyOrderedSet<Value> computeFromOrderingSequence(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                                                          @Nonnull final List<? extends Value> orderingValues) {
        final var filteredOrderingValues =
                orderingValues.stream()
                        .peek(orderingValue -> Verify.verify(bindingMap.containsKey(orderingValue)))
                        .filter(orderingValue -> bindingMap.get(orderingValue).stream().noneMatch(Binding::isFixed))
                        .collect(ImmutableList.toImmutableList());

        return PartiallyOrderedSet.<Value>builder()
                .addAll(bindingMap.keySet())
                .addListWithDependencies(filteredOrderingValues)
                .build();
    }

    /**
     * Helper method to normalize the binding map handed in. The steps to normalize a binding map can be summarized
     * as follows:
     * <ul>
     *     <li>
     *         If the binding set of a value contains only a directional binding, that binding is added to the
     *         resulting biding map (for that value).
     *     </li>
     *     <li>
     *         If the binding set of a value contains only fixed bindings, these fixed bindings are added to the
     *         resulting biding map (for that value).
     *     </li>
     *     <li>
     *         If the binding set of a value contains a directional binding and otherwise only fixed bindings, the
     *         directional binding is added to the resulting binding map (for that value). Thus the binding set is
     *         promoted to the directional binding.
     *     </li>
     *     <li>
     *         We verify that the binding set of a value does not contain multiple directional bindings (they must
     *         be different in their sort order as the bindings for a value are represented by a set, i.e. it is
     *         impossible to see e.g. multiple {@code ASCENDING}s in the binding set.).
     *     </li>
     * </ul>
     * <br>
     * Callers that create orderings are supposed to ensure that the binding map is already normalized. This method
     * just double-checks that that is indeed the case (as part of a sanity check). This method is public and can be
     * called by a client prior to creating an ordering, however, without a proper reason it is discouraged to do so
     * as that may lead to poorly understood conditions in the callers logic.
     * @param bindingMap a binding map that is potentially not normalized.
     * @return a normalized binding map which may just be the copy of the binding map passed in if that binding had
     *         already been normalized
     */
    @Nonnull
    public static ImmutableSetMultimap<Value, Binding> normalizeBindingMap(@Nonnull final SetMultimap<Value, Binding> bindingMap) {
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
                            Verify.verify(seenSortOrder != ProvidedSortOrder.DESCENDING);
                            if (seenSortOrder != ProvidedSortOrder.ASCENDING) {
                                // Not seen an ASCENDING binding already
                                normalizedBindingMapBuilder.put(value, binding);
                            }
                            break;
                        case DESCENDING:
                            Verify.verify(!isFixed);
                            Verify.verify(seenSortOrder != ProvidedSortOrder.ASCENDING);
                            if (seenSortOrder != ProvidedSortOrder.DESCENDING) {
                                // Not seen an DESCENDING binding already
                                normalizedBindingMapBuilder.put(value, binding);
                            }
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
                        case CHOOSE:
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
     * dependencies from or to a particular element contained in the set if that element is also has a binding that is
     * fixed. If an element is fixed, that is, it is constant for all practical purposes, it is in also independent with
     * respect to all other elements in the set.
     * @param bindingMap a multimap relating values and equality comparisons
     * @param orderingSet a partially ordered set representing the ordering set of an ordering
     * @return a new (normalized) partially ordered set representing the dependencies between elements in an ordering
     */
    @Nonnull
    public static PartiallyOrderedSet<Value> normalizeOrderingSet(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                                                  @Nonnull final PartiallyOrderedSet<Value> orderingSet) {
        final var normalizedDependencyMapBuilder = ImmutableSetMultimap.<Value, Value>builder();
        final var dependencyMap = orderingSet.getDependencyMap().asMap();
        for (final var dependencySetEntry : dependencyMap.entrySet()) {
            //
            // If the current value is not fixed check its dependencies, completely skip the dependency if this value
            // is fixed.
            //
            if (!areAllBindingsFixed(Objects.requireNonNull(bindingMap.get(dependencySetEntry.getKey())))) {
                // follow all dependencies to values that are not fixed anymore
                final var toBeProcessed = new ArrayDeque<>(dependencySetEntry.getValue());
                while (!toBeProcessed.isEmpty()) {
                    final var dependentValue = toBeProcessed.removeFirst();
                    if (!areAllBindingsFixed(Objects.requireNonNull(bindingMap.get(dependentValue)))) {
                        // add a direct dependency from the original value to this one
                        normalizedDependencyMapBuilder.put(dependencySetEntry.getKey(), dependentValue);
                    } else {
                        // this one is fixed; we have to skip it and look at its dependencies as well
                        final var transitivelyDependentValues = dependencyMap.get(dependentValue);
                        if (transitivelyDependentValues != null) {
                            toBeProcessed.addAll(transitivelyDependentValues);
                        }
                    }
                }
            }
        }
        return PartiallyOrderedSet.of(orderingSet.getSet(), normalizedDependencyMapBuilder.build());
    }

    public static boolean areAllBindingsFixed(@Nonnull final Collection<Binding> bindings) {
        return bindings.stream().allMatch(Binding::isFixed);
    }

    public static boolean hasMultipleFixedBindings(@Nonnull final Collection<Binding> bindings) {
        return bindings.stream().filter(Binding::isFixed).count() > 1;
    }

    public static Binding fixedBinding(@Nonnull final Collection<Binding> bindings) {
        Debugger.sanityCheck(() -> Verify.verify(areAllBindingsFixed(bindings) && !hasMultipleFixedBindings(bindings)));
        return Iterables.getOnlyElement(bindings);
    }

    public static boolean isSingularDirectionalBinding(@Nonnull final Collection<Binding> bindings) {
        Verify.verify(!bindings.isEmpty());
        if (bindings.size() == 1) {
            return Iterables.getOnlyElement(bindings).getSortOrder().isDirectional();
        }
        return false;
    }

    public static ProvidedSortOrder sortOrder(@Nonnull final Collection<Binding> bindings) {
        Verify.verify(!bindings.isEmpty());

        if (isSingularDirectionalBinding(bindings)) {
            return Iterables.getOnlyElement(bindings).getSortOrder();
        }

        if (areAllBindingsFixed(bindings)) {
            return ProvidedSortOrder.FIXED;
        }

        throw new RecordCoreException("inconsistent ordering state");
    }

    @Nonnull
    @SuppressWarnings("java:S135")
    public static <O extends SetOperationsOrdering> O merge(@Nonnull final Iterable<Ordering> orderings,
                                                            @Nonnull final MergeOperator<O> mergeOperator,
                                                            @Nonnull final BiPredicate<O, O> isDistinctPredicate) {
        return Streams.stream(orderings)
                .map(mergeOperator::createFromOrdering)
                .reduce((left, right) -> merge(left, right, mergeOperator, isDistinctPredicate.test(left, right)))
                .orElseThrow(() -> new IllegalStateException("must have an ordering"));
    }

    /**
     * Method to combine a list of {@link Ordering}s into one {@code O} that extends {@link SetOperationsOrdering}.
     * This method is e.g. used to establish a resulting ordering of a set-operation such as intersection, a union
     * distinct, or an in-union operation. With respect to a set-operation two or more orderings can be compatible or
     * incompatible. If they are incompatible, this method will return an empty {@link Ordering}, otherwise the computed
     * common ordering.
     * <pre>
     * Example 1
     * {@code
     *   ordering 1: ordering keys: (a, rec_id; a ← rec_id) fixed bindings: ∅
     *   ordering 2: ordering keys: (a, b, rec_id; a ← b; b ← rec_id) fixed bindings: ∅
     *   common ordering is: ordering keys: (a) equality-bound keys: ∅
     * }
     * </pre>
     *
     * <pre>
     * Example 2
     * {@code
     *   ordering 1: ordering keys: (rec_id) fixed bindings: a = 3
     *   ordering 2: ordering keys: (rec_id) fixed bindings: a = 3
     *   common ordering is: ordering keys: (rec_id) fixed bindings: a = 3
     * }
     * </pre>
     *
     * <pre>
     * Example 3
     * {@code
     *   ordering 1: ordering keys: (a, rec_id, a ← rec_id) fixed bindings: ∅
     *   ordering 2: ordering keys: (rec_id) fixed bindings: a = $p
     *   common ordering is: ordering keys: (a, rec_id; a ← rec_id) fixed bindings: ∅
     * }
     * </pre>
     *
     * If they are compatible, however, it is not clear per se what the resulting comparison key should
     * be, though.
     *
     * <pre>
     * Example 4
     * {@code
     *   ordering 1: ordering keys: (a, rec_id) fixed bindings: a = 3
     *   ordering 2: ordering keys: (rec_id) fixed bindings: a = 5
     * }
     * </pre>
     *
     * It is unclear as to what the common ordering should be. It could be just
     * <pre>
     * {@code
     *   common ordering: ordering keys: (rec_id) fixed bindings: ∅
     * }
     * </pre>
     * That is too restrictive for unions. For a union, the caller can indicate what a desirable outcome should be as
     * the operator itself can establish that order. For example, a distinct union where one leg is equality-bound via
     * {@code a = 3} and a second leg is bound via {@code a = 5} can use a comparison key of {@code a, rec_id}. The
     * resulting ordering would be
     * <pre>
     * {@code
     *   common ordering: ordering keys: (a ← rec_id) fixed bindings: ∅
     * }
     * </pre>
     * As the merge operation behaves slightly differently between different set operations, the caller needs to pass
     * in a {@link MergeOperator} that implements the behavior that is specific to the particular set-operation
     * we compute the merged ordering for.
     * <br>
     * In particular this method always returns a subclass of {@link SetOperationsOrdering}. The merge operator passed
     * in determines the particular kind that is returned. While it is not permissible for a regular {@link Ordering}
     * to return hold multiple fixed bindings for a {@link Value}, {@link SetOperationsOrdering} do allow exactly that.
     * Depending on the subclass of {@link SetOperationsOrdering} the multitude of these bindings needs to be
     * interpreted differently. For {@link Union}, multiple fixed bindings are thought to be or-ed, while for an
     * {@link Intersection}, multiple fixed bindings are thought to be and-ed.
     * <br>
     * {@link SetOperationsOrdering} defines methods that can only be applied to orderings that are produced by a
     * merge operation. For most of these methods, the caller needs to pass in a {@link RequestedOrdering} in order
     * to fix the ambiguities of the {@link SetOperationsOrdering} at hand. Most prominently,
     * {@link SetOperationsOrdering#applyComparisonKey(List, SetMultimap)} can take a {@link SetOperationsOrdering},
     * and by means of a {@link RequestedOrdering} can create a regular {@link Ordering} by promoting multiple fixed
     * bindings into directional ones.
     *
     * @param left an {@link Ordering}
     * @param right an {@link Ordering}
     * @param mergeOperator an operator used to combine the orderings
     * @param isDistinct indicator if the resulting order is thought to be distinct
     * @param <O> type parameter bound to at least a {@link SetOperationsOrdering}
     * @return an {@link Ordering}
     */
    @Nonnull
    @SuppressWarnings("java:S135")
    public static <O extends SetOperationsOrdering> O merge(@Nonnull final Ordering left,
                                                            @Nonnull final Ordering right,
                                                            @Nonnull final MergeOperator<O> mergeOperator,
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

            //
            // Join the left eligible elements with the matching right eligible elements. Combine their bindings.
            //
            final var intersectedElementsBuilder = ImmutableSet.<Value>builder();
            for (final var leftElement : leftElements) {
                for (final var rightElement : rightElements) {
                    if (leftElement.equals(rightElement)) {
                        final var combinedBindings =
                                mergeOperator.combineBindings(leftBindingMap.get(leftElement), rightBindingMap.get(rightElement));
                        if (!combinedBindings.isEmpty()) {
                            intersectedElementsBuilder.add(leftElement);
                            elementsBuilder.add(leftElement);
                            bindingMapBuilder.putAll(leftElement, combinedBindings);
                        }
                    }
                }
            }

            //
            // Note that this is a full-outer operation, that is we need to also consider elements that do not have
            // matching counterparts on the other side. This is important as e.g. a singular fixed bindings from one of
            // the legs of an intersection operation that does not even have a binding on the other legs still
            // needs to have that singular binding in the resulting set ordering.
            //
            for (final var leftElement : Sets.difference(leftElements, rightElements)) {
                final var combinedBindings =
                        mergeOperator.combineBindings(leftBindingMap.get(leftElement), ImmutableSet.of());
                if (!combinedBindings.isEmpty()) {
                    elementsBuilder.add(leftElement);
                    bindingMapBuilder.putAll(leftElement, combinedBindings);
                }
            }
            for (final var rightElement : Sets.difference(rightElements, leftElements)) {
                final var combinedBindings =
                        mergeOperator.combineBindings(ImmutableSet.of(), leftBindingMap.get(rightElement));
                if (!combinedBindings.isEmpty()) {
                    elementsBuilder.add(rightElement);
                    bindingMapBuilder.putAll(rightElement, combinedBindings);
                }
            }

            final var intersectedElements = intersectedElementsBuilder.build();
            if (intersectedElements.isEmpty()) {
                break;
            }

            for (final var intersectedElement : intersectedElements) {
                for (final var lastElement : lastElements) {
                    if (leftDependencies.get(intersectedElement).contains(lastElement) ||
                            rightDependencies.get(intersectedElement).contains(lastElement)) {
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
        return mergeOperator.createOrdering(bindingMapBuilder.build(), orderingSet, isDistinct);
    }

    /**
     * Union the bindings of a {@link Value} common to two orderings.  This method implements a merge
     * operator's {@link MergeOperator#combineBindings(Set, Set)}.
     * @param leftBindings set of bindings of the left ordering
     * @param rightBindings set of bindings of the right ordering
     * @return newly combined set of bindings
     */
    @Nonnull
    private static Set<Binding> combineBindingsForUnion(@Nonnull final Set<Binding> leftBindings,
                                                        @Nonnull final Set<Binding> rightBindings) {
        if (leftBindings.isEmpty() || rightBindings.isEmpty()) {
            return ImmutableSet.of();
        }

        final var leftSortOrder = sortOrder(leftBindings);
        final var rightSortOrder = sortOrder(rightBindings);

        if (leftSortOrder.isDirectional() && rightSortOrder.isDirectional()) {
            if (leftSortOrder != rightSortOrder) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(Binding.sorted(leftSortOrder));
        }

        if (leftSortOrder.isDirectional() && rightSortOrder == ProvidedSortOrder.FIXED) {
            return ImmutableSet.of(Binding.sorted(leftSortOrder));
        }
        if (leftSortOrder == ProvidedSortOrder.FIXED && rightSortOrder.isDirectional()) {
            return ImmutableSet.of(Binding.sorted(rightSortOrder));
        }

        Debugger.sanityCheck(() -> {
            Verify.verify(areAllBindingsFixed(leftBindings));
            Verify.verify(areAllBindingsFixed(rightBindings));
        });

        return ImmutableSet.copyOf(Sets.union(leftBindings, rightBindings));
    }

    /**
     * Intersect the bindings of a {@link Value} common to two orderings. This method implements a merge
     * operator's {@link MergeOperator#combineBindings(Set, Set)}.
     * @param leftBindings set of bindings of the left ordering
     * @param rightBindings set of bindings of the right ordering
     * @return newly combined set of bindings
     */
    @Nonnull
    private static Set<Binding> combineBindingsForIntersection(@Nonnull final Set<Binding> leftBindings,
                                                               @Nonnull final Set<Binding> rightBindings) {
        if (leftBindings.isEmpty() && rightBindings.isEmpty()) {
            return ImmutableSet.of();
        }

        if (rightBindings.isEmpty() && areAllBindingsFixed(leftBindings)) {
            return leftBindings;
        }

        if (leftBindings.isEmpty() && areAllBindingsFixed(rightBindings)) {
            return rightBindings;
        }

        if (leftBindings.isEmpty() || rightBindings.isEmpty()) {
            return ImmutableSet.of();
        }

        final var leftSortOrder = sortOrder(leftBindings);
        final var rightSortOrder = sortOrder(rightBindings);

        if (leftSortOrder.isDirectional() && rightSortOrder.isDirectional()) {
            if (leftSortOrder != rightSortOrder) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(Binding.sorted(leftSortOrder));
        }

        if (leftSortOrder.isDirectional() && rightSortOrder == ProvidedSortOrder.FIXED) {
            return rightBindings;
        }
        if (leftSortOrder == ProvidedSortOrder.FIXED && rightSortOrder.isDirectional()) {
            return leftBindings;
        }

        Debugger.sanityCheck(() -> {
            Verify.verify(areAllBindingsFixed(leftBindings));
            Verify.verify(areAllBindingsFixed(rightBindings));
        });
        return ImmutableSet.copyOf(Sets.union(leftBindings, rightBindings));
    }

    /**
     * Helper method to concatenate the ordering key parts of the participating orderings in iteration order.
     * @param orderings a collection of orderings
     * @return a new ordering representing a concatenation of the given left and right ordering
     */
    @Nonnull
    public static Ordering concatOrderings(@Nonnull final Collection<Ordering> orderings) {

        return orderings.stream()
                .reduce(Ordering::concatOrderings)
                .orElseThrow(() -> new RecordCoreException("unable to concatenate orderings"));
    }

    /**
     * Helper method to concatenate the ordering key parts of the participating orderings in iteration order.
     * @param leftOrdering an {@link Ordering}
     * @param rightOrdering another {@link Ordering} to be concatenated to {@code leftOrdering}
     * @return a new {@link Ordering}
     */
    @Nonnull
    public static Ordering concatOrderings(@Nonnull final Ordering leftOrdering,
                                           @Nonnull final Ordering rightOrdering) {
        final var leftBindingMap = leftOrdering.getBindingMap();
        final var rightBindingMap = rightOrdering.getBindingMap();
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
        // Find the maximum values of the left ordering. The eligible set of a partially-ordered set are its minimum
        // elements. Thus, the maximum elements are the minimum elements (that is the eligible elements) of the dual
        // of the partially-ordered set.
        // Find the maximum values of the left ordering. Make the minimum elements of the right ordering become dependent
        // on the maximum elements of the left ordering.
        //
        final var leftDualOrdering = leftOrderingSet.dualOrder();
        final var leftMaxElements = leftDualOrdering.eligibleSet().eligibleElements();
        final var rightMinElements = rightOrderingSet.eligibleSet().eligibleElements();

        for (final var leftMaxElement : leftMaxElements) {
            for (final var rightMinElement : rightMinElements) {
                if (!Ordering.areAllBindingsFixed(leftBindingMap.get(leftMaxElement)) &&
                        !Ordering.areAllBindingsFixed(rightBindingMap.get(rightMinElement))) {
                    dependencyMapBuilder.put(rightMinElement, leftMaxElement);
                }
            }
        }

        final var concatenatedOrderingSet = PartiallyOrderedSet.of(orderingElements, dependencyMapBuilder.build());

        final var combinedBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
        combinedBindingMapBuilder.putAll(leftBindingMap);
        combinedBindingMapBuilder.putAll(rightBindingMap);

        return Ordering.ofOrderingSet(combinedBindingMapBuilder.build(), concatenatedOrderingSet, rightOrdering.isDistinct());
    }

    @Nonnull
    public static Ordering empty() {
        return EMPTY;
    }

    @Nonnull
    protected static BiConsumer<SetMultimap<Value, Binding>, PartiallyOrderedSet<Value>> normalizationCheckConsumer() {
        return Ordering::normalizationCheck;
    }

    protected static void normalizationCheck(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                             @Nonnull final PartiallyOrderedSet<Value> orderingSet) {
        final var normalizedBindingMap = normalizeBindingMap(bindingMap);
        Verify.verify(bindingMap.equals(normalizedBindingMap));
        final var normalizedOrderingSet = normalizeOrderingSet(bindingMap, orderingSet);
        Verify.verify(orderingSet.equals(normalizedOrderingSet));
    }

    protected static void singularFixedBindingCheck(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                                    @Nonnull final PartiallyOrderedSet<Value> orderingSet) {
        for (final var valueBindingsEntry : bindingMap.asMap().entrySet()) {
            final var bindings = valueBindingsEntry.getValue();
            Verify.verify(!areAllBindingsFixed(bindings) || !hasMultipleFixedBindings(bindings));
        }
    }

    protected static void noChooseBindingCheck(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                               @Nonnull final PartiallyOrderedSet<Value> orderingSet) {
        for (final var valueBindingsEntry : bindingMap.asMap().entrySet()) {
            final var bindings = valueBindingsEntry.getValue();
            if (isSingularDirectionalBinding(bindings)) {
                Verify.verify(sortOrder(bindings) != ProvidedSortOrder.CHOOSE);
            }
        }
    }

    @Nonnull
    public static Ordering ofOrderingSet(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                         @Nonnull final PartiallyOrderedSet<Value> orderingSet,
                                         final boolean isDistinct) {
        return new Ordering(bindingMap, orderingSet, isDistinct,
                normalizationCheckConsumer().andThen(Ordering::singularFixedBindingCheck).andThen(Ordering::noChooseBindingCheck));
    }

    @Nonnull
    public static Ordering ofOrderingSequence(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                              @Nonnull final List<? extends Value> orderingAsList,
                                              final boolean isDistinct) {
        return ofOrderingSet(bindingMap, computeFromOrderingSequence(bindingMap, orderingAsList), isDistinct);
    }

    /**
     * Helper to attempt to resolve a comparison direction. This is needed for distinct set operations that use
     * comparison keys. In the future there won't be a common direction but each comparison value will encode that
     * information separately. Note that this method is probably going to be deprecated once we have comparison keys
     * that use an independent comparison direction per ordering part.
     * @param providedOrderingParts an iterable of {@link ProvidedOrderingPart}s
     * @return {@code Optional.empty()} if individual orderings are mixed (i.e. some are ascending, others are
     *         descending); {@code Optional.of(false)} if all directional sort orders are not descending;
     *         {@code Optional.of(true)} if all directional sort orders are descending
     */
    @Nonnull
    public static Optional<Boolean> resolveComparisonDirectionMaybe(@Nonnull final Iterable<ProvidedOrderingPart> providedOrderingParts) {
        boolean seenAscending = false;
        boolean seenDescending = false;

        for (final var providedOrderingPart : providedOrderingParts) {
            final var sortOrder = providedOrderingPart.getSortOrder();
            switch (sortOrder) {
                case ASCENDING:
                    seenAscending = true;
                    break;
                case DESCENDING:
                    seenDescending = true;
                    break;
                case FIXED:
                case CHOOSE:
                    break;
                default:
                    throw new RecordCoreException("unexpected sort order");
            }
        }
        if (seenAscending && seenDescending) {
            // shrug
            return Optional.empty();
        }

        if (!seenAscending && !seenDescending) {
            // in the absence of anything we return forward by default
            return Optional.of(false);
        }

        return seenAscending ? Optional.of(false) : Optional.of(true);
    }

    /**
     * A helper class used inside an {@link Ordering} to indicate if a value is considered to be ascending, descending,
     * or fixed to a comparison/a set of comparisons (which have to be of type {@link ComparisonRange.Type#EQUALITY}).
     * If the value is fixed to a set of more than one comparison, the context defines how that is interpreted:
     * For regular {@link Ordering}s and {@link Intersection}s, the bindings are interpreted as a logical conjunction,
     * for {@link Union} the bindings are interpreted as a disjunction.
     */
    public static class Binding {
        @Nonnull
        private final ProvidedSortOrder sortOrder;

        /**
         * Comparison is set if {@code sortOrder} is set to {@link ProvidedSortOrder#FIXED},
         * {@code null} otherwise.
         */
        @Nullable
        private final Comparison comparison;

        private Binding(@Nonnull final ProvidedSortOrder sortOrder, @Nullable final Comparison comparison) {
            this.sortOrder = sortOrder;
            this.comparison = comparison;
        }

        @Nonnull
        public ProvidedSortOrder getSortOrder() {
            return sortOrder;
        }

        public boolean isFixed() {
            return sortOrder == ProvidedSortOrder.FIXED;
        }

        @Nonnull
        public Comparison getComparison() {
            Verify.verify(sortOrder == ProvidedSortOrder.FIXED);
            return Objects.requireNonNull(comparison);
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

        @Override
        public String toString() {
            return sortOrder.getArrowIndicator() + (comparison == null ? "" : ":" + comparison);
        }

        @Nonnull
        public static Binding ascending() {
            return sorted(ProvidedSortOrder.ASCENDING);
        }

        @Nonnull
        public static Binding descending() {
            return sorted(ProvidedSortOrder.DESCENDING);
        }

        @Nonnull
        public static Binding choose() {
            return sorted(ProvidedSortOrder.CHOOSE);
        }

        @Nonnull
        public static Binding sorted(final boolean isReverse) {
            return sorted(ProvidedSortOrder.fromIsReverse(isReverse));
        }

        @Nonnull
        public static Binding sorted(@Nonnull final ProvidedSortOrder sortOrder) {
            Verify.verify(sortOrder.isDirectional());
            return new Binding(sortOrder, null);
        }

        @Nonnull
        public static Binding fixed(@Nonnull final Comparison comparison) {
            return new Binding(ProvidedSortOrder.FIXED, comparison);
        }
    }

    /**
     * Abstract static class that represents an ordering that is produced by an ordered set operation such as a union or
     * intersection operation prior to the application of a comparison key. This sort ordering slightly modifies the
     * semantics of multiple fixed bindings in the binding set of a {@link Value} imposed by {@link Ordering}
     * by interpreting multiple bindings according to the kind of set operation. It also allows to create bindings
     * of type {@link ProvidedSortOrder#CHOOSE}.
     */
    public abstract static class SetOperationsOrdering extends Ordering {
        public SetOperationsOrdering(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                                     @Nonnull final PartiallyOrderedSet<Value> orderingSet, final boolean isDistinct) {
            super(bindingMap, orderingSet, isDistinct, Ordering::normalizationCheck); // do not do the single binding check
        }

        @Nonnull
        public Iterable<List<Value>> enumerateSatisfyingComparisonKeyValues(@Nonnull final RequestedOrdering requestedOrdering) {
            if (requestedOrdering.isDistinct() && !isDistinct()) {
                return ImmutableList.of();
            }

            final var bindingMap = getBindingMap();
            final var reducedRequestedOrderingValuesBuilder = ImmutableList.<Value>builder();
            for (final var requestedOrderingPart : requestedOrdering.getOrderingParts()) {
                if (!bindingMap.containsKey(requestedOrderingPart.getValue())) {
                    return ImmutableList.of();
                }
                final var bindings = bindingMap.get(requestedOrderingPart.getValue());
                final var sortOrder = sortOrder(bindings);
                if (!sortOrder.isCompatibleWithRequestedSortOrder(requestedOrderingPart.getSortOrder())) {
                    return ImmutableList.of();
                }

                if (sortOrder != ProvidedSortOrder.FIXED) {
                    reducedRequestedOrderingValuesBuilder.add(requestedOrderingPart.getValue());
                } else {
                    // if more than one FIXED binding; ask the specific promotion predicate what to do
                    if (bindings.size() > 1 && promoteToDirectional()) {
                        reducedRequestedOrderingValuesBuilder.add(requestedOrderingPart.getValue());
                    }
                }
            }

            final var reducedRequestedOrderingValues = reducedRequestedOrderingValuesBuilder.build();

            //
            // Filter out all elements that only have singular fixed bindings (or that should be treated as such).
            // For instance, a value b may have two fixed binding that come from different legs in a union. b should
            // participate in the enumeration. On the contrary, if this method is called from an intersection context
            // we need to consider the bindings to be identical, as they may be identical even though their value
            // representation may not be identical (b = 5; b = 2 + 3). Because they are identical, the actual values
            // flowed at runtime are constant, thus we don't need to have b participate in the comparison key
            // enumeration.
            //
            final var filteredOrderingSet =
                    getOrderingSet().filterElements(value -> {
                        final var bindings = bindingMap.get(value);
                        return isSingularDirectionalValue(value) ||
                                //
                                // This commented line changes the behavior in a way that values that have multiple
                                // fixed bindings but no requested sort order do not get a comparison key part and
                                // therefore do not take part in the resulting provided ordering of the set operation.
                                // It turns out that that is probably a bad idea as it is wasting an opportunity to make
                                // something ordered.
                                // (bindings.size() > 1 && promoteToDirectional() && valuesRequestedSortOrderMap.containsKey(value));
                                (bindings.size() > 1 && promoteToDirectional());
                    });

            return TopologicalSort.satisfyingPermutations(
                    filteredOrderingSet,
                    reducedRequestedOrderingValues,
                    Function.identity(),
                    permutation -> reducedRequestedOrderingValues.size());
        }

        protected abstract boolean promoteToDirectional();

        @Nonnull
        public Ordering applyComparisonKey(@Nonnull final List<? extends Value> comparisonKeyValues,
                                           @Nonnull final SetMultimap<Value, Binding> comparisonKeyBindingMap) {
            final var orderingSet = getOrderingSet();
            final var comparisonKeyOrderingSet =
                    PartiallyOrderedSet.<Value>builder()
                            .addListWithDependencies(comparisonKeyValues)
                            .build();

            Debugger.sanityCheck(() -> Verify.verify(orderingSet.getSet().containsAll(comparisonKeyOrderingSet.getSet())));

            final var resultBindingMapBuilder = ImmutableSetMultimap.<Value, Binding>builder();
            for (final var entry : getBindingMap().asMap().entrySet()) {
                final var value = entry.getKey();
                final var bindings = entry.getValue();

                if (!comparisonKeyBindingMap.containsKey(value)) {
                    if (areAllBindingsFixed(bindings) && !hasMultipleFixedBindings(bindings)) {
                        // there is only one fixed binding
                        resultBindingMapBuilder.putAll(value, bindings);
                    }
                    //
                    // We don't output a binding if the there are multiple fixed bindings or there is a directional
                    // binding that is not part of the comparison key
                    //
                } else {
                    final var comparisonKeyBindings = comparisonKeyBindingMap.get(value);
                    Verify.verify(comparisonKeyBindings.stream().noneMatch(Binding::isFixed));
                    resultBindingMapBuilder.put(value, Iterables.getOnlyElement(comparisonKeyBindings));
                }
            }

            final var resultBindingMap = resultBindingMapBuilder.build();
            // result binding map's key set is a subset of the set backing this ordering set
            final var resultSet = Sets.intersection(orderingSet.getSet(), resultBindingMap.keySet());

            final var otherDependencyMap = comparisonKeyOrderingSet.getDependencyMap();
            final var resultDependencyMap =
                    ImmutableSetMultimap.<Value, Value>builder()
                            .putAll(orderingSet.getDependencyMap())
                            .putAll(otherDependencyMap)
                            .build();
            final var resultOrderingSet =
                    PartiallyOrderedSet.of(resultSet, resultDependencyMap);
            return Ordering.ofOrderingSet(resultBindingMap, resultOrderingSet, isDistinct());
        }

        /**
         * Method that reduces this set-operation ordering to only directional ordering parts by using a
         * comparison key, a {@link RequestedOrdering} and a default.
         * @param comparisonKeyValues list of {@link Value}s representing the comparison key
         * @param requestedOrdering a requested ordering
         * @param defaultProvidedSortOrder a default sort order to be applied if the requested ordering does not
         *        contain a value
         * @return a list of {@link ProvidedOrderingPart}s
         */
        @Nonnull
        public List<ProvidedOrderingPart> directionalOrderingParts(@Nonnull final List<Value> comparisonKeyValues,
                                                                   @Nonnull final RequestedOrdering requestedOrdering,
                                                                   @Nonnull final ProvidedSortOrder defaultProvidedSortOrder) {
            final var valueRequestedSortOrderMapMap =
                    requestedOrdering.getValueRequestedSortOrderMap();
            return directionalOrderingParts(comparisonKeyValues, valueRequestedSortOrderMapMap, defaultProvidedSortOrder);
        }

        /**
         * Method that reduces this set-operation ordering to only directional ordering parts by using a
         * comparison key, a {@link RequestedOrdering} and a default.
         * @param comparisonKeyValues list of {@link Value}s representing the comparison key
         * @param valueRequestedSortOrderMap a requested ordering as map
         * @param defaultProvidedSortOrder a default sort order to be applied if the requested ordering does not
         *        contain a value
         * @return a list of {@link ProvidedOrderingPart}s
         */
        @Nonnull
        public List<ProvidedOrderingPart> directionalOrderingParts(@Nonnull final List<Value> comparisonKeyValues,
                                                                   @Nonnull final Map<Value, RequestedSortOrder> valueRequestedSortOrderMap,
                                                                   @Nonnull final ProvidedSortOrder defaultProvidedSortOrder) {
            final var bindingMap = getBindingMap();
            final var resultBuilder = ImmutableList.<ProvidedOrderingPart>builder();
            for (final var comparisonKeyValue : comparisonKeyValues) {
                Verify.verify(bindingMap.containsKey(comparisonKeyValue));
                final var bindings = bindingMap.get(comparisonKeyValue);
                if (isSingularDirectionalBinding(bindings)) {
                    resultBuilder.add(new ProvidedOrderingPart(comparisonKeyValue, sortOrder(bindings)));
                } else {
                    Debugger.sanityCheck(() -> areAllBindingsFixed(bindings));
                    if (!valueRequestedSortOrderMap.containsKey(comparisonKeyValue)) {
                        resultBuilder.add(new ProvidedOrderingPart(comparisonKeyValue, defaultProvidedSortOrder));
                    } else {
                        final var requestedSortOrder = valueRequestedSortOrderMap.get(comparisonKeyValue);
                        switch (requestedSortOrder) {
                            case ASCENDING:
                            case DESCENDING:
                                resultBuilder.add(new ProvidedOrderingPart(comparisonKeyValue, requestedSortOrder.toProvidedSortOrder()));
                                break;
                            case ANY:
                                resultBuilder.add(new ProvidedOrderingPart(comparisonKeyValue, defaultProvidedSortOrder));
                                break;
                            default:
                                throw new RecordCoreException("unable to resolve directional order");
                        }
                    }
                }
            }
            return resultBuilder.build();
        }
    }

    /**
     * TODO.
     */
    public static class Union extends SetOperationsOrdering {
        public Union(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                     @Nonnull final PartiallyOrderedSet<Value> orderingSet, final boolean isDistinct) {
            super(bindingMap, orderingSet, isDistinct);
        }

        @Override
        protected boolean promoteToDirectional() {
            return true;
        }
    }

    /**
     * TODO.
     */
    public static class Intersection extends SetOperationsOrdering {
        public Intersection(@Nonnull final SetMultimap<Value, Binding> bindingMap,
                            @Nonnull final PartiallyOrderedSet<Value> orderingSet, final boolean isDistinct) {
            super(bindingMap, orderingSet, isDistinct);
        }

        @Override
        protected boolean promoteToDirectional() {
            return false;
        }
    }

    /**
     * Merge operator for orderings.
     * @param <O> the type of the resulting ordering
     */
    public interface MergeOperator<O extends SetOperationsOrdering> {
        @Nonnull
        Set<Binding> combineBindings(@Nonnull Set<Binding> leftBindings, @Nonnull Set<Binding> rightBindings);

        @Nonnull
        O createOrdering(@Nonnull SetMultimap<Value, Binding> bindingMap,
                         @Nonnull PartiallyOrderedSet<Value> orderingSet,
                         boolean isDistinct);

        default O createFromOrdering(@Nonnull final Ordering ordering) {
            return createOrdering(ordering.getBindingMap(), ordering.getOrderingSet(), ordering.isDistinct());
        }
    }
}
