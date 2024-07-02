/*
 * PartiallyOrderedSet.java
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

package com.apple.foundationdb.record.query.combinatorics;

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A class to represent partially ordered set of elements of some type. A partially ordered set or partial order over
 * {@code objects(T)} is defined as a subset of {@code object(T) cross objects(T)} (that is, a mathematical relation)
 * that is:
 * <ul>
 *     <li>
 *        irreflexive
 *     </li>
 *     <li>
 *        transitive
 *     </li>
 *     <li>
 *         asymmetrical
 *     </li>
 * </ul>
 *
 * This class is closely connected to other classes in this package. See also {@link TopologicalSort} and
 * {@link TransitiveClosure}.
 *
 * @param <T> the type of elements
 */
public class PartiallyOrderedSet<T> {
    @Nonnull
    private final ImmutableSet<T> set;
    @Nonnull
    private final ImmutableSetMultimap<T, T> dependencyMap;
    @Nonnull
    private final Supplier<PartiallyOrderedSet<T>> dualSupplier;
    @Nonnull
    private final Supplier<ImmutableSetMultimap<T, T>> transitiveClosureSupplier;

    private PartiallyOrderedSet(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> dependencyMap) {
        this.set = ImmutableSet.copyOf(set);
        this.dependencyMap = ImmutableSetMultimap.copyOf(dependencyMap);
        this.dualSupplier = Suppliers.memoize(() -> PartiallyOrderedSet.of(set, this.dependencyMap.inverse()));
        this.transitiveClosureSupplier = Suppliers.memoize(() -> TransitiveClosure.transitiveClosure(set, this.dependencyMap));
    }

    @Nonnull
    public ImmutableSet<T> getSet() {
        return set;
    }

    public boolean isEmpty() {
        return getSet().isEmpty();
    }

    @Nonnull
    public ImmutableSetMultimap<T, T> getDependencyMap() {
        return dependencyMap;
    }

    @Nonnull
    public ImmutableSetMultimap<T, T> getTransitiveClosure() {
        return transitiveClosureSupplier.get();
    }

    public int size() {
        return set.size();
    }

    @Nonnull
    public PartiallyOrderedSet<T> dualOrder() {
        return dualSupplier.get();
    }

    @Nonnull
    public EligibleSet<T> eligibleSet() {
        return new EligibleSet<>(this);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartiallyOrderedSet)) {
            return false;
        }
        final PartiallyOrderedSet<?> that = (PartiallyOrderedSet<?>)o;
        return getSet().equals(that.getSet()) && getTransitiveClosure().equals(that.getTransitiveClosure());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSet(), getTransitiveClosure());
    }

    @Override
    public String toString() {
        return "{" +
               set.stream().map(Object::toString).collect(Collectors.joining(", ")) +
               (dependencyMap.isEmpty() ? "" : "| " + dependencyMap.entries().stream().map(entry -> entry.getValue() + "←" + entry.getKey()).collect(Collectors.joining(", "))) +
               "}";
    }

    @Nonnull
    public <R> PartiallyOrderedSet<R> mapEach(@Nonnull final Function<T, R> mapFunction) {
        final var resultMapBuilder = ImmutableBiMap.<T, R>builder();
        for (final var element : getSet()) {
            resultMapBuilder.put(element, mapFunction.apply(element));
        }
        final var elementsToMappedElementsMap = resultMapBuilder.build();

        final var resultDependencyMapBuilder = ImmutableSetMultimap.<R, R>builder();
        for (final var entry : getTransitiveClosure().entries()) {
            final var key = entry.getKey();
            final var value = entry.getValue();
            Verify.verify(elementsToMappedElementsMap.containsKey(key));
            Verify.verify(elementsToMappedElementsMap.containsKey(value));
            resultDependencyMapBuilder.put(Verify.verifyNotNull(elementsToMappedElementsMap.get(key)),
                    Verify.verifyNotNull(elementsToMappedElementsMap.get(value)));
        }

        return PartiallyOrderedSet.of(elementsToMappedElementsMap.values(), resultDependencyMapBuilder.build());
    }

    @Nonnull
    public <R> PartiallyOrderedSet<R> mapAll(@Nonnull final Function<Iterable<? extends T>, Map<T, R>> mapFunction) {
        return mapAll(mapFunction.apply(getSet()));
    }

    @Nonnull
    public <R> PartiallyOrderedSet<R> mapAll(@Nonnull final Map<T, R> map) {
        final var mappedElements = Sets.newLinkedHashSet(map.values());

        final var resultDependencyMapBuilder = ImmutableSetMultimap.<R, R>builder();
        for (final var entry : getTransitiveClosure().entries()) {
            final var key = entry.getKey();
            final var value = entry.getValue();

            if (map.containsKey(key) && map.containsKey(value)) {
                resultDependencyMapBuilder.put(map.get(key), map.get(value));
            } else {
                if (!map.containsKey(value)) {
                    // if key depends on value that does not exist -- do not insert the dependency and also remove key
                    final var mappedKey = map.get(key);
                    if (mappedKey != null) {
                        mappedElements.remove(mappedKey);
                    }
                }
            }
        }

        // this needs the dependency map to be cleansed (which is done in the constructor)
        return PartiallyOrderedSet.of(mappedElements, resultDependencyMapBuilder.build());
    }

    /**
     * Method that computes a new partially-ordered set that only retains elements that pass the filtering predicate.
     * The filtering is applied in a way that we do not break dependencies.
     * @param predicate a predicate that can decide whether an independent element is removed or retained
     * @return a new {@link PartiallyOrderedSet}
     */
    @Nonnull
    public PartiallyOrderedSet<T> filterElements(@Nonnull final Predicate<T> predicate) {
        final var translationMap = ImmutableMap.<T, T>builder();
        for (final var t : getSet()) {
            if (predicate.test(t)) {
                translationMap.put(t, t);
            }
        }

        return mapAll(translationMap.build());
    }

    @Nonnull
    public static <T> PartiallyOrderedSet<T> empty() {
        return new PartiallyOrderedSet<>(ImmutableSet.of(), ImmutableSetMultimap.of());
    }

    @Nonnull
    private static <T> SetMultimap<T, T> cleanseDependencyMap(@Nonnull final Set<T> set,
                                                              @Nonnull final SetMultimap<T, T> dependencyMap) {
        boolean needsCopy = false;
        final ImmutableSetMultimap.Builder<T, T> cleanDependencyMapBuilder = ImmutableSetMultimap.builder();

        for (final Map.Entry<T, T> entry : dependencyMap.entries()) {
            final T key = entry.getKey();
            final T value = entry.getValue();
            if (set.contains(key) && set.contains(value)) {
                cleanDependencyMapBuilder.put(key, entry.getValue());
            } else {
                // There is an entry we don't want in the dependency map.
                needsCopy = true;
            }
        }
        if (needsCopy) {
            return cleanDependencyMapBuilder.build();
        }
        return dependencyMap;
    }

    /**
     * A class allowing to compute a subset of elements in a partial order that are said to be <em>eligible</em>.
     * An <em>eligible</em> element is an element that is smallest with respect to the partial order. In total order,
     * there would only ever be exactly one such element. In a partial order there can be many such elements.
     *
     * Objects of this class should be initially created by calling {@link #eligibleSet()}. The current set of
     * eligible elements can then be retrieved using {@link #eligibleElements()}. In order to advance <em>iteration</em>
     * the caller can remove elements from the set of currently eligible elements, thus creating a new,
     * albeit smaller in cardinality, partial order.
     *
     * By repeatedly removing elements, a caller can consume the entire partial order in a well-defined way. This
     * concept of iteration is a little more complex than the standard Java {@link java.util.Iterator} approach,
     * as the next step of the iteration is defined by removing a particular subset of elements that is determined
     * by the caller.
     *
     * @param <T> type
     */
    public static class EligibleSet<T> {
        @Nonnull
        private final PartiallyOrderedSet<T> partiallyOrderedSet;

        @Nonnull
        private final Map<T, Integer> inDegreeMap;

        @Nonnull
        private final Supplier<Set<T>> eligibleElementsSupplier;

        private EligibleSet(@Nonnull final PartiallyOrderedSet<T> partiallyOrderedSet) {
            this.partiallyOrderedSet = partiallyOrderedSet;
            this.inDegreeMap = computeInDegreeMap(partiallyOrderedSet);
            this.eligibleElementsSupplier = Suppliers.memoize(this::computeEligibleElements);
        }

        @Nonnull
        public PartiallyOrderedSet<T> getPartialOrder() {
            return partiallyOrderedSet;
        }

        public boolean isEmpty() {
            return eligibleElements().isEmpty();
        }

        @Nonnull
        public Set<T> eligibleElements() {
            return eligibleElementsSupplier.get();
        }

        @Nonnull
        private Set<T> computeEligibleElements() {
            return this.inDegreeMap
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() == 0)
                    .map(Map.Entry::getKey)
                    .collect(ImmutableSet.toImmutableSet());
        }

        public EligibleSet<T> removeEligibleElements(@Nonnull final Set<T> toBeRemovedEligibleElements) {
            Debugger.sanityCheck(() -> Preconditions.checkArgument(eligibleElements().containsAll(toBeRemovedEligibleElements)));

            final var set = partiallyOrderedSet.getSet();
            final var newSetBuilder = ImmutableSet.<T>builder();
            for (var element : set) {
                if (!toBeRemovedEligibleElements.contains(element)) {
                    newSetBuilder.add(element);
                }
            }

            final var dependencies = partiallyOrderedSet.getDependencyMap();
            final var newDependencies = ImmutableSetMultimap.<T, T>builder();
            for (final var entry : dependencies.entries()) {
                Verify.verify(!toBeRemovedEligibleElements.contains(entry.getKey())); // no dependency pointing to the entry
                if (!toBeRemovedEligibleElements.contains(entry.getValue())) {
                    newDependencies.put(entry);
                }
            }

            // create a new eligible set
            return new EligibleSet<>(PartiallyOrderedSet.of(newSetBuilder.build(), newDependencies.build()));
        }

        @Nonnull
        @SuppressWarnings("java:S3398")
        private static <T> Map<T, Integer> computeInDegreeMap(@Nonnull final PartiallyOrderedSet<T> partiallyOrderedSet) {
            final HashMap<T, Integer> result = Maps.newLinkedHashMapWithExpectedSize(partiallyOrderedSet.size());
            partiallyOrderedSet.getSet().forEach(element -> result.put(element, 0));

            for (final Map.Entry<T, T> entry : partiallyOrderedSet.getDependencyMap().entries()) {
                result.compute(entry.getKey(), (t, v) -> Objects.requireNonNull(v) + 1);
            }
            return result;
        }
    }

    /**
     * Construct the transitive closure from a given set and a function that maps each element of the given set to
     * other elements that constitutes that element's dependencies.
     * @param <T> the type of the elements in {@code set}
     * @param set a set of elements
     * @param dependsOnFn a function mapping each element of the set to its dependencies of type {@code T}. Note that
     *        the returned dependencies may not be a strict subset of the set passed in, in which case those elements
     *        in the dependency set that are not in {@code set} are simply ignored.
     * @return a multi map containing the transitive closure
     */
    @Nonnull
    static <T> ImmutableSetMultimap<T, T> fromFunctionalDependencies(@Nonnull final Set<T> set, @Nonnull final Function<T, Set<T>> dependsOnFn) {
        final ImmutableSetMultimap.Builder<T, T> builder = ImmutableSetMultimap.builder();

        for (final T element : set) {
            final Set<T> dependsOnElements = dependsOnFn.apply(element);
            for (final T dependsOnElement : dependsOnElements) {
                if (set.contains(dependsOnElement)) {
                    builder.put(element, dependsOnElement);
                }
            }
        }
        return builder.build();
    }

    @Nonnull
    static <T> ImmutableSetMultimap<T, T> invertFromFunctionalDependencies(@Nonnull final Set<T> set, @Nonnull final Function<T, Set<T>> dependsOnFn) {
        // invert the dependencies
        final ImmutableSetMultimap.Builder<T, T> builder = ImmutableSetMultimap.builder();

        for (final T element : set) {
            final Set<T> dependsOnElements = dependsOnFn.apply(element);
            for (final T dependsOnElement : dependsOnElements) {
                if (set.contains(dependsOnElement)) {
                    builder.put(dependsOnElement, element);
                }
            }
        }
        return builder.build();
    }

    public static <T> PartiallyOrderedSet<T> of(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> dependencyMap) {
        return new PartiallyOrderedSet<>(set, cleanseDependencyMap(set, dependencyMap));
    }

    @Nonnull
    public static <T> PartiallyOrderedSet<T> of(@Nonnull final Set<T> set, @Nonnull final Function<T, Set<T>> dependsOnFn) {
        return of(set, fromFunctionalDependencies(set, dependsOnFn));
    }

    @Nonnull
    public static <T> PartiallyOrderedSet<T> ofInverted(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> dependencyMap) {
        return ofInverted(set, dependencyMap::get);
    }

    @Nonnull
    public static <T> PartiallyOrderedSet<T> ofInverted(@Nonnull final Set<T> set, @Nonnull final Function<T, Set<T>> dependsOnFn) {
        return of(set, invertFromFunctionalDependencies(set, dependsOnFn));
    }

    public static <T> PartiallyOrderedSet.Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder for {@code PartiallyOrderedSet}.
     * @param <T> the type of elements
     */
    public static class Builder<T> {
        @Nonnull
        private final ImmutableSet.Builder<T> setBuilder;
        @Nonnull
        private final ImmutableSetMultimap.Builder<T, T> dependencyMapBuilder;

        private Builder() {
            this.setBuilder = ImmutableSet.builder();
            this.dependencyMapBuilder = ImmutableSetMultimap.builder();
        }

        public Builder<T> add(@Nonnull final T element) {
            setBuilder.add(element);
            return this;
        }

        public Builder<T> addDependency(@Nonnull final T targetElement, final T sourceElement) {
            setBuilder.add(sourceElement);
            setBuilder.add(targetElement);
            dependencyMapBuilder.put(targetElement, sourceElement);
            return this;
        }

        public Builder<T> addAll(@Nonnull final Iterable<? extends T> additionalElements) {
            setBuilder.addAll(additionalElements);
            return this;
        }

        public Builder<T> addListWithDependencies(@Nonnull final List<? extends T> additionalElements) {
            setBuilder.addAll(additionalElements);

            final var iterator = additionalElements.iterator();
            if (iterator.hasNext()) {
                var last = iterator.next();
                while (iterator.hasNext()) {
                    final var current = iterator.next();
                    dependencyMapBuilder.put(current, last);
                    last = current;
                }
            }

            return this;
        }

        public PartiallyOrderedSet<T> build() {
            return PartiallyOrderedSet.of(setBuilder.build(), dependencyMapBuilder.build());
        }
    }
}
