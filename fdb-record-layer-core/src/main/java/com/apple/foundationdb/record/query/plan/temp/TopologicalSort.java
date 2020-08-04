/*
 * TopologicalSort.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.google.common.base.Verify;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Utility class to provide helpers related to topological sorts.
 *
 * The main purpose on this class is to provide a specific iterable that can efficiently traverse
 * all possible permutations of the input set that do not violate the given dependency constraints.
 *
 * The iterable {@link TopologicalOrderPermutationIterable} adheres to the following requirements:
 * <ol>
 * <li>it does not violate the given constraints</li>
 * <li>it produces all possible orderings under the given constraints</li>
 * <li>it reacts appropriately to give circular dependency constraints between elements (i.e., no infinite loops)</li>
 * <li>it iterates all orderings on the fly. That is, it stores only the position of the iteration and does not (pre)create
 *    the orderings in memory.</li>
 * </ol>
 *
 * {@link TopologicalOrderPermutationIterable} subclasses {@link Iterable} in order to provide an additional feature
 * that allows for skipping. Assume we have a set
 * <pre>
 * {@code
 * { a, b, c, d } with constraints { b -> c, b -> d } (c depends on b, d depends on b)
 * }
 * </pre>
 *
 * Possible orderings are
 * <pre>
 * {@code
 * (a, b, c, d)
 * (a, b, d, c)
 * (b, a, c, d)
 * (b, a, d, c)
 * (b, c, a, d)
 * (b, c, d, a)
 * (b, d, a, c)
 * (b, d, c, a)
 * }
 * </pre>
 *
 * Frequently we test for a certain property or perform a particular operation given one possible ordering but it is clear
 * that it is not necessary to consider more such orderings that share a common prefix. In the example, it may be
 * beneficial to skip the rest of the {@code (b, a, ...)} orderings after the first one was returned
 * ({@code (b, a, c, d}). In this case we would like to instruct the iterator to skip all such orderings and continue
 * iteration at {@code (b, c, a, d)}. Similarly, we want to skip all orderings starting with {@code (b, ...)} once we
 * encountered the first such ordering. This the iterators created by the provided {@link TopologicalOrderPermutationIterable}
 * of type {@link TopologicalOrderPermutationIterator} provide a method {@link TopologicalOrderPermutationIterator#skip}
 * to allow skipping to a given prefix.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class TopologicalSort {
    /**
     * Iterable that provides special iterators of type {@link TopologicalOrderPermutationIterator}.
     * @param <T> type
     */
    public interface TopologicalOrderPermutationIterable<T> extends Iterable<List<T>> {
        @Nonnull
        @Override
        TopologicalOrderPermutationIterator<T> iterator();
    }

    /**
     * An iterator extending {@link Iterator} providing the ability to skip a certain prefix.
     * @param <T> type
     */
    public interface TopologicalOrderPermutationIterator<T> extends Iterator<List<T>> {
        /**
         * Instructs the iterator to advance to the next possible ordering using the given zero-indexed level.
         *
         * Example 1: If the last returned ordering of the iterator {@code it} is {@code (e0, e1, e2, e3)} and
         *            {@code it.skip(2)} is called, the state of the iterator is advanced in a way that either
         *            reaches the end of iteration or the next item that is returned is {@code (e0', e1', e3', e4')}
         *            where the prefix {@code (e1', e2', e3')} is not equal to {code (e1, e2, e3)}.
         * Example 2: If the last returned ordering of the iterator {@code it} is {@code (e1, e2, e3, e4)} and
         *            {@code it.skip(1)} is called, the state of the iterator is advanced in a way that either reaches
         *            the end of iteration or the next item that is returned is {@code (e1', e2', e3', e4')} where
         *            the prefix {@code (e1', e2')} is not equal to {code (e1, e2)}.
         *
         * @param level skip level
         */
        void skip(int level);
    }

    /**
     * A complex iterable implementing {@link TopologicalOrderPermutationIterable} that is used for sets of cardinality greater
     * than 1 (i.e., the regular case).
     * @param <T> type
     */
    private static class BacktrackIterable<T> implements TopologicalOrderPermutationIterable<T> {
        @Nonnull
        private final ImmutableSet<T> set;
        @Nonnull
        private final SetMultimap<T, T> dependsOnMap;

        private class BacktrackIterator extends AbstractIterator<List<T>> implements TopologicalSort.TopologicalOrderPermutationIterator<T> {
            // state
            private final Set<T> bound;
            private final List<PeekingIterator<T>> state;

            private BacktrackIterator() {
                this.bound = Sets.newHashSetWithExpectedSize(set.size());
                this.state = Lists.newArrayListWithCapacity(set.size());
                for (int i = 0; i < set.size(); i ++ ) {
                    this.state.add(null);
                }
            }

            @Nullable
            @Override
            protected List<T> computeNext() {
                //
                // Iterating through all possible correct orderings of set is inherently easier to encode recursively,
                // however, this being an iterator together with the requirement to skip subtrees of iterations warrants
                // an iterative solution. All hail the Church-Turing thesis!
                //
                if (set.isEmpty()) {
                    return endOfData();
                }

                //
                // If nothing is bound yet, we are at the beginning and should start at level 0, otherwise
                // we conceptually start at the level of the finest granularity bound. Note that that finest
                // granularity is usually the granularity of the last element in the set, but it is possible
                // for fewer elements to be bound if this is the first call after a skip.
                //
                int currentLevel = bound.isEmpty() ? 0 : bound.size() - 1;

                //
                // For each permutation of elements we return, we need to bind n elements (n == set.size()).
                // We maintain an iterator through the set (that is stable) for each level up to currentLevel.
                // That's the state! The iterator for levels greater than currentLevel must be null. The iterators
                // for levels below current level must be on a valid element. The iterator for the currentLevel
                // maybe null or a valid element.
                //

                //
                // We also use a set "bound" that keeps elements that are currently bound by iterators.
                // "bound" is solely kept for convenience and is entirely computable from the current
                // state of all iterators. We must keep it in sync with the state of the iterators at all times.
                //
                do {
                    //
                    // Set the currentIterator. That is the iterator at level currentLevel. If it is null,
                    // we create a new iterator over the set.
                    //
                    final PeekingIterator<T> currentIterator;
                    if (state.get(currentLevel) == null) {
                        currentIterator = Iterators.peekingIterator(set.iterator());
                        state.set(currentLevel, currentIterator);
                    } else {
                        currentIterator = state.get(currentLevel);
                        unbind(currentLevel);
                        currentIterator.next();
                    }

                    //
                    // Search currentLevel for a next item that does not violate any constraints. Doing so
                    // may exhaust currentIterator in which case we couldn't find another element on the current level.
                    // In that case we need to abandon the current level and search on the level above (making that
                    // level the current level). If we reach level -1 (i.e., we reach the end of the iterator at level 0
                    // we are done.
                    // If we do find an element not violating any constraints on the current level we conceptually
                    // bind it the element we found and continue on downward.
                    //
                    final boolean isDown = searchLevel(currentIterator);
                    if (!isDown) {
                        // back tracking -- need to clear out the current iterator
                        state.set(currentLevel, null);
                    }
                    currentLevel = isDown
                                   ? currentLevel + 1
                                   : currentLevel - 1;
                    
                    if (currentLevel == -1) {
                        return endOfData();
                    }
                } while (bound.size() < set.size()); // as long as we still have to find a binding

                return state.stream()
                        .map(PeekingIterator::peek)
                        .collect(ImmutableList.toImmutableList());
            }

            @SuppressWarnings({"squid:S135", "PMD.AvoidBranchingStatementAsLastInLoop"})
            private boolean searchLevel(final PeekingIterator<T> currentIterator) {
                while (currentIterator.hasNext()) {
                    final T next = currentIterator.peek();

                    // check if it is bound already; t is invisible to this loop if it is bound
                    if (bound.contains(next)) {
                        currentIterator.next();
                        continue;
                    }

                    //
                    // Check if t is only dependent on elements in the current bound set, if it is not it must come later.
                    // Note that the intersection removes elements the current element depends on that are not in set.
                    // That behavior is for convenience reasons and and specifically not an error.
                    //
                    final Set<T> dependsOn = Sets.intersection(set, dependsOnMap.get(next));
                    if (!bound.containsAll(dependsOn)) {
                        currentIterator.next();
                        continue;
                    }

                    // this level can be bound now
                    bound.add(next);
                    return true; // go down
                }
                return false; // go up
            }

            private void unbind(final int level) {
                // reset all the following ones
                for (int i = level; i < set.size(); i ++ ) {
                    // either iterator is on a valid item or iterator is null
                    if (state.get(i) != null) {
                        bound.remove(state.get(i).peek());
                    } else {
                        break;
                    }
                }
            }

            /**
             * Method that skips advances to the next element on the given zero-indexed level.
             * @param level level to advance
             */
            @Override
            public void skip(final int level) {
                if (level >= set.size()) {
                    throw new IndexOutOfBoundsException();
                }

                if (state.get(level) == null) {
                    throw new UnsupportedOperationException("cannot skip/unbind as level is not bound at all");
                }

                // reset all the following ones
                for (int i = level + 1; i < set.size(); i ++ ) {
                    // either iterator is on a valid item or iterator is null
                    if (state.get(i) != null) {
                        bound.remove(state.get(i).peek());
                        state.set(i, null);
                    } else {
                        break;
                    }
                }
            }
        }

        private BacktrackIterable(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> dependsOnMap) {
            Verify.verify(set.size() > 1);
            this.set = ImmutableSet.copyOf(set);
            this.dependsOnMap = dependsOnMap;
        }

        @Nonnull
        @Override
        public TopologicalOrderPermutationIterator<T> iterator() {
            return new BacktrackIterator();
        }
    }

    /**
     * A complex iterable implementing a {@link TopologicalOrderPermutationIterable} that is used for sets of cardinality greater
     * than 1 (i.e., the regular case).
     * @param <T> type
     */
    private static class KahnIterable<T> implements TopologicalOrderPermutationIterable<T> {
        @Nonnull
        private final ImmutableSet<T> set;
        @Nonnull
        private final SetMultimap<T, T> usedByMap;

        private class KahnIterator extends AbstractIterator<List<T>> implements TopologicalOrderPermutationIterator<T> {
            // state
            private final Set<T> bound;
            private final Map<T, Integer> inDegreeMap;
            private final List<Set<T>> eligibleElementSets;
            private final List<PeekingIterator<T>> iterators;

            private KahnIterator() {
                this.bound = Sets.newHashSetWithExpectedSize(set.size());
                this.inDegreeMap = computeInDegreeMap();
                this.eligibleElementSets = Lists.newArrayListWithCapacity(set.size());
                // add the set of immediately satisfiable sets
                this.eligibleElementSets
                        .add(this.inDegreeMap
                                .entrySet()
                                .stream()
                                .filter(entry -> entry.getValue() == 0)
                                .map(Map.Entry::getKey)
                                .collect(ImmutableSet.toImmutableSet()));
                for (int i = 1; i < set.size(); i ++ ) {
                    this.eligibleElementSets.add(null);
                }
                this.iterators = Lists.newArrayListWithCapacity(set.size());
                for (int i = 0; i < set.size(); i ++ ) {
                    this.iterators.add(null);
                }
            }

            @Nullable
            @Override
            protected List<T> computeNext() {
                //
                // Iterating through all possible correct orderings of set is inherently easier to encode recursively,
                // however, this being an iterator together with the requirement to skip subtrees of iterations warrants
                // an iterative solution. All hail the Church-Turing thesis!
                //
                if (set.isEmpty()) {
                    return endOfData();
                }

                //
                // If nothing is bound yet, we are at the beginning and should start at level 0, otherwise
                // we conceptually start at the level of the finest granularity bound. Note that that finest
                // granularity is usually the granularity of the last element in the set, but it is possible
                // for fewer elements to be bound if this is the first call after a skip.
                //
                int currentLevel = bound.isEmpty() ? 0 : bound.size() - 1;

                //
                // For each permutation of elements we return, we need to bind n elements (n == set.size()).
                // We maintain an iterator through the set (that is stable) for each level up to currentLevel.
                // That's the state! The iterator for levels greater than currentLevel must be null. The iterators
                // for levels below current level must be on a valid element. The iterator for the currentLevel
                // maybe null or a valid element.
                //

                //
                // We also use a set "bound" that keeps elements that are currently bound by iterators.
                // "bound" is solely kept for convenience and is entirely computable from the current
                // state of all iterators. We must keep it in sync with the state of the iterators at all times.
                //
                do {
                    //
                    // Set the currentIterator. That is the iterator at level currentLevel. If it is null,
                    // we create a new iterator over the set.
                    //
                    final PeekingIterator<T> currentIterator;
                    if (iterators.get(currentLevel) == null) {
                        currentIterator = Iterators.peekingIterator(eligibleElementSets.get(currentLevel).iterator());
                        iterators.set(currentLevel, currentIterator);
                    } else {
                        currentIterator = iterators.get(currentLevel);
                        unbindTail(currentLevel);
                        currentIterator.next();
                    }

                    //
                    // Search currentLevel for a next item that does not violate any constraints. Doing so
                    // may exhaust currentIterator in which case we couldn't find another element on the current level.
                    // In that case we need to abandon the current level and search on the level above (making that
                    // level the current level). If we reach level -1 (i.e., we reach the end of the iterator at level 0
                    // we are done.
                    // If we do find an element not violating any constraints on the current level we conceptually
                    // bind it the element we found and continue on downward.
                    //
                    final boolean foundOnLevel = nextOnLevel(currentIterator);
                    if (!foundOnLevel) {
                        // back tracking -- need to clear out the current iterator
                        iterators.set(currentLevel, null);
                    }
                    currentLevel = foundOnLevel
                                   ? currentLevel + 1
                                   : currentLevel - 1;

                    if (currentLevel == -1) {
                        return endOfData();
                    }
                } while (bound.size() < set.size()); // as long as we still have to find a binding

                return iterators.stream()
                        .map(PeekingIterator::peek)
                        .collect(ImmutableList.toImmutableList());
            }

            @SuppressWarnings({"squid:S135", "UnstableApiUsage", "PMD.AvoidBranchingStatementAsLastInLoop"})
            private boolean nextOnLevel(final PeekingIterator<T> currentIterator) {
                while (currentIterator.hasNext()) {
                    final T next = currentIterator.peek();

                    // check if it is bound already; t is invisible to this loop if it is bound
                    if (bound.contains(next)) {
                        currentIterator.next();
                        continue;
                    }

                    // this level can be bound now
                    bound.add(next);

                    final Set<T> targets = usedByMap.get(next);
                    final ImmutableSet.Builder<T> newlyEligibleElementsBuilder = ImmutableSet.builderWithExpectedSize(targets.size());
                    for (final T target : targets) {
                        final int newInDegree = inDegreeMap.compute(target, (k, v) -> Objects.requireNonNull(v) - 1);
                        Verify.verify(newInDegree >= 0);
                        if (newInDegree == 0) {
                            newlyEligibleElementsBuilder.add(target);
                        }
                    }

                    if (bound.size() < set.size()) {
                        newlyEligibleElementsBuilder
                                .addAll(eligibleElementSets.get(bound.size() - 1))
                                .build();
                        eligibleElementSets.set(bound.size(), newlyEligibleElementsBuilder.build());
                    } else {
                        // the last round cannot possible have added new elements into the eligibility sets
                        Verify.verify(newlyEligibleElementsBuilder.build().isEmpty());
                    }

                    return true; // able to bind element, go right
                }

                return false; // unable to bind element, go left
            }

            private void unbindTail(final int level) {
                // reset all the following ones
                for (int i = level; i < set.size(); i ++ ) {
                    // either iterator is on a valid item or iterator is null
                    if (iterators.get(i) != null) {
                        unbindAt(i);
                    } else {
                        break;
                    }
                }
            }

            private void unbindAt(final int level) {
                final T toUnbind = iterators.get(level).peek();
                bound.remove(toUnbind);
                final Set<T> targets = usedByMap.get(toUnbind);
                for (final T target : targets) {
                    final int newInDegree = inDegreeMap.compute(target, (k, v) -> Objects.requireNonNull(v) + 1);
                    Verify.verify(newInDegree > 0);
                }
            }

            /**
             * Method that skips advances to the next element on the given zero-indexed level.
             * @param level level to advance
             */
            @Override
            public void skip(final int level) {
                if (level >= set.size()) {
                    throw new IndexOutOfBoundsException();
                }

                if (iterators.get(level) == null) {
                    throw new UnsupportedOperationException("cannot skip/unbind as level is not bound at all");
                }

                // reset all the following ones
                for (int i = level + 1; i < set.size(); i ++ ) {
                    // either iterator is on a valid item or iterator is null
                    if (iterators.get(i) != null) {
                        unbindAt(i);
                        iterators.set(i, null);
                    } else {
                        break;
                    }
                }
            }
        }

        private KahnIterable(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> usedByMap) {
            Verify.verify(set.size() > 1);
            this.set = ImmutableSet.copyOf(set);
            this.usedByMap = usedByMap;
        }

        @Nonnull
        @Override
        public TopologicalOrderPermutationIterator<T> iterator() {
            return new KahnIterator();
        }

        @Nonnull
        @SuppressWarnings("java:S3398")
        private Map<T, Integer> computeInDegreeMap() {
            final HashMap<T, Integer> result = Maps.newHashMapWithExpectedSize(set.size());
            set.forEach(element -> result.put(element, 0));

            for (final Map.Entry<T, T> entry : usedByMap.entries()) {
                result.compute(entry.getValue(), (t, v) -> Objects.requireNonNull(v) + 1);
            }
            return result;
        }

        private static <T> SetMultimap<T, T> computeUsedByMap(@Nonnull final Set<T> set, @Nonnull final Function<T, Set<T>> dependsOnFn) {
            // invert the dependencies
            final SetMultimap<T, T> result = Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);

            for (final T element : set) {
                final Set<T> dependsOnElements = dependsOnFn.apply(element);
                for (final T dependsOnElement : dependsOnElements) {
                    if (set.contains(dependsOnElement)) {
                        result.put(dependsOnElement, element);
                    }
                }
            }

            // normally we should make an immutable copy here; due to performance reasons and the fact that this
            // map is private we don't
            return result;
        }
    }

    /**
     * An implementation of {@link TopologicalOrderPermutationIterable} that is optimized to work for empty
     * input sets. The case where the input set is empty is trivial and also properly handled by {@link BacktrackIterable}.
     * Iterators created by this class, however, avoid to build complex state objects during their lifecycle.
     *
     * @param <T> type
     */
    private static class EmptyIterable<T> implements TopologicalOrderPermutationIterable<T> {
        private class EmptyIterator extends AbstractIterator<List<T>> implements TopologicalOrderPermutationIterator<T> {
            @Override
            public void skip(final int level) {
                throw new UnsupportedOperationException("cannot skip on empty iterator");
            }

            @Nullable
            @Override
            protected List<T> computeNext() {
                return endOfData();
            }
        }

        @Nonnull
        @Override
        public TopologicalOrderPermutationIterator<T> iterator() {
            return new EmptyIterator();
        }
    }

    /**
     * An implementation of {@link TopologicalOrderPermutationIterable} that is optimized to work for single item
     * input sets. The case where the input set is exactly one item is trivial and also properly handled by
     * {@link BacktrackIterable}. Iterators created by this class, however, avoid to build complex state objects
     * during their lifecycle.
     *
     * @param <T> type
     */
    private static class SingleIterable<T> implements TopologicalOrderPermutationIterable<T> {
        @Nonnull
        private final T singleElement;

        private SingleIterable(@Nonnull final T singleElement) {
            this.singleElement = singleElement;
        }

        private class SingleIterator extends AbstractIterator<List<T>> implements TopologicalOrderPermutationIterator<T> {
            boolean atFirst = true;

            @Override
            public void skip(final int level) {
                if (atFirst) {
                    throw new UnsupportedOperationException("cannot skip on before first element");
                }
                // no op, we are at the end
            }

            @Override
            protected List<T> computeNext() {
                if (!atFirst) {
                    return endOfData();
                }

                atFirst = false;

                return ImmutableList.of(singleElement);
            }
        }

        @Nonnull
        @Override
        public TopologicalOrderPermutationIterator<T> iterator() {
            return new SingleIterator();
        }
    }

    /**
     * Create a {@link TopologicalOrderPermutationIterable} based on a set and a function describing
     * the depends-on relationships between items in the given set.
     * @param set the set to create the iterable over
     * @param dependsOnFn a function from {@code T} to {@code Set<T>} that can be called during the lifecycle of all
     *        iterators multiple times repeatedly or not at all for any given element in {@code set}. This method is
     *        expected to return instantly and must be stable. Note it is allowed for the set the given function returns
     *        to contain elements of type {@code T} that are not in {@code set}. These items are ignored by the
     *        underlying algorithm (that is, they are satisfied by every ordering).
     * @param <T> type
     * @return a new {@link TopologicalOrderPermutationIterable} that obeys the constraints as expressed in
     *         {@code dependsOnFn} in a sense that the iterators created by this iterator will not return
     *         orderings that violate the given depends-on constraints
     */
    public static <T> TopologicalOrderPermutationIterable<T> topologicalOrderPermutations(@Nonnull final Set<T> set,
                                                                                          @Nonnull final Function<T, Set<T>> dependsOnFn) {
        // try simple
        @Nullable
        final TopologicalOrderPermutationIterable<T> maybeSimpleIterable = trySimpleIterable(set);
        if (maybeSimpleIterable != null) {
            return maybeSimpleIterable;
        }

        //
        // We can use two implementations to deal with the complex case. If there are quite a few dependencies,
        // we should use Kahn's algorithm, as finding a topological ordering is linear and there hopefully are not too
        // many possibilities to iterate.
        // When the number of dependencies is smaller the degree of freedom is naturally higher and finding a
        // sound topological ordering is balanced out by the work to enumerate all such orderings making
        // Kahn's algorithm inefficient as it creates more objects (churn) than the backtracking algorithm.
        // We just use a naive way of making the decision for now.
        // TODO revisit later
        //

        // try Kahn's algorithm
        final SetMultimap<T, T> usedByMap = KahnIterable.computeUsedByMap(set, dependsOnFn);
        if ((double)usedByMap.size() / (double) set.size() > 0.5d) {
            return new KahnIterable<>(set, usedByMap);
        }

        // use backtracking
        final SetMultimap<T, T> dependsOnMap = Multimaps.newSetMultimap(Maps.newHashMap(), HashSet::new);
        Multimaps.invertFrom(usedByMap, dependsOnMap);
        return new BacktrackIterable<>(set, dependsOnMap);
    }

    @Nullable
    private static <T> TopologicalOrderPermutationIterable<T> trySimpleIterable(@Nonnull final Set<T> set) {
        if (set.isEmpty()) {
            return new EmptyIterable<>();
        } else if (set.size() == 1) {
            return new SingleIterable<>(Iterables.getOnlyElement(set));
        }
        return null;
    }

    /**
     * Create a correct topological ordering based on a set and a function describing
     * the depends-on relationships between items in the given set.
     * @param set the set to create the iterable over
     * @param dependsOnFn a function from {@code T} to {@code Set<T>} that can be called during the lifecycle of all
     *        iterators multiple times repeatedly or not at all for any given element in {@code set}. This method is
     *        expected to return instantly and must be stable. Note it is allowed for the set the given function returns
     *        to contain elements of type {@code T} that are not in {@code set}. These items are ignored by the
     *        underlying algorithm (that is, they are satisfied by every ordering).
     * @param <T> type
     * @return a permutation of the set that is topologically correctly ordered with respect to {@code dependsOnFn}
     */
    public static <T> Optional<List<T>> anyTopologicalOrderPermutation(@Nonnull final Set<T> set, @Nonnull final Function<T, Set<T>> dependsOnFn) {
        final TopologicalOrderPermutationIterator<T> iterator;

        // try simple
        @Nullable
        final TopologicalOrderPermutationIterable<T> maybeSimpleIterable = trySimpleIterable(set);
        if (maybeSimpleIterable != null) {
            iterator = maybeSimpleIterable.iterator();
        }  else {
            // no simple iterable -> use Kahn's algorithm which is superior to backtracking if we only look for
            // one permutation.
            iterator = new KahnIterable<>(set, KahnIterable.computeUsedByMap(set, dependsOnFn)).iterator();
        }
        if (iterator.hasNext()) {
            return Optional.of(iterator.next());
        }
        return Optional.empty();
    }
}
