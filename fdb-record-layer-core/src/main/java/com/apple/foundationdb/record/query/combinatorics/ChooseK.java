/*
 * ChooseK.java
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

import com.apple.foundationdb.annotation.API;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.MutablePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Utility class to provide helpers related to enumeration of cross products.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class ChooseK {

    private ChooseK() {
        // prevent instantiation
    }

    /**
     * A complex iterable implementing {@link EnumeratingIterable} that is used for sets of cardinality greater
     * than 1 (i.e., the regular case).
     * @param <T> type
     */
    private static class ComplexIterable<T> implements EnumeratingIterable<T> {
        @Nonnull
        private final List<T> elements;
        private final int numberOfElementsToChoose;

        private class ComplexIterator extends AbstractIterator<List<T>> implements EnumeratingIterator<T> {
            // state
            private int bound;
            private final List<MutablePair<Integer, Integer>> state;
            int currentOffset;

            private ComplexIterator() {
                this.bound = 0;
                this.state = Lists.newArrayListWithCapacity(numberOfElementsToChoose);
                for (int i = 0; i < numberOfElementsToChoose; i ++) {
                    this.state.add(MutablePair.of(-1, -1));
                }
                this.currentOffset = 0;
            }

            @Nullable
            @Override
            protected List<T> computeNext() {
                if (elements.isEmpty()) {
                    return endOfData();
                }

                //
                // If nothing is bound yet, we are at the beginning and should start at level 0, otherwise
                // we conceptually start at the level of the finest granularity bound.
                //
                int currentLevel = bound == 0 ? 0 : bound - 1;

                //
                // For permutation of elements we return, we need to bind n elements (n == sources.size()).
                // We maintain an iterator through the iterators over sources (that is stable) for each level up to
                // currentLevel. That's the state!
                // The iterator for levels greater than currentLevel may be null. The iterators
                // for levels below current level must be on a valid element. The iterator for the currentLevel
                // maybe null or a valid element.
                //

                //
                // We also use an integer value "bound" that keep track of the level that is currently bound by iterators.
                //
                do {
                    final int lastOffset;

                    //
                    // Set the currentIterator. That is the iterator at level currentLevel. If it is null,
                    // we create a new iterator over the set.
                    //
                    MutablePair<Integer, Integer> currentPair = state.get(currentLevel);
                    if (currentPair.left == -1) {
                        currentPair.left = 1;
                        currentPair.right = elements.size() - currentOffset + 1;
                        lastOffset = 0;
                    } else {
                        unbind(currentLevel);
                        lastOffset = currentPair.left;
                        currentPair.left++;
                    }

                    //
                    // Search currentLevel for a next item. Doing so may exhaust currentIterator in which case we
                    // couldn't find another element on the current level.
                    // In that case we need to abandon the current level and search on the level above (making that
                    // level the current level). If we reach level -1 (i.e., we reach the end of the iterator at level 0
                    // we are done.
                    // If we do find an element not violating any constraints on the current level we conceptually
                    // bind the element we found and continue on downward.
                    //
                    final boolean isDown = currentPair.left < currentPair.right;
                    if (isDown) {
                        bound += 1;
                        currentOffset += 1;
                        currentLevel += 1;
                    } else {
                        // back tracking -- need to clear out the current iterator
                        currentPair.left = -1;
                        currentPair.right = -1;
                        currentOffset -= lastOffset;
                        currentLevel -= 1;
                    }

                    if (currentLevel == -1) {
                        return endOfData();
                    }
                } while (bound < numberOfElementsToChoose); // as long as we still have to find a binding

                final ImmutableList.Builder<T> resultBuilder = ImmutableList.builder();

                int resultOffset = 0;
                for (final MutablePair<Integer, Integer> element : state) {
                    resultOffset += element.left;
                    resultBuilder.add(elements.get(resultOffset - 1));
                }

                return resultBuilder.build();
            }

            private void unbind(final int level) {
                // reset all the following ones
                for (int i = level; i < numberOfElementsToChoose; i ++ ) {
                    // either iterator is on a valid item or iterator is null
                    MutablePair<Integer, Integer> currentPair = state.get(i);
                    if (currentPair.left != -1) {
                        bound -= 1;
                        if (i > level) {
                            currentPair.left = -1;
                            currentPair.right = -1;
                        }
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
                if (level >= numberOfElementsToChoose) {
                    throw new IndexOutOfBoundsException();
                }

                if (state.get(level).left == -1) {
                    throw new UnsupportedOperationException("cannot skip/unbind as level is not bound at all");
                }

                // reset all the following ones
                for (int i = level + 1; i < numberOfElementsToChoose; i ++ ) {
                    // either iterator is on a valid item or iterator is null
                    final MutablePair<Integer, Integer> pair = state.get(i);
                    if (pair != null) {
                        bound -= 1;
                        currentOffset -= pair.left;
                        pair.left = -1;
                        pair.right = -1;
                    } else {
                        break;
                    }
                }
            }
        }

        private ComplexIterable(@Nonnull final Iterable<? extends T> elements, final int numberOfElementsToChoose) {
            this.elements = ImmutableList.copyOf(elements);
            this.numberOfElementsToChoose = numberOfElementsToChoose;
        }

        @Nonnull
        @Override
        public EnumeratingIterator<T> iterator() {
            return new ComplexIterator();
        }
    }

    /**
     * An implementation of {@link EnumeratingIterable} that is optimized to work for single item
     * input sets. The case where the input set is exactly one item is trivial and also properly handled by
     * {@link ComplexIterable}. Iterators created by this class, however, avoid building complex state objects
     * during their lifecycle.
     *
     * @param <T> type
     */
    private static class SingleIterable<T> implements EnumeratingIterable<T> {
        @Nonnull
        private final List<T> singleElement;

        private SingleIterable(@Nonnull final List<T> singleElement) {
            this.singleElement = singleElement;
        }

        private class SingleIterator extends AbstractIterator<List<T>> implements EnumeratingIterator<T> {
            boolean atFirst = true;

            @Override
            public void skip(final int level) {
                if (atFirst) {
                    throw new UnsupportedOperationException("cannot skip on before first element");
                }

                // skipping is a non-op
            }

            @Override
            protected List<T> computeNext() {
                if (atFirst) {
                    atFirst = false;
                    return singleElement;
                }

                return endOfData();
            }
        }

        @Nonnull
        @Override
        public EnumeratingIterator<T> iterator() {
            return new SingleIterator();
        }
    }

    /**
     * Create a {@link EnumeratingIterable} based on a set and a function describing
     * the depends-on relationships between items in the given set.
     * @param elements the list of collections to create the iterable over
     * @param numberOfElementsToChoose number {@code k} of elements to choose
     * @param <T> type
     * @return a new {@link EnumeratingIterable} that obeys the constraints as expressed in
     *         {@code dependsOnFn} in a sense that the iterators created by this iterator will not return
     *         orderings that violate the given depends-on constraints
     */
    public static <T> EnumeratingIterable<T> chooseK(@Nonnull final Collection<? extends T> elements, final int numberOfElementsToChoose) {
        Preconditions.checkArgument(numberOfElementsToChoose >= 0 && numberOfElementsToChoose <= elements.size());
        // try simple
        @Nullable
        final EnumeratingIterable<T> maybeSimpleIterable = trySimpleIterable(elements, numberOfElementsToChoose);
        if (maybeSimpleIterable != null) {
            return maybeSimpleIterable;
        }

        return new ComplexIterable<>(elements, numberOfElementsToChoose);
    }

    @Nullable
    private static <T> EnumeratingIterable<T> trySimpleIterable(@Nonnull final Collection<? extends T> elements, final int numberOfElementsToChoose) {
        if (elements.isEmpty() || numberOfElementsToChoose == 0) {
            return new SingleIterable<>(ImmutableList.of());
        } else if (elements.size() == 1) {
            final T onlyElement = Iterables.getOnlyElement(elements);
            return new SingleIterable<>(ImmutableList.of(onlyElement));
        }
        return null;
    }
}
