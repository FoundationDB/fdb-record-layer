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
import com.google.common.base.Verify;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Utility class to provide helpers related to enumeration of {@code n choose k}.
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

        /**
         * Each "level" represents an element in the final combination. In particular, level {@code i}
         * represents the {@code i}<sup>th</sup> element in the final combination.
         */
        private static final class LevelState {
            private static final int UNBOUND = -1;

            /**
             * The offset separating this level from the previous position. If this is
             * set to {@link #UNBOUND}, then the position has not yet been bound to a valid element.
             */
            private int offset = UNBOUND;
            /**
             * The maximum offset to update this level to.
             */
            private int maxOffset = UNBOUND;

            public boolean isBound() {
                return offset != UNBOUND;
            }

            public void bind(int offset, int maxOffset) {
                Verify.verify(offset >= 1, "Position must be bound to an offset of at least 1");
                this.offset = offset;
                this.maxOffset = maxOffset;
            }

            public void unbind() {
                this.offset = UNBOUND;
                this.maxOffset = UNBOUND;
            }

            public boolean hasMore() {
                return offset < maxOffset;
            }
        }

        private class ComplexIterator extends AbstractIterator<List<T>> implements EnumeratingIterator<T> {
            // state
            private int bound;
            private final List<LevelState> state;
            int currentOffset;

            private ComplexIterator() {
                this.bound = 0;
                this.state = Lists.newArrayListWithCapacity(numberOfElementsToChoose);
                for (int i = 0; i < numberOfElementsToChoose; i ++) {
                    this.state.add(new LevelState());
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
                // If nothing is bound yet, we are at the beginning and should start at level 0. Otherwise,
                // we should begin at the last bound element and increment our current state to select
                // the next one
                //
                int currentLevel = bound == 0 ? 0 : bound - 1;

                //
                // For a combination of elements we return, we need to bind n elements (n == numberOfElementsToChoose).
                // We maintain an iterator through the different offsets for each level, which can be mapped
                // to indexes in the original list. That's the state!
                // The offset for levels greater than currentLevel may be unbound. The offsets
                // for levels below current level must be bound to a valid element. The offset for currentLevel
                // maybe either bound or unbound.
                //

                //
                // We also use an integer value "bound" that keep track of the number of levels that are currently
                // bound to valid values.
                //
                do {
                    final int lastOffset;

                    //
                    // Set the current level's state, that is, assign an offset for the current element.
                    // If it is unbound, we start it over from the beginning; otherwise, we increment its
                    // offset to move on to the next element.
                    //
                    LevelState currentState = state.get(currentLevel);
                    if (currentState.isBound()) {
                        unbind(currentLevel); // Mark this level as unbound and clear out greater levels
                        lastOffset = currentState.offset;
                        currentState.offset++;
                    } else {
                        currentState.bind(1, elements.size() - currentOffset + 1);
                        lastOffset = 0;
                    }

                    //
                    // Search the current level for a next item. If the currentState has no more elements,
                    // we need to abandon it and search the level above (making that level the current
                    // level). If we reach level -1 (i.e., we reach the end of the iterator at level 0), we are done.
                    // If we do find an element not violating any constraints on the current level we bind
                    // the element we found and continue on downward.
                    //
                    if (currentState.hasMore()) {
                        bound += 1;
                        currentOffset += 1;
                        currentLevel += 1;
                    } else {
                        // back tracking -- need to clear out the level's state
                        currentState.unbind();
                        currentOffset -= lastOffset;
                        currentLevel -= 1;
                    }

                    if (currentLevel == -1) {
                        return endOfData();
                    }
                } while (bound < numberOfElementsToChoose); // as long as we still have to find a binding

                final ImmutableList.Builder<T> resultBuilder = ImmutableList.builder();

                int resultOffset = 0;
                for (final LevelState position : state) {
                    resultOffset += position.offset;
                    resultBuilder.add(elements.get(resultOffset - 1));
                }

                return resultBuilder.build();
            }

            private void unbind(final int level) {
                // Mark this level as unbound and reset all the levels after this one
                for (int i = level; i < numberOfElementsToChoose; i ++ ) {
                    LevelState currentPosition = state.get(i);
                    if (currentPosition.isBound()) {
                        bound -= 1;
                        if (i > level) {
                            currentPosition.unbind();
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

                if (!state.get(level).isBound()) {
                    throw new UnsupportedOperationException("cannot skip/unbind as level is not bound at all");
                }

                // reset all the following ones
                for (int i = level + 1; i < numberOfElementsToChoose; i ++ ) {
                    final LevelState levelState = state.get(i);
                    if (levelState.isBound()) {
                        bound -= 1;
                        currentOffset -= levelState.offset;
                        levelState.unbind();
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
     * Create an {@link EnumeratingIterable} of the choose-K sets for a given number of elements based on a set and
     * a function describing the depends-on relationships between items in the given set.
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

    /**
     * Create an {@link Iterable} of the choose-K sets for a given number range of elements based on a set and
     * a function describing the depends-on relationships between items in the given set.
     * @param elements the list of collections to create the iterable over
     * @param startInclusive starting number of elements to choose (inclusive)
     * @param endExclusive ending number of elements to choose (exclusive)
     * @param <T> type
     * @return a new {@link Iterable} that obeys the constraints as expressed in
     *         {@code dependsOnFn} in a sense that the iterators created by this iterator will not return
     *         orderings that violate the given depends-on constraints
     */
    public static <T> Iterable<List<T>> chooseK(@Nonnull final Collection<T> elements, final int startInclusive, final int endExclusive) {
        Preconditions.checkArgument(startInclusive >= 0 && startInclusive <= elements.size());
        Preconditions.checkArgument(endExclusive >= startInclusive && endExclusive - 1 <= elements.size());

        if (startInclusive == endExclusive) {
            return EnumeratingIterable.emptyIterable();
        }

        final var elementsAsList = ImmutableList.copyOf(elements);
        return () -> IntStream.range(startInclusive, endExclusive)
                .boxed()
                .flatMap(index -> Streams.stream(chooseK(elementsAsList, index)))
                .iterator();
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
