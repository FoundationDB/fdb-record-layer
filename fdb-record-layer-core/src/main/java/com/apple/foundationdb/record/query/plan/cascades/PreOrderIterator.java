/*
 * PreOrderIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An iterator that accesses all elements of a {@link TreeLike} object in pre-order fashion.
 * It attempts to reduce the number of memory allocations while still being performant. It does so
 * by implementing a mix of level-based traversal and pre-order traversal, where a {@link Deque} is used
 * (as a stack) to enable depth first search, however instead of maintaining individual {@link TreeLike} node in
 * each stack frame the entire list of node's children is stored instead in addition to an index pointing out
 * to the current element that is returned by the stream.
 *
 * @param <T> The type of the iterator element.
 */
@NotThreadSafe
public final class PreOrderIterator<T extends TreeLike<T>> implements Iterator<T> {

    @Nonnull
    private final Deque<Pair<Iterable<? extends T>, Integer>> stack;

    private static final int INITIAL_POSITION = -1;

    private PreOrderIterator(@Nonnull final T traversable) {
        // initialize the stack with the {@link TreeLike}'s depth as capacity to avoid resizing.
        stack = new ArrayDeque<>(traversable.height());
        // this is the only list allocation done to put the root in the stack.
        // all the remaining lists added to the stack are references to children
        // lists (copy by reference).
        stack.push(MutablePair.of(List.of(traversable.getThis()), INITIAL_POSITION));
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (stack.isEmpty()) {
                return false;
            }
            final var top = Verify.verifyNotNull(stack.peekLast());
            final var currentLevelIndex = top.getRight();
            final var currentLevelItemsCount = Iterables.size(top.getLeft());
            if (currentLevelIndex + 1 < currentLevelItemsCount) {
                return true;
            } else {
                // pop the stack, and continue doing so, until we either find a next element
                // by looking into previous levels in reverse (stack) order, progressively
                // do so until either an item is found, or the stack becomes empty, and no item
                // is to be found meaning that the traversal is complete.
                stack.removeLast();
            }
        }
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException("no more elements");
        }
        final var top = Verify.verifyNotNull(stack.peekLast());
        final var currentIndexPosition = top.getRight();
        final var currentLevelItems = top.getLeft();
        final var nextItemIndex = currentIndexPosition + 1;

        // mark the next item as the current one in this stack frame, note that
        // the position must be valid because hasNext() is called first, and hasNext()
        // makes sure that, if there is a next element, it modifies the stack index such
        // that incrementing it would lead to finding that next element correctly.
        final var result = Iterables.get(currentLevelItems, nextItemIndex);
        top.setValue(nextItemIndex);
        final var resultChildren = result.getChildren();

        // descend immediately to the children (if any) so to conform to pre-order DFS semantics.
        if (!Iterables.isEmpty(resultChildren)) {
            stack.add(MutablePair.of(resultChildren, INITIAL_POSITION));
        }
        return result;
    }

    @Nonnull
    public static <T extends TreeLike<T>> PreOrderIterator<T> over(@Nonnull final T traversable) {
        return new PreOrderIterator<>(traversable);
    }
}
