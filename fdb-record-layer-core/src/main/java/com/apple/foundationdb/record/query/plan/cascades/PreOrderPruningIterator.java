/*
 * PreOrderPruningIterator.java
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

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
public final class PreOrderPruningIterator<T extends TreeLike<T>> implements Iterator<T> {

    @Nonnull
    private final Deque<Iterator<? extends  T>> stack;

    @Nonnull
    private final Predicate<T> descendIntoChildrenPredicate;

    private PreOrderPruningIterator(@Nonnull final T treeLike) {
        this(treeLike, ignored -> true);
    }

    private PreOrderPruningIterator(@Nonnull final T treeLike, @Nonnull final Predicate<T> descendIntoChildrenPredicate) {
        // initialize the stack with the {@link TreeLike}'s depth as capacity to avoid resizing.
        stack = new ArrayDeque<>(treeLike.height());
        // this is the only list allocation done to put the root in the stack.
        // all the remaining lists added to the stack are references to children
        // lists (copy by reference).
        stack.push(ImmutableList.of(treeLike.getThis()).iterator());
        this.descendIntoChildrenPredicate = descendIntoChildrenPredicate;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (stack.isEmpty()) {
                return false;
            }
            final var top = Verify.verifyNotNull(stack.peekLast());
            if (top.hasNext()) {
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
        // mark the next item as the current one in this stack frame, note that
        // the iterator must have another element because hasNext() is called first, and hasNext()
        // makes sure that, the top iterator in the stack always has a next element.
        final var result = top.next();

        // test whether we should descend into the children of the current node or prune them
        // and continue to the next unexplored node in pre-order.
        if (descendIntoChildrenPredicate.test(result)) {
            final var resultChildren = result.getChildren();

            // descend immediately to the children (if any) so to conform to pre-order DFS semantics.
            if (!Iterables.isEmpty(resultChildren)) {
                stack.add(resultChildren.iterator());
            }
        }
        return result;
    }

    /**
     * Retuns an iterator that traverses {@code treeLike} in pre-order.
     * @param treeLike a {@link TreeLike} treeLike.
     * @param <T> The type of {@code treeLike} items.
     * @return an iterator that traverses the items in pre-order.
     */
    @Nonnull
    public static <T extends TreeLike<T>> PreOrderPruningIterator<T> over(@Nonnull final T treeLike) {
        return new PreOrderPruningIterator<>(treeLike);
    }

    /**
     * Retuns an iterator that traverses {@code treeLike} in pre-order with a children-pruning condition.
     * @param treeLike a {@link TreeLike} treeLike.
     * @param descendIntoChildrenPredicate a condition that determines whether, for the currently visited node, the iterator
     *                          should descend to the children or not.
     * @param <T> The type of {@code treeLike} items.
     * @return an iterator that traverses the items in pre-order.
     */
    @Nonnull
    public static <T extends TreeLike<T>> PreOrderPruningIterator<T> overWithPruningPredicate(@Nonnull final T treeLike,
                                                                                              @Nonnull Predicate<T> descendIntoChildrenPredicate) {
        return new PreOrderPruningIterator<>(treeLike, descendIntoChildrenPredicate);
    }
}
