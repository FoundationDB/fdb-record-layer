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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
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
public final class PreOrderIterator<T extends TreeLike<T>> extends AbstractIterator<T> {
    @Nonnull
    private final T root;

    @Nonnull
    private final Deque<Iterator<? extends T>> stack;

    boolean skipNextSubtree;
    @Nullable
    private T lastElement;

    private PreOrderIterator(@Nonnull final T root) {
        this.root = root;
        // initialize the stack with the {@link TreeLike}'s depth as capacity to avoid resizing.
        // this is the only list allocation done to put the root in the stack.
        // all the remaining lists added to the stack are references to children
        // lists (copy by reference).
        this.stack = new ArrayDeque<>(root.height());
        this.lastElement = null;
    }

    @Nullable
    @Override
    protected T computeNext() {
        if (lastElement == null) {
            if (skipNextSubtree) {
                return endOfData();
            }
            stack.add(Iterators.singletonIterator(root));
        } else {
            if (!skipNextSubtree) {
                final var resultChildren = lastElement.getChildren();

                // descend immediately to the children (if any) so to conform to pre-order DFS semantics.
                if (!Iterables.isEmpty(resultChildren)) {
                    stack.add(resultChildren.iterator());
                }
            }
        }
        skipNextSubtree = false;
        final var current = advance();
        if (current == null) {
            return endOfData();
        } else {
            this.lastElement = current;
            return current;
        }
    }

    @Nullable
    private T advance() {
        while (true) {
            if (stack.isEmpty()) {
                return null;
            }
            final var top = Verify.verifyNotNull(stack.peekLast());
            if (top.hasNext()) {
                return top.next();
            } else {
                // pop the stack, and continue doing so, until we either find a next element
                // by looking into previous levels in reverse (stack) order, progressively
                // do so until either an item is found, or the stack becomes empty, and no item
                // is to be found meaning that the traversal is complete.
                stack.removeLast();
            }
        }
    }

    public void skipNextSubtree() {
        skipNextSubtree = true;
    }

    @Nonnull
    public Iterator<T> descendOnlyIf(@Nonnull Predicate<? super T> predicate) {
        return new AbstractIterator<T>() {
            @Nullable
            @Override
            protected T computeNext() {
                if (!PreOrderIterator.this.hasNext()) {
                    return endOfData();
                }
                final var current = PreOrderIterator.this.next();
                if (!predicate.test(current)) {
                    PreOrderIterator.this.skipNextSubtree();
                }
                return current;
            }
        };
    }

    /**
     * Retuns an iterator that traverses {@code treeLike} in pre-order.
     * @param treeLike a {@link TreeLike} treeLike.
     * @param <T> The type of {@code treeLike} items.
     * @return an iterator that traverses the items in pre-order.
     */
    @Nonnull
    public static <T extends TreeLike<T>> PreOrderIterator<T> over(@Nonnull final T treeLike) {
        return new PreOrderIterator<>(treeLike);
    }
}
