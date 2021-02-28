/*
 * TreeLike.java
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

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Interface for tree-like structures and helpers for folds and traversals.
 * @param <T> type parameter
 */
public interface TreeLike<T extends TreeLike<T>> {

    T getThis();

    /**
     * Method to retrieve a list of children values.
     * @return a list of children
     */
    Iterable<? extends T> getChildren();

    T withChildren(final Iterable<? extends T> newChildren);

    @Nonnull
    default Iterable<? extends T> inPreOrder() {
        final ImmutableList.Builder<Iterable<? extends T>> iterablesBuilder = ImmutableList.builder();
        iterablesBuilder.add(ImmutableList.of(getThis()));
        for (final T child : getChildren()) {
            iterablesBuilder.add(child.inPreOrder());
        }
        return Iterables.concat(iterablesBuilder.build());
    }

    @Nonnull
    default Iterable<? extends T> inPostOrder() {
        final ImmutableList.Builder<Iterable<? extends T>> iterablesBuilder = ImmutableList.builder();
        for (final T child : getChildren()) {
            iterablesBuilder.add(child.inPostOrder());
        }
        iterablesBuilder.add(ImmutableList.of(getThis()));
        return Iterables.concat(iterablesBuilder.build());
    }

    @Nonnull
    default Iterable<? extends T> filter(@Nonnull final Predicate<T> predicate) {
        return Iterables.filter(inPreOrder(), predicate::test);
    }

    @Nonnull
    default <M, F> F fold(@Nonnull final Function<T, M> mapFunction,
                          @Nonnull BiFunction<M, Iterable<? extends F>, F> foldFunction) {
        final M mappedThis = mapFunction.apply(getThis());
        final ImmutableList.Builder<F> foldedChildrenBuilder = ImmutableList.builder();

        for (final T child : getChildren()) {
            foldedChildrenBuilder.add(child.fold(mapFunction, foldFunction));
        }

        return foldFunction.apply(mappedThis, foldedChildrenBuilder.build());
    }

    @Nullable
    default <M, F> F foldNullable(@Nonnull final Function<T, M> mapFunction,
                                  @Nonnull BiFunction<M, Iterable<? extends F>, F> foldFunction) {
        final M mappedThis = mapFunction.apply(getThis());
        final List<F> foldedChildren = Lists.newArrayList();

        for (final T child : getChildren()) {
            foldedChildren.add(child.foldNullable(mapFunction, foldFunction));
        }

        return foldFunction.apply(mappedThis, foldedChildren);
    }

    @Nonnull
    default <V extends TreeLike<V>> Optional<V> foldTreeLikeMaybe(@Nonnull final BiFunction<T, Iterable<? extends V>, V> foldFunction) {
        return Optional.ofNullable(foldNullable(Function.identity(), foldFunction));
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    default Optional<T> mapLeavesMaybe(@Nonnull final UnaryOperator<T> replaceOperator) {
        return foldTreeLikeMaybe((t, foldedChildren) -> {
            if (Iterables.isEmpty(foldedChildren)) {
                return replaceOperator.apply(t);
            }

            final Iterator<? extends T> foldedChildrenIterator = foldedChildren.iterator();
            boolean isDifferent = false;
            for (T child : t.getChildren()) {
                Verify.verify(foldedChildrenIterator.hasNext());

                final T nextFoldedChild = foldedChildrenIterator.next();
                if (nextFoldedChild == null) {
                    return null;
                }
                if (child != nextFoldedChild) {
                    isDifferent = true;
                    break;
                }
            }

            if (isDifferent) {
                return t.withChildren(foldedChildren);
            }
            return t;
        });
    }
}
