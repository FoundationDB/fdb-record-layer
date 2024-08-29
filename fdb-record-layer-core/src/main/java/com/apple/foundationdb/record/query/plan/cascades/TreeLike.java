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

package com.apple.foundationdb.record.query.plan.cascades;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * Interface for tree-like structures and helpers for folds and traversals on these tree-like structures of node type
 * {@link T}.
 * @param <T> type parameter of the node
 */
public interface TreeLike<T extends TreeLike<T>> {

    /**
     * Method to get a {@code T}-typed {@code this}. This is the equivalent of Scala's typed self. Java's type inference
     * of code referring to {@code this} in this interface infers {@code TreeLike<T extends TreeLike<T>>}. By convention
     * any implementor should implement {@code class Tree implements TreeLike<Tree> ...} meaning that the implementor is
     * {@code T} for those cases. Providing a narrowing method to get {@code this} from the implementor avoids an
     * unchecked cast.
     * @return {@code this} of type {@code T}.
     */
    @Nonnull
    T getThis();

    /**
     * Method to retrieve a list of children values.
     * @return a list of children
     */
    @Nonnull
    Iterable<? extends T> getChildren();

    /**
     * Recreate the current treelike object with a new list of children.
     * @param newChildren new children
     * @return a copy of {@code this} using the new children passed in
     */
    @Nonnull
    T withChildren(Iterable<? extends T> newChildren);

    /**
     * Returns an iterator that traverse the nodes in pre-order.
     * @return an iterator that traverse the nodes in pre-order.
     */
    @Nonnull
    default Iterator<T> preOrderIterator() {
        return PreOrderPruningIterator.over(getThis());
    }

    default Iterator<T> preOrderPruningIterator(@Nonnull final Predicate<T> descendInChildren) {
        return PreOrderPruningIterator.overWithPruningPredicate(getThis(), descendInChildren);
    }

    /**
     * Returns a {@link Stream} that traverses the nodes in pre-order.
     * @return a {@link Stream} that traverses the nodes in pre-order.
     */
    @Nonnull
    default Stream<T> preOrderStream() {
        return Streams.stream(preOrderIterator());
    }

    /**
     * Method that returns an {@link Iterable} of nodes as encountered in pre-order traversal of this tree-like.
     * @return an {@link Iterable} of nodes
     */
    @Nonnull
    default Iterable<? extends T> preOrderIterable() {
        return preOrderIterable(self -> true);
    }

    /**
     * Method that returns an {@link Iterable} of nodes as encountered in pre-order traversal of this tree-like.
     * @param descentIntoPredicate a predicate that determines of the traversal enters the subtree underneath the
     *        current node
     * @return an {@link Iterable} of nodes
     */
    @Nonnull
    default Iterable<? extends T> preOrderIterable(@Nonnull final Predicate<T> descentIntoPredicate) {
        final ImmutableList.Builder<Iterable<? extends T>> iterablesBuilder = ImmutableList.builder();
        final var self = getThis();
        iterablesBuilder.add(ImmutableList.of(self));
        if (descentIntoPredicate.test(self)) {
            for (final T child : getChildren()) {
                iterablesBuilder.add(child.preOrderIterable());
            }
        }
        return Iterables.concat(iterablesBuilder.build());
    }

    /**
     * Method that returns an {@link Iterable} of nodes as encountered in post-order traversal of this tree-like.
     * @return an {@link Iterable} of nodes
     */
    @Nonnull
    default Iterable<? extends T> postOrderIterable() {
        final ImmutableList.Builder<Iterable<? extends T>> iterablesBuilder = ImmutableList.builder();
        for (final T child : getChildren()) {
            iterablesBuilder.add(child.postOrderIterable());
        }
        iterablesBuilder.add(ImmutableList.of(getThis()));
        return Iterables.concat(iterablesBuilder.build());
    }

    /**
     * Method that performs a tree fold over the tree-like rooted at {@code this} using the given map and fold functions.
     * Note that this variant of fold does not permit {@code null} values to be produced and consumed by the map and
     * fold functions passed in. In return this method guarantees that the result of the folding the tree rooted at
     * {@code this} is well-defined and not {@code null}.
     * @param mapFunction a mapping {@link Function} that computes a scalar value of type {@code M} from a node of type {@code T}.
     * @param foldFunction a folding {@link Function} that computes the result of the fold of a node {@code n} given
     *        an instance of type {@code M} (which is the result of applying {@code mapFunction})
     *        and an iterable of instances of type {@code F} that are the result of folding the children of that node.
     *        The result of the folding function is also of type {@code F}.
     *        Note that in this variant, the fold function is guaranteed to not be called with {@code null} for
     *        the mapped {@code this}. Also, the {@link Iterable} over the children's folds does not produce
     *        {@code null}s. In response to that the fold function itself is not allowed to return a {@code null}.
     * @param <M> the type parameter of the result of the mapping function
     * @param <F> the type parameter of the result of the folding function
     * @return the fold of the tree rooted at {@code this}
     */
    @Nonnull
    default <M, F> F fold(@Nonnull final NonnullFunction<T, M> mapFunction,
                          @Nonnull final NonnullBiFunction<M, Iterable<? extends F>, F> foldFunction) {
        final M mappedThis = mapFunction.apply(getThis());
        final ImmutableList.Builder<F> foldedChildrenBuilder = ImmutableList.builder();

        for (final T child : getChildren()) {
            foldedChildrenBuilder.add(child.fold(mapFunction, foldFunction));
        }

        return foldFunction.apply(mappedThis, foldedChildrenBuilder.build());
    }

    /**
     * Method that performs a tree fold over the tree-like rooted at {@code this} using the given map and fold functions.
     * Note that this variant of fold does permit {@code null} values to be produced and consumed by the map and
     * fold functions passed in. This method does not guarantee the existence of a well-defined tree-fold. A {@code null}
     * that is returned by either the map function or the fold function is normally considered to be a result indicative
     * of the absence of a well-defined fold for the respective subtree that is being considered at the time.
     * However, this method does not enforce this view meaning that callers can react to and pass on {@code null}s as
     * they see fit (by means of the provided lambdas for map and fold functions).
     * @param mapFunction a mapping {@link Function} that computes a scalar value of type {@code M} from a node of type {@code T}.
     * @param foldFunction a folding {@link Function} that computes the result of the fold of a node {@code n} given
     *        an instance of type {@code M} (which is the result of applying {@code mapFunction})
     *        and an iterable of instances of type {@code F} that are the result of folding the children of that node.
     *        The result of the folding function is also of type {@code F}.
     * @param <M> the type parameter of the result of the mapping function
     * @param <F> the type parameter of the result of the folding function
     * @return the fold of the tree rooted at {@code this} if it exists, {@code null} otherwise.
     */
    @Nullable
    default <M, F> F foldNullable(@Nonnull final Function<T, M> mapFunction,
                                  @Nonnull final BiFunction<M, Iterable<? extends F>, F> foldFunction) {
        final M mappedThis = mapFunction.apply(getThis());
        final List<F> foldedChildren = Lists.newArrayList();

        for (final T child : getChildren()) {
            foldedChildren.add(child.foldNullable(mapFunction, foldFunction));
        }

        return foldFunction.apply(mappedThis, foldedChildren);
    }

    /**
     * Method that maps the tree-like rooted at {@code this} to another tree-like using a fold operation that folds into
     * a {@code TreeLike<V>} if the fold exists.
     * This method is normally used when an immutable tree-like structure needs to be <em>modified</em> in a
     * persistent (copy-on-write) way meaning that parts or even all of the original tree rooted at {@code this}
     * needs to be recreated using the {@link #withChildren(Iterable)} method.
     * @param foldFunction a fold function that returns tree-like structures
     * @param <V> type parameter constraint to {@code TreeLike<V>}
     * @return an {@link Optional} of a new tree-like object that is result of the tree-map operation if the fold of the
     *         tree rooted at {@code this} exists, {@code Optional.empty()} otherwise
     */
    @Nonnull
    default <V extends TreeLike<V>> Optional<V> mapMaybe(@Nonnull final BiFunction<T, Iterable<? extends V>, V> foldFunction) {
        return Optional.ofNullable(foldNullable(Function.identity(), foldFunction));
    }

    /**
     * Method that maps the tree-like rooted at {@code this} to another tree-like of the same type. Unlike the other
     * methods providing generic tree folds and maps, this method provides an easy way to mutate the leaves of the
     * tree-like rooted at {@code this}, newly added leaves will not be passed again to the {@code replaceOperator}.
     * @param replaceOperator an operator that translates a tree-like of {@code T} into another tree-like of type
     *        {@code T}.
     * @return an {@link Optional} of a tree-like object that is the result of the tree-map operation if the fold of the
     *         tree rooted at {@code this} exists, {@code Optional.empty()} otherwise
     */
    @Nonnull
    default Optional<T> replaceLeavesMaybe(@Nonnull final UnaryOperator<T> replaceOperator) {
        return replaceLeavesMaybe(replaceOperator, false);
    }

    /**
     * Method that maps the tree-like rooted at {@code this} to another tree-like of the same type. Unlike the other
     * methods providing generic tree folds and maps, this method provides an easy way to mutate the leaves of the
     * tree-like rooted at {@code this}.
     * @param replaceOperator an operator that translates a tree-like of {@code T} into another tree-like of type
     *        {@code T}.
     * @param visitNewLeaves if {@code true}, passes new leaves to the {@code replaceOperator}, otherwise it will not.
     * @return an {@link Optional} of a tree-like object that is the result of the tree-map operation if the fold of the
     *         tree rooted at {@code this} exists, {@code Optional.empty()} otherwise
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nonnull
    default Optional<T> replaceLeavesMaybe(@Nonnull final UnaryOperator<T> replaceOperator, boolean visitNewLeaves) {
        if (visitNewLeaves) {
            return Optional.ofNullable(replace(node -> {
                if (Iterables.isEmpty(node.getChildren())) {
                    return replaceOperator.apply(node);
                }
                return node;
            }));
        } else {
            final Set<T> newLeaves = Sets.newIdentityHashSet();
            return Optional.ofNullable(replace(node -> {
                if (!newLeaves.contains(node) && Iterables.isEmpty(node.getChildren())) {
                    final var result = replaceOperator.apply(node);
                    if (result != null) {
                        result.preOrderStream().filter(n -> Iterables.isEmpty(n.getChildren())).forEach(newLeaves::add);
                    }
                    return result;
                }
                return node;
            }));
        }
    }

    /**
     * Replaces elements in the tree with new elements returned by the user-defined operator.
     * It traverses the tree in pre-order fashion, and is CoW, i.e. it is guaranteed to only make a copy of a node's
     * children iff one of the children is replaced by the replacement operator, otherwise, no allocation is made.
     * <br>
     * @param replacementOperator The replacement operator defined by the user, the operator is called on every node of
     *        the tree, and is expected to return the node itself if no changes to be made, this is
     *        necessary for replacement algorithm to avoid unnecessary allocations.
     * @return potentially a new tree which originates from {@code this} but having its nodes replaced with new ones,
     *         if no changes are made, {@code this} is returned.
     * <br>
     * Note: This can be thought of as a specialization of {@link TreeLike#mapMaybe(BiFunction)} where the mapping
     *       function domain is of the same type because that enables us to perform copy-on-write optimization.
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional for performance.
    @Nullable
    default T replace(@Nonnull final UnaryOperator<T> replacementOperator) {
        final var self = getThis();
        final var maybeReplaced = replacementOperator.apply(self);
        if (maybeReplaced == null) {
            return null;
        }
        if (Iterables.isEmpty(maybeReplaced.getChildren())) {
            return maybeReplaced;
        }
        final Iterable<? extends T> children = maybeReplaced.getChildren();
        int childrenCount = Iterables.size(children);
        boolean isAdding = false;
        // the build is initialized to null, so no allocation is made, it is lazily allocated iff a child is replaced.
        ImmutableList.Builder<T> replacedChildren = null;
        for (int i = 0; i < childrenCount; i++) {
            final var child = Iterables.get(children, i);
            final var maybeReplacedChild = child.replace(replacementOperator);
            if (maybeReplacedChild == null) {
                return null;
            }
            if (isAdding) {
                Verify.verifyNotNull(replacedChildren);
                replacedChildren.add(maybeReplacedChild);
            } else {
                if (maybeReplacedChild != child) {
                    // now that the replaced child is new, allocate the builder (and the tree).
                    replacedChildren = ImmutableList.builder();
                    for (int j = 0; j < i; j++) {
                        replacedChildren.add(Iterables.get(children, j));
                    }
                    replacedChildren.add(maybeReplacedChild);
                    isAdding = true;
                }
            }
        }
        return replacedChildren != null ? maybeReplaced.withChildren(replacedChildren.build()) : maybeReplaced;
    }

    /**
     * returns the height of the tree.
     * @return the height of the tree.
     */
    default int height() {
        return Streams.stream(getChildren()).mapToInt(TreeLike::height).max().orElse(0) + 1;
    }

    /**
     * Functional interface for a function whose result is not nullable.
     * @param <T> argument type
     * @param <R> result type
     */
    @FunctionalInterface
    interface NonnullFunction<T, R> {

        /**
         * Applies this function to the given argument.
         *
         * @param t the function argument
         * @return the function result
         */
        @Nonnull
        R apply(@Nonnull T t);
    }

    /**
     * Functional interface for a bi-function whose result is not nullable.
     * @param <T> argument type of the first argument to the function
     * @param <U> argument type of the second argument to the function
     * @param <R> result type
     */
    @FunctionalInterface
    interface NonnullBiFunction<T, U, R> {

        /**
         * Applies this function to the given arguments.
         *
         * @param t the first function argument
         * @param u the second function argument
         * @return the function result
         */
        @Nonnull
        R apply(@Nonnull T t, @Nonnull U u);
    }
}
