/*
 * SimpleValueVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * An interface for implementors to compute a result over a tree of {@link Value}s, bottom-up.
 * <p>
 * This is the {@link Value} counterpart of
 * {@link com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor}: the generated
 * {@link ValueVisitor} only dispatches on the dynamic type of a single value and leaves recursion to its caller, whereas
 * this interface supplies the recursion and hands each value the results already computed for its children. Implementors
 * write {@link #evaluateAtValue(Value, List)} and get a fold rather than a walk.
 * </p>
 * <p>
 * {@code evaluateAtValue} is called in visit post-order of the depth-first traversal of the tree: a value is evaluated
 * only once all of its children have been. Because {@link ValueVisitorWithDefaults} routes every specific visitation
 * method to {@link #visitDefault(Value)}, an implementor that cares about only a few kinds of value can override those
 * specific methods and let everything else fold through the default.
 * </p>
 * <p>
 * Note the traversal is over {@link Value#getChildren()} and so is a plain tree walk. Unlike the expression-side
 * visitor, there is no {@code Reference} to hold several equivalent members and no {@code Quantifier} to hop through,
 * so there is nothing here corresponding to {@code evaluateAtRef} or {@code evaluateAtQuantifier}. A shared subtree is
 * visited once per path that reaches it; this interface does no memoization.
 * </p>
 *
 * @param <T> the type of the result this visitor computes
 */
@API(API.Status.EXPERIMENTAL)
public interface SimpleValueVisitor<T> extends ValueVisitorWithDefaults<T> {

    /**
     * Whether the given value should be visited at all.
     * <p>
     * Returning {@code false} prunes that value and the whole subtree beneath it. It is consulted in
     * {@link Value#acceptVisitor(SimpleValueVisitor)}, which returns {@code null} for a pruned value, and a pruned child
     * contributes no element to the {@code childResults} handed to its parent. That differs from the expression-side
     * visitor, where pruning a member of a reference discards the result for the entire reference -- a value's children
     * are positional operands rather than interchangeable members, so pruning one cannot stand for pruning its parent.
     * It does mean {@code childResults} is not positionally aligned with {@link Value#getChildren()} once anything is
     * pruned, so a visitor that relies on operand position should not prune.
     * </p>
     *
     * @param value the value that is about to be visited
     *
     * @return {@code true} if the value should be visited
     */
    @SuppressWarnings("unused")
    default boolean shouldVisit(@Nonnull Value value) {
        return true;
    }

    /**
     * Computes the result for the given value from the results computed for its children.
     *
     * @param value the value being visited
     * @param childResults the results computed for the children of {@code value}, in order, excluding any child that
     *        {@link #shouldVisit(Value)} rejected
     *
     * @return the result for {@code value}
     */
    @Nonnull
    T evaluateAtValue(@Nonnull Value value, @Nonnull List<T> childResults);

    @Nonnull
    @Override
    default T visitDefault(@Nonnull final Value value) {
        return evaluateAtValue(value, visitChildren(value));
    }

    /**
     * Visits the children of the given value, in order, and collects their results. Recursion goes through
     * {@link Value#acceptVisitor(SimpleValueVisitor)} so that {@link #shouldVisit(Value)} is honoured in exactly one
     * place; a child it prunes yields {@code null} there and is left out of the returned list.
     *
     * @param value the value whose children to visit
     *
     * @return the results computed for the children that were visited
     */
    @Nonnull
    default List<T> visitChildren(@Nonnull final Value value) {
        final var childResults = Lists.<T>newArrayList();
        for (final Value child : value.getChildren()) {
            final var childResult = child.acceptVisitor(this);
            if (childResult != null) {
                childResults.add(childResult);
            }
        }
        return childResults;
    }
}
