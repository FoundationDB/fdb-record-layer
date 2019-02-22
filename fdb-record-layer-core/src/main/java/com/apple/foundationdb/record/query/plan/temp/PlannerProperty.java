/*
 * PlannerProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.List;

/**
 * An interface for certain Cascades-style properties, which are measurable features of an expression other than the
 * structure of the expression tree. In particular, a {@code PlannerProperty} is a property that depends on (much of)
 * the contents of the subtree rooted at the expression on which it is evaluated, rather than just a finite depth set
 * of paths as a {@link com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher} would. For example,
 * the sort order and set of record types produced by a
 * {@link com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression} could be a
 * {@code PlannerProperty}.
 *
 * <p>
 * To avoid littering {@link PlannerExpression} classes with methods for various properties, properties are implemented
 * using a variant of the hierarchical visitor pattern the tree of {@code PlannerExpression}s and {@link ExpressionRef}s.
 * A property can be evaluated against an expression tree by having the visitor traverse the tree. Note that the
 * "{@code visitLeave()}" methods {@link #evaluateAtExpression} and {@link #evaluateAtRef} are handed the results of the
 * visitor evaluated at their children and members respectively. Since most properties are easy to describe as a
 * recursion with depth one, this makes properties easier to read and write.
 * </p>
 *
 * @param <T> the result type of the property
 */
@API(API.Status.EXPERIMENTAL)
public interface PlannerProperty<T> {
    /**
     * Return whether the property should visit the subtree rooted at the given expression.
     * Called on nodes in the expression tree in visit pre-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the first time, {@code shouldVisit()} is called on that node.
     * If {@code shouldVisit()} returns {@code false}, then {@link #evaluateAtExpression(PlannerExpression, List)} will
     * not be called on the given expression.
     * @param expression the planner expression to visit
     * @return {@code true} if the children of {@code expression} should be visited and {@code false} if they should not be visited
     */
    boolean shouldVisit(@Nonnull PlannerExpression expression);

    /**
     * Return whether the property should visit the subtree rooted at the given expression.
     * Called on nodes in the expression tree in visit pre-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the first time, {@code shouldVisit()} is called on that node.
     * If {@code shouldVisit()} returns {@code false}, then {@link #evaluateAtRef(ExpressionRef, List)} will
     * not be called on the given expression.
     * @param ref the expression reference to visit
     * @return {@code true} if the members of {@code ref} should be visited and {@code false} if they should not be visited
     */
    boolean shouldVisit(@Nonnull ExpressionRef<? extends PlannerExpression> ref);

    /**
     * Evaluate the property at the given expression, using the results of evaluating the property at its children.
     * Called on nodes in the expression tree in visit post-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the last time (after all of its children have been visited, if applicable),
     * {@code evaluateAtExpression()} is called on that node.
     * @param expression the cursor to visit
     * @param childResults the results of the property evaluated at the children of {@code expression}
     * @return the value of property at the given expression
     */
    @Nonnull
    T evaluateAtExpression(@Nonnull PlannerExpression expression, @Nonnull List<T> childResults);

    /**
     * Evaluate the property at the given reference, using the results of evaluating the property at its members.
     * Called on nodes in the expression tree in visit post-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the last time (after all of its children have been visited, if applicable),
     * {@code evaluateAtRef()} is called on that node.
     * @param ref the expression reference to visit
     * @param memberResults the results of the property evaluated at the members of {@code ref}
     * @return the value of property at the given reference
     */
    @Nonnull
    T evaluateAtRef(@Nonnull ExpressionRef<? extends PlannerExpression> ref, @Nonnull List<T> memberResults);
}
