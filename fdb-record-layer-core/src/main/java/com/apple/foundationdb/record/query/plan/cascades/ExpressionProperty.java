/*
 * ExpressionProperty.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * An interface for certain Cascades-style properties, which are measurable features of an expression other than the
 * structure of the expression tree. In particular, a {@code ExpressionProperty} is a property that depends on (much of)
 * the contents of the subtree rooted at the expression on which it is evaluated, rather than just a finite depth set
 * of paths as a {@link BindingMatcher} would. For example,
 * the sort order and set of record types produced by a
 * {@link RelationalExpression} could be a
 * {@code ExpressionProperty}.
 *
 * <p>
 * To avoid littering {@link RelationalExpression} classes with methods for various properties, properties are implemented
 * using a variant of the hierarchical visitor pattern over a DAG of {@link RelationalExpression}s, {@link Quantifier}s,
 * and {@link Reference}s.
 * A property can be evaluated against an expression tree by having the visitor traverse a DAG of heterogeneous objects
 * where expressions are said to own quantifiers which range over expression references which then contain expressions
 * again. Shared subgraphs are visited multiple times. If desired, the caller must ensure that a subgraph is not
 * visited more than once.
 * </p>
 *
 * <p>
 * Note that the methods {@link #evaluateAtExpression}, {@link #evaluateAtQuantifier}, and {@link #evaluateAtRef} are handed the
 * results of the visitor evaluated at their owned quantifiers, references, and members respectively. Since most properties
 * are easy to describe as a recursion with depth one, this makes properties easier to read and write.
 * </p>
 *
 * <p>
 * Note that the default implementations provided in this interface walk the entire DAG, i.e. all {@link #shouldVisit}
 * methods return {@code true}. That implies that properties have been computed for sub-structures of the graph when
 * {@code evaluateAtXXX()} is called and therefore are not {@code null}. That means that all such arguments can be
 * assumed to be non-nullable even though parameters are annotated as {@link Nullable}. On the other hand, if the
 * implementor also overrides {@code shouldVisit} to return {@code false}, arguments to {@code evaluateAtXXX} may become
 * nullable or contain {@code null}s if they are collections and should be dealt with gracefully.
 * </p>
 *
 * @param <T> the result type of the property
 */
@API(API.Status.EXPERIMENTAL)
public interface ExpressionProperty<T> extends RelationalExpressionVisitor<T> {
    /**
     * Return whether the property should visit the subgraph rooted at the given expression.
     * Called on nodes in the expression graph in visit pre-order of the depth-first traversal of the graph.
     * That is, as each node is visited for the first time, {@code shouldVisit()} is called on that node.
     * If {@code shouldVisit()} returns {@code false}, then {@link #evaluateAtExpression(RelationalExpression, List)} will
     * not be called on the given expression.
     * @param expression the planner expression to visit
     * @return {@code true} if the children of {@code expression} should be visited and {@code false} if they should not be visited
     */
    @SuppressWarnings("unused")
    default boolean shouldVisit(@Nonnull RelationalExpression expression) {
        return true;
    }

    /**
     * Return whether the property should visit the given reference and, transitively, the members of the reference.
     * Called on expression references in the graph in visit pre-order of the depth-first traversal of the graph.
     * That is, as a reference is visited, {@code shouldVisit()} is called on that reference.
     * If {@code shouldVisit()} returns {@code false}, then {@link #evaluateAtRef(Reference, List)} will
     * not be called on the given reference.
     * @param ref the expression reference to visit
     * @return {@code true} if the members of {@code ref} should be visited and {@code false} if they should not be visited
     */
    @SuppressWarnings("unused")
    default boolean shouldVisit(@Nonnull Reference ref) {
        return true;
    }

    /**
     * Return whether the property should visit the given quantifier and the references that the quantifier
     * ranges over.
     * Called on quantifiers in the graph in visit pre-order of the depth-first traversal of the graph.
     * That is, as a quantifier is visited, {@code shouldVisit()} is called on that quantifier.
     * If {@code shouldVisit()} returns {@code false}, then {@link #evaluateAtQuantifier(Quantifier, Object)} will
     * not be called on the given quantifier.
     * @param quantifier the quantifier to visit
     * @return {@code true} if the expression reference {@code quantifier} ranges over should be visited and
     *         {@code false} if it should not be visited
     */
    @SuppressWarnings("unused")
    default boolean shouldVisit(@Nonnull Quantifier quantifier) {
        return true;
    }

    /**
     * Evaluate the property at the given expression, using the results of evaluating the property at its children.
     * Called on nodes in the graph in visit post-order of the depth-first traversal of the graph.
     * That is, as each expression is visited (after all of its children have been visited, if applicable),
     * {@code evaluateAtExpression()} is called on that expression.
     * @param expression the cursor to visit
     * @param childResults the results of the property evaluated at the children of {@code expression}
     * @return the value of property at the given expression
     */
    @Nonnull
    T evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<T> childResults);

    /**
     * Evaluate the property at the given reference, using the results of evaluating the property at its members.
     * Called on nodes in the graph in visit post-order of the depth-first traversal of the graph.
     * That is, as each reference is visited (after all of its members have been visited, if applicable),
     * {@code evaluateAtRef()} is called on that reference.
     * @param ref the expression reference to visit
     * @param memberResults the results of the property evaluated at the members of {@code ref}
     * @return the value of property at the given reference
     */
    @Nonnull
    T evaluateAtRef(@Nonnull Reference ref, @Nonnull List<T> memberResults);

    /**
     * Evaluate the property at the given quantifier, using the result of evaluating the property at the
     * reference the quantifier ranges over.
     * Called on quantifiers in the graph in visit post-order of the depth-first traversal of the graph.
     * That is, as each quantifier is visited (after the expression reference it ranges over has been visited, if applicable),
     * {@code evaluateAtQuantifier()} is called on that quantifier.
     * @param quantifier the quantifier to visit
     * @param rangesOverResult the result of the property evaluated at the {@link Reference} {@code quantifier}
     *        ranges over
     * @return the value of property at the given quantifier
     */
    @Nonnull
    @SuppressWarnings("unused")
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    default T evaluateAtQuantifier(@Nonnull final Quantifier quantifier, @Nullable final T rangesOverResult) {
        // since we visit the expression reference under the quantifier, we can insist that rangesOverResult is never null
        return Objects.requireNonNull(rangesOverResult);
    }

    @Nonnull
    @Override
    default T visitDefault(@Nonnull final RelationalExpression relationalExpression) {
        final var quantifierResults = visitQuantifiers(relationalExpression);
        return evaluateAtExpression(relationalExpression, quantifierResults);
    }

    @Nonnull
    default List<T> visitQuantifiers(@Nonnull final RelationalExpression relationalExpression) {
        final List<? extends Quantifier> quantifiers = relationalExpression.getQuantifiers();
        final var quantifierResults = Lists.<T>newArrayListWithCapacity(quantifiers.size());
        for (final Quantifier quantifier : quantifiers) {
            quantifierResults.add(quantifier.acceptPropertyVisitor(this));
        }

        return quantifierResults;
    }
}
