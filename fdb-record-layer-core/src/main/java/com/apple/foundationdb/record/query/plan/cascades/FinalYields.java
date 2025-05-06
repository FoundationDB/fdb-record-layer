/*
 * FinalYields.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Interface to capture all yielding methods that an implementation rule is allowed to call.
 */
public interface FinalYields {
    /**
     * Yield a set of plans which by definition are final expressions and only allowed to be yielded during
     * {@link PlannerPhase#PLANNING}. The yielded plans will be inserted (if necessary) into the set of
     * final expressions of the current reference.
     * @param plans a set of {@link RecordQueryPlan}s
     */
    default void yieldPlans(@Nonnull Set<? extends RecordQueryPlan> plans) {
        for (final var plan : plans) {
            yieldPlan(plan);
        }
    }

    /**
     * Yield a plan which by definition is a final expressions and which is only allowed to be yielded during
     * {@link PlannerPhase#PLANNING}. The yielded plan will be inserted (if necessary) into the set of
     * final expressions of the current reference.
     * @param plan a {@link RecordQueryPlan}s
     */
    void yieldPlan(@Nonnull RecordQueryPlan plan);

    /**
     * Yield a set of final expressions. The yielded expressions will be inserted (if necessary) into the set of
     * final expressions of the current reference.
     * @param expressions a set of {@link RelationalExpression}s
     */
    default void yieldFinalExpressions(@Nonnull Set<? extends RelationalExpression> expressions) {
        for (final var expression : expressions) {
            yieldFinalExpression(expression);
        }
    }

    /**
     * Yield a final expression. The yielded expression will be inserted (if necessary) into the set of
     * final expressions of the current reference.
     * @param expression a set of {@link RelationalExpression}s
     */
    void yieldFinalExpression(@Nonnull RelationalExpression expression);

    /**
     * Yield a set of expressions which may consist of a mixture of exploratory and final expressions. This method can
     * only be called during {@link PlannerPhase#PLANNING} as the distinction between exploratory and final expression
     * is made by using the Java type for each expression in the set of expressions that is passed in. If an expression
     * is of type {@link RecordQueryPlan}, the expression is considered a final expression, otherwise it is considered
     * an exploratory expression. The yielded expressions will be inserted (if necessary) either into the set of
     * exploratory expressions or into the set of final expressions of the current reference.
     * @param expressions a set of {@link RelationalExpression}s
     */
    default void yieldPlannedExpressions(@Nonnull Set<? extends RelationalExpression> expressions) {
        for (final var expression : expressions) {
            yieldPlannedExpression(expression);
        }
    }

    /**
     * Yield an expression which may be an exploratory or a final expression. This method can only be called during
     * {@link PlannerPhase#PLANNING} as the distinction between exploratory and final expression is made by using the
     * Java type for the expression that is passed in. If an expression is of type {@link RecordQueryPlan}, the
     * expression is considered a final expression, otherwise it is considered an exploratory expression. The yielded
     * expressions will be inserted (if necessary) either into the set of exploratory expressions or into the set of
     * final expressions of the current reference.
     * @param expression a set of {@link RelationalExpression}s
     */
    void yieldPlannedExpression(@Nonnull RelationalExpression expression);
}
