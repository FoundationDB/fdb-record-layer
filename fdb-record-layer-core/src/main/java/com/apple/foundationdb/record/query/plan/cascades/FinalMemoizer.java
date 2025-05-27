/*
 * FinalMemoizer.java
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

import com.apple.foundationdb.record.query.plan.cascades.Memoizer.ReferenceBuilder;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer.ReferenceOfPlansBuilder;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Interface to capture the parts of the memoizer API that only implementation rules are allowed to call.
 */
public interface FinalMemoizer {
    /**
     * Memoize a collection of final expressions that are also already members of the reference that is also passed in.
     * That reference must already have been explored which can usually statically reasoned about in the caller. The
     * reference that is returned is always newly created and never reused. As the expressions in the reference passed
     * in have already been explored, the new reference's final members are also all considered to be explored as well
     * and will not need to be re-explored.
     *
     * @param reference the source reference containing all passed-in expressions
     * @param expressions the expressions to memoize
     *
     * @return a new reference
     */
    @Nonnull
    Reference memoizeFinalExpressionsFromOther(@Nonnull Reference reference,
                                               @Nonnull Collection<? extends RelationalExpression> expressions);

    /**
     * Memoize the given final {@link RelationalExpression}. A new reference is created and returned to the caller.
     * The expression that is passed in must be a final expression of the current planner phase.
     * @param expression final expression to memoize
     * @return a new reference
     */
    @Nonnull
    Reference memoizeFinalExpression(@Nonnull RelationalExpression expression);

    /**
     * Memoize the given exploratory of final {@link RelationalExpression}. A new reference is created and returned to
     * the caller.
     * <br>
     * This method can only be called during {@link PlannerPhase#PLANNING} as the distinction between exploratory and
     * final expression is made by using the Java type for the expression that is passed in. If an expression is of
     * type {@link RecordQueryPlan}, the expression is considered a final expression, otherwise it is considered an
     * exploratory expression. Note that this method is only allowed to be called during {@link PlannerPhase#PLANNING}.
     * @param expression final expression to memoize
     * @return a new reference or a reused reference (only if the expression is considered to be exploartory)
     */
    @Nonnull
    Reference memoizeUnknownExpression(@Nonnull RelationalExpression expression);

    /**
     * Memoize a collection of plans that are also already members of the reference that is also passed in. That
     * reference must already have been explored which can usually statically reasoned about in the caller. The
     * reference that is returned is always newly created and never reused. As the expressions in the reference passed
     * in have already been explored, the new reference's final members are also all considered to be explored as well
     * and will not need to be re-explored. Note that this method is only allowed to be called during
     * {@link PlannerPhase#PLANNING}.
     *
     * @param reference the source reference containing all passed-in expressions
     * @param plans the plans to memoize
     *
     * @return a new reference
     */
    @Nonnull
    Reference memoizeMemberPlansFromOther(@Nonnull Reference reference,
                                          @Nonnull Collection<? extends RecordQueryPlan> plans);

    /**
     * Memoize the given plan. A new reference is created and returned to the caller. Note that this method is only
     * allowed to be called during {@link PlannerPhase#PLANNING}.
     * @param plan plan to memoize
     * @return a new reference
     */
    @Nonnull
    Reference memoizePlan(@Nonnull RecordQueryPlan plan);

    /**
     * Return a new {@link ReferenceBuilder} for final expressions. The expressions passed in are memoized when
     * the builder's {@link ReferenceBuilder#reference()} is called. The expressions are treated as final expressions
     * meaning that the reference that is eventually obtained is a new {@link Reference}.
     * @param expressions a collection of expressions to potentially memoize
     * @return a new {@link ReferenceBuilder}
     */
    @Nonnull
    ReferenceBuilder memoizeFinalExpressionsBuilder(@Nonnull Collection<? extends RelationalExpression> expressions);

    /**
     * Return a new {@link ReferenceOfPlansBuilder} for plans that are members of another given {@link Reference}. The
     * plans passed in are memoized when the builder's {@link ReferenceOfPlansBuilder#reference()} is called. The plans
     * are treated as final expressions meaning that the reference that is eventually obtained is a new
     * {@link Reference}. Just as in {@link #memoizeMemberPlansFromOther(Reference, Collection)}, the other given
     * reference must already have been explored. The reference that is returned is also immediately explored. Note that
     * this method is only allowed to be called during {@link PlannerPhase#PLANNING}.
     * @param plans a collection of plans to potentially memoize
     * @return a new {@link ReferenceOfPlansBuilder}
     */
    @Nonnull
    ReferenceOfPlansBuilder memoizeMemberPlansBuilder(@Nonnull Reference reference,
                                                      @Nonnull Collection<? extends RecordQueryPlan> plans);

    /**
     * Return a new {@link ReferenceOfPlansBuilder} for the given plan. The plan passed in is memoized when the
     * builder's {@link ReferenceOfPlansBuilder#reference()} is called. The plan is treated as final expressions
     * meaning that the reference that is eventually obtained is a new {@link Reference}. Note that this method is only
     * allowed to be called during {@link PlannerPhase#PLANNING}.
     * @param plan a plan to potentially memoize
     * @return a new {@link ReferenceOfPlansBuilder}
     */
    @Nonnull
    default ReferenceOfPlansBuilder memoizePlanBuilder(@Nonnull final RecordQueryPlan plan) {
        return memoizePlansBuilder(ImmutableList.of(plan));
    }

    /**
     * Return a new {@link ReferenceOfPlansBuilder} for the given plans. The plans passed in are memoized when the
     * builder's {@link ReferenceOfPlansBuilder#reference()} is called. The plans are treated as final expressions
     * meaning that the reference that is eventually obtained is a new {@link Reference}. Note that this method is only
     * allowed to be called during {@link PlannerPhase#PLANNING}.
     * @param plans a collection of plans to potentially memoize
     * @return a new {@link ReferenceOfPlansBuilder}
     */
    @Nonnull
    ReferenceOfPlansBuilder memoizePlansBuilder(@Nonnull Collection<? extends RecordQueryPlan> plans);
}
