/*
 * Memoizer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

/**
 * An interface for memoizing {@link Reference}s and their member {@link RelationalExpression}s. The methods declared in
 * this interface mostly have one thing in common. They expect among their parameters an expression or a collection of
 * expressions which are to be memoized and return a {@link Reference} which may be a new reference that was just
 * created or an already existing reference that was previously memoized by this {@code Memoizer} and that was deemed
 * to be compatible to be reused.
 * <br>
 * There are numerous considerations that determine if a reference can be safely reused. Most of these considerations
 * can be derived from the individual use case and the context the method is called from. Each individual method
 * declaration in this interface will also indicate (via java doc) if the method can return a reused expression or
 * if the caller can always expect a fresh reference to be returned. Note that the terminology used here is that
 * a <em>memoized expression</em> indicates that the memoization structures of the planner are aware of this expression.
 * A reference (not an expression) can be reused as an effect of memoization of the given expressions (depending on
 * use case and context).
 */
@API(API.Status.EXPERIMENTAL)
public interface Memoizer {
    /**
     * Memoize the given {@link RelationalExpression}. If a previously memoized expression is found that is semantically
     * equivalent to the expression that is passed in, the reference containing the previously memoized expression is
     * returned allowing the planner to reuse that reference. If no such previously memoized expression is found, a new
     * reference is created and returned to the caller.
     * <br>
     * The expression that is passed in must be an exploratory expression of the current planner phase. If this method
     * is used in an incompatible context and a reused reference is returned, the effects of mutating such a reference
     * through other planner logic are undefined and should be avoided.
     * @param expression expression to memoize
     * @return a new or a reused reference
     */
    @Nonnull
    Reference memoizeExploratoryExpression(@Nonnull RelationalExpression expression);

    /**
     * Memoize a collection of final expressions that are also already members of the reference that is also passed in.
     * That reference must already have been explored which can usually statically reasoned about in the caller. The
     * reference that is returned is always newly created and never reused. The new reference's final members are
     * all considered to be explored as well.
     * @param reference the source reference containing all passed-in expressions
     * @param expressions the expressions to memoize
     * @return a new reference
     */
    @Nonnull
    Reference memoizeMemberExpressions(@Nonnull Reference reference,
                                       @Nonnull Collection<? extends RelationalExpression> expressions);

    @Nonnull
    Reference memoizeFinalExpression(@Nonnull RelationalExpression expression);

    @Nonnull
    Reference memoizePlannedExpression(@Nonnull RelationalExpression expression);

    @Nonnull
    Reference memoizeMemberPlans(@Nonnull Reference reference,
                                 @Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    Reference memoizePlan(@Nonnull RecordQueryPlan plan);

    @Nonnull
    ReferenceBuilder memoizeExploratoryExpressionBuilder(@Nonnull RelationalExpression expression);

    @Nonnull
    ReferenceBuilder memoizeFinalExpressionsBuilder(@Nonnull Collection<? extends RelationalExpression> expressions);

    @Nonnull
    ReferenceOfPlansBuilder memoizeMemberPlansBuilder(@Nonnull Reference reference,
                                                      @Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    ReferenceOfPlansBuilder memoizePlanBuilder(@Nonnull RecordQueryPlan plan);

    @Nonnull
    ReferenceOfPlansBuilder memoizePlansBuilder(@Nonnull Collection<? extends RecordQueryPlan> recordQueryPlans);

    /**
     * Builder for references.
     */
    interface ReferenceBuilder {
        @Nonnull
        Reference reference();

        @Nonnull
        Set<? extends RelationalExpression> members();
    }

    /**
     * Builder for references.
     */
    interface ReferenceOfPlansBuilder extends ReferenceBuilder {
        @Nonnull
        @Override
        Set<? extends RecordQueryPlan> members();
    }
}
