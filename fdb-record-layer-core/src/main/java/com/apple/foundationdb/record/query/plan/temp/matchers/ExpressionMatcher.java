/*
 * ExpressionMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A <code>ExpressionMatcher</code> is an expression that can be matched against a
 * {@link PlannerExpression} tree, while binding certain expressions/references in the tree to expression matcher objects.
 * The bindings can be retrieved from the rule call once the binding is matched.
 * Extreme care should be taken when implementing <code>ExpressionMatcher</code>, since it can be very delicate.
 * In particular, expression matchers may (or may not) be reused between successive rule calls and should be stateless.
 * Additionally, implementors of <code>ExpressionMatcher</code> must use the (default) reference equals.
 * @param <T> the bindable type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public interface ExpressionMatcher<T extends Bindable> {
    /**
     * Get a class or interface extending {@link PlannerExpression} that is a super class of every planner expression
     * that this matcher can match. Ideally, it should be the lowest such class but it may not be.
     * A planner will generally use this method to quickly determine a set of rules that could match an expression,
     * without considering each rule and trying to apply it. A good implementation of this method helps the planner
     * match rules efficiently.
     * @return a class object for a class that is a super class of every planner expression this matcher can match
     */
    @Nonnull
    Class<? extends PlannerExpression> getRootClass();

    /**
     * Return the child matchers of this matcher as a list.
     * @return a list of the child matchers of this matcher
     */
    @Nonnull
    List<ExpressionMatcher<? extends Bindable>> getChildren();

    /**
     * Determine whether this matcher matches the given expression.
     * @param expression a planner expression to test against
     * @return whether this matcher matches the expression, does not match the expression, or the match is impossible to determine
     */
    Result matches(@Nonnull Bindable expression);

    /**
     * Whether this matcher matches the expression, does not match the expression, or the match is impossible to determine.
     */
    enum Result {
        MATCHES,
        DOES_NOT_MATCH,
        UNKNOWN
    }
}
