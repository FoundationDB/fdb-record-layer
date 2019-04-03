/*
 * PlannerRule.java
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
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Classes that inherit from <code>PlannerRule</code> form the base of the Cascades planning system. During the planning
 * process, rules are matched to a current {@link PlannerExpression} and then provide zero or more logically equivalent
 * expressions.
 *
 * The rule matching occurs in two stages: first, the planner examines the {@link #matcher} of the rule,
 * which is a {@link TypeMatcher} expression that expresses the operators that the rule applies to and the hierarchical
 * structure that they take on in the expression tree. If the rule matches the binding, its {@link #onMatch(PlannerRuleCall)}
 * method is called, with a parameter that provides the planner's {@link PlanContext} and access to the map of bindings,
 * among other things. This method can inspect the operators bound during the structural matching and implement other
 * logic.
 *
 * The <code>onMatch()</code> method returns logically equivalent expressions to the planner by calling the
 * {@link PlannerRuleCall#yield(ExpressionRef)} method on its rule call, with a new
 * {@link MutableExpressionRef}. The <code>yield()</code> method can be called more than once, or zero times if no
 * alternative expressions are found.
 *
 * A rule should not attempt to modify any of the bound objects that the rule call provides. Nearly all such objects are
 * immutable, and the mutable ones are hidden behind interfaces that do not expose mutation methods. In particular,
 * a rule should never cast an {@link ExpressionRef} to {@link MutableExpressionRef}, in an attempt to access it.
 *
 * A <code>PlannerRule</code> should not store state between successive calls to {@link #onMatch(PlannerRuleCall)},
 * since it may be reused an arbitrary number of times and may be reinstantiated at any time. To simplify cleanup,
 * any state that needs to be shared within a rule call should be encapsulated in a helper object (such as a static
 * inner class) or documented carefully.
 *
 * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
 * @see com.apple.foundationdb.record.query.plan.temp
 * @see PlannerRuleCall
 */
@API(API.Status.EXPERIMENTAL)
public abstract class PlannerRule<T extends PlannerExpression> {
    @Nonnull
    private final ExpressionMatcher<T> matcher;

    /**
     * Returns the class of the operator at the root of the binding expression, if this rule uses a non-trivial binding.
     * Used primarily for indexing rules for more efficient rule search.
     * @return the class of the root of this rule's binding, or <code>Optional.empty()</code> if the rule matches anything
     * @see PlannerRuleSet
     */
    public Optional<Class<? extends PlannerExpression>> getRootOperator() {
        return Optional.of(matcher.getRootClass());
    }

    public PlannerRule(@Nonnull ExpressionMatcher<T> matcher) {
        this.matcher = matcher;
    }

    @Nonnull
    public abstract ChangesMade onMatch(@Nonnull PlannerRuleCall call);

    @Nonnull
    public ExpressionMatcher<T> getMatcher() {
        return matcher;
    }

    /**
     * An enum to describe the results of the rule's {@link #onMatch(PlannerRuleCall)} method. {@code ChangesMade} is
     * an enum rather than a boolean so that we can extend the degree to which rules report what they did to the planner
     * in the future without needing to update too many rules.
     */
    public enum ChangesMade {
        NO_CHANGE,
        MADE_CHANGES
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
