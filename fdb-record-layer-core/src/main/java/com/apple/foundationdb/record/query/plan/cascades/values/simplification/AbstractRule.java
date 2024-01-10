/*
 * AbstractValueRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * The rule matching occurs in two stages: first, the planner examines the {@link #matcher} of the rule,
 * which is a {@link BindingMatcher} expression that expresses the operators that the rule applies to and the hierarchical
 * structure that they take on in the expression tree. If the rule matches the binding, its {@link #onMatch(PlannerRuleCall)}
 * method is called, with a parameter that provides the planner's {@link PlanContext} and access to the map of bindings,
 * among other things. This method can inspect the operators bound during the structural matching and implement other
 * logic.
 * <br>
 * The <code>onMatch()</code> method returns logically equivalent expressions to the planner by calling the
 * {@link PlannerRuleCall#yieldExpression(Object)} method on its rule call, with a new
 * {@link ExpressionRef}. The <code>yield()</code> method can be called more than once, or zero times if no
 * alternative expressions are found.
 * <br>
 * A rule should not attempt to modify any of the bound objects that the rule call provides. Nearly all such objects are
 * immutable, and the mutable ones are hidden behind interfaces that do not expose mutation methods. In particular,
 * a rule should never cast an {@link ExpressionRef} in an attempt to access it.
 * <br>
 * A rule should not store state between successive calls to {@link #onMatch(PlannerRuleCall)},
 * since it may be reused an arbitrary number of times and may be re-instantiated at any time. To simplify cleanup,
 * any state that needs to be shared within a rule call should be encapsulated in a helper object (such as a static
 * inner class) or documented carefully.
 *
 * @param <RESULT> the type of the result being yielded by rule implementations
 * @param <CALL> the type of rule call that is used in calls to {@link #onMatch(PlannerRuleCall)} )}
 * @param <BASE> the type of entity the rule matches
 * @param <TYPE> a type of specific subtype that this rule matches
 * @see com.apple.foundationdb.record.query.plan.cascades
 * @see PlannerRuleCall
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AbstractRule<RESULT, CALL extends AbstractRuleCall<RESULT, CALL, BASE>, BASE, TYPE extends BASE> implements PlannerRule<RESULT, CALL, TYPE> {
    @Nonnull
    private final BindingMatcher<TYPE> matcher;

    public AbstractRule(@Nonnull BindingMatcher<TYPE> matcher) {
        this.matcher = matcher;
    }

    /**
     * Returns the class of the operator at the root of the binding expression, if this rule uses a non-trivial binding.
     * Used primarily for indexing rules for more efficient rule search.
     * @return the class of the root of this rule's binding, or <code>Optional.empty()</code> if the rule matches anything
     * @see PlannerRuleSet
     */
    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(matcher.getRootClass());
    }

    @Nonnull
    @Override
    public BindingMatcher<TYPE> getMatcher() {
        return matcher;
    }

    @Nonnull
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
