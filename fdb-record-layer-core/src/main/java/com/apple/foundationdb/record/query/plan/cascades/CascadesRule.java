/*
 * CascadesRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Classes that inherit from <code>CascadesRule</code> form the base of the Cascades planning system. During the planning
 * process, rules are matched to a current {@link RelationalExpression} and then provide zero or more logically equivalent
 * expressions.
 *
 * The rule matching occurs in two stages: first, the planner examines the {@link #matcher} of the rule,
 * which is a {@link BindingMatcher} expression that expresses the operators that the rule applies to and the hierarchical
 * structure that they take on in the expression tree. If the rule matches the binding, its {@link #onMatch(CascadesRuleCall)}
 * method is called, with a parameter that provides the planner's {@link PlanContext} and access to the map of bindings,
 * among other things. This method can inspect the operators bound during the structural matching and implement other
 * logic.
 *
 * The <code>onMatch()</code> method returns logically equivalent expressions to the planner by calling the
 * {@link PlannerRuleCall#yieldResult(Object)} method on its rule call, with a new
 * {@link Reference}. The <code>yield()</code> method can be called more than once, or zero times if no
 * alternative expressions are found.
 *
 * A rule should not attempt to modify any of the bound objects that the rule call provides. Nearly all such objects are
 * immutable, and the mutable ones are hidden behind interfaces that do not expose mutation methods. In particular,
 * a rule should never cast an {@link Reference} in an attempt to access it.
 *
 * A <code>CascadesRule</code> should not store state between successive calls to {@link #onMatch(CascadesRuleCall)},
 * since it may be reused an arbitrary number of times and may be reinstantiated at any time. To simplify cleanup,
 * any state that needs to be shared within a rule call should be encapsulated in a helper object (such as a static
 * inner class) or documented carefully.
 *
 * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
 * @see com.apple.foundationdb.record.query.plan.cascades
 * @see PlannerRuleCall
 */
@API(API.Status.EXPERIMENTAL)
public abstract class CascadesRule<T> implements PlannerRule<Reference, CascadesRuleCall, T> {
    @Nonnull
    private final BindingMatcher<T> matcher;

    @Nonnull
    private final Set<PlannerConstraint<?>> requirementDependencies;

    public CascadesRule(@Nonnull BindingMatcher<T> matcher) {
        this.matcher = matcher;
        this.requirementDependencies = ImmutableSet.of();
    }

    public CascadesRule(@Nonnull BindingMatcher<T> matcher, Collection<PlannerConstraint<?>> requirementDependencies) {
        this.matcher = matcher;
        this.requirementDependencies = ImmutableSet.copyOf(requirementDependencies);
    }

    /**
     * Returns the class of the operator at the root of the binding expression, if this rule uses a non-trivial binding.
     * Used primarily for indexing rules for more efficient rule search.
     * @return the class of the root of this rule's binding, or <code>Optional.empty()</code> if the rule matches anything
     * @see PlanningRuleSet
     */
    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(matcher.getRootClass());
    }

    @Nonnull
    public Set<PlannerConstraint<?>> getConstraintDependencies() {
        return requirementDependencies;
    }

    @Override
    public abstract void onMatch(@Nonnull CascadesRuleCall call);

    @Nonnull
    @Override
    public BindingMatcher<T> getMatcher() {
        return matcher;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
