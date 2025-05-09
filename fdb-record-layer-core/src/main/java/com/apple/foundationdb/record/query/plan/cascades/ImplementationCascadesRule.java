/*
 * ImplementationCascadesRule.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Abstract class to be extended for all implementation rules. The main purpose of this class is to constrain the
 * parameter of the {@link #onMatch(CascadesRuleCall)} method to {@link #onMatch(ImplementationCascadesRuleCall)} which
 * provides a restricted API for the specific rule implementation.
 * <br>
 * The main difference of an implementation rule as compared to an explorations rule (apart from how the planner
 * distinguishes between exploratory and final expressions) lies in the mechanics of how the expression DAG is modified
 * by the rule as a consequence of how the rule reasons about its subject and the subjects' inputs.
 * <br>
 * An implementation rule always matches some expression called the subject together with some sub DAG that is reachable
 * from the subject which always terminates at a collection of expression partitions. An expression partition consists
 * of a subset of a reference's final expression members. When the implementation rule is executed, it creates new
 * variants by a combination of memoizing and yielding final expressions that are solely based on those expression
 * partitions (unlike during the execution of exploration rules where entire references are reused). In order to utilize
 * the plans or (final) expressions of a plan partition, an implementation rule usually calls
 * {@link FinalMemoizer#memoizeFinalExpressionsFromOther(Reference, Collection)} or
 * {@link FinalMemoizer#memoizeMemberPlansFromOther(Reference, Collection)} to create a new reference (the memoizer
 * guarantees to create a new reference) of a reference only containing the plans of the expression partition.
 * <br>
 * An expression partition can only contain final expressions and a final expression's children references can only
 * contain exactly one final expression each (which is the pruned expression). Those pruned expressions themselves where
 * yielded (before pruning) by some other execution of some implementation rule prior to the execution of this
 * implementation rule. Thus, it can inductively be shown that implementation rules construct a DAG bottom up that never
 * shares references among different variations. We also say that we <em>disentangle</em> the expression DAG underneath
 * the current group from the rest of the expression DAG. That property of the subgraph of being disentangled is
 * extremely important as the planner prunes the children of the yielded expression immediately after the rule is called
 * which constitutes as a destructive modification of the pruned reference.
 * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
 */
@API(API.Status.EXPERIMENTAL)
public abstract class ImplementationCascadesRule<T extends RelationalExpression> extends CascadesRule<T> {
    public ImplementationCascadesRule(@Nonnull BindingMatcher<T> matcher) {
        this(matcher, ImmutableSet.of());
    }

    public ImplementationCascadesRule(@Nonnull final BindingMatcher<T> matcher,
                                      @Nonnull final Collection<PlannerConstraint<?>> requirementDependencies) {
        super(matcher, requirementDependencies);
    }

    /**
     * Note that this method is intentionally final to prevent reimplementation by subclasses. Subclassed should instead
     * override the more constrained {@link #onMatch(ImplementationCascadesRuleCall)}.
     * @param call the regular {@link CascadesRuleCall}
     */
    @Override
    public final void onMatch(@Nonnull final CascadesRuleCall call) {
        // needs to be cast up to select the right overloaded method
        onMatch((ImplementationCascadesRuleCall)call);
    }

    /**
     * Abstract method to be implemented by the specific rule.
     * @param call the constrained rule call
     */
    public abstract void onMatch(@Nonnull ImplementationCascadesRuleCall call);
}
