/*
 * ExplorationCascadesRule.java
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

import javax.annotation.Nonnull;

/**
 * Abstract class to be extended for all exploration rules. The main purpose of this class is to constrain the
 * parameter of the {@link #onMatch(CascadesRuleCall)} method to {@link #onMatch(ExplorationCascadesRuleCall)} which
 * provides a restricted API for the specific rule implementation.
 * <br>
 * The main difference of an exploration rule as compared to an implementation rule (apart from how the planner
 * distinguishes between exploratory and final expressions) lies in the mechanics of how the expression DAG is modified
 * by the rule as a consequence of how the rule reasons about its subject and the subjects' inputs.
 * <br>
 * An exploration rule always matches some expression called the subject together with some sub DAG that is reachable
 * from the subject that always terminates at a set of references. Those references can be leaf references of the DAG
 * itself or just intermediate references of the DAG that just happened to be the furthest we matched with the
 * precondition matchers starting at the subject. Let's call those references base references. When the exploration
 * rule is executed, it creates new variants by a combination of memoizing and yielding exploratory expressions
 * that are solely based on those base references. That means that the base references of the rule execution are now
 * shared between different variations. That sharing is important for performance reasons (memory and computation) but
 * it requires extra care as those shared references must not be destructively modified which can be statically
 * guaranteed only during the exploration of the DAG.
 * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
 */
@API(API.Status.EXPERIMENTAL)
public interface ExplorationCascadesRule<T extends RelationalExpression> extends CascadesRule<T> {
    /**
     * Note that this method is intentionally {@code final} to prevent reimplementation by subclasses. Subclassed should
     * instead override the more constrained {@link #onMatch(ExplorationCascadesRuleCall)}.
     * @param call the regular {@link CascadesRuleCall}
     */
    @Override
    default void onMatch(@Nonnull final CascadesRuleCall call) {
        // needs to be cast up to select the right overloaded method
        onMatch((ExplorationCascadesRuleCall)call);
    }

    /**
     * Abstract method to be implemented by the specific rule.
     * @param call the constrained rule call
     */
    void onMatch(@Nonnull ExplorationCascadesRuleCall call);
}
