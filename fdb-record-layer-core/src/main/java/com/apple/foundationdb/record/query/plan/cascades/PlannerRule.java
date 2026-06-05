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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Basic rule interface.
 *
 * @param <C> the kind of subclass of {@link PlannerRuleCall} that {@link #onMatch(PlannerRuleCall)} is called on
 * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
 * @see com.apple.foundationdb.record.query.plan.cascades
 * @see PlannerRuleCall
 */
@API(API.Status.EXPERIMENTAL)
public interface PlannerRule<C extends PlannerRuleCall, T> {
    /**
     * Returns the set of concrete operator classes that this rule should be indexed under in a rule set. This is used
     * by the planner to quickly pick the subset of rules that could possibly fire on a visited node. If the set is
     * empty, the rule will go into the always-rules bucket. Otherwise, the rule is indexed under each class returned.
     * @return a (possibly empty) set of classes
     * @see PlanningRuleSet
     * @see BindingMatcher#getRootClasses()
     */
    @Nonnull
    Set<Class<?>> getRootOperators();

    void onMatch(@Nonnull C call);

    @Nonnull
    BindingMatcher<T> getMatcher();

    /**
     * Marker interface for rules that run in pre-order.
     */
    interface PreOrderRule {
    }
}
