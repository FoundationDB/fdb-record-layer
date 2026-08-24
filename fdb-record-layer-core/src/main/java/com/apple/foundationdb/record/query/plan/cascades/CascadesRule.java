/*
 * AbstractCascadesRule.java
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

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * Root interface implemented by all Cascades planner rules.
 *
 * <p>Besides the basic {@link PlannerRule} contract, a {@code CascadesRule} declares the {@link PlannerConstraint} set
 * it depends on via {@link #getConstraintDependencies()}.
 *
 * @param <T> A parent planner expression type of all possible root planner expressions that this rule could match.
 */
@API(API.Status.EXPERIMENTAL)
public interface CascadesRule<T> extends PlannerRule<CascadesRuleCall, T> {
    @Nonnull
    Set<PlannerConstraint<?>> getConstraintDependencies();

    default boolean onlyOnPrunedInputs() {
        return false;
    }

    // This is a temporary workaround to force rules that run on `final` expressions to fire on expressions
    // with "pruned" children. We want to make this the default behavior, however currently, the optimization
    // that yields COVERING plan requires all final expressions in child groups. Hence, we currently mark
    // Rewriter phase implementation rules that run on final expression with `OnPrunedInputRule` marker.
    interface OnPrunedInputsRule<T> extends CascadesRule<T> {
        @Override
        default boolean onlyOnPrunedInputs() {
            return true;
        }
    }
}
