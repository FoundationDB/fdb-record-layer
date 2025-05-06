/*
 * ExplorationCascadesRuleCall.java
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

import javax.annotation.Nonnull;

/**
 * Interface to capture all methods that only exploration rules are allowed to call.
 */
@API(API.Status.EXPERIMENTAL)
public interface ExplorationCascadesRuleCall extends PlannerRuleCall<RelationalExpression>, CommonCascadesRuleCall, ExploratoryYields, ExploratoryMemoizer {
    /**
     * Yield an exploratory expression. This method is the default yield method as define in {@link PlannerRuleCall}.
     * As overriding this method cannot be prevented, all implementors should call
     * {@link #yieldExploratoryExpression(RelationalExpression)} to ensure proper semantics.
     * @param expression the expression to be yielded.
     */
    @Override
    default void yieldResult(@Nonnull final RelationalExpression expression) {
        yieldExploratoryExpression(expression);
    }

}
