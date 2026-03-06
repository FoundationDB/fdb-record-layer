/*
 * PickLeftTieBreaker.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * {@link Tiebreaker} implementation that always picks the right most one. This is intended to be the
 * tiebreaker of last resort. That is, if all other tiebreakers are unable to distinguish between two
 * elements, including the hash of the objects (see {@link RewritingCostModel#semanticHashTiebreaker()} and
 * {@link PlanningCostModel#planHashTiebreaker()}), then this tiebreaker should be invoked.
 */
final class PickRightTiebreaker implements Tiebreaker<RelationalExpression> {
    private static final PickRightTiebreaker INSTANCE = new PickRightTiebreaker();

    private PickRightTiebreaker() {
    }

    @Override
    public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                       @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                       @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                       @Nonnull final RelationalExpression a, @Nonnull final RelationalExpression b) {
        return 1;
    }

    @Nonnull
    static PickRightTiebreaker pickRightTiebreaker() {
        return INSTANCE;
    }
}
