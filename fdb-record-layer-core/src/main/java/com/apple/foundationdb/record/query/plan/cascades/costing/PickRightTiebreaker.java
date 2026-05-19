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
 * {@link Tiebreaker} implementation that always picks the left most one. This is intended to be the
 * tiebreaker of last resort. That is, if all other tiebreakers are unable to distinguish between two
 * elements, including the hash of the objects (see {@link RewritingCostModel#semanticHashTiebreaker()} and
 * {@link PlanningCostModel#planHashTiebreaker()}), then this tiebreaker should be invoked.
 *
 * <p>
 * In cases where there are otherwise multiple "best" plans, the idea with this tiebreaker is that it
 * should select the first element from an ordered collection that is in the best equivalency
 * class. To make this work, whenever accumulating values from a list, we should always pass in new
 * expressions as the left hand side ({@code a}), while the accumulator's value should be passed
 * in on the right hand side ({@code a}). It would probably be more natural to swap the comparand
 * order and have this elect to return the left hand element ({@code a}), but there are some {@link Tiebreaker}
 * implementations that are not antisymmetric. As such, swapping the order would currently result
 * in plan changes. See: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/3998">Issue #3998</a>.
 * </p>
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
