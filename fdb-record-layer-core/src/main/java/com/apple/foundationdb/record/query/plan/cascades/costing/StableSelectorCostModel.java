/*
 * StableSelectorCostModel.java
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.FindExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A comparator implementing a simple cost model for the {@link CascadesPlanner} to choose the plan with the smallest
 * plan hash.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class StableSelectorCostModel implements CascadesCostModel<RecordQueryPlan>, Comparator<RecordQueryPlan> {
    @Nonnull
    private static final Set<Class<? extends RelationalExpression>> interestingExpressionClasses =
            ImmutableSet.of();

    @Nonnull
    private static final Tiebreaker<RecordQueryPlan> tiebreaker =
            Tiebreaker.combineTiebreakers(ImmutableList.of(
                    PlanningCostModel.planHashTiebreaker(),
                    PickRightTiebreaker.pickRightTiebreaker()));

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return RecordQueryPlannerConfiguration.defaultPlannerConfiguration();
    }

    @Nonnull
    @Override
    public Optional<RecordQueryPlan> getBestExpression(@Nonnull final Set<? extends RelationalExpression> expressions, @Nonnull final Consumer<RecordQueryPlan> onRemoveConsumer) {
        return costExpressions(expressions, onRemoveConsumer).getOnlyExpressionMaybe();
    }

    @Nonnull
    private TiebreakerResult<RecordQueryPlan> costExpressions(@Nonnull final Set<? extends RelationalExpression> expressions,
                                                              @Nonnull final Consumer<RecordQueryPlan> onRemoveConsumer) {
        return Tiebreaker.ofContext(getConfiguration(), interestingExpressionClasses, expressions, RecordQueryPlan.class, onRemoveConsumer)
                .thenApply(tiebreaker);
    }

    @Override
    public int compare(@Nonnull final RecordQueryPlan a,
                       @Nonnull final RecordQueryPlan b) {
        return tiebreaker.compare(getConfiguration(),
                FindExpressionVisitor.evaluate(interestingExpressionClasses, a), FindExpressionVisitor.evaluate(interestingExpressionClasses, b),
                a, b);
    }
}
