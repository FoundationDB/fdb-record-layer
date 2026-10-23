/*
 * PlanCacheCostModel.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.costing.CascadesCostModel;
import com.apple.foundationdb.record.query.plan.cascades.costing.PickRightTiebreaker;
import com.apple.foundationdb.record.query.plan.cascades.costing.PlanOpsMap;
import com.apple.foundationdb.record.query.plan.cascades.costing.PlanningCostModel;
import com.apple.foundationdb.record.query.plan.cascades.costing.Tiebreaker;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

/**
 * Picks between the plans held in one plan cache entry. It is {@link PlanningCostModel} minus the tiebreakers that
 * consult a plan operator's match candidate, which a plan no longer carries once it has left the planner.
 * <br>
 * The tiebreakers read the {@link RecordQueryPlannerConfiguration} (e.g. whether to prefer an index scan over a
 * scan), so it must be the one the planner would have used for the query being looked up — the configuration held by
 * the secondary cache key the entry was found under — or a cached plan could win over one the planner would pick.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class PlanCacheCostModel implements CascadesCostModel<RecordQueryPlan>, Comparator<RecordQueryPlan> {
    @Nonnull
    private static final Tiebreaker<RecordQueryPlan> TIEBREAKER =
            Tiebreaker.combineTiebreakers(ImmutableList.of(
                    PlanningCostModel.lowestNumUnsatisfiedFiltersTiebreaker(),
                    PlanningCostModel.lowestNumDataAccessesTiebreaker(),
                    PlanningCostModel.recursiveCteOperatorTiebreaker(),
                    PlanningCostModel.inOperatorTiebreaker(),
                    PlanningCostModel.primaryScanVsIndexScanTiebreaker(),
                    PlanningCostModel.lowestTypeFilterCountTiebreaker(),
                    PlanningCostModel.deepestTypeFilterPositionTiebreaker(),
                    PlanningCostModel.betterIndexScanTiebreaker(),
                    PlanningCostModel.deepestDistinctTiebreaker(),
                    PlanningCostModel.highestNumInJoinTiebreaker(),
                    PlanningCostModel.lowestNumSimpleOperationsTiebreaker(),
                    PlanningCostModel.lowestNumDefaultOnEmptyTiebreaker(),
                    PlanningCostModel.planHashTiebreaker(),
                    PickRightTiebreaker.pickRightTiebreaker()));

    @Nonnull
    private final RecordQueryPlannerConfiguration configuration;

    public PlanCacheCostModel(@Nonnull final RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return configuration;
    }

    @Nonnull
    @Override
    public Optional<RecordQueryPlan> getBestExpression(@Nonnull final Set<? extends RelationalExpression> expressions) {
        return Tiebreaker.ofContext(getConfiguration(), expressions, RecordQueryPlan.class)
                .thenApply(TIEBREAKER)
                .getOnlyExpressionMaybe();
    }

    @Override
    public int compare(@Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
        return TIEBREAKER.compare(getConfiguration(), new PlanOpsMap(a), new PlanOpsMap(b), a, b);
    }
}
