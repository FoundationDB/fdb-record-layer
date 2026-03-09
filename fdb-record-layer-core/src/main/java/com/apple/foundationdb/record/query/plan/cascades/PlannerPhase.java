/*
 * PlannerPhase.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerPhase;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Enum for the planner phase a task of the {@link CascadesPlanner} can be in. Members of the enum define which ruleset
 * to apply, which cost model to apply, and if existent, the next phase following this phase.
 */
public enum PlannerPhase {
    // note that the phase are declared in a counterintuitive inverse way since a phase has to specify the next phase
    PLANNING(PlanningRuleSet.getDefault(), PlannerStage.PLANNED, PlanningCostModel::new),
    REWRITING(RewritingRuleSet.getDefault(), PlannerStage.CANONICAL, RewritingCostModel::new, PLANNING);

    @Nonnull
    private final CascadesRuleSet ruleSet;
    @Nonnull
    private final PlannerStage targetStage;
    @Nonnull
    private final Function<RecordQueryPlannerConfiguration, CascadesCostModel> costModelCreator;
    @Nullable
    private final PlannerPhase nextPhase;

    PlannerPhase(@Nonnull final CascadesRuleSet ruleSet,
                 @Nonnull final PlannerStage targetStage,
                 @Nonnull final Function<RecordQueryPlannerConfiguration, CascadesCostModel> costModelCreator) {
        this(ruleSet, targetStage, costModelCreator, null);
    }

    PlannerPhase(@Nonnull final CascadesRuleSet ruleSet,
                 @Nonnull final PlannerStage targetStage,
                 @Nonnull final Function<RecordQueryPlannerConfiguration, CascadesCostModel> costModelCreator,
                 @Nullable final PlannerPhase nextPhase) {
        this.ruleSet = ruleSet;
        this.targetStage = targetStage;
        this.costModelCreator = costModelCreator;
        this.nextPhase = nextPhase;
    }

    @Nonnull
    public CascadesRuleSet getRuleSet() {
        return ruleSet;
    }

    @Nonnull
    public PlannerStage getTargetPlannerStage() {
        return targetStage;
    }

    @Nonnull
    public CascadesCostModel createCostModel(@Nonnull final RecordQueryPlannerConfiguration configuration) {
        return costModelCreator.apply(configuration);
    }

    @Nonnull
    public PlannerPhase getNextPhase() {
        return Objects.requireNonNull(nextPhase);
    }

    public boolean hasNextPhase() {
        return nextPhase != null;
    }

    @Nonnull
    public PPlannerPhase toProto() {
        switch (this) {
            case REWRITING:
                return PPlannerPhase.REWRITING;
            case PLANNING:
                return PPlannerPhase.PLANNING;
            default:
                throw new RecordCoreException("no proto mapping for java planner phase");
        }
    }
}
