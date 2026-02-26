/*
 * ProceduralPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;

import javax.annotation.Nonnull;
import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public final class ProceduralPlan extends Plan<Void> {

    private final ConstantAction action;

    private ProceduralPlan(@Nonnull final ConstantAction action) {
        super("ProceduralPlan(" + action.getClass().getSimpleName() + ")");
        this.action = action;
    }

    @Override
    public boolean isUpdatePlan() {
        return true;
    }

    @Override
    public Plan<Void> optimize(@Nonnull CascadesPlanner planner, @Nonnull PlanContext planContext,
                               @Nonnull PlanHashable.PlanHashMode currentPlanHashMode) {
        return this;
    }

    @Override
    public Void executeInternal(@Nonnull final ExecutionContext context) throws RelationalException {
        final var metricCollector = Objects.requireNonNull(context.metricCollector);
        return metricCollector.clock(RelationalMetric.RelationalEvent.EXECUTE_PROCEDURAL_PLAN_ACTION, () -> {
            action.executeAction(context.transaction);
            return null;
        });
    }

    @Nonnull
    @Override
    public QueryPlanConstraint getConstraint() {
        return QueryPlanConstraint.noConstraint();
    }

    @Nonnull
    @Override
    public Plan<Void> withExecutionContext(@Nonnull final QueryExecutionContext queryExecutionContext) {
        return this;
    }

    @Nonnull
    @Override
    public String explain() {
        // TODO: this implementation is not correct as a few actions don't implement toString
        // TODO (Implement ProceduralPlan.explain)
        return "ProceduralPlan(" + action + ")";
    }

    public static ProceduralPlan of(@Nonnull final ConstantAction action) {
        return new ProceduralPlan(action);
    }
}
