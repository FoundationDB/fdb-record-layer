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

import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public final class ProceduralPlan extends Plan<Void> {

    private final ConstantAction action;

    private ProceduralPlan(final ConstantAction action) {
        super("ProceduralPlan(" + action.getClass().getSimpleName() + ")");
        this.action = action;
    }

    @Override
    public boolean isUpdatePlan() {
        return true;
    }

    @Override
    public Plan<Void> optimize(CascadesPlanner planner, PlanContext planContext,
                               PlanHashable.PlanHashMode currentPlanHashMode) {
        return this;
    }

    @Override
    // Void's only value is null, so this genuinely always returns null; NullAway still treats the plain
    // (unannotated) type variable T from Plan#executeInternal as @NonNull at this override, regardless of
    // Plan's generic bound now allowing @Nullable Object, so this can't be declared @Nullable Void either.
    @SuppressWarnings("NullAway")
    public Void executeInternal(final ExecutionContext context) throws RelationalException {
        final var metricCollector = Objects.requireNonNull(context.metricCollector);
        return metricCollector.clock(RelationalMetric.RelationalEvent.EXECUTE_PROCEDURAL_PLAN_ACTION, () -> {
            action.executeAction(context.transaction);
            return null;
        });
    }

    @Override
    public QueryPlanConstraint getConstraint() {
        return QueryPlanConstraint.noConstraint();
    }

    @Override
    public Plan<Void> withExecutionContext(final QueryExecutionContext queryExecutionContext) {
        return this;
    }

    @Override
    public String explain() {
        // TODO: this implementation is not correct as a few actions don't implement toString
        // TODO (Implement ProceduralPlan.explain)
        return "ProceduralPlan(" + action + ")";
    }

    public static ProceduralPlan of(final ConstantAction action) {
        return new ProceduralPlan(action);
    }
}
