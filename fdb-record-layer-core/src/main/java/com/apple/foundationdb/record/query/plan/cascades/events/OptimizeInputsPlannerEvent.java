/*
 * OptimizeInputsPlannerEvent.java
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

package com.apple.foundationdb.record.query.plan.cascades.events;

import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.POptimizeInputsPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.Deque;

/**
 * Events of this class are generated when the planner optimizes inputs.
 */
public class OptimizeInputsPlannerEvent extends AbstractPlannerEventWithState implements PlannerEventWithCurrentGroupReference {
    @Nonnull
    private final Reference currentGroupReference;
    @Nonnull
    private final RelationalExpression expression;

    public OptimizeInputsPlannerEvent(@Nonnull final PlannerPhase plannerPhase,
                                      @Nonnull final Reference rootReference,
                                      @Nonnull final Deque<CascadesPlanner.Task> taskStack,
                                      @Nonnull final Location location,
                                      @Nonnull final Reference currentGroupReference,
                                      @Nonnull final RelationalExpression expression) {
        super(plannerPhase, rootReference, taskStack, location);
        this.currentGroupReference = currentGroupReference;
        this.expression = expression;
    }

    @Override
    @Nonnull
    public String getDescription() {
        return "optimize inputs";
    }

    @Nonnull
    @Override
    public Shorthand getShorthand() {
        return Shorthand.OPTINPUTS;
    }

    @Override
    @Nonnull
    public Reference getCurrentReference() {
        return currentGroupReference;
    }

    @Nonnull
    public RelationalExpression getExpression() {
        return expression;
    }

    @Nonnull
    @Override
    public POptimizeInputsPlannerEvent toProto() {
        return POptimizeInputsPlannerEvent.newBuilder()
                .setSuper(toAbstractPlannerEventWithStateProto())
                .setCurrentGroupReference(currentGroupReference.toPlannerEventReferenceProto())
                .setExpression(expression.toPlannerEventExpressionProto())
                .build();
    }

    @Nonnull
    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setOptimizeInputsPlannerEvent(toProto());
    }
}
