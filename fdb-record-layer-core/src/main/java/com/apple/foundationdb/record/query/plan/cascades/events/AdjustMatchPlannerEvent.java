/*
 * AdjustMatchPlannerEvent.java
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
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PAdjustMatchPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import java.util.Deque;

/**
 * Events of this class are generated when the planner attempts to adjust an existing match.
 */
public class AdjustMatchPlannerEvent extends AbstractPlannerEventWithState implements PlannerEventWithCurrentGroupReference {
    private final Reference currentGroupReference;
    private final RelationalExpression expression;

    public AdjustMatchPlannerEvent(final PlannerPhase plannerPhase,
                                   final Reference rootReference,
                                   final Deque<CascadesPlanner.Task> taskStack,
                                   final Location location,
                                   final Reference currentGroupReference,
                                   final RelationalExpression expression) {
        super(plannerPhase, rootReference, taskStack, location);
        this.currentGroupReference = currentGroupReference;
        this.expression = expression;
    }

    @Override
    public String getDescription() {
        return "adjust match";
    }

    @Override
    public Shorthand getShorthand() {
        return Shorthand.ADJUSTMATCH;
    }

    @Override
    public Reference getCurrentReference() {
        return currentGroupReference;
    }

    public RelationalExpression getExpression() {
        return expression;
    }

    @Override
    public PAdjustMatchPlannerEvent toProto() {
        return PAdjustMatchPlannerEvent.newBuilder()
                .setSuper(toAbstractPlannerEventWithStateProto())
                .setCurrentGroupReference(currentGroupReference.toPlannerEventReferenceProto())
                .setExpression(expression.toPlannerEventExpressionProto())
                .build();
    }

    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setAdjustMatchPlannerEvent(toProto());
    }
}
