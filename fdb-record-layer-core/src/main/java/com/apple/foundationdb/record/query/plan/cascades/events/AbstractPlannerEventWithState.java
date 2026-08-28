/*
 * AbstractPlannerEventWithState.java
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
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PAbstractPlannerEventWithState;

import java.util.Deque;

/**
 * Abstract event class to capture {@code rootReference} amd {@code taskStack}.
 */
abstract class AbstractPlannerEventWithState implements PlannerEventWithState {
    private final PlannerPhase plannerPhase;
    private final Reference rootReference;
    private final Deque<CascadesPlanner.Task> taskStack;
    private final PlannerEvent.Location location;

    protected AbstractPlannerEventWithState(final PlannerPhase plannerPhase,
                                            final Reference rootReference,
                                            final Deque<CascadesPlanner.Task> taskStack,
                                            final PlannerEvent.Location location) {
        this.plannerPhase = plannerPhase;
        this.rootReference = rootReference;
        this.taskStack = taskStack;
        this.location = location;
    }

    @Override
    public PlannerPhase getPlannerPhase() {
        return plannerPhase;
    }

    @Override
    public Reference getRootReference() {
        return rootReference;
    }

    @Override
    public Deque<CascadesPlanner.Task> getTaskStack() {
        return taskStack;
    }

    @Override
    public PlannerEvent.Location getLocation() {
        return location;
    }

    public PAbstractPlannerEventWithState toAbstractPlannerEventWithStateProto() {
        return PAbstractPlannerEventWithState.newBuilder()
                .setPlannerPhase(plannerPhase.toProto())
                .setRootReference(rootReference.toPlannerEventReferenceProto())
                .setLocation(getLocation().name())
                .build();
    }
}
