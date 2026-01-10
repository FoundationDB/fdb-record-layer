/*
 * AbstractPlannerEventWithState.java
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

package com.apple.foundationdb.record.query.plan.cascades.events;


import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PAbstractPlannerEventWithState;

import javax.annotation.Nonnull;
import java.util.Deque;

/**
 * Abstract event class to capture {@code rootReference} amd {@code taskStack}.
 */
abstract class AbstractPlannerEventWithState implements PlannerEventWithState {
    @Nonnull
    private final PlannerPhase plannerPhase;
    @Nonnull
    private final Reference rootReference;
    @Nonnull
    private final Deque<CascadesPlanner.Task> taskStack;
    @Nonnull
    private final PlannerEvent.Location location;

    protected AbstractPlannerEventWithState(@Nonnull final PlannerPhase plannerPhase,
                                            @Nonnull final Reference rootReference,
                                            @Nonnull final Deque<CascadesPlanner.Task> taskStack,
                                            @Nonnull final PlannerEvent.Location location) {
        this.plannerPhase = plannerPhase;
        this.rootReference = rootReference;
        this.taskStack = taskStack;
        this.location = location;
    }

    @Nonnull
    @Override
    public PlannerPhase getPlannerPhase() {
        return plannerPhase;
    }

    @Override
    @Nonnull
    public Reference getRootReference() {
        return rootReference;
    }

    @Nonnull
    @Override
    public Deque<CascadesPlanner.Task> getTaskStack() {
        return taskStack;
    }

    @Nonnull
    @Override
    public PlannerEvent.Location getLocation() {
        return location;
    }

    @Nonnull
    public PAbstractPlannerEventWithState toAbstractPlannerEventWithStateProto() {
        return PAbstractPlannerEventWithState.newBuilder()
                .setPlannerPhase(plannerPhase.toProto())
                .setRootReference(PlannerEvent.toReferenceProto(rootReference))
                .setLocation(getLocation().name())
                .build();
    }
}
