/*
 * OptimizeGroupPlannerEvent.java
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
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.POptimizeGroupPlannerEvent;

import javax.annotation.Nonnull;
import java.util.Deque;

/**
 * Events of this class are generated when the planner optimizes a group.
 */
public class OptimizeGroupPlannerEvent extends AbstractPlannerEventWithState implements PlannerEventWithCurrentGroupReference {
    @Nonnull
    private final Reference currentGroupReference;

    public OptimizeGroupPlannerEvent(@Nonnull final PlannerPhase plannerPhase,
                                     @Nonnull final Reference rootReference,
                                     @Nonnull final Deque<CascadesPlanner.Task> taskStack,
                                     @Nonnull final Location location,
                                     @Nonnull final Reference currentGroupReference) {
        super(plannerPhase, rootReference, taskStack, location);
        this.currentGroupReference = currentGroupReference;
    }

    @Override
    @Nonnull
    public String getDescription() {
        return "optimizing group";
    }

    @Nonnull
    @Override
    public Shorthand getShorthand() {
        return Shorthand.OPTGROUP;
    }

    @Override
    @Nonnull
    public Reference getCurrentReference() {
        return currentGroupReference;
    }

    @Nonnull
    @Override
    public POptimizeGroupPlannerEvent toProto() {
        return POptimizeGroupPlannerEvent.newBuilder()
                .setSuper(toAbstractPlannerEventWithStateProto())
                .setCurrentGroupReference(currentGroupReference.toPlannerEventReferenceProto())
                .build();
    }

    @Nonnull
    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setOptimizeGroupPlannerEvent(toProto());
    }
}
