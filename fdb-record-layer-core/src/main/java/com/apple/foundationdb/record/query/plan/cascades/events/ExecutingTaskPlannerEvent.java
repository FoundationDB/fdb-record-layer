/*
 * ExecutingTaskPlannerEvent.java
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
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PExecutingTaskPlannerEvent;

import java.util.Deque;

/**
 * Events of this class are generated every time the planner executes a task.
 */
public class ExecutingTaskPlannerEvent extends AbstractPlannerEventWithState {
    private final CascadesPlanner.Task task;

    public ExecutingTaskPlannerEvent(final Reference rootReference,
                                     final Deque<CascadesPlanner.Task> taskStack,
                                     final Location location,
                                     final CascadesPlanner.Task task) {
        super(task.getPlannerPhase(), rootReference, taskStack, location);
        this.task = task;
    }

    @Override
    public String getDescription() {
        return "executing task";
    }

    @Override
    public Shorthand getShorthand() {
        return Shorthand.TASK;
    }

    public CascadesPlanner.Task getTask() {
        return task;
    }

    @Override
    public PExecutingTaskPlannerEvent toProto() {
        return PExecutingTaskPlannerEvent.newBuilder()
                .setSuper(toAbstractPlannerEventWithStateProto())
                .build();
    }

    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setExecutingTaskPlannerEvent(toProto());
    }
}
