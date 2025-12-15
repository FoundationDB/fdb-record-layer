/*
 * TransformPlannerEvent.java
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
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PTransformPlannerEvent;

import javax.annotation.Nonnull;
import java.util.Deque;

/**
 * Events of this class are generated when the planner transforms an expression using a rule.
 */
public class TransformPlannerEvent extends AbstractPlannerEventWithState implements PlannerEventWithCurrentGroupReference, PlannerEventWithRule {
    @Nonnull
    private final Reference currentGroupReference;
    @Nonnull
    private final Object bindable;
    @Nonnull
    private final CascadesRule<?> rule;

    public TransformPlannerEvent(@Nonnull final PlannerPhase plannerPhase,
                                 @Nonnull final Reference rootReference,
                                 @Nonnull final Deque<CascadesPlanner.Task> taskStack,
                                 @Nonnull final Location location,
                                 @Nonnull final Reference currentGroupReference,
                                 @Nonnull final Object bindable,
                                 @Nonnull final CascadesRule<?> rule) {
        super(plannerPhase, rootReference, taskStack, location);
        this.currentGroupReference = currentGroupReference;
        this.bindable = bindable;
        this.rule = rule;
    }

    @Override
    @Nonnull
    public String getDescription() {
        return "transform";
    }

    @Nonnull
    @Override
    public Shorthand getShorthand() {
        return Shorthand.TRANSFORM;
    }

    @Override
    @Nonnull
    public Reference getCurrentReference() {
        return currentGroupReference;
    }

    @Nonnull
    public Object getBindable() {
        return bindable;
    }

    @Nonnull
    @Override
    public CascadesRule<?> getRule() {
        return rule;
    }

    @Nonnull
    @Override
    public PTransformPlannerEvent toProto() {
        return PTransformPlannerEvent.newBuilder()
                .setSuper(toAbstractPlannerEventWithStateProto())
                .setCurrentGroupReference(PlannerEvent.toReferenceProto(currentGroupReference))
                .setBindable(PlannerEvent.toBindableProto(bindable))
                .setRule(rule.toString())
                .build();
    }

    @Nonnull
    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setTransformPlannerEvent(toProto());
    }
}
