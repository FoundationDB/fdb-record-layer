/*
 * TranslateCorrelationsPlannerEvent.java
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

import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PTranslateCorrelationsPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;

/**
 * Events of this class are generated when the planner creates new expressions as part of rebasing or as part of
 * a translation of correlations in a graph.
 */
public class TranslateCorrelationsPlannerEvent implements PlannerEvent {
    @Nonnull
    private final RelationalExpression expression;

    @Nonnull
    private final Location location;

    public TranslateCorrelationsPlannerEvent(@Nonnull final RelationalExpression expression,
                                             @Nonnull final Location location) {
        this.expression = expression;
        this.location = location;
    }

    @Override
    @Nonnull
    public String getDescription() {
        return "translate correlations";
    }

    @Nonnull
    @Override
    public Shorthand getShorthand() {
        return Shorthand.TRANSLATE_CORRELATIONS;
    }

    @Nonnull
    public RelationalExpression getExpression() {
        return expression;
    }

    @Nonnull
    @Override
    public Location getLocation() {
        return location;
    }

    @Nonnull
    @Override
    public PTranslateCorrelationsPlannerEvent toProto() {
        return PTranslateCorrelationsPlannerEvent.newBuilder()
                .setExpression(PlannerEvent.toExpressionProto(expression))
                .setLocation(location.name())
                .build();
    }

    @Nonnull
    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setTranslateCorrelationsPlannerEvent(toProto());
    }
}
