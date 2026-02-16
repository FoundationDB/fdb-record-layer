/*
 * InsertIntoMemoPlannerEvent.java
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

import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PInsertIntoMemoPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Events of this class are generated when the planner attempts to insert a new expression into the memoization
 * structures of the planner.
 */
public class InsertIntoMemoPlannerEvent implements PlannerEvent {
    @Nullable
    private final RelationalExpression expression;

    @Nonnull
    private final Location location;

    @Nonnull
    private final List<Reference> reusedExpressionReferences;

    private InsertIntoMemoPlannerEvent(@Nonnull final Location location,
                                       @Nullable final RelationalExpression expression,
                                       @Nonnull final Collection<Reference> reusedExpressionReferences) {
        if (expression != null) {
            Debugger.registerExpression(expression);
        }
        this.expression = expression;
        this.location = location;
        this.reusedExpressionReferences = ImmutableList.copyOf(reusedExpressionReferences);
        // Call debugger hook to potentially register this new reference.
        this.reusedExpressionReferences.forEach(Debugger::registerReference);
    }

    @Override
    @Nonnull
    public String getDescription() {
        return "insert into memo";
    }

    @Nonnull
    @Override
    public Shorthand getShorthand() {
        return Shorthand.INSERT_INTO_MEMO;
    }

    @Nullable
    public RelationalExpression getExpression() {
        return expression;
    }

    @Nonnull
    public Collection<Reference> getReusedExpressionReferences() {
        return reusedExpressionReferences;
    }

    @Nonnull
    @Override
    public Location getLocation() {
        return location;
    }

    @Nonnull
    @Override
    public PInsertIntoMemoPlannerEvent toProto() {
        final var builder = PInsertIntoMemoPlannerEvent.newBuilder()
                .setLocation(getLocation().name())
                .addAllReusedExpressionReferences(getReusedExpressionReferences().stream()
                        .map(PlannerEvent::toReferenceProto)
                        .collect(ImmutableList.toImmutableList()));
        if (expression != null) {
            builder.setExpression(PlannerEvent.toExpressionProto(expression));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setInsertIntoMemoPlannerEvent(toProto());
    }

    @Nonnull
    public static InsertIntoMemoPlannerEvent begin() {
        return new InsertIntoMemoPlannerEvent(Location.BEGIN, null, ImmutableList.of());
    }

    @Nonnull
    public static InsertIntoMemoPlannerEvent end() {
        return new InsertIntoMemoPlannerEvent(Location.END, null, ImmutableList.of());
    }

    @Nonnull
    public static InsertIntoMemoPlannerEvent newExp(@Nonnull final RelationalExpression expression) {
        return new InsertIntoMemoPlannerEvent(Location.NEW, expression, ImmutableList.of());
    }

    @Nonnull
    public static InsertIntoMemoPlannerEvent reusedExp(@Nonnull final RelationalExpression expression) {
        return new InsertIntoMemoPlannerEvent(Location.REUSED, expression, ImmutableList.of());
    }

    @Nonnull
    public static InsertIntoMemoPlannerEvent reusedExpWithReferences(@Nonnull final RelationalExpression expression,
                                                                     @Nonnull final List<Reference> references) {
        return new InsertIntoMemoPlannerEvent(Location.REUSED, expression, references);
    }
}
