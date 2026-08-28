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

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.List;

/**
 * Events of this class are generated when the planner attempts to insert a new expression into the memoization
 * structures of the planner.
 */
public class InsertIntoMemoPlannerEvent implements PlannerEvent {
    @Nullable
    private final RelationalExpression expression;

    private final Location location;

    private final List<Reference> reusedExpressionReferences;

    private InsertIntoMemoPlannerEvent(final Location location,
                                       @Nullable final RelationalExpression expression,
                                       final Collection<Reference> reusedExpressionReferences) {
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
    public String getDescription() {
        return "insert into memo";
    }

    @Override
    public Shorthand getShorthand() {
        return Shorthand.INSERT_INTO_MEMO;
    }

    @Nullable
    public RelationalExpression getExpression() {
        return expression;
    }

    public Collection<Reference> getReusedExpressionReferences() {
        return reusedExpressionReferences;
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public PInsertIntoMemoPlannerEvent toProto() {
        final var builder = PInsertIntoMemoPlannerEvent.newBuilder()
                .setLocation(getLocation().name())
                .addAllReusedExpressionReferences(getReusedExpressionReferences().stream()
                        .map(Reference::toPlannerEventReferenceProto)
                        .collect(ImmutableList.toImmutableList()));
        if (expression != null) {
            builder.setExpression(expression.toPlannerEventExpressionProto());
        }
        return builder.build();
    }

    @Override
    public PPlannerEvent.Builder toEventBuilder() {
        return PPlannerEvent.newBuilder()
                .setInsertIntoMemoPlannerEvent(toProto());
    }

    public static InsertIntoMemoPlannerEvent begin() {
        return new InsertIntoMemoPlannerEvent(Location.BEGIN, null, ImmutableList.of());
    }

    public static InsertIntoMemoPlannerEvent end() {
        return new InsertIntoMemoPlannerEvent(Location.END, null, ImmutableList.of());
    }

    public static InsertIntoMemoPlannerEvent newExp(final RelationalExpression expression) {
        return new InsertIntoMemoPlannerEvent(Location.NEW, expression, ImmutableList.of());
    }

    public static InsertIntoMemoPlannerEvent reusedExp(final RelationalExpression expression) {
        return new InsertIntoMemoPlannerEvent(Location.REUSED, expression, ImmutableList.of());
    }

    public static InsertIntoMemoPlannerEvent reusedExpWithReferences(final RelationalExpression expression,
                                                                     final List<Reference> references) {
        return new InsertIntoMemoPlannerEvent(Location.REUSED, expression, references);
    }
}
