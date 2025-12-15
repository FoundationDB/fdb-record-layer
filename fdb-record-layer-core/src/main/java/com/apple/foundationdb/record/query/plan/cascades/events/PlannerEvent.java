/*
 * PlannerEvent.java
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

import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PBindable;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PMatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PPartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PRegisteredReference;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PRegisteredRelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * Tag interface for all events.
 */
public interface PlannerEvent {
    /**
     * Shorthands to identify a kind of event.
     */
    enum Shorthand {
        TASK,
        OPTGROUP,
        EXPEXP,
        EXPGROUP,
        ADJUSTMATCH,
        MATCHEXPCAND,
        OPTINPUTS,
        RULECALL,
        TRANSFORM,
        INSERT_INTO_MEMO,
        TRANSLATE_CORRELATIONS,
        INITPHASE
    }

    /**
     * Enum to indicate where an event happened.
     */
    enum Location {
        ANY,
        BEGIN,
        END,
        MATCH_PRE,
        YIELD,
        FAILURE,
        COUNT,
        NEW,
        REUSED,
        DISCARDED_INTERSECTION_COMBINATIONS,
        ALL_INTERSECTION_COMBINATIONS
    }

    /**
     * Getter.
     * @return description of an event
     */
    @Nonnull
    String getDescription();

    /**
     * Getter.
     *
     * @return the shorthand for the event. This is the string used for interaction on the command line, e.g.
     *         setting a breakpoint, etc.
     */
    @Nonnull
    Shorthand getShorthand();

    /**
     * Getter.
     *
     * @return the location of where the event came from
     */
    @Nonnull
    Location getLocation();

    @Nonnull
    Message toProto();

    @Nonnull
    default PPlannerEvent toEventProto() {
        return toEventBuilder()
                .setDescription(getDescription())
                .setShorthand(getShorthand().name())
                .build();
    }

    @Nonnull
    PPlannerEvent.Builder toEventBuilder();

    @Nonnull
    static PRegisteredRelationalExpression toExpressionProto(@Nonnull final RelationalExpression expression) {
        return PRegisteredRelationalExpression.newBuilder()
                .setName(Debugger.mapDebugger(debugger -> debugger.nameForObject(expression)).orElseThrow())
                .setSemanticHashCode(expression.semanticHashCode())
                .build();
    }

    @Nonnull
    static PRegisteredReference toReferenceProto(@Nonnull final Reference reference) {
        final var builder = PRegisteredReference.newBuilder()
                .setName(Debugger.mapDebugger(debugger -> debugger.nameForObject(reference)).orElseThrow());
        for (final var member : reference.getAllMemberExpressions()) {
            builder.addExpressions(toExpressionProto(member));
        }
        return builder.build();
    }

    @Nonnull
    static PBindable toBindableProto(@Nonnull final Object bindable) {
        final var builder = PBindable.newBuilder();
        if (bindable instanceof RelationalExpression) {
            builder.setExpression(toExpressionProto((RelationalExpression)bindable));
        } else if (bindable instanceof PartialMatch) {
            builder.setPartialMatch(toPartialMatchProto((PartialMatch)bindable));
        } else if (bindable instanceof MatchPartition) {
            builder.setMatchPartition(toMatchPartitionProto((MatchPartition)bindable));
        }
        return builder.build();
    }

    @Nonnull
    static PPartialMatch toPartialMatchProto(@Nonnull final PartialMatch partialMatch) {
        return PPartialMatch.newBuilder()
                .setMatchCandidate(partialMatch.toString())
                .setQueryRef(toReferenceProto(partialMatch.getQueryRef()))
                .setQueryExpression(toExpressionProto(partialMatch.getQueryExpression()))
                .setCandidateRef(toReferenceProto(partialMatch.getCandidateRef()))
                .build();
    }

    @Nonnull
    static PMatchPartition toMatchPartitionProto(@Nonnull final MatchPartition matchPartition) {
        final var builder = PMatchPartition.newBuilder();
        for (final var partialMatch : matchPartition.getPartialMatches()) {
            builder.addPartialMatches(toPartialMatchProto(partialMatch));
        }
        return builder.build();
    }
}
