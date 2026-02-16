/*
 * DefaultPlannerEventStatsCollectorTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultPlannerEventStatsCollectorTest {
    private DefaultPlannerEventStatsCollector collector;

    @BeforeEach
    void setUp() {
        collector = new DefaultPlannerEventStatsCollector();
        PlannerEventStatsCollector.setCollector(collector);
        PlannerEventStatsCollector.withCollector(
                c -> c.onQuery("SELECT * from A", PlanContext.EMPTY_CONTEXT));
    }

    @Test
    void testOnEventUpdatesStatsMap() {
        PlannerEventListeners.dispatchEvent(
                new InitiatePhasePlannerEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN));
        PlannerEventListeners.dispatchEvent(
                new InitiatePhasePlannerEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.END));
        PlannerEventListeners.dispatchEvent(
                new InitiatePhasePlannerEvent(
                        PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN));
        PlannerEventListeners.dispatchEvent(
                InsertIntoMemoPlannerEvent.newExp(
                        new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList())));
        PlannerEventListeners.dispatchEvent(
                new InitiatePhasePlannerEvent(
                        PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.END));

        assertThat(collector.getStatsMaps()).isNotEmpty();
        final PlannerEventStatsMaps statsMaps = collector.getStatsMaps().get();
        assertThat(statsMaps.getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.REWRITING))
                .hasValueSatisfying(
                        m -> assertThat(m).hasSize(1)
                                .containsKey(InitiatePhasePlannerEvent.class)
                );
        assertThat(statsMaps.getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.PLANNING))
                .hasValueSatisfying(
                        m -> assertThat(m).hasSize(1)
                                .containsKey(InitiatePhasePlannerEvent.class)
                );
        assertThat(statsMaps.getEventWithoutStateClassStatsMap()).hasSize(1)
                .containsKey(InsertIntoMemoPlannerEvent.class);
    }

    @Test
    void testOnQueryResetsState() {
        final PlannerEventStatsCollectorState initialState = collector.getCurrentState();

        PlannerEventStatsCollector.withCollector(
                collector -> collector.onQuery("SELECT * from A", PlanContext.EMPTY_CONTEXT));

        assertThat(collector.getCurrentState()).isNotSameAs(initialState);
    }

    @Test
    void testOnDoneResetsState() {
        final PlannerEventStatsCollectorState initialState = collector.getCurrentState();

        PlannerEventStatsCollector.withCollector(PlannerEventListeners.EventListener::onDone);

        assertThat(collector.getCurrentState()).isNotSameAs(initialState);
    }
}
