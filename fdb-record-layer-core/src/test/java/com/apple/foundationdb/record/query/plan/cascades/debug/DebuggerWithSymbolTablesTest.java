/*
 * DebuggerWithSymbolTablesTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebuggerWithSymbolTablesTest {
    private DebuggerWithSymbolTables debugger;

    void setupDebugger() {
        Debugger.setDebugger(debugger);
        Debugger.setup();
        Debugger.withDebugger(d -> d.onQuery("SELECT * from A", PlanContext.EMPTY_CONTEXT));
    }

    @BeforeEach
    void setUp() {
        debugger = DebuggerWithSymbolTables.withoutSanityChecks();
        setupDebugger();
    }

    @AfterAll
    static void tearDown() {
        Debugger.setDebugger(null);
    }

    @Test
    void testOnQueryResetsState() {
        final EventState initialEventState = debugger.getCurrentEventState();
        final SymbolTables initialSymbolTables = debugger.getCurrentSymbolState();

        StatsDebugger.withDebugger(d -> d.onQuery("SELECT * from B", PlanContext.EMPTY_CONTEXT));

        assertThat(debugger.getCurrentEventState()).isNotSameAs(initialEventState);
        assertThat(debugger.getCurrentSymbolState()).isNotSameAs(initialSymbolTables);
    }

    @Test
    void testOnEventUpdatesStatsMap() {
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.END))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.END))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(Debugger.InsertIntoMemoEvent.newExp(
                        new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList())))
        );

        assertThat(debugger.getStatsMaps()).isNotEmpty();
        final StatsMaps statsMaps = debugger.getStatsMaps().get();
        assertThat(statsMaps.getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.REWRITING))
                .hasValueSatisfying(
                        m -> assertThat(m).hasSize(1)
                                .containsKey(Debugger.InitiatePlannerPhaseEvent.class)
                );
        assertThat(statsMaps.getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.PLANNING))
                .hasValueSatisfying(
                        m -> assertThat(m).hasSize(1)
                                .containsKey(Debugger.InitiatePlannerPhaseEvent.class)
                );
        assertThat(debugger.getStatsMaps().get().getEventWithoutStateClassStatsMap()).hasSize(1)
                .containsKey(Debugger.InsertIntoMemoEvent.class);
    }

    @Test
    void testDebuggerWithEventRecording() {
        debugger = DebuggerWithSymbolTables.withRerecordEvents();
        setupDebugger();

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.END))
        );

        assertThat(debugger.getCurrentEventState().getEvents()).hasSize(2);
        assertThat(debugger.getCurrentEventState().getEventProtos()).hasSize(2);
    }

    @Test
    void testDebuggerWithSanityChecksEnabled() {
        debugger = DebuggerWithSymbolTables.withSanityChecks();
        setupDebugger();

        assertThatThrownBy(() -> Debugger.sanityCheck(() -> { throw new RuntimeException(); }))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testOnDoneResetsEventState() {
        final EventState initialEventState = debugger.getCurrentEventState();
        final SymbolTables initialSymbolTables = debugger.getCurrentSymbolState();

        StatsDebugger.withDebugger(Debugger::onDone);

        assertThat(debugger.getCurrentEventState()).isNotSameAs(initialEventState);
        assertThat(debugger.getCurrentSymbolState()).isNotSameAs(initialSymbolTables);
    }
}
