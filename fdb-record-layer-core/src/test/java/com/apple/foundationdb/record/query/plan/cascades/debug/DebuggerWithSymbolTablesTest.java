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
import com.apple.foundationdb.record.query.plan.cascades.events.InitiatePhasePlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;

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
        final State initialState = debugger.getCurrentState();

        Debugger.withDebugger(d -> d.onQuery("SELECT * from B", PlanContext.EMPTY_CONTEXT));

        assertThat(debugger.getCurrentState()).isNotSameAs(initialState);
    }

    @Test
    void testDebuggerWithEventRecording() {
        debugger = DebuggerWithSymbolTables.withEventRecording();
        setupDebugger();
        final var beginEvent = new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN);
        final var endEvent = new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.END);

        PlannerEventListeners.dispatchEvent(beginEvent);
        PlannerEventListeners.dispatchEvent(endEvent);

        assertThat(debugger.getCurrentState().getEvents()).hasSize(2).containsExactly(beginEvent, endEvent);
    }

    @Test
    void testDebuggerWithSanityChecksEnabled() {
        debugger = DebuggerWithSymbolTables.withSanityChecks();
        setupDebugger();

        assertThatThrownBy(() -> Debugger.sanityCheck(() -> { throw new RuntimeException(); }))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testOnDoneResetsState() {
        final State initialState = debugger.getCurrentState();

        Debugger.withDebugger(Debugger::onDone);

        assertThat(debugger.getCurrentState()).isNotSameAs(initialState);
    }
}
