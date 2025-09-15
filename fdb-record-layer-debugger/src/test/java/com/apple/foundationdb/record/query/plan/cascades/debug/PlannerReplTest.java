/*
 * PlannerReplTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;

import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class PlannerReplTest {
    private PipedOutputStream outIn;
    private Terminal terminal;
    private ByteArrayOutputStream outputStream;
    private PlannerRepl debugger;

    @BeforeEach
    void setUp() throws IOException {
        PipedInputStream in = new PipedInputStream();
        outIn = new PipedOutputStream(in);
        outputStream = new ByteArrayOutputStream(2048);
        terminal = new DumbTerminal(in, outputStream);
        debugger = new PlannerRepl(terminal, false);

        Debugger.setDebugger(debugger);
        Debugger.setup();
        Debugger.withDebugger(d -> d.onQuery("SELECT * FROM A", PlanContext.EMPTY_CONTEXT));
    }

    @AfterAll
    static void tearDown() {
        Debugger.setDebugger(null);
    }

    @Test
    void testOnQueryPrintsQueryAndCreatesNewState() throws IOException, InterruptedException {
        outIn.write("cont\n".getBytes(StandardCharsets.UTF_8));
        final String query = "SELECT * FROM B";
        final EventState eventStatePreQuery = debugger.getCurrentState();
        final SymbolTables symbolStatePreQuery = debugger.getCurrentSymbolState();

        Debugger.withDebugger(d -> d.onQuery(query, PlanContext.EMPTY_CONTEXT));

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(ReplTestUtil.coloredKeyValue("query", query));
        assertThat(debugger.getCurrentState()).isNotSameAs(eventStatePreQuery);
        assertThat(debugger.getCurrentSymbolState()).isNotSameAs(symbolStatePreQuery);
    }

    @Test
    void testRestartResetsState() throws IOException {
        outIn.write("cont\n".getBytes(StandardCharsets.UTF_8));
        final EventState eventStatePreQuery = debugger.getCurrentState();
        final SymbolTables symbolStatePreQuery = debugger.getCurrentSymbolState();

        debugger.restartState();

        assertThat(debugger.getCurrentState()).isNotSameAs(eventStatePreQuery);
        assertThat(debugger.getCurrentSymbolState()).isNotSameAs(symbolStatePreQuery);
    }

    @Test
    void testOnEventUpdatesStatsMap() throws IOException {
        outIn.write("cont\n".getBytes(StandardCharsets.UTF_8));

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.END))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(Debugger.InsertIntoMemoEvent.newExp(new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList())))
        );

        assertThat(debugger.getCurrentState().getEvents()).hasSize(3);
        assertThat(debugger.getCurrentState().getEventProtos()).hasSize(3);
        assertThat(debugger.getStatsMaps()).isNotEmpty();
        assertThat(debugger.getStatsMaps().get().getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.REWRITING))
                .hasValueSatisfying(
                    m -> assertThat(m).hasSize(1)
                            .containsKey(Debugger.InitiatePlannerPhaseEvent.class)
                );
        assertThat(debugger.getStatsMaps().get().getEventWithoutStateClassStatsMap()).hasSize(1)
                .containsKey(Debugger.InsertIntoMemoEvent.class);
    }

    @Test
    void testGetIndex() throws IOException {
        outIn.write("cont\n".getBytes(StandardCharsets.UTF_8));
        final RelationalExpression exp0 = new SelectExpression(
                LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        final RelationalExpression exp1 = new SelectExpression(
                LiteralValue.ofScalar(2), Collections.emptyList(), Collections.emptyList());
        final Reference ref0 = Reference.initialOf(exp0, exp1);
        debugger.onRegisterQuantifier(Quantifier.forEach(ref0, CorrelationIdentifier.of("0")));

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Debugger.Location.BEGIN))
        );

        assertThat(SymbolDebugger.mapDebugger(d -> d.onGetIndex(RelationalExpression.class))).hasValue(2);
        assertThat(SymbolDebugger.mapDebugger(d -> d.onGetIndex(Reference.class))).hasValue(1);
        assertThat(SymbolDebugger.mapDebugger(d -> d.onGetIndex(Quantifier.class))).hasValue(1);
    }

    @Test
    void testUpdateIndex() {
        assertThat(SymbolDebugger.mapDebugger(d -> d.onGetIndex(RelationalExpression.class))).hasValue(0);

        SymbolDebugger.withDebugger(d -> d.onUpdateIndex(RelationalExpression.class, (i) -> i + 1));

        assertThat(SymbolDebugger.mapDebugger(d -> d.onGetIndex(RelationalExpression.class))).hasValue(1);
    }

    @Test
    void testPrintIdentifiers() throws IOException {
        outIn.write("exp0\nexp1\nref0\nqun0\ncont\n".getBytes(StandardCharsets.UTF_8));
        var exp0 = new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        var exp1 = new SelectExpression(LiteralValue.ofScalar(2), Collections.emptyList(), Collections.emptyList());
        var ref0 = Reference.initialOf(exp0, exp1);
        var qun0 = Quantifier.forEach(ref0, CorrelationIdentifier.of("0"));

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Debugger.Location.BEGIN))
        );

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(exp0)),
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(exp1)),
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(ref0)),
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(qun0))
        );
    }
}
