/*
 * CommandsTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class CommandsTest {
    private String query;
    private PipedOutputStream outIn;
    private Terminal terminal;
    private ByteArrayOutputStream outputStream;
    private PlannerRepl debugger;

    @BeforeEach
    void setUp() throws IOException {
        query = "SELECT * FROM A";
        PipedInputStream in = new PipedInputStream();
        outIn = new PipedOutputStream(in);
        outputStream = new ByteArrayOutputStream(2048);
        terminal = new DumbTerminal(in, outputStream);
        debugger = new PlannerRepl(terminal, false);

        Debugger.setDebugger(debugger);
        Debugger.setup();
        Debugger.withDebugger(d -> d.onQuery(query, PlanContext.EMPTY_CONTEXT));
    }

    @Test
    void testBreakCommand() throws IOException {
        outIn.write("break rule somerule BEGIN\nbreak list\ncont\n".getBytes(StandardCharsets.UTF_8));

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN))
        );

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("id", "0"),
                ReplTestUtil.coloredKeyValue("kind", "OnRuleBreakPoint"),
                ReplTestUtil.coloredKeyValue("location", "BEGIN"),
                ReplTestUtil.coloredKeyValue("ruleNamePrefix", "somerule")
        );
    }

    @Test
    void testCurrentCommand() throws IOException {
        outIn.write("current\ncont\n".getBytes(StandardCharsets.UTF_8));

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN))
        );

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("event", "initphase"),
                ReplTestUtil.coloredKeyValue("description", "initiating planner phase")
        );
    }

    @Test
    void testEventsCommand() throws IOException {
        outIn.write("step 1\nstep 1\nevents\ncont\n".getBytes(StandardCharsets.UTF_8));
        final RelationalExpression exp1 = new SelectExpression(
                LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.END))
        );
        StatsDebugger.withDebugger(
                d -> d.onEvent(Debugger.InsertIntoMemoEvent.newExp(exp1))
        );
        
        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("tick", "0"),
                        ReplTestUtil.coloredKeyValue("shorthand", "initphase")
                ),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("tick", "1"),
                        ReplTestUtil.coloredKeyValue("shorthand", "initphase")
                ),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("tick", "2"),
                        ReplTestUtil.coloredKeyValue("shorthand", "insert_into_memo")
                )
        );
    }

    @Test
    void testExpsCommand() throws IOException {
        outIn.write("exps\ncont\n".getBytes(StandardCharsets.UTF_8));
        final RelationalExpression exp0 = new SelectExpression(
                LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        final RelationalExpression exp1 = new SelectExpression(
                LiteralValue.ofScalar(2), Collections.emptyList(), Collections.emptyList());
        final Reference ref0 = Reference.initialOf(exp0, exp1);

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Debugger.Location.BEGIN))
        );

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("id", debugger.nameForObject(exp0)),
                ReplTestUtil.coloredKeyValue("id", debugger.nameForObject(exp0))
        );
    }

    @Test
    void testRefsCommand() throws IOException {
        outIn.write("refs\ncont\n".getBytes(StandardCharsets.UTF_8));
        final RelationalExpression exp0 = new SelectExpression(
                LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        final RelationalExpression exp1 = new SelectExpression(
                LiteralValue.ofScalar(2), Collections.emptyList(), Collections.emptyList());
        final Reference ref0 = Reference.initialOf(exp0, exp1);
        
        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Debugger.Location.BEGIN))
        );

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("id", debugger.nameForObject(ref0)),
                ReplTestUtil.coloredKeyValue(
                        "members", "{" + debugger.nameForObject(exp0) + ", " + debugger.nameForObject(exp1) + "}"
                )
        );
    }

    @Test
    void testQunsCommand() throws IOException {
        outIn.write("quns\ncont\n".getBytes(StandardCharsets.UTF_8));
        final Reference ref0 = Reference.empty();
        final Quantifier qun = Quantifier.forEach(ref0);

        StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Debugger.Location.BEGIN))
        );

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("id", debugger.nameForObject(qun)),
                ReplTestUtil.coloredKeyValue("ranges over", debugger.nameForObject(ref0))
        );
    }

    @Test
    void testRestartCommand() throws IOException {
        outIn.write("restart\n".getBytes(StandardCharsets.UTF_8));
        final EventState eventStateBeforeRestart = debugger.getCurrentState();
        final SymbolTables symbolStateBeforeRestart = debugger.getCurrentSymbolState();

        assertThatThrownBy(() -> StatsDebugger.withDebugger(
                d -> d.onEvent(new Debugger.InitiatePlannerPhaseEvent(
                        PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Debugger.Location.BEGIN)))
        ).isInstanceOf(RestartException.class);

        assertThat(debugger.getCurrentState()).isNotSameAs(eventStateBeforeRestart);
        assertThat(debugger.getCurrentSymbolState()).isNotSameAs(symbolStateBeforeRestart);
    }
}
