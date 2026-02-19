/*
 * PlannerReplTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.InitiatePhasePlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Location;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
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
    void testOnQueryPrintsQueryAndCreatesNewState() throws IOException {
        outIn.write("cont\n".getBytes(StandardCharsets.UTF_8));
        final String query = "SELECT * FROM B";
        final State initalState = debugger.getCurrentState();

        Debugger.withDebugger(d -> d.onQuery(query, PlanContext.EMPTY_CONTEXT));

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(ReplTestUtil.coloredKeyValue("query", query));
        assertThat(debugger.getCurrentState()).isNotSameAs(initalState);
    }

    @Test
    void testRestartResetsState() throws IOException {
        outIn.write("cont\n".getBytes(StandardCharsets.UTF_8));
        final State initalState = debugger.getCurrentState();

        debugger.restartState();

        assertThat(debugger.getCurrentState()).isNotSameAs(initalState);
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

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Location.BEGIN));

        assertThat(Debugger.mapDebugger(d -> d.onGetIndex(RelationalExpression.class))).hasValue(2);
        assertThat(Debugger.mapDebugger(d -> d.onGetIndex(Reference.class))).hasValue(1);
        assertThat(Debugger.mapDebugger(d -> d.onGetIndex(Quantifier.class))).hasValue(1);
    }

    @Test
    void testUpdateIndex() {
        assertThat(Debugger.mapDebugger(d -> d.onGetIndex(RelationalExpression.class))).hasValue(0);

        Debugger.withDebugger(d -> d.onUpdateIndex(RelationalExpression.class, (i) -> i + 1));

        assertThat(Debugger.mapDebugger(d -> d.onGetIndex(RelationalExpression.class))).hasValue(1);
    }
}
