/*
 * CommandsTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupByMappings;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Traversal;
import com.apple.foundationdb.record.query.plan.cascades.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.events.ExecutingTaskPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.InitiatePhasePlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.InsertIntoMemoPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Location;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformRuleCallPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementExplodeRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementSimpleSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @AfterAll
    static void tearDown() {
        Debugger.setDebugger(null);
    }

    @Test
    void testCurrentCommand() throws IOException {
        outIn.write("current\ncont\n".getBytes(StandardCharsets.UTF_8));

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.BEGIN));

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("event", "initphase"),
                ReplTestUtil.coloredKeyValue("description", "initiating planner phase")
        );
    }

    @Test
    void testEventsCommand() throws IOException {
        outIn.write("step 1\nstep 1\nevents\ncont\n".getBytes(StandardCharsets.UTF_8));
        final RelationalExpression exp = new SelectExpression(
                LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.BEGIN));
        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.END));
        PlannerEventListeners.dispatchEvent(() -> InsertIntoMemoPlannerEvent.newExp(exp));

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

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Location.BEGIN));

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

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Location.BEGIN));

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("id", debugger.nameForObject(ref0)),
                        ReplTestUtil.coloredKeyValue("kind", "Reference"),
                        ReplTestUtil.coloredKeyValue(
                            "members", "{" + debugger.nameForObject(exp0) + ", " + debugger.nameForObject(exp1) + "}"
                    )
                )
        );
    }

    @Test
    void testQunsCommand() throws IOException {
        outIn.write("quns\ncont\n".getBytes(StandardCharsets.UTF_8));
        final Reference ref = Reference.empty();
        final Quantifier qun = Quantifier.forEach(ref);

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, ref, new ArrayDeque<>(), Location.BEGIN));

        terminal.writer().flush();
        assertThat(outputStream.toString()).contains(
                String.join(
                    "; ",
                    ReplTestUtil.coloredKeyValue("id", debugger.nameForObject(qun)),
                    ReplTestUtil.coloredKeyValue("kind", qun.getShorthand()),
                    ReplTestUtil.coloredKeyValue("alias", qun.getAlias().toString()),
                    ReplTestUtil.coloredKeyValue("ranges over", debugger.nameForObject(ref))
                )
        );
    }

    @Test
    void testRestartCommand() throws IOException {
        outIn.write("restart\n".getBytes(StandardCharsets.UTF_8));
        final State initialStateBeforeRestart = debugger.getCurrentState();

        assertThatThrownBy(
                () -> PlannerEventListeners.dispatchEvent(() ->
                        new InitiatePhasePlannerEvent(
                                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.BEGIN))
        ).isInstanceOf(RestartException.class);

        assertThat(debugger.getCurrentState()).isNotSameAs(initialStateBeforeRestart);
    }

    @Test
    void testPrintIdentifiers() throws IOException {
        outIn.write("exp0\nexp1\nref0\nqun0\ncont\n".getBytes(StandardCharsets.UTF_8));
        var exp0 = new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        var exp1 = new SelectExpression(LiteralValue.ofScalar(2), Collections.emptyList(), Collections.emptyList());
        var ref0 = Reference.initialOf(exp0, exp1);
        var qun0 = Quantifier.forEach(ref0, CorrelationIdentifier.of("0"));

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, ref0, new ArrayDeque<>(), Location.BEGIN));

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(exp0)),
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(exp1)),
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(ref0)),
                ReplTestUtil.coloredKeyValue("name", debugger.nameForObject(qun0))
        );
    }

    @Test
    void testTasksCommand() throws IOException {
        outIn.write("tasks\ncont\n".getBytes(StandardCharsets.UTF_8));
        var ref = Reference.empty();

        PlannerEventListeners.dispatchEvent(() ->
                new InitiatePhasePlannerEvent(
                    PlannerPhase.REWRITING, ref, new ArrayDeque<>(List.of(new DummyCascadesTask())),
                        Location.BEGIN));

        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("location", "begin"),
                        ReplTestUtil.coloredKeyValue("shorthand", "task"),
                        ReplTestUtil.coloredKeyValue("description", "executing task"),
                        ReplTestUtil.coloredKeyValue("kind", "DummyCascadesTask")
                )
        );
    }

    @Test
    void testHelpCommand() throws IOException {
        outIn.write("help\ncont\n".getBytes(StandardCharsets.UTF_8));
        var ref = Reference.empty();

        PlannerEventListeners.dispatchEvent(() ->
                new InitiatePhasePlannerEvent(
                        PlannerPhase.REWRITING, ref, new ArrayDeque<>(), Location.BEGIN));

        assertThat(outputStream.toString()).contains(
                "Basic Usage", ReplTestUtil.coloredKeyValue("cont", "continue execution"));

    }

    @Test
    void testShowCommandWithInvalidIdentifierPrintsError() throws IOException {
        outIn.write("show ref1\ncont\n".getBytes(StandardCharsets.UTF_8));
        var ref = Reference.empty();

        PlannerEventListeners.dispatchEvent(() ->
                new InitiatePhasePlannerEvent(
                        PlannerPhase.REWRITING, ref, new ArrayDeque<>(), Location.BEGIN));

        assertThat(outputStream.toString()).contains("not sure what to show");
    }

    @Test
    void testCountingTautologyBreakPoint() throws IOException {
        outIn.write("step 2\ncurrent\ncont\n".getBytes(StandardCharsets.UTF_8));

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.BEGIN));
        PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::begin);
        PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::end);

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("paused in", "Test worker at") + " " + ReplTestUtil.coloredKeyValue("tick", "2"),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "insert_into_memo"),
                        ReplTestUtil.coloredKeyValue("location", "end")
                )
        ).doesNotContain(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "insert_into_memo"),
                        ReplTestUtil.coloredKeyValue("location", "begin")
                )
        );
    }

    @Test
    void testOnEventTypeBreakPoint() throws IOException {
        outIn.write("break initphase end\nbreak list\ncont\ncurrent\ncont\n".getBytes(StandardCharsets.UTF_8));

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.BEGIN));
        PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::begin);
        PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::end);
        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.END));

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("kind", "OnEventTypeBreakPoint"),
                        ReplTestUtil.coloredKeyValue("enabled", "true"),
                        ReplTestUtil.coloredKeyValue("count down", "-1"),
                        ReplTestUtil.coloredKeyValue("shorthand", "initphase"),
                        ReplTestUtil.coloredKeyValue("location", "end")
                ),

                ReplTestUtil.coloredKeyValue("paused in", "Test worker at") + " " + ReplTestUtil.coloredKeyValue("tick", "3"),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "initphase"),
                        ReplTestUtil.coloredKeyValue("location", "end"),
                        ReplTestUtil.coloredKeyValue("planner phase", "rewriting")
                )
        );
    }

    @Test
    void testOnPhaseBreakPoint() throws IOException {
        outIn.write("phase planning\ncont\ncurrent\ncont\n".getBytes(StandardCharsets.UTF_8));

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), Location.BEGIN));
        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.END));
        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.PLANNING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN));

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                ReplTestUtil.coloredKeyValue("paused in", "Test worker at") + " " + ReplTestUtil.coloredKeyValue("tick", "2"),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "initphase"),
                        ReplTestUtil.coloredKeyValue("location", "begin"),
                        ReplTestUtil.coloredKeyValue("planner phase", "planning")
                )
        );
    }

    @Test
    void testOnRuleBreakPoint() throws IOException {
        outIn.write("break rule ImplementSimpleSelectRule begin\nbreak list\ncont\ncurrent\ncont\n".getBytes(StandardCharsets.UTF_8));

        final var exp = new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        final var ref = Reference.initialOf(exp);
        final var matchingRule = new ImplementSimpleSelectRule();
        final var nonMatchingRule = new ImplementExplodeRule();
        Debugger.registerExpression(exp);

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN));
        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.END));
        PlannerEventListeners.dispatchEvent(() ->
                new TransformPlannerEvent(PlannerPhase.PLANNING, ref, new ArrayDeque<>(), PlannerEvent.Location.BEGIN,
                        ref, exp, nonMatchingRule));
        PlannerEventListeners.dispatchEvent(() ->
                new TransformPlannerEvent(PlannerPhase.PLANNING, ref, new ArrayDeque<>(), PlannerEvent.Location.BEGIN,
                        ref, exp, matchingRule));

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("kind", "OnRuleBreakPoint"),
                        ReplTestUtil.coloredKeyValue("enabled", "true"),
                        ReplTestUtil.coloredKeyValue("count down", "-1"),
                        ReplTestUtil.coloredKeyValue("ruleNamePrefix", "ImplementSimpleSelectRule")
                ),
                ReplTestUtil.coloredKeyValue("paused in", "Test worker at") + " " + ReplTestUtil.coloredKeyValue("tick", "3"),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "transform"),
                        ReplTestUtil.coloredKeyValue("location", "begin"),
                        ReplTestUtil.coloredKeyValue("description", "transform"),
                        ReplTestUtil.coloredKeyValue("root", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("group", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("expression", Debugger.getDebugger().nameForObject(exp)),
                        ReplTestUtil.coloredKeyValue("rule", "ImplementSimpleSelectRule")
                )
        ).doesNotContain(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "transform"),
                        ReplTestUtil.coloredKeyValue("location", "begin"),
                        ReplTestUtil.coloredKeyValue("description", "transform"),
                        ReplTestUtil.coloredKeyValue("root", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("group", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("expression", Debugger.getDebugger().nameForObject(exp)),
                        ReplTestUtil.coloredKeyValue("rule", "ImplementExplodeRule")
                )
        );
    }

    @Test
    void testOnRuleCallBreakPoint() throws IOException {
        outIn.write("break rulecall ImplementSimpleSelectRule begin\nbreak list\ncont\ncurrent\ncont\n".getBytes(StandardCharsets.UTF_8));

        final var exp = new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        final var ref = Reference.initialOf(exp);
        final var matchingRule = new ImplementSimpleSelectRule();
        final var nonMatchingRule = new ImplementExplodeRule();

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN));
        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.END));
        PlannerEventListeners.dispatchEvent(() ->
                new TransformRuleCallPlannerEvent(PlannerPhase.PLANNING, ref, new ArrayDeque<>(), PlannerEvent.Location.BEGIN,
                        ref, exp, nonMatchingRule, new CascadesRuleCall(PlannerPhase.PLANNING, PlanContext.EMPTY_CONTEXT,
                        nonMatchingRule, ref, Traversal.withRoot(ref), new ArrayDeque<>(), PlannerBindings.empty(), EvaluationContext.EMPTY)));
        PlannerEventListeners.dispatchEvent(() ->
                new TransformRuleCallPlannerEvent(PlannerPhase.PLANNING, ref, new ArrayDeque<>(),
                        PlannerEvent.Location.BEGIN, ref, exp, matchingRule,
                        new CascadesRuleCall(PlannerPhase.PLANNING, PlanContext.EMPTY_CONTEXT, matchingRule, ref,
                                Traversal.withRoot(ref), new ArrayDeque<>(), PlannerBindings.empty(), EvaluationContext.EMPTY)));


        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("kind", "OnRuleCallBreakPoint"),
                        ReplTestUtil.coloredKeyValue("enabled", "true"),
                        ReplTestUtil.coloredKeyValue("count down", "-1"),
                        ReplTestUtil.coloredKeyValue("ruleNamePrefix", "ImplementSimpleSelectRule")
                ),
                ReplTestUtil.coloredKeyValue("paused in", "Test worker at") + " " + ReplTestUtil.coloredKeyValue("tick", "3"),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "rulecall"),
                        ReplTestUtil.coloredKeyValue("location", "begin"),
                        ReplTestUtil.coloredKeyValue("description", "transform rule call"),
                        ReplTestUtil.coloredKeyValue("root", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("group", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("expression", Debugger.getDebugger().nameForObject(exp)),
                        ReplTestUtil.coloredKeyValue("rule", "ImplementSimpleSelectRule")
                )
        ).doesNotContain(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "rulecall"),
                        ReplTestUtil.coloredKeyValue("location", "begin"),
                        ReplTestUtil.coloredKeyValue("description", "transform rule call"),
                        ReplTestUtil.coloredKeyValue("root", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("group", Debugger.getDebugger().nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("expression", Debugger.getDebugger().nameForObject(exp)),
                        ReplTestUtil.coloredKeyValue("rule", "ImplementExplodeRule")
                )
        );
    }

    @Test
    void testOnYieldExpressionBreakPoint() throws IOException {
        outIn.write(("break yield exp exp1\nbreak list\ncont\ncurrent\ncont\n").getBytes(StandardCharsets.UTF_8));

        final var exp = new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        final var ref = Reference.initialOf(exp);
        final var yieldedExp = new SelectExpression(LiteralValue.ofScalar(42), Collections.emptyList(), Collections.emptyList());
        final var ruleCall = new CascadesRuleCall(PlannerPhase.PLANNING, PlanContext.EMPTY_CONTEXT, new ImplementSimpleSelectRule(), ref,
                Traversal.withRoot(ref), new ArrayDeque<>(), PlannerBindings.empty(), EvaluationContext.EMPTY);
        ruleCall.yieldExploratoryExpression(yieldedExp);

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN));
        PlannerEventListeners.dispatchEvent(() ->
                new TransformRuleCallPlannerEvent(PlannerPhase.REWRITING, ref, new ArrayDeque<>(),
                        PlannerEvent.Location.END, ref, exp, new ImplementSimpleSelectRule(), ruleCall));

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("kind", "OnYieldExpressionBreakPoint"),
                        ReplTestUtil.coloredKeyValue("enabled", "true"),
                        ReplTestUtil.coloredKeyValue("count down", "-1"),
                        ReplTestUtil.coloredKeyValue("shorthand", "rulecall"),
                        ReplTestUtil.coloredKeyValue("location", "end"),
                        ReplTestUtil.coloredKeyValue("expression", "exp1")
                ),
                ReplTestUtil.coloredKeyValue("paused in", "Test worker at") + " " + ReplTestUtil.coloredKeyValue("tick", "4"),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "rulecall"),
                        ReplTestUtil.coloredKeyValue("location", "end"),
                        ReplTestUtil.coloredKeyValue("description", "transform rule call"),
                        ReplTestUtil.coloredKeyValue("root", debugger.nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("group", debugger.nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("expression", debugger.nameForObject(exp)),
                        ReplTestUtil.coloredKeyValue("rule", "ImplementSimpleSelectRule")
                )
        );
    }

    @Test
    void testOnYieldMatchBreakPoint() throws IOException {
        outIn.write(("break yield match idx1\nbreak list\ncont\ncurrent\ncont\n").getBytes(StandardCharsets.UTF_8));

        final var matchCandidate = new ValueIndexScanMatchCandidate(
                new Index("idx1", EmptyKeyExpression.EMPTY),
                Collections.emptyList(),
                Traversal.withRoot(Reference.empty()),
                Collections.emptyList(),
                Type.Record.erased(),
                CorrelationIdentifier.of("idx1"),
                Collections.emptyList(),
                Collections.emptyList(),
                EmptyKeyExpression.EMPTY,
                EmptyKeyExpression.EMPTY
        );
        debugger.onQuery("SELECT * FROM A", new PlanContext() {
            @Nonnull
            @Override
            public RecordQueryPlannerConfiguration getPlannerConfiguration() {
                return RecordQueryPlannerConfiguration.defaultPlannerConfiguration();
            }

            @Nonnull
            @Override
            public Set<MatchCandidate> getMatchCandidates() {
                return Set.of(matchCandidate);
            }
        });

        final var exp = new SelectExpression(LiteralValue.ofScalar(1), Collections.emptyList(), Collections.emptyList());
        final var ref = Reference.initialOf(exp);
        final var ruleCall = new CascadesRuleCall(PlannerPhase.PLANNING, PlanContext.EMPTY_CONTEXT,
                new ImplementSimpleSelectRule(), ref, Traversal.withRoot(ref), new ArrayDeque<>(),
                PlannerBindings.empty(), EvaluationContext.EMPTY);
        ruleCall.yieldPartialMatch(AliasMap.emptyMap(), matchCandidate, exp, ref, new DummyMatchInfo());

        PlannerEventListeners.dispatchEvent(() -> new InitiatePhasePlannerEvent(
                PlannerPhase.REWRITING, Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN));
        PlannerEventListeners.dispatchEvent(() ->
                new TransformRuleCallPlannerEvent(PlannerPhase.REWRITING, ref, new ArrayDeque<>(),
                        PlannerEvent.Location.END, ref, exp, new ImplementSimpleSelectRule(), ruleCall));

        terminal.writer().close();
        assertThat(outputStream.toString()).contains(
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("kind", "OnYieldMatchBreakPoint"),
                        ReplTestUtil.coloredKeyValue("enabled", "true"),
                        ReplTestUtil.coloredKeyValue("count down", "-1"),
                        ReplTestUtil.coloredKeyValue("shorthand", "rulecall"),
                        ReplTestUtil.coloredKeyValue("location", "end"),
                        ReplTestUtil.coloredKeyValue("candidate", "idx1")
                ),
                ReplTestUtil.coloredKeyValue("paused in", "Test worker at") + " " + ReplTestUtil.coloredKeyValue("tick", "1"),
                String.join(
                        "; ",
                        ReplTestUtil.coloredKeyValue("shorthand", "rulecall"),
                        ReplTestUtil.coloredKeyValue("location", "end"),
                        ReplTestUtil.coloredKeyValue("description", "transform rule call"),
                        ReplTestUtil.coloredKeyValue("root", debugger.nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("group", debugger.nameForObject(ref)),
                        ReplTestUtil.coloredKeyValue("expression", debugger.nameForObject(exp)),
                        ReplTestUtil.coloredKeyValue("rule", "ImplementSimpleSelectRule")
                )
        );
    }

    private static class DummyCascadesTask implements CascadesPlanner.Task {
        @Nonnull
        @Override
        public PlannerPhase getPlannerPhase() {
            return PlannerPhase.PLANNING;
        }

        @Override
        public void execute() {
        }

        @Override
        public PlannerEvent toTaskEvent(final PlannerEvent.Location location) {
            return new ExecutingTaskPlannerEvent(Reference.empty(), new ArrayDeque<>(), PlannerEvent.Location.BEGIN, this);
        }
    }

    private static class DummyMatchInfo implements MatchInfo {
        @Nonnull
        @Override
        public List<OrderingPart.MatchedOrderingPart> getMatchedOrderingParts() {
            return List.of();
        }

        @Nonnull
        @Override
        public MaxMatchMap getMaxMatchMap() {
            return null;
        }

        @Override
        public boolean isAdjusted() {
            return false;
        }

        @Nonnull
        @Override
        public RegularMatchInfo getRegularMatchInfo() {
            return null;
        }

        @Nonnull
        @Override
        public Map<QueryPredicate, PredicateMultiMap.PredicateMapping> collectPulledUpPredicateMappings(@Nonnull final RelationalExpression candidateExpression, @Nonnull final Set<QueryPredicate> interestingPredicates) {
            return Map.of();
        }

        @Nonnull
        @Override
        public GroupByMappings getGroupByMappings() {
            return GroupByMappings.empty();
        }
    }
}
