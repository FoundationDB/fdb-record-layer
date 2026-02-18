/*
 * PlannerEventSerializationTests.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Traversal;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PAbstractPlannerEventWithState;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PExpression;
import com.apple.foundationdb.record.query.plan.cascades.events.eventprotos.PReference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementSimpleSelectRule;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for verifying that PlannerEvent classes correctly serialize their state to protobuf via toEventProto().
 */
class PlannerEventSerializationTests {

    private RelationalExpression rootExpression;
    private Reference rootReference;
    private Deque<CascadesPlanner.Task> taskStack;
    private ImplementSimpleSelectRule testRule;

    @BeforeEach
    void setUp() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withoutSanityChecks());
        Debugger.setup();
        Debugger.withDebugger(d -> d.onQuery("SELECT * FROM A", PlanContext.EMPTY_CONTEXT));

        rootExpression = new SelectExpression(
                LiteralValue.ofScalar("test"), Collections.emptyList(), Collections.emptyList());
        rootReference = Reference.initialOf(rootExpression);
        taskStack = new ArrayDeque<>();
        testRule = new ImplementSimpleSelectRule();
    }

    @AfterAll
    static void tearDown() {
        Debugger.setDebugger(null);
    }

    @Test
    void testInitiatePhasePlannerEventToEventProto() {
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new InitiatePhasePlannerEvent(plannerPhase, rootReference, taskStack, location);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasInitiatePhasePlannerEvent()).isTrue();
        final var eventFromProto = plannerEventProto.getInitiatePhasePlannerEvent().getSuper();
        assertEventWithStateHasExpectedElements(eventFromProto, location);
    }


    @Test
    void testAdjustMatchPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new AdjustMatchPlannerEvent(
                plannerPhase, rootReference, taskStack, location, currentGroupReference, currentExpression);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasAdjustMatchPlannerEvent()).isTrue();
        final var adjustMatchPlannerEventProto = plannerEventProto.getAdjustMatchPlannerEvent();
        final var plannerEventWithStateProto = adjustMatchPlannerEventProto.getSuper();
        assertReferenceMatches(adjustMatchPlannerEventProto.getCurrentGroupReference(), currentGroupReference, currentExpression);
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
    }

    @Test
    void testExecutingTaskPlannerEventToEventProto() {
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new ExecutingTaskPlannerEvent(
                rootReference,
                taskStack,
                location,
                new CascadesPlanner.Task() {
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
                        return null;
                    }
                }
        );

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasExecutingTaskPlannerEvent()).isTrue();
        final var plannerEventWithStateProto = plannerEventProto.getExecutingTaskPlannerEvent().getSuper();
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
    }

    @Test
    void testExploreExpressionPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new ExploreExpressionPlannerEvent(
                plannerPhase, rootReference, taskStack, location, currentGroupReference, currentExpression);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasExploreExpressionPlannerEvent()).isTrue();
        final var exploreExpressionPlannerEventProto = plannerEventProto.getExploreExpressionPlannerEvent();
        final var plannerEventWithStateProto = exploreExpressionPlannerEventProto.getSuper();
        assertReferenceMatches(exploreExpressionPlannerEventProto.getCurrentGroupReference(), currentGroupReference, currentExpression);
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
    }

    @Test
    void testExploreGroupPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new ExploreGroupPlannerEvent(
                plannerPhase, rootReference, taskStack, location, currentGroupReference);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasExploreGroupPlannerEvent()).isTrue();
        final var exploreGroupPlannerEventProto = plannerEventProto.getExploreGroupPlannerEvent();
        final var plannerEventWithStateProto = exploreGroupPlannerEventProto.getSuper();
        assertReferenceMatches(exploreGroupPlannerEventProto.getCurrentGroupReference(), currentGroupReference, currentExpression);
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
    }

    @Test
    void testInsertIntoMemoPlannerEventToEventProto() {
        final var newExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var reusedExpression = rootExpression;
        final var reusedReference = rootReference;
        final var insertIntoMemoEventWithNewExpression =
                InsertIntoMemoPlannerEvent.newExp(newExpression);
        final var insertIntoMemoEventWithReusedExpression =
                InsertIntoMemoPlannerEvent.reusedExpWithReferences(reusedExpression, ImmutableList.of(reusedReference));

        assertThat(insertIntoMemoEventWithNewExpression.toEventProto().hasInsertIntoMemoPlannerEvent()).isTrue();
        assertThat(insertIntoMemoEventWithReusedExpression.toEventProto().hasInsertIntoMemoPlannerEvent()).isTrue();

        final var insertIntoMemoEventWithNewExpressionProto =
                insertIntoMemoEventWithNewExpression.toEventProto().getInsertIntoMemoPlannerEvent();
        final var insertIntoMemoEventWithReusedExpressionProto =
                insertIntoMemoEventWithReusedExpression.toEventProto().getInsertIntoMemoPlannerEvent();

        assertThat(insertIntoMemoEventWithNewExpressionProto.getLocation()).isEqualTo(PlannerEvent.Location.NEW.toString());
        assertThat(insertIntoMemoEventWithReusedExpressionProto.getLocation()).isEqualTo(PlannerEvent.Location.REUSED.toString());

        assertExpressionMatches(insertIntoMemoEventWithNewExpressionProto.getExpression(), newExpression);
        assertExpressionMatches(insertIntoMemoEventWithReusedExpressionProto.getExpression(), reusedExpression);

        assertThat(insertIntoMemoEventWithNewExpressionProto.getReusedExpressionReferencesList()).isEmpty();
        assertThat(insertIntoMemoEventWithReusedExpressionProto.getReusedExpressionReferencesList()).singleElement()
                .satisfies(actualReference -> assertReferenceMatches(actualReference, reusedReference, reusedExpression));
    }

    @Test
    void testOptimizeGroupPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new OptimizeGroupPlannerEvent(
                plannerPhase, rootReference, taskStack, location, currentGroupReference);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasOptimizeGroupPlannerEvent()).isTrue();
        final var optimizeGroupPlannerEventProto = plannerEventProto.getOptimizeGroupPlannerEvent();
        final var plannerEventWithStateProto = optimizeGroupPlannerEventProto.getSuper();
        assertReferenceMatches(optimizeGroupPlannerEventProto.getCurrentGroupReference(), currentGroupReference, currentExpression);
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
    }

    @Test
    void testOptimizeInputsPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new OptimizeInputsPlannerEvent(
                plannerPhase, rootReference, taskStack, location, currentGroupReference, currentExpression);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasOptimizeInputsPlannerEvent()).isTrue();
        final var optimizeInputsPlannerEventProto = plannerEventProto.getOptimizeInputsPlannerEvent();
        final var plannerEventWithStateProto = optimizeInputsPlannerEventProto.getSuper();
        assertReferenceMatches(optimizeInputsPlannerEventProto.getCurrentGroupReference(), currentGroupReference, currentExpression);
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
    }

    @Test
    void testTransformPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new TransformPlannerEvent(
                plannerPhase, rootReference, taskStack, location, currentGroupReference, currentExpression, testRule);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasTransformPlannerEvent()).isTrue();
        final var transformPlannerEventProto = plannerEventProto.getTransformPlannerEvent();
        final var plannerEventWithStateProto = transformPlannerEventProto.getSuper();
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
        assertReferenceMatches(transformPlannerEventProto.getCurrentGroupReference(), currentGroupReference, currentExpression);
        assertExpressionMatches(transformPlannerEventProto.getBindable().getExpression(), currentExpression);
        assertThat(transformPlannerEventProto.getRule()).isEqualTo(testRule.toString());
    }

    @Test
    void testTransformRuleCallPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var plannerPhase = PlannerPhase.PLANNING;
        final var location = PlannerEvent.Location.BEGIN;
        final var testRuleCall = new CascadesRuleCall(
                PlannerPhase.PLANNING, PlanContext.EMPTY_CONTEXT, testRule, rootReference, Traversal.withRoot(rootReference),
                taskStack, PlannerBindings.empty(), EvaluationContext.EMPTY);
        final var plannerEvent = new TransformRuleCallPlannerEvent(plannerPhase, rootReference, taskStack,
                location, currentGroupReference, currentExpression, testRule, testRuleCall);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasTransformRuleCallPlannerEvent()).isTrue();
        final var transformRuleCallPlannerEventProto = plannerEventProto.getTransformRuleCallPlannerEvent();
        final var plannerEventWithStateProto = transformRuleCallPlannerEventProto.getSuper();
        assertEventWithStateHasExpectedElements(plannerEventWithStateProto, location);
        assertReferenceMatches(transformRuleCallPlannerEventProto.getCurrentGroupReference(), currentGroupReference, currentExpression);
        assertExpressionMatches(transformRuleCallPlannerEventProto.getBindable().getExpression(), currentExpression);
        assertThat(transformRuleCallPlannerEventProto.getRule()).isEqualTo(testRule.toString());
    }

    @Test
    void testTranslateCorrelationsPlannerEventToEventProto() {
        final var currentExpression = new SelectExpression(
                LiteralValue.ofScalar("42"), Collections.emptyList(), Collections.emptyList());
        final var currentGroupReference = Reference.initialOf(currentExpression);
        final var location = PlannerEvent.Location.BEGIN;
        final var plannerEvent = new TranslateCorrelationsPlannerEvent(currentExpression, location);

        final var plannerEventProto = plannerEvent.toEventProto();

        assertThat(plannerEventProto.hasTranslateCorrelationsPlannerEvent()).isTrue();
        final var transformRuleCallPlannerEventProto =
                plannerEventProto.getTranslateCorrelationsPlannerEvent();
        assertThat(transformRuleCallPlannerEventProto.getLocation()).isEqualTo(location.toString());
        assertExpressionMatches(transformRuleCallPlannerEventProto.getExpression(), currentExpression);
    }

    private void assertEventWithStateHasExpectedElements(final PAbstractPlannerEventWithState eventFromProto,
                                                         final PlannerEvent.Location location) {
        assertThat(eventFromProto.getLocation()).isEqualTo(location.toString());
        assertReferenceMatches(eventFromProto.getRootReference(), rootReference, rootExpression);
    }

    private void assertReferenceMatches(final PReference actualReference,
                                        final Reference expectedReference,
                                        final RelationalExpression expectedExpression) {
        assertThat(actualReference.getName())
                .isEqualTo(Objects.requireNonNull(Debugger.getDebugger()).nameForObject(expectedReference));
        assertThat(actualReference.getExpressionsList()).hasSize(1).singleElement()
                .satisfies(expFromProto -> assertExpressionMatches(expFromProto, expectedExpression));
    }

    private void assertExpressionMatches(final PExpression actualExpressionProto,
                                         final RelationalExpression expectedExpression) {
        assertThat(actualExpressionProto.getSemanticHashCode()).isEqualTo(expectedExpression.semanticHashCode());
        assertThat(actualExpressionProto.getName())
                .isEqualTo(Objects.requireNonNull(Debugger.getDebugger()).nameForObject(expectedExpression));
    }
}
