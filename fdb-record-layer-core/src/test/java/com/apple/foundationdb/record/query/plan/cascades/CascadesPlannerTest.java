/*
 * CascadesPlannerTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CascadesPlanner}.
 *
 * <p>The tests drive the planner’s rule dispatching logic directly, by constructing and executing planner tasks
 * directly (as opposed to going through a query).
 */
class CascadesPlannerTest {
    /**
     * If the first inner rule of a {@link ConditionalCascadesRule} makes progress, the remaining rules are not tried.
     */
    @Test
    void testConditionalCascadesRule1() {
        final RelationalExpression expression = scanExpression("A");
        final Reference group = Reference.initialOf(expression);
        final CascadesPlanner planner = newPlanner();

        final RecordingExplorationCascadesRule first = new RecordingExplorationCascadesRule(true);
        final RecordingExplorationCascadesRule second = new RecordingExplorationCascadesRule(true);
        final CascadesPlanner.ConditionalTransformExpression task = planner.new ConditionalTransformExpression(
                PlannerPhase.REWRITING, group, expression, ImmutableList.of(first, second));

        assertThat(task.execute()).isTrue();
        assertThat(first.matchCount).isEqualTo(1);
        assertThat(second.matchCount).isEqualTo(0);
        assertThat(planner.getTaskStack()).noneMatch(CascadesPlanner.ConditionalTransformExpression.class::isInstance);
    }

    /**
     * If an inner rule of a {@link ConditionalCascadesRule} makes no progress, the next rule is tried.
     */
    @Test
    void testConditionalCascadesRule2() {
        final RelationalExpression expression = scanExpression("A");
        final Reference group = Reference.initialOf(expression);
        final CascadesPlanner planner = newPlanner();

        final RecordingExplorationCascadesRule noProgress = new RecordingExplorationCascadesRule(false);
        final RecordingExplorationCascadesRule successful = new RecordingExplorationCascadesRule(true);
        final CascadesPlanner.ConditionalTransformExpression task = planner.new ConditionalTransformExpression(
                PlannerPhase.REWRITING, group, expression, ImmutableList.of(noProgress, successful));

        assertThat(task.execute()).isFalse();
        assertThat(noProgress.matchCount).isEqualTo(1);
        assertThat(successful.matchCount).isEqualTo(0);

        final CascadesPlanner.Task followUp = planner.getTaskStack().pop();
        assertThat(followUp).isInstanceOf(CascadesPlanner.ConditionalTransformExpression.class);
        assertThat(followUp.execute()).isTrue();
        assertThat(successful.matchCount).isEqualTo(1);
    }

    /**
     * If the last (or only) inner rule of a {@link ConditionalCascadesRule} makes no progress, no follow-up task is
     * pushed.
     */
    @Test
    void testConditionalCascadesRule3() {
        final RelationalExpression expression = scanExpression("A");
        final Reference group = Reference.initialOf(expression);
        final CascadesPlanner planner = newPlanner();

        final RecordingExplorationCascadesRule onlyRule = new RecordingExplorationCascadesRule(false);
        final CascadesPlanner.ConditionalTransformExpression task = planner.new ConditionalTransformExpression(
                PlannerPhase.REWRITING, group, expression, ImmutableList.of(onlyRule));

        assertThat(task.execute()).isFalse();
        assertThat(planner.getTaskStack()).isEmpty();
    }

    /**
     * The inner rules of a {@link ConditionalCascadesRule} that are disabled via the planner configuration are filtered
     * out.
     */
    @Test
    void testConditionalCascadesRule4() {
        final RelationalExpression expression = scanExpression("A");
        final Reference group = Reference.initialOf(expression);
        final CascadesPlanner planner = newPlanner();
        planner.setConfiguration(planner.getConfiguration().asBuilder()
                .disableTransformationRule(RecordingExplorationCascadesRule.class)
                .build());

        final RecordingExplorationCascadesRule disabled = new RecordingExplorationCascadesRule(true);
        final RecordingExplorationCascadesRule enabled = new OtherRecordingExplorationCascadesRule(true);
        // Make `disabled` the *first* inner rule. If it weren't filtered out, it would match and short-circuit the
        // chain before `enabled` (listed second) is ever reached, since `disabled` also yields on a match. So this
        // ordering is what actually distinguishes “filtered out” from “chain just moved on past it”.
        final ConditionalCascadesRule<RelationalExpression, RecordingExplorationCascadesRule> conditionalRule =
                new ConditionalCascadesRule<>(ImmutableList.of(disabled, enabled));
        final CascadesPlanner.ExploreExpression explore =
                planner.new ExploreExpression(PlannerPhase.REWRITING, group, expression);

        explore.pushTransformTaskMaybe(conditionalRule);

        final CascadesPlanner.Task pushedTask = planner.getTaskStack().pop();
        assertThat(pushedTask).isInstanceOf(CascadesPlanner.ConditionalTransformExpression.class);
        assertThat(pushedTask.execute()).isTrue();
        assertThat(enabled.matchCount).isEqualTo(1);
        assertThat(disabled.matchCount).isEqualTo(0);
    }

    /**
     * If {@code shouldExecute()} returns {@code false} for the group/expression pair, no rule is tried and no follow-up
     * task is pushed, even though more than one rule remains.
     */
    @Test
    void testConditionalCascadesRule5() {
        final Reference group = Reference.initialOf(scanExpression("A"));
        final RelationalExpression expressionNotInGroup = scanExpression("C");
        final CascadesPlanner planner = newPlanner();

        final RecordingExplorationCascadesRule first = new RecordingExplorationCascadesRule(true);
        final RecordingExplorationCascadesRule second = new RecordingExplorationCascadesRule(true);
        final CascadesPlanner.ConditionalTransformExpression task = planner.new ConditionalTransformExpression(
                PlannerPhase.REWRITING, group, expressionNotInGroup, ImmutableList.of(first, second));

        assertThat(task.execute()).isFalse();
        assertThat(first.matchCount).isEqualTo(0);
        assertThat(second.matchCount).isEqualTo(0);
        assertThat(planner.getTaskStack()).isEmpty();
    }

    /**
     * On re-exploration, only the inner rules of a {@link ConditionalCascadesRule} whose own constraint dependencies
     * are actually stale get pushed into the chain; inner rules that are not sensitive to the constraint that changed
     * are filtered out entirely, rather than being tried (and failing to match anything new) regardless.
     */
    @Test
    void testConditionalCascadesRule6() {
        final RelationalExpression expression = scanExpression("A");
        final Reference group = Reference.initialOf(expression);
        final CascadesPlanner planner = newPlanner();

        // Simulate a group that has already been fully explored and committed once.
        group.setExplored();
        // Push a new value for CONSTRAINT_A only; CONSTRAINT_B is left untouched, so only CONSTRAINT_A is stale
        // relative to the committed exploration.
        group.getConstraintsMap().pushProperty(CONSTRAINT_A, new Object());
        group.startExploration();

        final RecordingExplorationCascadesRule staleRule =
                new RecordingExplorationCascadesRule(true, ImmutableSet.<PlannerConstraint<?>>of(CONSTRAINT_A));
        final RecordingExplorationCascadesRule notStaleRule =
                new RecordingExplorationCascadesRule(true, ImmutableSet.<PlannerConstraint<?>>of(CONSTRAINT_B));
        // `notStaleRule` is listed *first*: if it weren't filtered out, it would match and short-circuit the chain
        // before `staleRule` (listed second) is ever reached, since `notStaleRule` also yields on a match. So this
        // ordering is what actually distinguishes "filtered out" from "chain just moved on past it".
        final ConditionalCascadesRule<RelationalExpression, RecordingExplorationCascadesRule> conditionalRule =
                new ConditionalCascadesRule<>(ImmutableList.of(notStaleRule, staleRule));
        // `ReExploreExpression` itself is not visible here; this reproduces its `shouldPushRule()` logic (staleness
        // with respect to the group's committed exploration) on top of the otherwise-testable base class.
        final CascadesPlanner.AbstractExploreExpression reExplore =
                planner.new AbstractExploreExpression(PlannerPhase.REWRITING, group, expression) {
                    @Override
                    protected boolean shouldPushRule(@Nonnull final CascadesRule<?> rule) {
                        return group.isFullyExploring() || !group.isExploredForAttributes(rule.getConstraintDependencies());
                    }
                };

        reExplore.pushTransformTaskMaybe(conditionalRule);

        final CascadesPlanner.Task pushedTask = planner.getTaskStack().pop();
        assertThat(pushedTask).isInstanceOf(CascadesPlanner.ConditionalTransformExpression.class);
        assertThat(pushedTask.execute()).isTrue();
        assertThat(staleRule.matchCount).isEqualTo(1);
        assertThat(notStaleRule.matchCount).isEqualTo(0);
        assertThat(planner.getTaskStack()).noneMatch(CascadesPlanner.ConditionalTransformExpression.class::isInstance);
    }

    /**
     * A stub {@link PlannerConstraint} used to exercise the per-inner-rule staleness filtering in
     * {@link CascadesPlanner.AbstractExploreExpression#pushTransformTaskMaybe}. Its {@code combine()} method is never
     * invoked by these tests; identity is all that matters.
     */
    private static final PlannerConstraint<Object> CONSTRAINT_A = new PlannerConstraint<Object>() {
        @Nonnull
        @Override
        public Optional<Object> combine(@Nonnull final Object currentConstraint, @Nonnull final Object newConstraint) {
            return Optional.empty();
        }
    };

    private static final PlannerConstraint<Object> CONSTRAINT_B = new PlannerConstraint<Object>() {
        @Nonnull
        @Override
        public Optional<Object> combine(@Nonnull final Object currentConstraint, @Nonnull final Object newConstraint) {
            return Optional.empty();
        }
    };

    /**
     * Creates a {@link CascadesPlanner} over an empty {@link TestRecords1Proto}-based store.
     */
    @Nonnull
    private static CascadesPlanner newPlanner() {
        final RecordMetaData metaData =
                RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor()).getRecordMetaData();
        return new CascadesPlanner(metaData, new RecordStoreState(null, null), IndexMaintainerFactoryRegistryImpl.instance());
    }

    /**
     * Creates a leaf {@link RelationalExpression} for the given record type, with no quantifiers.
     */
    @Nonnull
    private static FullUnorderedScanExpression scanExpression(@Nonnull final String recordType) {
        return new FullUnorderedScanExpression(ImmutableSet.of(recordType), new Type.AnyRecord(false), new AccessHints());
    }

    /**
     * A minimal {@link ExplorationCascadesRule} for testing purposes. Matches any {@link FullUnorderedScanExpression},
     * records how many times it was matched, and either yields a new expression (signaling progress) or does nothing,
     * depending on how it was constructed.
     */
    private static class RecordingExplorationCascadesRule
            extends AbstractCascadesRule<RelationalExpression>
            implements ExplorationCascadesRule<RelationalExpression> {
        private final boolean shouldYield;
        private int matchCount;

        RecordingExplorationCascadesRule(final boolean shouldYield) {
            this(shouldYield, ImmutableSet.of());
        }

        RecordingExplorationCascadesRule(final boolean shouldYield, @Nonnull final Set<PlannerConstraint<?>> constraintDependencies) {
            super(matcher(), constraintDependencies);
            this.shouldYield = shouldYield;
        }

        @Override
        public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
            matchCount++;
            if (shouldYield) {
                call.yieldExploratoryExpression(scanExpression("B"));
            }
        }

        @Nonnull
        @SuppressWarnings("unchecked")
        private static BindingMatcher<RelationalExpression> matcher() {
            return (BindingMatcher<RelationalExpression>)(BindingMatcher<?>)
                    RelationalExpressionMatchers.ofType(FullUnorderedScanExpression.class);
        }
    }

    /**
     * A rule identical to {@link RecordingExplorationCascadesRule} except for its class, so that it can be disabled
     * independently via {@code RecordQueryPlannerConfiguration}, which disables rules by their simple class name.
     */
    private static final class OtherRecordingExplorationCascadesRule extends RecordingExplorationCascadesRule {
        OtherRecordingExplorationCascadesRule(final boolean shouldYield) {
            super(shouldYield);
        }
    }
}
