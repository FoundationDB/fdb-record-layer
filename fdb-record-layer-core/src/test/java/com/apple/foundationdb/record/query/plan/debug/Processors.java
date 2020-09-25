/*
 * Processors.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.debug;

import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.ExecutingTaskEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.ExploreExpressionEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.ExploreGroupEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.MatchExpressionEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.MatchExpressionWithCandidateEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.OptimizeGroupEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.OptimizeInputsEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.TransformEvent;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.TransformRuleCallEvent;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import org.jline.reader.ParsedLine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class containing all implementations of {@link Processor}
 * in a <em>sealed trait</em> style (except it's not sealed).
 */
public class Processors {

    /**
     * Event-typed processor.
     * @param <E> the type of event
     */
    public interface Processor<E extends Debugger.Event> {
        default void onCallback(final PlannerRepl plannerRepl, final E event) {
            onList(plannerRepl, event);
        }

        default void onCommand(final PlannerRepl plannerRepl, final E event, final ParsedLine parsedLine) {
            plannerRepl.printlnError("unknown command or syntax error: " + parsedLine.toString());
            onList(plannerRepl, event);
            plannerRepl.println();
        }

        void onList(final PlannerRepl plannerRepl, final E event);

        void onDetail(final PlannerRepl plannerRepl, final E event);

        Class<E> getEventType();
    }

    /**
     * Processor for {@link ExecutingTaskEvent}.
     */
    @AutoService(Processor.class)
    public static class ExecutingTaskProcessor implements Processor<ExecutingTaskEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final ExecutingTaskEvent event) {
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("shorthand", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("kind", event.getTask().getClass().getSimpleName());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExecutingTaskEvent event) {
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("kind", event.getTask().getClass().getSimpleName() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()));
        }

        @Override
        public Class<ExecutingTaskEvent> getEventType() {
            return ExecutingTaskEvent.class;
        }
    }

    /**
     * Processor for {@link OptimizeGroupEvent}.
     */
    @AutoService(Processor.class)
    public static class OptimizeGroupProcessor implements Processor<OptimizeGroupEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final OptimizeGroupEvent event) {
            plannerRepl.printlnKeyValue("shorthand", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final OptimizeGroupEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()));
        }

        @Override
        public Class<OptimizeGroupEvent> getEventType() {
            return OptimizeGroupEvent.class;
        }
    }

    /**
     * Processor for {@link ExploreExpressionEvent}.
     */
    @AutoService(Processor.class)
    public static class ExploreExpressionProcessor implements Processor<ExploreExpressionEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final ExploreExpressionEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
            plannerRepl.printlnExpression(event.getExpression(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExploreExpressionEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()));
        }

        @Override
        public Class<ExploreExpressionEvent> getEventType() {
            return ExploreExpressionEvent.class;
        }
    }

    /**
     * Processor for {@link ExploreGroupEvent}.
     */
    @AutoService(Processor.class)
    public static class ExploreGroupProcessor implements Processor<ExploreGroupEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final ExploreGroupEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExploreGroupEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()));
        }

        @Override
        public Class<ExploreGroupEvent> getEventType() {
            return ExploreGroupEvent.class;
        }
    }

    /**
     * Processor for {@link TransformEvent}.
     */
    @AutoService(Processor.class)
    public static class TransformProcessor implements Processor<TransformEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final TransformEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
            plannerRepl.printlnKeyValue("rule", event.getRule().toString());
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final TransformEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()) + "; ");
            plannerRepl.printKeyValue("rule", event.getRule().toString());
        }

        @Override
        public Class<TransformEvent> getEventType() {
            return TransformEvent.class;
        }
    }

    /**
     * Processor for {@link TransformRuleCallEvent}.
     */
    @AutoService(Processor.class)
    public static class TransformRuleCallProcessor implements Processor<TransformRuleCallEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final TransformRuleCallEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
            plannerRepl.printlnKeyValue("rule", event.getRule().toString());
            plannerRepl.printlnKeyValue("bindings", "");
            final CascadesRuleCall ruleCall = event.getRuleCall();
            final ImmutableListMultimap<ExpressionMatcher<? extends Bindable>, Bindable> bindings = ruleCall.getBindings().asMultiMap();
            for (final Map.Entry<ExpressionMatcher<? extends Bindable>, Collection<Bindable>> entry : bindings.asMap().entrySet()) {
                plannerRepl.printlnKeyValue("  " + entry.getKey().getClass().getSimpleName(), "");
                for (Bindable bindable : entry.getValue()) {
                    plannerRepl.println("    " + plannerRepl.nameForObjectOrNotInCache(bindable));
                }
            }

            if (event.getLocation() == Debugger.Location.END) {
                plannerRepl.printlnKeyValue("yield", "");
                for (final RelationalExpression newExpression : ruleCall.getNewExpressions()) {
                    plannerRepl.printlnExpression(newExpression, "    ");
                    plannerRepl.println();
                }
            }
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final TransformRuleCallEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()) + "; ");
            plannerRepl.printKeyValue("rule", event.getRule().toString());
        }

        @Override
        public Class<TransformRuleCallEvent> getEventType() {
            return TransformRuleCallEvent.class;
        }
    }

    /**
     * Processor for {@link MatchExpressionEvent}.
     */
    @AutoService(Processor.class)
    public static class MatchExpressionProcessor implements Processor<MatchExpressionEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final MatchExpressionEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
            plannerRepl.printlnKeyValue("expression", "");
            plannerRepl.printlnExpression(event.getExpression(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final MatchExpressionEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()));
        }

        @Override
        public Class<MatchExpressionEvent> getEventType() {
            return MatchExpressionEvent.class;
        }
    }

    /**
     * Processor for {@link MatchExpressionWithCandidateEvent}.
     */
    @AutoService(Processor.class)
    public static class MatchExpressionWithCandidateProcessor implements Processor<MatchExpressionWithCandidateEvent> {

        public void onCommand(final PlannerRepl plannerRepl, final MatchExpressionWithCandidateEvent event, final ParsedLine parsedLine) {
            final List<String> words = parsedLine.words();

            if (words.size() >= 1) {
                final String word0 = words.get(0).toUpperCase();
                if ("MATCH".equals(word0)) {
                    if (words.size() >= 2) {
                        final String word1 = words.get(1).toUpperCase();
                        if ("SHOW".equals(word1)) {
                            if (words.size() == 3) {
                                final String word2 = words.get(1).toUpperCase();
                                if ("ALL".equals(word2)) {
                                    PlannerGraphProperty.show(true, event.getRootReference(), Objects.requireNonNull(plannerRepl.getPlanContext()).getMatchCandidates());
                                    return;
                                }
                            } else {
                                PlannerGraphProperty.show(true, event.getRootReference(), ImmutableSet.of(event.getMatchCandidate()));
                                return;
                            }
                        }
                    }
                }
            }

            Processor.super.onCommand(plannerRepl, event, parsedLine);
        }

        @Override
        public void onDetail(final PlannerRepl plannerRepl, final MatchExpressionWithCandidateEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
            plannerRepl.printlnKeyValue("expression", "");
            plannerRepl.printlnExpression(event.getExpression(), "  ");
            plannerRepl.printlnKeyValue("match candidate", event.getMatchCandidate().getName());
            plannerRepl.printlnKeyValue("candidate reference", "");
            plannerRepl.printlnReference(event.getCandidateRef(), "  ");
            plannerRepl.printlnKeyValue("candidate expression", "");
            plannerRepl.printlnExpression(event.getCandidateExpression(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final MatchExpressionWithCandidateEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()) + "; ");
            plannerRepl.printKeyValue("match candidate", event.getMatchCandidate().getName() + "; ");
            plannerRepl.printKeyValue("candidate reference", plannerRepl.nameForObjectOrNotInCache(event.getCandidateRef()) + "; ");
            plannerRepl.printKeyValue("candidate expression", plannerRepl.nameForObjectOrNotInCache(event.getCandidateExpression()));
        }

        @Override
        public Class<MatchExpressionWithCandidateEvent> getEventType() {
            return MatchExpressionWithCandidateEvent.class;
        }
    }

    /**
     * Processor for {@link OptimizeInputsEvent}.
     */
    @AutoService(Processor.class)
    public static class OptimizeInputsProcessor implements Processor<OptimizeInputsEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final OptimizeInputsEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase());
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase());
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentGroupReference(), "  ");
            plannerRepl.printlnKeyValue("expression", "");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final OptimizeInputsEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase() + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentGroupReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()) + "; ");
        }

        @Override
        public Class<OptimizeInputsEvent> getEventType() {
            return OptimizeInputsEvent.class;
        }
    }
}
