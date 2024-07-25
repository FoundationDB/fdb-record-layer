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

import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.AdjustMatchEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.ExecutingTaskEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.ExploreExpressionEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.ExploreGroupEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.InsertIntoMemoEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.OptimizeGroupEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.OptimizeInputsEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.TransformEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.TransformRuleCallEvent;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableListMultimap;
import org.jline.reader.ParsedLine;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;

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
            plannerRepl.printlnError("unknown command or syntax error: " + String.join(" ", parsedLine.words()));
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
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("kind", event.getTask().getClass().getSimpleName());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExecutingTaskEvent event) {
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
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
            plannerRepl.printlnKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final OptimizeGroupEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()));
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
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
            plannerRepl.printlnExpression(event.getExpression(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExploreExpressionEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
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
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExploreGroupEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()));
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
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
            final Object bindable = event.getBindable();
            if (bindable instanceof RelationalExpression) {
                plannerRepl.printlnExpression((RelationalExpression)bindable);
            } else {
                plannerRepl.printlnKeyValue("bindable", bindable.toString());
            }
            plannerRepl.printlnKeyValue("rule", event.getRule().toString());
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final TransformEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
            final Object bindable = event.getBindable();
            if (bindable instanceof RelationalExpression) {
                plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(bindable) + "; ");
            } else {
                plannerRepl.printKeyValue("bindable", bindable.toString() + "; ");
            }
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
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
            plannerRepl.printlnKeyValue("rule", event.getRule().toString());
            plannerRepl.printlnKeyValue("bindings", "");
            final CascadesRuleCall ruleCall = event.getRuleCall();
            final ImmutableListMultimap<BindingMatcher<?>, ?> bindings = ruleCall.getBindings().asMultiMap();
            for (final Map.Entry<BindingMatcher<?>, ? extends Collection<?>> entry : bindings.asMap().entrySet()) {
                plannerRepl.printlnKeyValue("  " + entry.getKey().getClass().getSimpleName(), "");
                for (Object bindable : entry.getValue()) {
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
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
            final Object bindable = event.getBindable();
            if (bindable instanceof RelationalExpression) {
                plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(bindable) + "; ");
            } else {
                plannerRepl.printKeyValue("bindable", bindable.toString() + "; ");
            }
            plannerRepl.printKeyValue("rule", event.getRule().toString());
        }

        @Override
        public Class<TransformRuleCallEvent> getEventType() {
            return TransformRuleCallEvent.class;
        }
    }

    /**
     * Processor for {@link AdjustMatchEvent}.
     */
    @AutoService(Processor.class)
    public static class AdjustMatchProcessor implements Processor<AdjustMatchEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final AdjustMatchEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
            plannerRepl.printlnKeyValue("expression", "");
            plannerRepl.printlnExpression(event.getExpression(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final AdjustMatchEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()));
        }

        @Override
        public Class<AdjustMatchEvent> getEventType() {
            return AdjustMatchEvent.class;
        }
    }

    /**
     * Processor for {@link OptimizeInputsEvent}.
     */
    @AutoService(Processor.class)
    public static class OptimizeInputsProcessor implements Processor<OptimizeInputsEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final OptimizeInputsEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
            plannerRepl.printlnKeyValue("expression", "");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final OptimizeInputsEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()) + "; ");
        }

        @Override
        public Class<OptimizeInputsEvent> getEventType() {
            return OptimizeInputsEvent.class;
        }
    }

    /**
     * Processor for {@link InsertIntoMemoEvent}.
     */
    @AutoService(Processor.class)
    public static class InsertMemoProcessor implements Processor<InsertIntoMemoEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final InsertIntoMemoEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final InsertIntoMemoEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
        }

        @Override
        public Class<InsertIntoMemoEvent> getEventType() {
            return InsertIntoMemoEvent.class;
        }
    }
}
