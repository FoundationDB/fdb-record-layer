/*
 * Processors.java
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

import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.events.AdjustMatchPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.ExecutingTaskPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.ExploreExpressionPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.ExploreGroupPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.InitiatePhasePlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.InsertIntoMemoPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.OptimizeGroupPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.OptimizeInputsPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformRuleCallPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
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
    public interface Processor<E extends PlannerEvent> {
        default void onCallback(final PlannerRepl plannerRepl, final E event) {
            onList(plannerRepl, event);
        }

        default void onCommand(final PlannerRepl plannerRepl, final E event, final ParsedLine parsedLine) {
            plannerRepl.printlnError("unknown command or syntax error: " + String.join(" ", parsedLine.words()));
            onList(plannerRepl, event);
            plannerRepl.println();
        }

        void onList(PlannerRepl plannerRepl, E event);

        void onDetail(PlannerRepl plannerRepl, E event);

        Class<E> getEventType();
    }

    /**
     * Processor for {@link ExecutingTaskPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class ExecutingTaskProcessor implements Processor<ExecutingTaskPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final ExecutingTaskPlannerEvent event) {
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("kind", event.getTask().getClass().getSimpleName());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExecutingTaskPlannerEvent event) {
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("kind", event.getTask().getClass().getSimpleName() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()));
        }

        @Override
        public Class<ExecutingTaskPlannerEvent> getEventType() {
            return ExecutingTaskPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link OptimizeGroupPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class OptimizeGroupProcessor implements Processor<OptimizeGroupPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final OptimizeGroupPlannerEvent event) {
            plannerRepl.printlnKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final OptimizeGroupPlannerEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()));
        }

        @Override
        public Class<OptimizeGroupPlannerEvent> getEventType() {
            return OptimizeGroupPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link ExploreExpressionPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class ExploreExpressionProcessor implements Processor<ExploreExpressionPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final ExploreExpressionPlannerEvent event) {
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
        public void onList(final PlannerRepl plannerRepl, final ExploreExpressionPlannerEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()));
        }

        @Override
        public Class<ExploreExpressionPlannerEvent> getEventType() {
            return ExploreExpressionPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link ExploreGroupPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class ExploreGroupProcessor implements Processor<ExploreGroupPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final ExploreGroupPlannerEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("current root reference", "");
            plannerRepl.printlnReference(event.getRootReference(), "  ");
            plannerRepl.printlnKeyValue("current group reference", "");
            plannerRepl.printlnReference(event.getCurrentReference(), "  ");
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final ExploreGroupPlannerEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()));
        }

        @Override
        public Class<ExploreGroupPlannerEvent> getEventType() {
            return ExploreGroupPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link TransformPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class TransformProcessor implements Processor<TransformPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final TransformPlannerEvent event) {
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
        public void onList(final PlannerRepl plannerRepl, final TransformPlannerEvent event) {
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
        public Class<TransformPlannerEvent> getEventType() {
            return TransformPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link TransformRuleCallPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class TransformRuleCallProcessor implements Processor<TransformRuleCallPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final TransformRuleCallPlannerEvent event) {
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

            if (event.getLocation() == PlannerEvent.Location.END) {
                plannerRepl.printlnKeyValue("yield", "");
                for (final RelationalExpression newExpression :
                        Iterables.concat(ruleCall.getNewFinalExpressions(), ruleCall.getNewExploratoryExpressions())) {
                    plannerRepl.printlnExpression(newExpression, "    ");
                    plannerRepl.println();
                }
            }
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final TransformRuleCallPlannerEvent event) {
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
        public Class<TransformRuleCallPlannerEvent> getEventType() {
            return TransformRuleCallPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link AdjustMatchPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class AdjustMatchProcessor implements Processor<AdjustMatchPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final AdjustMatchPlannerEvent event) {
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
        public void onList(final PlannerRepl plannerRepl, final AdjustMatchPlannerEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()));
        }

        @Override
        public Class<AdjustMatchPlannerEvent> getEventType() {
            return AdjustMatchPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link OptimizeInputsPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class OptimizeInputsProcessor implements Processor<OptimizeInputsPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final OptimizeInputsPlannerEvent event) {
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
        public void onList(final PlannerRepl plannerRepl, final OptimizeInputsPlannerEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("description", event.getDescription() + "; ");
            plannerRepl.printKeyValue("root", plannerRepl.nameForObjectOrNotInCache(event.getRootReference()) + "; ");
            plannerRepl.printKeyValue("group", plannerRepl.nameForObjectOrNotInCache(event.getCurrentReference()) + "; ");
            plannerRepl.printKeyValue("expression", plannerRepl.nameForObjectOrNotInCache(event.getExpression()) + "; ");
        }

        @Override
        public Class<OptimizeInputsPlannerEvent> getEventType() {
            return OptimizeInputsPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link InsertIntoMemoPlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class InsertMemoProcessor implements Processor<InsertIntoMemoPlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final InsertIntoMemoPlannerEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final InsertIntoMemoPlannerEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
        }

        @Override
        public Class<InsertIntoMemoPlannerEvent> getEventType() {
            return InsertIntoMemoPlannerEvent.class;
        }
    }

    /**
     * Processor for {@link InitiatePhasePlannerEvent}.
     */
    @AutoService(Processor.class)
    public static class InitiatePlannerPhaseProcessor implements Processor<InitiatePhasePlannerEvent> {
        @Override
        public void onDetail(final PlannerRepl plannerRepl, final InitiatePhasePlannerEvent event) {
            plannerRepl.printlnKeyValue("event", event.getShorthand().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT));
            plannerRepl.printlnKeyValue("description", event.getDescription());
            plannerRepl.printlnKeyValue("planner phase", event.getPlannerPhase().name().toLowerCase(Locale.ROOT));
        }

        @Override
        public void onList(final PlannerRepl plannerRepl, final InitiatePhasePlannerEvent event) {
            plannerRepl.printKeyValue("shorthand", event.getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", event.getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("planner phase", event.getPlannerPhase().name().toLowerCase(Locale.ROOT));
        }

        @Override
        public Class<InitiatePhasePlannerEvent> getEventType() {
            return InitiatePhasePlannerEvent.class;
        }
    }
}
