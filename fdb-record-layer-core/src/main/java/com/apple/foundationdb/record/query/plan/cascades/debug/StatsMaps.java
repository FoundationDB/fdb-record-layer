/*
 * StatsMaps.java
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

import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class StatsMaps {
    @Nonnull
    private final Map<Class<? extends Debugger.Event>, ? extends Stats> eventWithoutStateClassStatsMap;
    @Nonnull
    private final Map<PlannerPhase, Map<Class<? extends Debugger.EventWithState>, ? extends Stats>> eventWithStateClassStatsByPlannerPhaseMap;
    @Nonnull
    private final Map<Class<? extends CascadesRule<?>>, ? extends Stats> plannerRuleClassStatsMap;

    @Nonnull
    private final Supplier<Map<Class<? extends Debugger.Event>, Stats>> immutableEventClassStatsMapSupplier;
    @Nonnull
    private final Supplier<Map<Class<? extends Debugger.Event>, Stats>> immutableEventWithoutStateClassStatsMapSupplier;
    @Nonnull
    private final Supplier<Map<PlannerPhase, Map<Class<? extends Debugger.EventWithState>, Stats>>> immutableEventWithStateClassStatsByPlannerPhaseMapSupplier;
    @Nonnull
    private final Supplier<Map<Class<? extends CascadesRule<?>>, Stats>> immutablePlannerRuleClassStatsMapSupplier;


    public StatsMaps(@Nonnull final Map<Class<? extends Debugger.Event>, ? extends Stats> eventWithoutStateClassStatsMap,
                     @Nonnull final Map<PlannerPhase, Map<Class<? extends Debugger.EventWithState>, ? extends Stats>> eventWithStateClassStatsByPlannerPhaseMap,
                     @Nonnull final Map<Class<? extends CascadesRule<?>>, ? extends Stats> plannerRuleClassStatsMap) {

        this.eventWithoutStateClassStatsMap = eventWithoutStateClassStatsMap;
        this.eventWithStateClassStatsByPlannerPhaseMap = eventWithStateClassStatsByPlannerPhaseMap;
        this.plannerRuleClassStatsMap = plannerRuleClassStatsMap;

        this.immutableEventClassStatsMapSupplier = Suppliers.memoize(this::computeImmutableEventClassStatsMap);
        this.immutableEventWithoutStateClassStatsMapSupplier = Suppliers.memoize(this::computeImmutableEventWithoutStateClassStatsMap);
        this.immutableEventWithStateClassStatsByPlannerPhaseMapSupplier = Suppliers.memoize(this::computeImmutableEventWithStateClassStatsByPlannerPhaseMap);
        this.immutablePlannerRuleClassStatsMapSupplier = Suppliers.memoize(this::computeImmutablePlannerRuleClassStatsMap);
    }

    @Nonnull
    public Map<Class<? extends Debugger.Event>, Stats> getEventClassStatsMap() {
        return immutableEventClassStatsMapSupplier.get();
    }

    @Nonnull
    public Map<Class<? extends CascadesRule<?>>, Stats> getPlannerRuleClassStatsMap() {
        return immutablePlannerRuleClassStatsMapSupplier.get();
    }

    @Nonnull
    public Map<Class<? extends Debugger.Event>, Stats> getEventWithoutStateClassStatsMap() {
        return immutableEventClassStatsMapSupplier.get();
    }

    @Nonnull
    public Optional<Map<Class<? extends Debugger.EventWithState>, Stats>> getEventWithStateClassStatsMapByPlannerPhase(@Nonnull PlannerPhase plannerPhase) {
        return Optional.ofNullable(immutableEventWithStateClassStatsByPlannerPhaseMapSupplier.get().get(plannerPhase));
    }

    private Map<Class<? extends Debugger.Event>, Stats> computeImmutableEventWithoutStateClassStatsMap() {
        final var eventWithoutStateClassStatsMapBuilder =
                ImmutableMap.<Class<? extends Debugger.Event>, Stats>builder();

        eventWithoutStateClassStatsMap.forEach(
                (eventClass, stats) -> eventWithoutStateClassStatsMapBuilder.put(eventClass, stats.toImmutable())
        );

        return eventWithoutStateClassStatsMapBuilder.build();
    }

    @Nonnull
    private Map<Class<? extends Debugger.Event>, Stats> computeImmutableEventClassStatsMap() {
        // Add all events not tied to a specific planner phase first
        Map<Class<? extends Debugger.Event>, Stats> result = new LinkedHashMap<>(this.immutableEventWithoutStateClassStatsMapSupplier.get());

        // Merge the stats all events tied to a specific planner phase with other events
        for (final var eventWithStateClassStats : eventWithStateClassStatsByPlannerPhaseMap.values()) {
            for (final var eventWithStateClassStatsEntry : eventWithStateClassStats.entrySet()) {
                result.merge(
                        eventWithStateClassStatsEntry.getKey(),
                        eventWithStateClassStatsEntry.getValue().toImmutable(),
                        (s1, s2) -> Stats.merge(s1, s2).toImmutable());
            }
        }

        return ImmutableMap.copyOf(result);
    }

    @Nonnull
    private Map<PlannerPhase, Map<Class<? extends Debugger.EventWithState>, Stats>> computeImmutableEventWithStateClassStatsByPlannerPhaseMap() {
        final var eventClassStatsByPlannerPhaseMapBuilder =
                ImmutableMap.<PlannerPhase, Map<Class<? extends Debugger.EventWithState>, Stats>>builder();
        for (final var eventClassStatsByPlannerPhaseEntry : eventWithStateClassStatsByPlannerPhaseMap.entrySet()) {
            final Map<Class<? extends Debugger.EventWithState>, Stats> eventClassImmutableStats =
                    eventClassStatsByPlannerPhaseEntry.getValue().entrySet().stream().collect(
                            ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().toImmutable())
                    );
            eventClassStatsByPlannerPhaseMapBuilder.put(eventClassStatsByPlannerPhaseEntry.getKey(), eventClassImmutableStats);
        }
        return eventClassStatsByPlannerPhaseMapBuilder.build();
    }

    @Nonnull
    private Map<Class<? extends CascadesRule<?>>, Stats> computeImmutablePlannerRuleClassStatsMap() {
        final var plannerRuleClassStatsMapBuilder =
                ImmutableMap.<Class<? extends CascadesRule<?>>, Stats>builder();
        for (final var entry : plannerRuleClassStatsMap.entrySet()) {
            plannerRuleClassStatsMapBuilder.put(entry.getKey(), entry.getValue().toImmutable());
        }
        return plannerRuleClassStatsMapBuilder.build();
    }
}
