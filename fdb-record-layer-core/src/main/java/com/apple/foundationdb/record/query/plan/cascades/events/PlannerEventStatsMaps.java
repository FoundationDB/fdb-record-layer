/*
 * PlannerEventStatsMaps.java
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

package com.apple.foundationdb.record.query.plan.cascades.events;

import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * This class stores statistics for {@link PlannerEvent}s emitted during the planning of a query by the
 * {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}, and provides
 * different ways to access these statistics: by planner event type (i.e. one of {@link PlannerEvent} implementations),
 * {@link PlannerPhase} and {@link com.apple.foundationdb.record.query.plan.cascades.PlannerRule}.
 */
public class PlannerEventStatsMaps {
    @Nonnull
    private final Map<Class<? extends PlannerEvent>, ? extends PlannerEventStats> eventWithoutStateClassStatsMap;
    @Nonnull
    private final Map<PlannerPhase, Map<Class<? extends PlannerEventWithState>, ? extends PlannerEventStats>> eventWithStateClassStatsByPlannerPhaseMap;
    @Nonnull
    private final Map<Class<? extends CascadesRule<?>>, ? extends PlannerEventStats> plannerRuleClassStatsMap;

    @Nonnull
    private final Supplier<Map<Class<? extends PlannerEvent>, PlannerEventStats>> immutableEventClassStatsMapSupplier;
    @Nonnull
    private final Supplier<Map<Class<? extends PlannerEvent>, PlannerEventStats>> immutableEventWithoutStateClassStatsMapSupplier;
    @Nonnull
    private final Supplier<Map<PlannerPhase, Map<Class<? extends PlannerEventWithState>, PlannerEventStats>>> immutableEventWithStateClassStatsByPlannerPhaseMapSupplier;
    @Nonnull
    private final Supplier<Map<Class<? extends CascadesRule<?>>, PlannerEventStats>> immutablePlannerRuleClassStatsMapSupplier;


    public PlannerEventStatsMaps(@Nonnull final Map<Class<? extends PlannerEvent>, ? extends PlannerEventStats> eventWithoutStateClassStatsMap,
                                 @Nonnull final Map<PlannerPhase, Map<Class<? extends PlannerEventWithState>, ? extends PlannerEventStats>> eventWithStateClassStatsByPlannerPhaseMap,
                                 @Nonnull final Map<Class<? extends CascadesRule<?>>, ? extends PlannerEventStats> plannerRuleClassStatsMap) {

        this.eventWithoutStateClassStatsMap = eventWithoutStateClassStatsMap;
        this.eventWithStateClassStatsByPlannerPhaseMap = eventWithStateClassStatsByPlannerPhaseMap;
        this.plannerRuleClassStatsMap = plannerRuleClassStatsMap;

        this.immutableEventClassStatsMapSupplier = Suppliers.memoize(this::computeImmutableEventClassStatsMap);
        this.immutableEventWithoutStateClassStatsMapSupplier = Suppliers.memoize(this::computeImmutableEventWithoutStateClassStatsMap);
        this.immutableEventWithStateClassStatsByPlannerPhaseMapSupplier = Suppliers.memoize(this::computeImmutableEventWithStateClassStatsByPlannerPhaseMap);
        this.immutablePlannerRuleClassStatsMapSupplier = Suppliers.memoize(this::computeImmutablePlannerRuleClassStatsMap);
    }

    /**
     * Retrieves statistics about {@link PlannerEvent}s of different types.
     * @return a {@link Map} of classes implementing the {@link PlannerEvent} interface to {@link PlannerEventStats}
     *         instances.
     */
    @Nonnull
    public Map<Class<? extends PlannerEvent>, PlannerEventStats> getEventClassStatsMap() {
        return immutableEventClassStatsMapSupplier.get();
    }

    /**
     * Retrieves statistics about all {@link PlannerEvent}s associated with a certain planner {@link CascadesRule}.
     * @return a {@link Map} of {@link CascadesRule} classes to {@link PlannerEventStats} instances.
     */
    @Nonnull
    public Map<Class<? extends CascadesRule<?>>, PlannerEventStats> getPlannerRuleClassStatsMap() {
        return immutablePlannerRuleClassStatsMapSupplier.get();
    }

    /**
     * Retrieves statistics about all instances of {@link PlannerEvent}s that are not associated with a specific
     * {@link PlannerPhase}.
     * @return a {@link Map} from classes implementing the {@link PlannerEvent} interface to
     *         {@link PlannerEventStats} instances.
     */
    @Nonnull
    public Map<Class<? extends PlannerEvent>, PlannerEventStats> getEventWithoutStateClassStatsMap() {
        return immutableEventWithoutStateClassStatsMapSupplier.get();
    }

    /**
     * Retrieves statistics about all instances of {@link PlannerEvent}s that were emitted during the provided
     * {@link PlannerPhase}.
     * @param plannerPhase the planner phase the events were emitted in.
     * @return a {@link Map} of classes extending the interface {@link PlannerEventWithState} to
     *         {@link PlannerEventStats} instances.
     */
    @Nonnull
    public Optional<Map<Class<? extends PlannerEventWithState>, PlannerEventStats>> getEventWithStateClassStatsMapByPlannerPhase(@Nonnull PlannerPhase plannerPhase) {
        return Optional.ofNullable(immutableEventWithStateClassStatsByPlannerPhaseMapSupplier.get().get(plannerPhase));
    }

    @Nonnull
    private Map<Class<? extends PlannerEvent>, PlannerEventStats> computeImmutableEventWithoutStateClassStatsMap() {
        final var eventWithoutStateClassStatsMapBuilder =
                ImmutableMap.<Class<? extends PlannerEvent>, PlannerEventStats>builder();

        eventWithoutStateClassStatsMap.forEach(
                (eventClass, plannerEventStats) -> eventWithoutStateClassStatsMapBuilder.put(eventClass, plannerEventStats.toImmutable())
        );

        return eventWithoutStateClassStatsMapBuilder.build();
    }

    @Nonnull
    private Map<Class<? extends PlannerEvent>, PlannerEventStats> computeImmutableEventClassStatsMap() {
        // Add all events not tied to a specific planner phase first
        Map<Class<? extends PlannerEvent>, PlannerEventStats> result = new LinkedHashMap<>(this.immutableEventWithoutStateClassStatsMapSupplier.get());

        // Merge the PlannerEventStats all events tied to a specific planner phase with other events
        for (final var eventWithStateClassStats : eventWithStateClassStatsByPlannerPhaseMap.values()) {
            for (final var eventWithStateClassStatsEntry : eventWithStateClassStats.entrySet()) {
                result.merge(
                        eventWithStateClassStatsEntry.getKey(),
                        eventWithStateClassStatsEntry.getValue().toImmutable(),
                        (s1, s2) -> PlannerEventStats.merge(s1, s2).toImmutable());
            }
        }

        return ImmutableMap.copyOf(result);
    }

    @Nonnull
    private Map<PlannerPhase, Map<Class<? extends PlannerEventWithState>, PlannerEventStats>> computeImmutableEventWithStateClassStatsByPlannerPhaseMap() {
        final var eventClassStatsByPlannerPhaseMapBuilder =
                ImmutableMap.<PlannerPhase, Map<Class<? extends PlannerEventWithState>, PlannerEventStats>>builder();
        for (final var eventClassStatsByPlannerPhaseEntry : eventWithStateClassStatsByPlannerPhaseMap.entrySet()) {
            final Map<Class<? extends PlannerEventWithState>, PlannerEventStats> eventClassImmutableStats =
                    eventClassStatsByPlannerPhaseEntry.getValue().entrySet().stream().collect(
                            ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().toImmutable())
                    );
            eventClassStatsByPlannerPhaseMapBuilder.put(eventClassStatsByPlannerPhaseEntry.getKey(), eventClassImmutableStats);
        }
        return eventClassStatsByPlannerPhaseMapBuilder.build();
    }

    @Nonnull
    private Map<Class<? extends CascadesRule<?>>, PlannerEventStats> computeImmutablePlannerRuleClassStatsMap() {
        final var plannerRuleClassStatsMapBuilder =
                ImmutableMap.<Class<? extends CascadesRule<?>>, PlannerEventStats>builder();
        for (final var entry : plannerRuleClassStatsMap.entrySet()) {
            plannerRuleClassStatsMapBuilder.put(entry.getKey(), entry.getValue().toImmutable());
        }
        return plannerRuleClassStatsMapBuilder.build();
    }
}
