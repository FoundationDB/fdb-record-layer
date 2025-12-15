/*
 * PlannerEventStatsCollectorState.java
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class carries the state that {@link DefaultPlannerEventStatsCollector} uses to keep
 * track of planner event statistics.
 */
public class PlannerEventStatsCollectorState {
    private static final Logger logger = LoggerFactory.getLogger(PlannerEventStatsCollectorState.class);

    private final Map<Class<? extends PlannerEvent>, MutableStats> eventWithoutStateClassStatsMap;
    private final Map<PlannerPhase, Map<Class<? extends PlannerEventWithState>, MutableStats>> eventWithStateClassStatsMapByPlannerPhase;
    private final Map<Class<? extends CascadesRule<?>>, MutableStats> plannerRuleClassStatsMap;
    private final Deque<Pair<Class<? extends PlannerEvent>, EventDurations>> eventProfilingStack;

    public PlannerEventStatsCollectorState() {
        this.eventWithoutStateClassStatsMap = Maps.newLinkedHashMap();
        this.eventWithStateClassStatsMapByPlannerPhase = Maps.newEnumMap(PlannerPhase.class);
        this.plannerRuleClassStatsMap = Maps.newLinkedHashMap();
        this.eventProfilingStack = new ArrayDeque<>();
    }

    @SuppressWarnings("unchecked")
    public void addCurrentEvent(@Nonnull final PlannerEvent plannerEvent) {
        final long currentTsInNs = System.nanoTime();

        final Class<? extends PlannerEvent> currentEventClass = plannerEvent.getClass();
        switch (plannerEvent.getLocation()) {
            case BEGIN:
                eventProfilingStack.push(Pair.of(currentEventClass, new EventDurations(currentTsInNs)));
                updateCounts(plannerEvent);
                break;
            case END:
                Pair<Class<? extends PlannerEvent>, EventDurations> profilingPair = eventProfilingStack.pop();
                final Class<? extends PlannerEvent> eventClass = profilingPair.getKey();
                EventDurations eventDurations = profilingPair.getValue();
                if (logger.isWarnEnabled() && currentEventClass != eventClass) {
                    //
                    // This is a severe problem, however, we don't want to further increase the noise by
                    // throwing an exception here.
                    //
                    logger.warn(KeyValueLogMessage.of("unable to unwind stack properly",
                            "expected event class", eventClass.getSimpleName(),
                            "current event class", currentEventClass.getSimpleName()));
                }

                final long totalTime = currentTsInNs - eventDurations.getStartTsInNs();
                final long ownTime = totalTime - eventDurations.getAdjustmentForOwnTimeInNs();

                final MutableStats forEventClass = getEventStatsForEvent(plannerEvent);
                forEventClass.increaseTotalTimeInNs(totalTime);
                forEventClass.increaseOwnTimeInNs(ownTime);
                if (plannerEvent instanceof TransformRuleCallPlannerEvent) {
                    final CascadesRule<?> rule = ((TransformRuleCallPlannerEvent)plannerEvent).getRule();
                    final Class<? extends CascadesRule<?>> ruleClass = (Class<? extends CascadesRule<?>>)rule.getClass();
                    final MutableStats forPlannerRuleClass = getEventStatsForPlannerRuleClass(ruleClass);
                    forPlannerRuleClass.increaseTotalTimeInNs(totalTime);
                    forPlannerRuleClass.increaseOwnTimeInNs(ownTime);
                }

                // adjust the parent's own time info
                profilingPair = eventProfilingStack.peek();
                if (profilingPair != null) {
                    eventDurations = profilingPair.getValue();
                    eventDurations.increaseAdjustmentForOwnTimeInNs(totalTime);
                }
                break;
            default:
                updateCounts(plannerEvent);
                break;
        }
    }

    @SuppressWarnings("unchecked")
    private void updateCounts(@Nonnull final PlannerEvent plannerEvent) {
        final MutableStats forEventClass = getEventStatsForEvent(plannerEvent);
        forEventClass.increaseCount(plannerEvent.getLocation(), 1L);

        if (plannerEvent instanceof PlannerEventWithRule) {
            final CascadesRule<?> rule = ((PlannerEventWithRule)plannerEvent).getRule();
            final Class<? extends CascadesRule<?>> ruleClass = (Class<? extends CascadesRule<?>>)rule.getClass();
            final MutableStats forPlannerRuleClass = getEventStatsForPlannerRuleClass(ruleClass);
            forPlannerRuleClass.increaseCount(plannerEvent.getLocation(), 1L);
        }
    }

    private MutableStats getEventStatsForEvent(@Nonnull PlannerEvent plannerEvent) {
        return (plannerEvent instanceof PlannerEventWithState) ?
               getEventStatsForEventWithStateClassByPlannerPhase((PlannerEventWithState)plannerEvent) :
               getEventStatsForEventWithoutStateClass(plannerEvent.getClass());
    }

    private MutableStats getEventStatsForEventWithoutStateClass(@Nonnull Class<? extends PlannerEvent> eventClass) {
        return eventWithoutStateClassStatsMap.compute(eventClass, (eC, mutableStats) -> mutableStats != null ? mutableStats : new MutableStats());
    }

    private MutableStats getEventStatsForEventWithStateClassByPlannerPhase(@Nonnull PlannerEventWithState event) {
        return eventWithStateClassStatsMapByPlannerPhase.computeIfAbsent(event.getPlannerPhase(), pP -> new LinkedHashMap<>())
                .computeIfAbsent(event.getClass(), (eC) -> new MutableStats());
    }

    private MutableStats getEventStatsForPlannerRuleClass(@Nonnull Class<? extends CascadesRule<?>> plannerRuleClass) {
        return plannerRuleClassStatsMap.computeIfAbsent(plannerRuleClass, (eC) -> new MutableStats());
    }

    @Nonnull
    PlannerEventStatsMaps getStatsMaps() {
        return new PlannerEventStatsMaps(
                Collections.unmodifiableMap(eventWithoutStateClassStatsMap),
                Collections.unmodifiableMap(eventWithStateClassStatsMapByPlannerPhase),
                Collections.unmodifiableMap(plannerRuleClassStatsMap));
    }


    private static class MutableStats extends PlannerEventStats {
        public MutableStats() {
            super(Maps.newLinkedHashMap(), 0L, 0L);
        }

        public void setCount(@Nonnull PlannerEvent.Location location, final long count) {
            locationCountMap.put(location, count);
        }

        public void increaseCount(@Nonnull PlannerEvent.Location location, final long increase) {
            setCount(location, getCount(location) + increase);
        }

        public void setTotalTimeInNs(final long totalTimeInNs) {
            this.totalTimeInNs = totalTimeInNs;
        }

        public void increaseTotalTimeInNs(final long increaseInNs) {
            setTotalTimeInNs(getTotalTimeInNs() + increaseInNs);
        }

        public void setOwnTimeInNs(final long ownTimeInNs) {
            this.ownTimeInNs = ownTimeInNs;
        }

        public void increaseOwnTimeInNs(final long increaseInNs) {
            setOwnTimeInNs(getOwnTimeInNs() + increaseInNs);
        }
    }

    private static class EventDurations {
        private final long startTsInNs;
        private long adjustmentForOwnTimeInNs;

        public EventDurations(final long startTsInNs) {
            this.startTsInNs = startTsInNs;
        }

        public long getStartTsInNs() {
            return startTsInNs;
        }

        public long getAdjustmentForOwnTimeInNs() {
            return adjustmentForOwnTimeInNs;
        }

        public void setAdjustmentForOwnTimeInNs(final long adjustmentForOwnTimeInNs) {
            this.adjustmentForOwnTimeInNs = adjustmentForOwnTimeInNs;
        }

        public void increaseAdjustmentForOwnTimeInNs(final long increaseInNs) {
            setAdjustmentForOwnTimeInNs(getAdjustmentForOwnTimeInNs() + increaseInNs);
        }
    }
}
