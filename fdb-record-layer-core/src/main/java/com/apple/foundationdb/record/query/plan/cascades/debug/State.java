/*
 * State.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Verify;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

@SuppressWarnings("PMD.SystemPrintln")
public class State {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(State.class);

    @Nonnull
    private final Map<Class<?>, Integer> classToIndexMap;
    @Nonnull
    private final Cache<Integer, RelationalExpression> expressionCache;
    @Nonnull private final Cache<RelationalExpression, Integer> invertedExpressionsCache;
    @Nonnull private final Cache<Integer, Reference> referenceCache;
    @Nonnull private final Cache<Reference, Integer> invertedReferenceCache;
    @Nonnull private final Cache<Integer, Quantifier> quantifierCache;
    @Nonnull private final Cache<Quantifier, Integer> invertedQuantifierCache;

    @Nullable private final List<Debugger.Event> events;
    @Nullable private final List<PEvent> eventProtos;
    @Nullable private final Iterable<PEvent> prerecordedEventProtoIterable;
    @Nullable private Iterator<PEvent> prerecordedEventProtoIterator;

    @Nonnull private final Map<Class<? extends Debugger.Event>, MutableStats> eventClassStatsMap;

    @Nonnull private final Map<Class<? extends CascadesRule<?>>, MutableStats> plannerRuleClassStatsMap;

    @Nonnull private final Deque<Pair<Class<? extends Debugger.Event>, EventDurations>> eventProfilingStack;

    private int currentTick;
    private final long startTs;

    public static State initial(final boolean isRecordEvents, final boolean isRecordEventProtos,
                                @Nullable Iterable<PEvent> prerecordedEventProtoIterable) {
        return new State(isRecordEvents, isRecordEventProtos, prerecordedEventProtoIterable);
    }

    public static State copyOf(final State source) {
        final Cache<Integer, RelationalExpression> copyExpressionCache = CacheBuilder.newBuilder().weakValues().build();
        source.getExpressionCache().asMap().forEach(copyExpressionCache::put);
        final Cache<RelationalExpression, Integer> copyInvertedExpressionsCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedExpressionsCache().asMap().forEach(copyInvertedExpressionsCache::put);
        final Cache<Integer, Reference> copyReferenceCache = CacheBuilder.newBuilder().weakValues().build();
        source.getReferenceCache().asMap().forEach(copyReferenceCache::put);
        final Cache<Reference, Integer> copyInvertedReferenceCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedReferenceCache().asMap().forEach(copyInvertedReferenceCache::put);
        final Cache<Integer, Quantifier> copyQuantifierCache = CacheBuilder.newBuilder().weakValues().build();
        source.getQuantifierCache().asMap().forEach(copyQuantifierCache::put);
        final Cache<Quantifier, Integer> copyInvertedQuantifierCache = CacheBuilder.newBuilder().weakKeys().build();
        source.getInvertedQuantifierCache().asMap().forEach(copyInvertedQuantifierCache::put);

        return new State(source.getClassToIndexMap(),
                copyExpressionCache,
                copyInvertedExpressionsCache,
                copyReferenceCache,
                copyInvertedReferenceCache,
                copyQuantifierCache,
                copyInvertedQuantifierCache,
                source.events == null ? null : Lists.newArrayList(source.events),
                source.eventProtos == null ? null : Lists.newArrayList(source.eventProtos),
                source.prerecordedEventProtoIterable,
                Maps.newLinkedHashMap(source.eventClassStatsMap),
                Maps.newLinkedHashMap(source.plannerRuleClassStatsMap),
                new ArrayDeque<>(source.eventProfilingStack),
                source.getCurrentTick(),
                source.getStartTs());
    }

    private State(final boolean isRecordEvents, final boolean isRecordEventProtos,
                  @Nullable final Iterable<PEvent> prerecordedEventProtoIterable) {
        this(Maps.newHashMap(),
                CacheBuilder.newBuilder().weakValues().build(),
                CacheBuilder.newBuilder().weakKeys().build(),
                CacheBuilder.newBuilder().weakValues().build(),
                CacheBuilder.newBuilder().weakKeys().build(),
                CacheBuilder.newBuilder().weakValues().build(),
                CacheBuilder.newBuilder().weakKeys().build(),
                isRecordEventProtos ? Lists.newArrayList() : null,
                isRecordEvents ? Lists.newArrayList() : null,
                prerecordedEventProtoIterable,
                Maps.newLinkedHashMap(),
                Maps.newLinkedHashMap(),
                new ArrayDeque<>(),
                -1,
                System.nanoTime());
    }

    private State(@Nonnull final Map<Class<?>, Integer> classToIndexMap,
                  @Nonnull final Cache<Integer, RelationalExpression> expressionCache,
                  @Nonnull final Cache<RelationalExpression, Integer> invertedExpressionsCache,
                  @Nonnull final Cache<Integer, Reference> referenceCache,
                  @Nonnull final Cache<Reference, Integer> invertedReferenceCache,
                  @Nonnull final Cache<Integer, Quantifier> quantifierCache,
                  @Nonnull final Cache<Quantifier, Integer> invertedQuantifierCache,
                  @Nullable final List<Debugger.Event> events,
                  @Nullable final List<PEvent> eventProtos,
                  @Nullable final Iterable<PEvent> prerecordedEventProtoIterable,
                  @Nonnull final Map<Class<? extends Debugger.Event>, MutableStats> eventClassStatsMap,
                  @Nonnull final Map<Class<? extends CascadesRule<?>>, MutableStats> plannerRuleClassStatsMap,
                  @Nonnull final Deque<Pair<Class<? extends Debugger.Event>, EventDurations>> eventProfilingStack,
                  final int currentTick,
                  final long startTs) {
        this.classToIndexMap = Maps.newHashMap(classToIndexMap);
        this.expressionCache = expressionCache;
        this.invertedExpressionsCache = invertedExpressionsCache;
        this.referenceCache = referenceCache;
        this.invertedReferenceCache = invertedReferenceCache;
        this.quantifierCache = quantifierCache;
        this.invertedQuantifierCache = invertedQuantifierCache;
        this.events = events;
        this.eventProtos = eventProtos;
        this.prerecordedEventProtoIterable = prerecordedEventProtoIterable;
        this.prerecordedEventProtoIterator = prerecordedEventProtoIterable == null
                                             ? null : prerecordedEventProtoIterable.iterator();
        this.eventClassStatsMap = eventClassStatsMap;
        this.plannerRuleClassStatsMap = plannerRuleClassStatsMap;
        this.eventProfilingStack = eventProfilingStack;
        this.currentTick = currentTick;
        this.startTs = startTs;
    }

    @Nonnull
    private Map<Class<?>, Integer> getClassToIndexMap() {
        return classToIndexMap;
    }

    @Nonnull
    public Cache<Integer, RelationalExpression> getExpressionCache() {
        return expressionCache;
    }

    @Nonnull
    public Cache<RelationalExpression, Integer> getInvertedExpressionsCache() {
        return invertedExpressionsCache;
    }

    @Nonnull
    public Cache<Integer, Reference> getReferenceCache() {
        return referenceCache;
    }

    @Nonnull
    public Cache<Reference, Integer> getInvertedReferenceCache() {
        return invertedReferenceCache;
    }

    @Nonnull
    public Cache<Integer, Quantifier> getQuantifierCache() {
        return quantifierCache;
    }

    @Nonnull
    public Cache<Quantifier, Integer> getInvertedQuantifierCache() {
        return invertedQuantifierCache;
    }

    @Nullable
    public List<Debugger.Event> getEvents() {
        return events;
    }

    @Nullable
    public List<PEvent> getEventProtos() {
        return eventProtos;
    }

    @Nullable
    public Iterator<PEvent> getPrerecordedEventProtoIterator() {
        return prerecordedEventProtoIterator;
    }

    public int getCurrentTick() {
        return currentTick;
    }

    public long getStartTs() {
        return startTs;
    }

    public int getIndex(final Class<?> clazz) {
        return classToIndexMap.getOrDefault(clazz, 0);
    }

    @CanIgnoreReturnValue
    public int updateIndex(final Class<?> clazz, IntUnaryOperator computeFn) {
        return classToIndexMap.compute(clazz, (c, value) -> value == null ? computeFn.applyAsInt(0) : computeFn.applyAsInt(value));
    }

    public void registerExpression(final RelationalExpression expression) {
        if (invertedExpressionsCache.getIfPresent(expression) == null) {
            final int index = getIndex(RelationalExpression.class);
            expressionCache.put(index, expression);
            invertedExpressionsCache.put(expression, index);
            updateIndex(RelationalExpression.class, i -> i + 1);
        }
    }

    public void registerReference(final Reference reference) {
        if (invertedReferenceCache.getIfPresent(reference) == null) {
            final int index = getIndex(Reference.class);
            referenceCache.put(index, reference);
            invertedReferenceCache.put(reference, index);
            updateIndex(Reference.class, i -> i + 1);
        }
    }

    public void registerQuantifier(final Quantifier quantifier) {
        if (invertedQuantifierCache.getIfPresent(quantifier) == null) {
            final int index = getIndex(Quantifier.class);
            quantifierCache.put(index, quantifier);
            invertedQuantifierCache.put(quantifier, index);
            updateIndex(Quantifier.class, i -> i + 1);
        }
    }

    @SuppressWarnings("unchecked")
    public void addCurrentEvent(@Nonnull final Debugger.Event event) {
        if (events != null) {
            events.add(event);
        }
        if (eventProtos != null || prerecordedEventProtoIterator != null) {
            final var currentEventProto = event.toEventProto();
            if (prerecordedEventProtoIterator != null) {
                verifyCurrentEventProto(currentEventProto);
            }
            if (eventProtos != null) {
                eventProtos.add(currentEventProto);
            }
        }

        currentTick++;
        final long currentTsInNs = System.nanoTime();

        final Class<? extends Debugger.Event> currentEventClass = event.getClass();
        switch (event.getLocation()) {
            case BEGIN:
                eventProfilingStack.push(Pair.of(currentEventClass, new EventDurations(currentTsInNs)));
                updateCounts(event);
                break;
            case END:
                Pair<Class<? extends Debugger.Event>, EventDurations> profilingPair = eventProfilingStack.pop();
                final Class<? extends Debugger.Event> eventClass = profilingPair.getKey();
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

                final MutableStats forEventClass = getEventStatsForEventClass(currentEventClass);
                forEventClass.increaseTotalTimeInNs(totalTime);
                forEventClass.increaseOwnTimeInNs(ownTime);
                if (event instanceof Debugger.TransformRuleCallEvent) {
                    final CascadesRule<?> rule = ((Debugger.TransformRuleCallEvent)event).getRule();
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
                updateCounts(event);
                break;
        }
    }

    private void verifyCurrentEventProto(final PEvent currentEventProto) {
        Objects.requireNonNull(prerecordedEventProtoIterator);
        Verify.verify(prerecordedEventProtoIterator.hasNext(),
                "ran out of prerecorded events");
        final var expectedProto = prerecordedEventProtoIterator.next();
        if (!currentEventProto.equals(expectedProto)) {
            System.err.println("Mismatch found between prerecorded event and this event!");
            System.err.println("The following events prior to this event did match:");
            if (eventProtos != null) {
                for (int i = 0; i < eventProtos.size(); i++) {
                    final var oldEventProto = eventProtos.get(i);
                    System.err.println(i + ": " + oldEventProto.getDescription() + "; " + oldEventProto.getShorthand());
                }
            }

            System.err.println();
            System.err.println("The following event did not match:");
            System.err.println("Expected: " + expectedProto);
            System.err.println("Actual: " + currentEventProto);
            prerecordedEventProtoIterator = null;
            throw new RecordCoreException("Planning event does not match prerecorded event");
        }
    }

    @SuppressWarnings("unchecked")
    private void updateCounts(@Nonnull final Debugger.Event event) {
        final MutableStats forEventClass = getEventStatsForEventClass(event.getClass());
        forEventClass.increaseCount(event.getLocation(), 1L);
        if (event instanceof Debugger.EventWithRule) {
            final CascadesRule<?> rule = ((Debugger.EventWithRule)event).getRule();
            final Class<? extends CascadesRule<?>> ruleClass = (Class<? extends CascadesRule<?>>)rule.getClass();
            final MutableStats forPlannerRuleClass = getEventStatsForPlannerRuleClass(ruleClass);
            forPlannerRuleClass.increaseCount(event.getLocation(), 1L);
        }
    }

    private MutableStats getEventStatsForEventClass(@Nonnull Class<? extends Debugger.Event> eventClass) {
        return eventClassStatsMap.compute(eventClass, (eC, mutableStats) -> mutableStats != null ? mutableStats : new MutableStats());
    }

    private MutableStats getEventStatsForPlannerRuleClass(@Nonnull Class<? extends CascadesRule<?>> plannerRuleClass) {
        return plannerRuleClassStatsMap.compute(plannerRuleClass, (eC, mutableStats) -> mutableStats != null ? mutableStats : new MutableStats());
    }

    @Nonnull
    StatsMaps getStatsMaps() {
        return new StatsMaps(eventClassStatsMap, plannerRuleClassStatsMap);
    }

    public String showStats() {
        StringBuilder tableBuilder = new StringBuilder();
        tableBuilder.append("<table class=\"table\">");
        tableHeader(tableBuilder, "Event");
        final ImmutableMap<String, MutableStats> eventStatsMap =
                eventClassStatsMap.entrySet()
                        .stream()
                        .map(entry -> Pair.of(entry.getKey().getSimpleName(), entry.getValue()))
                        .sorted(Map.Entry.comparingByKey())
                        .collect(ImmutableMap.toImmutableMap(Pair::getKey, Pair::getValue));
        tableBody(tableBuilder, eventStatsMap);
        tableBuilder.append("</table>");

        final String eventProfilingString = tableBuilder.toString();

        tableBuilder = new StringBuilder();
        tableBuilder.append("<table class=\"table\">");
        tableHeader(tableBuilder, "Planner Rule");
        final ImmutableMap<String, MutableStats> plannerRuleStatsMap =
                plannerRuleClassStatsMap.entrySet()
                        .stream()
                        .map(entry -> Pair.of(entry.getKey().getSimpleName(), entry.getValue()))
                        .sorted(Map.Entry.comparingByKey())
                        .collect(ImmutableMap.toImmutableMap(Pair::getKey, Pair::getValue));
        tableBody(tableBuilder, plannerRuleStatsMap);
        tableBuilder.append("</table>");

        final String plannerRuleProfilingString = tableBuilder.toString();

        return BrowserHelper.browse("/showProfilingReport.html",
                ImmutableMap.of("$EVENT_PROFILING", eventProfilingString,
                        "$PLANNER_RULE_PROFILING", plannerRuleProfilingString));
    }

    private void tableHeader(@Nonnull final StringBuilder stringBuilder, @Nonnull final String category) {
        stringBuilder.append("<thead>");
        stringBuilder.append("<tr>");
        stringBuilder.append("<th scope=\"col\">").append(category).append("</th>");
        stringBuilder.append("<th scope=\"col\">Location</th>");
        stringBuilder.append("<th scope=\"col\">Count</th>");
        stringBuilder.append("<th scope=\"col\">Total Time (micros)</th>");
        stringBuilder.append("<th scope=\"col\">Average Time (micros)</th>");
        stringBuilder.append("<th scope=\"col\">Total Own Time (micros)</th>");
        stringBuilder.append("<th scope=\"col\">Average Own Time (micros)</th>");
        stringBuilder.append("</tr>");
        stringBuilder.append("</thead>");
    }

    private void tableBody(@Nonnull final StringBuilder stringBuilder, @Nonnull final Map<String, MutableStats> statsMap) {
        stringBuilder.append("<tbody class=\"table-group-divider\">");
        for (final Map.Entry<String, MutableStats> entry : statsMap.entrySet()) {
            final MutableStats mutableStats = entry.getValue();
            for (final var locationEntry : mutableStats.getLocationCountMap().entrySet()) {
                stringBuilder.append("<tr>");
                stringBuilder.append("<td>").append(entry.getKey()).append("</td>");
                if (locationEntry.getKey() == Debugger.Location.BEGIN) {
                    stringBuilder.append("<td></td>");
                } else {
                    stringBuilder.append("<td>").append(locationEntry.getKey().name()).append("</td>");
                }
                stringBuilder.append("<td class=\"text-end\">").append(locationEntry.getValue()).append("</td>");
                if (locationEntry.getKey() == Debugger.Location.BEGIN) {
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(mutableStats.getTotalTimeInNs())).append("</td>");
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(mutableStats.getTotalTimeInNs() / mutableStats.getCount(Debugger.Location.BEGIN))).append("</td>");
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(mutableStats.getOwnTimeInNs())).append("</td>");
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(mutableStats.getOwnTimeInNs() / mutableStats.getCount(Debugger.Location.BEGIN))).append("</td>");
                } else {
                    stringBuilder.append("<td></td>");
                    stringBuilder.append("<td></td>");
                }
                stringBuilder.append("</tr>");
            }
        }
        stringBuilder.append("</tbody>");
    }

    @Nonnull
    private String formatNsInMicros(final long ns) {
        final long micros = TimeUnit.NANOSECONDS.toMicros(ns);
        return String.format(Locale.ROOT, "%,d", micros);
    }

    private static class MutableStats extends Stats {
        public MutableStats() {
            super(Maps.newLinkedHashMap(), 0L, 0L);
        }

        public void setCount(@Nonnull Debugger.Location location, final long count) {
            locationCountMap.put(location, count);
        }

        public void increaseCount(@Nonnull Debugger.Location location, final long increase) {
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
