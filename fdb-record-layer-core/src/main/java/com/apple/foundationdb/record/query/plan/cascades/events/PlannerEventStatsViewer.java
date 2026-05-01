/*
 * StatsViewer.java
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

import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.debug.BrowserHelper;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A class that can be used to view {@link PlannerEventStatsMaps} for all {@link PlannerEvent} collected by
 * {@link PlannerEventStatsCollector} during planning in a browser.
 * <p>
 * To view the stats, evaluate the following expression in a debugging session at a breakpoint before the Cascades planner
 * returns the plan:
 * <pre>
 *     PlannerEventStatsViewer.showStats(PlannerEventStatsCollector.getCollector())
 * </pre>
 *
 * The stats will be displayed in a new window of the system browser.
 * </p>
 */
@SuppressWarnings("unused")
final class PlannerEventStatsViewer {
    private PlannerEventStatsViewer() {
        // prevent outside instantiation
    }

    public static String showStats(@Nonnull final PlannerEventStatsCollector plannerEventStatsCollector) {
        final var statsMaps = plannerEventStatsCollector.getStatsMaps();

        if (statsMaps.isEmpty()) {
            return "no stats available";
        }

        StringBuilder tableBuilder = new StringBuilder();

        final var phaseNameToStatsMap = ImmutableMap.of(
                "Rewriting", statsMaps.get().getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.REWRITING),
                "Planning", statsMaps.get().getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.PLANNING),
                "Unspecified", Optional.of(statsMaps.get().getEventWithoutStateClassStatsMap())
        );

        for (final var phaseNameToStatsMapEntry : phaseNameToStatsMap.entrySet()) {
            if (phaseNameToStatsMapEntry.getValue().map(Map::isEmpty).orElse(false)) {
                continue;
            }

            tableBuilder.append("<h4>").append(phaseNameToStatsMapEntry.getKey()).append(" Phase:</h4>");
            tableBuilder.append("<table class=\"table\">");
            tableHeader(tableBuilder, "Event");

            final ImmutableMap<String, PlannerEventStats> eventStatsMap =
                    phaseNameToStatsMapEntry.getValue().get().entrySet()
                            .stream()
                            .map(entry -> Pair.of(entry.getKey().getSimpleName(), entry.getValue()))
                            .sorted(Map.Entry.comparingByKey())
                            .collect(ImmutableMap.toImmutableMap(Pair::getKey, Pair::getValue));

            tableBody(tableBuilder, eventStatsMap);
            tableBuilder.append("</table>");
        }

        final String eventProfilingString = tableBuilder.toString();

        tableBuilder = new StringBuilder();
        tableBuilder.append("<table class=\"table\">");
        tableHeader(tableBuilder, "Planner Rule");
        final ImmutableMap<String, PlannerEventStats> plannerRuleStatsMap =
                statsMaps.get().getPlannerRuleClassStatsMap().entrySet()
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

    private static void tableHeader(@Nonnull final StringBuilder stringBuilder, @Nonnull final String category) {
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

    private static void tableBody(@Nonnull final StringBuilder stringBuilder, @Nonnull final Map<String, PlannerEventStats> statsMap) {
        stringBuilder.append("<tbody class=\"table-group-divider\">");
        for (final Map.Entry<String, PlannerEventStats> entry : statsMap.entrySet()) {
            final PlannerEventStats stats = entry.getValue();
            for (final var locationEntry : stats.getLocationCountMap().entrySet()) {
                stringBuilder.append("<tr>");
                stringBuilder.append("<td>").append(entry.getKey()).append("</td>");
                if (locationEntry.getKey() == PlannerEvent.Location.BEGIN) {
                    stringBuilder.append("<td></td>");
                } else {
                    stringBuilder.append("<td>").append(locationEntry.getKey().name()).append("</td>");
                }
                stringBuilder.append("<td class=\"text-end\">").append(locationEntry.getValue()).append("</td>");
                if (locationEntry.getKey() == PlannerEvent.Location.BEGIN) {
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(stats.getTotalTimeInNs())).append("</td>");
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(stats.getTotalTimeInNs() / stats.getCount(PlannerEvent.Location.BEGIN))).append("</td>");
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(stats.getOwnTimeInNs())).append("</td>");
                    stringBuilder.append("<td class=\"text-end\">").append(formatNsInMicros(stats.getOwnTimeInNs() / stats.getCount(PlannerEvent.Location.BEGIN))).append("</td>");
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
    private static String formatNsInMicros(final long ns) {
        final long micros = TimeUnit.NANOSECONDS.toMicros(ns);
        return String.format(Locale.ROOT, "%,d", micros);
    }
}
