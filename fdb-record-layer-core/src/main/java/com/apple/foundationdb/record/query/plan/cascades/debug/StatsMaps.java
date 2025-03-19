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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.function.Supplier;

public class StatsMaps {
    @Nonnull
    private final Map<Class<? extends Debugger.Event>, ? extends Stats> eventClassStatsMap;
    @Nonnull
    private final Map<Class<? extends CascadesRule<?>>, ? extends Stats> plannerRuleClassStatsMap;

    @Nonnull
    private final Supplier<Map<Class<? extends Debugger.Event>, Stats>> immutableEventClassStatsMapSupplier;
    @Nonnull
    private final Supplier<Map<Class<? extends CascadesRule<?>>, Stats>> immutablePlannerRuleClassStatsMapSupplier;

    public StatsMaps(@Nonnull final Map<Class<? extends Debugger.Event>, ? extends Stats> eventClassStatsMap,
                     @Nonnull final Map<Class<? extends CascadesRule<?>>, ? extends Stats> plannerRuleClassStatsMap) {
        this.eventClassStatsMap = eventClassStatsMap;
        this.plannerRuleClassStatsMap = plannerRuleClassStatsMap;
        this.immutableEventClassStatsMapSupplier = Suppliers.memoize(this::computeImmutableEventClassStatsMap);
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
    private Map<Class<? extends Debugger.Event>, Stats> computeImmutableEventClassStatsMap() {
        final var eventClassStatsMapBuilder =
                ImmutableMap.<Class<? extends Debugger.Event>, Stats>builder();
        for (final var entry : eventClassStatsMap.entrySet()) {
            eventClassStatsMapBuilder.put(entry.getKey(), entry.getValue().toImmutable());
        }
        return eventClassStatsMapBuilder.build();
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
