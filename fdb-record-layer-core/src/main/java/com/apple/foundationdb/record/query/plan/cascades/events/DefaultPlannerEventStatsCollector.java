/*
 * DefaultPlannerEventStatsCollector.java
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

import com.apple.foundationdb.record.query.plan.cascades.PlanContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

public class DefaultPlannerEventStatsCollector implements PlannerEventStatsCollector {
    @Nullable private PlannerEventStatsCollectorState currentPlannerEventStatsCollectorState;

    @Nonnull
    PlannerEventStatsCollectorState getCurrentState() {
        return Objects.requireNonNull(currentPlannerEventStatsCollectorState);
    }

    @Override
    public void onQuery(String queryAsString, PlanContext planContext) {
        reset();
    }

    @Override
    public void onEvent(final PlannerEvent plannerEvent) {
        if (currentPlannerEventStatsCollectorState == null) {
            return;
        }
        getCurrentState().addCurrentEvent(plannerEvent);
    }

    @Override
    public void onDone() {
        reset();
    }

    @Nonnull
    @Override
    public Optional<PlannerEventStatsMaps> getStatsMaps() {
        if (currentPlannerEventStatsCollectorState != null) {
            return Optional.of(currentPlannerEventStatsCollectorState.getStatsMaps());
        }
        return Optional.empty();
    }

    private void reset() {
        this.currentPlannerEventStatsCollectorState = new PlannerEventStatsCollectorState();
    }
}
