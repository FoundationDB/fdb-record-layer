/*
 * PlannerTimerEvents.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;

public enum PlannerTimerEvents implements StoreTimer.Event {
    REWRITING_PHASE_COMPLETE("complete the rewriting phase of the planner"),
    PLANNING_PHASE_COMPLETE("complete the planning phase of the planner");

    private final String title;
    private final String logKey;

    PlannerTimerEvents(String title) {
        this.title = title;
        this.logKey = StoreTimer.Event.super.logKey();
    }

    @Override
    public String title() {
        return title;
    }

    @Override
    @Nonnull
    public String logKey() {
        return this.logKey;
    }
}
