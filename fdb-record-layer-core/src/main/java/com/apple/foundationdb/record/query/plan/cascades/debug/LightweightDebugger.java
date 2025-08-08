/*
 * LightweightDebugger.java
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a lightweight debugger that only maintains debugging events for calculating {@link Stats}
 * (i.e. it implements only the {@link StatsDebugger} interface).
 * This debugger is safe for use in deployments.
 */
public class LightweightDebugger implements StatsDebugger {
    private static final Logger logger = LoggerFactory.getLogger(LightweightDebugger.class);

    @Nullable
    private EventState currentEventState;
    @Nullable
    private String queryAsString;
    @Nullable
    private PlanContext planContext;

    public LightweightDebugger() {
        this.currentEventState = null;
        this.planContext = null;
    }

    @Nonnull
    EventState getCurrentEventState() {
        return Objects.requireNonNull(currentEventState);
    }

    @Nullable
    @Override
    public PlanContext getPlanContext() {
        return planContext;
    }

    @Override
    public boolean isSane() {
        return true;
    }

    @Override
    public void onInstall() {
        // do nothing
    }

    @Override
    public void onSetup() {
        reset();
    }

    @Override
    public void onShow(@Nonnull final Reference ref) {
        // do nothing
    }

    @Override
    public void onQuery(@Nonnull final String recordQuery, @Nonnull final PlanContext planContext) {
        reset();

        this.queryAsString = recordQuery;
        this.planContext = planContext;

        logQuery();
    }

    @Override
    public void onEvent(final Debugger.Event event) {
        if ((queryAsString == null) || (planContext == null) || (currentEventState == null)) {
            return;
        }
        getCurrentEventState().addCurrentEvent(event);
    }

    @Override
    public void onDone() {
        if (currentEventState != null && queryAsString != null) {
            final var state = Objects.requireNonNull(currentEventState);
            if (logger.isDebugEnabled()) {
                logger.debug(KeyValueLogMessage.of("planning done",
                        "query", Objects.requireNonNull(queryAsString).substring(0, Math.min(queryAsString.length(), 30)),
                        "duration-in-ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - state.getStartTs()),
                        "ticks", state.getCurrentTick()));
            }
        }
        reset();
    }

    @Nonnull
    @Override
    public Optional<StatsMaps> getStatsMaps() {
        if (currentEventState != null) {
            return Optional.of(currentEventState.getStatsMaps());
        }
        return Optional.empty();
    }

    private void reset() {
        this.currentEventState = EventState.initial(false, false, null);
        this.planContext = null;
        this.queryAsString = null;
    }

    void logQuery() {
        if (logger.isDebugEnabled()) {
            logger.debug(KeyValueLogMessage.of("planning started", "query", queryAsString));
        }
    }
}
