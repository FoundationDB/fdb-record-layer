/*
 * PlannerEventStatsCollector.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * An interface that represents an object that listens to {@link PlannerEvent} emitted by the Cascades planner
 * throughout the planning of a single query, and collects statistics about these emitted events.
 * </p>
 * <p>
 * As the planner is currently single-threaded as per planning of a query, we keep an instance of an implementor of
 * this interface in the thread-local storage (per-thread singleton), similar to {@link com.apple.foundationdb.record.query.plan.cascades.debug.Debugger}.
 * </p>
 * <p>
 * Clients using implementors of this interface should never hold on/manage/use an instance of a collector directly.
 * Instead, clients should use {@link #withCollector} and {@link #flatMapCollector} to invoke methods on the currently
 * installed collector. There is a guarantee that {@link #withCollector} and other methods do not invoke any given action
 * if there is no collector currently set for this thread.
 * </p>
 * <p>
 * The statistics can be retrieved as a {@link PlannerEventStatsMaps} object via the method
 * {@link #getStatsMaps()}.
 * </p>
 */
public interface PlannerEventStatsCollector extends PlannerEventListeners.Listener {
    ThreadLocal<PlannerEventStatsCollector> THREAD_LOCAL = new ThreadLocal<>();

    static void setCollector(final PlannerEventStatsCollector collector) {
        if (THREAD_LOCAL.get() != null) {
            PlannerEventListeners.removeListener(THREAD_LOCAL.get());
        }

        if (collector == null) {
            THREAD_LOCAL.remove();
            return;
        }

        THREAD_LOCAL.set(collector);
        PlannerEventListeners.addListener(collector);
    }

    @Nullable
    static PlannerEventStatsCollector getCollector() {
        return THREAD_LOCAL.get();
    }

    @Nonnull
    static Optional<PlannerEventStatsCollector> getCollectorMaybe() {
        return Optional.ofNullable(getCollector());
    }

    static void withCollector(@Nonnull final Consumer<PlannerEventStatsCollector> action) {
        getCollectorMaybe().ifPresent(action);
    }

    @Nonnull
    static <T> Optional<T> flatMapCollector(@Nonnull final Function<PlannerEventStatsCollector, Optional<T>> function) {
        return getCollectorMaybe().flatMap(function);
    }

    /**
     * Set the stats collector to a new instance of DefaultPlannerEventStatsCollector if no collector is already set.
     */
    static void enableDefaultStatsCollector() {
        if (THREAD_LOCAL.get() == null) {
            setCollector(new DefaultPlannerEventStatsCollector());
        }
    }

    @Nonnull
    Optional<PlannerEventStatsMaps> getStatsMaps();
}
