/*
 * PlannerEventStatsCollector.java
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
 * this interface in the thread-local storage (per-thread singleton), similar to
 * {@link com.apple.foundationdb.record.query.plan.cascades.debug.Debugger}, via {@link PlannerEventListeners}.
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
public interface PlannerEventStatsCollector extends PlannerEventListeners.EventListener {
    /**
     * Set the thread-local collector instance. Override the currently set collector if necessary.
     * @param collector the new collector. If {@code collector} is {@code null}, the current stats collector
     *        will be removed.
     */
    static void setCollector(final PlannerEventStatsCollector collector) {
        if (collector == null) {
            PlannerEventListeners.removeListener(PlannerEventStatsCollector.class);
            return;
        }
        PlannerEventListeners.addListener(PlannerEventStatsCollector.class, collector);
    }

    /**
     * Get the current thread-local instance of {@link PlannerEventStatsCollector}.
     * @return the current thread-local instance of {@link PlannerEventStatsCollector}, or
     *         {@code null} if no instance is currently set.
     */
    @Nullable
    static PlannerEventStatsCollector getCollector() {
        return PlannerEventListeners.getListener(PlannerEventStatsCollector.class);
    }

    /**
     * Get an {@link Optional} with the current thread-local instance of {@link PlannerEventStatsCollector}.
     * @return an {@link Optional} that contains the current thread-local instance of {@link PlannerEventStatsCollector},
     *         or an empty {@link Optional} otherwise.
     */
    @Nonnull
    static Optional<PlannerEventStatsCollector> getCollectorMaybe() {
        return Optional.ofNullable(getCollector());
    }

    /**
     * Invoke the {@link Consumer} on the current thread-local instance of {@link PlannerEventStatsCollector}.
     * Does not do anything if there is no collector currently set.
     * @param action consumer to invoke
     */
    static void withCollector(@Nonnull final Consumer<PlannerEventStatsCollector> action) {
        getCollectorMaybe().ifPresent(action);
    }

    /**
     * Call the provided {@link Function} on the current thread-local instance of {@link PlannerEventStatsCollector}
     * if it is present, and return an {@link Optional} with the result.
     * @param function function to call
     * @return an {@link Optional} with the result of the function call if there is a collector instance set,
     *         or an empty {@link Optional} instead.
     */
    @Nonnull
    static <T> Optional<T> flatMapCollector(@Nonnull final Function<PlannerEventStatsCollector, Optional<T>> function) {
        return getCollectorMaybe().flatMap(function);
    }

    /**
     * Set the thread-local instance of {@link PlannerEventStatsCollector} to a new instance of
     * {@link DefaultPlannerEventStatsCollector} if no collector is already set.
     */
    static void enableDefaultStatsCollector() {
        if (getCollector() == null) {
            setCollector(new DefaultPlannerEventStatsCollector());
        }
    }

    /**
     * Get an instance {@link PlannerEventStatsMaps}, which contains statistics about {@link PlannerEvent}
     * emitted until this point.
     * @return planner event statistics as a {@link PlannerEventStatsMaps} instance.
     */
    @Nonnull
    Optional<PlannerEventStatsMaps> getStatsMaps();
}
