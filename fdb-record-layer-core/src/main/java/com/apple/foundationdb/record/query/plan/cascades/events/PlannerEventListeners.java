/*
 * PlannerEventDispatcher.java
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
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This class is used to store and manage instances of {@link PlannerEventListeners.Listener}
 * which are stored in thread-local storage, which receive {@link PlannerEvent} emitted by the
 * {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner} and perform actions.
 * </p>
 * <p>
 * Only a single instance of a class that extends the interface {@link PlannerEventListeners.Listener}
 * can be set as the active listener for that class at any given time.
 * </p>
 *
 */
public final class PlannerEventListeners {
    static final ThreadLocal<Map<Class<? extends Listener>, Listener>> THREAD_LOCAL =
            ThreadLocal.withInitial(HashMap::new);

    /**
     * Set the active thread-local instance for the provided listener class to the provided instance.
     * This will override any currently set instances for the same {@link Class}.
     * @param listenerClass the class that the provided listener is an instance of.
     * @param listener the new listener instance to use
     */
    public static void addListener(@Nonnull final Class<? extends Listener> listenerClass,
                                   @Nonnull final Listener listener) {
        THREAD_LOCAL.get().put(listenerClass, listener);
    }

    /**
     * Remove the {@link Listener} instance associated with the provided {@code listenerClass}, if it is set.
     * @param listenerClass the listener class to remove the listener instance of.
     */
    public static void removeListener(@Nonnull final Class<? extends Listener> listenerClass) {
        THREAD_LOCAL.get().remove(listenerClass);
    }

    /**
     * Clear all currently set listener instances.
     */
    public static void clearListeners() {
        THREAD_LOCAL.get().clear();
    }

    /**
     * Get the current active {@link Listener} instance for the provided {@link Class}.
     * @param listenerClass a class that implements the {@link Listener} interface.
     * @return the instance that is currently set for the provided {@code listenerClass}, null otherwise.
     */
    @Nullable
    public static Listener getListener(@Nonnull final Class<? extends Listener> listenerClass) {
        return THREAD_LOCAL.get().getOrDefault(listenerClass, null);
    }

    /**
     * Dispatch a {@link PlannerEvent} to all {@link Listener} instances in thread-local storage.
     * @param plannerEvent event to dispatch to {@link Listener}s.
     */
    public static void dispatchEvent(final PlannerEvent plannerEvent) {
        THREAD_LOCAL.get().values().forEach(l -> l.onEvent(plannerEvent));
    }

    /**
     * Signal to all {@link Listener} instances in thread-local storage that the planning of a new query is starting.
     * @param queryAsString query being planned.
     * @param planContext context object provided to the planner.
     */
    public static void dispatchOnQuery(final String queryAsString, final PlanContext planContext) {
        THREAD_LOCAL.get().values().forEach(l -> l.onQuery(queryAsString, planContext));
    }

    /**
     * Signal to all {@link Listener} instances in thread-local storage that the planning of a query is complete.
     */
    public static void dispatchOnDone() {
        THREAD_LOCAL.get().values().forEach(Listener::onDone);
    }

    /**
     * <p>
     * Classes implementing this interface listen to {@link PlannerEvent}s emitted by
     * {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner} and do some action on them.
     * </p>
     * <p>
     * See {@link com.apple.foundationdb.record.query.plan.cascades.debug.Debugger} and {@link PlannerEventStatsCollector}.
     * </p>
     */
    public interface Listener {
        /**
         * Callback to execute when a new query starts being planned.
         * @param queryAsString query being planned.
         * @param planContext context object provided to the planner.
         */
        void onQuery(String queryAsString, PlanContext planContext);

        /**
         * Callback to execute when a new {@link PlannerEvent} is emitted.
         * @param event the event that was emitted by the
         *        {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
         */
        void onEvent(PlannerEvent event);

        /**
         * Callback to execute when planning of a query is complete.
         */
        void onDone();
    }
}
