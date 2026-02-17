/*
 * Debugger.java
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventStatsCollector;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.base.Verify;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

/**
 * <p>
 * This interface functions as a stub providing hooks which can be called from the planner logic during planning.
 * As the planner is currently single-threaded as per planning of a query, we keep an instance of an implementor of
 * this class in the thread-local. (per-thread singleton) via {@link PlannerEventListeners}.
 * The main mean of communication with the debugger is the set of statics defined within this interface.
 * </p>
 * <p>
 * In order to enable debugging capabilities, clients should use {@link #setDebugger} which sets a debugger to be used
 * for the current thread. Once set, the planner starts interacting with the debugger in order to communicate important
 * state changes, like <em>begin of planning</em>, <em>end of planner</em>, etc.
 * </p>
 * <b>Debugging functionality should only be enabled in test cases, never in deployments</b>.
 * <p>
 * Clients using the debugger should never hold on/manage/use an instance of a debugger directly. Instead, clients
 * should use {@link #withDebugger} and {@link #mapDebugger} to invoke methods on the currently installed debugger.
 * There is a guarantee that {@link #withDebugger} does not invoke any given action if there is no debugger currently
 * set for this thread. In this way, the planner implementation can freely call debug hooks which never will incur any
 * penalties (performance or otherwise) for a production deployment.
 * </p>
 */
@SuppressWarnings("java:S1214")
public interface Debugger extends PlannerEventListeners.EventListener {
    /**
     * Set the debugger. Override the currently set debugger if necessary.
     * @param debugger the new debugger. If {@code debugger} is {@code null}, the current debugger
     *        will be removed.
     */
    static void setDebugger(final Debugger debugger) {
        if (debugger == null) {
            PlannerEventListeners.removeListener(Debugger.class);
            return;
        }
        PlannerEventListeners.addListener(Debugger.class, debugger);

        // If the debugger is enabled, event stats collection should also be enabled.
        PlannerEventStatsCollector.enableDefaultStatsCollector();
    }

    @Nullable
    static Debugger getDebugger() {
        return PlannerEventListeners.getListener(Debugger.class);
    }

    @Nonnull
    static Optional<Debugger> getDebuggerMaybe() {
        final var debugger = getDebugger();
        return Optional.ofNullable(debugger);
    }

    /**
     * Invoke the {@link Consumer} on the currently set debugger. Do not do anything if there is no debugger set.
     * @param action consumer to invoke
     */
    static void withDebugger(@Nonnull final Consumer<Debugger> action) {
        final Debugger debugger = getDebugger();
        if (debugger != null) {
            action.accept(debugger);
        }
    }

    static void verifyHeuristicPlanner() {
        Verify.verify(!isCascades());
    }

    @Nonnull
    static <T> T verifyHeuristicPlanner(@Nonnull T in) {
        Verify.verify(!isCascades());
        return in;
    }

    /**
     * Returns iff the cascades planner is currently planning a query.
     */
    static boolean isCascades() {
        return mapDebugger(debugger -> debugger.getPlanContext() != null)
                .orElse(false);
    }

    /**
     * Invoke the {@link Consumer} on the currently set debugger. Do not do anything if there is no debugger set.
     * @param runnable to invoke that may throw an exception
     */
    static void sanityCheck(@Nonnull final Runnable runnable) {
        withDebugger(debugger -> {
            if (!debugger.isSane()) {
                runnable.run();
            }
        });
    }

    /**
     * Apply the {@link Function} on the currently set debugger. Do not do anything if there is no debugger set.
     * @param function function to apply
     * @param <T> the type {@code function} produces
     * @return {@code Optional.empty()} if there is no debugger currently set for this thread or if the function
     *         returned {@code null}, {@code Optional.of(result)} where {@code result} is the result of applying
     *         {@code function}, otherwise.
     */
    @Nonnull
    static <T> Optional<T> mapDebugger(@Nonnull final Function<Debugger, T> function) {
        return getDebuggerMaybe().map(function);
    }

    static void install() {
        withDebugger(Debugger::onInstall);
    }

    static void setup() {
        withDebugger(Debugger::onSetup);
    }

    static void show(@Nonnull final Reference ref) {
        withDebugger(debugger -> debugger.onShow(ref));
    }

    static Optional<Integer> getIndexOptional(Class<?> clazz) {
        return mapDebugger(debugger -> debugger.onGetIndex(clazz));
    }

    @Nonnull
    @CanIgnoreReturnValue
    static Optional<Integer> updateIndex(Class<?> clazz, IntUnaryOperator updateFn) {
        return mapDebugger(debugger -> debugger.onUpdateIndex(clazz, updateFn));
    }

    static void registerExpression(RelationalExpression expression) {
        withDebugger(debugger -> debugger.onRegisterExpression(expression));
    }

    static void registerReference(Reference reference) {
        withDebugger(debugger -> debugger.onRegisterReference(reference));
    }

    static void registerQuantifier(Quantifier quantifier) {
        withDebugger(debugger -> debugger.onRegisterQuantifier(quantifier));
    }

    static Optional<Integer> getOrRegisterSingleton(Object singleton) {
        return mapDebugger(debugger -> debugger.onGetOrRegisterSingleton(singleton));
    }

    @Nullable
    String nameForObject(@Nonnull Object object);

    @Nullable
    PlanContext getPlanContext();

    boolean isSane();

    int onGetIndex(@Nonnull Class<?> clazz);

    int onUpdateIndex(@Nonnull Class<?> clazz, @Nonnull IntUnaryOperator updateFn);

    void onRegisterExpression(@Nonnull RelationalExpression expression);

    void onRegisterReference(@Nonnull Reference reference);

    void onRegisterQuantifier(@Nonnull Quantifier quantifier);

    int onGetOrRegisterSingleton(@Nonnull Object singleton);

    void onInstall();

    void onSetup();

    void onShow(@Nonnull Reference ref);
}
