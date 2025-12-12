/*
 * StatsDebugger.java
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

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface extends the {@link Debugger} interface and provides functionality related to
 * recording instances of {@link Debugger.Event} and producing statistics based on these events
 * in the form of {@link StatsMaps}.
 * <p>
 * Similar to the {@link Debugger} interface, the main mean of communication with the debugger
 * for operations related to recording events is the set of statics defined within this interface.
 * </p>
 */
public interface StatsDebugger extends Debugger {
    @Nonnull
    static Optional<StatsDebugger> getDebuggerMaybe() {
        final var debugger = Debugger.getDebugger();
        if (debugger instanceof StatsDebugger) {
            return Optional.of((StatsDebugger)debugger);
        }
        return Optional.empty();
    }

    static void withDebugger(@Nonnull final Consumer<StatsDebugger> action) {
        getDebuggerMaybe().ifPresent(action);
    }

    @Nonnull
    static <T> Optional<T> flatMapDebugger(@Nonnull final Function<StatsDebugger, Optional<T>> function) {
        return getDebuggerMaybe().flatMap(function);
    }

    @Nonnull
    Optional<StatsMaps> getStatsMaps();

    void onEvent(Debugger.Event event);
}
