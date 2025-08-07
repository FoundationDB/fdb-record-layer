/*
 * SymbolDebugger.java
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

import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

public interface SymbolDebugger extends Debugger {
    @Nonnull
    static Optional<SymbolDebugger> getDebuggerMaybe() {
        final var debugger = Debugger.getDebugger();
        if (debugger instanceof SymbolDebugger) {
            return Optional.of((SymbolDebugger)debugger);
        }
        return Optional.empty();
    }

    static void withDebugger(@Nonnull final Consumer<SymbolDebugger> action) {
        getDebuggerMaybe().ifPresent(action);
    }


    @Nonnull
    static <T> Optional<T> mapDebugger(@Nonnull final Function<SymbolDebugger, T> function) {
        return getDebuggerMaybe().map(function);
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

    int onGetIndex(@Nonnull Class<?> clazz);

    int onUpdateIndex(@Nonnull Class<?> clazz, @Nonnull IntUnaryOperator updateFn);

    void onRegisterExpression(@Nonnull RelationalExpression expression);

    void onRegisterReference(@Nonnull Reference reference);

    void onRegisterQuantifier(@Nonnull Quantifier quantifier);

    int onGetOrRegisterSingleton(@Nonnull Object singleton);

    @Nullable
    String nameForObject(@Nonnull Object object);
}
