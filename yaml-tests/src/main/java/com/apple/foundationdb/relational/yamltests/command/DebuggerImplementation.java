/*
 * DebuggerImplementation.java
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.debug.PlannerRepl;
import org.jline.terminal.TerminalBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.function.Supplier;

public enum DebuggerImplementation {
    INSANE(DebuggerWithSymbolTables::withSanityChecks),
    SANE(DebuggerWithSymbolTables::withoutSanityChecks),
    REPL(() -> {
        try {
            return new PlannerRepl(TerminalBuilder.builder().dumb(true).build(), false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    });

    @Nonnull
    private final Supplier<Debugger> debuggerSupplier;

    DebuggerImplementation(@Nonnull final Supplier<Debugger> debuggerSupplier) {
        this.debuggerSupplier = debuggerSupplier;
    }

    @Nonnull
    public Debugger newDebugger() {
        return debuggerSupplier.get();
    }
}
