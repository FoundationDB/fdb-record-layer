/*
 * TaskEventRegister.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.Transaction;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A sink that a vector engine's write listener notifies as deferred maintenance tasks are enqueued and executed during
 * an insert/delete/drain, in the operation's transaction. Implementations decide what that means:
 * {@link TaskCountRegister} keeps the partition's outstanding-work counts, and {@link MaintenanceControlRegister} flags
 * the index as needing a background merge. The maintainer constructs (and, via {@link #compose}, combines) the concrete
 * registers outside the engine, so the engine stays decoupled from the record store —
 * it just notifies whatever it was handed.
 */
interface TaskEventRegister {
    /**
     * Records that a deferred maintenance task was enqueued in {@code transaction}.
     * @param transaction the transaction the enqueue happened in
     */
    void onTaskEnqueued(@Nonnull Transaction transaction);

    /**
     * Records that a deferred maintenance task was executed (and thereby removed from the queue) in {@code transaction}.
     * @param transaction the transaction the execution happened in
     */
    void onTaskExecuted(@Nonnull Transaction transaction);

    /**
     * A register that does nothing — handed to engines that track no task events (e.g. HNSW), so callers can treat the
     * register as {@link Nonnull} and skip null checks.
     */
    @Nonnull
    TaskEventRegister NOOP = new TaskEventRegister() {
        @Override
        public void onTaskEnqueued(@Nonnull final Transaction transaction) {
            // nothing to record
        }

        @Override
        public void onTaskExecuted(@Nonnull final Transaction transaction) {
            // nothing to record
        }
    };

    /**
     * Combines registers into one that forwards each callback to all of them, in order — {@code first} then each of
     * {@code additional}. A convenience over {@link #compose(List)} for a statically-known set of registers.
     * @param first the register notified first
     * @param additional further registers notified after {@code first}, in order
     * @return a register forwarding {@code onTaskEnqueued}/{@code onTaskExecuted} to {@code first} then {@code additional}
     */
    @Nonnull
    static TaskEventRegister compose(@Nonnull final TaskEventRegister first,
                                     @Nonnull final TaskEventRegister... additional) {
        final ImmutableList.Builder<TaskEventRegister> builder = ImmutableList.builder();
        builder.add(first);
        for (final TaskEventRegister register : additional) {
            builder.add(register);
        }
        return compose(builder.build());
    }

    /**
     * Combines registers into one that forwards each callback to all of them, in order. An empty list yields
     * {@link #NOOP} and a single-element list yields that element, so a caller can assemble the applicable registers
     * unconditionally and compose them without special-casing how many there are.
     * @param registers the registers to notify, in order
     * @return {@link #NOOP} if {@code registers} is empty, the sole element if there is exactly one, otherwise a
     *         register forwarding {@code onTaskEnqueued}/{@code onTaskExecuted} to all of them in order
     */
    @Nonnull
    static TaskEventRegister compose(@Nonnull final List<TaskEventRegister> registers) {
        if (registers.isEmpty()) {
            return NOOP;
        }
        if (registers.size() == 1) {
            return registers.get(0);
        }
        final List<TaskEventRegister> all = ImmutableList.copyOf(registers);
        return new TaskEventRegister() {
            @Override
            public void onTaskEnqueued(@Nonnull final Transaction transaction) {
                for (final TaskEventRegister register : all) {
                    register.onTaskEnqueued(transaction);
                }
            }

            @Override
            public void onTaskExecuted(@Nonnull final Transaction transaction) {
                for (final TaskEventRegister register : all) {
                    register.onTaskExecuted(transaction);
                }
            }
        };
    }
}
