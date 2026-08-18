/*
 * TaskEventRegisterTest.java
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
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TaskEventRegister#compose} — the combinator the maintainer uses to hand a vector engine a
 * single register that fans a task-enqueued/executed callback out to several. Verifies its shortcuts (empty yields the
 * {@link TaskEventRegister#NOOP} singleton, one register is returned unchanged) and that a composite forwards every
 * callback to all of its registers in order. Pure logic, so no FDB: the callbacks only forward the transaction, so the
 * tests pass a {@code null} one and assert on the forwarding.
 */
class TaskEventRegisterTest {
    @Test
    void composeOfEmptyListIsTheNoopSingleton() {
        assertThat(TaskEventRegister.compose(ImmutableList.<TaskEventRegister>of()))
                .as("nothing to notify collapses to the shared NOOP").isSameAs(TaskEventRegister.NOOP);
    }

    @Test
    void composeOfASingleRegisterReturnsItUnchanged() {
        final TaskEventRegister only = new RecordingRegister("only", new ArrayList<>());
        assertThat(TaskEventRegister.compose(ImmutableList.of(only)))
                .as("a one-element list needs no wrapper").isSameAs(only);
        assertThat(TaskEventRegister.compose(only))
                .as("the varargs form with no additional registers likewise returns the sole register").isSameAs(only);
    }

    @Test
    void varargsComposeForwardsToFirstThenAdditionalInOrder() {
        final List<String> log = new ArrayList<>();
        final TaskEventRegister a = new RecordingRegister("a", log);
        final TaskEventRegister b = new RecordingRegister("b", log);
        final TaskEventRegister c = new RecordingRegister("c", log);

        final TaskEventRegister composed = TaskEventRegister.compose(a, b, c);
        composed.onTaskEnqueued(null);
        composed.onTaskExecuted(null);

        assertThat(log).containsExactly(
                "a:enqueued", "b:enqueued", "c:enqueued",
                "a:executed", "b:executed", "c:executed");
    }

    @Test
    void listComposeForwardsToEveryRegisterInOrder() {
        final List<String> log = new ArrayList<>();
        final TaskEventRegister composed = TaskEventRegister.compose(
                ImmutableList.<TaskEventRegister>of(new RecordingRegister("x", log), new RecordingRegister("y", log)));

        composed.onTaskExecuted(null);
        composed.onTaskEnqueued(null);

        assertThat(log).containsExactly("x:executed", "y:executed", "x:enqueued", "y:enqueued");
    }

    /** A register that appends {@code label:enqueued}/{@code label:executed} to a shared log as it is notified. */
    private static final class RecordingRegister implements TaskEventRegister {
        @Nonnull
        private final String label;
        @Nonnull
        private final List<String> log;

        RecordingRegister(@Nonnull final String label, @Nonnull final List<String> log) {
            this.label = label;
            this.log = log;
        }

        @Override
        public void onTaskEnqueued(@Nullable final Transaction transaction) {
            log.add(label + ":enqueued");
        }

        @Override
        public void onTaskExecuted(@Nullable final Transaction transaction) {
            log.add(label + ":executed");
        }
    }
}
