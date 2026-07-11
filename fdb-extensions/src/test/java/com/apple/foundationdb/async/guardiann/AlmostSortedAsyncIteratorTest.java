/*
 * AlmostSortedAsyncIteratorTest.java
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.async.NonAsyncIterator;
import com.apple.foundationdb.test.TestExecutors;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AlmostSortedAsyncIterator}.
 */
class AlmostSortedAsyncIteratorTest {
    private static final Executor EXECUTOR = TestExecutors.defaultThreadPool();

    @Test
    void fullySortsWhenWindowCoversWholeInput() {
        // window == number of elements: the whole input is buffered before the first emit, so ordering is exact.
        final AlmostSortedAsyncIterator<Integer> iterator = almostSorted(ImmutableList.of(5, 4, 3, 2, 1), 5);
        assertThat(drain(iterator)).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void windowLargerThanInputFullySorts() {
        final AlmostSortedAsyncIterator<Integer> iterator = almostSorted(ImmutableList.of(3, 1, 2), 10);
        assertThat(drain(iterator)).containsExactly(1, 2, 3);
    }

    @Test
    void windowOfOnePreservesInputOrder() {
        // A one-element buffer cannot reorder anything: it emits the input as-is.
        final AlmostSortedAsyncIterator<Integer> iterator = almostSorted(ImmutableList.of(3, 1, 2), 1);
        assertThat(drain(iterator)).containsExactly(3, 1, 2);
    }

    @Test
    void smallWindowIsOnlyApproximatelySorted() {
        // With a window of 2, the trailing 1 is more than a window away from its sorted position, so it cannot be
        // pulled to the front -- it is emitted late (after 2 and 3) rather than dropped.
        final AlmostSortedAsyncIterator<Integer> iterator = almostSorted(ImmutableList.of(2, 3, 4, 1), 2);
        final List<Integer> out = drain(iterator);
        assertThat(out).containsExactly(2, 3, 1, 4);
        assertThat(out).containsExactlyInAnyOrder(1, 2, 3, 4); // nothing lost or duplicated
    }

    @Test
    void emptyInputYieldsNothing() {
        final AlmostSortedAsyncIterator<Integer> iterator = almostSorted(ImmutableList.of(), 3);
        assertThat(iterator.onHasNext().join()).isFalse();
        assertThat(drain(iterator)).isEmpty();
    }

    @Test
    void nextOnExhaustedIteratorThrows() {
        try (AlmostSortedAsyncIterator<Integer> iterator = almostSorted(ImmutableList.of(1), 2)) {
            assertThat(iterator.next()).isEqualTo(1);
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void closeClosesUnderlyingIterator() {
        final RecordingCloseableIterator<Integer> in = new RecordingCloseableIterator<>(ImmutableList.of(1, 2, 3));
        new AlmostSortedAsyncIterator<>(in, Comparator.naturalOrder(), 2, EXECUTOR).close();
        assertThat(in.closed).isTrue();
        assertThat(in.cancelled).isFalse();
    }

    @Test
    void cancelCancelsUnderlyingIterator() {
        final RecordingCloseableIterator<Integer> in = new RecordingCloseableIterator<>(ImmutableList.of(1, 2, 3));
        new AlmostSortedAsyncIterator<>(in, Comparator.naturalOrder(), 2, EXECUTOR).cancel();
        assertThat(in.cancelled).isTrue();
    }

    @Test
    void drainsCorrectlyWhenInputCompletesAsynchronously() {
        // The input's onHasNext() completes only after a delay, exercising the "input not ready yet" branch of
        // computeNextRecord (where it must wait on the pending future rather than reading it with getNow).
        final AlmostSortedAsyncIterator<Integer> iterator = new AlmostSortedAsyncIterator<>(
                new DelayingAsyncIterator<>(ImmutableList.of(3, 1, 2, 5, 4)),
                Comparator.naturalOrder(), 5, EXECUTOR);
        assertThat(drain(iterator)).containsExactly(1, 2, 3, 4, 5);
    }

    // ------------------------------------------------------------------------------------------------------------

    @Nonnull
    private static AlmostSortedAsyncIterator<Integer> almostSorted(@Nonnull final List<Integer> input,
                                                                   final int maxQueueSize) {
        return new AlmostSortedAsyncIterator<>(new NonAsyncIterator<>(input.iterator()),
                Comparator.naturalOrder(), maxQueueSize, EXECUTOR);
    }

    @Nonnull
    private static <T> List<T> drain(@Nonnull final AsyncIterator<T> iterator) {
        final List<T> result = new ArrayList<>();
        while (iterator.onHasNext().join()) {
            result.add(iterator.next());
        }
        return result;
    }

    /** A closeable {@link AsyncIterator} over a list that records whether it was closed or cancelled. */
    private static final class RecordingCloseableIterator<T> implements CloseableAsyncIterator<T> {
        @Nonnull
        private final Iterator<T> underlying;
        boolean closed;
        boolean cancelled;

        RecordingCloseableIterator(@Nonnull final List<T> items) {
            this.underlying = items.iterator();
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return CompletableFuture.completedFuture(underlying.hasNext());
        }

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public T next() {
            return underlying.next();
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * An {@link AsyncIterator} over a list whose {@link #onHasNext()} completes only after a small delay, and that
     * caches the pending answer until {@link #next()} consumes it (as a real async iterator does).
     */
    private static final class DelayingAsyncIterator<T> implements AsyncIterator<T> {
        @Nonnull
        private final Iterator<T> underlying;
        @Nullable
        private CompletableFuture<Boolean> pending;

        DelayingAsyncIterator(@Nonnull final List<T> items) {
            this.underlying = items.iterator();
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            if (pending == null) {
                pending = MoreAsyncUtil.delayedFuture(2, TimeUnit.MILLISECONDS)
                        .thenApply(ignored -> underlying.hasNext());
            }
            return pending;
        }

        @Override
        public boolean hasNext() {
            return onHasNext().join();
        }

        @Override
        public T next() {
            pending = null;
            return underlying.next();
        }

        @Override
        public void cancel() {
        }
    }
}
