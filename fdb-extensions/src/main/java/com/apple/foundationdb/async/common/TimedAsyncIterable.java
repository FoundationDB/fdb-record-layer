/*
 * TimedAsyncIterable.java
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Wraps an {@link AsyncIterable} so the wall-clock time spent <em>awaiting</em> its elements is measured and reported
 * once the scan finishes. A range read (an {@code AsyncIterable} produced by {@code getRange}) has no single
 * completion to time the way a point read's {@link CompletableFuture} does: it is consumed lazily, batch by batch, by
 * whoever drains it. This wrapper times both ways a consumer can drain it and reports the elapsed nanos to a
 * {@link LongConsumer} exactly once per consumption:
 * <ul>
 *   <li>via {@link AsyncIterable#iterator()}: accumulates the time each {@code onHasNext}/{@code hasNext} spends
 *       waiting for the next batch and reports the running total when the iterator is exhausted, fails, or is
 *       cancelled;</li>
 *   <li>via {@link AsyncIterable#asList()}: times that single future directly (preserving the native, optimized
 *       collect path rather than routing it through {@link AsyncIterable#iterator()}).</li>
 * </ul>
 * It exists so a read listener can turn "time this range scan" into a single call rather than re-implementing an
 * {@link AsyncIterator} wrapper at every site.
 */
public final class TimedAsyncIterable {
    private TimedAsyncIterable() {
        // utility class
    }

    /**
     * Returns an {@link AsyncIterable} that behaves exactly like {@code iterable} but reports, once per consumption,
     * the total nanos spent awaiting the scan. Draining via {@link AsyncIterable#iterator()} reports on exhaustion,
     * error, or {@link AsyncIterator#cancel()} (an iterator simply abandoned without any of those never reports);
     * draining via {@link AsyncIterable#asList()} reports when that future completes.
     *
     * @param iterable the iterable to time
     * @param elapsedNanosConsumer receives the accumulated await time in nanoseconds
     * @param <T> the element type
     *
     * @return a timing wrapper around {@code iterable}
     */
    @Nonnull
    public static <T> AsyncIterable<T> wrap(@Nonnull final AsyncIterable<T> iterable,
                                            @Nonnull final LongConsumer elapsedNanosConsumer) {
        return new TimedIterable<>(iterable, elapsedNanosConsumer);
    }

    private record TimedIterable<T>(@Nonnull AsyncIterable<T> delegate, @Nonnull LongConsumer elapsedNanosConsumer)
            implements AsyncIterable<T> {

        @Nonnull
        @Override
        public AsyncIterator<T> iterator() {
            return new TimedAsyncIterator<>(delegate.iterator(), elapsedNanosConsumer);
        }

        @Nonnull
        @Override
        public CompletableFuture<List<T>> asList() {
            final long startNanos = System.nanoTime();
            return delegate.asList().whenComplete((list, error) ->
                    elapsedNanosConsumer.accept(System.nanoTime() - startNanos));
        }
    }

    private static final class TimedAsyncIterator<T> implements AsyncIterator<T> {
        @Nonnull
        private final AsyncIterator<T> delegate;
        @Nonnull
        private final LongConsumer elapsedNanosConsumer;
        @Nonnull
        private final AtomicLong elapsedNanos = new AtomicLong();
        @Nonnull
        private final AtomicBoolean reported = new AtomicBoolean();

        private TimedAsyncIterator(@Nonnull final AsyncIterator<T> delegate,
                                   @Nonnull final LongConsumer elapsedNanosConsumer) {
            this.delegate = delegate;
            this.elapsedNanosConsumer = elapsedNanosConsumer;
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            final long startNanos = System.nanoTime();
            return delegate.onHasNext().whenComplete((hasNext, error) -> {
                elapsedNanos.addAndGet(System.nanoTime() - startNanos);
                if (error != null || !Boolean.TRUE.equals(hasNext)) {
                    report();
                }
            });
        }

        @Override
        public boolean hasNext() {
            final long startNanos = System.nanoTime();
            final boolean hasNext = delegate.hasNext();
            elapsedNanos.addAndGet(System.nanoTime() - startNanos);
            if (!hasNext) {
                report();
            }
            return hasNext;
        }

        @Override
        public T next() {
            return delegate.next();
        }

        @Override
        public void cancel() {
            delegate.cancel();
            report();
        }

        private void report() {
            if (reported.compareAndSet(false, true)) {
                elapsedNanosConsumer.accept(elapsedNanos.get());
            }
        }
    }
}
