/*
 * TimedAsyncIterableTest.java
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
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TimedAsyncIterable}: it must pass the wrapped elements through unchanged and report the
 * accumulated await time exactly once — on full drain, on an empty scan, and on cancellation.
 */
class TimedAsyncIterableTest {

    @Test
    void passesElementsThroughAndReportsOnceAfterFullDrain() {
        final AtomicInteger reportCount = new AtomicInteger();
        final AtomicLong reportedNanos = new AtomicLong(-1L);
        final AsyncIterable<Integer> wrapped = TimedAsyncIterable.wrap(asyncIterableOf(ImmutableList.of(1, 2, 3)),
                nanos -> {
                    reportCount.incrementAndGet();
                    reportedNanos.set(nanos);
                });

        assertThat(drain(wrapped.iterator())).containsExactly(1, 2, 3);
        assertThat(reportCount.get()).as("reported exactly once on exhaustion").isEqualTo(1);
        assertThat(reportedNanos.get()).as("accumulated await time is non-negative").isGreaterThanOrEqualTo(0L);
    }

    @Test
    void reportsOnceForEmptyIterable() {
        final AtomicInteger reportCount = new AtomicInteger();
        final AsyncIterable<Integer> wrapped =
                TimedAsyncIterable.wrap(asyncIterableOf(ImmutableList.of()), nanos -> reportCount.incrementAndGet());

        assertThat(drain(wrapped.iterator())).isEmpty();
        assertThat(reportCount.get()).isEqualTo(1);
    }

    @Test
    void reportsOnceOnCancelBeforeExhaustion() {
        final AtomicInteger reportCount = new AtomicInteger();
        final AsyncIterator<Integer> iterator =
                TimedAsyncIterable.wrap(asyncIterableOf(ImmutableList.of(1, 2, 3)),
                        nanos -> reportCount.incrementAndGet()).iterator();

        assertThat(iterator.onHasNext().join()).isTrue();   // advance without exhausting: no report yet
        assertThat(reportCount.get()).isZero();

        iterator.cancel();
        assertThat(reportCount.get()).as("cancel reports once").isEqualTo(1);
        iterator.cancel();
        assertThat(reportCount.get()).as("further cancels are idempotent").isEqualTo(1);
    }

    @Test
    void asListPassesThroughAndReportsOnce() {
        final AtomicInteger reportCount = new AtomicInteger();
        final AtomicLong reportedNanos = new AtomicLong(-1L);
        final AsyncIterable<Integer> wrapped = TimedAsyncIterable.wrap(asyncIterableOf(ImmutableList.of(4, 5, 6)),
                nanos -> {
                    reportCount.incrementAndGet();
                    reportedNanos.set(nanos);
                });

        assertThat(wrapped.asList().join()).containsExactly(4, 5, 6);
        assertThat(reportCount.get()).as("asList reports exactly once").isEqualTo(1);
        assertThat(reportedNanos.get()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void reportsOnceWhenDrainedViaSynchronousHasNext() {
        final AtomicInteger reportCount = new AtomicInteger();
        final AtomicLong reportedNanos = new AtomicLong(-1L);
        final AsyncIterator<Integer> iterator = TimedAsyncIterable.wrap(asyncIterableOf(ImmutableList.of(1, 2, 3)),
                nanos -> {
                    reportCount.incrementAndGet();
                    reportedNanos.set(nanos);
                }).iterator();

        // Drive the synchronous hasNext()/next() path rather than onHasNext().
        final List<Integer> collected = new ArrayList<>();
        while (iterator.hasNext()) {
            collected.add(iterator.next());
        }

        assertThat(collected).containsExactly(1, 2, 3);
        assertThat(reportCount.get()).as("synchronous drain reports exactly once on exhaustion").isEqualTo(1);
        assertThat(reportedNanos.get()).isGreaterThanOrEqualTo(0L);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void accumulatesAtLeastTheUpstreamAwaitLatency(final long randomSeed) {
        final Random random = new Random(randomSeed);
        // A short sequence of smallish positive ints; each element's onHasNext() injects 10 * value ms of latency.
        final ImmutableList.Builder<Integer> sequenceBuilder = ImmutableList.builder();
        for (int i = 0; i < 5; i++) {
            sequenceBuilder.add(random.nextInt(5) + 1); // [1, 5]
        }
        final List<Integer> sequence = sequenceBuilder.build();
        final int totalValue = sequence.stream().mapToInt(Integer::intValue).sum();
        final long expectedMinNanos = TimeUnit.MILLISECONDS.toNanos(10L * totalValue);

        final AtomicLong reportedNanos = new AtomicLong(-1L);
        final AsyncIterable<Integer> wrapped = TimedAsyncIterable.wrap(delayedIterableOf(sequence), reportedNanos::set);

        assertThat(drain(wrapped.iterator())).containsExactlyElementsOf(sequence);
        assertThat(reportedNanos.get())
                .as("accumulated await time must be at least the injected upstream latency (10ms * sum=%d)", totalValue)
                .isGreaterThanOrEqualTo(expectedMinNanos);
    }

    @Nonnull
    private static List<Integer> drain(@Nonnull final AsyncIterator<Integer> iterator) {
        final List<Integer> collected = new ArrayList<>();
        while (Boolean.TRUE.equals(iterator.onHasNext().join())) {
            collected.add(iterator.next());
        }
        return collected;
    }

    /** A minimal synchronous {@link AsyncIterable} over a fixed list, with a no-op {@code cancel}. */
    @Nonnull
    private static AsyncIterable<Integer> asyncIterableOf(@Nonnull final List<Integer> items) {
        return delayedIterableOf(items, false);
    }

    /**
     * An {@link AsyncIterable} over a fixed list. When {@code delayed}, each {@code onHasNext()} that precedes an
     * element injects {@code 10 * value} ms of latency — modeling the batch-fetch wait that
     * {@link TimedAsyncIterable} measures (the latency lives in the awaited {@code onHasNext}, not in {@code next}).
     */
    @Nonnull
    private static AsyncIterable<Integer> delayedIterableOf(@Nonnull final List<Integer> items) {
        return delayedIterableOf(items, true);
    }

    @Nonnull
    private static AsyncIterable<Integer> delayedIterableOf(@Nonnull final List<Integer> items, final boolean delayed) {
        return new AsyncIterable<>() {
            @Nonnull
            @Override
            public AsyncIterator<Integer> iterator() {
                return new AsyncIterator<>() {
                    private int index;

                    @Override
                    public CompletableFuture<Boolean> onHasNext() {
                        if (index >= items.size()) {
                            return CompletableFuture.completedFuture(false);
                        }
                        if (!delayed) {
                            return CompletableFuture.completedFuture(true);
                        }
                        // Simulated fetch latency for the element about to be served.
                        return MoreAsyncUtil.delayedFuture(10L * items.get(index), TimeUnit.MILLISECONDS)
                                .thenApply(ignored -> true);
                    }

                    @Override
                    public boolean hasNext() {
                        return index < items.size();
                    }

                    @Override
                    public Integer next() {
                        return items.get(index++);
                    }

                    @Override
                    public void cancel() {
                        // nothing to release
                    }
                };
            }

            @Nonnull
            @Override
            public CompletableFuture<List<Integer>> asList() {
                return CompletableFuture.completedFuture(ImmutableList.copyOf(items));
            }
        };
    }
}
