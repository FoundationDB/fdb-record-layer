/*
 * MoreAsyncUtilTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async;

import com.apple.foundationdb.test.TestExecutors;
import com.apple.test.ParameterizedTestUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MoreAsyncUtil}.
 */
public class MoreAsyncUtilTest {
    static final Executor EXECUTOR = TestExecutors.defaultThreadPool();

    int count;

    @Test
    // Run to 1_000_000 with -Xms16m -Xmx16m -XX:+HeapDumpOnOutOfMemoryError
    // It used to OOM with long reachable chains of CompletableFuture's.
    public void slowLoop() {
        count = 1000;
        // Use thenApplyAsync to deliberately introduce a delay.
        AsyncUtil.whileTrue(() -> CompletableFuture.completedFuture(--count).thenApplyAsync(c -> c > 0), EXECUTOR).join();
        assertEquals(0, count, "should count down to zero");
    }

    @Test
    public void completedNormally() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        assertFalse(MoreAsyncUtil.isCompletedNormally(future));
        future.complete(null);
        assertTrue(MoreAsyncUtil.isCompletedNormally(future));
        future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("FATAL ERROR"));
        assertFalse(MoreAsyncUtil.isCompletedNormally(future));
    }

    @Test
    public void delaySimple() {
        long start = System.currentTimeMillis();
        CompletableFuture<Void> delayed = MoreAsyncUtil.delayedFuture(30, TimeUnit.MILLISECONDS);
        delayed.join();
        long end = System.currentTimeMillis();
        assertTrue(end - start >= 30, "Delay was not long enough");
    }

    @Test
    public void manyParallelDelay() {
        long start = System.currentTimeMillis();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[1000];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = MoreAsyncUtil.delayedFuture(i * 100, TimeUnit.MICROSECONDS);
        }
        CompletableFuture.allOf(futures).join();
        long end = System.currentTimeMillis();
        assertTrue(end - start >= 100, "Delay was not long enough");
    }

    /**
     * Assert that callbacks on {@link MoreAsyncUtil#delayedFuture(long, TimeUnit, ScheduledExecutorService)}
     * are executed on a scheduled executor service. Note that if the delayed future completes before the
     * callback is added (which can happen), the callbacks can actually be executed on the calling main thread.
     * For that reason, the asserts here also tolerate the callback being executed on the calling thread. The
     * main thing it is trying to test is that: (1) if a custom executor is passed, it does not use the default
     * executor, and (2) it doesn't use some other thread pool or executor service.
     *
     * @throws ExecutionException from waiting on futures
     * @throws InterruptedException from while waiting on futures
     */
    @Test
    public void executeDelayedCallbackOnExecutor() throws ExecutionException, InterruptedException {
        String callbackThreadName = MoreAsyncUtil.delayedFuture(5, TimeUnit.MILLISECONDS)
                .thenApply(ignore -> Thread.currentThread().getName())
                .get();
        assertThat("Callback should have been executed on thread started by default scheduled executor",
                callbackThreadName, isCurrentThreadNameOr(startsWith("fdb-scheduled-executor-")));

        ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("test-delayed-executor-thread-%d")
                .build());
        try {
            final String customExecutorThreadName = MoreAsyncUtil.delayedFuture(5, TimeUnit.MILLISECONDS, scheduledExecutor)
                    .thenApply(ignore -> Thread.currentThread().getName())
                    .get();
            assertThat(customExecutorThreadName, isCurrentThreadNameOr("test-delayed-executor-thread-0"));
        } finally {
            scheduledExecutor.shutdown();
        }
    }

    @Test
    public void getWithDeadlineRunsOnExecutor() throws ExecutionException, InterruptedException {
        ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("test-deadline-exceeded-thread-%d")
                .build());

        try {
            String callbackThreadName = MoreAsyncUtil.getWithDeadline(5, () -> new CompletableFuture<String>(), scheduledExecutor)
                    .exceptionally(err -> {
                        if (err instanceof ExecutionException || err instanceof CompletionException) {
                            err = err.getCause();
                        }
                        assertInstanceOf(MoreAsyncUtil.DeadlineExceededException.class, err);
                        return Thread.currentThread().getName();
                    })
                    .get();

            // Most of the time, the callback should come fom the scheduledExecutor. However, if there's some hiccup and actually
            // setting up the future chain takes longer than the deadline time, then it's possible for the callback to complete on
            // the test worker thread
            assertThat("Callback should have been executed on thread managed by scheduled executor or by calling thread",
                    callbackThreadName, isCurrentThreadNameOr("test-deadline-exceeded-thread-0"));
        } finally {
            scheduledExecutor.shutdown();
        }
    }

    // This test can take about 9 seconds as threads get eaten up running the sleep
    // future logic. It is included mainly to show that the other implementation is a lot
    // faster with many parallel things.
    //@Test
    public void manyParallelNaive() {
        long start = System.currentTimeMillis();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[1000];
        for (int i = 0; i < futures.length; i++) {
            int index = i;
            futures[i] = CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(index / 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return null;
            });
        }
        CompletableFuture.allOf(futures).join();
        long end = System.currentTimeMillis();
        assertTrue(end - start >= 100, "Delay was not long enough");
    }

    enum FutureBehavior {
        SucceedInstantly(true, false, CompletableFuture::completedFuture),
        SucceedSlowly(true, false, result -> MoreAsyncUtil.delayedFuture(100, TimeUnit.MILLISECONDS)
                .thenApply(vignore -> result)),
        RunForever(false, false, result -> new CompletableFuture<>()),
        FailInstantly(false, true, result -> {
            final CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException(result));
            return future;
        }),
        FailSlowly(false, true, result -> {
            final CompletableFuture<String> future = new CompletableFuture<>();
            MoreAsyncUtil.delayedFuture(100, TimeUnit.MILLISECONDS)
                    .whenComplete((vignore, e) -> future.completeExceptionally(new RuntimeException(result)));
            return future;
        });

        private final boolean succeeds;
        private final boolean fails;
        private final Function<String, CompletableFuture<String>> futureGenerator;

        FutureBehavior(final boolean succeeds, final boolean fails,
                       final Function<String, CompletableFuture<String>> futureGenerator) {
            this.succeeds = succeeds;
            this.fails = fails;
            this.futureGenerator = futureGenerator;
        }
    }

    public static Stream<Arguments> combineAndFailFast() {
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(FutureBehavior.values()),
                Arrays.stream(FutureBehavior.values())
        );
    }

    @ParameterizedTest
    @MethodSource
    void combineAndFailFast(FutureBehavior behavior1, FutureBehavior behavior2)
            throws ExecutionException, InterruptedException, TimeoutException {
        final CompletableFuture<String> future1 = behavior1.futureGenerator.apply("a");
        final CompletableFuture<String> future2 = behavior2.futureGenerator.apply("b");
        final CompletableFuture<String> future = MoreAsyncUtil.combineAndFailFast(future1, future2, (a, b) -> a + "-" + b);
        final int getTimeoutSeconds = 1;
        if (behavior1.succeeds && behavior2.succeeds) {
            assertEquals("a-b", future.get(getTimeoutSeconds, TimeUnit.SECONDS));
        } else if (behavior1.fails || behavior2.fails) {
            final ExecutionException executionException = assertThrows(ExecutionException.class,
                    () -> future.get(getTimeoutSeconds, TimeUnit.SECONDS));
            assertEquals(RuntimeException.class, executionException.getCause().getClass());
        } else {
            assertThrows(TimeoutException.class, () -> future.get(getTimeoutSeconds, TimeUnit.SECONDS));
        }


    }

    @Test
    void swallowException() throws ExecutionException, InterruptedException {
        RuntimeException runtimeException1 = new RuntimeException();

        {
            CompletableFuture<Void> completedExceptionally1 = new CompletableFuture<>();
            completedExceptionally1.completeExceptionally(runtimeException1);

            assertSwallowedOrNot(completedExceptionally1, runtimeException1);
        }
        {
            final CompletableFuture<Void> runAsync = CompletableFuture.runAsync(() -> {
                throw runtimeException1;
            });

            assertSwallowedOrNot(runAsync, runtimeException1);
        }
        {
            // the following should not throw
            // successful future
            MoreAsyncUtil.swallowException(CompletableFuture.completedFuture(null),
                    throwable -> true).get();
            MoreAsyncUtil.swallowException(CompletableFuture.runAsync(() -> { }),
                    throwable -> true).get();
            // successful future with bad handler
            MoreAsyncUtil.swallowException(CompletableFuture.completedFuture(null),
                    throwable -> {
                        throw new RuntimeException();
                    }).get();
        }


    }

    private static void assertSwallowedOrNot(final CompletableFuture<Void> completedExceptionally1, final RuntimeException runtimeException1) throws InterruptedException, ExecutionException {
        MoreAsyncUtil.swallowException(completedExceptionally1,
                throwable -> throwable.equals(runtimeException1)).get(); // should not throw
        final CompletableFuture<Void> notSwallowed = MoreAsyncUtil.swallowException(completedExceptionally1,
                throwable -> false);
        final ExecutionException executionException = assertThrows(ExecutionException.class, notSwallowed::get);
        assertEquals(runtimeException1, executionException.getCause());
    }

    @Test
    void consumeDrainsAllElements() {
        final Collection<Integer> elements = Arrays.asList(1, 2, 3, 4, 5);
        final AtomicInteger visited = new AtomicInteger();
        // mapIterable is lazy: the counter only advances as elements are pulled, so it measures what consume drains.
        final AsyncIterable<Integer> iterable = AsyncUtil.mapIterable(
                MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(elements), EXECUTOR),
                element -> {
                    visited.incrementAndGet();
                    return element;
                });

        assertNull(MoreAsyncUtil.consume(iterable, EXECUTOR).join(), "consume completes with a null Void result");
        assertEquals(elements.size(), visited.get(), "consume visits every element");
    }

    @Test
    void consumeEmptyIterableCompletes() {
        final Collection<Integer> elements = Arrays.asList();
        final AtomicInteger visited = new AtomicInteger();
        final AsyncIterable<Integer> iterable = AsyncUtil.mapIterable(
                MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(elements), EXECUTOR),
                element -> {
                    visited.incrementAndGet();
                    return element;
                });

        assertNull(MoreAsyncUtil.consume(iterable, EXECUTOR).join(), "consume of an empty iterable still completes");
        assertEquals(0, visited.get(), "an empty iterable has nothing to visit");
    }

    @Test
    void consumeRemainingDrainsFromCurrentPosition() {
        final Collection<Integer> elements = Arrays.asList(1, 2, 3, 4, 5);
        final AtomicInteger visited = new AtomicInteger();
        final AsyncIterator<Integer> iterator = AsyncUtil.mapIterable(
                MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(elements), EXECUTOR),
                element -> {
                    visited.incrementAndGet();
                    return element;
                }).iterator();

        // Pull the first element by hand; consumeRemaining should drain only what is left after the current position.
        assertTrue(iterator.onHasNext().join());
        assertEquals(1, (int) iterator.next());
        assertEquals(1, visited.get());

        assertNull(MoreAsyncUtil.consumeRemaining(iterator, EXECUTOR).join(),
                "consumeRemaining completes with a null Void result");
        assertEquals(elements.size(), visited.get(), "consumeRemaining drains the rest of the iterator");
    }

    @Test
    void forLoopAccumulatesAndThreadsState() {
        final List<Integer> visited = new ArrayList<>();
        // sum 1..5, threading the running total from one iteration into the next.
        final int result = MoreAsyncUtil.forLoop(1, 0,
                i -> i <= 5,
                i -> i + 1,
                (i, acc) -> {
                    visited.add(i);
                    return CompletableFuture.completedFuture(acc + i);
                },
                EXECUTOR).join();
        assertEquals(15, result, "body result of the final iteration");
        assertEquals(Arrays.asList(1, 2, 3, 4, 5), visited, "loop variable visited each value in order");
    }

    @Test
    void forLoopWithFalseConditionReturnsInitialAndSkipsBody() {
        final AtomicInteger bodyCalls = new AtomicInteger();
        final String result = MoreAsyncUtil.forLoop(5, "initial",
                i -> i < 0, // false from the start
                i -> i + 1,
                (i, acc) -> {
                    bodyCalls.incrementAndGet();
                    return CompletableFuture.completedFuture("changed");
                },
                EXECUTOR).join();
        assertEquals("initial", result, "a loop that never runs returns the initial state unchanged");
        assertEquals(0, bodyCalls.get(), "the body must not be invoked");
    }

    @Test
    void forLoopBiPredicateStopsOnAccumulatedResult() {
        final List<Integer> visited = new ArrayList<>();
        // BiPredicate overload: the condition inspects the accumulated result, not just the loop variable.
        final int result = MoreAsyncUtil.forLoop(0, 0,
                (i, acc) -> acc < 10,
                i -> i + 1,
                (i, acc) -> {
                    visited.add(i);
                    return CompletableFuture.completedFuture(acc + i);
                },
                EXECUTOR).join();
        assertEquals(10, result, "loop stops once the accumulated result reaches the threshold");
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), visited, "iterations run until the accumulator crosses 10");
    }

    @Test
    void forEachReturnsResultsInInputOrder() {
        final List<Integer> items = Arrays.asList(1, 2, 3, 4, 5);
        final List<Integer> result = MoreAsyncUtil.forEach(items,
                i -> CompletableFuture.completedFuture(i * i), 2, EXECUTOR).join();
        assertEquals(Arrays.asList(1, 4, 9, 16, 25), result);
    }

    @Test
    void forEachPreservesOrderWhenBodiesCompleteOutOfOrder() {
        final List<Integer> items = Arrays.asList(0, 1, 2, 3, 4);
        final List<CompletableFuture<Integer>> perItem = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            perItem.add(new CompletableFuture<>());
        }
        // Enough parallelism to start every body; each returns an as-yet-incomplete future.
        final CompletableFuture<List<Integer>> resultFuture =
                MoreAsyncUtil.forEach(items, perItem::get, items.size(), EXECUTOR);
        // Complete them in reverse order; the result must still come back in input order.
        for (int i = items.size() - 1; i >= 0; i--) {
            perItem.get(i).complete(i * 10);
        }
        assertEquals(Arrays.asList(0, 10, 20, 30, 40), resultFuture.join(),
                "results are ordered by input position, not completion order");
    }

    @Test
    void forEachRespectsParallelismBound() {
        final int parallelism = 3;
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final List<Integer> items = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
        final List<Integer> result = MoreAsyncUtil.forEach(items,
                i -> {
                    maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                    return MoreAsyncUtil.delayedFuture(20, TimeUnit.MILLISECONDS)
                            .thenApply(ignored -> {
                                inFlight.decrementAndGet();
                                return i;
                            });
                }, parallelism, EXECUTOR).join();
        assertEquals(items, result, "every item is processed, in order");
        assertTrue(maxInFlight.get() <= parallelism,
                "never more than 'parallelism' bodies in flight, saw " + maxInFlight.get());
    }

    @Test
    void forEachWithParallelismOneIsSequential() {
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final List<Integer> items = Arrays.asList(0, 1, 2, 3, 4);
        MoreAsyncUtil.forEach(items,
                i -> {
                    maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                    return MoreAsyncUtil.delayedFuture(5, TimeUnit.MILLISECONDS)
                            .thenApply(ignored -> {
                                inFlight.decrementAndGet();
                                return i;
                            });
                }, 1, EXECUTOR).join();
        assertEquals(1, maxInFlight.get(), "parallelism 1 runs strictly one body at a time");
    }

    @Test
    void forEachHandlesNullItems() {
        final List<String> items = Arrays.asList("a", null, "b");
        final List<String> result = MoreAsyncUtil.forEach(items,
                s -> CompletableFuture.completedFuture(s == null ? "<null>" : s + "!"),
                2, EXECUTOR).join();
        assertEquals(Arrays.asList("a!", "<null>", "b!"), result,
                "null items are passed through to the body, results stay in order");
    }

    @Test
    void forEachEmptyReturnsEmptyList() {
        final List<Integer> result = MoreAsyncUtil.forEach(new ArrayList<Integer>(),
                i -> CompletableFuture.completedFuture(i * i), 4, EXECUTOR).join();
        assertTrue(result.isEmpty(), "no items yields an empty result list");
    }

    @Test
    void forEachRejectsNonPositiveParallelism() {
        assertThrows(IllegalArgumentException.class,
                () -> MoreAsyncUtil.forEach(Arrays.asList(1, 2, 3),
                        CompletableFuture::completedFuture, 0, EXECUTOR));
    }

    @Test
    void mapIterablePipelinedMapsInInputOrder() {
        final List<Integer> result = AsyncUtil.collect(
                MoreAsyncUtil.mapIterablePipelined(EXECUTOR, iterableOf(1, 2, 3, 4, 5),
                        i -> CompletableFuture.completedFuture(i * i), 2),
                EXECUTOR).join();
        assertEquals(Arrays.asList(1, 4, 9, 16, 25), result);
    }

    @Test
    void mapIterablePipelinedPreservesOrderWhenMapsCompleteOutOfOrder() {
        final List<Integer> items = Arrays.asList(0, 1, 2, 3, 4);
        final List<CompletableFuture<Integer>> perItem = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            perItem.add(new CompletableFuture<>());
        }
        // pipelineSize == size so every map future is started before any completes.
        final CompletableFuture<List<Integer>> resultFuture = AsyncUtil.collect(
                MoreAsyncUtil.mapIterablePipelined(EXECUTOR,
                        MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(items), EXECUTOR),
                        perItem::get, items.size()),
                EXECUTOR);
        // Complete the map futures in reverse; the pipelined output must still be in input order.
        for (int i = items.size() - 1; i >= 0; i--) {
            perItem.get(i).complete(i * 10);
        }
        assertEquals(Arrays.asList(0, 10, 20, 30, 40), resultFuture.join(),
                "pipelined results are ordered by input position, not completion order");
    }

    @Test
    void mapIterablePipelinedIsLazyInvokingFuncOncePerElement() {
        final AtomicInteger calls = new AtomicInteger();
        final List<Integer> result = AsyncUtil.collect(
                MoreAsyncUtil.mapIterablePipelined(EXECUTOR, iterableOf(1, 2, 3),
                        i -> {
                            calls.incrementAndGet();
                            return CompletableFuture.completedFuture(i + 1);
                        }, 4),
                EXECUTOR).join();
        assertEquals(Arrays.asList(2, 3, 4), result);
        assertEquals(3, calls.get(), "func is applied exactly once per element");
    }

    @Test
    void mapIterablePipelinedEmptyYieldsEmpty() {
        final List<Integer> result = AsyncUtil.collect(
                MoreAsyncUtil.mapIterablePipelined(EXECUTOR, MoreAsyncUtilTest.<Integer>iterableOf(),
                        i -> CompletableFuture.completedFuture(i * i), 2),
                EXECUTOR).join();
        assertTrue(result.isEmpty());
    }

    @Test
    void mapIterablePipelinedDefaultExecutorMapsInInputOrder() {
        // The no-executor overload delegates to the executor overload via ForkJoinPool.commonPool().
        final List<Integer> result = AsyncUtil.collect(
                MoreAsyncUtil.mapIterablePipelined(iterableOf(1, 2, 3, 4, 5),
                        i -> CompletableFuture.completedFuture(i * i), 2),
                EXECUTOR).join();
        assertEquals(Arrays.asList(1, 4, 9, 16, 25), result);
    }

    @Test
    void dedupIterableRemovesOnlyAdjacentDuplicates() {
        // Non-adjacent repeats survive: the trailing 1 is kept because its predecessor (3) differs.
        final List<Integer> result = AsyncUtil.collect(
                MoreAsyncUtil.dedupIterable(EXECUTOR, iterableOf(1, 1, 2, 2, 2, 3, 1)), EXECUTOR).join();
        assertEquals(Arrays.asList(1, 2, 3, 1), result);
    }

    @Test
    void dedupIterableFullyDedupsSortedInput() {
        final List<Integer> result = AsyncUtil.collect(
                MoreAsyncUtil.dedupIterable(EXECUTOR, iterableOf(1, 1, 2, 3, 3, 3)), EXECUTOR).join();
        assertEquals(Arrays.asList(1, 2, 3), result);
    }

    @Test
    void dedupIterableEmptyYieldsEmpty() {
        final List<Integer> result = AsyncUtil.collect(
                MoreAsyncUtil.dedupIterable(EXECUTOR, MoreAsyncUtilTest.<Integer>iterableOf()), EXECUTOR).join();
        assertTrue(result.isEmpty());
    }

    @Test
    void dedupIterableEmitsLeadingNullAndDoesNotCollapseAdjacentNulls() {
        // The dedup filter seeds its "previous" marker with null, so the leading null passes through; and because a
        // null previous element never compares equal, a second adjacent null is not collapsed. This pins that quirk.
        final List<String> result = AsyncUtil.collect(
                MoreAsyncUtil.dedupIterable(EXECUTOR, iterableOf(null, null, "a", "a")), EXECUTOR).join();
        assertEquals(Arrays.asList(null, null, "a"), result);
    }

    @SafeVarargs
    @SuppressWarnings("varargs") // reading the non-reifiable T[] in the body trips -Xlint:varargs; the helper is safe
    private static <T> AsyncIterable<T> iterableOf(final T... items) {
        return MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(Arrays.asList(items)), EXECUTOR);
    }

    @Nonnull
    private static Matcher<String> isCurrentThreadNameOr(@Nonnull String threadName) {
        return isCurrentThreadNameOr(equalTo(threadName));
    }

    @Nonnull
    private static Matcher<String> isCurrentThreadNameOr(@Nonnull Matcher<String> threadMatcher) {
        return either(threadMatcher).or(equalTo(Thread.currentThread().getName()));
    }
}
