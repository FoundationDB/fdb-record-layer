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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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

    @Test
    public void executeDelayedCallbackOnExecutor() throws ExecutionException, InterruptedException {
        String callbackThreadName = MoreAsyncUtil.delayedFuture(5, TimeUnit.MILLISECONDS)
                .thenApply(ignore -> Thread.currentThread().getName())
                .get();
        assertThat("Callback should have been executed on thread started by default scheduled executor",
                callbackThreadName, startsWith("fdb-scheduled"));

        ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("test-delayed-executor-thread-%d")
                .build());
        try {
            final String customExecutorThreadName = MoreAsyncUtil.delayedFuture(5, TimeUnit.MILLISECONDS, scheduledExecutor)
                    .thenApply(ignore -> Thread.currentThread().getName())
                    .get();
            assertEquals("test-delayed-executor-thread-0", customExecutorThreadName);
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
                    callbackThreadName, either(equalTo("test-deadline-exceeded-thread-0")).or(equalTo(Thread.currentThread().getName())));
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
        return Arrays.stream(FutureBehavior.values())
                .flatMap(future1 ->
                        Arrays.stream(FutureBehavior.values())
                                .map(future2 -> Arguments.of(future1, future2)));
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

}
