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
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
