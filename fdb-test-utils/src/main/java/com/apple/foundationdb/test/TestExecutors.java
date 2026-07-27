/*
 * TestExecutors.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.test;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executors to use during testing.
 */
public final class TestExecutors {
    @Nonnull
    private static final Executor DEFAULT_THREAD_POOL = newThreadPool("fdb-unit-test");
    @Nonnull
    private static final ScheduledExecutorService DEFAULT_SCHEDULED_THREAD_POOL =
            newScheduledThreadPool("fdb-unit-test-scheduled");

    private TestExecutors() {
    }

    /**
     * Thread factory for creating threads used by test thread pools.
     */
    public static class TestThreadFactory implements ThreadFactory {
        private final String namePrefix;
        private final AtomicInteger count;

        public TestThreadFactory(@Nonnull String namePrefix) {
            this.namePrefix = namePrefix;
            this.count = new AtomicInteger();
        }

        @Override
        public Thread newThread(final Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(namePrefix + "-" + count.incrementAndGet());
            return t;
        }
    }

    public static Executor newThreadPool(@Nonnull String namePrefix) {
        return Executors.newCachedThreadPool(new TestThreadFactory(namePrefix));
    }

    @Nonnull
    public static Executor defaultThreadPool() {
        return DEFAULT_THREAD_POOL;
    }

    /**
     * Create a new {@link ScheduledExecutorService} with at least four threads (or one per
     * available CPU, whichever is larger). Test code that installs a scheduler on an
     * {@code FDBDatabaseFactory} should use this rather than the single-thread scheduler
     * {@code MoreAsyncUtil} installs by default — under JUnit-parallel execution the
     * single-thread default falls behind, causing {@code DeadlineExceededException}s from
     * {@code AsyncLoadingCache}.
     *
     * @param namePrefix prefix for thread names created by this pool
     * @return a fresh scheduled executor sized for the current JVM
     */
    @Nonnull
    private static ScheduledExecutorService newScheduledThreadPool(@Nonnull String namePrefix) {
        return Executors.newScheduledThreadPool(
                Math.max(Runtime.getRuntime().availableProcessors(), 4),
                new TestThreadFactory(namePrefix));
    }

    /**
     * JVM-wide singleton scheduled executor for test code. Many test extensions install this as the scheduled executor
     * on their own {@code FDBDatabaseFactory}.
     *
     * @return the shared singleton scheduled executor
     */
    @Nonnull
    public static ScheduledExecutorService defaultScheduledThreadPool() {
        return DEFAULT_SCHEDULED_THREAD_POOL;
    }
}
