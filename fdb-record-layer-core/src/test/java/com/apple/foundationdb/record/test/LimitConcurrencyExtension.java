/*
 * LimitConcurrency.java
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

package com.apple.foundationdb.record.test;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.Semaphore;

/**
 * Limit the concurrency between tests when concurrency is enabled.
 * <p>
 *     This is a workaround for: <a href="https://github.com/junit-team/junit5/issues/3108">junit issue #3108</a>,
 *     but tldr all of our code forks into the executor, which is a {@link java.util.concurrent.ForkJoinPool}, so the
 *     {@code ForkJoinPool} that is running tests thinks it can have more threads, and ends up starting nearly all the
 *     tests in parallel, and then, some of them try to join a future, and fail because of a
 *     {@link java.util.concurrent.RejectedExecutionException}.
 * </p>
 * <p>
 *     Note: I tried to do a similar solution as described in that issue using an
 *     {@link org.junit.jupiter.api.extension.InvocationInterceptor}, but the
 *     {@link java.util.concurrent.TimeoutException} failed all the tests because it wasn't in the chain.
 *     <pre>
 *         Chain of InvocationInterceptors never called invocation: org.junit.jupiter.engine.extension.TimeoutExtension,
 *         com.apple.foundationdb.record.lucene.LuceneIndexTest$1org.junit.platform.commons.JUnitException:
 *         Chain of InvocationInterceptors never called invocation: org.junit.jupiter.engine.extension.TimeoutExtension,
 *         com.apple.foundationdb.record.lucene.LuceneIndexTest$1
 *     </pre>
 * </p>
 */
public class LimitConcurrencyExtension implements AfterEachCallback, BeforeEachCallback {
    /**
     * Number of tests to allow to run concurrently.
     * <p>
     *     I tried to run {@code LuceneIndexTest} with
     *     <code>
     *         junit.jupiter.execution.parallel.config.strategy=dynamic
     *     </code>
     *     And tests seemed to timeout and freeze up. But it seems to reliably be ok with this set to {@code 10}, and
     *     <code>
     *         junit.jupiter.execution.parallel.config.fixed.parallelism=10
     *         junit.jupiter.execution.parallel.config.fixed.max-pool-size=20
     *     </code>
     *     Perhaps more experimentation is warranted here to allow us to run with more concurrency on more powerful
     *     hosts, or perhaps we should run with concurrency at a different level.
     * </p>
     */
    static Semaphore testConcurrency = new Semaphore(Runtime.getRuntime().availableProcessors());

    @Override
    public void beforeEach(final ExtensionContext context) throws Exception {
        testConcurrency.acquire();
    }

    @Override
    public void afterEach(final ExtensionContext context) {
        testConcurrency.release();
    }
}
