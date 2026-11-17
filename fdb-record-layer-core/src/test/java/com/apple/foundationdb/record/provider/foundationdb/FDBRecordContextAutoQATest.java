/*
 * FDBRecordContextAutoQATest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests of the {@link FDBRecordContext} class, focused on commit checks.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class FDBRecordContextAutoQATest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    private FDBDatabase fdb;

    @BeforeEach
    void getFDB() {
        fdb = dbExtension.getDatabase();
    }

    @Test
    void concurrentAddAndRemoveCommitChecks() throws InterruptedException {
        // BUG AQA-10: the commit-check map is guarded by two different locks (this and the map), so concurrent
        // removeCommitChecks and addCommitCheck throw ConcurrentModificationException. Spec: invariant stated by the
        // synchronization in FDBRecordContext and the comment in removeCommitChecks.
        // addCommitCheck and removeCommitChecks are both meant to be thread safe (both synchronize), so adding checks
        // from one thread while removing completed checks from another must not corrupt the commit check map.
        final int count = 200_000;
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final AtomicBoolean adding = new AtomicBoolean(true);
        final CountDownLatch start = new CountDownLatch(1);
        try (FDBRecordContext context = fdb.openContext()) {
            final Thread adder = new Thread(() -> runCatching(errors, () -> {
                start.await();
                try {
                    for (int i = 0; i < count; i++) {
                        context.addCommitCheck("check_" + i, FDBRecordContext.CommitCheckAsync.fromFuture(AsyncUtil.DONE));
                    }
                } finally {
                    adding.set(false);
                }
            }));
            final Thread remover = new Thread(() -> runCatching(errors, () -> {
                start.await();
                while (adding.get()) {
                    final List<CompletableFuture<Void>> removed = context.removeCommitChecks(check -> true, err -> false);
                    AsyncUtil.whenAll(removed).join();
                }
            }));
            adder.start();
            remover.start();
            start.countDown();
            adder.join();
            remover.join();
            if (!errors.isEmpty()) {
                throw new AssertionError("concurrent commit check modification failed", errors.peek());
            }
            AsyncUtil.whenAll(context.removeCommitChecks(check -> true, err -> false)).join();
            assertThat(context.getCommitChecks(check -> true)).isEmpty();
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void runCatching(@Nonnull ConcurrentLinkedQueue<Throwable> errors, @Nonnull ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable t) {
            errors.add(t);
        }
    }
}
