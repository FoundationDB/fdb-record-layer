/*
 * AsyncLoadingCacheTest.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test for {@link AsyncLockRegistry}.
 */
public class AsyncLockRegistryTest {

    @Nonnull
    private final ExecutorService executorService = Executors.newFixedThreadPool(8);

    @Nonnull
    final AsyncLockRegistry registry = new AsyncLockRegistry();

    @Nonnull
    final AsyncLockRegistry.LockIdentifier identifier = new AsyncLockRegistry.LockIdentifier(new Subspace(Tuple.from(1, 2, 3)));

    @Test
    public void orderedWriteTest() {
        final var resource = new ArrayList<Integer>();
        final var writeLockAndWaits = new ArrayList<NonnullPair<CompletableFuture<Void>, CompletableFuture<Void>>>();
        for (int i = 0; i < 1000; i++) {
            writeLockAndWaits.add(getWriteLock());
        }
        final var futures = new ArrayList<CompletableFuture<Void>>();
        for (int i = 1000 - 1; i >= 0; i--) {
            final var finalI = i;
            futures.add(runWithLock(() -> resource.add(finalI), writeLockAndWaits.get(i)));
        }
        checkAllCompletedNormally(futures);
        for (int i = 0; i < 1000; i++) {
            Assertions.assertEquals(i, resource.get(i));
        }
    }

    @Test
    public void sharedReadsExclusiveWriteTest() throws ExecutionException, InterruptedException {
        final var resource = IntStream.range(0, 1000).boxed().collect(Collectors.toList());

        // get 2 read locks, that will be shared
        final var readLockAndWait1 = getReadLock();
        final var readLockAndWait2 = getReadLock();
        // get a write lock, that should wait on the previous reads to finish
        final var writeLockAndWait = getWriteLock();
        // get 2 other read locks, that will be shared and wait for write to finish
        final var readLockAndWait3 = getReadLock();
        final var readLockAndWait4 = getReadLock();

        final var futures = new ArrayList<CompletableFuture<Void>>();
        futures.add(runWithLock(() -> {
            Assertions.assertEquals(2000, resource.size());
            for (int i = 0; i < 2000; i++) {
                Assertions.assertEquals(i, resource.get(i));
            }
        }, readLockAndWait4));
        futures.add(runWithLock(() -> {
            Assertions.assertEquals(2000, resource.size());
            for (int i = 0; i < 2000; i++) {
                Assertions.assertEquals(i, resource.get(i));
            }
        }, readLockAndWait3));
        checkWaiting(futures);
        futures.add(runWithLock(() -> {
            Assertions.assertEquals(1000, resource.size());
            for (int i = 0; i < 1000; i++) {
                Assertions.assertEquals(i, resource.get(i));
            }
        }, readLockAndWait2));
        futures.add(runWithLock(() -> {
            Assertions.assertEquals(1000, resource.size());
            for (int i = 0; i < 1000; i++) {
                Assertions.assertEquals(i, resource.get(i));
            }
        }, readLockAndWait1));
        // checks that the initial 2 reads get to completion
        checkAllCompletedNormally(ImmutableList.of(futures.get(2), futures.get(3)));
        // other 2 are still waiting
        checkWaiting(ImmutableList.of(futures.get(0), futures.get(1)));
        futures.add(runWithLock(() -> {
            Assertions.assertEquals(1000, resource.size());
            for (int i = 0; i < 1000; i++) {
                resource.add(1000 + i);
            }
        }, writeLockAndWait));
        checkAllCompletedNormally(futures);
        AsyncUtil.whenAll(futures).get();
    }

    @Test
    public void writeWaitForReadTest() throws InterruptedException {
        final var readLockAndWait = getReadLock();
        final var writeLockAndWait = getWriteLock();

        // check that the read don't wait
        checkAllCompletedNormally(ImmutableList.of(readLockAndWait.getRight()));
        // check that the write waits
        checkWaiting(ImmutableList.of(writeLockAndWait.getRight()));
        // complete the read and check that the write don't wait now
        readLockAndWait.getLeft().complete(null);
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait.getRight()));
    }

    @Test
    public void writeWaitForMultipleReadsTest() throws InterruptedException {
        // get multiple read locks
        final var readLockAndWait1 = getReadLock();
        final var readLockAndWait2 = getReadLock();
        // get a write lock
        final var writeLockAndWait = getWriteLock();

        // check that the read don't wait
        checkAllCompletedNormally(ImmutableList.of(readLockAndWait1.getRight(), readLockAndWait2.getRight()));
        // check that the write waits
        checkWaiting(ImmutableList.of(writeLockAndWait.getRight()));
        // complete one read and check that the write still waits
        readLockAndWait1.getLeft().complete(null);
        checkWaiting(ImmutableList.of(writeLockAndWait.getRight()));
        // complete other read and check that write don't wait
        readLockAndWait2.getLeft().complete(null);
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait.getRight()));
    }

    @Test
    public void writeWaitForWriteTest() throws InterruptedException {
        final var writeLockAndWait1 = getWriteLock();
        final var writeLockAndWait2 = getWriteLock();

        // check that the first write don't wait
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
        // check that the other write waits
        checkWaiting(ImmutableList.of(writeLockAndWait2.getRight()));
        // complete first write and check that the other write don't wait now
        writeLockAndWait1.getLeft().complete(null);
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait2.getRight()));
    }

    @Test
    public void multipleReadsWaitForWriteTest() throws InterruptedException {
        final var writeLockAndWait = getWriteLock();
        // get multiple read lock
        final var readLockAndWait1 = getReadLock();
        final var readLockAndWait2 = getReadLock();

        // check that the first write don't wait
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait.getRight()));
        // check that the reads wait
        checkWaiting(ImmutableList.of(readLockAndWait1.getRight(), readLockAndWait2.getRight()));
        // complete the write and check that the reads don't wait now
        writeLockAndWait.getLeft().complete(null);
        checkAllCompletedNormally(ImmutableList.of(readLockAndWait1.getRight(), readLockAndWait2.getRight()));
    }

    private NonnullPair<CompletableFuture<Void>, CompletableFuture<Void>> getWriteLock() {
        final var newWriteLock = new CompletableFuture<Void>();
        return NonnullPair.of(newWriteLock, registry.getWriteLock(identifier, newWriteLock));
    }

    private NonnullPair<CompletableFuture<Void>, CompletableFuture<Void>> getReadLock() {
        final var newReadLock = new CompletableFuture<Void>();
        return NonnullPair.of(newReadLock, registry.getReadLock(identifier, newReadLock));
    }

    private CompletableFuture<Void> runWithLock(@Nonnull Runnable runCheck, @Nonnull NonnullPair<CompletableFuture<Void>, CompletableFuture<Void>> lockAndWait) {
        return lockAndWait.getRight().thenRunAsync(runCheck, executorService).whenComplete((ignore, throwable) -> {
            lockAndWait.getLeft().complete(null);
            if (throwable != null) {
                throw new RuntimeException("wrapped throwable");
            }
        });
    }

    public static <T> void checkAllCompletedNormally(@Nonnull List<CompletableFuture<T>> futures) {
        try {
            AsyncUtil.whenAll(futures).orTimeout(1, TimeUnit.SECONDS).get();
        } catch (Exception e) {
            Assertions.fail("Tasks didn't complete normally", e);
        }
        for (var f: futures) {
            Assertions.assertTrue(f.isDone());
            Assertions.assertFalse(f.isCompletedExceptionally());
        }
    }

    public static <T> void checkWaiting(@Nonnull List<CompletableFuture<T>> futures) throws InterruptedException {
        Thread.sleep(1000);
        for (var f: futures) {
            Assertions.assertFalse(f.isDone());
        }
    }
}
