/*
 * LockRegistryTest.java
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

package com.apple.foundationdb.record.locking;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link LockRegistry}.
 */
public class LockRegistryTest {

    @Nonnull
    private final ExecutorService executorService = Executors.newFixedThreadPool(8);

    @Nonnull
    final LockRegistry registry = new LockRegistry(null);

    @Nonnull
    final LockIdentifier identifier = new LockIdentifier(new Subspace(Tuple.from(1, 2, 3)));

    static Stream<Arguments> argumentsForTests() {
        return Stream.of(Arguments.of(50),
                Arguments.of(100),
                Arguments.of(500),
                Arguments.of(1000),
                Arguments.of(5000)
        );
    }

    @ParameterizedTest
    @MethodSource("argumentsForTests")
    void orderedWriteTest(final int numRuns) {
        final List<Integer> resource = new ArrayList<>();
        final List<NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>>> writeLockAndWaits = new ArrayList<>();
        for (int i = 0; i < numRuns; i++) {
            writeLockAndWaits.add(acquireWriteLock());
        }
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = numRuns - 1; i >= 0; i--) {
            final int finalI = i;
            futures.add(runWithLock(() -> resource.add(finalI), writeLockAndWaits.get(i)));
        }
        checkAllCompletedNormally(futures);
        for (int i = 0; i < numRuns; i++) {
            assertThat(resource.get(i))
                    .isEqualTo(i);
        }
    }

    @ParameterizedTest
    @MethodSource("argumentsForTests")
    void sharedReadsExclusiveWriteTest(final int numRuns) throws ExecutionException, InterruptedException {
        final List<Integer> resource = IntStream.range(0, numRuns).boxed().collect(Collectors.toList());

        // get 2 read locks, that will be shared
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait1 = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait2 = acquireReadLock();
        // get a write lock, that should wait on the previous reads to finish
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait = acquireWriteLock();
        // get 2 other read locks, that will be shared and wait for write to finish
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait3 = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait4 = acquireReadLock();

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(runWithLock(() -> {
            assertThat(resource)
                    .hasSize(numRuns * 2);
            for (int i = 0; i < numRuns * 2; i++) {
                assertThat(resource.get(i))
                        .isEqualTo(i);
            }
        }, readLockAndWait4));
        futures.add(runWithLock(() -> {
            assertThat(resource)
                    .hasSize(numRuns * 2);
            for (int i = 0; i < numRuns * 2; i++) {
                assertThat(resource.get(i))
                        .isEqualTo(i);
            }
        }, readLockAndWait3));
        checkWaiting(futures);
        futures.add(runWithLock(() -> {
            assertThat(resource)
                    .hasSize(numRuns);
            Assertions.assertEquals(numRuns, resource.size());
            for (int i = 0; i < numRuns; i++) {
                assertThat(resource.get(i))
                        .isEqualTo(i);
            }
        }, readLockAndWait2));
        futures.add(runWithLock(() -> {
            assertThat(resource)
                    .hasSize(numRuns);
            for (int i = 0; i < numRuns; i++) {
                assertThat(resource.get(i))
                        .isEqualTo(i);
            }
        }, readLockAndWait1));
        // checks that the initial 2 reads get to completion
        checkAllCompletedNormally(ImmutableList.of(futures.get(2), futures.get(3)));
        // other 2 are still waiting
        checkWaiting(ImmutableList.of(futures.get(0), futures.get(1)));
        futures.add(runWithLock(() -> {
            assertThat(resource)
                    .hasSize(numRuns);
            for (int i = 0; i < numRuns; i++) {
                resource.add(numRuns + i);
            }
        }, writeLockAndWait));
        checkAllCompletedNormally(futures);
        AsyncUtil.whenAll(futures).get();
    }

    @Test
    void writeWaitForReadTest() throws InterruptedException {
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait = acquireWriteLock();

        // check that the read don't wait
        checkAllCompletedNormally(ImmutableList.of(readLockAndWait.getRight()));
        // check that the write waits
        checkWaiting(ImmutableList.of(writeLockAndWait.getRight()));
        // complete the read and check that the write don't wait now
        readLockAndWait.getLeft().get().release();
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait.getRight()));
    }

    @Test
    void writeWaitForMultipleReadsTest() throws InterruptedException {
        // get multiple read locks
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait1 = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait2 = acquireReadLock();
        // get a write lock
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait = acquireWriteLock();

        // check that the read don't wait
        checkAllCompletedNormally(ImmutableList.of(readLockAndWait1.getRight(), readLockAndWait2.getRight()));
        // check that the write waits
        checkWaiting(ImmutableList.of(writeLockAndWait.getRight()));
        // complete one read and check that the write still waits
        readLockAndWait1.getLeft().get().release();
        checkWaiting(ImmutableList.of(writeLockAndWait.getRight()));
        // complete other read and check that write don't wait
        readLockAndWait2.getLeft().get().release();
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait.getRight()));
    }

    @Test
    void writeWaitsEvenIfLastReadWasReleasedAtAcquisition() {
        // start two reads
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait1 = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait2 = acquireReadLock();
        checkAllCompletedNormally(ImmutableList.of(readLockAndWait1.getRight(), readLockAndWait2.getRight()));

        // release the second one, so the most recent lock for this ID should be marked as released
        readLockAndWait2.getLeft().get().release();
        assertThat(registry.getHeldLocks())
                .hasEntrySatisfying(identifier, lockInRegistry -> assertThat(lockInRegistry.isLockReleased()).isTrue());

        // new write lock has to wait for the first read even though the top lock in the registry is already released
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait = acquireWriteLock();
        assertThat(writeLockAndWait.getRight())
                .isNotDone();

        readLockAndWait1.getLeft().get().release();
        assertThat(writeLockAndWait.getRight())
                .isCompleted();
        assertThat(writeLockAndWait.getLeft())
                .hasValueSatisfying(writeLock -> assertThat(writeLock).isNotNull().isSameAs(registry.getHeldLocks().get(identifier)));
    }

    @Test
    void writeWaitForWriteTest() throws InterruptedException {
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait1 = acquireWriteLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait2 = acquireWriteLock();

        // check that the first write don't wait
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
        // check that the other write waits
        checkWaiting(ImmutableList.of(writeLockAndWait2.getRight()));
        // complete first write and check that the other write don't wait now
        writeLockAndWait1.getLeft().get().release();
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait2.getRight()));
    }

    @ParameterizedTest
    @BooleanSource
    void writeProceedsIfParentFails(boolean parentIsWrite) {
        final CompletableFuture<Void> parentOperation = new CompletableFuture<>();
        final CompletableFuture<Integer> childOperation = CompletableFuture.completedFuture(1);

        final CompletableFuture<Void> parentUnderLock = parentIsWrite ? registry.doWithWriteLock(identifier, () -> parentOperation) : registry.doWithReadLock(identifier, () -> parentOperation);
        final CompletableFuture<Integer> writeUnderLock = registry.doWithWriteLock(identifier, () -> childOperation);

        // Write should not have started as the parent has not completed
        assertThat(writeUnderLock)
                .isNotDone();

        // Complete the parent operation with an error
        parentOperation.completeExceptionally(new Throwable("error for test"));
        assertThat(parentUnderLock)
                .isCompletedExceptionally();

        // Now the child write should proceed (completing immediately) and should complete successfully
        assertThat(writeUnderLock)
                .isCompletedWithValue(1);
    }

    @Test
    void multipleReadsWaitForWriteTest() throws InterruptedException {
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait = acquireWriteLock();
        // get multiple read lock
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait1 = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait2 = acquireReadLock();

        // check that the first write don't wait
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait.getRight()));
        // check that the reads wait
        checkWaiting(ImmutableList.of(readLockAndWait1.getRight(), readLockAndWait2.getRight()));
        // complete the write and check that the reads don't wait now
        writeLockAndWait.getLeft().get().release();
        checkAllCompletedNormally(ImmutableList.of(readLockAndWait1.getRight(), readLockAndWait2.getRight()));
    }

    @Test
    void doWithReadLockTest() throws InterruptedException, ExecutionException {
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait1 = acquireWriteLock();
        final CompletableFuture<Integer> read = registry.doWithReadLock(identifier, () -> CompletableFuture.completedFuture(1));
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> writeLockAndWait2 = acquireWriteLock();

        // check that the first write don't wait
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait1.getRight()));
        // check that the read and other write lock wait
        checkWaiting(ImmutableList.of(read));
        checkWaiting(ImmutableList.of(writeLockAndWait2.getRight()));
        // complete first write, check that the read and other write don't wait
        writeLockAndWait1.getLeft().get().release();
        checkAllCompletedNormally(ImmutableList.of(read));
        Assertions.assertEquals(1, (int)read.get());
        checkAllCompletedNormally(ImmutableList.of(writeLockAndWait2.getRight()));
    }

    @Test
    void readsDependOnEachOtherTest() throws ExecutionException, InterruptedException {
        final CompletableFuture<Void> future1 = new CompletableFuture<>();
        final CompletableFuture<Void> future2 = new CompletableFuture<>();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait1 = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> readLockAndWait2 = acquireReadLock();
        final List<CompletableFuture<Void>> tasks = new ArrayList<>();

        tasks.add(runWithLock(() -> {
            try {
                future1.complete(null);
                future2.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, readLockAndWait1));
        future1.get();
        tasks.add(runWithLock(() -> {
            try {
                future2.complete(null);
                future1.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, readLockAndWait2));
        checkAllCompletedNormally(tasks);
    }

    @ParameterizedTest(name = "cleanUpWhenDone[writeLock={0}]")
    @BooleanSource
    void cleanUpWhenDone(boolean writeLock) {
        assertThat(registry.getHeldLocks())
                .isEmpty();

        // As the lock was not previously held, it should immediately be ready
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> lockAndWait = writeLock ? acquireWriteLock() : acquireReadLock();
        assertThat(lockAndWait.getRight())
                .isDone();
        assertThat(lockAndWait.getLeft())
                .hasValueSatisfying(lockInRef -> assertThat(lockInRef).isNotNull());
        assertThat(registry.getHeldLocks())
                .hasEntrySatisfying(identifier, lockInMap -> assertThat(lockInMap).isSameAs(lockAndWait.getLeft().get()));

        // Release the lock
        lockAndWait.getLeft().get().release();

        // The map entry should now be cleared out
        assertThat(registry.getHeldLocks())
                .isEmpty();
    }

    @ParameterizedTest(name = "cleanUpWhenAllWorkIsDone[oldestFirst={0}]")
    @BooleanSource
    void cleanUpWhenAllWorkIsDone(boolean oldestFirst) {
        assertThat(registry.getHeldLocks())
                .isEmpty();

        // Acquire two read locks. They should both be immediately acquired
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> lockAndWait1 = acquireReadLock();
        final NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> lockAndWait2 = acquireReadLock();
        assertThat(lockAndWait1.getRight())
                .isDone();
        assertThat(lockAndWait2.getRight())
                .isDone();
        assertThat(lockAndWait1.getLeft())
                .hasValueSatisfying(lockInRef -> assertThat(lockInRef).isNotNull());
        assertThat(lockAndWait2.getLeft())
                .hasValueSatisfying(lockInRef -> assertThat(lockInRef).isNotNull());

        // The registry should hold the second lock in its map
        assertThat(registry.getHeldLocks())
                .hasEntrySatisfying(identifier, lockInMap -> assertThat(lockInMap).isSameAs(lockAndWait2.getLeft().get()));

        // Release one lock
        if (oldestFirst) {
            lockAndWait1.getLeft().get().release();
        } else {
            lockAndWait2.getLeft().get().release();
        }

        // The registry should still point to the second lock
        assertThat(registry.getHeldLocks())
                .hasEntrySatisfying(identifier, lockInMap -> assertThat(lockInMap).isSameAs(lockAndWait2.getLeft().get()));

        // Release the other lock
        if (oldestFirst) {
            lockAndWait2.getLeft().get().release();
        } else {
            lockAndWait1.getLeft().get().release();
        }

        // Now the entry in the registry should be released
        assertThat(registry.getHeldLocks())
                .isEmpty();
    }

    @ParameterizedTest
    @BooleanSource
    void cleanUpEvenIfTaskFails(boolean writeLock) {
        final CompletableFuture<Void> underlyingFuture = new CompletableFuture<>();
        final CompletableFuture<Void> futureUnderLock = writeLock ? registry.doWithWriteLock(identifier, () -> underlyingFuture) : registry.doWithReadLock(identifier, () -> underlyingFuture);
        assertThat(futureUnderLock)
                .isNotDone();
        assertThat(registry.getHeldLocks())
                .containsKey(identifier);

        // Complete the underlying operation
        underlyingFuture.completeExceptionally(new Throwable("error for test"));
        assertThat(futureUnderLock)
                .isCompletedExceptionally();

        // The lock future failed but the cleanup of the lock registry should still be run
        assertThat(registry.getHeldLocks())
                .isEmpty();
    }

    private NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> acquireWriteLock() {
        final AtomicReference<AsyncLock> asyncLock = new AtomicReference<>();
        return NonnullPair.of(asyncLock,
                registry.acquireWriteLock(identifier).thenApply(lock -> {
                    asyncLock.set(lock);
                    return null;
                }));
    }

    private NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> acquireReadLock() {
        final AtomicReference<AsyncLock> asyncLock = new AtomicReference<>();
        return NonnullPair.of(asyncLock,
                registry.acquireReadLock(identifier).thenApply(lock -> {
                    asyncLock.set(lock);
                    return null;
                }));
    }

    private CompletableFuture<Void> runWithLock(@Nonnull Runnable runCheck, @Nonnull NonnullPair<AtomicReference<AsyncLock>, CompletableFuture<Void>> lockAndWait) {
        return lockAndWait.getRight().thenRunAsync(runCheck, executorService).whenComplete((ignore, throwable) -> {
            lockAndWait.getLeft().get().release();
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
        for (CompletableFuture<T> f: futures) {
            assertThat(f)
                    .isDone()
                    .isNotCompletedExceptionally();
        }
    }

    public static <T> void checkWaiting(@Nonnull List<CompletableFuture<T>> futures) throws InterruptedException {
        Thread.sleep(100);
        for (CompletableFuture<T> f: futures) {
            assertThat(f)
                    .isNotDone();
        }
    }
}
