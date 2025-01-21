/*
 * SynchronizedSessionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner}.
 */
@Tag(Tags.RequiresFDB)
public abstract class SynchronizedSessionTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedSessionTest.class);
    private static final AtomicInteger threadCounter = new AtomicInteger();

    @RegisterExtension
    @Order(0)
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    @Order(1)
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase database;
    private Subspace lockSubspace1;
    private Subspace lockSubspace2;

    private boolean runAsync;

    private static final long DEFAULT_LEASE_LENGTH_MILLIS = 250;

    private SynchronizedSessionTest(boolean runAsync) {
        this.runAsync = runAsync;
    }

    @BeforeEach
    void initializeSubspace() {
        database = dbExtension.getDatabase();
        try (FDBRecordContext context = database.openContext()) {
            lockSubspace1 = pathManager.createPath(TestKeySpace.RAW_DATA).toSubspace(context);
            lockSubspace2 = pathManager.createPath(TestKeySpace.RAW_DATA).toSubspace(context);
            context.commit();
        }
    }

    // Sessions on same lock, runners on distinct sessions.
    @Test
    public void initializeSecondSessionOnLockShouldFail() throws InterruptedException {
        // Get runner by creating a session.
        try (SynchronizedSessionRunner session1Runner = newRunnerStartSession(lockSubspace1)) {
            // Session 1 is active.
            checkActive(session1Runner);

            // Should not be able to create another session.
            assertFailedStartSession(lockSubspace1);

            // Session 1 is still active.
            checkActive(session1Runner);

            // If there is no other session, Session 1 is able to run even if it hasn't been active for a long time
            waitLongEnough();
            checkActive(session1Runner);

            session1Runner.endSession();
        }
    }

    @Test
    public void secondSessionShouldTakeLockIfTheFirstOneExpiresAndTheFirstOneCannotContinue() throws Exception {
        try (SynchronizedSessionRunner session1Runner = newRunnerStartSession(lockSubspace1)) {
            checkActive(session1Runner);

            waitLongEnough();

            // Able to create another session and the new session is active.
            try (SynchronizedSessionRunner session2Runner = newRunnerStartSession(lockSubspace1)) {
                checkActive(session2Runner);
            }

            // Should not be able to use session1, neither by existing runner nor by new runner.
            assertFailedContinueSession(session1Runner);
            assertFailedJoinSession(lockSubspace1, session1Runner.getSessionId());

            session1Runner.endSession();
        }
    }

    // Sessions on distinct locks.
    @Test
    public void sessionsOnDifferentLocksShouldNotInterfere() {
        try (SynchronizedSessionRunner sessionOnLock1 = newRunnerStartSession(lockSubspace1)) {
            sessionOnLock1.run(context -> {
                try (SynchronizedSessionRunner sessionOnLock2 = newRunnerStartSession(lockSubspace2)) {
                    checkActive(sessionOnLock2);
                    sessionOnLock2.endSession();
                }
                return null;
            });
            sessionOnLock1.endSession();
        }
    }

    // Runners on same session.
    @Test
    public void runnersOnSameSessionShouldWorkAndExpireTogether() throws InterruptedException {
        // Get runner by creating a session.
        try (SynchronizedSessionRunner session1Runner1 = newRunnerStartSession(lockSubspace1)) {
            // Session 1 Runner 1 is active.
            checkActive(session1Runner1);

            UUID session1Id = session1Runner1.getSessionId();
            try (SynchronizedSessionRunner session1Runner2 = newRunnerJoinSession(lockSubspace1, session1Id)) {
                // Both runners are active
                checkActive(session1Runner2);
                checkActive(session1Runner1);

                AtomicLong state = new AtomicLong(0);
                // This job lives through 0 to 4.
                CompletableFuture<Void> session1Runner1Job1 = session1Runner1.runAsync(context -> {
                    state.set(1);
                    return updateWhenState(3, 4, state, context);
                });
                // This job lives through 1 to 2.
                CompletableFuture<Void> session1Runner1Job2 = session1Runner1.runAsync(context ->
                        updateWhenState(1, 2, state, context));
                // This job lives through 2 to 3.
                CompletableFuture<Void> session1Runner2Job1 = session1Runner2.runAsync(context ->
                        updateWhenState(2, 3, state, context));
                // This shows that for in one session:
                // - Different jobs of one runner can run together
                // - Jobs of different runners can run together
                database.newRunner().asyncToSync(null, CompletableFuture.allOf(session1Runner1Job1, session1Runner1Job2, session1Runner2Job1));
                assertEquals(4, state.get());

                // Should not be able to continue the runners after the lock is taken by others.
                waitLongEnough();
                try (SynchronizedSessionRunner session2Runner = newRunnerStartSession(lockSubspace1)) {
                    assertFailedContinueSession(session1Runner1);
                    assertFailedContinueSession(session1Runner2);

                    session2Runner.endSession();
                }
            }
        }
    }

    private CompletableFuture<Void> updateWhenState(long expect, long update, AtomicLong state, FDBRecordContext context) {
        return AsyncUtil.whileTrue(
                () -> CompletableFuture.completedFuture(!state.compareAndSet(expect, update)),
                context.getExecutor());
    }

    @Test
    public void clearSession() {
        try (SynchronizedSessionRunner session1Runner = newRunnerStartSession(lockSubspace1)) {
            checkActive(session1Runner);

            session1Runner.endSession();

            // Runners of the current session should not be able to work (neither existing runner nor newly created runner).
            assertFailedContinueSession(session1Runner);
            assertFailedJoinSession(lockSubspace1, session1Runner.getSessionId());

            // The new session should be able to be created and used right away.
            try (SynchronizedSessionRunner session2Runner = newRunnerStartSession(lockSubspace1)) {
                checkActive(session2Runner);

                // This call should no nothing because Session 1 has ended.
                session1Runner.endSession();
                // Make sure manually ending Session 1 does not clear the current lock.
                checkActive(session2Runner);

                // Use session1Runner to end eny active session (i.e. Session 2 here) even Session 1 has ended.
                session1Runner.endAnySession();
                // Check Session 2 is ended.
                assertFailedContinueSession(session2Runner);
                assertFailedJoinSession(lockSubspace1, session2Runner.getSessionId());
            }
        }
    }

    @Test
    public void takeLaterOneWhenThereAreDifferentLeaseEndTimes() throws Exception {
        try (SynchronizedSessionRunner session1Runner1 = newRunnerStartSession(lockSubspace1)) {
            checkActive(session1Runner1);

            UUID session1Id = session1Runner1.getSessionId();
            try (SynchronizedSessionRunner session1Runner2 = database.newRunner().joinSynchronizedSession(lockSubspace1, session1Id, DEFAULT_LEASE_LENGTH_MILLIS * 2)) {
                Deque<Throwable> uncaughtExceptions = new ConcurrentLinkedDeque<>();
                Thread run1 = createThread(uncaughtExceptions, () -> checkActive(session1Runner1));
                Thread run2 = createThread(uncaughtExceptions, () -> checkActive(session1Runner2));

                run1.start();
                run2.start();
                run1.join();
                run2.join();

                assertNoExceptions(uncaughtExceptions);

                // Runner 1 set the lease end time to 2 seconds in the future, while Runner 2 set it to 3 seconds,
                // the later one should be honoured. So a new session shouldn't take the lock until 3 seconds.
                Thread.sleep(DEFAULT_LEASE_LENGTH_MILLIS + 100);
                assertFailedStartSession(lockSubspace1);

                Thread.sleep(DEFAULT_LEASE_LENGTH_MILLIS);
                try (SynchronizedSessionRunner session2Runner = newRunnerStartSession(lockSubspace1)) {
                    checkActive(session2Runner);
                }
            }
            session1Runner1.endSession();
        }
    }

    @Test
    @Tag(Tags.Slow)
    public void singleRunnerRenewLeaseContinuously() throws Exception {
        testRenewLeaseContinuously(true);
    }

    @Test
    @Tag(Tags.Slow)
    public void newRunnersRenewLeaseContinuously() throws Exception {
        testRenewLeaseContinuously(false);
    }

    private void testRenewLeaseContinuously(boolean reuseRunner) throws Exception {
        try (SynchronizedSessionRunner session1Runner0 = database.newRunner().startSynchronizedSession(lockSubspace1, 1_000)) {
            UUID session1 = session1Runner0.getSessionId();
            AtomicBoolean session1Stopped = new AtomicBoolean(false);
            Deque<Throwable> uncaughtExceptions = new ConcurrentLinkedDeque<>();
            Thread longSession = createThread(uncaughtExceptions, () -> {
                for (int i = 0; i < 10; i++) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    SynchronizedSessionRunner runner = reuseRunner ? session1Runner0 : database.newRunner()
                            .joinSynchronizedSession(lockSubspace1, session1, 1_000);
                    checkActive(runner);
                }
                session1Stopped.set(true);
            });
            Thread tryStartSession = createThread(uncaughtExceptions, () -> {
                while (!session1Stopped.get()) {
                    assertFailedStartSession(lockSubspace1);
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(500));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            longSession.start();
            tryStartSession.start();
            tryStartSession.join();

            assertNoExceptions(uncaughtExceptions);

            session1Runner0.endSession();
        }
    }

    private SynchronizedSessionRunner newRunnerStartSession(Subspace lockSubspace) {
        return database.newRunner().startSynchronizedSession(lockSubspace, DEFAULT_LEASE_LENGTH_MILLIS);
    }

    private SynchronizedSessionRunner newRunnerJoinSession(Subspace lockSubspace, UUID sessionId) {
        return database.newRunner().joinSynchronizedSession(lockSubspace, sessionId, DEFAULT_LEASE_LENGTH_MILLIS);
    }

    private void checkActive(SynchronizedSessionRunner runner) {
        if (runAsync) {
            runner.runAsync(c -> AsyncUtil.DONE);
        } else {
            runner.run(c -> null);
        }
    }

    private void assertFailedStartSession(Subspace lockSubspace) {
        SynchronizedSessionLockedException exception = assertThrows(SynchronizedSessionLockedException.class,
                () -> newRunnerStartSession(lockSubspace));
        assertEquals("Failed to initialize the session because of an existing session in progress", exception.getMessage());
    }

    private void assertFailedJoinSession(Subspace lockSubspace, UUID sessionId) {
        // It doesn't fail when the runner is created because there is no checking, but when it runs.
        try (SynchronizedSessionRunner expiredSessionsRunner = newRunnerJoinSession(lockSubspace, sessionId)) {
            assertFailedContinueSession(expiredSessionsRunner);
        }
    }

    private void assertFailedContinueSession(SynchronizedSessionRunner synchronizedSessionRunner) {
        SynchronizedSessionLockedException exception = assertThrows(SynchronizedSessionLockedException.class,
                () -> synchronizedSessionRunner.run(c -> null));
        assertEquals("Failed to continue the session", exception.getMessage());
    }

    @Nonnull
    private static Thread createThread(Collection<? super Throwable> uncaughtExceptions, Runnable task) {
        Thread t = new Thread(task);
        t.setName("synchronized-session-test-" + threadCounter.getAndIncrement());
        t.setDaemon(true);
        t.setUncaughtExceptionHandler((thread, exception) -> {
            LOGGER.error(KeyValueLogMessage.of("Uncaught exception in thread",
                    "thread", thread.getName()
            ), exception);
            uncaughtExceptions.add(exception);
        });
        return t;
    }

    private static void assertNoExceptions(Collection<? extends Throwable> uncaughtExceptions) {
        if (!uncaughtExceptions.isEmpty()) {
            Throwable t = uncaughtExceptions.iterator().next();
            fail("Found " + uncaughtExceptions.size() + " unexpected exceptions", t);
        }
    }

    private void waitLongEnough() throws InterruptedException {
        Thread.sleep(DEFAULT_LEASE_LENGTH_MILLIS + 100);
    }

    /**
     * Run {@link SynchronizedSessionRunner} synchronously.
     */
    public static class Run extends SynchronizedSessionTest {
        Run() {
            super(false);
        }
    }

    /**
     * Run {@link SynchronizedSessionRunner} asynchronously.
     */
    public static class RunAsync extends SynchronizedSessionTest {
        RunAsync() {
            super(true);
        }
    }
}
