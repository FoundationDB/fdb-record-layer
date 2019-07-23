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
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionExpiredException;
import com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link com.apple.foundationdb.record.provider.foundationdb.synchronizedsession.SynchronizedSessionRunner}
 */
@Tag(Tags.RequiresFDB)
public abstract class SynchronizedSessionTest extends FDBTestBase {

    private FDBDatabase database;

    private boolean runAsync;

    private static final long DEFAULT_LEASE_LENGTH_MILL = 2_000;

    private Random random = new Random();

    private SynchronizedSessionTest(boolean runAsync) {
        this.runAsync = runAsync;
    }

    @BeforeEach
    public void getDatabase() {
        database = FDBDatabaseFactory.instance().getDatabase();
    }

    // Sessions on same lock, runners on distinct sessions.
    @Test
    public void initializeSecondSessionOnLockShouldFail() throws InterruptedException {
        final Subspace lockSubspace = new Subspace(Tuple.from(Math.random()));
        // Get runner by creating a session.
        try (SynchronizedSessionRunner session1Runner = newRunnerStartSession(lockSubspace)) {
            // Session 1 is active.
            checkActive(session1Runner);

            // Should not be able to create another session.
            assertFailedStartSession(lockSubspace);

            // Session 1 is still active.
            checkActive(session1Runner);

            // If there is no other session, Session 1 is able to run even if it hasn't been active for a long time
            waitLongEnough();
            checkActive(session1Runner);
        }
    }

    @Test
    public void secondSessionShouldTakeLockIfTheFirstOneExpiresAndTheFirstOneCannotContinue() throws Exception {
        final Subspace lockSubspace = new Subspace(Tuple.from(Math.random()));
        try (SynchronizedSessionRunner session1Runner = newRunnerStartSession(lockSubspace)) {
            checkActive(session1Runner);

            waitLongEnough();

            // Able to create another session and the new session is active.
            try (SynchronizedSessionRunner session2Runner = newRunnerStartSession(lockSubspace)) {
                checkActive(session2Runner);
            }

            // Should not be able to use session1, neither by existing runner nor by new runner.
            assertFailedContinueSession(session1Runner);
            assertFailedJoinSession(lockSubspace, session1Runner.getSessionId());
        }
    }

    // Sessions on distinct locks.
    @Test
    public void sessionsOnDifferentLocksShouldNotInterfere() {
        final Subspace lockSubspace1 = new Subspace(Tuple.from(Math.random()));
        final Subspace lockSubspace2 = new Subspace(Tuple.from(Math.random()));
        try (SynchronizedSessionRunner sessionOnLock1 = newRunnerStartSession(lockSubspace1)) {
            sessionOnLock1.run(context -> {
                try (SynchronizedSessionRunner sessionOnLock2 = newRunnerStartSession(lockSubspace2)) {
                    checkActive(sessionOnLock2);
                }
                return null;
            });
        }
    }

    // Runners on same session.
    @Test
    public void runnersOnSameSessionShouldWorkAndExpireTogether() throws InterruptedException {
        final Subspace lockSubspace = new Subspace(Tuple.from(Math.random()));
        // Get runner by creating a session.
        try (SynchronizedSessionRunner session1Runner1 = newRunnerStartSession(lockSubspace)) {
            // Session 1 Runner 1 is active.
            checkActive(session1Runner1);

            UUID session1Id = session1Runner1.getSessionId();
            try (SynchronizedSessionRunner session1Runner2 = newRunnerJoinSession(lockSubspace, session1Id)) {
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
                newRunnerStartSession(lockSubspace);
                assertFailedContinueSession(session1Runner1);
                assertFailedContinueSession(session1Runner2);
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
        final Subspace lockSubspace = new Subspace(Tuple.from(Math.random()));
        try (SynchronizedSessionRunner session1Runner1 = newRunnerStartSession(lockSubspace)) {
            checkActive(session1Runner1);

            session1Runner1.closeSession();

            // Runners of the current session should not be able to work (neither existing runner nor newly created runner).
            assertFailedContinueSession(session1Runner1);
            try (SynchronizedSessionRunner session1Runner2 = newRunnerJoinSession(lockSubspace, session1Runner1.getSessionId())) {
                assertFailedContinueSession(session1Runner2);
            }

            // The new session should be able to be created and used right away.
            try (SynchronizedSessionRunner session2Runner = newRunnerStartSession(lockSubspace)) {
                checkActive(session2Runner);
            }
        }
    }

    @Test
    public void takeLaterOneWhenThereAreDifferentLeaseEndTimes() throws Exception {
        final Subspace lockSubspace = new Subspace(Tuple.from(Math.random()));
        try (SynchronizedSessionRunner session1Runner1 = database.newRunner().toSynchronized(lockSubspace, 2_000)) {
            checkActive(session1Runner1);

            UUID session1Id = session1Runner1.getSessionId();
            try (SynchronizedSessionRunner session1Runner2 = database.newRunner().toSynchronized(lockSubspace, session1Id, 4_000)) {
                checkActive(session1Runner2);
                checkActive(session1Runner1);

                AtomicBoolean start = new AtomicBoolean(false);
                CompletableFuture<Void> session1Runner1Job = session1Runner1.runAsync(context -> AsyncUtil.whileTrue(
                        () -> CompletableFuture.completedFuture(start.get()),
                        context.getExecutor()));
                CompletableFuture<Void> session1Runner2Job = session1Runner1.runAsync(context -> AsyncUtil.whileTrue(
                        () -> CompletableFuture.completedFuture(start.get()),
                        context.getExecutor()));
                database.newRunner().asyncToSync(null, CompletableFuture.allOf(session1Runner1Job, session1Runner2Job));

                // Make the the jobs run almost at the same time with unknown order.
                start.set(true);

                // Runner 1 set the lease end time to 2 seconds in the future, while Runner 2 set it to 4 seconds,
                // the later one should be honoured. So a new session shouldn't take the lock until 4 seconds.
                Thread.sleep(2_000 + 100);
                assertFailedStartSession(lockSubspace);

                Thread.sleep(2_000);
                try (SynchronizedSessionRunner session2Runner = newRunnerStartSession(lockSubspace)) {
                    checkActive(session2Runner);
                }
            }
        }
    }

    @Test
    public void singleRunnerRenewLeaseContinuously() throws Exception {
        testRenewLeaseContinuously(true);
    }

    @Test
    public void newRunnersRenewLeaseContinuously() throws Exception {
        testRenewLeaseContinuously(false);
    }

    private void testRenewLeaseContinuously(boolean reuseRunner) throws Exception {
        final Subspace lockSubspace = new Subspace(Tuple.from(Math.random()));
        try (SynchronizedSessionRunner session1Runner0 = database.newRunner().toSynchronized(lockSubspace, 1_000)) {
            UUID session1 = session1Runner0.getSessionId();
            AtomicBoolean session1Stopped = new AtomicBoolean(false);
            Thread longSession = new Thread(() -> {
                for (int i = 0; i < 10; i++) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    SynchronizedSessionRunner runner = reuseRunner ? session1Runner0 : database.newRunner()
                            .toSynchronized(lockSubspace, session1, 1_000);
                    checkActive(runner);
                }
                session1Stopped.set(true);
            });
            Thread tryStartSession = new Thread(() -> {
                while (!session1Stopped.get()) {
                    assertFailedStartSession(lockSubspace);
                    try {
                        Thread.sleep(random.nextInt(500));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            longSession.start();
            tryStartSession.start();
            tryStartSession.join();
        }
    }

    private SynchronizedSessionRunner newRunnerStartSession(Subspace lockSubspace) {
        return database.newRunner().toSynchronized(lockSubspace, DEFAULT_LEASE_LENGTH_MILL);
    }

    private SynchronizedSessionRunner newRunnerJoinSession(Subspace lockSubspace, UUID sessionId) {
        return database.newRunner().toSynchronized(lockSubspace, sessionId, DEFAULT_LEASE_LENGTH_MILL);
    }

    private void checkActive(SynchronizedSessionRunner runner) {
        if (runAsync) {
            runner.runAsync(c -> AsyncUtil.DONE);
        } else {
            runner.run(c -> null);
        }
    }

    private void assertFailedStartSession(Subspace lockSubspace) {
        SynchronizedSessionExpiredException exception = assertThrows(SynchronizedSessionExpiredException.class,
                () -> newRunnerStartSession(lockSubspace));
        assertEquals("Failed to initialize the session", exception.getMessage());
    }

    private void assertFailedJoinSession(Subspace lockSubspace, UUID sessionId) {
        // It doesn't fail when the runner is created because there is no checking, but when it runs.
        try (SynchronizedSessionRunner expiredSessionsRunner = newRunnerJoinSession(lockSubspace, sessionId)) {
            assertFailedContinueSession(expiredSessionsRunner);
        }
    }

    private void assertFailedContinueSession(SynchronizedSessionRunner synchronizedSessionRunner) {
        SynchronizedSessionExpiredException exception = assertThrows(SynchronizedSessionExpiredException.class,
                () -> synchronizedSessionRunner.run(c -> null));
        assertEquals("Failed to continue the session", exception.getMessage());
    }

    private void waitLongEnough() throws InterruptedException {
        Thread.sleep(DEFAULT_LEASE_LENGTH_MILL + 100);
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
