/*
 * ThrottledRetryingRunnerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

@Tag(Tags.RequiresFDB)
class ThrottledRetryingRunnerTest {
    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);

    private Database db;
    private Subspace subspace;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeEach
    void setUp() {
        db = dbExtension.getDatabase();
        subspace = subspaceExtension.getSubspace();
        scheduledExecutor = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryBuilder().setNameFormat("throttled-runner-test-%d").build());
    }

    @AfterEach
    void tearDown() {
        scheduledExecutor.shutdown();
    }

    // -------------------------------------------------------------------------
    // throttlePerSecDelayMillis static helper
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
            // elapsedMs, maxPerSec, eventCount, expectedDelayMs
            "1000, 100,   0,    0",   // no events: no delay
            "1000, 100, 100,    0",   // exactly right rate: no delay
            "1000, 100,  80,    0",   // below rate: no delay
            "1000, 100, 200, 1000",   // double the rate: 1s delay
            " 250,  50, 100, 1750",   // 100 events should take 2s, only 250ms elapsed
            "   1,  50, 100, 1999",   // nearly instant: almost full 2s delay
            "   0, 100,   0,    0",   // zero events, no limit
            "1000,   0, 999,    0",   // maxPerSec=0 means disabled
    })
    void testThrottlePerSecDelayMillis(long elapsedMs, int maxPerSec, int eventCount, long expectedDelay) {
        assertThat(ThrottledRetryingRunner.throttlePerSecDelayMillis(elapsedMs, maxPerSec, eventCount))
                .isEqualTo(expectedDelay);
    }

    // -------------------------------------------------------------------------
    // QuotaManager limit adjustment
    // -------------------------------------------------------------------------

    @Test
    void testIncreaseLimitFormula() {
        // limit=0 is no-limit mode; increaseLimit is a no-op
        ThrottledRetryingRunner.QuotaManager qm = new ThrottledRetryingRunner.QuotaManager(0);
        qm.increaseLimit();
        assertThat(qm.getLimit()).isEqualTo(0);

        // With a real maxLimit, decreaseLimit reads processedCount directly (before initTransaction)
        ThrottledRetryingRunner.QuotaManager qm2 = new ThrottledRetryingRunner.QuotaManager(1000);
        qm2.processedCountAdd(80);
        qm2.decreaseLimit(); // limit = max(1, 80*9/10) = 72
        assertThat(qm2.getLimit()).isEqualTo(72);
        qm2.increaseLimit(); // limit = max(72*5/4, 72+4) = max(90, 76) = 90
        assertThat(qm2.getLimit()).isEqualTo(90);
    }

    @Test
    void testDecreaseLimitFormula() {
        ThrottledRetryingRunner.QuotaManager qm = new ThrottledRetryingRunner.QuotaManager(1000);
        // Process 100 items, then fail → limit = max(1, 100*9/10) = 90
        qm.processedCountAdd(100);
        qm.decreaseLimit();
        assertThat(qm.getLimit()).isEqualTo(90);

        // Process 10 items, fail again → max(1, 10*9/10) = 9
        qm.initTransaction();
        qm.processedCountAdd(10);
        qm.decreaseLimit();
        assertThat(qm.getLimit()).isEqualTo(9);

        // Decrease floors at 1
        qm.initTransaction();
        qm.processedCountInc();
        qm.decreaseLimit();
        assertThat(qm.getLimit()).isEqualTo(1);

        // Zero processed also floors at 1
        qm.initTransaction();
        qm.decreaseLimit();
        assertThat(qm.getLimit()).isEqualTo(1);
    }

    @Test
    void testDecreaseLimitNoOpWhenLimitIsZero() {
        ThrottledRetryingRunner.QuotaManager qm = new ThrottledRetryingRunner.QuotaManager(0);
        qm.processedCountAdd(100);
        qm.initTransaction();
        qm.decreaseLimit();
        assertThat(qm.getLimit()).isEqualTo(0);
    }

    @Test
    void testLimitCappedAtMaxLimit() {
        ThrottledRetryingRunner.QuotaManager qm = new ThrottledRetryingRunner.QuotaManager(50);
        // Drive limit down by simulating a failure with 40 in-flight items
        qm.processedCountAdd(40);
        qm.decreaseLimit(); // limit = max(1, 40*9/10) = 36
        // Keep increasing until we hit the cap
        for (int i = 0; i < 100; i++) {
            qm.increaseLimit();
        }
        assertThat(qm.getLimit()).isEqualTo(50); // capped at maxLimit
    }

    // -------------------------------------------------------------------------
    // Basic iteration: task runs the expected number of times
    // -------------------------------------------------------------------------

    @Test
    void taskRunsUntilExhausted() {
        final int iterations = 5;
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                if (callCount.incrementAndGet() >= iterations) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }

        assertThat(callCount.get()).isEqualTo(iterations);
    }

    @Test
    void taskExhaustedImmediatelyRunsOnce() {
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                callCount.incrementAndGet();
                return exhausted();
            }).join();
        }

        assertThat(callCount.get()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // cannotRunWhenClosed
    // -------------------------------------------------------------------------

    @Test
    void cannotRunWhenClosed() {
        ThrottledRetryingRunner runner = runnerBuilder().build();
        runner.iterateAll((tr, quota, cont) -> exhausted()).join();
        runner.close();

        assertThatThrownBy(() ->
                runner.iterateAll((tr, quota, cont) -> exhausted()).join())
                .hasCauseInstanceOf(TransactionalRunner.RunnerClosed.class);
    }

    // -------------------------------------------------------------------------
    // Retries on failure
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 3, 10})
    void retriesCorrectNumberOfTimes(int numRetries) {
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withNumOfRetries(numRetries)
                .build()) {
            Throwable ex = catchThrowableOfType(RuntimeException.class, () ->
                    runner.iterateAll((tr, quota, cont) -> {
                        callCount.incrementAndGet();
                        return CompletableFuture.failedFuture(new RuntimeException("always fails"));
                    }).join());

            assertThat(ex).isNotNull();
            assertThat(ex.getMessage()).contains("always fails");
        }

        // First attempt + numRetries retries
        assertThat(callCount.get()).isEqualTo(numRetries + 1);
    }

    @Test
    void retryCounterResetsAfterSuccess() {
        // Fail twice, succeed, then fail twice again — should not exhaust the retry budget
        // because the counter resets on each success.
        AtomicInteger totalCalls = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withNumOfRetries(2)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int call = totalCalls.incrementAndGet();
                // Fail on calls 1 and 2
                if (call == 1 || call == 2) {
                    return CompletableFuture.failedFuture(new RuntimeException("transient"));
                }
                // Succeed on call 3 (resets counter), then fail on 4 and 5, succeed on 6, done
                if (call == 4 || call == 5) {
                    return CompletableFuture.failedFuture(new RuntimeException("transient"));
                }
                successCount.incrementAndGet();
                if (successCount.get() == 2) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }

        assertThat(successCount.get()).isEqualTo(2);
    }

    @Test
    void runnerClosedDuringIterationAbortsImmediately() {
        AtomicInteger callCount = new AtomicInteger(0);

        ThrottledRetryingRunner runner = runnerBuilder().withNumOfRetries(100).build();
        runner.close(); // close before iterating

        assertThatThrownBy(() ->
                runner.iterateAll((tr, quota, cont) -> {
                    callCount.incrementAndGet();
                    return exhausted();
                }).join())
                .hasCauseInstanceOf(TransactionalRunner.RunnerClosed.class);

        // iterateAll checks closed before starting, so the task is never called
        assertThat(callCount.get()).isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // Counts: processedCount and deletedCount
    // -------------------------------------------------------------------------

    @Test
    void countsAreResetEachTransaction() {
        // Each transaction should see processedCount starting at 0
        List<Integer> observedCounts = new ArrayList<>();
        AtomicInteger totalCalls = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                observedCounts.add(quota.getProcessedCount()); // always 0 at start
                quota.processedCountAdd(10);
                if (totalCalls.incrementAndGet() >= 3) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }

        assertThat(observedCounts).containsExactly(0, 0, 0);
    }

    @Test
    void processedAndDeletedCountsAreIndependent() {
        AtomicInteger observedProcessed = new AtomicInteger(-1);
        AtomicInteger observedDeleted = new AtomicInteger(-1);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                quota.processedCountAdd(7);
                quota.deletedCountAdd(3);
                observedProcessed.set(quota.getProcessedCount());
                observedDeleted.set(quota.getDeletedCount());
                return exhausted();
            }).join();
        }

        assertThat(observedProcessed.get()).isEqualTo(7);
        assertThat(observedDeleted.get()).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // Limit: adaptive adjustment
    // -------------------------------------------------------------------------

    @Test
    void limitStartsAtMaxLimitAndIsExposedToTask() {
        AtomicInteger observedLimit = new AtomicInteger(-1);

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxLimit(50)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                observedLimit.set(quota.getLimit());
                return exhausted();
            }).join();
        }

        assertThat(observedLimit.get()).isEqualTo(50);
    }

    @Test
    void limitIsZeroWhenMaxLimitNotConfigured() {
        AtomicInteger observedLimit = new AtomicInteger(-1);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                observedLimit.set(quota.getLimit());
                return exhausted();
            }).join();
        }

        assertThat(observedLimit.get()).isEqualTo(0);
    }

    @Test
    void limitDecreasesOnFailureThenIncreasesOnSuccess() {
        AtomicInteger callCount = new AtomicInteger(0);
        List<Integer> observedLimits = new ArrayList<>();

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxLimit(200)
                .withIncreaseLimitAfter(2) // increase after 2 successes to keep the test short
                .withNumOfRetries(10)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int call = callCount.incrementAndGet();
                observedLimits.add(quota.getLimit());

                if (call == 1) {
                    // Process 100 items and fail → next limit = 90
                    quota.processedCountAdd(100);
                    return CompletableFuture.failedFuture(new RuntimeException("fail"));
                }
                if (call <= 3) {
                    // Two successes → limit increases from 90
                    quota.processedCountInc();
                    return hasMore();
                }
                return exhausted();
            }).join();
        }

        // call 1: limit=200 (initial), fails with 100 processed → new limit = 90
        assertThat(observedLimits.get(0)).isEqualTo(200);
        // call 2: limit=90
        assertThat(observedLimits.get(1)).isEqualTo(90);
        // call 3: limit=90 still (need 2 consecutive successes before increase)
        assertThat(observedLimits.get(2)).isEqualTo(90);
        // call 4: after 2 successes, limit increased: max(90*5/4, 90+4) = max(112,94) = 112
        assertThat(observedLimits.get(3)).isEqualTo(112);
    }

    @Test
    void limitDecreasesProgressivelyTowardOne() {
        AtomicInteger callCount = new AtomicInteger(0);
        List<Integer> observedLimits = new ArrayList<>();

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxLimit(1000)
                .withNumOfRetries(20)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int call = callCount.incrementAndGet();
                observedLimits.add(quota.getLimit());
                if (call <= 10) {
                    quota.processedCountInc(); // 1 item processed → limit floor at 1
                    return CompletableFuture.failedFuture(new RuntimeException("fail"));
                }
                return exhausted();
            }).join();
        }

        // After first failure with 1 processed: max(1, 1*9/10=0) = 1
        assertThat(observedLimits.get(1)).isEqualTo(1);
        // Stays at 1 regardless of further failures
        for (int i = 2; i < 10; i++) {
            assertThat(observedLimits.get(i)).isEqualTo(1);
        }
    }

    @Test
    void limitIsNeverAdjustedWhenMaxLimitIsZero() {
        AtomicInteger callCount = new AtomicInteger(0);
        List<Integer> observedLimits = new ArrayList<>();

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withNumOfRetries(5)
                // no withMaxLimit → limit stays 0
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int call = callCount.incrementAndGet();
                observedLimits.add(quota.getLimit());
                quota.processedCountAdd(100);
                if (call <= 3) {
                    return CompletableFuture.failedFuture(new RuntimeException("fail"));
                }
                return exhausted();
            }).join();
        }

        assertThat(observedLimits).containsOnly(0);
    }

    // -------------------------------------------------------------------------
    // commitWhenDone: writes are visible (commit) or not (no commit)
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void commitWhenDoneControlsWhetherWritesArePersisted(boolean commitWhenDone) {
        byte[] key = subspace.pack(Tuple.from("commit-test"));
        byte[] value = "hello".getBytes();

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withCommitWhenDone(commitWhenDone)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                tr.set(key, value);
                return exhausted();
            }).join();
        }

        byte[] read = db.run(tr -> tr.get(key).join());
        if (commitWhenDone) {
            assertThat(read).isEqualTo(value);
        } else {
            assertThat(read).isNull();
        }
    }

    // -------------------------------------------------------------------------
    // Each transaction gets a fresh Transaction object
    // -------------------------------------------------------------------------

    @Test
    void eachIterationReceivesADistinctTransaction() {
        List<Transaction> seenTransactions = new ArrayList<>();
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                seenTransactions.add(tr);
                if (callCount.incrementAndGet() >= 3) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }

        assertThat(seenTransactions).hasSize(3);
        assertThat(seenTransactions.get(0)).isNotSameAs(seenTransactions.get(1));
        assertThat(seenTransactions.get(1)).isNotSameAs(seenTransactions.get(2));
    }

    // -------------------------------------------------------------------------
    // Rate limiting
    // -------------------------------------------------------------------------

    @Test
    void throttlingByScannedItemsSlowsItDown() {
        final int itemsPerTransaction = 10;
        final int maxPerSec = 20;
        final int transactions = 3;
        AtomicInteger callCount = new AtomicInteger(0);

        long start = System.currentTimeMillis();
        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxItemsScannedPerSec(maxPerSec)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                quota.processedCountAdd(itemsPerTransaction);
                if (callCount.incrementAndGet() >= transactions) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }
        long elapsed = System.currentTimeMillis() - start;

        // totalItems / maxPerSec = (3*10)/20 = 1.5s minimum
        long expectedMinMs = TimeUnit.SECONDS.toMillis(itemsPerTransaction * transactions / maxPerSec);
        assertThat(elapsed).isGreaterThanOrEqualTo(expectedMinMs);
    }

    @Test
    void throttlingByDeletedItemsSlowsItDown() {
        final int deletesPerTransaction = 10;
        final int maxPerSec = 20;
        final int transactions = 3;
        AtomicInteger callCount = new AtomicInteger(0);

        long start = System.currentTimeMillis();
        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxItemsDeletedPerSec(maxPerSec)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                quota.deletedCountAdd(deletesPerTransaction);
                if (callCount.incrementAndGet() >= transactions) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }
        long elapsed = System.currentTimeMillis() - start;

        long expectedMinMs = TimeUnit.SECONDS.toMillis(deletesPerTransaction * transactions / maxPerSec);
        assertThat(elapsed).isGreaterThanOrEqualTo(expectedMinMs);
    }

    @Test
    void noThrottlingWithZeroRateLimit() {
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                quota.processedCountAdd(1000);
                if (callCount.incrementAndGet() >= 5) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }

        assertThat(callCount.get()).isEqualTo(5);
    }

    // -------------------------------------------------------------------------
    // Real DB writes
    // -------------------------------------------------------------------------

    @Test
    void dataWrittenInEachTransactionIsVisible() {
        final int numTransactions = 5;
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int n = callCount.incrementAndGet();
                tr.set(subspace.pack(Tuple.from(n)), Tuple.from(n).pack());
                if (n >= numTransactions) {
                    return exhausted();
                }
                return hasMore();
            }).join();
        }

        db.run(tr -> {
            for (int i = 1; i <= numTransactions; i++) {
                byte[] val = tr.get(subspace.pack(Tuple.from(i))).join();
                assertThat(val).isEqualTo(Tuple.from(i).pack());
            }
            return null;
        });
    }

    // -------------------------------------------------------------------------
    // Continuation passing
    // -------------------------------------------------------------------------

    @Test
    void firstCallReceivesStartContinuation() {
        AtomicReference<ThrottledRetryingRunner.Continuation> observed = new AtomicReference<>();

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                observed.set(cont);
                return exhausted();
            }).join();
        }

        assertThat(observed.get()).isInstanceOf(ThrottledRetryingRunner.StartContinuation.class);
        assertThat(observed.get()).isSameAs(ThrottledRetryingRunner.StartContinuation.INSTANCE);
    }

    @Test
    void continuationFromSuccessIsPassedToNextCall() {
        // Each transaction returns a custom continuation object; the next call should receive it.
        List<ThrottledRetryingRunner.Continuation> received = new ArrayList<>();
        AtomicInteger callCount = new AtomicInteger(0);

        // Distinct continuation objects for each transaction
        ThrottledRetryingRunner.Continuation cont1 = () -> true;
        ThrottledRetryingRunner.Continuation cont2 = () -> true;

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                received.add(cont);
                int call = callCount.incrementAndGet();
                if (call == 1) {
                    return CompletableFuture.completedFuture(cont1);
                }
                if (call == 2) {
                    return CompletableFuture.completedFuture(cont2);
                }
                return exhausted();
            }).join();
        }

        // call 1: receives StartContinuation
        assertThat(received.get(0)).isInstanceOf(ThrottledRetryingRunner.StartContinuation.class);
        // call 2: receives the continuation returned by call 1
        assertThat(received.get(1)).isSameAs(cont1);
        // call 3: receives the continuation returned by call 2
        assertThat(received.get(2)).isSameAs(cont2);
    }

    @Test
    void continuationIsNotAdvancedOnRetry() {
        // When a transaction fails, the retry should receive the same continuation as the failed
        // attempt — not a new one — so that the task can resume from the same position.
        List<ThrottledRetryingRunner.Continuation> received = new ArrayList<>();
        AtomicInteger callCount = new AtomicInteger(0);

        ThrottledRetryingRunner.Continuation contAfterFirstSuccess = () -> true;

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withNumOfRetries(3)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                received.add(cont);
                int call = callCount.incrementAndGet();

                if (call == 1) {
                    // First success: return a custom continuation
                    return CompletableFuture.completedFuture(contAfterFirstSuccess);
                }
                if (call == 2 || call == 3) {
                    // Fail twice: continuation should NOT advance
                    return CompletableFuture.failedFuture(new RuntimeException("transient"));
                }
                // Final success
                return exhausted();
            }).join();
        }

        // call 1: StartContinuation (first call)
        assertThat(received.get(0)).isInstanceOf(ThrottledRetryingRunner.StartContinuation.class);
        // call 2: contAfterFirstSuccess (first call succeeded)
        assertThat(received.get(1)).isSameAs(contAfterFirstSuccess);
        // call 3: still contAfterFirstSuccess — call 2 failed, continuation not advanced
        assertThat(received.get(2)).isSameAs(contAfterFirstSuccess);
        // call 4: still contAfterFirstSuccess — call 3 failed too
        assertThat(received.get(3)).isSameAs(contAfterFirstSuccess);
    }

    @Test
    void startContinuationReusedAcrossRetriesBeforeAnySuccess() {
        // Before any successful transaction, all retries should receive StartContinuation.
        List<ThrottledRetryingRunner.Continuation> received = new ArrayList<>();
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withNumOfRetries(3)
                .build()) {
            runner.iterateAll((tr, quota, cont) -> {
                received.add(cont);
                if (callCount.incrementAndGet() <= 2) {
                    return CompletableFuture.failedFuture(new RuntimeException("transient"));
                }
                return exhausted();
            }).join();
        }

        // All three calls should receive StartContinuation since no success ever happened
        assertThat(received).hasSize(3);
        assertThat(received).allMatch(c -> c instanceof ThrottledRetryingRunner.StartContinuation);
    }

    @Test
    void continuationCarriesStateToNextTransaction() {
        // A realistic scenario: the continuation carries a "cursor position" (last key seen).
        // Each transaction processes one key and the next transaction picks up where it left off.
        final int numKeys = 4;

        // Write test data
        db.run(tr -> {
            for (int i = 0; i < numKeys; i++) {
                tr.set(subspace.pack(Tuple.from(i)), Tuple.from(i * 10).pack());
            }
            return null;
        });

        // A continuation that carries the next index to read
        class IndexContinuation implements ThrottledRetryingRunner.Continuation {
            final int nextIndex;

            IndexContinuation(int nextIndex) {
                this.nextIndex = nextIndex;
            }

            @Override
            public boolean hasMore() {
                return nextIndex < numKeys;
            }
        }

        List<Integer> processedValues = new ArrayList<>();

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int idx = (cont instanceof IndexContinuation) ? ((IndexContinuation) cont).nextIndex : 0;
                byte[] raw = tr.get(subspace.pack(Tuple.from(idx))).join();
                processedValues.add((int) Tuple.fromBytes(raw).getLong(0));
                return CompletableFuture.completedFuture(new IndexContinuation(idx + 1));
            }).join();
        }

        assertThat(processedValues).containsExactly(0, 10, 20, 30);
    }

    // -------------------------------------------------------------------------
    // Real transaction conflict
    // -------------------------------------------------------------------------

    @Test
    void retriesOnRealTransactionConflict() {
        // Arrange a genuine FDB NOT_COMMITTED conflict without any threading:
        //   1. Task (attempt 1) reads conflictKey  →  establishes a read-conflict range on tr
        //   2. Task also writes to ownWriteKey  →  makes tr a write transaction so FDB actually
        //      checks read conflicts on commit (FDB silently skips conflict checking for
        //      read-only transactions)
        //   3. Task opens a second transaction, writes to conflictKey, and commits it
        //   4. TransactionalRunner then commits tr  →  FDB detects the conflict and rejects it
        //   5. Runner retries; attempt 2 sees no conflict and succeeds
        byte[] conflictKey = subspace.pack(Tuple.from("conflict-key"));
        byte[] ownWriteKey = subspace.pack(Tuple.from("own-write"));
        AtomicInteger attemptCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().withNumOfRetries(3).build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int attempt = attemptCount.incrementAndGet();
                if (attempt == 1) {
                    // Write to a separate key to make tr a write transaction.
                    tr.set(ownWriteKey, new byte[]{0});
                    // Read conflictKey to add it to tr's read-conflict range, then commit a
                    // write to the same key in a separate transaction before tr commits.
                    return tr.get(conflictKey)
                            .thenCompose(ignored ->
                                db.runAsync(tr2 -> {
                                    tr2.set(conflictKey, new byte[]{1});
                                    return CompletableFuture.completedFuture(null);
                                })
                            )
                            .thenCompose(ignored -> exhausted());
                }
                // Attempt 2: no conflict
                return exhausted();
            }).join();
        }

        assertThat(attemptCount.get()).isEqualTo(2);
    }

    @Test
    void conflictDoesNotAdvanceContinuation() {
        // Write 6 data keys to process in batches of 2 across 3 transactions.
        //
        // Expected flow:
        //   Attempt 1 (idx=0): processes items 0,1 — commits → continuation advances to idx=2
        //   Attempt 2 (idx=2): processes items 2,3 — CONFLICT → not committed
        //   Attempt 3 (idx=2): retry receives idx=2 (not idx=4!) — processes 2,3 again — commits
        //   Attempt 4 (idx=4): processes items 4,5 — commits → hasMore()=false, loop ends
        //
        // The key assertion is that attempt 3 starts at idx=2, not idx=4. If the continuation
        // were incorrectly advanced on failure, items 2 and 3 would never be written to FDB.
        final int numKeys = 6;
        final int batchSize = 2;

        db.run(tr -> {
            for (int i = 0; i < numKeys; i++) {
                tr.set(subspace.pack(Tuple.from("data", i)), Tuple.from(i).pack());
            }
            return null;
        });

        byte[] conflictKey = subspace.pack(Tuple.from("conflict-trigger"));
        byte[] ownWriteKey  = subspace.pack(Tuple.from("own-write"));

        AtomicInteger attemptCount = new AtomicInteger(0);
        AtomicInteger retryStartIdx = new AtomicInteger(-1); // captured from attempt 3

        class IndexContinuation implements ThrottledRetryingRunner.Continuation {
            final int nextIndex;

            IndexContinuation(int nextIndex) {
                this.nextIndex = nextIndex;
            }

            @Override
            public boolean hasMore() {
                return nextIndex < numKeys;
            }
        }

        try (ThrottledRetryingRunner runner = runnerBuilder().withNumOfRetries(3).build()) {
            runner.iterateAll((tr, quota, cont) -> {
                int attempt = attemptCount.incrementAndGet();
                int startIdx = (cont instanceof IndexContinuation)
                        ? ((IndexContinuation) cont).nextIndex : 0;
                int endIdx = Math.min(startIdx + batchSize, numKeys);

                if (attempt == 3) {
                    retryStartIdx.set(startIdx);
                }

                // Write a "processed" marker for each item in this batch
                for (int i = startIdx; i < endIdx; i++) {
                    tr.set(subspace.pack(Tuple.from("done", i)), Tuple.from(i).pack());
                }

                if (attempt == 2) {
                    // Inject a conflict on the second attempt:
                    // - write to ownWriteKey to make tr a write transaction (FDB only checks
                    //   read conflicts when the transaction has writes)
                    // - read conflictKey to add it to tr's read-conflict range
                    // - commit a write to conflictKey in a separate transaction so that tr
                    //   fails with NOT_COMMITTED
                    tr.set(ownWriteKey, new byte[]{0});
                    return tr.get(conflictKey)
                            .thenCompose(ignored ->
                                db.runAsync(tr2 -> {
                                    tr2.set(conflictKey, new byte[]{1});
                                    return CompletableFuture.completedFuture(null);
                                })
                            )
                            .thenCompose(ignored ->
                                CompletableFuture.completedFuture(new IndexContinuation(endIdx)));
                }

                return CompletableFuture.completedFuture(new IndexContinuation(endIdx));
            }).join();
        }

        // Attempt 3 must have restarted at idx=2 (the last committed continuation),
        // not at idx=4 (what would have been returned if the failed attempt had committed)
        assertThat(retryStartIdx.get()).isEqualTo(2);

        // 4 attempts total: tx1(0-1) + tx2(2-3, conflict) + retry(2-3) + tx3(4-5)
        assertThat(attemptCount.get()).isEqualTo(4);

        // All 6 items must be present in FDB exactly once — none skipped, none missing
        db.run(tr -> {
            for (int i = 0; i < numKeys; i++) {
                byte[] val = tr.get(subspace.pack(Tuple.from("done", i))).join();
                assertThat(val).as("item %d should be committed", i)
                        .isEqualTo(Tuple.from(i).pack());
            }
            return null;
        });
    }


    // -------------------------------------------------------------------------

    /** Returns a completed future with a continuation indicating there is more work to do. */
    private static CompletableFuture<ThrottledRetryingRunner.Continuation> hasMore() {
        return CompletableFuture.completedFuture(() -> true);
    }

    /** Returns a completed future with a continuation indicating the source is exhausted. */
    private static CompletableFuture<ThrottledRetryingRunner.Continuation> exhausted() {
        return CompletableFuture.completedFuture(() -> false);
    }

    private ThrottledRetryingRunner.Builder runnerBuilder() {
        return ThrottledRetryingRunner.builder(db, scheduledExecutor);
    }
}