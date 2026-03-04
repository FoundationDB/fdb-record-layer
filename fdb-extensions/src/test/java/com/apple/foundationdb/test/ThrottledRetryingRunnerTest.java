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
            runner.iterateAll((tr, quota) -> {
                int n = callCount.incrementAndGet();
                if (n >= iterations) {
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        assertThat(callCount.get()).isEqualTo(iterations);
    }

    @Test
    void taskExhaustedImmediatelyRunsOnce() {
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota) -> {
                callCount.incrementAndGet();
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
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
        runner.iterateAll((tr, quota) -> {
            quota.markExhausted();
            return CompletableFuture.completedFuture(null);
        }).join();
        runner.close();

        assertThatThrownBy(() ->
                runner.iterateAll((tr, quota) -> {
                    quota.markExhausted();
                    return CompletableFuture.completedFuture(null);
                }).join())
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
                    runner.iterateAll((tr, quota) -> {
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
            runner.iterateAll((tr, quota) -> {
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
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
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
                runner.iterateAll((tr, quota) -> {
                    callCount.incrementAndGet();
                    quota.markExhausted();
                    return CompletableFuture.completedFuture(null);
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
            runner.iterateAll((tr, quota) -> {
                observedCounts.add(quota.getProcessedCount()); // always 0 at start
                quota.processedCountAdd(10);
                if (totalCalls.incrementAndGet() >= 3) {
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        assertThat(observedCounts).containsExactly(0, 0, 0);
    }

    @Test
    void processedAndDeletedCountsAreIndependent() {
        AtomicInteger observedProcessed = new AtomicInteger(-1);
        AtomicInteger observedDeleted = new AtomicInteger(-1);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota) -> {
                quota.processedCountAdd(7);
                quota.deletedCountAdd(3);
                observedProcessed.set(quota.getProcessedCount());
                observedDeleted.set(quota.getDeletedCount());
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
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
            runner.iterateAll((tr, quota) -> {
                observedLimit.set(quota.getLimit());
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        assertThat(observedLimit.get()).isEqualTo(50);
    }

    @Test
    void limitIsZeroWhenMaxLimitNotConfigured() {
        AtomicInteger observedLimit = new AtomicInteger(-1);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota) -> {
                observedLimit.set(quota.getLimit());
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        assertThat(observedLimit.get()).isEqualTo(0);
    }

    @Test
    void limitDecreasesOnFailureThenIncreasesOnSuccess() {
        // On failure: limit = max(1, processedCount * 9/10)
        // On success: after increaseLimitAfter consecutive successes, limit increases
        AtomicInteger callCount = new AtomicInteger(0);
        List<Integer> observedLimits = new ArrayList<>();

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxLimit(200)
                .withIncreaseLimitAfter(2) // increase after 2 successes to keep the test short
                .withNumOfRetries(10)
                .build()) {
            runner.iterateAll((tr, quota) -> {
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
                    return CompletableFuture.completedFuture(null); // keep going
                }
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
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
        // Each failure with very few processed items should converge the limit toward 1
        AtomicInteger callCount = new AtomicInteger(0);
        List<Integer> observedLimits = new ArrayList<>();

        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxLimit(1000)
                .withNumOfRetries(20)
                .build()) {
            runner.iterateAll((tr, quota) -> {
                int call = callCount.incrementAndGet();
                observedLimits.add(quota.getLimit());
                if (call <= 10) {
                    // Process 1 item each time → limit = max(1, 1*9/10) = 1 after first decrease
                    quota.processedCountInc();
                    return CompletableFuture.failedFuture(new RuntimeException("fail"));
                }
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        // After first failure with 1 processed: max(1, 0) = 1
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
            runner.iterateAll((tr, quota) -> {
                int call = callCount.incrementAndGet();
                observedLimits.add(quota.getLimit());
                quota.processedCountAdd(100);
                if (call <= 3) {
                    return CompletableFuture.failedFuture(new RuntimeException("fail"));
                }
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
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

        // Write a key inside iterateAll
        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withCommitWhenDone(commitWhenDone)
                .build()) {
            runner.iterateAll((tr, quota) -> {
                tr.set(key, value);
                quota.markExhausted();
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        // Check whether the key is actually present
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
            runner.iterateAll((tr, quota) -> {
                seenTransactions.add(tr);
                if (callCount.incrementAndGet() >= 3) {
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        assertThat(seenTransactions).hasSize(3);
        // All three must be distinct objects
        assertThat(seenTransactions.get(0)).isNotSameAs(seenTransactions.get(1));
        assertThat(seenTransactions.get(1)).isNotSameAs(seenTransactions.get(2));
    }

    // -------------------------------------------------------------------------
    // Rate limiting: elapsed time grows when throttling is applied
    // -------------------------------------------------------------------------

    @Test
    void throttlingByScannedItemsSlowsItDown() {
        // 3 transactions each processing 100 items at max 50/sec → each batch should take ≥2s
        // Use a low count to keep the test fast: 3 transactions × 10 items at max 20/sec = ≥1.5s
        final int itemsPerTransaction = 10;
        final int maxPerSec = 20;
        final int transactions = 3;
        AtomicInteger callCount = new AtomicInteger(0);

        long start = System.currentTimeMillis();
        try (ThrottledRetryingRunner runner = runnerBuilder()
                .withMaxItemsScannedPerSec(maxPerSec)
                .build()) {
            runner.iterateAll((tr, quota) -> {
                quota.processedCountAdd(itemsPerTransaction);
                if (callCount.incrementAndGet() >= transactions) {
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
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
            runner.iterateAll((tr, quota) -> {
                quota.deletedCountAdd(deletesPerTransaction);
                if (callCount.incrementAndGet() >= transactions) {
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
            }).join();
        }
        long elapsed = System.currentTimeMillis() - start;

        long expectedMinMs = TimeUnit.SECONDS.toMillis(deletesPerTransaction * transactions / maxPerSec);
        assertThat(elapsed).isGreaterThanOrEqualTo(expectedMinMs);
    }

    @Test
    void noThrottlingWithZeroRateLimit() {
        // With no rate limit configured the runner should complete nearly instantly
        // (well under 1 second for trivial work). We just assert it completes normally.
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota) -> {
                quota.processedCountAdd(1000); // lots of items but no limit configured
                if (callCount.incrementAndGet() >= 5) {
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
            }).join();
        }

        assertThat(callCount.get()).isEqualTo(5);
    }

    // -------------------------------------------------------------------------
    // Real DB writes: data persisted across transactions within one iterateAll
    // -------------------------------------------------------------------------

    @Test
    void dataWrittenInEachTransactionIsVisible() {
        // Write one key per transaction, confirm all are present at the end
        final int numTransactions = 5;
        AtomicInteger callCount = new AtomicInteger(0);

        try (ThrottledRetryingRunner runner = runnerBuilder().build()) {
            runner.iterateAll((tr, quota) -> {
                int n = callCount.incrementAndGet();
                tr.set(subspace.pack(Tuple.from(n)), Tuple.from(n).pack());
                if (n >= numTransactions) {
                    quota.markExhausted();
                }
                return CompletableFuture.completedFuture(null);
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
    // Helpers
    // -------------------------------------------------------------------------

    private ThrottledRetryingRunner.Builder runnerBuilder() {
        return ThrottledRetryingRunner.builder(db, scheduledExecutor);
    }
}
