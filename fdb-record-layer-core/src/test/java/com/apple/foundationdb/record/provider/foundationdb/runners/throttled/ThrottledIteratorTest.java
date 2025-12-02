/*
 * ThrottledIteratorTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.runners.throttled;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.cursors.FutureCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.tuple.Tuple;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThrottledIteratorTest extends FDBRecordStoreTestBase {
    @ParameterizedTest
    @CsvSource({
            "1000,100,0,0",     // less than max
            "2000,100,180,0",
            "100,10,1,0",
            "105,10,1,0",

            "1000,100,100,0",    // just right
            "100,10,1,0",

            "1000,100,200,1000", // delay required - the more interesting cases...
            "2000,100,210,100",
            "250,100,100,750",
            "250,50,100,1750",  // 100 events should take two seconds, wait what it takes to get there
            "1,50,100,1999",
            "1999,50,100,1",
            "10,10,1,90",       // 10 events per second, require 100ms per one event

            "500,100,49,0",     // consecutive
            "500,100,50,0",
            "500,100,51,10",

    })
    void testThrottledIteratorGetDelay(long rangeProcessingTimeMillis, int maxPerSec, int eventsCount, long expectedResult) {
        long ret = ThrottledRetryingIterator.throttlePerSecGetDelayMillis(rangeProcessingTimeMillis, maxPerSec, eventsCount);
        assertThat(ret).isEqualTo(expectedResult);
    }

    @Test
    void testIncreaseLimit() {
        assertThat(ThrottledRetryingIterator.increaseLimit(0)).isEqualTo(0);
        assertThat(ThrottledRetryingIterator.increaseLimit(100)).isEqualTo(125);
        assertThat(ThrottledRetryingIterator.increaseLimit(1)).isEqualTo(5);
        assertThat(ThrottledRetryingIterator.increaseLimit(3)).isEqualTo(7);
        assertThat(ThrottledRetryingIterator.increaseLimit(10)).isEqualTo(14);
    }

    @Test
    void testDecreaseLimit() {
        assertThat(ThrottledRetryingIterator.decreaseLimit(0)).isEqualTo(1);
        assertThat(ThrottledRetryingIterator.decreaseLimit(1)).isEqualTo(1);
        assertThat(ThrottledRetryingIterator.decreaseLimit(2)).isEqualTo(1);
        assertThat(ThrottledRetryingIterator.decreaseLimit(3)).isEqualTo(2);
        assertThat(ThrottledRetryingIterator.decreaseLimit(100)).isEqualTo(90);
    }

    @Test
    void cannotRunWhenClosed() throws Exception {
        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            return AsyncUtil.DONE;
        };

        ThrottledRetryingIterator<Integer> iterator;
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(10, itemHandler, null, null, -1, -1, -1, -1, null).build()) {
            iterator = throttledIterator;
            throttledIterator.iterateAll(storeBuilder).join();
        }

        Assertions.assertThatThrownBy(() -> iterator.iterateAll(recordStore.asBuilder()).join()).hasCauseInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
    }

    @CsvSource({"-1", "0", "1", "3", "100"})
    @ParameterizedTest
    void testThrottleIteratorSuccessDeleteLimit(int deleteLimit) throws Exception {
        // Iterate range, verify that the number of items deleted matches the number of records
        // Ensure multiple transactions are playing nicely with the deleted limit
        final int numRecords = 42; // mostly harmless
        AtomicInteger iteratedCount = new AtomicInteger(0); // total number of scanned items
        AtomicInteger deletedCount = new AtomicInteger(0); // total number of "deleted" items
        AtomicInteger successTransactionCount = new AtomicInteger(0); // number of invocations of RangeSuccess callback
        AtomicInteger limitRef = new AtomicInteger(-1);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            quotaManager.deleteCountAdd(1);
            return AsyncUtil.DONE;
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification = quotaManager -> {
            successTransactionCount.incrementAndGet();
            iteratedCount.addAndGet(quotaManager.getScannedCount());
            deletedCount.addAndGet(quotaManager.getDeletesCount());
        };

        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(numRecords, itemHandler, null, successNotification, -1, deleteLimit, -1, -1, limitRef).build()) {
            throttledIterator.iterateAll(storeBuilder).join();
        }

        assertThat(iteratedCount.get()).isEqualTo(numRecords);
        assertThat(deletedCount.get()).isEqualTo(numRecords);

        assertThat(limitRef.get()).isZero();
        if (deleteLimit <= 0) {
            assertThat(successTransactionCount.get()).isOne();
        } else {
            assertThat(successTransactionCount.get()).isEqualTo(numRecords / deleteLimit + 1);
        }
    }

    @CsvSource({"-1", "0", "50", "100"})
    @ParameterizedTest
    void testThrottleIteratorSuccessSecondsLimit(int maxPerSecLimit) throws Exception {
        // Iterate range, verify that the number of items scanned matches the number of records
        // Assert that the total test takes longer because of the max per sec limit
        final int numRecords = 50;
        AtomicInteger iteratedCount = new AtomicInteger(0); // total number of scanned items
        AtomicInteger scannedCount = new AtomicInteger(0); // total number of scanned items
        AtomicInteger deletedCount = new AtomicInteger(0); // total number of "deleted" items
        AtomicInteger successTransactionCount = new AtomicInteger(0); // number of invocations of RangeSuccess callback
        AtomicInteger limitRef = new AtomicInteger(-1);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            quotaManager.deleteCountAdd(1);
            // Fail the first time, to get the maxRowLimit going
            scannedCount.addAndGet(1);
            if (scannedCount.get() == 1) {
                throw new RuntimeException("Blah");
            }
            return AsyncUtil.DONE;
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification = quotaManager -> {
            successTransactionCount.incrementAndGet();
            iteratedCount.addAndGet(quotaManager.getScannedCount());
            deletedCount.addAndGet(quotaManager.getDeletesCount());
        };
        // Create the store first
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            commit(context);
        }
        long startTimeMillis = System.currentTimeMillis();
        try (FDBRecordContext context = openContext()) {
            // For variety, leave this iterator running within the existing transaction
            openSimpleRecordStore(context);
            try (ThrottledRetryingIterator<Integer> throttledIterator =
                    iteratorBuilder(numRecords, itemHandler, null, successNotification,  maxPerSecLimit, -1, -1, -1, limitRef).build()) {
                throttledIterator.iterateAll(recordStore.asBuilder()).join();
            }
            commit(context);
        }

        long totalTimeMillis = System.currentTimeMillis() - startTimeMillis;
        assertThat(iteratedCount.get()).isEqualTo(numRecords);
        assertThat(deletedCount.get()).isEqualTo(numRecords);
        if (maxPerSecLimit > 0) {
            assertThat(totalTimeMillis).isGreaterThan(TimeUnit.SECONDS.toMillis(numRecords / maxPerSecLimit));
        }
    }

    @Test
    void testThrottleIteratorTransactionTimeLimit() throws Exception {
        // Set time limit for the transaction, add delay to each item handler
        final int numRecords = 55;
        final int delay = 100;
        final int transactionTimeMillis = 500;
        AtomicInteger initTransactionCount = new AtomicInteger(0);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            return MoreAsyncUtil.delayedFuture(delay, TimeUnit.MILLISECONDS);
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            initTransactionCount.incrementAndGet();
        };

        long startTimeMillis = System.currentTimeMillis();
        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(numRecords, itemHandler, initNotification, null, -1, -1, -1, transactionTimeMillis, null).build()) {
            throttledIterator.iterateAll(storeBuilder).join();
        }

        long totalTimeMillis = System.currentTimeMillis() - startTimeMillis;
        assertThat(totalTimeMillis).isGreaterThan(numRecords * delay);
        assertThat(initTransactionCount.get()).isGreaterThanOrEqualTo((numRecords * delay / transactionTimeMillis) - 1);
    }

    @CsvSource({"-1", "0", "1", "3", "100"})
    @ParameterizedTest
    void testThrottleIteratorFailuresDeleteLimit(int deleteLimit) throws Exception {
        // Fail some handlings, ensure transaction restarts, items scanned
        final int numRecords = 43;
        AtomicInteger totalScanned = new AtomicInteger(0); // number of items scanned
        AtomicInteger totalDeleted = new AtomicInteger(0); // number of items deleted
        AtomicInteger failCount = new AtomicInteger(0); // number of exception thrown
        AtomicInteger transactionStartCount = new AtomicInteger(0); // number of invocations of transactionInit callback
        AtomicInteger transactionCommitCount = new AtomicInteger(0); // number of invocations of transactionSuccess callback
        AtomicInteger lastFailedItem = new AtomicInteger(0); // last item that triggered a failure
        final AtomicInteger limitRef = new AtomicInteger(-1);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> CompletableFuture.supplyAsync(() -> {
            // fail 5 times
            if (failCount.get() < 5) {
                int itemNumber = item.get();
                // fail every other item starting at item 3
                if ((itemNumber > 2) && (itemNumber >= lastFailedItem.get() + 2)) {
                    failCount.incrementAndGet();
                    lastFailedItem.set(itemNumber);
                    throw new RuntimeException("intentionally failed while testing item " + item.get());
                }
            }
            quotaManager.deleteCountAdd(1);
            return null;
        });
        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            transactionStartCount.incrementAndGet();
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification = quotaManager -> {
            transactionCommitCount.incrementAndGet();
            totalScanned.addAndGet(quotaManager.getScannedCount());
            totalDeleted.addAndGet(quotaManager.getDeletesCount());
        };

        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(numRecords, itemHandler, initNotification, successNotification, -1, deleteLimit, -1, -1, limitRef).build()) {
            throttledIterator.iterateAll(storeBuilder).join();
        }

        assertThat(totalScanned.get()).isEqualTo(numRecords);
        assertThat(totalDeleted.get()).isEqualTo(numRecords);
        assertThat(failCount.get()).isEqualTo(5);
        assertThat(transactionStartCount.get()).isEqualTo(transactionCommitCount.get() + failCount.get());
        assertThat(limitRef.get()).isLessThanOrEqualTo(3); // Scan failure after 3 will cause the limit to become 3
    }

    @CsvSource({"-1", "0", "25", "50", "100"})
    @ParameterizedTest
    void testThrottleIteratorWithFailuresSecondsLimit(int maxPerSecLimit) throws Exception {
        // Assert correct handling of max per sec items with failures
        final int numRecords = 43;
        AtomicInteger totalScanned = new AtomicInteger(0); // number of items scanned
        AtomicInteger failCount = new AtomicInteger(0); // number of exception thrown
        AtomicInteger transactionStartCount = new AtomicInteger(0); // number of invocations of RangeInit callback
        AtomicInteger transactionCommitCount = new AtomicInteger(0); // number of invocations of RangeSuccess callback
        AtomicInteger lastFailedItem = new AtomicInteger(0); // last item that triggered a failure

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> CompletableFuture.supplyAsync(() -> {
            // fail 5 times
            if (failCount.get() < 5) {
                int itemNumber = item.get();
                // fail every other item starting at item 3
                if ((itemNumber > 2) && (itemNumber >= lastFailedItem.get() + 2)) {
                    failCount.incrementAndGet();
                    lastFailedItem.set(itemNumber);
                    throw new RuntimeException("intentionally failed while testing item " + item.get());
                }
            }
            return null;
        });
        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            transactionStartCount.incrementAndGet();
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification = quotaManager -> {
            transactionCommitCount.incrementAndGet();
            totalScanned.addAndGet(quotaManager.getScannedCount());
        };

        long startTime = System.currentTimeMillis();
        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(numRecords, itemHandler, initNotification, successNotification, maxPerSecLimit, -1, -1, -1, null).build()) {
            throttledIterator.iterateAll(storeBuilder).join();
        }

        long totalTimeMillis = System.currentTimeMillis() - startTime;
        if (maxPerSecLimit > 0) {
            assertThat(totalTimeMillis).isGreaterThan(TimeUnit.SECONDS.toMillis(numRecords / maxPerSecLimit));
        }

        assertThat(totalScanned.get()).isEqualTo(numRecords);
        assertThat(failCount.get()).isEqualTo(5);
        assertThat(transactionStartCount.get()).isEqualTo(transactionCommitCount.get() + failCount.get()); //
    }

    @CsvSource({"-1", "0", "1", "10"})
    @ParameterizedTest
    void testConstantFailures(int numRetries) throws Exception {
        // All item handlers will fail, ensure iteration fails with correct number of retries
        final String failureMessage = "intentionally failed while testing";
        AtomicInteger transactionStart = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(false);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> futureFailure();
        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            transactionStart.incrementAndGet();
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification = quotaManager -> {
            success.set(true);
        };

        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(500, itemHandler, initNotification, successNotification, -1, -1, numRetries, -1, null).build()) {
            Throwable ex = Assertions.catchThrowableOfType(RuntimeException.class, () -> throttledIterator.iterateAll(storeBuilder).join());
            assertThat(ex.getMessage()).contains(failureMessage);
        }

        if (numRetries == -1) {
            assertThat(transactionStart.get()).isEqualTo(ThrottledRetryingIterator.NUMBER_OF_RETRIES + 1);
        } else {
            assertThat(transactionStart.get()).isEqualTo(numRetries + 1);
        }
        assertThat(success.get()).isFalse();
    }

    @Test
    void testLimitHandlingOnFailure() throws Exception {
        // Actually compare set limit when transactions fail
        final String failureMessage = "intentionally failed while testing";
        final AtomicInteger limitRef = new AtomicInteger(0);
        final AtomicInteger failCount = new AtomicInteger(0);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            int limit = limitRef.get();
            int scannedCount = quotaManager.getScannedCount();
            switch (failCount.get()) {
                case 0:
                    assertThat(limit).isEqualTo(0);
                    if (scannedCount == 100) {
                        failCount.incrementAndGet();
                        return futureFailure();
                    }
                    return AsyncUtil.DONE;
                case 1:
                    assertThat(limit).isEqualTo(90); // (90% of 100)
                    if (scannedCount == 50) {
                        failCount.incrementAndGet();
                        return futureFailure();
                    }
                    return AsyncUtil.DONE;
                case 2:
                    assertThat(limit).isEqualTo(45); // (90% of 50)
                    // from now on: fail at first item
                    break;
                default:
                    assertThat(failCount.get()).isLessThanOrEqualTo(100);
                    break;
            }
            failCount.incrementAndGet();
            return futureFailure();
        };

        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(999, itemHandler, null, null, -1, -1, -1, -1, limitRef).build()) {
            Throwable ex = Assertions.catchThrowableOfType(RuntimeException.class, () -> throttledIterator.iterateAll(storeBuilder).join());
            assertThat(ex.getMessage()).contains(failureMessage);
        }

        assertThat(limitRef.get()).isOne();
    }

    @Test
    void testLimitHandlingOnSuccess() throws Exception {
        // Actually compare rows limit when transactions succeed
        final AtomicInteger limitRef = new AtomicInteger(0);
        final AtomicInteger fullCount = new AtomicInteger(0);
        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            int limit = limitRef.get();
            int count = fullCount.incrementAndGet();
            // Fail once to get the limit down
            if (count == 1) {
                throw new RuntimeException("Blah");
            }
            if (count <= 41) {         // 1 * 40 + 1 (limit * successes) before change
                assertThat(limit).isEqualTo(1);
            } else if (count <= 241) {  // 41 + (5 * 40)
                assertThat(limit).isEqualTo(5);
            } else if (count <= 601) { // 241 + (9 * 40)
                assertThat(limit).isEqualTo(9);
            } else {
                // end all iterations
                quotaManager.markExhausted();
            }
            return AsyncUtil.DONE;
        };

        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(2000, itemHandler, null, null, -1, -1, -1, -1, limitRef).build()) {
            throttledIterator.iterateAll(storeBuilder).join();
        }
    }

    @CsvSource({"0", "1", "20", "50"})
    @ParameterizedTest
    void testEarlyReturn(int lastItemToScan) throws Exception {
        // Early termination of iteration via setting markExhausted
        final int numRecords = 50;
        AtomicInteger totalScanned = new AtomicInteger(0); // number of items scanned

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            int itemNumber = item.get();
            if (itemNumber == lastItemToScan) {
                quotaManager.markExhausted();
            }
            return AsyncUtil.DONE;
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification = quotaManager -> {
            totalScanned.addAndGet(quotaManager.getScannedCount());
        };

        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        try (ThrottledRetryingIterator<Integer> throttledIterator =
                     iteratorBuilder(numRecords, itemHandler, null, successNotification, -1, -1, -1, -1, null).build()) {
            throttledIterator.iterateAll(storeBuilder).join();
        }

        assertThat(totalScanned.get()).isEqualTo(Math.min(50, lastItemToScan + 1));
    }

    @Test
    void testWithRealRecords() throws Exception {
        // A test with saved records, to see that future handling works
        final int numRecords = 50;
        List<Integer> itemsScanned = new ArrayList<>(numRecords);

        final CursorFactory<Tuple> cursorFactory = (store, lastResult, rowLimit) -> {
            final byte[] continuation = lastResult == null ? null : lastResult.getContinuation().toBytes();
            final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(executeProperties -> executeProperties.setReturnedRowLimit(rowLimit));
            return store.scanRecordKeys(continuation, scanProperties);
        };

        final ItemHandler<Tuple> itemHandler = (store, item, quotaManager) -> {
            return store.loadRecordAsync(item.get()).thenApply(rec -> {
                TestRecords1Proto.MySimpleRecord.Builder simpleRec = TestRecords1Proto.MySimpleRecord.newBuilder();
                simpleRec.mergeFrom(rec.getRecord());
                itemsScanned.add((int)simpleRec.getRecNo());
                return null;
            });
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int i = 0; i < numRecords; i++) {
                final TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setStrValueIndexed("Some text")
                        .setNumValue3Indexed(1415 + i * 7)
                        .build();
                recordStore.saveRecord(record);
            }
            commit(context);
        }

        // For this test, start and finalize the iteration within the transaction
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Tuple> builder = ThrottledRetryingIterator
                    .builder(fdb, cursorFactory, itemHandler)
                    .withNumOfRetries(2);
            try (ThrottledRetryingIterator<Tuple> iterator = builder.build()) {
                iterator.iterateAll(recordStore.asBuilder()).join();
            }
        }
        assertThat(itemsScanned).isEqualTo(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()));

        // For this test, start iteration within the transaction but allow it to run (and create more transactions) outside
        // of the original transaction
        itemsScanned.clear();
        try (ThrottledRetryingIterator<Tuple> iterator = ThrottledRetryingIterator
                .builder(fdb, cursorFactory, itemHandler)
                .withNumOfRetries(2)
                .build()) {
            CompletableFuture<Void> iterateAll;
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                iterateAll = iterator.iterateAll(recordStore.asBuilder());
            }
            iterateAll.join();
        }
        assertThat(itemsScanned).isEqualTo(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()));
    }

    @Test
    void testLateCompleteFutures() throws Exception {
        // A test that completes the first future outside the transaction
        int numRecords = 50;
        List<CompletableFuture<Void>> futures = new ArrayList<>(numRecords);
        // A gate to stop the test until the futures get scheduled
        Semaphore gate = new Semaphore(0);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            // First future hangs on, all others are immediately completed
            CompletableFuture<Void> future = (item.get() == 0) ? new CompletableFuture<>() : CompletableFuture.completedFuture(null);
            futures.add(future);
            // Release the rest of the flow once handling of the first item initiated
            gate.release();
            return future;
        };

        try (ThrottledRetryingIterator<Integer> throttledIterator =
                iteratorBuilder(numRecords, itemHandler, null, null, -1, -1, -1, -1, null).build()) {
            final CompletableFuture<Void> iterateAll;
            // This time the transaction is opened after the iterator is created and the iteration starts inside it
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                iterateAll = throttledIterator.iterateAll(recordStore.asBuilder());
                commit(context);
            }
            // Block here to let the futures a chance to get scheduled (the store future will trigger the iteration and then the itemHandler)
            gate.acquire();
            // Only first future in the list - waiting for it to complete
            assertThat(futures).hasSize(1);
            // complete the first future, release all of them
            futures.get(0).complete(null);
            iterateAll.join();
        }
        assertThat(futures).hasSize(50);
    }

    @Test
    void testIteratorClosesIncompleteFutures() throws Exception {
        // close the runner before the future completes (the futures should be closed)
        int numRecords = 50;
        AtomicInteger transactionStart = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>(numRecords);
        AtomicReference<RecordCursor<Integer>> cursor = new AtomicReference<>();
        // A gate to stop the test until the futures get scheduled
        Semaphore gate = new Semaphore(0);

        final CursorFactory<Integer> cursorFactory = (store, lastResult, rowLimit) -> {
            cursor.set(RecordCursor.fromList(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()), null));
            return cursor.get();
        };

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            // First future hangs on, all others are immediately completed
            CompletableFuture<Void> future = (item.get() == 0) ? new CompletableFuture<>() : CompletableFuture.completedFuture(null);
            futures.add(future);
            // Release the rest of the flow once handling of the first item initiated
            gate.release();
            return future;
        };

        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            transactionStart.incrementAndGet();
        };

        final CompletableFuture<Void> iterateAll;
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        final ThrottledRetryingIterator.Builder<Integer> builder = ThrottledRetryingIterator.builder(fdb, cursorFactory, itemHandler)
                .withTransactionInitNotification(initNotification);
        try (ThrottledRetryingIterator<Integer> iterator = builder.build()) {
            iterateAll = iterator.iterateAll(storeBuilder);
            // Block here to let the futures a chance to get scheduled (the store future will trigger the iteration and then the itemHandler)
            gate.acquire();
            // Closing the iterator before the first future completes
        }

        // Only first future in the list, none other was created since the first one didn't complete
        assertThat(futures).hasSize(1);
        assertThat(futures.get(0).isCompletedExceptionally()).isTrue();
        assertThatThrownBy(() -> futures.get(0).get()).hasCauseInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
        // Overall status is failed because we can't runAsync() anymore
        assertThatThrownBy(iterateAll::join).hasCauseInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
        // Only one transaction started (no retry), since the runner was closed
        assertThat(transactionStart.get()).isOne();
        // Cursor is closed
        Assertions.assertThat(cursor.get().isClosed()).isTrue();
    }

    /**
     * A test crafted to ensure that the future returned by the onNext call is closed.
     * This future is hidden from outside so we have to trick things a little to get to it.
     */
    @Test
    void testIteratorClosesOnNextCloses() throws Exception {
        // This future never completes
        CompletableFuture<RecordCursorResult<Integer>> future = new CompletableFuture<>();
        AtomicReference<RecordCursor<Integer>> cursor = new AtomicReference<>();
        // A gate to stop the test until the futures get scheduled
        Semaphore gate = new Semaphore(0);

        final CursorFactory<Integer> cursorFactory = (store, lastResult, rowLimit) -> {
            cursor.set(new SingleItemCursor<>(store.getExecutor(), future));
            // Release the rest of the flow once handling of the first item initiated
            gate.release();
            return cursor.get();
        };

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            // This should never be called since the future never completes
            return CompletableFuture.failedFuture(new RuntimeException("Should not be called"));
        };

        final CompletableFuture<Void> iterateAll;
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder();
            commit(context);
        }
        final ThrottledRetryingIterator.Builder<Integer> builder = ThrottledRetryingIterator.builder(fdb, cursorFactory, itemHandler);
        try (ThrottledRetryingIterator<Integer> iterator = builder.build()) {
            iterateAll = iterator.iterateAll(storeBuilder);
            // Block here to let the futures a chance to get scheduled (the store future will trigger the iteration and then the itemHandler)
            gate.acquire();
            // Closing the iterator before the first future completes
        }

        // Only first future in the list, none other was created since the first one didn't complete
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(() -> future.get()).hasCauseInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
        // Overall status is failed because we can't runAsync() anymore
        assertThatThrownBy(iterateAll::join).hasCauseInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
        // Cursor is closed
        Assertions.assertThat(cursor.get().isClosed()).isTrue();
    }

    private static Stream<Arguments> mdcParams() {
        return Stream.of(
                Arguments.of("value", 10, 0, 1), // have MDC value, one transaction
                Arguments.of(null, 10, 0, 1),    // null MDC, one transaction
                Arguments.of("value", 10, 6, 2)  // have MDC, 2 transactions
        );
    }

    @ParameterizedTest
    @MethodSource("mdcParams")
    void testMdcContextPropagation(String mdcValue, int numRecords, int deletesPerTransaction, int expectedTransactions) throws Exception {
        String mdcKey = "mdckey";
        final Map<String, String> original = MDC.getCopyOfContextMap();

        try {
            // Set MDC context if provided
            if (mdcValue != null) {
                MDC.clear();
                MDC.put(mdcKey, mdcValue);
            }
            final AtomicInteger transactionCount = new AtomicInteger(0);
            final Consumer<ThrottledRetryingIterator.QuotaManager> transactionStart =
                    quotaManager -> transactionCount.incrementAndGet();
            final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
                assertThat(MDC.get(mdcKey)).isEqualTo(mdcValue); // covers null case
                quotaManager.deleteCountInc();
                return AsyncUtil.DONE;
            };

            final FDBRecordStore.Builder storeBuilder;
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                storeBuilder = recordStore.asBuilder();
                commit(context);
            }

            Map<String, String> mdcContext = (mdcValue == null) ? null : MDC.getCopyOfContextMap();
            ThrottledRetryingIterator.Builder<Integer> builder =
                    ThrottledRetryingIterator.builder(fdb, intCursor(numRecords, null), itemHandler)
                            .withMdcContext(mdcContext);

            builder.withMaxRecordsDeletesPerTransaction(deletesPerTransaction)
                    .withTransactionInitNotification(transactionStart);

            try (ThrottledRetryingIterator<Integer> throttledIterator = builder.build()) {
                throttledIterator.iterateAll(storeBuilder).join();
            }

            assertThat(transactionCount.get()).isEqualTo(expectedTransactions);
        } finally {
            MDC.setContextMap(original);
        }
    }

    private ThrottledRetryingIterator.Builder<Integer> iteratorBuilder(final int numRecords,
                                                                       final ItemHandler<Integer> itemHandler,
                                                                       final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification,
                                                                       final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification,
                                                                       final int maxPerSecLimit,
                                                                       final int maxDeletedPerTransaction, final int numRetries,
                                                                       final int transactionTimeMillis, final AtomicInteger limitRef) {

        ThrottledRetryingIterator.Builder<Integer> throttledIterator = ThrottledRetryingIterator.builder(fdb, intCursor(numRecords, limitRef), itemHandler);

        if (successNotification != null) {
            throttledIterator.withTransactionSuccessNotification(successNotification);
        }
        if (initNotification != null) {
            throttledIterator.withTransactionInitNotification(initNotification);
        }
        if (maxPerSecLimit != -1) {
            throttledIterator.withMaxRecordsScannedPerSec(maxPerSecLimit);
        }
        if (maxDeletedPerTransaction != -1) {
            throttledIterator.withMaxRecordsDeletesPerTransaction(maxDeletedPerTransaction);
        }
        if (numRetries != -1) {
            throttledIterator.withNumOfRetries(numRetries);
        }
        if (transactionTimeMillis != -1) {
            throttledIterator.withTransactionTimeQuotaMillis(transactionTimeMillis);
        }
        return throttledIterator;
    }

    private CursorFactory<Integer> intCursor(int numInts, AtomicInteger limitRef) {
        return listCursor(IntStream.range(0, numInts).boxed().collect(Collectors.toList()), limitRef);
    }

    private <T> CursorFactory<T> listCursor(List<T> items, AtomicInteger limitRef) {
        return (store, cont, limit) -> {
            if (limitRef != null) {
                limitRef.set(limit);
            }
            final byte[] continuation = cont == null ? null : cont.getContinuation().toBytes();
            return RecordCursor.fromList(items, continuation).limitRowsTo(limit);
        };
    }

    private CompletableFuture<Void> futureFailure() {
        return CompletableFuture.failedFuture(new RuntimeException("intentionally failed while testing"));
    }

    /**
     * A special purpose cursor that returns the future it was given and then ends.
     * This is different than the {@link FutureCursor} since it returns the future it was given exactly without any
     * additional dependents, making the called be able to assert on the returned future externally.
     */
    private class SingleItemCursor<T> implements RecordCursor<T> {
        private boolean done = false;
        private final CompletableFuture<RecordCursorResult<T>> future;
        private final Executor executor;

        public SingleItemCursor(final Executor executor, final CompletableFuture<RecordCursorResult<T>> future) {
            this.future = future;
            this.executor = executor;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<T>> onNext() {
            if (done) {
                return CompletableFuture.completedFuture(RecordCursorResult.exhausted());
            } else {
                done = true;
                return future;
            }
        }

        @Override
        public void close() {
            done = true;
        }

        @Override
        public boolean isClosed() {
            return done;
        }

        @Nonnull
        @Override
        public Executor getExecutor() {
            return executor;
        }

        @Override
        public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
            visitor.visitEnter(this);
            return visitor.visitLeave(this);
        }
    }
}
