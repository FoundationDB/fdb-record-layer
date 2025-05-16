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

package com.apple.foundationdb.record.provider.foundationdb.cursors.throttled;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.tuple.Tuple;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        assertThat(ThrottledRetryingIterator.increaseLimit(0, 0)).isEqualTo(0);
        assertThat(ThrottledRetryingIterator.increaseLimit(0, 100)).isEqualTo(0);
        assertThat(ThrottledRetryingIterator.increaseLimit(100, 0)).isEqualTo(125);
        assertThat(ThrottledRetryingIterator.increaseLimit(1, 0)).isEqualTo(5);
        assertThat(ThrottledRetryingIterator.increaseLimit(3, 0)).isEqualTo(7);
        assertThat(ThrottledRetryingIterator.increaseLimit(10, 10)).isEqualTo(10);
        assertThat(ThrottledRetryingIterator.increaseLimit(10, 5)).isEqualTo(5);
    }

    @Test
    void testDecreaseLimit() {
        assertThat(ThrottledRetryingIterator.decreaseLimit(0)).isEqualTo(1);
        assertThat(ThrottledRetryingIterator.decreaseLimit(1)).isEqualTo(1);
        assertThat(ThrottledRetryingIterator.decreaseLimit(2)).isEqualTo(1);
        assertThat(ThrottledRetryingIterator.decreaseLimit(3)).isEqualTo(2);
        assertThat(ThrottledRetryingIterator.decreaseLimit(100)).isEqualTo(90);
    }

    @CsvSource({"-1, -1", "0, 0", "-1, 0", "0,-1", "-1, 100", "0, 100", "1, 100", "1, -1", "1, 0", "3, 100", "100, 100"})
    @ParameterizedTest
    void testThrottleIteratorSuccessRowLimit(int initialRowLimit, int maxRowsLimit) throws Exception {
        // Iterate range, verify that the number of items scanned matches the number of records
        //Ensure multiple transactions are playing nicely with the scanned range
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

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(numRecords, itemHandler, null, successNotification, initialRowLimit, maxRowsLimit, -1, -1, -1, limitRef);
            throttledIterator.build().iterateAll(recordStore.asBuilder()).join();
        }

        assertThat(iteratedCount.get()).isEqualTo(numRecords);
        assertThat(deletedCount.get()).isEqualTo(numRecords);
        if ((maxRowsLimit <= 0) && (initialRowLimit <= 0)) {
            assertThat(limitRef.get()).isZero();
        } else {
            if (initialRowLimit > 0) {
                assertThat(limitRef.get()).isGreaterThanOrEqualTo(initialRowLimit);
            }
            if (maxRowsLimit > 0) {
                assertThat(limitRef.get()).isLessThanOrEqualTo(maxRowsLimit);
            }
        }
        if ((limitRef.get() == 0) || (limitRef.get() == 100)) {
            assertThat(successTransactionCount.get()).isOne();
        } else {
            assertThat(successTransactionCount.get()).isGreaterThan(1);
        }
    }

    @CsvSource({"-1", "0", "50", "100"})
    @ParameterizedTest
    void testThrottleIteratorSuccessSecondsLimit(int maxPerSecLimit) throws Exception {
        // Iterate range, verify that the number of items scanned matches the number of records
        // Assert that the total test takes longer because of the max per sec limit
        final int numRecords = 50;
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

        long startTimeMillis = System.currentTimeMillis();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(numRecords, itemHandler, null, successNotification, -1, 10, maxPerSecLimit, -1, -1, limitRef);
            throttledIterator.build().iterateAll(recordStore.asBuilder()).join();
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
        final int numRecords = 50;
        final int delay = 10;
        final int transactionTimeMillis = 50;
        AtomicInteger initTransactionCount = new AtomicInteger(0);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            return MoreAsyncUtil.delayedFuture(delay, TimeUnit.MILLISECONDS);
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            initTransactionCount.incrementAndGet();
        };

        long startTimeMillis = System.currentTimeMillis();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(numRecords, itemHandler, initNotification, null, -1, -1, -1, -1, transactionTimeMillis, null);
            throttledIterator.build().iterateAll(recordStore.asBuilder()).join();
        }

        long totalTimeMillis = System.currentTimeMillis() - startTimeMillis;
        assertThat(totalTimeMillis).isGreaterThan(numRecords * delay);
        assertThat(initTransactionCount.get()).isGreaterThanOrEqualTo(numRecords * delay / transactionTimeMillis);
    }

    @CsvSource({"-1, -1", "0, 0", "-1, 0", "0,-1", "-1, 100", "0, 100", "1, 100", "1, -1", "1, 0", "3, 100", "100, 100"})
    @ParameterizedTest
    void testThrottleIteratorFailuresRowLimit(int initialRowLimit, int maxRowsLimit) throws Exception {
        // Fail some handlings, ensure transaction restarts, items scanned
        final int numRecords = 43;
        AtomicInteger totalScanned = new AtomicInteger(0); // number of items scanned
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
            return null;
        });
        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            transactionStartCount.incrementAndGet();
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification = quotaManager -> {
            transactionCommitCount.incrementAndGet();
            totalScanned.addAndGet(quotaManager.getScannedCount());
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(numRecords, itemHandler, initNotification, successNotification, initialRowLimit, maxRowsLimit, -1, -1, -1, limitRef);
            throttledIterator.build().iterateAll(recordStore.asBuilder()).join();
        }
        assertThat(totalScanned.get()).isEqualTo(numRecords);
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
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(numRecords, itemHandler, initNotification, successNotification, -1, 10, maxPerSecLimit, -1, -1, null);
            throttledIterator.build().iterateAll(recordStore.asBuilder()).join();
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

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(500, itemHandler, initNotification, successNotification, -1, 10, -1, numRetries, -1, null);
            Throwable ex = Assertions.catchThrowableOfType(RuntimeException.class, () -> throttledIterator.build().iterateAll(recordStore.asBuilder()).join());

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
                    assertThat(limit).isEqualTo(200);
                    if (scannedCount == 100) {
                        failCount.incrementAndGet();
                        return futureFailure();
                    }
                    return AsyncUtil.DONE;
                case 1:
                    assertThat(limit).isEqualTo(90);
                    if (scannedCount == 50) {
                        failCount.incrementAndGet();
                        return futureFailure();
                    }
                    return AsyncUtil.DONE;
                case 2:
                    assertThat(limit).isEqualTo(45);
                    // from now on: fail at first item
                    break;
                default:
                    assertThat(failCount.get()).isLessThanOrEqualTo(100);
                    break;
            }
            failCount.incrementAndGet();
            return futureFailure();
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(999, itemHandler, null, null, 200, -1, -1, -1, -1, limitRef);
            Throwable ex = Assertions.catchThrowableOfType(RuntimeException.class, () -> throttledIterator.build().iterateAll(recordStore.asBuilder()).join());
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
            int scannedCount = quotaManager.getScannedCount();
            int count = fullCount.incrementAndGet();
            if (count <= 400) {         // 10 * 40 (limit * successes) before change
                assertThat(limit).isEqualTo(10);
            } else if (count <= 960) {  // 400 + (14 * 40)
                assertThat(limit).isEqualTo(14);
            } else if (count <= 1480) { // 960 + (18 * 40)
                assertThat(limit).isEqualTo(18);
            } else {
                // end all iterations
                quotaManager.markExhausted();
            }
            return AsyncUtil.DONE;
        };

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(2000, itemHandler, null, null, 10, 100, -1, -1, -1, limitRef);
            throttledIterator.build().iterateAll(recordStore.asBuilder()).join();
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

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            ThrottledRetryingIterator.Builder<Integer> throttledIterator =
                    iteratorBuilder(numRecords, itemHandler, null, successNotification, -1, 10, -1, -1, -1, null);
            throttledIterator.build().iterateAll(recordStore.asBuilder()).join();
        }
        assertThat(totalScanned.get()).isEqualTo(Math.min(50, lastItemToScan + 1));
    }

    @CsvSource({"-1", "0", "1", "10", "100"})
    @ParameterizedTest
    void testWithRealRecords(int maxRowLimit) throws Exception {
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
                    .withNumOfRetries(2)
                    .withMaxRecordsScannedPerTransaction(maxRowLimit);
            try (ThrottledRetryingIterator<Tuple> iterator = builder.build()) {
                iterator.iterateAll(recordStore.asBuilder()).join();
            }
        }
        assertThat(itemsScanned).isEqualTo(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()));

        // For this test, start iteration within the transaction but allow it to run (and create more transactions) outside
        // of the original transaction
        itemsScanned.clear();
        ThrottledRetryingIterator<Tuple> iterator = ThrottledRetryingIterator
                .builder(fdb, cursorFactory, itemHandler)
                .withNumOfRetries(2)
                .withMaxRecordsScannedPerTransaction(maxRowLimit)
                .build();
        CompletableFuture<Void> iterateAll;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            iterateAll = iterator.iterateAll(recordStore.asBuilder());
        }
        iterateAll.join();
        iterator.close();
        assertThat(itemsScanned).isEqualTo(IntStream.range(0, numRecords).boxed().collect(Collectors.toList()));
    }

    @Test
    void testLateCompleteFutures() throws Exception {
        // A test that completes the first future outside the transaction
        int numRecords = 50;
        List<CompletableFuture<Void>> futures = new ArrayList<>(numRecords);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            // First future hangs on, all others are immediately completed
            CompletableFuture<Void> future = (item.get() == 0) ? new CompletableFuture<>() : CompletableFuture.completedFuture(null);
            futures.add(future);
            return future;
        };

        ThrottledRetryingIterator<Integer> throttledIterator =
                iteratorBuilder(numRecords, itemHandler, null, null, -1, 10, -1, -1, -1, null).build();
        final CompletableFuture<Void> iterateAll;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            iterateAll = throttledIterator.iterateAll(recordStore.asBuilder());
        }
        // Only first future in the list - waiting for it to complete
        assertThat(futures).hasSize(1);
        // complete the first future, release all of them
        futures.get(0).complete(null);
        iterateAll.join();
        throttledIterator.close();
        assertThat(futures).hasSize(50);
    }

    @Test
    void testIteratorClosesIncompleteFutures() throws Exception {
        // close the runner before the future completes (the futures should be closed)
        int numRecords = 50;
        AtomicInteger transactionStart = new AtomicInteger(0);
        List<CompletableFuture<Void>> futures = new ArrayList<>(numRecords);

        final ItemHandler<Integer> itemHandler = (store, item, quotaManager) -> {
            // First future hangs on, all others are immediately completed
            CompletableFuture<Void> future = (item.get() == 0) ? new CompletableFuture<>() : CompletableFuture.completedFuture(null);
            futures.add(future);
            return future;
        };
        final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification = quotaManager -> {
            transactionStart.incrementAndGet();
        };

        ThrottledRetryingIterator<Integer> throttledIterator =
                iteratorBuilder(numRecords, itemHandler, initNotification, null, -1, 10, -1, -1, -1, null).build();
        final CompletableFuture<Void> iterateAll;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            iterateAll = throttledIterator.iterateAll(recordStore.asBuilder());
        }
        // Closing the iterator before the first future completes
        throttledIterator.close();
        // Only first future in the list, none other was created since the first one didn't complete
        assertThat(futures).hasSize(1);
        assertThat(futures.get(0).isCompletedExceptionally()).isTrue();
        assertThatThrownBy(() -> futures.get(0).get()).hasCauseInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
        // Overall status is failed because we can't runAsync() anymore
        assertThatThrownBy(iterateAll::join).hasCauseInstanceOf(FDBDatabaseRunner.RunnerClosed.class);
        // Only one transaction started (no retry), since the runner was closed
        assertThat(transactionStart.get()).isOne();
    }

    private ThrottledRetryingIterator.Builder<Integer> iteratorBuilder(final int numRecords,
                                                                       final ItemHandler<Integer> itemHandler,
                                                                       final Consumer<ThrottledRetryingIterator.QuotaManager> initNotification,
                                                                       final Consumer<ThrottledRetryingIterator.QuotaManager> successNotification,
                                                                       final int initialRowLimit, final int maxRowsLimit,
                                                                       final int maxPerSecLimit,
                                                                       final int numRetries,
                                                                       final int transactionTimeMillis, final AtomicInteger limitRef) {

        ThrottledRetryingIterator.Builder<Integer> throttledIterator = ThrottledRetryingIterator.builder(fdb, intCursor(numRecords, limitRef), itemHandler);

        if (successNotification != null) {
            throttledIterator.withTransactionSuccessNotification(successNotification);
        }
        if (initNotification != null) {
            throttledIterator.withTransactionInitNotification(initNotification);
        }
        if (maxRowsLimit != -1) {
            throttledIterator.withMaxRecordsScannedPerTransaction(maxRowsLimit);
        }
        if (initialRowLimit != -1) {
            throttledIterator.withInitialRecordsScannedPerTransaction(initialRowLimit);
        }
        if (maxPerSecLimit != -1) {
            throttledIterator.withMaxRecordsScannedPerSec(maxPerSecLimit);
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
}
